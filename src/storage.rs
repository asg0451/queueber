use capnp::message::{self, TypedReader};
use capnp::serialize_packed;
use rocksdb::{DB, Options, SliceTransform, WriteBatchWithTransaction};
use std::io::BufReader;
use std::path::Path;
use uuid::Uuid;

use crate::errors::{Error, Result};
use crate::protocol;

// DB SCHEMA:
// "available/" + id -> stored_item{item, visibility_ts_index_key}
// "in_progress/" + id -> stored_item{item, visibility_ts_index_key}
// "visibility_index/" + visibility_ts -> id
// "leases/" + lease -> lease_entry{keys}

const AVAILABLE_PREFIX: &[u8] = b"available/";
const IN_PROGRESS_PREFIX: &[u8] = b"in_progress/";
const VISIBILITY_INDEX_PREFIX: &[u8] = b"visibility_index/";
const LEASE_PREFIX: &[u8] = b"leases/";

pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        // Optimize for prefix scans used by `prefix_iterator` across all key namespaces.
        // Extract the namespace prefix up to and including the first '/'.
        // Examples:
        //  - b"visibility_index/123/abc" -> b"visibility_index/"
        //  - b"available/42" -> b"available/"
        //  - b"leases/<uuid>" -> b"leases/"
        // If a key contains no '/', return the full key.
        let ns_prefix = SliceTransform::create(
            "ns_prefix",
            |key: &[u8]| match key.iter().position(|b| *b == b'/') {
                Some(idx) => &key[..=idx],
                None => key,
            },
            Some(|_key: &[u8]| true),
        );
        opts.set_prefix_extractor(ns_prefix);
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    // id+item -> store stored_item and visibility index entry
    #[tracing::instrument(skip(self))]
    pub fn add_available_item<'i>(
        &self,
        (id, item): (&'i [u8], protocol::item::Reader<'i>),
    ) -> Result<()> {
        let main_key = make_main_key(id, AVAILABLE_PREFIX)?;
        let visibility_index_key =
            make_visibility_index_key(id, item.get_visibility_timeout_secs())?;

        // make stored item to insert
        let mut simsg = message::Builder::new_default();
        let mut stored_item = simsg.init_root::<protocol::stored_item::Builder>();
        stored_item.set_contents(item.get_contents()?);
        stored_item.set_id(id);
        stored_item.set_visibility_ts_index_key(&visibility_index_key);
        let mut contents = Vec::with_capacity(simsg.size_in_words() * 8); // TODO: reuse
        serialize_packed::write_message(&mut contents, &simsg)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.put(&main_key, &contents);
        batch.put(&visibility_index_key, &main_key);
        self.db.write(batch)?;

        tracing::debug!(
            "inserted item: ({}: {}), ({}: {})",
            String::from_utf8_lossy(&main_key),
            String::from_utf8_lossy(&contents),
            String::from_utf8_lossy(&visibility_index_key),
            String::from_utf8_lossy(&main_key)
        );

        Ok(())
    }

    pub fn add_available_items<'i>(
        &self,
        items: impl Iterator<Item = (&'i [u8], protocol::item::Reader<'i>)>,
    ) -> Result<()> {
        // TODO: more intelligently
        for entry in items {
            self.add_available_item(entry)?;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn get_next_available_entries(
        &self,
        n: usize,
    ) -> Result<(
        Lease,
        Vec<
            TypedReader<
                capnp::message::Builder<message::HeapAllocator>,
                protocol::polled_item::Owned,
            >,
        >,
    )> {
        // Get current timestamp to filter only visible items
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        
        // Create a range iterator that efficiently scans only visible items
        // The visibility index key format is: visibility_index/ + timestamp + "/" + id
        // Since timestamps are stored as little-endian bytes and keys are ordered,
        // we can create a range from the start to a key that represents the current timestamp
        let start_key = VISIBILITY_INDEX_PREFIX.to_vec();
        
        // Create an end key that represents the maximum timestamp for the current time
        // We add 1 to the current timestamp to create an exclusive upper bound
        let end_timestamp = now + 1;
        let end_timestamp_bytes = end_timestamp.to_le_bytes();
        let mut end_key = Vec::with_capacity(VISIBILITY_INDEX_PREFIX.len() + end_timestamp_bytes.len());
        end_key.extend_from_slice(VISIBILITY_INDEX_PREFIX);
        end_key.extend_from_slice(&end_timestamp_bytes);
        
        // Use a range iterator that scans from start_key to end_key (exclusive)
        // This leverages the ordered nature of the keys for efficient prefix scanning
        // and automatically stops when we reach keys with timestamps >= end_timestamp
        let iter = self.db.iterator(rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        // move these items to in progress and create a lease
        // NOTE: this means either scanning twice or buffering. going with the latter for now, since n is probably small.
        // TODO: can we do better?

        // TODO: use a mockable clock
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = make_lease_key(&lease);

        let mut polled_items = Vec::with_capacity(n);
        let mut lease_keys = Vec::with_capacity(n);

        let mut in_progress_batch = WriteBatchWithTransaction::<false>::default();

        for (items_processed, kv) in iter.enumerate() {
            let (idx_key, main_key) = kv?;

            debug_assert_eq!(
                &idx_key[..VISIBILITY_INDEX_PREFIX.len()],
                VISIBILITY_INDEX_PREFIX
            );

            // Stop if we've reached the end of our range (keys with timestamps >= end_timestamp)
            if idx_key.as_ref() >= end_key.as_slice() {
                break;
            }

            // Extract timestamp from visibility index key for validation
            // Key format: visibility_index/ + timestamp + "/" + id
            let timestamp_start = VISIBILITY_INDEX_PREFIX.len();
            let timestamp_end = idx_key[timestamp_start..]
                .iter()
                .position(|&b| b == b'/')
                .ok_or_else(|| Error::AssertionFailed {
                    msg: "Invalid visibility index key format".to_string(),
                    backtrace: std::backtrace::Backtrace::capture(),
                })? + timestamp_start;
            
            let timestamp_bytes = &idx_key[timestamp_start..timestamp_end];
            let item_timestamp = u64::from_le_bytes(
                timestamp_bytes.try_into().map_err(|_| Error::AssertionFailed {
                    msg: "Invalid timestamp bytes in visibility index key".to_string(),
                    backtrace: std::backtrace::Backtrace::capture(),
                })?
            );
            
            // Double-check that the item is actually visible (defensive programming)
            if item_timestamp > now {
                break;
            }
            
            // Stop if we've processed enough items
            if items_processed >= n {
                break;
            }

            let main_value = self
                .db
                .get(&main_key)?
                .ok_or_else(|| Error::AssertionFailed {
                    msg: format!(
                        "database integrity violated: main key not found: {:?}",
                        &main_key
                    ),
                    backtrace: std::backtrace::Backtrace::capture(),
                })?;

            let stored_item_message = serialize_packed::read_message(
                BufReader::new(&main_value[..]), // TODO: avoid allocation
                message::ReaderOptions::new(),
            )?;
            let stored_item = stored_item_message.get_root::<protocol::stored_item::Reader>()?;

            debug_assert!(!stored_item.get_id()?.is_empty());
            debug_assert_eq!(stored_item.get_id()?, &main_key[AVAILABLE_PREFIX.len()..]);

            tracing::debug!(
                "got stored item: {{id: {}, contents: {}}}",
                String::from_utf8_lossy(stored_item.get_id()?),
                String::from_utf8_lossy(stored_item.get_contents()?)
            );

            // build the polled item
            let mut builder = message::Builder::new_default(); // TODO: reduce allocs
            let mut polled_item = builder.init_root::<protocol::polled_item::Builder>();
            polled_item.set_contents(stored_item.get_contents()?);
            polled_item.set_id(stored_item.get_id()?);
            let polled_item = builder.into_typed().into_reader();
            polled_items.push(polled_item);

            // move the item to in progress
            let new_main_key = make_main_key(stored_item.get_id()?, IN_PROGRESS_PREFIX)?;
            in_progress_batch.delete(&main_key);
            in_progress_batch.put(&new_main_key, &main_value);

            // remove the visibility index entry
            in_progress_batch.delete(&idx_key);

            // add the lease entry key
            lease_keys.push(main_key[AVAILABLE_PREFIX.len()..].to_vec());
            // TODO: make newtype safety stuff for these
        }

        // Create the lease entry with the actual number of keys
        let mut lease_entry = message::Builder::new_default();
        let lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
        let mut lease_entry_keys = lease_entry_builder.init_keys(lease_keys.len() as _);
        for (i, key) in lease_keys.iter().enumerate() {
            lease_entry_keys.set(i as _, key);
        }
        
        // add the lease to the lease set
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8);
        serialize_packed::write_message(&mut lease_entry_bs, &lease_entry)?;
        in_progress_batch.put(&lease_key, &lease_entry_bs);

        self.db.write(in_progress_batch)?;

        Ok((lease, polled_items))
    }
}

// it's a uuidv7
type Lease = [u8; 16];

// TODO: is there a way to do this without allocating / with an arena or smth? i'd like to avoid allocating in the hot path
// returns (main key, visibility index key)
fn make_main_key(id: &[u8], main_prefix: &'static [u8]) -> Result<Vec<u8>> {
    let mut main_key = Vec::with_capacity(main_prefix.len() + id.len());
    main_key.extend_from_slice(main_prefix);
    main_key.extend_from_slice(id);

    Ok(main_key)
}

fn make_visibility_index_key(id: &[u8], visibility_timeout_secs: u64) -> Result<Vec<u8>> {
    // TODO: use a mockable clock
    let now = std::time::SystemTime::now();
    let visible_ts_bs = (now + std::time::Duration::from_secs(visibility_timeout_secs))
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_le_bytes();

    let mut visibility_index_key =
        Vec::with_capacity(VISIBILITY_INDEX_PREFIX.len() + visible_ts_bs.len() + 1 + id.len());
    visibility_index_key.extend_from_slice(VISIBILITY_INDEX_PREFIX);
    visibility_index_key.extend_from_slice(&visible_ts_bs);
    visibility_index_key.extend_from_slice(b"/");
    visibility_index_key.extend_from_slice(id);

    Ok(visibility_index_key)
}

fn make_lease_key(lease: &Lease) -> Vec<u8> {
    let mut lease_key = Vec::with_capacity(LEASE_PREFIX.len() + lease.len());
    lease_key.extend_from_slice(LEASE_PREFIX);
    lease_key.extend_from_slice(lease);
    lease_key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol;
    use capnp::message::{Builder, HeapAllocator};

    #[test]
    fn e2e_test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let storage = Storage::new(Path::new("/tmp/queueber_test_db")).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all("/tmp/queueber_test_db").unwrap());

        let mut message = Builder::new_default();
        let item = make_item(&mut message);
        storage.add_available_item((b"42", item)).unwrap();
        let (lease, entries) = storage.get_next_available_entries(1)?;
        assert!(!lease.iter().all(|b| *b == 0));
        assert_eq!(entries.len(), 1);
        let si = entries[0].get()?;
        assert_eq!(si.get_id()?, b"42");
        assert_eq!(si.get_contents()?, b"hello");

        // check lease
        let lease_key = make_lease_key(&lease);
        let lease_value = storage.db.get(&lease_key)?.ok_or("lease not found")?;
        let lease_entry = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get(0)?, b"42");

        Ok(())
    }

    #[test]
    fn poll_moves_multiple_items_and_updates_indexes()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_db_multi";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add two items
        let mut message = Builder::new_default();
        let item = make_item(&mut message);
        storage.add_available_item((b"id1", item)).unwrap();
        let mut message2 = Builder::new_default();
        let item2 = make_item(&mut message2);
        storage.add_available_item((b"id2", item2)).unwrap();

        // poll two items
        let (lease, entries) = storage.get_next_available_entries(2)?;
        assert!(!lease.iter().all(|b| *b == 0));
        assert_eq!(entries.len(), 2);

        // collect ids
        let mut polled_ids = entries
            .iter()
            .map(|e| e.get().and_then(|pi| Ok(pi.get_id()?.to_vec())))
            .collect::<std::result::Result<Vec<_>, capnp::Error>>()?;
        polled_ids.sort();
        assert_eq!(polled_ids, vec![b"id1".to_vec(), b"id2".to_vec()]);

        // Inspect in-progress entries and ensure indexes/available entries were removed
        for id in [&b"id1"[..], &b"id2"[..]] {
            let in_progress_key = {
                let mut k = Vec::new();
                k.extend_from_slice(IN_PROGRESS_PREFIX);
                k.extend_from_slice(id);
                k
            };
            let value = storage
                .db
                .get(&in_progress_key)?
                .ok_or("in_progress value missing")?;

            // parse stored item to get original visibility index key
            let msg = serialize_packed::read_message(
                BufReader::new(&value[..]),
                message::ReaderOptions::new(),
            )?;
            let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;

            // available entry must be gone
            let mut avail_key = Vec::new();
            avail_key.extend_from_slice(AVAILABLE_PREFIX);
            avail_key.extend_from_slice(id);
            assert!(storage.db.get(&avail_key)?.is_none());

            // visibility index entry must be gone
            let idx_key = stored_item.get_visibility_ts_index_key()?;
            assert!(storage.db.get(idx_key)?.is_none());
        }

        // lease entry exists and has keys (at least the ones we set)
        let lease_key = make_lease_key(&lease);
        let lease_value = storage.db.get(&lease_key)?.ok_or("lease not found")?;
        let lease_entry = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert!(keys.len() >= 2);

        Ok(())
    }

    fn make_item<'i>(message: &'i mut Builder<HeapAllocator>) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(0); // Items are immediately visible
        item.into_reader()
    }

    fn make_item_with_timeout<'i>(
        message: &'i mut Builder<HeapAllocator>,
        visibility_timeout_secs: u64,
    ) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(visibility_timeout_secs);
        item.into_reader()
    }

    #[test]
    fn poll_filters_invisible_items() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_visibility";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // Add an item that's immediately visible
        let mut message1 = Builder::new_default();
        let item1 = make_item_with_timeout(&mut message1, 0);
        storage.add_available_item((b"visible", item1)).unwrap();

        // Add an item that won't be visible for 10 seconds
        let mut message2 = Builder::new_default();
        let item2 = make_item_with_timeout(&mut message2, 10);
        storage.add_available_item((b"invisible", item2)).unwrap();

        // Poll for items - should only get the visible one
        let (lease, entries) = storage.get_next_available_entries(10)?;
        assert!(!lease.iter().all(|b| *b == 0));
        assert_eq!(entries.len(), 1); // Should only get the visible item

        let si = entries[0].get()?;
        assert_eq!(si.get_id()?, b"visible");
        assert_eq!(si.get_contents()?, b"hello");

        Ok(())
    }

    #[test]
    fn poll_returns_no_items_when_all_invisible() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_all_invisible";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // Add items that won't be visible for 10 seconds
        let mut message1 = Builder::new_default();
        let item1 = make_item_with_timeout(&mut message1, 10);
        storage.add_available_item((b"invisible1", item1)).unwrap();

        let mut message2 = Builder::new_default();
        let item2 = make_item_with_timeout(&mut message2, 10);
        storage.add_available_item((b"invisible2", item2)).unwrap();

        // Poll for items - should get none since all are invisible
        let (lease, entries) = storage.get_next_available_entries(10)?;
        assert!(!lease.iter().all(|b| *b == 0));
        assert_eq!(entries.len(), 0); // Should get no items

        Ok(())
    }

    #[test]
    fn poll_efficiently_scans_only_visible_range() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_efficient_scan";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // Add items with different visibility timeouts to test the range scanning
        let mut message1 = Builder::new_default();
        let item1 = make_item_with_timeout(&mut message1, 0); // Immediately visible
        storage.add_available_item((b"visible_now", item1)).unwrap();

        let mut message2 = Builder::new_default();
        let item2 = make_item_with_timeout(&mut message2, 5); // Visible in 5 seconds
        storage.add_available_item((b"visible_later", item2)).unwrap();

        let mut message3 = Builder::new_default();
        let item3 = make_item_with_timeout(&mut message3, 10); // Visible in 10 seconds
        storage.add_available_item((b"visible_much_later", item3)).unwrap();

        // Poll for items - should only get the immediately visible one
        let (lease, entries) = storage.get_next_available_entries(10)?;
        assert!(!lease.iter().all(|b| *b == 0));
        assert_eq!(entries.len(), 1); // Should only get the visible item

        let si = entries[0].get()?;
        assert_eq!(si.get_id()?, b"visible_now");
        assert_eq!(si.get_contents()?, b"hello");

        // Verify that the other items are still in the available state
        // (not moved to in_progress since they weren't visible)
        let available_key1 = {
            let mut k = Vec::new();
            k.extend_from_slice(b"available/");
            k.extend_from_slice(b"visible_later");
            k
        };
        assert!(storage.db.get(&available_key1)?.is_some());

        let available_key2 = {
            let mut k = Vec::new();
            k.extend_from_slice(b"available/");
            k.extend_from_slice(b"visible_much_later");
            k
        };
        assert!(storage.db.get(&available_key2)?.is_some());

        Ok(())
    }
}
