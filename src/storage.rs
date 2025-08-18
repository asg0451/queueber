use capnp::message::{self, TypedReader};
use capnp::serialize_packed;
use rocksdb::{DB, Options, SliceTransform, WriteBatchWithTransaction};
use std::io::BufReader;
use std::path::Path;
use uuid::Uuid;

use crate::dbkeys::{AvailableKey, InProgressKey, LeaseKey, VisibilityIndexKey};
use crate::errors::{Error, Result};
use crate::protocol;

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
        let main_key = AvailableKey::from_id(id);

        // compute visible-at timestamp and build index key
        let now = std::time::SystemTime::now();
        let visible_ts_secs_le = (now
            + std::time::Duration::from_secs(item.get_visibility_timeout_secs()))
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_le_bytes();

        let visibility_index_key =
            VisibilityIndexKey::from_visible_ts_and_id(visible_ts_secs_le, id);

        // make stored item to insert
        let mut simsg = message::Builder::new_default();
        let mut stored_item = simsg.init_root::<protocol::stored_item::Builder>();
        stored_item.set_contents(item.get_contents()?);
        stored_item.set_id(id);
        stored_item.set_visibility_ts_index_key(visibility_index_key.as_bytes());
        let mut contents = Vec::with_capacity(simsg.size_in_words() * 8); // TODO: reuse
        serialize_packed::write_message(&mut contents, &simsg)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.put(main_key.as_ref(), &contents);
        batch.put(visibility_index_key.as_ref(), main_key.as_ref());
        self.db.write(batch)?;

        tracing::debug!(
            "inserted item: ({}: {}), ({}: {})",
            String::from_utf8_lossy(main_key.as_ref()),
            String::from_utf8_lossy(&contents),
            String::from_utf8_lossy(visibility_index_key.as_ref()),
            String::from_utf8_lossy(main_key.as_ref())
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
        // Iterate over all visibility index entries and pick up to n that
        // are visible as of now. We cannot rely on RocksDB key ordering here
        // because the timestamp is encoded little-endian in the key, which
        // does not preserve lexicographic ordering by time.
        let iter = self.db.prefix_iterator(VisibilityIndexKey::PREFIX);

        // Determine current time (secs since epoch) to filter visible items.
        let now_secs: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        // move these items to in progress and create a lease
        // NOTE: this means either scanning twice or buffering. going with the latter for now, since n is probably small.
        // TODO: can we do better?

        // TODO: RACE CONDITION - concurrent polls can hand out the same messages!
        // The current implementation reads items with a prefix iterator and then moves them to in_progress,
        // but there's no locking between the read and write operations. Multiple concurrent polls could
        // read the same items before any of them complete their write batch, leading to duplicate message
        // delivery. Need to implement proper locking or use atomic operations to prevent this.

        // TODO: use a mockable clock
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = LeaseKey::from_lease_bytes(&lease);

        let mut polled_items = Vec::with_capacity(n);

        let mut in_progress_batch = WriteBatchWithTransaction::<false>::default();

        for kv in iter {
            if polled_items.len() >= n {
                break;
            }

            let (idx_key, main_key) = kv?;

            debug_assert_eq!(
                &idx_key[..VisibilityIndexKey::PREFIX.len()],
                VisibilityIndexKey::PREFIX
            );

            // Parse visible-at timestamp from the index key:
            // Key format: b"visibility_index/" + 8 bytes (LE ts) + b"/" + id
            let ts_start = VisibilityIndexKey::PREFIX.len();
            let ts_end = ts_start + 8;
            if idx_key.len() < ts_end + 1 {
                // Malformed key; skip it defensively
                continue;
            }
            let mut ts_le = [0u8; 8];
            ts_le.copy_from_slice(&idx_key[ts_start..ts_end]);
            let visible_at_secs = u64::from_le_bytes(ts_le);
            // Skip entries that are not yet visible
            if visible_at_secs > now_secs {
                continue;
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
            // The value stored at the visibility index key is the available main key
            // for the item. Ensure it matches the item's id.
            debug_assert_eq!(
                stored_item.get_id()?,
                &main_key[AvailableKey::PREFIX.len()..]
            );

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
            let new_main_key = InProgressKey::from_id(stored_item.get_id()?);
            in_progress_batch.delete(&main_key);
            in_progress_batch.put(new_main_key.as_ref(), &main_value);

            // remove the visibility index entry
            in_progress_batch.delete(&idx_key);

            // Defer lease entry construction; we'll write it after we know
            // the exact number of items collected.
            // TODO: make newtype safety stuff for these
        }

        // Build the lease entry with exactly the number of collected items.
        let mut lease_entry = message::Builder::new_default();
        let lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
        let mut lease_entry_keys = lease_entry_builder.init_keys(polled_items.len() as u32);
        // We need to reconstruct the list of IDs from in-progress entries we just moved.
        // Gather IDs by reading them back from the in-progress keys present in the batch is
        // cumbersome; instead, rebuild by scanning the DB again would be inefficient. To keep
        // things simple, we re-read the keys from the batch state we still have locally: the
        // polled_items vector mirrors the selected items in order, so we will re-open each
        // in-progress entry's ID from polled_items.
        // Note: polled_items readers contain the ID; we can access it again here.
        for (i, typed_item) in polled_items.iter().enumerate() {
            let item_reader: protocol::polled_item::Reader = typed_item.get()?;
            lease_entry_keys.set(i as u32, item_reader.get_id()?);
        }
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8);
        serialize_packed::write_message(&mut lease_entry_bs, &lease_entry)?;
        in_progress_batch.put(lease_key.as_ref(), &lease_entry_bs);

        self.db.write(in_progress_batch)?;

        Ok((lease, polled_items))
    }

    /// Remove an in-progress item if the provided lease currently owns it.
    /// Returns true if an item was removed, false if not found or lease mismatch.
    #[tracing::instrument(skip(self))]
    pub fn remove_in_progress_item(&self, id: &[u8], lease: &Lease) -> Result<bool> {
        // Build keys
        let in_progress_key = InProgressKey::from_id(id);
        let lease_key = LeaseKey::from_lease_bytes(lease);

        // Validate item exists in in_progress
        let in_progress_value_opt = self.db.get(in_progress_key.as_ref())?;
        if in_progress_value_opt.is_none() {
            return Ok(false);
        }

        // Validate lease exists
        let lease_value_opt = self.db.get(lease_key.as_ref())?;
        let Some(lease_value) = lease_value_opt else {
            return Ok(false);
        };

        // Parse lease entry and verify it contains the id
        let lease_msg = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry_reader.get_keys()?;

        // Build filtered keys excluding the provided id using iterator combinators
        let mut found = false;
        let kept_keys: Vec<&[u8]> = keys
            .iter()
            .filter_map(|res| match res {
                Ok(k) if k == id => {
                    found = true;
                    None
                }
                Ok(k) => Some(k),
                Err(_) => None,
            })
            .collect();

        if !found {
            return Ok(false);
        }

        // Prepare batch: delete in_progress item; update or delete lease entry
        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.delete(in_progress_key.as_ref());

        if kept_keys.is_empty() {
            // No items remain under this lease; remove lease entry
            batch.delete(lease_key.as_ref());
        } else {
            // Rebuild lease entry with remaining keys
            let mut msg = message::Builder::new_default();
            let builder = msg.init_root::<protocol::lease_entry::Builder>();
            let mut out_keys = builder.init_keys(kept_keys.len() as u32);
            for (i, k) in kept_keys.into_iter().enumerate() {
                out_keys.set(i as u32, k);
            }
            let mut buf = Vec::with_capacity(msg.size_in_words() * 8);
            serialize_packed::write_message(&mut buf, &msg)?;
            batch.put(lease_key.as_ref(), &buf);
        }

        self.db.write(batch)?;
        Ok(true)
    }
}

// it's a uuidv7
type Lease = [u8; 16];

// helper key builders moved to `crate::dbkeys` newtypes

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol;
    use capnp::message::{Builder, HeapAllocator};
    use uuid::Uuid;

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
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get(lease_key.as_ref())?
            .ok_or("lease not found")?;
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
            let in_progress_key = InProgressKey::from_id(id);
            let value = storage
                .db
                .get(in_progress_key.as_ref())?
                .ok_or("in_progress value missing")?;

            // parse stored item to get original visibility index key
            let msg = serialize_packed::read_message(
                BufReader::new(&value[..]),
                message::ReaderOptions::new(),
            )?;
            let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;

            // available entry must be gone
            let avail_key = AvailableKey::from_id(id);
            assert!(storage.db.get(avail_key.as_ref())?.is_none());

            // visibility index entry must be gone
            let idx_key = stored_item.get_visibility_ts_index_key()?;
            assert!(storage.db.get(idx_key)?.is_none());
        }

        // lease entry exists and has keys (at least the ones we set)
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get(lease_key.as_ref())?
            .ok_or("lease not found")?;
        let lease_entry = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert!(keys.len() >= 2);

        Ok(())
    }

    #[test]
    fn remove_item_with_correct_lease_updates_state()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_db_remove_update";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add two items
        let mut msg1 = Builder::new_default();
        let item1 = make_item(&mut msg1);
        storage.add_available_item((b"id1", item1)).unwrap();
        let mut msg2 = Builder::new_default();
        let item2 = make_item(&mut msg2);
        storage.add_available_item((b"id2", item2)).unwrap();

        // poll both
        let (lease, entries) = storage.get_next_available_entries(2)?;
        assert_eq!(entries.len(), 2);

        // remove id1 under the lease
        let removed = storage.remove_in_progress_item(b"id1", &lease)?;
        assert!(removed);

        // id1 gone from in_progress, id2 remains
        let inprog_id1 = InProgressKey::from_id(b"id1");
        assert!(storage.db.get(inprog_id1.as_ref())?.is_none());

        let inprog_id2 = InProgressKey::from_id(b"id2");
        assert!(storage.db.get(inprog_id2.as_ref())?.is_some());

        // lease entry should contain only id2
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage.db.get(lease_key.as_ref())?.ok_or("lease missing")?;
        let lease_entry = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get(0)?, b"id2");

        Ok(())
    }

    #[test]
    fn remove_last_item_deletes_lease_entry() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_db_remove_last";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add one item and poll
        let mut msg = Builder::new_default();
        let item = make_item(&mut msg);
        storage.add_available_item((b"only", item)).unwrap();
        let (lease, entries) = storage.get_next_available_entries(1)?;
        assert_eq!(entries.len(), 1);

        // remove under lease
        assert!(storage.remove_in_progress_item(b"only", &lease)?);

        // in_progress entry gone
        let inprog = InProgressKey::from_id(b"only");
        assert!(storage.db.get(inprog.as_ref())?.is_none());

        // lease entry deleted
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        assert!(storage.db.get(lease_key.as_ref())?.is_none());

        Ok(())
    }

    #[test]
    fn remove_with_wrong_lease_is_noop() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_db_remove_wrong_lease";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add one item and poll
        let mut msg = Builder::new_default();
        let item = make_item(&mut msg);
        storage.add_available_item((b"only", item)).unwrap();
        let (lease, entries) = storage.get_next_available_entries(1)?;
        assert_eq!(entries.len(), 1);

        // attempt removal with a different lease (non-existent)
        let wrong_lease: [u8; 16] = Uuid::now_v7().into_bytes();
        assert_ne!(lease, wrong_lease);
        let removed = storage.remove_in_progress_item(b"only", &wrong_lease)?;
        assert!(!removed);

        // in_progress still exists
        let inprog = InProgressKey::from_id(b"only");
        assert!(storage.db.get(inprog.as_ref())?.is_some());

        // original lease entry still exists and contains the id
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage.db.get(lease_key.as_ref())?.ok_or("lease missing")?;
        let lease_entry = serialize_packed::read_message(
            BufReader::new(&lease_value[..]),
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get(0)?, b"only");

        Ok(())
    }

    fn make_item<'i>(message: &'i mut Builder<HeapAllocator>) -> protocol::item::Reader<'i> {
        make_item_with_visibility(message, 0)
    }

    fn make_item_with_visibility<'i>(
        message: &'i mut Builder<HeapAllocator>,
        visibility_secs: u64,
    ) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(visibility_secs);
        item.into_reader()
    }

    #[test]
    fn poll_does_not_return_future_items() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let db_path = "/tmp/queueber_test_db_future_visibility";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add one item with future visibility (e.g., 60 seconds)
        let mut msg = Builder::new_default();
        let item = make_item_with_visibility(&mut msg, 60);
        let id = b"future";
        storage.add_available_item((id, item)).unwrap();

        // Poll should return no items yet
        let (_lease, entries) = storage.get_next_available_entries(1)?;
        assert_eq!(entries.len(), 0);

        // The item should still be in available, not in in_progress, and its index should remain
        let avail_key = AvailableKey::from_id(id);
        assert!(storage.db.get(avail_key.as_ref())?.is_some());

        let inprog_key = InProgressKey::from_id(id);
        assert!(storage.db.get(inprog_key.as_ref())?.is_none());

        // Read stored item to get its visibility index key and ensure it still exists
        let value = storage.db.get(avail_key.as_ref())?.ok_or("missing available value")?;
        let msg = serialize_packed::read_message(
            std::io::BufReader::new(&value[..]),
            message::ReaderOptions::new(),
        )?;
        let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;
        let idx_key = stored_item.get_visibility_ts_index_key()?;
        assert!(storage.db.get(idx_key)?.is_some());

        Ok(())
    }
}
