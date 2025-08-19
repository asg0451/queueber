use capnp::message::{self, TypedReader};
use capnp::serialize_packed;
use rocksdb::{Options, SliceTransform, TransactionDB, WriteBatchWithTransaction};
use std::io::BufReader;
use std::path::Path;
use uuid::Uuid;

use crate::dbkeys::{
    AvailableKey, InProgressKey, LeaseExpiryIndexKey, LeaseKey, VisibilityIndexKey,
};
use crate::errors::Result;
use crate::protocol;

pub struct Storage {
    db: TransactionDB,
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
        let txn_opts = rocksdb::TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_opts, path)?;
        Ok(Self { db })
    }

    // id+item -> store stored_item and visibility index entry
    pub fn add_available_item<'i>(
        &self,
        (id, item): (&'i [u8], protocol::item::Reader<'i>),
    ) -> Result<()> {
        self.add_available_item_from_parts(
            id,
            item.get_contents()?,
            item.get_visibility_timeout_secs(),
        )
    }

    pub fn add_available_items<'i>(
        &self,
        items: impl Iterator<Item = (&'i [u8], protocol::item::Reader<'i>)>,
    ) -> Result<()> {
        // Delegate to the single-item owned-part API to avoid code duplication.
        for (id, item) in items {
            self.add_available_item_from_parts(
                id,
                item.get_contents()?,
                item.get_visibility_timeout_secs(),
            )?;
        }
        Ok(())
    }

    // Convenience helpers for feeding from owned parts when capnp Readers are not Send.
    pub fn add_available_item_from_parts(
        &self,
        id: &[u8],
        contents: &[u8],
        visibility_timeout_secs: u64,
    ) -> Result<()> {
        let main_key = AvailableKey::from_id(id);
        let now = std::time::SystemTime::now();
        let visible_ts_secs = (now + std::time::Duration::from_secs(visibility_timeout_secs))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let visibility_index_key = VisibilityIndexKey::from_visible_ts_and_id(visible_ts_secs, id);

        let mut simsg = message::Builder::new_default();
        let mut stored_item = simsg.init_root::<protocol::stored_item::Builder>();
        stored_item.set_contents(contents);
        stored_item.set_id(id);
        stored_item.set_visibility_ts_index_key(visibility_index_key.as_bytes());
        let mut stored_contents = Vec::with_capacity(simsg.size_in_words() * 8);
        serialize_packed::write_message(&mut stored_contents, &simsg)?;

        let mut batch = WriteBatchWithTransaction::<true>::default();
        batch.put(main_key.as_ref(), &stored_contents);
        batch.put(visibility_index_key.as_ref(), main_key.as_ref());
        self.db.write(batch)?;

        tracing::debug!(
            "inserted item (from parts): ({}: {}), ({}: {})",
            String::from_utf8_lossy(main_key.as_ref()),
            String::from_utf8_lossy(&stored_contents),
            String::from_utf8_lossy(visibility_index_key.as_ref()),
            String::from_utf8_lossy(main_key.as_ref())
        );

        Ok(())
    }

    pub fn add_available_items_from_parts<'a, I>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], (&'a [u8], u64))>,
    {
        for (id, (contents, vis)) in items.into_iter() {
            self.add_available_item_from_parts(id, contents, vis)?;
        }
        Ok(())
    }

    /// Return the next visibility timestamp (secs since epoch) among items in the
    /// visibility index, if any. This is determined by reading the first key in
    /// the `visibility_index/` namespace which is ordered by big-endian timestamp.
    pub fn peek_next_visibility_ts_secs(&self) -> Result<Option<u64>> {
        let mut iter = self.db.prefix_iterator(VisibilityIndexKey::PREFIX);
        if let Some(kv) = iter.next() {
            let (idx_key, _val) = kv?;
            debug_assert_eq!(
                &idx_key[..VisibilityIndexKey::PREFIX.len()],
                VisibilityIndexKey::PREFIX
            );
            Ok(VisibilityIndexKey::parse_visible_ts_secs(&idx_key))
        } else {
            Ok(None)
        }
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
        // default lease validity of 30 seconds for callers that don't provide it
        self.get_next_available_entries_with_lease(n, 30)
    }

    #[allow(clippy::type_complexity)]
    pub fn get_next_available_entries_with_lease(
        &self,
        n: usize,
        lease_validity_secs: u64,
    ) -> Result<(
        Lease,
        Vec<
            TypedReader<
                capnp::message::Builder<message::HeapAllocator>,
                protocol::polled_item::Owned,
            >,
        >,
    )> {
        // Determine current time (secs since epoch) to filter visible items.
        let now = std::time::SystemTime::now();
        let now_secs: u64 = now.duration_since(std::time::UNIX_EPOCH)?.as_secs();

        // move these items to in progress and create a lease
        // TODO: use a mockable clock
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let expiry_ts_secs = (now + std::time::Duration::from_secs(lease_validity_secs))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let lease_expiry_index_key =
            LeaseExpiryIndexKey::from_expiry_ts_and_lease(expiry_ts_secs, &lease);

        // Iterate over visibility index entries in lexicographic order; since
        // timestamps are encoded big-endian in the key, this corresponds to
        // ascending time order.
        let iter = self.db.prefix_iterator(VisibilityIndexKey::PREFIX);

        let mut polled_items = Vec::with_capacity(n);

        for kv in iter {
            if polled_items.len() >= n {
                break;
            }

            let (idx_key, main_key) = kv?;

            debug_assert_eq!(
                &idx_key[..VisibilityIndexKey::PREFIX.len()],
                VisibilityIndexKey::PREFIX
            );

            // Parse visible-at timestamp using helper; because keys are time-ordered (BE),
            // we can stop scanning as soon as we hit a future timestamp.
            let Some(visible_at_secs) = VisibilityIndexKey::parse_visible_ts_secs(&idx_key) else {
                continue;
            };
            if visible_at_secs > now_secs {
                break;
            }

            // Try to claim this item atomically using a transaction
            let txn = self.db.transaction();
            // Lock both the index entry and the main key to avoid concurrent polls claiming it
            let idx_value_opt = txn.get_for_update(&idx_key, true)?;
            let main_value_opt = txn.get_for_update(&main_key, true)?;
            let Some(main_value) = main_value_opt else {
                // Check if this is a database integrity issue or just normal concurrency
                // If another poller already claimed and removed the available entry (and possibly
                // the index), we should skip this entry rather than failing. This can also happen
                // if the item was acknowledged quickly under its lease.
                let id_suffix = AvailableKey::id_suffix_from_key_bytes(&main_key);
                let in_progress_key = InProgressKey::from_id(id_suffix);
                if txn.get(&in_progress_key)?.is_some() {
                    // Item was already claimed by another poll, skip it
                    continue;
                } else {
                    // If the visibility index entry itself no longer exists, this is a benign race
                    // (we iterated a stale view). Skip it.
                    if idx_value_opt.is_none() {
                        continue;
                    }

                    // Otherwise, the index points at a non-existent main key and there is no
                    // in-progress entry. Treat this as a stale/orphaned index entry and self-heal
                    // by deleting it within the same transaction that holds the lock to avoid
                    // deadlocks/timeouts, then commit.
                    txn.delete(&idx_key)?;
                    txn.commit()?;
                    continue;
                }
            };

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
                AvailableKey::id_suffix_from_key_bytes(&main_key)
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
            // Apply moves inside the transaction to guarantee exclusivity
            txn.delete(&main_key)?;
            txn.put(new_main_key.as_ref(), &main_value)?;
            txn.delete(&idx_key)?;
            txn.commit()?;
        }

        // Build the lease entry by re-reading the IDs from the polled_items
        // vector. capnp lists aren't dynamically sized so we need to know how
        // many to init before we start writing (?)
        let mut lease_entry = message::Builder::new_default();
        let lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
        let mut lease_entry_keys = lease_entry_builder.init_keys(polled_items.len() as u32);
        for (i, typed_item) in polled_items.iter().enumerate() {
            let item_reader: protocol::polled_item::Reader = typed_item.get()?;
            lease_entry_keys.set(i as u32, item_reader.get_id()?);
        }
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8);
        serialize_packed::write_message(&mut lease_entry_bs, &lease_entry)?;

        if !polled_items.is_empty() {
            // Write the lease entry only if we actually polled items
            let mut in_progress_batch = WriteBatchWithTransaction::<true>::default();
            in_progress_batch.put(lease_key.as_ref(), &lease_entry_bs);
            // index the lease by expiry time for the background sweeper
            in_progress_batch.put(lease_expiry_index_key.as_ref(), lease_key.as_ref());
            self.db.write(in_progress_batch)?;
        }

        Ok((lease, polled_items))
    }

    /// Remove an in-progress item if the provided lease currently owns it.
    /// Returns true if an item was removed, false if not found or lease mismatch.
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
        let mut batch = WriteBatchWithTransaction::<true>::default();
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

    /// Sweep expired leases: for each expired lease, move any remaining
    /// in-progress items back to available and delete the lease and its index.
    /// Returns the number of expired leases processed.
    pub fn expire_due_leases(&self) -> Result<usize> {
        let iter = self.db.prefix_iterator(LeaseExpiryIndexKey::PREFIX);

        let now_secs: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let mut processed = 0usize;

        for kv in iter {
            let (idx_key, _lease_key_bytes_val) = kv?;

            debug_assert_eq!(
                &idx_key[..LeaseExpiryIndexKey::PREFIX.len()],
                LeaseExpiryIndexKey::PREFIX
            );

            let Some((expiry_ts_secs, lease_bytes)) =
                LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)
            else {
                continue;
            };
            if expiry_ts_secs > now_secs {
                break;
            }

            let lease_key = LeaseKey::from_lease_bytes(lease_bytes);

            // Load the lease entry; if missing, just delete the expiry index
            let lease_value_opt = self.db.get(lease_key.as_ref())?;
            if lease_value_opt.is_none() {
                let mut batch = WriteBatchWithTransaction::<true>::default();
                batch.delete(&idx_key);
                self.db.write(batch)?;
                processed += 1;
                continue;
            }
            let lease_value = lease_value_opt.unwrap();

            // Parse lease entry keys
            let lease_msg = serialize_packed::read_message(
                BufReader::new(&lease_value[..]),
                message::ReaderOptions::new(),
            )?;
            let lease_entry_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let keys = lease_entry_reader.get_keys()?;

            let mut batch = WriteBatchWithTransaction::<true>::default();

            // For each id, if it's still in in_progress, move back to available and
            // restore the visibility index entry with visibility at now
            for res in keys.iter() {
                let Ok(id) = res else { continue };
                let in_progress_key = InProgressKey::from_id(id);
                if let Some(value) = self.db.get(in_progress_key.as_ref())? {
                    // Parse stored item to get id and contents
                    let msg = serialize_packed::read_message(
                        BufReader::new(&value[..]),
                        message::ReaderOptions::new(),
                    )?;
                    let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;

                    // Build a new stored_item with visibility index for now
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs();
                    let vis_idx_now =
                        VisibilityIndexKey::from_visible_ts_and_id(now_secs, stored_item.get_id()?);

                    let mut out_msg = message::Builder::new_default();
                    let mut out_item = out_msg.init_root::<protocol::stored_item::Builder>();
                    out_item.set_contents(stored_item.get_contents()?);
                    out_item.set_id(stored_item.get_id()?);
                    out_item.set_visibility_ts_index_key(vis_idx_now.as_bytes());
                    let mut out_buf = Vec::with_capacity(out_msg.size_in_words() * 8);
                    serialize_packed::write_message(&mut out_buf, &out_msg)?;

                    let avail_key = AvailableKey::from_id(id);
                    batch.put(avail_key.as_ref(), &out_buf);
                    batch.put(vis_idx_now.as_ref(), avail_key.as_ref());
                    batch.delete(in_progress_key.as_ref());
                }
            }

            // Remove the lease entry and its expiry index
            batch.delete(lease_key.as_ref());
            batch.delete(&idx_key);

            self.db.write(batch)?;
            processed += 1;
        }

        Ok(processed)
    }
}

// it's a uuidv7
type Lease = [u8; 16];

// helper key builders moved to `crate::dbkeys` newtypes

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::protocol;
    use capnp::message::{Builder, HeapAllocator};
    use uuid::Uuid;

    #[test]
    fn e2e_test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
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
            .with_max_level(tracing::Level::INFO)
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
            .with_max_level(tracing::Level::INFO)
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
            .with_max_level(tracing::Level::INFO)
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
            .with_max_level(tracing::Level::INFO)
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
            .with_max_level(tracing::Level::INFO)
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
        let value = storage
            .db
            .get(avail_key.as_ref())?
            .ok_or("missing available value")?;
        let msg = serialize_packed::read_message(
            std::io::BufReader::new(&value[..]),
            message::ReaderOptions::new(),
        )?;
        let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;
        let idx_key = stored_item.get_visibility_ts_index_key()?;
        assert!(storage.db.get(idx_key)?.is_some());

        Ok(())
    }

    #[test]
    fn concurrent_adds_and_poll_integration() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_env_filter("concurrency=debug")
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(Storage::new(tmp.path()).expect("storage"));

        let writers = 4;
        let per_writer = 16;
        let mut handles = Vec::new();
        for w in 0..writers {
            tracing::debug!("starting writer {w}");
            let st = Arc::clone(&storage);
            handles.push(std::thread::spawn(move || -> Result<()> {
                for i in 0..per_writer {
                    let id = format!("w{w}-i{i}");
                    st.add_available_item_from_parts(id.as_bytes(), b"payload", 0)?;
                    tracing::debug!("writer {w} added item {id}");
                }
                Ok(())
            }));
        }
        tracing::debug!("waiting for writers to finish");
        for h in handles {
            h.join().expect("thread join").expect("writer result");
        }

        tracing::debug!("getting next available entries");
        let total = (writers * per_writer) as usize;
        let (_lease, items) = storage.get_next_available_entries(total)?;
        assert_eq!(items.len(), total);

        Ok(())
    }

    #[test]
    fn expired_leases_requeue_immediately() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        storage.add_available_item_from_parts(b"e1", b"p", 0)?;
        let (_lease, items) = storage.get_next_available_entries_with_lease(1, 1)?;
        assert_eq!(items.len(), 1);

        std::thread::sleep(std::time::Duration::from_millis(1100));
        let n = storage.expire_due_leases()?;
        assert!(n >= 1);

        let (_lease2, items2) = storage.get_next_available_entries(1)?;
        assert_eq!(items2.len(), 1);
        Ok(())
    }

    #[test]
    fn stale_visibility_index_entry_is_healed() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        // Create a visibility index entry pointing at a non-existent main key
        let ghost_id = b"ghost";
        let idx_key = VisibilityIndexKey::from_visible_ts_and_id(0, ghost_id);
        let main_key = AvailableKey::from_id(ghost_id);
        let mut batch = WriteBatchWithTransaction::<true>::default();
        batch.put(idx_key.as_ref(), main_key.as_ref());
        storage.db.write(batch)?;

        // Poll should self-heal by deleting the orphaned index and not panic
        let (_lease, items) = storage.get_next_available_entries(1)?;
        assert_eq!(items.len(), 0);
        // The index entry should be gone after healing
        assert!(storage.db.get(idx_key.as_ref())?.is_none());
        Ok(())
    }

    #[test]
    fn concurrent_polls_do_not_get_overlapping_messages() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(Storage::new(tmp.path()).expect("storage"));

        // Add 10 items that are immediately visible
        for i in 0..10 {
            let id = format!("item-{}", i);
            storage.add_available_item_from_parts(id.as_bytes(), b"payload", 0)?;
        }

        // Spawn 3 concurrent polls, each trying to get 4 items
        let mut handles = Vec::new();
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));

        for poller_id in 0..3 {
            let st = Arc::clone(&storage);
            let results = Arc::clone(&results);
            handles.push(std::thread::spawn(move || -> Result<()> {
                let (_lease, items) = st.get_next_available_entries(4)?;
                let mut item_ids = Vec::new();
                for item in items {
                    let item_reader = item.get()?;
                    item_ids.push(item_reader.get_id()?.to_vec());
                }

                let mut results = results.lock().unwrap();
                results.push((poller_id, item_ids));
                Ok(())
            }));
        }

        // Wait for all polls to complete
        for h in handles {
            h.join().expect("thread join").expect("poller result");
        }

        // Check that no item appears in multiple polls
        let results = results.lock().unwrap();
        let mut all_item_ids = Vec::new();
        for (poller_id, item_ids) in results.iter() {
            println!(
                "Poller {} got items: {:?}",
                poller_id,
                item_ids
                    .iter()
                    .map(|id| String::from_utf8_lossy(id))
                    .collect::<Vec<_>>()
            );
            all_item_ids.extend(item_ids.clone());
        }

        // Check for duplicates
        let mut sorted_ids = all_item_ids.clone();
        sorted_ids.sort();
        sorted_ids.dedup();

        if sorted_ids.len() != all_item_ids.len() {
            panic!(
                "Found duplicate items across concurrent polls! Total items: {}, Unique items: {}",
                all_item_ids.len(),
                sorted_ids.len()
            );
        }

        // Verify we got exactly 10 items total (some polls might get fewer than 4 if others got them first)
        assert_eq!(
            all_item_ids.len(),
            10,
            "Expected exactly 10 items total across all polls"
        );

        Ok(())
    }
}
