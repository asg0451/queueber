use capnp::message::{self, TypedReader};
use capnp::serialize;
use rocksdb::{OptimisticTransactionDB, Options, SliceTransform, WriteBatchWithTransaction};
use std::path::Path;
use uuid::Uuid;

use crate::dbkeys::{
    AvailableKey, InProgressKey, LeaseExpiryIndexKey, LeaseKey, VisibilityIndexKey,
};
use crate::errors::{Error, Result};
use crate::protocol;

pub struct Storage {
    db: OptimisticTransactionDB,
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
        let db = OptimisticTransactionDB::open(&opts, path)?;
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
        // Build keys and contents
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
        serialize::write_message(&mut stored_contents, &simsg)?;

        // Atomically insert the item and visibility index entry

        let mut batch = WriteBatchWithTransaction::<true>::default();
        batch.put(main_key.as_ref(), &stored_contents);
        batch.put(visibility_index_key.as_ref(), main_key.as_ref());
        self.db.write(batch)?;

        tracing::debug!(
            "inserted item (from parts): ({}: <contents len: {}>), (viz/{}: avail/{})",
            Uuid::from_slice(id).unwrap_or_default(),
            contents.len(),
            Uuid::from_slice(
                VisibilityIndexKey::split_ts_and_id(visibility_index_key.as_ref())
                    .unwrap()
                    .1
            )
            .unwrap_or_default(),
            Uuid::from_slice(AvailableKey::id_suffix_from_key_bytes(main_key.as_ref()))
                .unwrap_or_default()
        );

        Ok(())
    }

    // TODO: swap which one wraps which
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
            let (idx_key, _) = kv?;
            Ok(Some(VisibilityIndexKey::parse_visible_ts_secs(&idx_key)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_next_available_entries(
        &self,
        n: usize,
    ) -> Result<(Lease, Vec<PolledItemOwnedReader>)> {
        // default lease validity of 30 seconds for callers that don't provide it
        self.get_next_available_entries_with_lease(n, 30)
    }

    pub fn get_next_available_entries_with_lease(
        &self,
        n: usize,
        lease_validity_secs: u64,
    ) -> Result<(Lease, Vec<PolledItemOwnedReader>)> {
        // Create a lease and its expiry index entry
        let now = std::time::SystemTime::now();
        let now_secs: u64 = now.duration_since(std::time::UNIX_EPOCH)?.as_secs();
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let expiry_ts_secs = (now + std::time::Duration::from_secs(lease_validity_secs))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let lease_expiry_index_key =
            LeaseExpiryIndexKey::from_expiry_ts_and_lease(expiry_ts_secs, &lease);

        // Find the next n items that are available and visible.
        // Use a transaction for consistency, though I don't like paying the cost for it.
        let txn = self.db.transaction();
        let viz_iter = txn.prefix_iterator(VisibilityIndexKey::PREFIX);

        let mut polled_items = Vec::with_capacity(n);

        for kv in viz_iter {
            if polled_items.len() >= n {
                break;
            }

            let (idx_key, main_key) = kv?;

            let visible_at_secs = VisibilityIndexKey::parse_visible_ts_secs(&idx_key)?;
            if visible_at_secs > now_secs {
                // Because keys are ordered by timestamp, we can stop scanning.
                break;
            }

            tracing::debug!(
                "got visibility index entry: (viz/{}: avail/{})",
                Uuid::from_slice(VisibilityIndexKey::split_ts_and_id(&idx_key).unwrap().1)
                    .unwrap_or_default(),
                Uuid::from_slice(AvailableKey::id_suffix_from_key_bytes(&main_key))
                    .unwrap_or_default()
            );

            // Attempt to lock the index entry so we know it's ours.
            if txn.get_pinned_for_update(&idx_key, true)?.is_none() {
                // We lost the race on this one, try another.
                continue;
            }

            // Fetch and decode the item.
            let main_value = txn.get_pinned_for_update(&main_key, true)?.ok_or_else(|| {
                Error::assertion_failed(&format!("main key not found: {:?}", main_key.as_ref()))
            })?;
            let stored_item_message = serialize::read_message_from_flat_slice(
                &mut &main_value[..],
                message::ReaderOptions::new(),
            )?;
            let stored_item = stored_item_message.get_root::<protocol::stored_item::Reader>()?;

            debug_assert!(!stored_item.get_id()?.is_empty());
            debug_assert!(!stored_item.get_contents()?.is_empty());
            debug_assert!(!stored_item.get_visibility_ts_index_key()?.is_empty());
            debug_assert_eq!(
                stored_item.get_id()?,
                AvailableKey::id_suffix_from_key_bytes(&main_key)
            );

            tracing::debug!(
                "got stored item: (id: {}, contents: <contents len: {}>)",
                Uuid::from_slice(stored_item.get_id()?).unwrap_or_default(),
                stored_item.get_contents()?.len()
            );

            // Build the polled item.
            let mut builder = message::Builder::new_default(); // TODO: reduce allocs
            let mut polled_item = builder.init_root::<protocol::polled_item::Builder>();
            polled_item.set_contents(stored_item.get_contents()?);
            polled_item.set_id(stored_item.get_id()?);
            let polled_item = builder.into_typed().into_reader();
            polled_items.push(polled_item);

            // Move the item to in progress and delete the index entry.
            let new_main_key = InProgressKey::from_id(stored_item.get_id()?);
            txn.delete(&main_key)?;
            txn.put(new_main_key.as_ref(), &main_value)?;
            txn.delete(&idx_key)?;
        }

        // If no items were found, return a nil lease and empty polled items. TODO: is this to golangy (:
        if polled_items.is_empty() {
            return Ok((Uuid::nil().into_bytes(), Vec::new()));
        }

        // Build the lease entry.
        let lease_entry = build_lease_entry_message(lease_validity_secs, &polled_items)?;
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8); // TODO: avoid allocation
        serialize::write_message(&mut lease_entry_bs, &lease_entry)?;

        // Write the lease entry and its expiry index
        txn.put(lease_key.as_ref(), &lease_entry_bs)?;
        txn.put(lease_expiry_index_key.as_ref(), lease_key.as_ref())?;

        txn.commit()?;

        tracing::debug!(
            "handed out lease: {:?} with {} items: {:?}",
            Uuid::from_bytes(lease),
            polled_items.len(),
            polled_items
                .iter()
                .map(|i| Uuid::from_slice(i.get().unwrap().get_id().unwrap()).unwrap_or_default())
                .collect::<Vec<_>>()
        );

        Ok((lease, polled_items))
    }

    /// Remove an in-progress item if the provided lease currently owns it.
    /// Returns true if an item was removed, false if not found or lease mismatch.
    pub fn remove_in_progress_item(&self, id: &[u8], lease: &Lease) -> Result<bool> {
        // Build keys
        let in_progress_key = InProgressKey::from_id(id);
        let lease_key = LeaseKey::from_lease_bytes(lease);

        let txn = self.db.transaction();
        // Validate item exists in in_progress
        if txn.get_pinned_for_update(&in_progress_key, true)?.is_none() {
            return Ok(false);
        }

        // Validate lease exists
        let Some(lease_value) = txn.get_pinned_for_update(&lease_key, true)? else {
            tracing::info!("lease entry not found: {:?}", Uuid::from_bytes(*lease));
            return Ok(false);
        };

        // Parse lease entry and verify it contains the id.
        // TODO: make the keys inside sorted so we can binary search for the id.
        let lease_msg = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry_reader.get_keys()?;
        let Some((found_idx, _)) = keys
            .iter()
            .enumerate()
            .find(|(_, k)| k.as_ref().ok() == Some(&id))
        else {
            return Ok(false);
        };

        // Delete item from in_progress.
        txn.delete(in_progress_key.as_ref())?;

        // Rewrite the lease entry to exclude the id. If no items remain under this lease,
        // delete the lease entry instead.
        if keys.len() - 1 == 0 {
            txn.delete(lease_key.as_ref())?;
        } else {
            // Rebuild lease entry with remaining keys.
            // TODO: this could be done more efficiently by unifying the above search and this one.
            let mut msg = message::Builder::new_default(); // TODO: reduce allocs
            let builder = msg.init_root::<protocol::lease_entry::Builder>();
            let mut out_keys = builder.init_keys(keys.len() as u32 - 1);
            let mut new_idx = 0;
            #[allow(clippy::explicit_counter_loop)] // TODO: clean this up
            for (_, k) in keys.iter().enumerate().filter(|(i, _)| *i != found_idx) {
                let k = k?;
                out_keys.set(new_idx as u32, k);
                new_idx += 1;
            }
            let mut buf = Vec::with_capacity(msg.size_in_words() * 8); // TODO: reduce allocs
            serialize::write_message(&mut buf, &msg)?;
            txn.put(lease_key.as_ref(), &buf)?;
        }
        drop(lease_value);

        txn.commit()?;

        Ok(true)
    }

    /// Sweep expired leases: for each expired lease, move any remaining
    /// in-progress items back to available and delete the lease and its index.
    /// Returns the number of expired leases processed.
    pub fn expire_due_leases(&self) -> Result<usize> {
        let now_secs: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let mut processed = 0usize;

        let txn = self.db.transaction();
        let iter = txn.prefix_iterator(LeaseExpiryIndexKey::PREFIX);
        for kv in iter {
            let (idx_key, _lease_key_bytes_val) = kv?;

            debug_assert_eq!(
                &idx_key[..LeaseExpiryIndexKey::PREFIX.len()],
                LeaseExpiryIndexKey::PREFIX
            );

            let _idx_val = txn.get_pinned_for_update(&idx_key, true)?.ok_or_else(|| {
                Error::assertion_failed("visibility index entry not found after we scanned it. expiry should be the only one deleting leases (once expiry is single flighted... TODO)")
            })?;

            let (expiry_ts_secs, lease_bytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;

            if expiry_ts_secs > now_secs {
                // Because keys are ordered by timestamp, we can stop scanning
                break;
            }

            let lease_key = LeaseKey::from_lease_bytes(lease_bytes);

            // Load the lease entry.
            let lease_value = txn
                .get_pinned_for_update(lease_key.as_ref(), true)?
                .ok_or_else(|| {
                    Error::assertion_failed(&format!(
                        "lease entry {:?} not found for expiry index {:?}",
                        lease_key, idx_key
                    ))
                })?;

            // TODO: ensure extend doesnt create multiple index entries for the same lease.

            // Parse lease entry keys
            let lease_msg = serialize::read_message_from_flat_slice(
                &mut &lease_value[..],
                message::ReaderOptions::new(),
            )?;
            let lease_entry_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let keys = lease_entry_reader.get_keys()?; // TODO: rename to ids

            // Move each item back to available.
            for id in keys.iter() {
                let id = id?;
                let in_progress_key = InProgressKey::from_id(id);
                let item_value = txn
                    .get_pinned_for_update(in_progress_key.as_ref(), true)?
                    .ok_or_else(|| {
                        Error::assertion_failed(&format!(
                            "in_progress entry {:?} not found for lease entry {:?}",
                            in_progress_key, lease_key
                        ))
                    })?;

                // Write the item back to available, and add a visibility index entry.
                let avail_key = AvailableKey::from_id(id);
                txn.put(avail_key.as_ref(), &item_value)?;
                let vis_idx_now = VisibilityIndexKey::from_visible_ts_and_id(now_secs, id);
                txn.put(vis_idx_now.as_ref(), avail_key.as_ref())?;
                txn.delete(in_progress_key.as_ref())?;
            }

            // Remove the lease entry and its expiry index
            txn.delete(lease_key.as_ref())?;
            txn.delete(&idx_key)?;
            processed += 1;
        }
        txn.commit()?;
        Ok(processed)
    }

    /// Extend an existing lease's validity by resetting its expiry to now + lease_validity_secs.
    /// Returns false if the lease does not exist.
    pub fn extend_lease(&self, lease: &Lease, lease_validity_secs: u64) -> Result<bool> {
        let txn = self.db.transaction();
        // Validate lease exists; if not, do nothing
        let lease_key = LeaseKey::from_lease_bytes(lease);
        if txn
            .get_pinned_for_update(lease_key.as_ref(), true)?
            .is_none()
        {
            return Ok(false);
        }

        // Compute and write the new expiry index key.
        let now = std::time::SystemTime::now();
        let expiry_ts_secs = (now + std::time::Duration::from_secs(lease_validity_secs))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let new_idx_key = LeaseExpiryIndexKey::from_expiry_ts_and_lease(expiry_ts_secs, lease);
        txn.put(new_idx_key.as_ref(), lease_key.as_ref())?;

        // Find current expiry index entry for this lease and delete it.
        // TODO: add this index entry to the lease entry so we can do a point lookup.
        let mut existing_idx_key: Option<Vec<u8>> = None;
        for kv in txn.prefix_iterator(LeaseExpiryIndexKey::PREFIX) {
            let (idx_key, _val) = kv?;
            let (_ts, lbytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;
            if lbytes == lease {
                existing_idx_key = Some(idx_key.to_vec());
                break;
            }
        }
        if let Some(k) = existing_idx_key {
            txn.delete(&k)?;
        }

        // Update the lease entry's expiryTsSecs while preserving keys
        if let Some(lease_value) = txn.get_pinned_for_update(lease_key.as_ref(), true)? {
            let lease_msg = serialize::read_message_from_flat_slice(
                &mut &lease_value[..],
                message::ReaderOptions::new(),
            )?;
            let lease_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let keys = lease_reader.get_keys()?;

            // TODO: do this with set_root or some such / more efficiently.
            let mut out = message::Builder::new_default();
            let mut builder = out.init_root::<protocol::lease_entry::Builder>();
            builder.set_expiry_ts_secs(expiry_ts_secs);
            let mut out_keys = builder.reborrow().init_keys(keys.len());
            for i in 0..keys.len() {
                out_keys.set(i, keys.get(i)?);
            }
            let mut buf = Vec::with_capacity(out.size_in_words() * 8);
            serialize::write_message(&mut buf, &out)?;
            txn.put(lease_key.as_ref(), &buf)?;
        }
        Ok(true)
    }
}

/// uuidv7 bytes
type Lease = [u8; 16];

type PolledItemOwnedReader =
    TypedReader<capnp::message::Builder<message::HeapAllocator>, protocol::polled_item::Owned>;

fn build_lease_entry_message(
    lease_validity_secs: u64,
    polled_items: &[PolledItemOwnedReader],
) -> Result<capnp::message::Builder<message::HeapAllocator>> {
    // Build the lease entry. capnp lists aren't dynamically sized so we
    // need to know how many to init before we start writing (?).
    let mut lease_entry = message::Builder::new_default();
    let mut lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
    lease_entry_builder.set_expiry_ts_secs(lease_validity_secs);
    let mut lease_entry_keys = lease_entry_builder.init_keys(polled_items.len() as u32);
    for (i, typed_item) in polled_items.iter().enumerate() {
        let item_reader: protocol::polled_item::Reader = typed_item.get()?;
        lease_entry_keys.set(i as u32, item_reader.get_id()?);
    }
    Ok(lease_entry)
}

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
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
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
            let msg = serialize::read_message_from_flat_slice(
                &mut &value[..],
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
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
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
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
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
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_keys()?;
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get(0)?, b"only");

        Ok(())
    }

    #[test]
    fn extend_existing_lease_updates_expiry_index() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let db_path = "/tmp/queueber_test_db_extend";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // Add and poll to create a lease with 1s expiry
        storage.add_available_item_from_parts(Uuid::now_v7().as_bytes(), b"p", 0)?;
        let (lease, items) = storage.get_next_available_entries_with_lease(1, 1)?;
        assert_eq!(items.len(), 1);

        // Extend the lease by 3s; should return true
        let ok = storage.extend_lease(&lease, 3)?;
        assert!(ok);

        // Sleep slightly over 1s and ensure lease hasn't expired yet
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let n = storage.expire_due_leases()?;
        assert_eq!(n, 0, "lease should not expire after extend");

        Ok(())
    }

    #[test]
    fn extend_unknown_lease_returns_false() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let db_path = "/tmp/queueber_test_db_extend_unknown";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        let lease: [u8; 16] = uuid::Uuid::now_v7().into_bytes();
        let ok = storage.extend_lease(&lease, 1)?;
        assert!(!ok);
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
        let msg = serialize::read_message_from_flat_slice(
            &mut &value[..],
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
    fn concurrent_polls_do_not_get_overlapping_messages() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_thread_ids(true)
            .with_max_level(tracing::Level::INFO)
            .with_env_filter(std::env::var("RUST_LOG").unwrap_or_default())
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(Storage::new(tmp.path()).expect("storage"));

        // Add 10 items that are immediately visible
        for _ in 0..10 {
            let id = Uuid::now_v7().as_bytes().to_vec();
            storage.add_available_item_from_parts(&id, b"payload", 0)?;
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
