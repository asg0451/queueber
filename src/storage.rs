use capnp::message::{self, TypedReader};
use capnp::serialize;
use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, OptimisticTransactionDB,
    OptimisticTransactionOptions, Options, ReadOptions, SliceTransform, WriteBatchWithTransaction,
    WriteOptions,
};
use std::path::Path;
use uuid::Uuid;

use crate::dbkeys::{
    AvailableKey, InProgressKey, LeaseExpiryIndexKey, LeaseKey, VisibilityIndexKey,
};
use crate::errors::{Error, Result};
use crate::protocol;
use rand::Rng;

#[inline]
fn binary_search_by_index<'a, F>(len: u32, mut get_at: F, needle: &[u8]) -> Option<usize>
where
    F: FnMut(u32) -> Option<&'a [u8]>,
{
    if len == 0 {
        return None;
    }
    let mut lo: i32 = 0;
    let mut hi: i32 = (len as i32) - 1;
    while lo <= hi {
        let mid = lo + ((hi - lo) / 2);
        let mid_id = get_at(mid as u32)?;
        match mid_id.cmp(needle) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid - 1,
            std::cmp::Ordering::Equal => return Some(mid as usize),
        }
    }
    None
}

#[inline]
#[allow(unused_variables, unused_mut)] // because in non debug mode it complains otherwise
fn debug_assert_sorted_by_index<'a, F>(len: u32, mut get_at: F)
where
    F: FnMut(u32) -> Option<&'a [u8]>,
{
    #[cfg(debug_assertions)]
    {
        if len == 0 {
            return;
        }
        let mut prev = get_at(0);
        let mut i = 1u32;
        while i < len {
            let cur = get_at(i);
            if let (Some(p), Some(c)) = (prev, cur) {
                debug_assert!(p <= c, "ids not sorted at {}", i);
            }
            prev = cur;
            i += 1;
        }
    }
}

pub struct Storage {
    db: OptimisticTransactionDB,
}

impl Storage {
    pub fn new(path: &Path) -> Result<Self> {
        // DB-level options
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Prefix extractor: treat the namespace segment up to and including the first '/'
        // as the prefix across our keyspaces.
        let make_ns_prefix = || {
            SliceTransform::create(
                "ns_prefix",
                |key: &[u8]| match key.iter().position(|b| *b == b'/') {
                    Some(idx) => &key[..=idx],
                    None => key,
                },
                Some(|_key: &[u8]| true),
            )
        };

        // Build per-CF options. Simplicity: use the same prefix extractor for all CFs.
        let mut cf_default = Options::default();
        cf_default.set_prefix_extractor(make_ns_prefix());
        let mut cf_available = Options::default();
        cf_available.set_prefix_extractor(make_ns_prefix());
        let mut cf_in_progress = Options::default();
        cf_in_progress.set_prefix_extractor(make_ns_prefix());
        let mut cf_visibility_index = Options::default();
        cf_visibility_index.set_prefix_extractor(make_ns_prefix());
        let mut cf_leases = Options::default();
        cf_leases.set_prefix_extractor(make_ns_prefix());
        let mut cf_lease_expiry = Options::default();
        cf_lease_expiry.set_prefix_extractor(make_ns_prefix());

        let cf_descs = vec![
            ColumnFamilyDescriptor::new("default", cf_default),
            ColumnFamilyDescriptor::new("available", cf_available),
            ColumnFamilyDescriptor::new("in_progress", cf_in_progress),
            ColumnFamilyDescriptor::new("visibility_index", cf_visibility_index),
            ColumnFamilyDescriptor::new("leases", cf_leases),
            ColumnFamilyDescriptor::new("lease_expiry", cf_lease_expiry),
        ];

        let db = OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, cf_descs)?;
        Ok(Self { db })
    }

    #[inline]
    fn cf_available(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("available")
            .expect("available CF must exist")
    }

    #[inline]
    fn cf_in_progress(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("in_progress")
            .expect("in_progress CF must exist")
    }

    #[inline]
    fn cf_visibility_index(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("visibility_index")
            .expect("visibility_index CF must exist")
    }

    #[inline]
    fn cf_leases(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle("leases").expect("leases CF must exist")
    }

    #[inline]
    fn cf_lease_expiry(&self) -> &rocksdb::ColumnFamily {
        self.db
            .cf_handle("lease_expiry")
            .expect("lease_expiry CF must exist")
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

    // Convenience helpers for feeding from owned parts when capnp Readers are not Send.
    pub fn add_available_item_from_parts(
        &self,
        id: &[u8],
        contents: &[u8],
        visibility_timeout_secs: u64,
    ) -> Result<()> {
        let iter = std::iter::once((id, (contents, visibility_timeout_secs)));
        self.add_available_items_from_parts(iter)
    }

    pub fn add_available_items_from_parts<'a, I>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], (&'a [u8], u64))>,
    {
        let mut batch = WriteBatchWithTransaction::<true>::default();
        // Reuse a byte buffer for capnp serialization across items to avoid repeated allocations.
        let mut stored_contents: Vec<u8> = Vec::new();
        for (id, (contents, visibility_timeout_secs)) in items.into_iter() {
            let main_key = AvailableKey::from_id(id);
            let now = std::time::SystemTime::now();
            let visible_ts_secs = (now + std::time::Duration::from_secs(visibility_timeout_secs))
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();
            let visibility_index_key =
                VisibilityIndexKey::from_visible_ts_and_id(visible_ts_secs, id);

            let mut simsg = message::Builder::new_default();
            let mut stored_item = simsg.init_root::<protocol::stored_item::Builder>();
            stored_item.set_contents(contents);
            stored_item.set_id(id);
            stored_item.set_visibility_ts_index_key(visibility_index_key.as_bytes());
            // Ensure buffer has enough capacity, then clear and reuse it
            let needed = simsg.size_in_words() * 8;
            if stored_contents.capacity() < needed {
                stored_contents.reserve(needed - stored_contents.capacity());
            }
            stored_contents.clear();
            serialize::write_message(&mut stored_contents, &simsg)?;

            batch.put_cf(self.cf_available(), main_key.as_ref(), &stored_contents);
            batch.put_cf(
                self.cf_visibility_index(),
                visibility_index_key.as_ref(),
                main_key.as_ref(),
            );

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
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Return the next visibility timestamp (secs since epoch) among items in the
    /// visibility index, if any. This is determined by reading the first key in
    /// the `visibility_index/` namespace which is ordered by big-endian timestamp.
    pub fn peek_next_visibility_ts_secs(&self) -> Result<Option<u64>> {
        let mut iter = self
            .db
            .prefix_iterator_cf(self.cf_visibility_index(), VisibilityIndexKey::PREFIX);
        if let Some(kv) = iter.next() {
            let (idx_key, _) = kv?;
            Ok(Some(VisibilityIndexKey::parse_visible_ts_secs(&idx_key)?))
        } else {
            Ok(None)
        }
    }

    // used in tests only
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
        // Use a transaction for consistency and bind to a snapshot.
        let mut txn_opts = OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        let txn = self.db.transaction_opt(&WriteOptions::default(), &txn_opts);
        // Create read options bound to the txn snapshot for both iterator and gets
        let snapshot = txn.snapshot();
        let mut ro_iter = ReadOptions::default();
        ro_iter.set_snapshot(&snapshot);
        ro_iter.set_prefix_same_as_start(true);
        // Upper bound: include all visibility_index entries with timestamp <= now_secs
        // by setting an exclusive upper bound at (now_secs + 1).
        let mut viz_upper_bound: Vec<u8> = Vec::with_capacity(VisibilityIndexKey::PREFIX.len() + 8);
        viz_upper_bound.extend_from_slice(VisibilityIndexKey::PREFIX);
        viz_upper_bound.extend_from_slice(&(now_secs.saturating_add(1)).to_be_bytes());
        ro_iter.set_iterate_upper_bound(viz_upper_bound.clone());
        let mut ro_get = ReadOptions::default();
        ro_get.set_snapshot(&snapshot);
        // Prefix-bounded forward iterator from the prefix start using the snapshot-bound ReadOptions
        let mode = IteratorMode::From(VisibilityIndexKey::PREFIX, Direction::Forward);
        let viz_iter = txn.iterator_cf_opt(self.cf_visibility_index(), ro_iter, mode);

        // Stage 1: scan visibility index, lock eligible entries, and collect their main keys.
        let mut locked_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n); // (idx_key, main_key)
        for kv in viz_iter {
            if locked_pairs.len() >= n {
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
            if txn
                .get_pinned_for_update_cf_opt(self.cf_visibility_index(), &idx_key, true, &ro_get)?
                .is_none()
            {
                // We lost the race on this one, try another.
                continue;
            }

            locked_pairs.push((idx_key.to_vec(), main_key.to_vec()));
        }

        // Nothing to do if we didn't lock any candidates.
        if locked_pairs.is_empty() {
            return Ok((Uuid::nil().into_bytes(), Vec::new()));
        }

        // Stage 2: batch read the main values via multi_get on a snapshot-bound ReadOptions.
        let mut ro_get_for_batch = ReadOptions::default();
        ro_get_for_batch.set_snapshot(&snapshot);

        // Prepare request pairs of (&CF, K) while owning the backing key storage.
        let cf_avail = self.cf_available();
        let owned_keys: Vec<Vec<u8>> = locked_pairs.iter().map(|(_, k)| k.clone()).collect();
        let reqs: Vec<(&rocksdb::ColumnFamily, Vec<u8>)> =
            owned_keys.into_iter().map(|k| (cf_avail, k)).collect();

        // Fetch in one round trip; fall back to per-key get_for_update for any misses.
        let mut fetched_values: Vec<Vec<u8>> = Vec::with_capacity(locked_pairs.len());
        let multi_results = self.db.multi_get_cf_opt(reqs, &ro_get_for_batch);

        // The rocksdb crate returns a Vec<Result<Option<PinnableSlice>, Error>> for multi_get.
        // Normalize into concrete owned Vec<u8>, performing fallback lookups for misses.
        for (i, res) in multi_results.into_iter().enumerate() {
            let val_bytes: Vec<u8> = match res {
                Ok(Some(ps)) => ps.to_vec(),
                Ok(None) => {
                    // Fallback: lock and fetch the specific key using the transaction.
                    let main_key = &locked_pairs[i].1;
                    let Some(pinned) = txn.get_pinned_for_update_cf_opt(
                        self.cf_available(),
                        main_key,
                        true,
                        &ro_get,
                    )?
                    else {
                        return Err(Error::assertion_failed(&format!(
                            "main key not found: {:?}",
                            main_key
                        )));
                    };
                    pinned.to_vec()
                }
                Err(e) => return Err(Error::from(e)),
            };
            fetched_values.push(val_bytes);
        }

        // Stage 3: decode, build response items, and move entries to in_progress within the txn.
        let mut polled_items = Vec::with_capacity(fetched_values.len().min(n));
        for (i, (idx_key, main_key)) in locked_pairs.into_iter().enumerate() {
            if polled_items.len() >= n {
                break;
            }

            let main_value = &fetched_values[i];
            let stored_item_message = serialize::read_message_from_flat_slice(
                &mut &main_value[..],
                message::ReaderOptions::new(),
            )?;
            let stored_item = stored_item_message.get_root::<protocol::stored_item::Reader>()?;

            debug_assert!(!stored_item.get_id()?.is_empty());
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
            // Remove the visibility index first, then move value.
            txn.delete_cf(self.cf_visibility_index(), &idx_key)?;
            txn.delete_cf(self.cf_available(), &main_key)?;
            txn.put_cf(self.cf_in_progress(), new_main_key.as_ref(), main_value)?;
        }

        // If no items were found, return a nil lease and empty polled items. TODO: is this to golangy (:
        if polled_items.is_empty() {
            return Ok((Uuid::nil().into_bytes(), Vec::new()));
        }

        // Build the lease entry.
        let lease_entry = build_lease_entry_message(
            expiry_ts_secs,
            lease_expiry_index_key.as_bytes(),
            &polled_items,
        )?;
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8); // TODO: avoid allocation
        serialize::write_message(&mut lease_entry_bs, &lease_entry)?;

        // Write the lease entry and its expiry index
        txn.put_cf(self.cf_leases(), lease_key.as_ref(), &lease_entry_bs)?;
        txn.put_cf(
            self.cf_lease_expiry(),
            lease_expiry_index_key.as_ref(),
            lease_key.as_ref(),
        )?;

        drop(snapshot);
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
        if txn
            .get_pinned_for_update_cf(self.cf_in_progress(), &in_progress_key, true)?
            .is_none()
        {
            return Ok(false);
        }

        // Validate lease exists
        let Some(lease_value) = txn.get_pinned_for_update_cf(self.cf_leases(), &lease_key, true)?
        else {
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
        // Reject operations under an expired lease, even if background expiry hasn't run yet.
        let now_secs: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        if lease_entry_reader.get_expiry_ts_secs() <= now_secs {
            tracing::info!(
                "expired lease cannot operate: {:?}",
                Uuid::from_bytes(*lease)
            );
            return Ok(false);
        }
        let ids = lease_entry_reader.get_ids()?;
        debug_assert_sorted_by_index(ids.len(), |i| ids.get(i).ok());
        let Some(found_idx) = binary_search_by_index(ids.len(), |i| ids.get(i).ok(), id) else {
            return Ok(false);
        };

        // Delete item from in_progress.
        txn.delete_cf(self.cf_in_progress(), in_progress_key.as_ref())?;

        // Rewrite the lease entry to exclude the id. If no items remain under this lease,
        // delete the lease entry instead.
        if ids.len() - 1 == 0 {
            txn.delete_cf(self.cf_leases(), lease_key.as_ref())?;
        } else {
            // Rebuild lease entry with remaining keys.
            // TODO: this could be done more efficiently by unifying the above search and this one.
            let mut msg = message::Builder::new_default(); // TODO: reduce allocs
            let mut builder = msg.init_root::<protocol::lease_entry::Builder>();
            // Preserve expiry fields from the existing lease entry
            builder.set_expiry_ts_secs(lease_entry_reader.get_expiry_ts_secs());
            let prev_idx_key = lease_entry_reader
                .get_expiry_ts_index_key()
                .unwrap_or_default();
            if !prev_idx_key.is_empty() {
                builder.set_expiry_ts_index_key(prev_idx_key);
            }
            // Collect remaining ids, sort them, then write back.
            let mut remaining: Vec<&[u8]> = Vec::with_capacity((ids.len() - 1) as usize);
            for (i, k) in ids.iter().enumerate() {
                if i == found_idx {
                    continue;
                }
                remaining.push(k?);
            }
            remaining.sort_unstable();
            let mut out_ids = builder.init_ids(remaining.len() as u32);
            for (i, idb) in remaining.into_iter().enumerate() {
                out_ids.set(i as u32, idb);
            }
            let mut buf = Vec::with_capacity(msg.size_in_words() * 8); // TODO: reduce allocs
            serialize::write_message(&mut buf, &msg)?;
            txn.put_cf(self.cf_leases(), lease_key.as_ref(), &buf)?;
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
        // Restrict iterator to only keys with expiry_ts <= now by setting an
        // exclusive upper bound at (now + 1).
        let mut ro = ReadOptions::default();
        ro.set_prefix_same_as_start(true);
        let mut expiry_upper_bound: Vec<u8> =
            Vec::with_capacity(LeaseExpiryIndexKey::PREFIX.len() + 8);
        expiry_upper_bound.extend_from_slice(LeaseExpiryIndexKey::PREFIX);
        expiry_upper_bound.extend_from_slice(&(now_secs.saturating_add(1)).to_be_bytes());
        ro.set_iterate_upper_bound(expiry_upper_bound.clone());
        let mode = IteratorMode::From(LeaseExpiryIndexKey::PREFIX, Direction::Forward);
        let iter = txn.iterator_cf_opt(self.cf_lease_expiry(), ro, mode);
        // Avoid deleting keys while iterating; collect expiry index keys to delete later.
        let mut expiry_index_keys_to_delete: Vec<Vec<u8>> = Vec::new();
        for kv in iter {
            let (idx_key, _) = kv?;

            debug_assert_eq!(
                &idx_key[..LeaseExpiryIndexKey::PREFIX.len()],
                LeaseExpiryIndexKey::PREFIX
            );

            let _idx_val = txn
                .get_pinned_for_update_cf(self.cf_lease_expiry(), &idx_key, true)?
                .ok_or_else(|| {
                Error::assertion_failed("visibility index entry not found after we scanned it. expiry should be the only one deleting leases (once expiry is single flighted... TODO)")
            })?;

            let (expiry_ts_secs, lease_bytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;

            if expiry_ts_secs > now_secs {
                // Because keys are ordered by timestamp, we can stop scanning
                break;
            }

            let lease_key = LeaseKey::from_lease_bytes(lease_bytes);

            // Load the lease entry. If it's not found, we lost the race with another call and that's fine; move on.
            let Some(lease_value) =
                txn.get_pinned_for_update_cf(self.cf_leases(), lease_key.as_ref(), true)?
            else {
                tracing::debug!(
                    "lease entry not found after we scanned it. ignoring. lease: {:?}",
                    Uuid::from_slice(lease_bytes).unwrap_or_default()
                );
                continue;
            };

            // TODO: ensure extend doesnt create multiple index entries for the same lease.

            // Parse lease entry keys
            let lease_msg = serialize::read_message_from_flat_slice(
                &mut &lease_value[..],
                message::ReaderOptions::new(),
            )?;
            let lease_entry_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let keys = lease_entry_reader.get_ids()?;

            // Reuse capnp builder and output buffer across items to reduce allocations.
            let mut builder = capnp::message::Builder::new_default();
            let mut updated: Vec<u8> = Vec::new();

            // Move each item back to available.
            for id in keys.iter() {
                let id = id?;
                let in_progress_key = InProgressKey::from_id(id);
                let Some(item_value) = txn.get_pinned_for_update_cf(
                    self.cf_in_progress(),
                    in_progress_key.as_ref(),
                    true,
                )?
                else {
                    // Item has already been removed or re-queued; skip.
                    continue;
                };

                // Write the item back to available, and add a visibility index entry.
                let avail_key = AvailableKey::from_id(id);
                // Overwrite the stored item's embedded visibility index key to the new one
                let stored_msg = serialize::read_message_from_flat_slice(
                    &mut &item_value[..],
                    message::ReaderOptions::new(),
                )?;
                {
                    let item_reader = stored_msg.get_root::<protocol::stored_item::Reader>()?;
                    let mut stored_item = builder.init_root::<protocol::stored_item::Builder>();
                    stored_item.set_contents(item_reader.get_contents()?);
                    stored_item.set_id(item_reader.get_id()?);
                    // set below after vis_idx_now computed
                }
                let vis_idx_now = VisibilityIndexKey::from_visible_ts_and_id(now_secs, id);
                {
                    let mut stored_item = builder.get_root::<protocol::stored_item::Builder>()?;
                    stored_item.set_visibility_ts_index_key(vis_idx_now.as_bytes());
                }
                // Reuse buffer for serialization
                let need = builder.size_in_words() * 8;
                if updated.capacity() < need {
                    updated.reserve(need - updated.capacity());
                }
                updated.clear();
                serialize::write_message(&mut updated, &builder)?;
                txn.put_cf(self.cf_available(), avail_key.as_ref(), &updated)?;
                txn.put_cf(
                    self.cf_visibility_index(),
                    vis_idx_now.as_ref(),
                    avail_key.as_ref(),
                )?;
                txn.delete_cf(self.cf_in_progress(), in_progress_key.as_ref())?;
            }

            // Remove the lease entry immediately; defer deleting the expiry index key
            txn.delete_cf(self.cf_leases(), lease_key.as_ref())?;
            expiry_index_keys_to_delete.push(idx_key.to_vec());
            processed += 1;
        }
        // Now delete collected expiry index keys outside of the iterator loop
        for key in expiry_index_keys_to_delete {
            txn.delete_cf(self.cf_lease_expiry(), &key)?;
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
        {
            let Some(lease_value) =
                txn.get_pinned_for_update_cf(self.cf_leases(), lease_key.as_ref(), true)?
            else {
                return Ok(false);
            };

            // Reject extending an already expired lease
            let lease_msg = serialize::read_message_from_flat_slice(
                &mut &lease_value[..],
                message::ReaderOptions::new(),
            )?;
            let lease_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let now_secs: u64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();
            if lease_reader.get_expiry_ts_secs() <= now_secs {
                return Ok(false);
            }

            // Compute and write the new expiry index key.
            let expiry_ts_secs = (std::time::SystemTime::now()
                + std::time::Duration::from_secs(lease_validity_secs))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
            let new_idx_key = LeaseExpiryIndexKey::from_expiry_ts_and_lease(expiry_ts_secs, lease);
            txn.put_cf(
                self.cf_lease_expiry(),
                new_idx_key.as_ref(),
                lease_key.as_ref(),
            )?;

            // Update the lease entry: delete old expiry index (if present in entry),
            // then set the new expiry ts and index key while preserving ids.
            let keys = lease_reader.get_ids()?;
            // Delete the previous expiry index key referenced by the lease entry.
            let prev_idx_key = lease_reader.get_expiry_ts_index_key()?;
            debug_assert!(
                !prev_idx_key.is_empty(),
                "expiryTsIndexKey must be present after full cutover"
            );
            if prev_idx_key != new_idx_key.as_bytes() {
                txn.delete_cf(self.cf_lease_expiry(), prev_idx_key)?;
            }

            // TODO: do this with set_root or some such / more efficiently.
            let mut out = message::Builder::new_default();
            let mut builder = out.init_root::<protocol::lease_entry::Builder>();
            builder.set_expiry_ts_secs(expiry_ts_secs);
            builder.set_expiry_ts_index_key(new_idx_key.as_bytes());
            let mut out_keys = builder.reborrow().init_ids(keys.len());
            for i in 0..keys.len() {
                out_keys.set(i, keys.get(i)?);
            }
            let mut buf = Vec::with_capacity(out.size_in_words() * 8);
            serialize::write_message(&mut buf, &out)?;
            txn.put_cf(self.cf_leases(), lease_key.as_ref(), &buf)?;
            // lease_value and related readers/messages drop here before commit
        }

        txn.commit()?;
        Ok(true)
    }
}

/// uuidv7 bytes
type Lease = [u8; 16];

type PolledItemOwnedReader =
    TypedReader<capnp::message::Builder<message::HeapAllocator>, protocol::polled_item::Owned>;

fn build_lease_entry_message(
    expiry_ts_secs: u64,
    expiry_index_key_bytes: &[u8],
    polled_items: &[PolledItemOwnedReader],
) -> Result<capnp::message::Builder<message::HeapAllocator>> {
    // Build the lease entry. capnp lists aren't dynamically sized so we
    // need to know how many to init before we start writing (?).
    let mut lease_entry = message::Builder::new_default();
    let mut lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
    lease_entry_builder.set_expiry_ts_secs(expiry_ts_secs);
    lease_entry_builder.set_expiry_ts_index_key(expiry_index_key_bytes);
    // Write ids unsorted first.
    let n = polled_items.len();
    // Stable sort without copying id bytes: collect borrowed slices, sort stably, then write
    let mut borrowed: Vec<&[u8]> = Vec::with_capacity(n);
    for item in polled_items.iter() {
        borrowed.push(item.get()?.get_id()?);
    }
    borrowed.sort(); // stable
    let mut out_ids = lease_entry_builder.init_ids(n as u32);
    for (i, id) in borrowed.into_iter().enumerate() {
        out_ids.set(i as u32, id);
    }
    Ok(lease_entry)
}

// Retry wrapper that composes busy-retry behavior around a `Storage` instance.
pub struct RetriedStorage<S> {
    inner: std::sync::Arc<S>,
}

impl RetriedStorage<Storage> {
    pub fn new(inner: Storage) -> Self {
        Self {
            inner: std::sync::Arc::new(inner),
        }
    }

    pub fn into_inner(self) -> std::sync::Arc<Storage> {
        self.inner
    }
}

impl RetriedStorage<Storage> {
    pub async fn add_available_item_from_parts(
        &self,
        id: &[u8],
        contents: &[u8],
        visibility_timeout_secs: u64,
    ) -> Result<()> {
        let inner = std::sync::Arc::clone(&self.inner);

        type OwnedBuffers = (std::sync::Arc<[u8]>, std::sync::Arc<[u8]>);
        struct State {
            attempt: u32,
            owned: Option<OwnedBuffers>,
        }
        let state = std::sync::Arc::new(std::sync::Mutex::new(State {
            attempt: 0,
            owned: None,
        }));

        with_rocksdb_busy_retry_async("add_available_item_from_parts", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            let state_ref = std::sync::Arc::clone(&state);
            async move {
                {
                    let mut guard = state_ref.lock().unwrap();
                    if guard.attempt > 0 && guard.owned.is_none() {
                        guard.owned = Some((
                            std::sync::Arc::<[u8]>::from(id.to_vec()),
                            std::sync::Arc::<[u8]>::from(contents.to_vec()),
                        ));
                    }
                }
                if let Some((id_arc, contents_arc)) = {
                    let guard = state_ref.lock().unwrap();
                    guard.owned.as_ref().cloned()
                } {
                    tokio::task::spawn_blocking(move || {
                        inner_ref.add_available_item_from_parts(
                            &id_arc,
                            &contents_arc,
                            visibility_timeout_secs,
                        )
                    })
                    .await
                    .map_err(Into::<Error>::into)??;
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    Ok(())
                } else {
                    // First attempt: zero-copy direct or block_in_place depending on runtime
                    let res = match tokio::runtime::Handle::try_current() {
                        Ok(handle)
                            if handle.runtime_flavor()
                                == tokio::runtime::RuntimeFlavor::CurrentThread =>
                        {
                            inner_ref.add_available_item_from_parts(
                                id,
                                contents,
                                visibility_timeout_secs,
                            )
                        }
                        _ => tokio::task::block_in_place(move || {
                            inner_ref.add_available_item_from_parts(
                                id,
                                contents,
                                visibility_timeout_secs,
                            )
                        }),
                    };
                    // Increment attempt after finishing
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    res.map(|_| ())
                }
            }
        })
        .await
    }

    pub async fn add_available_items_from_parts<'a, I>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], (&'a [u8], u64))>,
    {
        type BorrowedItem<'b> = (&'b [u8], (&'b [u8], u64));
        type OwnedItem = (std::sync::Arc<[u8]>, (std::sync::Arc<[u8]>, u64));

        let borrowed: Vec<BorrowedItem<'a>> = items.into_iter().collect();
        let inner = std::sync::Arc::clone(&self.inner);
        let borrowed_arc = std::sync::Arc::new(borrowed);

        struct State {
            attempt: u32,
            owned: Option<std::sync::Arc<Vec<OwnedItem>>>,
        }
        let state = std::sync::Arc::new(std::sync::Mutex::new(State {
            attempt: 0,
            owned: None,
        }));

        with_rocksdb_busy_retry_async("add_available_items_from_parts", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            let state_ref = std::sync::Arc::clone(&state);
            let borrowed_ref = std::sync::Arc::clone(&borrowed_arc);
            async move {
                {
                    let mut guard = state_ref.lock().unwrap();
                    if guard.attempt > 0 && guard.owned.is_none() {
                        let owned_vec: Vec<OwnedItem> = borrowed_ref
                            .iter()
                            .map(|(id, (c, v))| {
                                (
                                    std::sync::Arc::<[u8]>::from(*id),
                                    (std::sync::Arc::<[u8]>::from(*c), *v),
                                )
                            })
                            .collect();
                        guard.owned = Some(std::sync::Arc::new(owned_vec));
                    }
                }

                if let Some(owned_ref) = { state_ref.lock().unwrap().owned.as_ref().cloned() } {
                    tokio::task::spawn_blocking(move || {
                        let iter = owned_ref
                            .iter()
                            .map(|(id, (c, v))| (id.as_ref(), (c.as_ref(), *v)));
                        inner_ref.add_available_items_from_parts(iter)
                    })
                    .await
                    .map_err(Into::<Error>::into)??;
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    Ok(())
                } else {
                    let res = match tokio::runtime::Handle::try_current() {
                        Ok(handle)
                            if handle.runtime_flavor()
                                == tokio::runtime::RuntimeFlavor::CurrentThread =>
                        {
                            let iter = borrowed_ref.iter().map(|(id, (c, v))| ((*id), ((*c), *v)));
                            inner_ref.add_available_items_from_parts(iter)
                        }
                        _ => {
                            let borrowed_ref = std::sync::Arc::clone(&borrowed_ref);
                            tokio::task::block_in_place(move || {
                                let iter =
                                    borrowed_ref.iter().map(|(id, (c, v))| ((*id), ((*c), *v)));
                                inner_ref.add_available_items_from_parts(iter)
                            })
                        }
                    };
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    res.map(|_| ())
                }
            }
        })
        .await
    }

    pub async fn peek_next_visibility_ts_secs(&self) -> Result<Option<u64>> {
        let inner = std::sync::Arc::clone(&self.inner);
        with_rocksdb_busy_retry_async("peek_next_visibility_ts_secs", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            async move {
                let out =
                    tokio::task::spawn_blocking(move || inner_ref.peek_next_visibility_ts_secs())
                        .await
                        .map_err(Into::<Error>::into)??;
                Ok(out)
            }
        })
        .await
    }

    pub async fn get_next_available_entries(
        &self,
        n: usize,
    ) -> Result<(Lease, Vec<PolledItemOwnedReader>)> {
        let inner = std::sync::Arc::clone(&self.inner);
        with_rocksdb_busy_retry_async("get_next_available_entries", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            async move {
                let out =
                    tokio::task::spawn_blocking(move || inner_ref.get_next_available_entries(n))
                        .await
                        .map_err(Into::<Error>::into)??;
                Ok(out)
            }
        })
        .await
    }

    pub async fn get_next_available_entries_with_lease(
        &self,
        n: usize,
        lease_validity_secs: u64,
    ) -> Result<(Lease, Vec<PolledItemOwnedReader>)> {
        let inner = std::sync::Arc::clone(&self.inner);
        with_rocksdb_busy_retry_async("get_next_available_entries_with_lease", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            async move {
                let out = tokio::task::spawn_blocking(move || {
                    inner_ref.get_next_available_entries_with_lease(n, lease_validity_secs)
                })
                .await
                .map_err(Into::<Error>::into)??;
                Ok(out)
            }
        })
        .await
    }

    pub async fn remove_in_progress_item(&self, id: &[u8], lease: &Lease) -> Result<bool> {
        let lease_copy = *lease;
        let inner = std::sync::Arc::clone(&self.inner);

        struct State {
            attempt: u32,
            owned_id: Option<std::sync::Arc<[u8]>>,
        }
        let state = std::sync::Arc::new(std::sync::Mutex::new(State {
            attempt: 0,
            owned_id: None,
        }));

        with_rocksdb_busy_retry_async("remove_in_progress_item", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            let state_ref = std::sync::Arc::clone(&state);
            async move {
                {
                    let mut guard = state_ref.lock().unwrap();
                    if guard.attempt > 0 && guard.owned_id.is_none() {
                        guard.owned_id = Some(std::sync::Arc::<[u8]>::from(id.to_vec()));
                    }
                }

                if let Some(id_arc) = { state_ref.lock().unwrap().owned_id.as_ref().cloned() } {
                    let out = tokio::task::spawn_blocking(move || {
                        inner_ref.remove_in_progress_item(&id_arc, &lease_copy)
                    })
                    .await
                    .map_err(Into::<Error>::into)??;
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    Ok(out)
                } else {
                    let out = match tokio::runtime::Handle::try_current() {
                        Ok(handle)
                            if handle.runtime_flavor()
                                == tokio::runtime::RuntimeFlavor::CurrentThread =>
                        {
                            inner_ref.remove_in_progress_item(id, &lease_copy)
                        }
                        _ => tokio::task::block_in_place(move || {
                            inner_ref.remove_in_progress_item(id, &lease_copy)
                        }),
                    }?;
                    let mut guard = state_ref.lock().unwrap();
                    guard.attempt = guard.attempt.saturating_add(1);
                    Ok(out)
                }
            }
        })
        .await
    }

    pub async fn expire_due_leases(&self) -> Result<usize> {
        let inner = std::sync::Arc::clone(&self.inner);
        with_rocksdb_busy_retry_async("expire_due_leases", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            async move {
                let out = tokio::task::spawn_blocking(move || inner_ref.expire_due_leases())
                    .await
                    .map_err(Into::<Error>::into)??;
                Ok(out)
            }
        })
        .await
    }

    pub async fn extend_lease(&self, lease: &Lease, lease_validity_secs: u64) -> Result<bool> {
        let lease = *lease;
        let inner = std::sync::Arc::clone(&self.inner);
        with_rocksdb_busy_retry_async("extend_lease", || {
            let inner_ref = std::sync::Arc::clone(&inner);
            async move {
                let out = tokio::task::spawn_blocking(move || {
                    inner_ref.extend_lease(&lease, lease_validity_secs)
                })
                .await
                .map_err(Into::<Error>::into)??;
                Ok(out)
            }
        })
        .await
    }
}

async fn with_rocksdb_busy_retry_async<T, Fut, F>(name: &str, mut f: F) -> Result<T>
where
    Fut: std::future::Future<Output = Result<T>>,
    F: FnMut() -> Fut,
{
    const MAX_RETRIES: u32 = 15;
    const BASE_DELAY_MS: u64 = 10;
    const MAX_DELAY_MS: u64 = 5000;

    let mut attempt: u32 = 0;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                let is_busy = matches!(
                    e,
                    Error::Rocksdb { ref source, .. } if source.kind() == rocksdb::ErrorKind::Busy
                );
                if !is_busy {
                    return Err(e);
                }
                if attempt >= MAX_RETRIES {
                    tracing::warn!(operation = %name, attempts = attempt + 1, "RocksDB Busy: giving up after max retries");
                    return Err(e);
                }
                let exp = 1u64 << attempt.min(20);
                let ceiling = (BASE_DELAY_MS.saturating_mul(exp)).min(MAX_DELAY_MS);
                let jitter_ms = if ceiling == 0 {
                    0
                } else {
                    rand::thread_rng().gen_range(0..=ceiling)
                };
                tracing::debug!(operation = %name, attempt = attempt + 1, delay_ms = jitter_ms, "RocksDB Busy: backing off with jitter");
                tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
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
            .get_cf(
                storage.db.cf_handle("leases").expect("cf leases"),
                lease_key.as_ref(),
            )?
            .ok_or("lease not found")?;
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let ids = lease_entry.get_ids()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.get(0)?, b"42");

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
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    in_progress_key.as_ref(),
                )?
                .ok_or("in_progress value missing")?;

            // parse stored item to get original visibility index key
            let msg = serialize::read_message_from_flat_slice(
                &mut &value[..],
                message::ReaderOptions::new(),
            )?;
            let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;

            // available entry must be gone
            let avail_key = AvailableKey::from_id(id);
            assert!(
                storage
                    .db
                    .get_cf(
                        storage.db.cf_handle("available").expect("cf available"),
                        avail_key.as_ref(),
                    )?
                    .is_none()
            );

            // visibility index entry must be gone
            let idx_key = stored_item.get_visibility_ts_index_key()?;
            assert!(
                storage
                    .db
                    .get_cf(
                        storage
                            .db
                            .cf_handle("visibility_index")
                            .expect("cf visibility_index"),
                        idx_key,
                    )?
                    .is_none()
            );
        }

        // lease entry exists and has keys (at least the ones we set)
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get_cf(
                storage.db.cf_handle("leases").expect("cf leases"),
                lease_key.as_ref(),
            )?
            .ok_or("lease not found")?;
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let ids = lease_entry.get_ids()?;
        assert!(ids.len() >= 2);

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
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    inprog_id1.as_ref(),
                )?
                .is_none()
        );

        let inprog_id2 = InProgressKey::from_id(b"id2");
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    inprog_id2.as_ref(),
                )?
                .is_some()
        );

        // lease entry should contain only id2
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get_cf(
                storage.db.cf_handle("leases").expect("cf leases"),
                lease_key.as_ref(),
            )?
            .ok_or("lease missing")?;
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let ids = lease_entry.get_ids()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.get(0)?, b"id2");

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
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    inprog.as_ref(),
                )?
                .is_none()
        );

        // lease entry deleted
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("leases").expect("cf leases"),
                    lease_key.as_ref(),
                )?
                .is_none()
        );

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
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    inprog.as_ref(),
                )?
                .is_some()
        );

        // original lease entry still exists and contains the id
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get_cf(
                storage.db.cf_handle("leases").expect("cf leases"),
                lease_key.as_ref(),
            )?
            .ok_or("lease missing")?;
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let ids = lease_entry.get_ids()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.get(0)?, b"only");

        Ok(())
    }

    #[test]
    fn expired_lease_cannot_remove_before_sweeper_runs() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        // Add one item and poll with very short lease
        storage.add_available_item_from_parts(b"x", b"p", 0)?;
        let (lease, items) = storage.get_next_available_entries_with_lease(1, 1)?;
        assert_eq!(items.len(), 1);

        // Wait until the lease would be expired
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Without running the sweeper, removal under the expired lease should be rejected
        let removed = storage.remove_in_progress_item(b"x", &lease)?;
        assert!(!removed, "expired lease should not be able to remove item");

        Ok(())
    }

    #[test]
    fn expired_lease_cannot_extend_and_does_not_create_new_index() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        // Create an item and a short-lived lease
        storage.add_available_item_from_parts(b"y", b"p", 0)?;
        let (lease, items) = storage.get_next_available_entries_with_lease(1, 1)?;
        assert_eq!(items.len(), 1);

        // Wait for expiry wall clock
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Capture count of index entries referencing this lease before extend attempt
        let mut before = 0usize;
        {
            let txn = storage.db.transaction();
            let mut ro = rocksdb::ReadOptions::default();
            ro.set_prefix_same_as_start(true);
            let mode = rocksdb::IteratorMode::From(
                LeaseExpiryIndexKey::PREFIX,
                rocksdb::Direction::Forward,
            );
            let iter = txn.iterator_cf_opt(storage.cf_lease_expiry(), ro, mode);
            for kv in iter {
                let (idx_key, _val) = kv?;
                let (_ts, lbytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;
                if lbytes == lease {
                    before += 1;
                }
            }
        }

        // Attempt to extend; should be rejected
        let ok = storage.extend_lease(&lease, 5)?;
        assert!(!ok, "should not extend an expired lease");

        // Ensure no new index entries were created for this lease
        let mut after = 0usize;
        {
            let txn = storage.db.transaction();
            let mut ro = rocksdb::ReadOptions::default();
            ro.set_prefix_same_as_start(true);
            let mode = rocksdb::IteratorMode::From(
                LeaseExpiryIndexKey::PREFIX,
                rocksdb::Direction::Forward,
            );
            let iter = txn.iterator_cf_opt(storage.cf_lease_expiry(), ro, mode);
            for kv in iter {
                let (idx_key, _val) = kv?;
                let (_ts, lbytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;
                if lbytes == lease {
                    after += 1;
                }
            }
        }
        assert_eq!(
            before, after,
            "extend should not add index entries for expired lease"
        );

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
    fn extend_multiple_times_keeps_single_expiry_index_entry() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let db_path = "/tmp/queueber_test_db_extend_dupes";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // Create one item and poll to create a lease
        storage.add_available_item_from_parts(Uuid::now_v7().as_bytes(), b"x", 0)?;
        let (lease, items) = storage.get_next_available_entries_with_lease(1, 1)?;
        assert_eq!(items.len(), 1);

        // Extend several times
        assert!(storage.extend_lease(&lease, 2)?);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(storage.extend_lease(&lease, 3)?);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(storage.extend_lease(&lease, 4)?);

        // Count expiry index entries that reference this lease
        let mut count = 0usize;
        let txn = storage.db.transaction();
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_prefix_same_as_start(true);
        let mode =
            rocksdb::IteratorMode::From(LeaseExpiryIndexKey::PREFIX, rocksdb::Direction::Forward);
        let iter = txn.iterator_cf_opt(
            storage
                .db
                .cf_handle("lease_expiry")
                .expect("cf lease_expiry"),
            ro,
            mode,
        );
        for kv in iter {
            let (idx_key, _val) = kv?;
            let (_ts, lbytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;
            if lbytes == lease {
                count += 1;
            }
        }
        assert_eq!(
            count, 1,
            "should only be one expiry index entry per lease after extends"
        );

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
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("available").expect("cf available"),
                    avail_key.as_ref(),
                )?
                .is_some()
        );

        let inprog_key = InProgressKey::from_id(id);
        assert!(
            storage
                .db
                .get_cf(
                    storage.db.cf_handle("in_progress").expect("cf in_progress"),
                    inprog_key.as_ref(),
                )?
                .is_none()
        );

        // Read stored item to get its visibility index key and ensure it still exists
        let value = storage
            .db
            .get_cf(
                storage.db.cf_handle("available").expect("cf available"),
                avail_key.as_ref(),
            )?
            .ok_or("missing available value")?;
        let msg = serialize::read_message_from_flat_slice(
            &mut &value[..],
            message::ReaderOptions::new(),
        )?;
        let stored_item = msg.get_root::<protocol::stored_item::Reader>()?;
        let idx_key = stored_item.get_visibility_ts_index_key()?;
        assert!(
            storage
                .db
                .get_cf(
                    storage
                        .db
                        .cf_handle("visibility_index")
                        .expect("cf visibility_index"),
                    idx_key,
                )?
                .is_some()
        );

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
        let retried = Arc::new(RetriedStorage::new(
            Storage::new(tmp.path()).expect("storage"),
        ));

        // Add 10 items that are immediately visible
        for _ in 0..10 {
            let id = Uuid::now_v7().as_bytes().to_vec();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                retried
                    .add_available_item_from_parts(&id, b"payload", 0)
                    .await
            })?;
        }

        // Spawn 3 concurrent polls, each trying to get 4 items
        let mut handles = Vec::new();
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));

        for poller_id in 0..3 {
            let st = Arc::clone(&retried);
            let results = Arc::clone(&results);
            handles.push(std::thread::spawn(move || -> Result<()> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let (_lease, items) =
                    rt.block_on(async { st.get_next_available_entries(4).await })?;
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

    #[test]
    fn expire_due_leases_ignores_orphaned_index_entries() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        // Create an orphaned expiry index entry in the past that points to a non-existent lease
        let lease = Uuid::now_v7().into_bytes();
        let past_secs = (std::time::SystemTime::now() - std::time::Duration::from_secs(2))
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let idx_key = LeaseExpiryIndexKey::from_expiry_ts_and_lease(past_secs, &lease);
        let lease_key = LeaseKey::from_lease_bytes(&lease);

        storage.db.put_cf(
            storage
                .db
                .cf_handle("lease_expiry")
                .expect("cf lease_expiry"),
            idx_key.as_ref(),
            lease_key.as_ref(),
        )?;

        // Should not error and should process zero leases
        let processed = storage.expire_due_leases()?;
        assert_eq!(processed, 0);

        Ok(())
    }

    #[test]
    fn poll_reads_main_with_snapshot_even_if_deleted_after_lock() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");

        // Add one immediately visible item
        let id = b"snaptest";
        storage.add_available_item_from_parts(id, b"payload", 0)?;

        // Begin a transaction with a snapshot
        let mut txn_opts = rocksdb::OptimisticTransactionOptions::default();
        txn_opts.set_snapshot(true);
        let txn = storage
            .db
            .transaction_opt(&rocksdb::WriteOptions::default(), &txn_opts);

        // Iterate visibility index and capture the first entry
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_prefix_same_as_start(true);
        let mode =
            rocksdb::IteratorMode::From(VisibilityIndexKey::PREFIX, rocksdb::Direction::Forward);
        let mut viz_iter = txn.iterator_cf_opt(
            storage
                .db
                .cf_handle("visibility_index")
                .expect("cf visibility_index"),
            ro,
            mode,
        );
        let (idx_key, main_key) = viz_iter
            .next()
            .expect("expected at least one visibility index entry")?;

        // Lock the visibility index entry so we simulate the poller owning it
        let locked = txn.get_pinned_for_update_cf(
            storage
                .db
                .cf_handle("visibility_index")
                .expect("cf visibility_index"),
            &idx_key,
            true,
        )?;
        assert!(locked.is_some(), "failed to lock visibility index entry");

        // Delete the main key outside the transaction after we've taken the snapshot
        let main_key_vec = main_key.as_ref().to_vec();
        storage.db.delete_cf(
            storage.db.cf_handle("available").expect("cf available"),
            &main_key_vec,
        )?;

        // Read using the transaction's snapshot; this should still see the value
        let mut ropts = rocksdb::ReadOptions::default();
        ropts.set_snapshot(&txn.snapshot());
        let got = txn.get_pinned_for_update_cf_opt(
            storage.db.cf_handle("available").expect("cf available"),
            &main_key_vec,
            true,
            &ropts,
        )?;
        assert!(
            got.is_some(),
            "snapshot read should see the main value even if concurrently deleted"
        );

        Ok(())
    }

    #[test]
    fn lease_entry_keys_are_sorted_after_poll()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let db_path = "/tmp/queueber_test_db_sorted_keys";
        let storage = Storage::new(Path::new(db_path)).unwrap();
        scopeguard::defer!(std::fs::remove_dir_all(db_path).unwrap());

        // add items with out-of-order ids
        for id in [b"c", b"a", b"b"] {
            let mut msg = Builder::new_default();
            let item = make_item(&mut msg);
            storage.add_available_item((id.as_slice(), item))?;
        }

        // poll three items into one lease
        let (lease, entries) = storage.get_next_available_entries(3)?;
        assert_eq!(entries.len(), 3);

        // read lease entry directly
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let lease_value = storage
            .db
            .get_cf(
                storage.db.cf_handle("leases").expect("cf leases"),
                lease_key.as_ref(),
            )?
            .ok_or("lease not found")?;
        let lease_entry = serialize::read_message_from_flat_slice(
            &mut &lease_value[..],
            message::ReaderOptions::new(),
        )?;
        let lease_entry = lease_entry.get_root::<protocol::lease_entry::Reader>()?;
        let keys = lease_entry.get_ids()?;
        assert_eq!(keys.len(), 3);
        let k0 = keys.get(0)?;
        let k1 = keys.get(1)?;
        let k2 = keys.get(2)?;
        assert!(k0 <= k1 && k1 <= k2, "lease keys not sorted");

        Ok(())
    }
}
