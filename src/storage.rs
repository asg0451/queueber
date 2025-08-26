use capnp::message::{self, TypedReader};
use capnp::serialize;
use rocksdb::{
    Direction, IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options,
    ReadOptions, SliceTransform, WriteBatchWithTransaction, WriteOptions,
};
use std::path::Path;
use uuid::Uuid;

use crate::dbkeys::{
    AvailableKey, InProgressKey, LeaseExpiryIndexKey, LeaseKey, VisibilityIndexKey,
};
use crate::errors::{Error, Result};
use crate::protocol;
use rand::Rng;

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
        let iter = std::iter::once((id, (contents, visibility_timeout_secs)));
        self.add_available_items_from_parts(iter)
    }

    pub fn add_available_items_from_parts<'a, I>(&self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], (&'a [u8], u64))>,
    {
        let mut batch = WriteBatchWithTransaction::<true>::default();
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
            let mut stored_contents = Vec::with_capacity(simsg.size_in_words() * 8);
            serialize::write_message(&mut stored_contents, &simsg)?;

            batch.put(main_key.as_ref(), &stored_contents);
            batch.put(visibility_index_key.as_ref(), main_key.as_ref());

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
        let mut iter = self.db.prefix_iterator(VisibilityIndexKey::PREFIX);
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
        let mut ro_get = ReadOptions::default();
        ro_get.set_snapshot(&snapshot);
        // Prefix-bounded forward iterator from the prefix start using the snapshot-bound ReadOptions
        let mode = IteratorMode::From(VisibilityIndexKey::PREFIX, Direction::Forward);
        let viz_iter = txn.iterator_opt(mode, ro_iter);

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
            if txn
                .get_pinned_for_update_opt(&idx_key, true, &ro_get)?
                .is_none()
            {
                // We lost the race on this one, try another.
                continue;
            }

            // Fetch and decode the item.
            let main_value = txn
                .get_pinned_for_update_opt(&main_key, true, &ro_get)?
                .ok_or_else(|| {
                    Error::assertion_failed(&format!("main key not found: {:?}", main_key.as_ref()))
                })?;
            let stored_item_message = serialize::read_message_from_flat_slice(
                &mut &main_value[..],
                message::ReaderOptions::new(),
            )?;
            let stored_item = stored_item_message.get_root::<protocol::stored_item::Reader>()?;

            debug_assert!(!stored_item.get_id()?.is_empty());
            // contents may be empty; only assert structural invariants
            // debug_assert!(!stored_item.get_contents()?.is_empty());
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
            // First remove the visibility index so others won't see it.
            // Important: Although the transaction commits atomically, readers without a
            // snapshot may interleave reads (index then main). Deleting the index first
            // ensures concurrent readers don't see an index that points to a deleted main.
            // NOTE: i dont think this is true lmao and it's still broken in any case.
            // TODO: use snapshots in txns maybe that will help
            txn.delete(&idx_key)?;
            // Then move the value from available -> in_progress.
            txn.delete(&main_key)?;
            txn.put(new_main_key.as_ref(), &main_value)?;
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
        let ids = lease_entry_reader.get_ids()?;
        let Some((found_idx, _)) = ids
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
        if ids.len() - 1 == 0 {
            txn.delete(lease_key.as_ref())?;
        } else {
            // Rebuild lease entry with remaining keys.
            // TODO: this could be done more efficiently by unifying the above search and this one.
            let mut msg = message::Builder::new_default(); // TODO: reduce allocs
            let builder = msg.init_root::<protocol::lease_entry::Builder>();
            let mut out_keys = builder.init_ids(ids.len() as u32 - 1);
            let mut new_idx = 0;
            #[allow(clippy::explicit_counter_loop)] // TODO: clean this up
            for (_, k) in ids.iter().enumerate().filter(|(i, _)| *i != found_idx) {
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
        // Avoid deleting keys while iterating; collect expiry index keys to delete later.
        let mut expiry_index_keys_to_delete: Vec<Vec<u8>> = Vec::new();
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

            // Load the lease entry. If it's not found, we lost the race with another call and that's fine; move on.
            let Some(lease_value) = txn.get_pinned_for_update(lease_key.as_ref(), true)? else {
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

            // Move each item back to available.
            for id in keys.iter() {
                let id = id?;
                let in_progress_key = InProgressKey::from_id(id);
                let Some(item_value) = txn.get_pinned_for_update(in_progress_key.as_ref(), true)?
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
                let mut builder = capnp::message::Builder::new_default();
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
                let mut updated = Vec::with_capacity(builder.size_in_words() * 8);
                serialize::write_message(&mut updated, &builder)?;
                txn.put(avail_key.as_ref(), &updated)?;
                txn.put(vis_idx_now.as_ref(), avail_key.as_ref())?;
                txn.delete(in_progress_key.as_ref())?;
            }

            // Remove the lease entry immediately; defer deleting the expiry index key
            txn.delete(lease_key.as_ref())?;
            expiry_index_keys_to_delete.push(idx_key.to_vec());
            processed += 1;
        }
        // Now delete collected expiry index keys outside of the iterator loop
        for key in expiry_index_keys_to_delete {
            txn.delete(&key)?;
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

        // Find current expiry index entries for this lease and delete them after iteration
        // TODO: add this index entry to the lease entry so we can do a point lookup.
        let mut old_expiry_keys: Vec<Vec<u8>> = Vec::new();
        for kv in txn.prefix_iterator(LeaseExpiryIndexKey::PREFIX) {
            let (idx_key, _val) = kv?;
            let (_ts, lbytes) = LeaseExpiryIndexKey::split_ts_and_lease(&idx_key)?;
            if lbytes == lease && idx_key.as_ref() != new_idx_key.as_ref() {
                old_expiry_keys.push(idx_key.to_vec());
            }
        }
        for k in old_expiry_keys {
            txn.delete(&k)?;
        }

        // Update the lease entry's expiryTsSecs while preserving keys
        if let Some(lease_value) = txn.get_pinned_for_update(lease_key.as_ref(), true)? {
            let lease_msg = serialize::read_message_from_flat_slice(
                &mut &lease_value[..],
                message::ReaderOptions::new(),
            )?;
            let lease_reader = lease_msg.get_root::<protocol::lease_entry::Reader>()?;
            let keys = lease_reader.get_ids()?;

            // TODO: do this with set_root or some such / more efficiently.
            let mut out = message::Builder::new_default();
            let mut builder = out.init_root::<protocol::lease_entry::Builder>();
            builder.set_expiry_ts_secs(expiry_ts_secs);
            let mut out_keys = builder.reborrow().init_ids(keys.len());
            for i in 0..keys.len() {
                out_keys.set(i, keys.get(i)?);
            }
            let mut buf = Vec::with_capacity(out.size_in_words() * 8);
            serialize::write_message(&mut buf, &out)?;
            txn.put(lease_key.as_ref(), &buf)?;
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
    lease_validity_secs: u64,
    polled_items: &[PolledItemOwnedReader],
) -> Result<capnp::message::Builder<message::HeapAllocator>> {
    // Build the lease entry. capnp lists aren't dynamically sized so we
    // need to know how many to init before we start writing (?).
    let mut lease_entry = message::Builder::new_default();
    let mut lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
    lease_entry_builder.set_expiry_ts_secs(lease_validity_secs);
    let mut lease_entry_keys = lease_entry_builder.init_ids(polled_items.len() as u32);
    for (i, typed_item) in polled_items.iter().enumerate() {
        let item_reader: protocol::polled_item::Reader = typed_item.get()?;
        lease_entry_keys.set(i as u32, item_reader.get_id()?);
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
    const MAX_RETRIES: u32 = 8;
    const BASE_DELAY_MS: u64 = 10;
    const MAX_DELAY_MS: u64 = 1000;

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
            .get(lease_key.as_ref())?
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
        let ids = lease_entry.get_ids()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.get(0)?, b"only");

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

        storage.db.put(idx_key.as_ref(), lease_key.as_ref())?;

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
        let mut viz_iter = txn.prefix_iterator(VisibilityIndexKey::PREFIX);
        let (idx_key, main_key) = viz_iter
            .next()
            .expect("expected at least one visibility index entry")?;

        // Lock the visibility index entry so we simulate the poller owning it
        let locked = txn.get_pinned_for_update(&idx_key, true)?;
        assert!(locked.is_some(), "failed to lock visibility index entry");

        // Delete the main key outside the transaction after we've taken the snapshot
        let main_key_vec = main_key.as_ref().to_vec();
        storage.db.delete(&main_key_vec)?;

        // Read using the transaction's snapshot; this should still see the value
        let mut ropts = rocksdb::ReadOptions::default();
        ropts.set_snapshot(&txn.snapshot());
        let got = txn.get_pinned_for_update_opt(&main_key_vec, true, &ropts)?;
        assert!(
            got.is_some(),
            "snapshot read should see the main value even if concurrently deleted"
        );

        Ok(())
    }
}
