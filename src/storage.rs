use capnp::message::{self, TypedReader};
use capnp::serialize_packed;
use rocksdb::{DB, Options, WriteBatchWithTransaction};
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
        // TODO: for more efficient prefix-partitioned scans:
        // opts.set_prefix_extractor(SliceTransform::create("prefix",...?
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
        let iter = self
            .db
            .prefix_iterator(VisibilityIndexKey::PREFIX)
            .take(n);

        // move these items to in progress and create a lease
        // NOTE: this means either scanning twice or buffering. going with the latter for now, since n is probably small.
        // TODO: can we do better?

        // TODO: use a mockable clock
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = LeaseKey::from_lease_bytes(&lease);
        let mut lease_entry = message::Builder::new_default();
        let lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
        let mut lease_entry_keys = lease_entry_builder.init_keys(n as _);

        let mut polled_items = Vec::with_capacity(n);

        let mut in_progress_batch = WriteBatchWithTransaction::<false>::default();

        for (i, kv) in iter.enumerate() {
            let (idx_key, main_key) = kv?;

            debug_assert_eq!(&idx_key[..VisibilityIndexKey::PREFIX.len()], VisibilityIndexKey::PREFIX);

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

            // add the lease entry key
            lease_entry_keys.set(i as _, stored_item.get_id()?);
            // TODO: make newtype safety stuff for these
        }

        // add the lease to the lease set
        let mut lease_entry_bs = Vec::with_capacity(lease_entry.size_in_words() * 8);
        serialize_packed::write_message(&mut lease_entry_bs, &lease_entry)?;
        in_progress_batch.put(lease_key.as_ref(), &lease_entry_bs);

        self.db.write(in_progress_batch)?;

        Ok((lease, polled_items))
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

    #[test]
    fn e2e_test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();

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

    fn make_item<'i>(message: &'i mut Builder<HeapAllocator>) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(10);
        item.into_reader()
    }
}
