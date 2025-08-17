use capnp::message::{self, TypedReader};
use capnp::serialize_packed;
use rocksdb::{DB, Options, WriteBatchWithTransaction};
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

    // TODO: move to in progress
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
        let iter = self.db.prefix_iterator(VISIBILITY_INDEX_PREFIX).take(n);

        // move these items to in progress and create a lease
        // NOTE: this means either scanning twice or buffering. going with the latter for now, since n is probably small.
        // TODO: can we do better?

        // TODO: use a mockable clock
        let lease = Uuid::now_v7().into_bytes();
        let lease_key = make_lease_key(&lease);
        let mut lease_entry = message::Builder::new_default();
        let lease_entry_builder = lease_entry.init_root::<protocol::lease_entry::Builder>();
        let mut lease_entry_keys = lease_entry_builder.init_keys(n as _);

        let mut polled_items = Vec::with_capacity(n);

        let mut in_progress_batch = WriteBatchWithTransaction::<false>::default();

        for (i, kv) in iter.enumerate() {
            let (idx_key, main_key) = kv?;

            debug_assert_eq!(
                &idx_key[..VISIBILITY_INDEX_PREFIX.len()],
                VISIBILITY_INDEX_PREFIX
            );

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

            debug_assert!(stored_item.get_id()?.len() > 0);
            debug_assert_eq!(stored_item.get_id()?, &main_key[AVAILABLE_PREFIX.len()..]);

            tracing::debug!(
                "got stored item: {{id: {}, contents: {}}}",
                String::from_utf8_lossy(&stored_item.get_id()?),
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
            lease_entry_keys.set(i as _, &main_key[AVAILABLE_PREFIX.len()..]);
            // TODO: make newtype safety stuff for these
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
fn make_main_key<'i>(id: &'i [u8], main_prefix: &'static [u8]) -> Result<Vec<u8>> {
    let mut main_key = Vec::with_capacity(main_prefix.len() + id.len());
    main_key.extend_from_slice(main_prefix);
    main_key.extend_from_slice(id);

    Ok(main_key)
}

fn make_visibility_index_key<'i>(id: &'i [u8], visibility_timeout_secs: u64) -> Result<Vec<u8>> {
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

    fn make_item<'i>(message: &'i mut Builder<HeapAllocator>) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(10);
        item.into_reader()
    }
}
