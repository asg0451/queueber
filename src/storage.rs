use capnp::message::{self, TypedReader};
use capnp::{serialize, serialize_packed};
use rocksdb::{DB, Options, WriteBatchWithTransaction};
use std::io::BufReader;
use std::path::Path;

use crate::errors::{Error, Result};
use crate::protocol;

// DB SCHEMA:
// "available/" + id -> stored_item{item, visibility_ts_index_key}
// "in_progress/" + id -> stored_item{item, visibility_ts_index_key}
// "visibility_index/" + visibility_ts -> id
// "leases/" + lease -> lease_info ? TODO

const AVAILABLE_PREFIX: &[u8] = b"available/";
const IN_PROGRESS_PREFIX: &[u8] = b"in_progress/";
const VISIBILITY_INDEX_PREFIX: &[u8] = b"visibility_index/";

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

    #[tracing::instrument(skip(self))]
    pub fn add_available_item<'i>(
        &self,
        (id, item): (&'i [u8], protocol::item::Reader<'i>),
    ) -> Result<()> {
        let (main_key, visibility_index_key) = make_keys(id, item, AVAILABLE_PREFIX)?;

        // make stored item to insert
        let mut simsg = message::Builder::new_default();
        let mut stored_item = simsg.init_root::<protocol::stored_item::Builder>();
        stored_item.set_item(item)?;
        stored_item.set_visibility_ts_index_key(&visibility_index_key);
        let mut contents = Vec::with_capacity(simsg.size_in_words() * 8); // TODO: reuse
        serialize_packed::write_message(&mut contents, &simsg)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.put(&main_key, &contents);
        batch.put(&visibility_index_key, &main_key);
        self.db.write(batch)?;
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
    ) -> impl Iterator<Item = Result<TypedReader<serialize::OwnedSegments, protocol::stored_item::Owned>>>
    {
        let iter = self.db.prefix_iterator(VISIBILITY_INDEX_PREFIX).take(n);
        iter.map(|kv| {
            let (_idx_key, main_key) = kv?;
            let main_value = self.db.get(&main_key)?.ok_or_else(|| {
                Error::AssertionFailed(format!(
                    "database integrity violated: main key not found: {:?}",
                    &main_key
                ))
            })?;

            let message = serialize_packed::read_message(
                BufReader::new(&main_value[..]), // TODO: avoid allocation
                message::ReaderOptions::new(),
            )?;
            let t: TypedReader<serialize::OwnedSegments, protocol::stored_item::Owned> =
                TypedReader::new(message);
            Ok(t)
        })
    }
}

// TODO: is there a way to do this without allocating / with an arena or smth? i'd like to avoid allocating in the hot path
// returns (main key, visibility index key)
fn make_keys<'i>(
    id: &'i [u8],
    item: protocol::item::Reader<'i>,
    main_prefix: &'static [u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    let vts = item.get_visibility_timeout_secs();

    // TODO: use a mockable clock
    let now = std::time::SystemTime::now();
    let visible_ts_bs = (now + std::time::Duration::from_secs(vts))
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_le_bytes();

    let mut main_key = Vec::with_capacity(main_prefix.len() + id.len());
    main_key.extend_from_slice(main_prefix);
    main_key.extend_from_slice(id);

    let mut visibility_index_key =
        Vec::with_capacity(VISIBILITY_INDEX_PREFIX.len() + visible_ts_bs.len());
    visibility_index_key.extend_from_slice(VISIBILITY_INDEX_PREFIX);
    visibility_index_key.extend_from_slice(&visible_ts_bs);

    Ok((main_key, visibility_index_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol;
    use capnp::message::{Builder, HeapAllocator, Reader};

    #[test]
    fn e2e_test() -> Result<()> {
        let storage = Storage::new(Path::new("test_db")).unwrap();
        let mut message = Builder::new_default();
        let item = make_item(&mut message);
        storage.add_available_item((b"42", item)).unwrap();
        let entries = storage
            .get_next_available_entries(1)
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 1);
        let si = entries[0].get()?;
        assert_eq!(si.get_item()?.get_contents()?, b"hello");
        assert_eq!(si.get_item()?.get_visibility_timeout_secs(), 10);
        Ok(())
    }

    fn make_item<'i>(message: &'i mut Builder<HeapAllocator>) -> protocol::item::Reader<'i> {
        let mut item = message.init_root::<protocol::item::Builder>();
        item.set_contents(b"hello");
        item.set_visibility_timeout_secs(10);
        item.into_reader()
    }
}
