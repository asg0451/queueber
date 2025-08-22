use crate::{
    errors::{Error, Result},
    protocol::{self},
};
use capnp::message::{self, HeapAllocator, TypedBuilder};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, WriteBatch,
};
use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};
use uuid::Uuid;

pub struct Storage {
    db: DBWithThreadMode<MultiThreaded>,
    availability: BTreeMap<SystemTime, Uuid>,
}

const DEFAULT_CF_NAME: &str = "default";
const VISIBILITY_CF_NAME: &str = "visibility";

impl Storage {
    pub fn new(path: &Path) -> Result<Self> {
        let opts = Options::default();

        let default_cf = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME, opts.clone());
        let visibility_cf = ColumnFamilyDescriptor::new(VISIBILITY_CF_NAME, opts.clone());
        let cfs = vec![default_cf, visibility_cf];
        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&opts, path, cfs)?;

        // TODO: less stupid
        let mut storage = Self {
            db,
            availability: BTreeMap::new(),
        };
        let visibility = storage.rehydrate_visibility()?;
        storage.availability = visibility;

        Ok(storage)
    }

    pub fn add_item(
        &mut self,
        id: &[u8],
        contents: &[u8],
        visibility_timeout_secs: u64,
    ) -> Result<()> {
        let visible_at_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + visibility_timeout_secs;

        self.availability.insert(
            SystemTime::UNIX_EPOCH + Duration::from_secs(visible_at_sec),
            Uuid::from_slice(id)?,
        );

        let (default_cf, visibility_cf) = self.cf_handles()?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&default_cf, id, contents);
        batch.put_cf(&visibility_cf, id, &visible_at_sec.to_le_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn poll_pop_available_items(
        &mut self,
        n: usize,
    ) -> Result<Vec<TypedBuilder<protocol::polled_item::Owned, HeapAllocator>>> {
        self.availability.pop_first()

        let (default_cf, _) = self.cf_handles()?;
        let mut items = Vec::with_capacity(ids.len());
        for id in ids {
            let contents = self
                .db
                .get_cf(&default_cf, id)?
                .ok_or_else(|| Error::ItemNotFound {
                    backtrace: Backtrace::capture(),
                })?;

            let mut msg = message::Builder::new_default();
            let mut polled_item = msg.init_root::<protocol::polled_item::Builder>();
            polled_item.set_id(id.as_bytes());
            polled_item.set_contents(&contents[..]);
            items.push(msg.into_typed());
        }

        // TODO: and then lease idk

        Ok(items)
    }

    // TODO: is this too slow?
    fn cf_handles(&self) -> Result<(Arc<BoundColumnFamily<'_>>, Arc<BoundColumnFamily<'_>>)> {
        let default_cf = self
            .db
            .cf_handle(DEFAULT_CF_NAME)
            .ok_or(Error::AssertionFailed {
                msg: "Default CF not found".to_string(),
                backtrace: Backtrace::capture(),
            })?;
        let visibility_cf =
            self.db
                .cf_handle(VISIBILITY_CF_NAME)
                .ok_or(Error::AssertionFailed {
                    msg: "Visibility CF not found".to_string(),
                    backtrace: Backtrace::capture(),
                })?;
        Ok((default_cf, visibility_cf))
    }

    fn rehydrate_visibility(&self) -> Result<BTreeMap<SystemTime, Uuid>> {
        let mut visibility = BTreeMap::new();
        let (_, visibility_cf) = self.cf_handles()?;
        let iter = self.db.iterator_cf(&visibility_cf, IteratorMode::Start);
        for e in iter {
            let (visible_at, id) = e?;
            visibility.insert(
                SystemTime::UNIX_EPOCH
                    + Duration::from_secs(u64::from_le_bytes(
                        visible_at.as_ref().try_into().unwrap(),
                    )),
                Uuid::from_slice(id.as_ref())?,
            );
        }
        Ok(visibility)
    }
}

#[cfg(test)]
mod tests {
    // TODO: add tests
}
