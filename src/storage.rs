use rocksdb::DB;
use std::path::Path;

use crate::errors::Result;

pub struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(path: &Path) -> Result<Self> {
        let db = DB::open_default(path)?;
        Ok(Self { db })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }
}
