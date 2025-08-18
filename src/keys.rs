use crate::errors::{Error, Result};

// DB SCHEMA:
// "available/" + id -> stored_item{item, visibility_ts_index_key}
// "in_progress/" + id -> stored_item{item, visibility_ts_index_key}
// "visibility_index/" + visibility_ts -> id
// "leases/" + lease -> lease_entry{keys}

pub const AVAILABLE_PREFIX: &[u8] = b"available/";
pub const IN_PROGRESS_PREFIX: &[u8] = b"in_progress/";
pub const VISIBILITY_INDEX_PREFIX: &[u8] = b"visibility_index/";
pub const LEASE_PREFIX: &[u8] = b"leases/";

// it's a uuidv7
pub type Lease = [u8; 16];

/// Extract timestamp from a visibility index key
/// Key format: visibility_index/ + timestamp + "/" + id
pub fn extract_timestamp_from_visibility_key(idx_key: &[u8]) -> Result<u64> {
    let timestamp_start = VISIBILITY_INDEX_PREFIX.len();
    let timestamp_end = idx_key[timestamp_start..]
        .iter()
        .position(|&b| b == b'/')
        .ok_or_else(|| Error::AssertionFailed {
            msg: "Invalid visibility index key format".to_string(),
            backtrace: std::backtrace::Backtrace::capture(),
        })? + timestamp_start;
    
    let timestamp_bytes = &idx_key[timestamp_start..timestamp_end];
    let item_timestamp = u64::from_le_bytes(
        timestamp_bytes.try_into().map_err(|_| Error::AssertionFailed {
            msg: "Invalid timestamp bytes in visibility index key".to_string(),
            backtrace: std::backtrace::Backtrace::capture(),
        })?
    );
    
    Ok(item_timestamp)
}

/// Create a main key with the given prefix and id
pub fn make_main_key(id: &[u8], main_prefix: &'static [u8]) -> Result<Vec<u8>> {
    let mut main_key = Vec::with_capacity(main_prefix.len() + id.len());
    main_key.extend_from_slice(main_prefix);
    main_key.extend_from_slice(id);

    Ok(main_key)
}

/// Create a visibility index key with the given id and visibility timeout
pub fn make_visibility_index_key(id: &[u8], visibility_timeout_secs: u64) -> Result<Vec<u8>> {
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

/// Create a lease key
pub fn make_lease_key(lease: &Lease) -> Vec<u8> {
    let mut lease_key = Vec::with_capacity(LEASE_PREFIX.len() + lease.len());
    lease_key.extend_from_slice(LEASE_PREFIX);
    lease_key.extend_from_slice(lease);
    lease_key
}