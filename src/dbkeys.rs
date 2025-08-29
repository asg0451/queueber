//! Strongly-typed wrappers for RocksDB keys to avoid prefix mixups.

use std::backtrace::Backtrace;

use crate::errors::{Error, Result};

/// Key for items that are available to be polled: raw `id` bytes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AvailableKey<'a>(&'a [u8]);

/// Key for items that are currently in progress: raw `id` bytes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InProgressKey<'a>(&'a [u8]);

/// Visibility index key: `visible_ts_be + b"/" + id`.
///
/// Notes:
/// - Timestamp is encoded as 8-byte big-endian to preserve lexicographic
///   ordering by time. This allows efficient range/prefix scans that stop
///   once we encounter future-visible entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityIndexKey(Vec<u8>);

/// Lease key: raw `lease_bytes`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct LeaseKey<'a>(&'a [u8]);

/// Lease expiry index key: `expiry_ts_be + b"/" + lease_bytes`.
///
/// Notes:
/// - Timestamp is encoded as 8-byte big-endian to preserve lexicographic ordering by time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseExpiryIndexKey(Vec<u8>);

impl<'a> AvailableKey<'a> {
    pub fn from_id(id: &'a [u8]) -> Self {
        Self(id)
    }

    /// Returns the underlying database key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0
    }

    /// Returns the item id suffix contained in this key.
    pub fn id_suffix(&self) -> &[u8] {
        self.0
    }

    /// Returns the item id from a raw key without allocating.
    ///
    /// With tightened encodings, the key is the id itself.
    pub fn id_suffix_from_key_bytes(main_key_bytes: &[u8]) -> &[u8] {
        main_key_bytes
    }
}

impl AsRef<[u8]> for AvailableKey<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> InProgressKey<'a> {
    pub fn from_id(id: &'a [u8]) -> Self {
        Self(id)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0
    }
}

impl AsRef<[u8]> for InProgressKey<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl VisibilityIndexKey {
    /// Build a visibility index key from a visible-at timestamp (secs since epoch) and id.
    /// Timestamp is encoded as big-endian to preserve lexicographic ordering.
    pub fn from_visible_ts_and_id(visible_ts_secs: u64, id: &[u8]) -> Self {
        let ts_be = visible_ts_secs.to_be_bytes();
        let mut key = Vec::with_capacity(ts_be.len() + 1 + id.len());
        key.extend_from_slice(&ts_be);
        key.extend_from_slice(b"/");
        key.extend_from_slice(id);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Parse the visible-at timestamp (secs since epoch) from a visibility index key.
    pub fn parse_visible_ts_secs(idx_key: &[u8]) -> Result<u64> {
        let ts_end = 8;
        if idx_key.len() < ts_end + 1 {
            return Err(Error::AssertionFailed {
                msg: format!("malformed key: {:?}", idx_key),
                backtrace: Backtrace::capture(),
            });
        }
        let mut ts_be = [0u8; 8];
        ts_be.copy_from_slice(&idx_key[..ts_end]);
        Ok(u64::from_be_bytes(ts_be))
    }

    /// Split a visibility index key into (timestamp_secs, id_slice).
    pub fn split_ts_and_id(idx_key: &[u8]) -> Result<(u64, &[u8])> {
        let ts = Self::parse_visible_ts_secs(idx_key)?;
        let id_start = 8 + 1; // ts + '/'
        if id_start > idx_key.len() {
            return Err(Error::AssertionFailed {
                msg: format!("malformed key: {:?}", idx_key),
                backtrace: Backtrace::capture(),
            });
        }
        Ok((ts, &idx_key[id_start..]))
    }
}

impl AsRef<[u8]> for VisibilityIndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> LeaseKey<'a> {
    pub fn from_lease_bytes(lease: &'a [u8]) -> Self {
        Self(lease)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0
    }
}

impl AsRef<[u8]> for LeaseKey<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl LeaseExpiryIndexKey {
    pub fn from_expiry_ts_and_lease(expiry_ts_secs: u64, lease: &[u8]) -> Self {
        let ts_be = expiry_ts_secs.to_be_bytes();
        let mut key = Vec::with_capacity(ts_be.len() + 1 + lease.len());
        key.extend_from_slice(&ts_be);
        key.extend_from_slice(b"/");
        key.extend_from_slice(lease);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn parse_expiry_ts_secs(idx_key: &[u8]) -> Result<u64> {
        let ts_end = 8;
        if idx_key.len() < ts_end + 1 {
            return Err(Error::assertion_failed(&format!(
                "malformed key: {:?}",
                idx_key
            )));
        }
        let mut ts_be = [0u8; 8];
        ts_be.copy_from_slice(&idx_key[..ts_end]);
        Ok(u64::from_be_bytes(ts_be))
    }

    pub fn split_ts_and_lease(idx_key: &[u8]) -> Result<(u64, &[u8])> {
        let ts = Self::parse_expiry_ts_secs(idx_key)?;
        let lease_start = 8 + 1; // ts + '/'
        if lease_start > idx_key.len() {
            return Err(Error::assertion_failed(&format!(
                "malformed key: {:?}",
                idx_key
            )));
        }
        Ok((ts, &idx_key[lease_start..]))
    }
}

impl AsRef<[u8]> for LeaseExpiryIndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
