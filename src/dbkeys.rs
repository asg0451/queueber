//! Strongly-typed wrappers for RocksDB keys to avoid prefix mixups.

/// Key for items that are available to be polled: `b"available/" + id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableKey(Vec<u8>);

/// Key for items that are currently in progress: `b"in_progress/" + id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InProgressKey(Vec<u8>);

/// Visibility index key: `b"visibility_index/" + visible_ts_be + b"/" + id`.
///
/// Notes:
/// - Timestamp is encoded as 8-byte big-endian to preserve lexicographic
///   ordering by time. This allows efficient range/prefix scans that stop
///   once we encounter future-visible entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityIndexKey(Vec<u8>);

/// Lease key: `b"leases/" + lease_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseKey(Vec<u8>);

impl AvailableKey {
    pub const PREFIX: &'static [u8] = b"available/";

    pub fn from_id(id: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + id.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(id);
        Self(key)
    }

    /// Returns the underlying database key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the item id suffix contained in this key.
    pub fn id_suffix(&self) -> &[u8] {
        &self.0[Self::PREFIX.len()..]
    }
}

impl AsRef<[u8]> for AvailableKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl InProgressKey {
    pub const PREFIX: &'static [u8] = b"in_progress/";

    pub fn from_id(id: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + id.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(id);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for InProgressKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl VisibilityIndexKey {
    pub const PREFIX: &'static [u8] = b"visibility_index/";

    /// Build a visibility index key from a visible-at timestamp (secs since epoch) and id.
    /// Timestamp is encoded as big-endian to preserve lexicographic ordering.
    pub fn from_visible_ts_and_id(visible_ts_secs: u64, id: &[u8]) -> Self {
        let ts_be = visible_ts_secs.to_be_bytes();
        let mut key = Vec::with_capacity(Self::PREFIX.len() + ts_be.len() + 1 + id.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(&ts_be);
        key.extend_from_slice(b"/");
        key.extend_from_slice(id);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Parse the visible-at timestamp (secs since epoch) from a visibility index key.
    /// Returns None if the key is malformed or doesn't have enough bytes.
    pub fn parse_visible_ts_secs(idx_key: &[u8]) -> Option<u64> {
        let ts_start = Self::PREFIX.len();
        let ts_end = ts_start + 8;
        if idx_key.len() < ts_end + 1 {
            // need at least trailing '/' after ts
            return None;
        }
        let mut ts_be = [0u8; 8];
        ts_be.copy_from_slice(&idx_key[ts_start..ts_end]);
        Some(u64::from_be_bytes(ts_be))
    }

    /// Split a visibility index key into (timestamp_secs, id_slice).
    /// Returns None if the key is malformed.
    pub fn split_ts_and_id(idx_key: &[u8]) -> Option<(u64, &[u8])> {
        let ts = Self::parse_visible_ts_secs(idx_key)?;
        let id_start = Self::PREFIX.len() + 8 + 1; // prefix + ts + '/'
        if id_start > idx_key.len() {
            return None;
        }
        Some((ts, &idx_key[id_start..]))
    }
}

impl AsRef<[u8]> for VisibilityIndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl LeaseKey {
    pub const PREFIX: &'static [u8] = b"leases/";

    pub fn from_lease_bytes(lease: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + lease.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(lease);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for LeaseKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
