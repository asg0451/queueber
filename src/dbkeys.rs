//! Strongly-typed wrappers for RocksDB keys to avoid prefix mixups.

/// Key for items that are available to be polled: `b"available/" + id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableKey(Vec<u8>);

/// Key for items that are currently in progress: `b"in_progress/" + id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InProgressKey(Vec<u8>);

/// Visibility index key: `b"visibility_index/" + visible_ts_le + b"/" + id`.
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
    pub fn as_bytes(&self) -> &[u8] { &self.0 }

    /// Returns the item id suffix contained in this key.
    pub fn id_suffix(&self) -> &[u8] { &self.0[Self::PREFIX.len()..] }
}

impl AsRef<[u8]> for AvailableKey {
    fn as_ref(&self) -> &[u8] { self.as_bytes() }
}

impl InProgressKey {
    pub const PREFIX: &'static [u8] = b"in_progress/";

    pub fn from_id(id: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + id.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(id);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl AsRef<[u8]> for InProgressKey {
    fn as_ref(&self) -> &[u8] { self.as_bytes() }
}

impl VisibilityIndexKey {
    pub const PREFIX: &'static [u8] = b"visibility_index/";

    /// Build a visibility index key from a visible-at timestamp (secs since epoch) and id.
    pub fn from_visible_ts_and_id(visible_ts_secs_le: [u8; 8], id: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + visible_ts_secs_le.len() + 1 + id.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(&visible_ts_secs_le);
        key.extend_from_slice(b"/");
        key.extend_from_slice(id);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl AsRef<[u8]> for VisibilityIndexKey {
    fn as_ref(&self) -> &[u8] { self.as_bytes() }
}

impl LeaseKey {
    pub const PREFIX: &'static [u8] = b"leases/";

    pub fn from_lease_bytes(lease: &[u8]) -> Self {
        let mut key = Vec::with_capacity(Self::PREFIX.len() + lease.len());
        key.extend_from_slice(Self::PREFIX);
        key.extend_from_slice(lease);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl AsRef<[u8]> for LeaseKey {
    fn as_ref(&self) -> &[u8] { self.as_bytes() }
}

