# Queueber Database Schema

This document provides a comprehensive overview of Queueber's database schema and how RocksDB is used to implement a distributed message queue with leasing semantics.

## Overview

Queueber is a message queue system that uses RocksDB for persistence and Cap'n Proto for data serialization. The system implements a queue with the following key features:

- **Items** can be added to the queue with optional visibility timeouts (delayed processing)
- **Polling** moves items from available to in-progress state with a lease
- **Leases** provide exclusive access to items with automatic expiry
- **Removal** completes item processing and removes them from the queue
- **Automatic requeuing** when leases expire

## RocksDB Column Family Architecture

The database uses RocksDB's OptimisticTransactionDB with 6 column families to separate different data access patterns:

| Column Family | Purpose | Access Pattern | Value Size |
|---------------|---------|----------------|------------|
| `default` | Default CF (currently unused) | - | - |
| `available` | Items ready for polling | Point reads/writes | Large (item contents) |
| `in_progress` | Items currently being processed | Point reads/deletes | Large (item contents) |
| `visibility_index` | Time-ordered index for delayed items | Forward scans | Small (key references) |
| `leases` | Active lease tracking | Point reads/updates | Medium (ID lists) |
| `lease_expiry` | Time-ordered lease expiry index | Forward scans | Small (lease references) |

### Column Family Configuration

Each column family is optimized for its specific access pattern:

```rust
// Namespace prefix extractor: treats "namespace/" as the prefix
let make_ns_prefix = || {
    SliceTransform::create(
        "ns_prefix",
        |key: &[u8]| match key.iter().position(|b| *b == b'/') {
            Some(idx) => &key[..=idx],
            None => key,
        },
        Some(|_key: &[u8]| true),
    )
};
```

All column families use the same prefix extractor to enable efficient prefix-based iteration.

## Key Schema

All keys follow a consistent namespace-based structure using strongly-typed wrappers defined in `src/dbkeys.rs`.

### Available Items: `available/<id>`

**Key Format:** `b"available/" + id`
- `id`: 16-byte UUID v7 (time-ordered)

**Value:** Cap'n Proto `StoredItem` containing:
```capnp
struct StoredItem {
    contents @0 :Data;           # Opaque item payload
    visibilityTsIndexKey @1 :Data; # Corresponding visibility index key
    id @2 :Data;                 # Item ID (same as key suffix)
}
```

### In-Progress Items: `in_progress/<id>`

**Key Format:** `b"in_progress/" + id`
- `id`: Same 16-byte UUID from available state

**Value:** Same `StoredItem` structure as available items (preserves original visibility key for requeuing)

### Visibility Index: `visibility_index/<timestamp>/<id>`

**Key Format:** `b"visibility_index/" + visible_ts_be + b"/" + id`
- `visible_ts_be`: 8-byte big-endian Unix timestamp (seconds)
- `id`: 16-byte UUID v7

**Value:** Reference to main item key (`b"available/" + id`)

**Purpose:** Time-ordered secondary index enabling efficient scans for items ready to be polled.

### Leases: `leases/<lease_bytes>`

**Key Format:** `b"leases/" + lease_bytes`
- `lease_bytes`: 16-byte UUID v7 lease identifier

**Value:** Cap'n Proto `LeaseEntry` containing:
```capnp
struct LeaseEntry {
    ids @0 :List(Data);          # Sorted list of item IDs owned by this lease
    expiryTsSecs @1 :UInt64;     # Lease expiry timestamp
    expiryTsIndexKey @2 :Data;   # Corresponding lease expiry index key
}
```

### Lease Expiry Index: `lease_expiry/<timestamp>/<lease_bytes>`

**Key Format:** `b"lease_expiry/" + expiry_ts_be + b"/" + lease_bytes`
- `expiry_ts_be`: 8-byte big-endian Unix timestamp (seconds)  
- `lease_bytes`: 16-byte UUID v7 lease identifier

**Value:** Reference to lease key (`b"leases/" + lease_bytes`)

**Purpose:** Time-ordered index for efficient lease expiry sweeping.

## Queue Operations

### Adding Items (`add_available_item_from_parts`)

1. **Generate visibility timestamp:** `now + visibility_timeout_secs`
2. **Create keys:**
   - Main key: `available/<id>`
   - Index key: `visibility_index/<visible_ts>/<id>`
3. **Build stored item:** Cap'n Proto message with contents, ID, and index key
4. **Atomic write batch:**
   ```
   PUT available/<id> → StoredItem
   PUT visibility_index/<visible_ts>/<id> → available/<id>
   ```

### Polling Items (`get_next_available_entries_with_lease`)

1. **Create lease:** Generate UUID v7 and compute expiry timestamp
2. **Scan visibility index:** 
   - Iterate `visibility_index/` prefix in timestamp order
   - Use upper bound: `visibility_index/<now+1>` to stop at future items
3. **Claim items atomically:**
   - Use `get_for_update` to lock index entry
   - Multi-get main values using snapshot-consistent reads  
   - Move claimed items to `in_progress/<id>`
   - Delete `available/<id>` and `visibility_index/<ts>/<id>`
4. **Create lease entry:**
   ```
   PUT leases/<lease> → LeaseEntry{ids, expiry_ts, expiry_index_key}
   PUT lease_expiry/<expiry_ts>/<lease> → leases/<lease>
   ```

### Removing Items (`remove_in_progress_item`)

1. **Validate ownership:**
   - Check `in_progress/<id>` exists
   - Check `leases/<lease>` exists and contains `id`
   - Reject if lease is expired (even before sweeper runs)
2. **Remove item:** `DELETE in_progress/<id>`
3. **Update lease:**
   - If other items remain: rewrite lease entry with remaining IDs
   - If no items remain: delete entire lease entry

### Lease Expiry (`expire_due_leases`)

1. **Scan expired leases:**
   - Iterate `lease_expiry/` prefix up to current timestamp
   - Use upper bound: `lease_expiry/<now+1>`
2. **Requeue items:**
   - For each item still in `in_progress/<id>`:
   - Rebuild `StoredItem` with immediate visibility (`now`)
   - Move back to `available/<id>`
   - Restore `visibility_index/<now>/<id>`
   - Delete `in_progress/<id>`
3. **Clean up lease:**
   ```
   DELETE leases/<lease>
   DELETE lease_expiry/<expiry_ts>/<lease>
   ```

## Concurrency and Consistency

### Transaction Usage

- **Optimistic transactions** with snapshots for polling operations
- **Snapshot isolation** ensures consistent reads across column families
- **get_for_update** provides pessimistic locking within transactions
- **Write batches** for atomic multi-key operations

### Race Condition Handling

The system gracefully handles common race conditions:

- **Stale index entries:** If `visibility_index` points to missing `available` item, the orphaned index is cleaned up
- **Concurrent polling:** Items can only be claimed once via `get_for_update` locking
- **Lease conflicts:** Operations under expired or non-existent leases are rejected

### Invariants

1. **Mutual exclusion:** Items exist in exactly one of `available/` or `in_progress/`
2. **Index consistency:** Every `visibility_index/` entry should reference an existing `available/` item (self-healing on detection of violations)
3. **Lease validity:** Active leases contain only items that exist in `in_progress/`
4. **Time ordering:** Timestamp-based keys use big-endian encoding for lexicographic ordering

## Data Serialization

All structured data uses Cap'n Proto for efficient serialization:

- **Cross-language compatibility:** Schema defined in `queueber.capnp`
- **Zero-copy reads:** Direct access to serialized data without parsing overhead
- **Compact representation:** Efficient binary format reduces storage overhead

### Main Data Structures

```capnp
# User-facing item structure
struct Item {
    contents @0 :Data;
    visibilityTimeoutSecs @1 :UInt64;
}

# Internal storage structure  
struct StoredItem {
    contents @0 :Data;
    visibilityTsIndexKey @1 :Data;
    id @2 :Data;
}

# Polling result
struct PolledItem {
    contents @0 :Data;
    id @1 :Data;
}

# Lease tracking
struct LeaseEntry {
    ids @0 :List(Data);
    expiryTsSecs @1 :UInt64;
    expiryTsIndexKey @2 :Data;
}
```

## Performance Characteristics

### Efficient Operations

- **Polling:** O(log n) visibility index scan + O(1) item retrieval
- **Adding:** O(1) puts to main and index column families
- **Removing:** O(1) deletes + O(k) lease entry rewrite (k = items in lease)
- **Expiry:** O(m) where m = expired leases, not total items

### Optimizations

- **Prefix extractors** enable efficient column family scans
- **Snapshot isolation** provides consistent multi-key reads without locks
- **Multi-get batching** reduces round trips for bulk item retrieval
- **Buffer reuse** minimizes allocations during high-throughput operations

### Write Amplification Mitigation

- **Column family separation** reduces compaction interference between large items and small indexes
- **Optimistic transactions** avoid unnecessary write-ahead logging for read-heavy operations
- **Batched writes** group related operations into single commits

## Monitoring and Observability

The system includes structured logging at key points:

- **Item lifecycle:** Add, poll, remove, expire operations logged with UUIDs
- **Lease operations:** Lease creation, extension, and expiry logged
- **Performance metrics:** Operation latencies and retry counts via `with_rocksdb_busy_retry_async`

## Future Considerations

### Schema Evolution

- **Backward compatibility:** Cap'n Proto supports schema evolution
- **Key format stability:** Existing key formats preserved during column family migration
- **Migration support:** Graceful transition from single-CF to multi-CF layout

### Scaling Considerations

- **Horizontal scaling:** Current design supports single-node deployment
- **Partitioning:** Key prefixes could enable future sharding by queue/tenant
- **Compaction tuning:** Per-CF compaction strategies based on access patterns

---

This schema provides the foundation for a scalable, consistent message queue that can handle high-throughput workloads while maintaining strong consistency guarantees through RocksDB's transaction system.