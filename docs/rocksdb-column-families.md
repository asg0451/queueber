## Using RocksDB Column Families in Queueber

### Context

Queueber currently stores all keys in a single RocksDB column family (the implicit "default" CF) and uses a DB-level prefix extractor that isolates namespaces by their `"namespace/"` prefix (e.g., `available/…`, `visibility_index/…`). Operations include:

- `available/` main values: point reads/writes, larger values
- `in_progress/` values: point reads/deletes, ephemeral
- `visibility_index/` entries: forward prefix scans up to a time bound, tiny values
- `leases/` entries: point reads/modifies
- `lease_expiry/` entries: forward prefix scans up to a time bound, tiny values

Mixing these workloads in one CF causes compaction and cache contention between large values and tiny index rows, increases write amplification, and makes it hard to tune options per access pattern.

### Proposal: split namespaces into dedicated column families

Map each logical namespace to its own CF:

- `available/` → `cf_available`
- `in_progress/` → `cf_in_progress`
- `visibility_index/` → `cf_visibility_index`
- `leases/` → `cf_leases`
- `lease_expiry/` → `cf_lease_expiry`

Transactions and snapshots in `OptimisticTransactionDB` remain atomic across CFs; you can use the `_cf` APIs on both DB and transaction handles.

### Expected benefits

- Reduce compaction interference: index CFs compact independently from large-value CFs
- Lower write stalls and write amplification under mixed workloads
- Better cache efficiency with per-CF block cache sizes and block sizes
- Tailored Bloom/prefix settings for scan-heavy vs point-read CFs

### Per-CF tuning (baseline)

Common DB-level:

- `create_missing_column_families(true)` and `set_atomic_flush(true)` for multi-CF crash consistency
- Increase background work: `increase_parallelism(num_cpus)` and `set_max_background_jobs(2*num_cpus)`
- Smooth I/O: `set_bytes_per_sync(1<<20)`, `set_wal_bytes_per_sync(1<<20)`

`cf_available` (larger values, point gets):

- Compression: ZSTD (low level) or LZ4; level-style compaction
- Larger block size (e.g., 32–64 KiB) and larger target file size
- Moderate Bloom (whole-key) if point reads dominate

`cf_in_progress`, `cf_leases` (small values, short-lived):

- Smaller blocks (e.g., 4–8 KiB), whole-key Bloom filter
- Light compression or none if disk space is ample

`cf_visibility_index`, `cf_lease_expiry` (tiny values, time-ordered scans):

- Block-based table with prefix Bloom; small blocks (e.g., 4 KiB)
- Prefix extractor tuned for efficient upper-bounded forward scans
  - Keep existing `"namespace/"` DB prefix for easy `prefix_iterator_cf`
  - Optionally add a per-CF transform that includes the 8-byte big-endian timestamp to further narrow Bloom

### Opening the DB with CF descriptors (sketch)

```rust
let mut db_opts = rocksdb::Options::default();
db_opts.create_if_missing(true);
db_opts.create_missing_column_families(true);
db_opts.set_atomic_flush(true);

let mut mk_block = |block_size_kb: usize, prefix: Option<rocksdb::SliceTransform>| {
    let mut bbo = rocksdb::BlockBasedOptions::default();
    bbo.set_block_size((block_size_kb * 1024) as usize);
    bbo.set_bloom_filter(10.0, false); // whole-key Bloom by default
    let mut cf = rocksdb::Options::default();
    cf.set_block_based_table_factory(&bbo);
    if let Some(p) = prefix { cf.set_prefix_extractor(p); cf.set_memtable_prefix_bloom_ratio(0.125); }
    cf
};

// Per-CF options
let cf_default = rocksdb::Options::default();
let cf_available = mk_block(64, None);
let cf_in_progress = mk_block(8, None);

// Prefix for namespace-only (for easy `prefix_iterator_cf` by `b"visibility_index/"`)
let ns_prefix = rocksdb::SliceTransform::create("ns_prefix", |key: &[u8]| {
    key.iter().position(|b| *b == b'/').map(|i| &key[..=i]).unwrap_or(key)
}, Some(|_| true));

let mut cf_visibility_index = mk_block(4, Some(ns_prefix.clone()));
let mut cf_lease_expiry = mk_block(4, Some(ns_prefix));

let cf_descs = vec![
    rocksdb::ColumnFamilyDescriptor::new("default", cf_default),
    rocksdb::ColumnFamilyDescriptor::new("available", cf_available),
    rocksdb::ColumnFamilyDescriptor::new("in_progress", cf_in_progress),
    rocksdb::ColumnFamilyDescriptor::new("visibility_index", cf_visibility_index),
    rocksdb::ColumnFamilyDescriptor::new("leases", rocksdb::Options::default()),
    rocksdb::ColumnFamilyDescriptor::new("lease_expiry", cf_lease_expiry),
];

let db = rocksdb::OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, cf_descs)?;

// Retrieve CF handles once and store them in Storage
let cf_available_h = db.cf_handle("available").unwrap();
let cf_in_progress_h = db.cf_handle("in_progress").unwrap();
let cf_visibility_index_h = db.cf_handle("visibility_index").unwrap();
let cf_leases_h = db.cf_handle("leases").unwrap();
let cf_lease_expiry_h = db.cf_handle("lease_expiry").unwrap();
```

### Code changes required (outline)

1) Extend `Storage` to hold CF handles:

```rust
struct Storage {
    db: rocksdb::OptimisticTransactionDB,
    cf_available: rocksdb::BoundColumnFamily,
    cf_in_progress: rocksdb::BoundColumnFamily,
    cf_visibility_index: rocksdb::BoundColumnFamily,
    cf_leases: rocksdb::BoundColumnFamily,
    cf_lease_expiry: rocksdb::BoundColumnFamily,
}
```

2) Switch DB calls to `_cf` variants:

- `db.put(..)` → `db.put_cf(cf_available, ..)` etc.
- Iterators: `db.prefix_iterator(PREFIX)` → `db.prefix_iterator_cf(cf_visibility_index, PREFIX)`
- Transactions: use `txn.get_pinned_for_update_cf_opt(cf, key, …)`, `txn.iterator_cf_opt(cf, mode, ro)`, `txn.put_cf(cf, …)`, `txn.delete_cf(cf, …)`

3) Keep key formats initially. After CF split, consider dropping the leading `"namespace/"` in each CF to save bytes; CF boundaries already disambiguate.

### Migration plan

1) Backward-compatible open:

- Open with `create_missing_column_families(true)` and begin writing all new data into CFs
- Continue reading old keys from `default` during a transitional window

2) Optional background migration:

- Iterate `default` keys by namespace prefix, rewrite into the target CF, then delete from `default`
- Perform in small batches to avoid stalls

3) Operational safeguards:

- Enable `atomic_flush(true)`; ensure WAL is enabled (default) during migration
- Validate with integration tests and stress runs before toggling any feature flags

### Testing and validation

- Unit/integration tests should pass unchanged; add tests that exercise CF-backed paths
- Run existing benches (`benches/storage_bench.rs`) before/after to compare:
  - Poll throughput under mixed add/poll/remove
  - Stall frequency and p99 latency while many small index updates occur
- Run `./stress.sh` to check for concurrency regressions

### Risks and mitigations

- Too many CFs can raise CPU/memory overhead. We keep it to 5, one per namespace
- Snapshot semantics must be respected across CFs. Use the same transaction snapshot when mixing iterators/gets on multiple CFs
- Incorrect CF handle usage can break reads. Centralize CF access in `Storage` and avoid passing raw handles around

### Rollout plan

1) Land CF plumbing with conservative defaults and behind a feature flag (optional)
2) Validate tests/benches; iterate on per-CF options
3) Migrate any existing data if running against a persistent DB
4) Remove feature flag after bake-in
