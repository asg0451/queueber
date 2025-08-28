## Performance optimization notes

### Hot paths overview

- **Storage (RocksDB)**: enqueue/add, poll/lease, remove/ack, lease expiry sweeper; namespaces/keys defined in `src/dbkeys.rs` and Cap'n Proto schemas in `queueber.capnp`.
- **Server**: RPC handling in `src/server/mod.rs`; batching in `src/server/coalescer.rs`.
- **Serialization**: Cap'n Proto message build/read in `src/protocol.rs` and storage paths.

### Currently implemented (confirmations)

- Per-worker listeners with `SO_REUSEPORT` and large backlog; each worker accepts directly (`bind_reuseport()` in `src/bin/queueber/main.rs`).
- RocksDB column families for all namespaces (`available`, `in_progress`, `visibility_index`, `leases`, `lease_expiry`) with `_cf` APIs.
- Prefix extractor configured and used for prefix scans; snapshot-bound iterators with `prefix_same_as_start(true)` and an upper bound in poll and lease sweeper.
- Pinned reads via `get_pinned_for_update_cf_opt`/`get_pinned_for_update_cf` on index and main values to avoid copies.
- Single-transaction poll move: delete index, move `available` → `in_progress`, then write `leases` and `lease_expiry` before commit.
- Enqueue uses a single write batch and reuses a serialization buffer for stored items.
- Background tasks are spawned once (lease expiry and visibility wakeup) and notify pollers.
- Poll coalescer is integrated; server long-polls with deadlines and `Notify`.

### Micro-optimizations (low-risk, quick wins)

- **Batch time acquisition in enqueue**: In `Storage::add_available_items_from_parts`, compute `now` once per batch and add per-item offsets; avoid per-item syscalls and conversions.
- **Cap'n Proto buffer reuse everywhere**: Reuse a scratch `Vec<u8>` (already done for stored items) for building `lease_entry` and `polled_item` messages in poll paths to reduce allocations and copies.
- **Pinned reads (audit)**: Already used in hot paths; audit for any remaining copies that can be replaced with pinned slices.
- **Batch point-reads**: After selecting keys from visibility index, use `multi_get_cf` to fetch `available` values in one call; fall back only for misses.
- **Iterator upper bounds (verify)**: Poll and lease sweeper already bound by `now`; ensure all time-ordered scans set `iterate_upper_bound` and `prefix_same_as_start`.
- **Small sorts and buffers**: In `build_lease_entry_message`, skip sorting for `n <= 1`; consider `smallvec::SmallVec<[&[u8]; 32]>` for common small batches.
- **Pre-size builders**: Pre-size Cap'n Proto lists and reuse a `Builder` per RPC handler where feasible to reduce allocator pressure on hot paths.
- **UUID generation batching**: Generate uuidv7 in a tight loop with a cached timestamp/clock state; you already avoid per-id heap allocs with `[u8; 16]`.
- **Consolidate spawn_blocking**: Group related DB operations into a single `spawn_blocking` section to reduce task churn under small batches.
- **Coalesce notify**: When many immediately-visible items are added, issue a single `notify_one()` on transition from empty → non-empty instead of per-item.
- **RPC IO buffers**: Increase `BufReader`/`BufWriter` sizes around Cap'n Proto streams to reduce read/write syscalls on large messages.
- **Clippy/inlining cleanups**: Keep `#[inline]` on tiny CF accessors (done) and remove accidental clones/copies in poll distribution logic.

### Macro-optimizations (higher impact)

#### Column families and per-CF tuning

- CF split is implemented. Focus on tuning and validation:
- Ensure all code paths use `_cf` APIs for puts/gets/iters/transactions.
- **DB-level options**:
  - `increase_parallelism(num_cpus)`, `set_max_background_jobs(2 * num_cpus)`.
  - `set_bytes_per_sync(1<<20)`, `set_wal_bytes_per_sync(1<<20)`.
  - `set_atomic_flush(true)`.
- **Per-CF table options**:
  - `available`: larger blocks (32–64 KiB), ZSTD/LZ4 light, optional whole-key Bloom.
  - `in_progress`/`leases`: small blocks (4–8 KiB), whole-key Bloom, light/no compression.
  - `visibility_index`/`lease_expiry`: small blocks (~4 KiB), prefix Bloom tuned for forward scans using the namespace prefix extractor.

#### Tighten key encodings post-CF

- After CF split, drop leading `"namespace/"` segment in keys to save bytes; CF identity disambiguates namespaces. Update `src/dbkeys.rs` consistently.

#### Poll coalescer and long-poll integration

- Extend `PollCoalescer` to wait on a `Notify` when a batch finds no items and wake on add/sweeper events; respect per-request `timeoutSecs`.
- Trigger immediate batch execution when queued `num_items` exceeds a threshold (don’t always sleep `batch_window_ms`).
- Track per-request deadlines; return early with None on timeout.

#### Single-transaction poll moves

- Already done; validate ordering and conflict behavior. Consider additional wins:
- Use `multi_get_cf` for main values after selecting candidates to reduce per-item gets.
- Ensure `RetriedStorage` retries on write-conflict with bounded backoff.

#### Use `multi_get_cf` + scatter

- After collecting candidate IDs from the index, issue a `multi_get_cf` on `available` to fetch values in parallel, then stage deletes/inserts in the same transaction.

#### Lease sweeper efficiency

- Sweep `lease_expiry` with bounded iterators (`upper_bound = now`), batch deletes/visibility updates, and cap work per tick by wall time to avoid latency spikes.

#### Sharding

- Introduce queue/tenant sharding (see `docs/multi-tenant-queues.md`) and create per-shard `PollCoalescer`, `Notify`, and sweeper to reduce cross-tenant contention. Per-worker coalescers and `SO_REUSEPORT` are already in place.

#### Backpressure and quotas

- Enforce per-tenant/queue rate and bytes limits on ingress (especially `add`) to smooth spikes and prevent write stalls.

#### Observability

- Add tracing spans/counters for:
  - RocksDB scan durations, write batch sizes, commit and retry latencies.
  - Coalescer batch sizes, wait times, distribution fairness.
  - End-to-end RPC latencies and payload sizes.

#### Benchmark toggles

- Feature flags to switch:
  - Cap'n Proto packed vs unpacked serialization on hot paths.
  - WAL disabled (bench-only) to establish upper throughput bounds.
  - Compression type/level per CF and block cache sizes.

### Storage-path specifics to validate (`src/storage.rs`)

- Ensure DB open with CF descriptors is complete and CF handles are cached; migrate to `_cf` variants across the board.
- For all iterators, set `ReadOptions` with `prefix_same_as_start(true)` and `iterate_upper_bound` for time-bounded scans.
- Keep using `WriteBatchWithTransaction` for enqueue and poll; consider `disableWAL(true)` strictly for synthetic benchmarks.
- Tune `RetriedStorage` backoff and max retries to avoid stall cascades under contention.

### Network and RPC path (`src/server/mod.rs`)

- Increase buffer sizes for `BufReader/BufWriter` around Cap'n Proto streams; consider pooling.
- Keep per-connection RPC execution on the same worker runtime; avoid unnecessary task hops.

### Measurement plan

- Use `benches/*` and `criterion` to measure CPU/latency; extend with scenarios covering add/poll/remove mixes.
- Run `stress.sh` under varying concurrency to validate coalescing and contention improvements.
- Validate worker and blocking pool utilization (`top -H`, tracing).
- Add CPU/heap profiling (`pprof`/jemalloc) to identify remaining hotspots.

### References

- `src/storage.rs`, `src/dbkeys.rs`, `src/server/mod.rs`, `src/server/coalescer.rs`, `src/protocol.rs`.
- `docs/db-access-patterns.md`, `docs/rocksdb-column-families.md`, `docs/multi-tenant-queues.md`.
