## Queueber performance notes

### Threading model and why you only see 4 "tokio-runtime-worker" threads

- The server entrypoint (`src/bin/queueber/main.rs`) uses `#[tokio::main]` for the top-level runtime (multi-thread by default) to accept connections. Those threads show up as "tokio-runtime-worker".
- The actual RPC handling is offloaded to a pool of OS threads you spawn manually with `std::thread::spawn`, each running a single-threaded Tokio runtime (`Builder::new_current_thread()` with a `LocalSet`). These threads will not be named "tokio-runtime-worker" because they are not part of the multi-thread Tokio scheduler.
- `worker_count` is computed as `std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)`. If this returns 8 on your machine, the server will spawn 8 such OS threads. They may not be obvious in profiling tools because they are unnamed; they often appear with the process name or no distinctive label.
- Therefore, seeing only 4 "tokio-runtime-worker" threads does not indicate your worker pool is 4; it only reflects the top-level acceptor runtime’s threads. The worker pool’s threads won’t carry that name.

#### How to double-check at runtime

1) Name the worker threads for visibility (recommended):

```rust
let name = format!("rpc-worker-{i}");
let builder = std::thread::Builder::new().name(name);
let handle = builder.spawn(move || {
    // current_thread runtime setup...
})?;
```

2) Print Tokio runtime metrics (requires `--cfg tokio_unstable`, already set in CI):

```rust
// after startup
#[cfg(tokio_unstable)]
{
    let m = tokio::runtime::Handle::current().metrics();
    tracing::info!(
        workers = m.num_workers(),
        active = m.active_tasks_count(),
        "top-level runtime metrics"
    );
}
```

3) Inspect threads from the shell while the server runs:

```bash
ps -L -p $(pidof queueber) -o pid,tid,comm | sort -u
top -H -p $(pidof queueber)
```

You should see 1 acceptor thread group (with ~available_parallelism workers), plus your N `rpc-worker-*` threads after naming them.

### Why most worker threads look idle in practice

Observed pattern: one thread busy with Cap'n Proto RPC, one thread doing storage polls, one RocksDB background thread, others idle.

Root causes:

- Single-connection affinity: a single client connection funnels all its RPCs through the worker that owns that connection. With one busy client connection, only that worker is active while other workers sit idle. To utilize many workers, you need multiple concurrent connections or a different scheduling model.
- Blocking DB work on the worker thread: `poll()` and `remove()` call into `Storage` synchronously. RocksDB is a blocking API, so this ties up the worker thread during each call. Only `add()` currently offloads via `spawn_blocking`.
- Narrow wakeups: `notify.notify_one()` wakes a single waiter. With many waiting pollers, only one wakes on visibility/add events; others keep sleeping until the next notify.
- Duplicated background tasks per worker: `Server::new` (constructed per worker) spawns `lease_expiry` and `visibility_wakeup` tasks for each worker. This leads to redundant polling of RocksDB and potential contention while not increasing throughput.

Recommended fixes (incremental):

1) Offload all RocksDB work:
   - Wrap `storage.get_next_available_entries_with_lease` and `storage.remove_in_progress_item` in `tokio::task::spawn_blocking` inside `poll()` and `remove()` so the worker thread is not blocked.
   - Consider a dedicated `rayon`/custom thread pool for storage ops if you want stronger isolation from Tokio's blocking pool.

2) Improve wakeups:
   - Switch `notify.notify_one()` to `notify.notify_waiters()` when items become visible or are added. This wakes all waiters; you may add simple backoff if stampedes become an issue.

3) De-duplicate background tasks:
   - Spawn a single `lease_expiry` and a single `visibility_wakeup` task in the top-level runtime, not per worker. They can still use `spawn_blocking` for DB access and notify the shared `Notify`.

4) Connection-level parallelism:
   - If the client is single-connection, it will monopolize one worker. For benchmarks intended to measure concurrent throughput across workers, open multiple client connections.
   - Alternatively, refactor to a `Send`-safe per-connection task and use a multi-thread Tokio runtime without `LocalSet`, allowing tasks to distribute across cores. This depends on capnp-rpc constraints.

5) Validate utilization after changes:
   - With worker thread naming, use `top -H` or tracing to verify more workers are active under load. You should see blocking pool threads increase when many polls occur concurrently.

### Configuration knobs

- Expose `--workers <N>` on the server binary to override the default `available_parallelism()` for the RPC worker pool. Default can remain `available_parallelism()`.
- If you want explicit control over the acceptor runtime threads as well, replace the macro with a manual runtime builder and set `.worker_threads(M)`. Otherwise, the macro will size it to `available_parallelism()`.

### Low-overhead wins

- Serialization:
  - `serialize_packed` trades bandwidth for CPU. Add a feature flag to switch hot paths to `capnp::serialize` (unpacked) for lower CPU during benchmarks.
  - Reuse buffers for message building to reduce alloc pressure on hot paths.

- DB write batching:
  - In `get_next_available_entries_with_lease`, claim up to `n` items and perform the moves (delete available + index, put in-progress) inside a single transaction instead of per-item commits. Also write the lease and expiry index in the same transaction to cut fsyncs.

- RocksDB tuning (latency-oriented baseline):
  - `opts.increase_parallelism(num_cpus)`, `opts.set_max_background_jobs(2 * num_cpus)`.
  - Add a block-based table with a bloom filter; enable prefix bloom for your `prefix_iterator` workloads.
  - Consider `DBCompressionType::None` if disk space is acceptable; otherwise ZSTD with a very light level.
  - Smooth I/O: `opts.set_bytes_per_sync(1<<20)`, `opts.set_wal_bytes_per_sync(1<<20)`.
  - For synthetic benchmarks only, you can disable WAL (`WriteOptions::disable_wal(true)`) to compare upper bounds.

- Networking path:
  - Replace acceptor fan-out via mpsc with per-worker listeners using `SO_REUSEPORT` so each worker accepts directly. This removes a hop and improves scalability under high accept rates.
  - Keep `TCP_NODELAY` (already enabled). Set a larger backlog (e.g., 4096).

- Micro-optimizations:
  - Prefer `Uuid::new_v4()` (or a fast 128-bit random) for leases unless you need time-sortable IDs; you already index lease expiry by timestamp.
  - Minimize logging in hot paths or gate behind `trace`.
  - Avoid rebuilding Cap’n Proto messages where a lightweight copy/reference suffices.

### Larger simplification (optional)

If you can make per-connection work `Send` (or isolate the non-`Send` parts), you can drop the single-threaded `LocalSet` workers entirely and run a single multi-thread Tokio runtime, `spawn`ing tasks per connection. This removes the custom worker pool and mpsc dispatch, at the cost of ensuring your RPC code is `Send`-safe.

### What to implement first

1) Name worker threads and add a `--workers` CLI flag; log both `worker_count` and top-level runtime metrics at startup so it’s unambiguous how many threads are active.
2) Add a feature-flag to toggle packed vs. unpacked Cap’n Proto serialization for storage.
3) Batch claims and include lease writes in the same RocksDB transaction.
4) Add a tuned `BlockBasedOptions` with bloom filters and increase `max_background_jobs`.
5) Consider switching to `SO_REUSEPORT` per-worker accept.

These yield quick, measurable wins without changing external behavior.

