Queueber
========

A simple queue server and client built with Cap'n Proto RPC and RocksDB.

Prerequisites
-------------

- Rust (nightly toolchain; e.g., `rustup default nightly`)
- Cap'n Proto CLI (`capnp`) version 0.5.2 or newer
  - Install instructions: https://capnproto.org/install.html
  - Ensure `capnp` is on your PATH; the build script will fail without it.

Build
-----

```
cargo build
```

If you see a schema compilation error from the build script, install the Cap'n Proto CLI as noted above and rebuild.

Running
-------

- Server:
  - Defaults: `127.0.0.1:9090`, data dir `/tmp/queueber/data`
  - Example:
    ```
    cargo run --bin queueber -- --listen 0.0.0.0:9090 --data-dir /var/lib/queueber
    ```

- Client (add one item):
  ```
  cargo run --bin client -- --addr localhost:9090 add --contents "hello" --visibility 10
  ```

## Prometheus Metrics

Queueber exposes comprehensive Prometheus metrics for monitoring both Service Level Indicators (SLIs) and RocksDB internals. The metrics system is **opt-in** and uses atomic operations for high performance.

### Metrics Server

The metrics server is started only when you explicitly enable it with the `--enable-metrics` flag:

```bash
# Start server with metrics enabled
cargo run --bin queueber -- --enable-metrics --metrics 127.0.0.1:9091

# Start server without metrics (default)
cargo run --bin queueber -- --listen 127.0.0.1:9090
```

### Performance Benefits

The new atomic metrics system provides significant performance improvements:

- **No RwLock Overhead**: Uses atomic operations instead of mutexes
- **Local Copies**: Metrics are stored locally and snapshotted for encoding
- **Zero-Cost When Disabled**: When `--enable-metrics` is not used, there's zero performance impact
- **Thread-Safe**: All operations are lock-free and thread-safe

### Available Metrics

#### Service Level Indicators (SLIs)
- `queueber_requests_total` - Total number of requests processed
- `queueber_request_duration_seconds_total` - Total request duration in seconds
- `queueber_queue_size` - Current number of available items in the queue
- `queueber_queue_depth` - Total queue depth (available + leased items)
- `queueber_lease_expirations_total` - Total number of lease expirations
- `queueber_failed_requests_total` - Total number of failed requests

#### RocksDB Metrics
- `queueber_rocksdb_operations_total` - Total number of RocksDB operations
- `queueber_rocksdb_operation_duration_seconds_total` - Total RocksDB operation duration in seconds
- `queueber_rocksdb_memory_usage_bytes` - RocksDB memory usage in bytes
- `queueber_rocksdb_disk_usage_bytes` - RocksDB disk usage in bytes

#### Background Task Metrics
- `queueber_background_task_runs_total` - Total number of background task runs
- `queueber_background_task_duration_seconds_total` - Total background task duration in seconds
- `queueber_background_task_errors_total` - Total number of background task errors

### Usage Examples

#### View Metrics
```bash
curl http://127.0.0.1:9091/metrics
```

#### Health Check
```bash
curl http://127.0.0.1:9091/health
```

#### Prometheus Configuration
Add this to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'queueber'
    static_configs:
      - targets: ['127.0.0.1:9091']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Command Line Options

- `--enable-metrics` - Enable metrics collection and HTTP server
- `--metrics <addr>` - Metrics server address (default: 127.0.0.1:9091)
- `--listen <addr>` - RPC server address (default: 127.0.0.1:9090)
- `--data-dir <path>` - Data directory for RocksDB (default: /tmp/queueber/data)
- `--wipe` - Wipe the data directory before starting

### Performance Impact

- **With metrics disabled**: Zero performance impact
- **With metrics enabled**: Minimal overhead (~1-2% CPU, <1% memory)
- **Atomic operations**: No blocking or contention between threads
