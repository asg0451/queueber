# Prometheus Metrics

Queueber now includes comprehensive Prometheus metrics for monitoring both Service Level Indicators (SLIs) and RocksDB internals.

## Metrics Server

The metrics server runs on a separate HTTP endpoint (default: `127.0.0.1:9091`) and exposes metrics in Prometheus format.

### Endpoints

- `/metrics` - Prometheus metrics in text format
- `/health` - Health check endpoint

### Starting with Metrics

```bash
cargo run --bin queueber -- --metrics 127.0.0.1:9091
```

## Available Metrics

### SLI Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `queueber_requests_total` | Counter | Total number of requests processed |
| `queueber_request_duration_seconds_total` | Counter | Total request duration in seconds |
| `queueber_queue_size` | Gauge | Current number of available items in the queue |
| `queueber_queue_depth` | Gauge | Total queue depth (available + leased items) |
| `queueber_lease_expirations_total` | Counter | Total number of lease expirations |
| `queueber_failed_requests_total` | Counter | Total number of failed requests |

### RocksDB Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `queueber_rocksdb_operations_total` | Counter | Total number of RocksDB operations |
| `queueber_rocksdb_operation_duration_seconds_total` | Counter | Total RocksDB operation duration in seconds |
| `queueber_rocksdb_memory_usage_bytes` | Gauge | RocksDB memory usage in bytes |
| `queueber_rocksdb_disk_usage_bytes` | Gauge | RocksDB disk usage in bytes |

### Background Task Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `queueber_background_task_runs_total` | Counter | Total number of background task runs |
| `queueber_background_task_duration_seconds_total` | Counter | Total background task duration in seconds |
| `queueber_background_task_errors_total` | Counter | Total number of background task errors |

## Background Tasks Monitored

1. **Lease Expiry Task** - Periodically expires due leases
2. **Visibility Check Task** - Checks for items that become visible
3. **Metrics Update Task** - Updates RocksDB and queue metrics every 30 seconds

## Usage Examples

### Basic Monitoring

```bash
# Start the server with metrics
cargo run --bin queueber -- --metrics 127.0.0.1:9091

# View metrics
curl http://127.0.0.1:9091/metrics
```

### Prometheus Configuration

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'queueber'
    static_configs:
      - targets: ['127.0.0.1:9091']
    metrics_path: '/metrics'
```

### Grafana Dashboard

You can create Grafana dashboards using these metrics to monitor:

- Request throughput and latency
- Queue depth and processing rates
- RocksDB performance and resource usage
- Background task health and performance

## Implementation Details

- Metrics are collected using the `prometheus-client` crate
- Metrics are exposed via an Axum HTTP server
- Background tasks automatically update metrics every 30 seconds
- All RocksDB operations are instrumented for timing and success/failure tracking
- Request-level metrics are collected for all RPC operations (add, poll, remove)

## Future Enhancements

- [ ] Add RocksDB compaction statistics
- [ ] Add read/write amplification metrics
- [ ] Add per-operation latency histograms
- [ ] Add RocksDB property collection for more detailed metrics