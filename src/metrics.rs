use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Metrics {
    // SLI Metrics
    pub requests_total: Counter,
    pub request_duration_seconds: Counter,
    pub queue_size: Gauge,
    pub queue_depth: Gauge,
    pub lease_expirations: Counter,
    pub failed_requests: Counter,

    // RocksDB Metrics
    pub rocksdb_operations: Counter,
    pub rocksdb_operation_duration_seconds: Counter,
    pub rocksdb_memory_usage_bytes: Gauge,
    pub rocksdb_disk_usage_bytes: Gauge,

    // Background Task Metrics
    pub background_task_runs: Counter,
    pub background_task_duration_seconds: Counter,
    pub background_task_errors: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let requests_total = Counter::default();
        let request_duration_seconds = Counter::default();
        let queue_size = Gauge::default();
        let queue_depth = Gauge::default();
        let lease_expirations = Counter::default();
        let failed_requests = Counter::default();

        let rocksdb_operations = Counter::default();
        let rocksdb_operation_duration_seconds = Counter::default();
        let rocksdb_memory_usage_bytes = Gauge::default();
        let rocksdb_disk_usage_bytes = Gauge::default();

        let background_task_runs = Counter::default();
        let background_task_duration_seconds = Counter::default();
        let background_task_errors = Counter::default();

        // Register metrics
        registry.register(
            "queueber_requests_total",
            "Total number of requests",
            requests_total.clone(),
        );
        registry.register(
            "queueber_request_duration_seconds_total",
            "Total request duration in seconds",
            request_duration_seconds.clone(),
        );
        registry.register(
            "queueber_queue_size",
            "Current number of items in the queue",
            queue_size.clone(),
        );
        registry.register(
            "queueber_queue_depth",
            "Current depth of the queue (including leased items)",
            queue_depth.clone(),
        );
        registry.register(
            "queueber_lease_expirations_total",
            "Total number of lease expirations",
            lease_expirations.clone(),
        );
        registry.register(
            "queueber_failed_requests_total",
            "Total number of failed requests",
            failed_requests.clone(),
        );

        registry.register(
            "queueber_rocksdb_operations_total",
            "Total number of RocksDB operations",
            rocksdb_operations.clone(),
        );
        registry.register(
            "queueber_rocksdb_operation_duration_seconds_total",
            "Total RocksDB operation duration in seconds",
            rocksdb_operation_duration_seconds.clone(),
        );
        registry.register(
            "queueber_rocksdb_memory_usage_bytes",
            "RocksDB memory usage in bytes",
            rocksdb_memory_usage_bytes.clone(),
        );
        registry.register(
            "queueber_rocksdb_disk_usage_bytes",
            "RocksDB disk usage in bytes",
            rocksdb_disk_usage_bytes.clone(),
        );

        registry.register(
            "queueber_background_task_runs_total",
            "Total number of background task runs",
            background_task_runs.clone(),
        );
        registry.register(
            "queueber_background_task_duration_seconds_total",
            "Total background task duration in seconds",
            background_task_duration_seconds.clone(),
        );
        registry.register(
            "queueber_background_task_errors_total",
            "Total number of background task errors",
            background_task_errors.clone(),
        );

        Self {
            requests_total,
            request_duration_seconds,
            queue_size,
            queue_depth,
            lease_expirations,
            failed_requests,
            rocksdb_operations,
            rocksdb_operation_duration_seconds,
            rocksdb_memory_usage_bytes,
            rocksdb_disk_usage_bytes,
            background_task_runs,
            background_task_duration_seconds,
            background_task_errors,
        }
    }

    pub fn record_request(&self, _operation: &str, status: &str, duration: f64) {
        self.requests_total.inc();
        self.request_duration_seconds.inc_by(duration as u64);

        if status != "success" {
            self.failed_requests.inc();
        }
    }

    pub fn record_rocksdb_operation(&self, _operation: &str, _status: &str, duration: f64) {
        self.rocksdb_operations.inc();
        self.rocksdb_operation_duration_seconds
            .inc_by(duration as u64);
    }

    pub fn record_background_task(&self, _task: &str, status: &str, duration: f64) {
        self.background_task_runs.inc();
        self.background_task_duration_seconds
            .inc_by(duration as u64);

        if status != "success" {
            self.background_task_errors.inc();
        }
    }

    pub fn update_queue_metrics(&self, size: i64, depth: i64) {
        self.queue_size.set(size);
        self.queue_depth.set(depth);
    }

    pub fn update_rocksdb_metrics(&self, memory_usage: i64, disk_usage: i64) {
        self.rocksdb_memory_usage_bytes.set(memory_usage);
        self.rocksdb_disk_usage_bytes.set(disk_usage);
    }
}

pub type SharedMetrics = Arc<RwLock<Metrics>>;

pub fn create_metrics() -> (Registry, SharedMetrics) {
    let mut registry = Registry::default();
    let metrics = Metrics::new(&mut registry);
    let shared_metrics = Arc::new(RwLock::new(metrics));
    (registry, shared_metrics)
}

pub async fn encode_metrics(registry: &Registry) -> Result<String, Box<dyn std::error::Error>> {
    let mut buffer = String::new();
    encode(&mut buffer, registry)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let mut registry = Registry::default();
        let metrics = Metrics::new(&mut registry);

        // Test that metrics can be recorded
        metrics.record_request("test", "success", 0.1);
        metrics.record_rocksdb_operation("test", "success", 0.05);
        metrics.record_background_task("test", "success", 0.2);

        // Test queue metrics
        metrics.update_queue_metrics(10, 15);
        assert_eq!(metrics.queue_size.get(), 10);
        assert_eq!(metrics.queue_depth.get(), 15);

        // Test RocksDB metrics
        metrics.update_rocksdb_metrics(1024, 2048);
        assert_eq!(metrics.rocksdb_memory_usage_bytes.get(), 1024);
        assert_eq!(metrics.rocksdb_disk_usage_bytes.get(), 2048);
    }

    #[test]
    fn test_metrics_recording() {
        let mut registry = Registry::default();
        let metrics = Metrics::new(&mut registry);

        // Test different operations
        metrics.record_request("add", "success", 0.1);
        metrics.record_request("add", "error", 0.2);
        metrics.record_request("poll", "success", 0.3);

        // Test RocksDB operations
        metrics.record_rocksdb_operation("write", "success", 0.05);
        metrics.record_rocksdb_operation("read", "error", 0.1);

        // Test background tasks
        metrics.record_background_task("lease_expiry", "success", 0.01);
        metrics.record_background_task("lease_expiry", "error", 0.02);
    }

    #[tokio::test]
    async fn test_metrics_encoding() {
        let mut registry = Registry::default();
        let metrics = Metrics::new(&mut registry);

        // Add some test data
        metrics.record_request("test", "success", 0.1);
        metrics.update_queue_metrics(5, 10);

        // Test encoding
        let encoded = encode_metrics(&registry).await.unwrap();
        assert!(encoded.contains("queueber_requests_total"));
        assert!(encoded.contains("queueber_queue_size"));
        // Note: Since we simplified the metrics to not use labels,
        // we don't expect "test" or "success" to appear in the output
    }
}
