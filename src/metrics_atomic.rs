use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;

/// Atomic-based metrics that use local copies and atomics for better performance
#[derive(Clone)]
pub struct AtomicMetrics {
    // SLI Metrics
    requests_total: Arc<AtomicU64>,
    request_duration_seconds_total: Arc<AtomicU64>,
    queue_size: Arc<AtomicI64>,
    queue_depth: Arc<AtomicI64>,
    lease_expirations_total: Arc<AtomicU64>,
    failed_requests_total: Arc<AtomicU64>,

    // RocksDB Metrics
    rocksdb_operations_total: Arc<AtomicU64>,
    rocksdb_operation_duration_seconds_total: Arc<AtomicU64>,
    rocksdb_memory_usage_bytes: Arc<AtomicI64>,
    rocksdb_disk_usage_bytes: Arc<AtomicI64>,

    // Background Task Metrics
    background_task_runs_total: Arc<AtomicU64>,
    background_task_duration_seconds_total: Arc<AtomicU64>,
    background_task_errors_total: Arc<AtomicU64>,
}

impl Default for AtomicMetrics {
    fn default() -> Self {
        Self {
            requests_total: Arc::new(AtomicU64::new(0)),
            request_duration_seconds_total: Arc::new(AtomicU64::new(0)),
            queue_size: Arc::new(AtomicI64::new(0)),
            queue_depth: Arc::new(AtomicI64::new(0)),
            lease_expirations_total: Arc::new(AtomicU64::new(0)),
            failed_requests_total: Arc::new(AtomicU64::new(0)),
            rocksdb_operations_total: Arc::new(AtomicU64::new(0)),
            rocksdb_operation_duration_seconds_total: Arc::new(AtomicU64::new(0)),
            rocksdb_memory_usage_bytes: Arc::new(AtomicI64::new(0)),
            rocksdb_disk_usage_bytes: Arc::new(AtomicI64::new(0)),
            background_task_runs_total: Arc::new(AtomicU64::new(0)),
            background_task_duration_seconds_total: Arc::new(AtomicU64::new(0)),
            background_task_errors_total: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl AtomicMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a request with timing (duration in seconds)
    pub fn record_request(&self, _operation: &str, status: &str, duration: f64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.request_duration_seconds_total.fetch_add(
            (duration * 1_000_000_000.0) as u64, // Convert to nanoseconds for precision
            Ordering::Relaxed,
        );
        if status == "error" {
            self.failed_requests_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a RocksDB operation with timing
    pub fn record_rocksdb_operation(&self, _operation: &str, status: &str, duration: f64) {
        self.rocksdb_operations_total.fetch_add(1, Ordering::Relaxed);
        self.rocksdb_operation_duration_seconds_total.fetch_add(
            (duration * 1_000_000_000.0) as u64, // Convert to nanoseconds for precision
            Ordering::Relaxed,
        );
        if status == "error" {
            self.failed_requests_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a background task with timing
    pub fn record_background_task(&self, _task: &str, status: &str, duration: f64) {
        self.background_task_runs_total.fetch_add(1, Ordering::Relaxed);
        self.background_task_duration_seconds_total.fetch_add(
            (duration * 1_000_000_000.0) as u64, // Convert to nanoseconds for precision
            Ordering::Relaxed,
        );
        if status == "error" {
            self.background_task_errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Update queue metrics
    pub fn update_queue_metrics(&self, size: i64, depth: i64) {
        self.queue_size.store(size, Ordering::Relaxed);
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Update RocksDB metrics
    pub fn update_rocksdb_metrics(&self, memory_usage: i64, disk_usage: i64) {
        self.rocksdb_memory_usage_bytes.store(memory_usage, Ordering::Relaxed);
        self.rocksdb_disk_usage_bytes.store(disk_usage, Ordering::Relaxed);
    }

    /// Record lease expiration
    pub fn record_lease_expiration(&self) {
        self.lease_expirations_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of all metrics for encoding
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            request_duration_seconds_total: self.request_duration_seconds_total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            queue_size: self.queue_size.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            lease_expirations_total: self.lease_expirations_total.load(Ordering::Relaxed),
            failed_requests_total: self.failed_requests_total.load(Ordering::Relaxed),
            rocksdb_operations_total: self.rocksdb_operations_total.load(Ordering::Relaxed),
            rocksdb_operation_duration_seconds_total: self.rocksdb_operation_duration_seconds_total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            rocksdb_memory_usage_bytes: self.rocksdb_memory_usage_bytes.load(Ordering::Relaxed),
            rocksdb_disk_usage_bytes: self.rocksdb_disk_usage_bytes.load(Ordering::Relaxed),
            background_task_runs_total: self.background_task_runs_total.load(Ordering::Relaxed),
            background_task_duration_seconds_total: self.background_task_duration_seconds_total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            background_task_errors_total: self.background_task_errors_total.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of metrics for encoding to Prometheus format
pub struct MetricsSnapshot {
    pub requests_total: u64,
    pub request_duration_seconds_total: f64,
    pub queue_size: i64,
    pub queue_depth: i64,
    pub lease_expirations_total: u64,
    pub failed_requests_total: u64,
    pub rocksdb_operations_total: u64,
    pub rocksdb_operation_duration_seconds_total: f64,
    pub rocksdb_memory_usage_bytes: i64,
    pub rocksdb_disk_usage_bytes: i64,
    pub background_task_runs_total: u64,
    pub background_task_duration_seconds_total: f64,
    pub background_task_errors_total: u64,
}

/// Create atomic metrics
pub fn create_atomic_metrics() -> AtomicMetrics {
    AtomicMetrics::new()
}

/// Encode metrics snapshot to Prometheus text format
pub fn encode_atomic_metrics(snapshot: &MetricsSnapshot) -> Result<String, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    
    // Write metrics in Prometheus text format
    writeln!(buffer, "# HELP queueber_requests_total Total number of requests")?;
    writeln!(buffer, "# TYPE queueber_requests_total counter")?;
    writeln!(buffer, "queueber_requests_total {}", snapshot.requests_total)?;
    
    writeln!(buffer, "# HELP queueber_request_duration_seconds_total Total request duration in seconds")?;
    writeln!(buffer, "# TYPE queueber_request_duration_seconds_total counter")?;
    writeln!(buffer, "queueber_request_duration_seconds_total {}", snapshot.request_duration_seconds_total)?;
    
    writeln!(buffer, "# HELP queueber_queue_size Current number of available items")?;
    writeln!(buffer, "# TYPE queueber_queue_size gauge")?;
    writeln!(buffer, "queueber_queue_size {}", snapshot.queue_size)?;
    
    writeln!(buffer, "# HELP queueber_queue_depth Total queue depth (available + leased items)")?;
    writeln!(buffer, "# TYPE queueber_queue_depth gauge")?;
    writeln!(buffer, "queueber_queue_depth {}", snapshot.queue_depth)?;
    
    writeln!(buffer, "# HELP queueber_lease_expirations_total Total number of lease expirations")?;
    writeln!(buffer, "# TYPE queueber_lease_expirations_total counter")?;
    writeln!(buffer, "queueber_lease_expirations_total {}", snapshot.lease_expirations_total)?;
    
    writeln!(buffer, "# HELP queueber_failed_requests_total Total number of failed requests")?;
    writeln!(buffer, "# TYPE queueber_failed_requests_total counter")?;
    writeln!(buffer, "queueber_failed_requests_total {}", snapshot.failed_requests_total)?;
    
    writeln!(buffer, "# HELP queueber_rocksdb_operations_total Total number of RocksDB operations")?;
    writeln!(buffer, "# TYPE queueber_rocksdb_operations_total counter")?;
    writeln!(buffer, "queueber_rocksdb_operations_total {}", snapshot.rocksdb_operations_total)?;
    
    writeln!(buffer, "# HELP queueber_rocksdb_operation_duration_seconds_total Total RocksDB operation duration in seconds")?;
    writeln!(buffer, "# TYPE queueber_rocksdb_operation_duration_seconds_total counter")?;
    writeln!(buffer, "queueber_rocksdb_operation_duration_seconds_total {}", snapshot.rocksdb_operation_duration_seconds_total)?;
    
    writeln!(buffer, "# HELP queueber_rocksdb_memory_usage_bytes RocksDB memory usage in bytes")?;
    writeln!(buffer, "# TYPE queueber_rocksdb_memory_usage_bytes gauge")?;
    writeln!(buffer, "queueber_rocksdb_memory_usage_bytes {}", snapshot.rocksdb_memory_usage_bytes)?;
    
    writeln!(buffer, "# HELP queueber_rocksdb_disk_usage_bytes RocksDB disk usage in bytes")?;
    writeln!(buffer, "# TYPE queueber_rocksdb_disk_usage_bytes gauge")?;
    writeln!(buffer, "queueber_rocksdb_disk_usage_bytes {}", snapshot.rocksdb_disk_usage_bytes)?;
    
    writeln!(buffer, "# HELP queueber_background_task_runs_total Total number of background task runs")?;
    writeln!(buffer, "# TYPE queueber_background_task_runs_total counter")?;
    writeln!(buffer, "queueber_background_task_runs_total {}", snapshot.background_task_runs_total)?;
    
    writeln!(buffer, "# HELP queueber_background_task_duration_seconds_total Total background task duration in seconds")?;
    writeln!(buffer, "# TYPE queueber_background_task_duration_seconds_total counter")?;
    writeln!(buffer, "queueber_background_task_duration_seconds_total {}", snapshot.background_task_duration_seconds_total)?;
    
    writeln!(buffer, "# HELP queueber_background_task_errors_total Total number of background task errors")?;
    writeln!(buffer, "# TYPE queueber_background_task_errors_total counter")?;
    writeln!(buffer, "queueber_background_task_errors_total {}", snapshot.background_task_errors_total)?;
    
    Ok(String::from_utf8(buffer)?)
}

use std::io::Write;