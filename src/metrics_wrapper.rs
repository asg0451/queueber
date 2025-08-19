use crate::metrics::SharedMetrics;

/// A wrapper that provides clean metrics collection methods
#[derive(Clone)]
pub struct MetricsWrapper {
    metrics: Option<SharedMetrics>,
}

impl MetricsWrapper {
    pub fn new(metrics: Option<SharedMetrics>) -> Self {
        Self { metrics }
    }

    pub fn none() -> Self {
        Self { metrics: None }
    }

    /// Record a request with timing
    pub fn record_request(&self, operation: &str, status: &str, duration: f64) {
        if let Some(metrics) = &self.metrics
            && let Ok(guard) = metrics.try_write() {
            guard.record_request(operation, status, duration);
        }
    }

    /// Record a RocksDB operation with timing
    pub fn record_rocksdb_operation(&self, operation: &str, status: &str, duration: f64) {
        if let Some(metrics) = &self.metrics
            && let Ok(guard) = metrics.try_write() {
            guard.record_rocksdb_operation(operation, status, duration);
        }
    }

    /// Record a background task with timing
    pub fn record_background_task(&self, task: &str, status: &str, duration: f64) {
        if let Some(metrics) = &self.metrics
            && let Ok(guard) = metrics.try_write() {
            guard.record_background_task(task, status, duration);
        }
    }

    /// Update queue metrics
    pub fn update_queue_metrics(&self, size: i64, depth: i64) {
        if let Some(metrics) = &self.metrics
            && let Ok(guard) = metrics.try_write() {
            guard.update_queue_metrics(size, depth);
        }
    }

    /// Update RocksDB metrics
    pub fn update_rocksdb_metrics(&self, memory_usage: i64, disk_usage: i64) {
        if let Some(metrics) = &self.metrics
            && let Ok(guard) = metrics.try_write() {
            guard.update_rocksdb_metrics(memory_usage, disk_usage);
        }
    }

    /// Time a RocksDB operation and record it
    pub fn time_rocksdb_operation<F, T, E>(&self, operation: &str, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        let start = std::time::Instant::now();
        let result = f();
        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        self.record_rocksdb_operation(operation, status, duration);
        result
    }

    /// Time a request and record it
    pub async fn time_request<F, Fut, T, E>(&self, operation: &str, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let start = std::time::Instant::now();
        let result = f().await;
        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        self.record_request(operation, status, duration);
        result
    }

    /// Time a background task and record it
    pub async fn time_background_task<F, Fut, T, E>(&self, task: &str, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let start = std::time::Instant::now();
        let result = f().await;
        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        self.record_background_task(task, status, duration);
        result
    }
}