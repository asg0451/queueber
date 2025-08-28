use prometheus::{
    exponential_buckets, register_counter_vec, register_gauge, register_histogram_vec,
    register_int_counter_vec, register_int_gauge, CounterVec, Gauge, Histogram, HistogramVec,
    IntCounterVec, IntGauge, Registry,
};
use std::sync::LazyLock;
use std::time::Instant;

// Re-export prometheus types for convenience
pub use prometheus::{Encoder, TextEncoder};

/// Global metrics registry - initialized once at startup
pub static METRICS: LazyLock<QueueberMetrics> = LazyLock::new(QueueberMetrics::new);

/// All metrics for the queueber system
pub struct QueueberMetrics {
    /// RPC operation counts by operation type and status
    pub rpc_requests_total: IntCounterVec,
    
    /// RPC operation duration in seconds
    pub rpc_duration_seconds: HistogramVec,
    
    /// Storage operation counts by operation type and status
    pub storage_operations_total: IntCounterVec,
    
    /// Storage operation duration in seconds
    pub storage_duration_seconds: HistogramVec,
    
    /// Background task metrics
    pub background_tasks_total: CounterVec,
    
    /// Queue metrics
    pub queue_items_added_total: IntCounterVec,
    pub queue_items_polled_total: IntCounterVec,
    pub queue_items_removed_total: IntCounterVec,
    pub queue_items_extended_total: IntCounterVec,
    
    /// Active leases
    pub active_leases: IntGauge,
    
    /// Poll coalescing metrics
    pub poll_requests_coalesced: IntCounterVec,
    
    /// RocksDB metrics (these will be updated periodically)
    pub rocksdb_live_sst_files: IntGauge,
    pub rocksdb_total_sst_files_size: Gauge,
    pub rocksdb_estimated_keys: IntGauge,
    pub rocksdb_mem_table_flush_pending: IntGauge,
    pub rocksdb_compaction_pending: IntGauge,
    
    /// Error metrics
    pub errors_total: IntCounterVec,
}

impl QueueberMetrics {
    fn new() -> Self {
        // Duration buckets: 100μs, 500μs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
        let duration_buckets = exponential_buckets(0.0001, 2.5, 12).unwrap();
        
        Self {
            rpc_requests_total: register_int_counter_vec!(
                "queueber_rpc_requests_total",
                "Total number of RPC requests",
                &["operation", "status"]
            ).unwrap(),
            
            rpc_duration_seconds: register_histogram_vec!(
                "queueber_rpc_duration_seconds",
                "Duration of RPC requests in seconds",
                &["operation"],
                duration_buckets.clone()
            ).unwrap(),
            
            storage_operations_total: register_int_counter_vec!(
                "queueber_storage_operations_total",
                "Total number of storage operations",
                &["operation", "status"]
            ).unwrap(),
            
            storage_duration_seconds: register_histogram_vec!(
                "queueber_storage_duration_seconds",
                "Duration of storage operations in seconds",
                &["operation"],
                duration_buckets
            ).unwrap(),
            
            background_tasks_total: register_counter_vec!(
                "queueber_background_tasks_total",
                "Total number of background task executions",
                &["task", "status"]
            ).unwrap(),
            
            queue_items_added_total: register_int_counter_vec!(
                "queueber_queue_items_added_total",
                "Total number of items added to queues",
                &["queue"]
            ).unwrap(),
            
            queue_items_polled_total: register_int_counter_vec!(
                "queueber_queue_items_polled_total",
                "Total number of items polled from queues",
                &["queue"]
            ).unwrap(),
            
            queue_items_removed_total: register_int_counter_vec!(
                "queueber_queue_items_removed_total",
                "Total number of items removed from queues",
                &["queue"]
            ).unwrap(),
            
            queue_items_extended_total: register_int_counter_vec!(
                "queueber_queue_items_extended_total",
                "Total number of lease extensions",
                &["queue", "success"]
            ).unwrap(),
            
            active_leases: register_int_gauge!(
                "queueber_active_leases",
                "Number of currently active leases"
            ).unwrap(),
            
            poll_requests_coalesced: register_int_counter_vec!(
                "queueber_poll_requests_coalesced_total",
                "Total number of poll requests that were coalesced",
                &["status"]
            ).unwrap(),
            
            rocksdb_live_sst_files: register_int_gauge!(
                "queueber_rocksdb_live_sst_files",
                "Number of live SST files in RocksDB"
            ).unwrap(),
            
            rocksdb_total_sst_files_size: register_gauge!(
                "queueber_rocksdb_total_sst_files_size_bytes",
                "Total size of SST files in RocksDB in bytes"
            ).unwrap(),
            
            rocksdb_estimated_keys: register_int_gauge!(
                "queueber_rocksdb_estimated_keys",
                "Estimated number of keys in RocksDB"
            ).unwrap(),
            
            rocksdb_mem_table_flush_pending: register_int_gauge!(
                "queueber_rocksdb_mem_table_flush_pending",
                "Number of pending mem table flushes"
            ).unwrap(),
            
            rocksdb_compaction_pending: register_int_gauge!(
                "queueber_rocksdb_compaction_pending",
                "Number of pending compactions"
            ).unwrap(),
            
            errors_total: register_int_counter_vec!(
                "queueber_errors_total",
                "Total number of errors",
                &["component", "error_type"]
            ).unwrap(),
        }
    }
    
    /// Get the prometheus registry for scraping metrics
    pub fn registry() -> &'static Registry {
        prometheus::default_registry()
    }
}

/// Timer for measuring operation duration with automatic cleanup
pub struct Timer {
    histogram: Histogram,
    start: Instant,
}

impl Timer {
    pub fn new(histogram: Histogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        self.histogram.observe(duration.as_secs_f64());
    }
}

/// Convenience macros for common metric operations with minimal overhead
#[macro_export]
macro_rules! rpc_timer {
    ($operation:literal) => {
        $crate::metrics::Timer::new(
            $crate::metrics::METRICS
                .rpc_duration_seconds
                .with_label_values(&[$operation])
        )
    };
}

#[macro_export]
macro_rules! storage_timer {
    ($operation:literal) => {
        $crate::metrics::Timer::new(
            $crate::metrics::METRICS
                .storage_duration_seconds
                .with_label_values(&[$operation])
        )
    };
}

#[macro_export]
macro_rules! inc_rpc_counter {
    ($operation:literal, $status:literal) => {
        $crate::metrics::METRICS
            .rpc_requests_total
            .with_label_values(&[$operation, $status])
            .inc()
    };
}

#[macro_export]
macro_rules! inc_storage_counter {
    ($operation:literal, $status:literal) => {
        $crate::metrics::METRICS
            .storage_operations_total
            .with_label_values(&[$operation, $status])
            .inc()
    };
}

#[macro_export]
macro_rules! inc_error_counter {
    ($component:literal, $error_type:literal) => {
        $crate::metrics::METRICS
            .errors_total
            .with_label_values(&[$component, $error_type])
            .inc()
    };
}

/// Initialize metrics subsystem (should be called once at startup)
pub fn init() {
    LazyLock::force(&METRICS);
    tracing::info!("Metrics initialized with {} registered metrics", 
        QueueberMetrics::registry().gather().len());
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_initialization() {
        init();
        assert!(!QueueberMetrics::registry().gather().is_empty());
    }
    
    #[tokio::test]
    async fn test_timer() {
        init();
        {
            let _timer = rpc_timer!("test");
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        // Timer should have recorded a measurement
        let metric_families = QueueberMetrics::registry().gather();
        let rpc_duration = metric_families.iter()
            .find(|mf| mf.get_name() == "queueber_rpc_duration_seconds")
            .unwrap();
        assert!(rpc_duration.get_metric()[0].get_histogram().get_sample_count() > 0);
    }
}