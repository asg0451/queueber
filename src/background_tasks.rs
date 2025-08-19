use std::sync::Arc;
use tokio::sync::{Notify, watch};
use tokio::time::Duration;
use crate::Storage;
use crate::metrics_wrapper_atomic::AtomicMetricsWrapper;

pub struct BackgroundTasks {
    storage: Arc<Storage>,
    notify: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
    metrics: AtomicMetricsWrapper,
}

impl BackgroundTasks {
    pub fn new(
        storage: Arc<Storage>,
        notify: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
        metrics: AtomicMetricsWrapper,
    ) -> Self {
        Self {
            storage,
            notify,
            shutdown_tx,
            metrics,
        }
    }

    pub fn spawn_all(self) {
        self.spawn_lease_expiry_task();
        self.spawn_visibility_check_task();
        self.spawn_metrics_update_task();
    }

    fn spawn_lease_expiry_task(&self) {
        let storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);
        let shutdown_tx = self.shutdown_tx.clone();
        let metrics = self.metrics.clone();

        tokio::task::Builder::new()
            .name("lease_expiry")
            .spawn(async move {
                loop {
                    let st = Arc::clone(&storage);
                    match tokio::task::Builder::new()
                        .name("expire_due_leases")
                        .spawn_blocking(move || st.expire_due_leases())
                        .unwrap()
                        .await
                    {
                        Ok(Ok(n)) => {
                            if n > 0 {
                                notify.notify_one();
                            }
                            // Record metrics
                            metrics.record_background_task("lease_expiry", "success", 0.0);
                        }
                        Ok(Err(_e)) => {
                            metrics.record_background_task("lease_expiry", "error", 0.0);
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                        Err(_join_err) => {
                            metrics.record_background_task("lease_expiry", "join_error", 0.0);
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
            .unwrap();
    }

    fn spawn_visibility_check_task(&self) {
        let storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);
        let shutdown_tx = self.shutdown_tx.clone();
        let metrics = self.metrics.clone();

        tokio::task::Builder::new()
            .name("visibility_wakeup")
            .spawn(async move {
                loop {
                    // Peek earliest visibility timestamp from RocksDB (blocking)
                    let next_vis_opt = tokio::task::Builder::new()
                        .name("peek_next_visibility_ts_secs")
                        .spawn_blocking({
                            let st = Arc::clone(&storage);
                            move || st.peek_next_visibility_ts_secs()
                        })
                        .unwrap()
                        .await;

                    match next_vis_opt {
                        Ok(Ok(Some(ts_secs))) => {
                            // Compute duration until visibility; if already visible, notify now.
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(ts_secs);
                            if ts_secs <= now_secs {
                                notify.notify_one();
                                // Avoid busy loop; small sleep before checking again.
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            } else {
                                let sleep_dur = std::time::Duration::from_secs(ts_secs - now_secs);
                                tokio::time::sleep(sleep_dur).await;
                                notify.notify_one();
                            }
                            metrics.record_background_task("visibility_check", "found", 0.0);
                        }
                        Ok(Ok(None)) => {
                            // No items; back off
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            metrics.record_background_task("visibility_check", "no_items", 0.0);
                        }
                        // TODO: can this be prettier?
                        Ok(Err(e)) => {
                            tracing::error!("peek_next_visibility_ts_secs: {}", e);
                            metrics.record_background_task("visibility_check", "error", 0.0);
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                        Err(join_err) => {
                            tracing::error!("peek_next_visibility_ts_secs: {}", join_err);
                            metrics.record_background_task("visibility_check", "join_error", 0.0);
                            let _ = shutdown_tx.send(true);
                            break;
                        }
                    }
                }
            })
            .unwrap();
    }

    fn spawn_metrics_update_task(&self) {
        let storage = Arc::clone(&self.storage);
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                // Update RocksDB metrics
                storage.update_rocksdb_metrics();
                
                // Update queue metrics
                storage.update_queue_metrics();
                
                metrics.record_background_task("metrics_update", "success", 0.0);
                
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
}