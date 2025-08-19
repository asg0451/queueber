use capnp::capability::Promise;
use capnp::message::{Builder, HeapAllocator, TypedReader};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::Storage;
use crate::errors::{Error, Result};
use crate::metrics::SharedMetrics;
use crate::protocol::queue::{
    AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
};

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: Arc<Storage>,
    notify: Arc<Notify>,
    #[allow(dead_code)]
    shutdown_tx: watch::Sender<bool>,
    metrics: Option<SharedMetrics>,
}

impl Server {
    pub fn new(
        storage: Arc<Storage>,
        notify: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
    ) -> Self {
        Self::new_with_metrics(storage, notify, shutdown_tx, None)
    }

    pub fn new_with_metrics(
        storage: Arc<Storage>,
        notify: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
        metrics: Option<SharedMetrics>,
    ) -> Self {
        let bg_storage = Arc::clone(&storage);
        let bg_notify = Arc::clone(&notify);
        let lease_expiry_shutdown = shutdown_tx.clone();
        let visibility_wakeup_shutdown = shutdown_tx.clone();
        let bg_metrics = metrics.clone();

        // Background task to expire leases periodically
        tokio::spawn(async move {
            loop {
                let start_time = std::time::Instant::now();
                let st = Arc::clone(&bg_storage);
                let result = tokio::task::spawn_blocking(move || st.expire_due_leases()).await;

                let duration = start_time.elapsed().as_secs_f64();
                #[allow(clippy::collapsible_if)]
                if let Some(metrics) = &bg_metrics {
                    if let Ok(metrics_guard) = metrics.try_write() {
                        match &result {
                            Ok(Ok(n)) => {
                                if *n > 0 {
                                    bg_notify.notify_one();
                                    metrics_guard.record_background_task(
                                        "lease_expiry",
                                        "success",
                                        duration,
                                    );
                                } else {
                                    metrics_guard.record_background_task(
                                        "lease_expiry",
                                        "no_expirations",
                                        duration,
                                    );
                                }
                            }
                            Ok(Err(_e)) => {
                                metrics_guard.record_background_task(
                                    "lease_expiry",
                                    "error",
                                    duration,
                                );
                                let _ = lease_expiry_shutdown.send(true);
                                break;
                            }
                            Err(_join_err) => {
                                metrics_guard.record_background_task(
                                    "lease_expiry",
                                    "join_error",
                                    duration,
                                );
                                let _ = lease_expiry_shutdown.send(true);
                                break;
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        // Background task to wake pollers when any invisible message becomes visible.
        let vis_storage = Arc::clone(&storage);
        let vis_notify = Arc::clone(&notify);
        let vis_metrics = metrics.clone();
        tokio::spawn(async move {
            loop {
                let start_time = std::time::Instant::now();
                // Peek earliest visibility timestamp from RocksDB (blocking)
                let next_vis_opt = tokio::task::spawn_blocking({
                    let st = Arc::clone(&vis_storage);
                    move || st.peek_next_visibility_ts_secs()
                })
                .await;

                let duration = start_time.elapsed().as_secs_f64();
                #[allow(clippy::collapsible_if)]
                if let Some(metrics) = &vis_metrics {
                    if let Ok(metrics_guard) = metrics.try_write() {
                        match &next_vis_opt {
                            Ok(Ok(Some(_ts_secs))) => {
                                metrics_guard.record_background_task(
                                    "visibility_check",
                                    "found",
                                    duration,
                                );
                            }
                            Ok(Ok(None)) => {
                                metrics_guard.record_background_task(
                                    "visibility_check",
                                    "no_items",
                                    duration,
                                );
                            }
                            Ok(Err(_e)) => {
                                metrics_guard.record_background_task(
                                    "visibility_check",
                                    "error",
                                    duration,
                                );
                                let _ = visibility_wakeup_shutdown.send(true);
                                break;
                            }
                            Err(_join_err) => {
                                metrics_guard.record_background_task(
                                    "visibility_check",
                                    "join_error",
                                    duration,
                                );
                                let _ = visibility_wakeup_shutdown.send(true);
                                break;
                            }
                        }
                    }
                }

                match next_vis_opt {
                    Ok(Ok(Some(ts_secs))) => {
                        // Compute duration until visibility; if already visible, notify now.
                        let now_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(ts_secs);
                        if ts_secs <= now_secs {
                            vis_notify.notify_one();
                            // Avoid busy loop; small sleep before checking again.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        } else {
                            let sleep_dur = std::time::Duration::from_secs(ts_secs - now_secs);
                            tokio::time::sleep(sleep_dur).await;
                            vis_notify.notify_one();
                        }
                    }
                    Ok(Ok(None)) => {
                        // No items; back off
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    // TODO: can this be prettier?
                    Ok(Err(e)) => {
                        tracing::error!("peek_next_visibility_ts_secs: {}", e);
                        let _ = visibility_wakeup_shutdown.send(true);
                        break;
                    }
                    Err(join_err) => {
                        tracing::error!("peek_next_visibility_ts_secs: {}", join_err);
                        let _ = visibility_wakeup_shutdown.send(true);
                        break;
                    }
                }
            }
        });

        // Background task to update metrics periodically
        if let Some(metrics) = &metrics {
            let metrics_storage = Arc::clone(&storage);
            let metrics_clone = metrics.clone();
            tokio::spawn(async move {
                loop {
                    let start_time = std::time::Instant::now();

                    // Update RocksDB metrics
                    metrics_storage.update_rocksdb_metrics();

                    // Update queue metrics
                    metrics_storage.update_queue_metrics();

                    let duration = start_time.elapsed().as_secs_f64();
                    if let Ok(metrics_guard) = metrics_clone.try_write() {
                        metrics_guard.record_background_task("metrics_update", "success", duration);
                    }

                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            });
        }

        Self {
            storage,
            notify,
            shutdown_tx,
            metrics,
        }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let start_time = std::time::Instant::now();
        let metrics = self.metrics.clone();

        let req = params.get()?;
        let req = req.get_req()?;
        let items = req.get_items()?;

        // Generate ids upfront and copy request data into owned memory so we can move
        // it into a blocking task (capnp readers are not Send).
        let ids: Vec<Vec<u8>> = items
            .iter()
            .map(|_| uuid::Uuid::now_v7().as_bytes().to_vec())
            .collect();

        let items_owned = items
            .iter()
            .map(|item| -> Result<_> {
                let contents = item.get_contents()?.to_vec();
                let vis = item.get_visibility_timeout_secs();
                Ok((contents, vis))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(Into::<capnp::Error>::into)?;

        let any_immediately_visible = items_owned.iter().any(|(_, vis)| *vis == 0);

        let storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);
        let ids_for_resp = ids.clone();

        Promise::from_future(async move {
            let result = async {
                // Offload RocksDB work to the blocking thread pool.
                tokio::task::spawn_blocking(move || {
                    let iter = ids
                        .iter()
                        .map(|id| id.as_slice())
                        .zip(items_owned.iter().map(|(c, v)| (c.as_slice(), *v)));
                    storage.add_available_items_from_parts(iter)
                })
                .await
                .map_err(Into::<Error>::into)??;

                if any_immediately_visible {
                    notify.notify_one();
                }

                // Build the response on the RPC thread.
                let mut ids_builder = results
                    .get()
                    .init_resp()
                    .init_ids(ids_for_resp.len() as u32);
                for (i, id) in ids_for_resp.iter().enumerate() {
                    ids_builder.set(i as u32, id);
                }
                Ok(())
            }
            .await;

            // Record metrics
            if let Some(metrics) = metrics {
                let duration = start_time.elapsed().as_secs_f64();
                let status = if result.is_ok() { "success" } else { "error" };
                if let Ok(metrics_guard) = metrics.try_write() {
                    metrics_guard.record_request("add", status, duration);
                }
            }

            result
        })
    }

    fn remove(
        &mut self,
        params: RemoveParams,
        mut results: RemoveResults,
    ) -> Promise<(), capnp::Error> {
        let start_time = std::time::Instant::now();
        let metrics = self.metrics.clone();

        let req = params.get()?.get_req()?;
        let id = req.get_id()?;
        let lease_bytes = req.get_lease()?;

        if lease_bytes.len() != 16 {
            return Promise::err(capnp::Error::failed("invalid lease length".to_string()));
        }
        let mut lease: [u8; 16] = [0; 16];
        lease.copy_from_slice(lease_bytes);

        let result = self
            .storage
            .remove_in_progress_item(id, &lease)
            .map_err(Into::into);

        // Record metrics
        if let Some(metrics) = metrics {
            let duration = start_time.elapsed().as_secs_f64();
            let status = if result.is_ok() { "success" } else { "error" };
            if let Ok(metrics_guard) = metrics.try_write() {
                metrics_guard.record_request("remove", status, duration);
            }
        }

        match result {
            Ok(removed) => {
                results.get().init_resp().set_removed(removed);
                Promise::ok(())
            }
            Err(e) => Promise::err(e),
        }
    }

    fn poll(&mut self, params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        let start_time = std::time::Instant::now();
        let metrics = self.metrics.clone();
        let storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);

        Promise::from_future(async move {
            let result = async {
                let req = params.get()?.get_req()?;
                let lease_validity_secs = req.get_lease_validity_secs();
                if lease_validity_secs == 0 {
                    return Err(capnp::Error::failed(
                        "invariant: leaseValiditySecs must be > 0".to_string(),
                    ));
                }
                let num_items = match req.get_num_items() {
                    0 => 1,
                    n => n as usize,
                };
                let timeout_secs = req.get_timeout_secs();

                let deadline = if timeout_secs > 0 {
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs))
                } else {
                    None
                };

                loop {
                    let (lease, items) = storage
                        .get_next_available_entries_with_lease(num_items, lease_validity_secs)?;
                    if !items.is_empty() {
                        write_poll_response(&lease, items, &mut results)?;
                        return Ok(());
                    }

                    match deadline {
                        Some(d) => {
                            let now = std::time::Instant::now();
                            if now >= d {
                                return Ok(());
                            }
                            let remaining = d - now;
                            tokio::select! {
                                _ = notify.notified() => {},
                                _ = tokio::time::sleep(remaining) => { return Ok(()); },
                            }
                        }
                        None => {
                            notify.notified().await;
                        }
                    }
                }
            }
            .await;

            // Record metrics
            if let Some(metrics) = metrics {
                let duration = start_time.elapsed().as_secs_f64();
                let status = if result.is_ok() { "success" } else { "error" };
                if let Ok(metrics_guard) = metrics.try_write() {
                    metrics_guard.record_request("poll", status, duration);
                }
            }

            result
        })
    }
}

fn write_poll_response(
    lease: &[u8; 16],
    items: Vec<TypedReader<Builder<HeapAllocator>, crate::protocol::polled_item::Owned>>,
    results: &mut crate::protocol::queue::PollResults,
) -> Result<()> {
    let mut resp = results.get().init_resp();
    resp.set_lease(lease);

    let mut items_builder = resp.init_items(items.len() as u32);
    for (i, typed_polled_item) in items.into_iter().enumerate() {
        let item_reader = typed_polled_item.get()?;
        let mut out_item = items_builder.reborrow().get(i as u32);
        out_item.set_contents(item_reader.get_contents()?);
        out_item.set_id(item_reader.get_id()?);
    }
    Ok(())
}
