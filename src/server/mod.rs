use capnp::capability::Promise;
use capnp::message::{Builder, HeapAllocator, TypedReader};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Duration;

use crate::errors::Result;
use crate::protocol::queue::{
    AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
};
use crate::storage::{RetriedStorage, Storage};
mod coalescer;
use coalescer::PollCoalescer;

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: Arc<RetriedStorage<Storage>>,
    notify: Arc<Notify>,
    #[allow(dead_code)]
    shutdown_tx: watch::Sender<bool>,
    coalescer: Arc<PollCoalescer>,
}

impl Server {
    pub fn new(
        storage: Arc<RetriedStorage<Storage>>,
        notify: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
    ) -> Self {
        let coalescer = Arc::new(PollCoalescer::new(Arc::clone(&storage)));
        Self {
            storage,
            notify,
            shutdown_tx,
            coalescer,
        }
    }

    /// Build a server with a custom coalescing window (ms) for polling.
    pub fn new_with_coalescing_window_ms(
        storage: Arc<RetriedStorage<Storage>>,
        notify: Arc<Notify>,
        shutdown_tx: watch::Sender<bool>,
        batch_window_ms: u64,
    ) -> Self {
        let coalescer = Arc::new(PollCoalescer::with_batch_window_ms(
            Arc::clone(&storage),
            batch_window_ms,
        ));
        Self {
            storage,
            notify,
            shutdown_tx,
            coalescer,
        }
    }
}

const BACKGROUND_LEASE_EXPIRY_INTERVAL_SECS: u64 = 1;

pub fn spawn_background_tasks(
    storage: Arc<RetriedStorage<Storage>>,
    notify: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
) {
    let bg_storage = Arc::clone(&storage);
    let bg_notify = Arc::clone(&notify);
    let lease_expiry_shutdown = shutdown_tx.clone();
    let visibility_wakeup_shutdown = shutdown_tx.clone();

    // Background task to expire leases periodically
    tokio::task::Builder::new()
        .name("lease_expiry")
        .spawn(async move {
            let st = Arc::clone(&bg_storage);
            loop {
                tokio::time::sleep(Duration::from_secs(BACKGROUND_LEASE_EXPIRY_INTERVAL_SECS))
                    .await;
                match st.expire_due_leases().await {
                    Ok(n) => {
                        if n > 0 {
                            bg_notify.notify_one();
                        }
                    }
                    Err(e) => {
                        tracing::error!("expire_due_leases: {}", e);
                        let _ = lease_expiry_shutdown.send(true);
                        break;
                    }
                }
            }
        })
        .unwrap();

    // Background task to wake pollers when any invisible message becomes visible.
    let vis_storage = Arc::clone(&storage);
    let vis_notify = Arc::clone(&notify);
    tokio::task::Builder::new()
        .name("visibility_wakeup")
        .spawn(async move {
            loop {
                // Peek earliest visibility timestamp from RocksDB (blocking)
                let next_vis_opt = vis_storage.peek_next_visibility_ts_secs().await;

                match next_vis_opt {
                    Ok(Some(ts_secs)) => {
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
                    Ok(None) => {
                        // No items; back off
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Err(e) => {
                        tracing::error!("peek_next_visibility_ts_secs: {}", e);
                        let _ = visibility_wakeup_shutdown.send(true);
                        break;
                    }
                }
            }
        })
        .unwrap();
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let req = params.get()?;
        let req = req.get_req()?;
        let items = req.get_items()?;

        // Generate ids upfront and copy request data into owned memory so we can move
        // it into a blocking task (capnp readers are not Send).
        // Store ids inline as [u8;16] to avoid per-id heap allocations.
        let ids: Vec<[u8; 16]> = items
            .iter()
            .map(|_| uuid::Uuid::now_v7().into_bytes())
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

        Promise::from_future(async move {
            // Offload RocksDB work to the blocking thread pool.
            let iter = ids
                .iter()
                .map(|id| id.as_slice())
                .zip(items_owned.iter().map(|(c, v)| (c.as_slice(), *v)));
            storage.add_available_items_from_parts(iter).await?;

            if any_immediately_visible {
                notify.notify_one();
            }

            // Build the response on the RPC thread.
            let mut ids_builder = results.get().init_resp().init_ids(ids.len() as u32);
            for (i, id) in ids.iter().enumerate() {
                ids_builder.set(i as u32, id);
            }
            Ok(())
        })
    }

    fn remove(
        &mut self,
        params: RemoveParams,
        mut results: RemoveResults,
    ) -> Promise<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let id = req.get_id()?;
        let lease_bytes = req.get_lease()?;

        if lease_bytes.len() != 16 {
            return Promise::err(capnp::Error::failed("invalid lease length".to_string()));
        }
        let mut lease: [u8; 16] = [0; 16];
        lease.copy_from_slice(lease_bytes);

        // Own the id bytes so they can be moved across await boundaries with minimal copying.
        let id_owned: std::sync::Arc<[u8]> = std::sync::Arc::from(id);

        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let removed = storage
                .remove_in_progress_item(id_owned.as_ref(), &lease)
                .await?;

            results.get().init_resp().set_removed(removed);
            Ok(())
        })
    }

    fn poll(&mut self, params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        let _storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);
        let coalescer = Arc::clone(&self.coalescer);

        Promise::from_future(async move {
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
                match coalescer.poll(num_items, lease_validity_secs).await {
                    Ok(Some((lease, items))) => {
                        write_poll_response(&lease, items, &mut results)?;
                        return Ok(());
                    }
                    Ok(None) => {}
                    Err(e) => return Err(capnp::Error::failed(e.to_string())),
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
        })
    }

    fn extend(
        &mut self,
        params: crate::protocol::queue::ExtendParams,
        mut results: crate::protocol::queue::ExtendResults,
    ) -> Promise<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let lease_bytes = req.get_lease()?;
        let lease_validity_secs = req.get_lease_validity_secs();
        if lease_validity_secs == 0 {
            return Promise::err(capnp::Error::failed(
                "invariant: leaseValiditySecs must be > 0".to_string(),
            ));
        }
        if lease_bytes.len() != 16 {
            return Promise::err(capnp::Error::failed("invalid lease length".to_string()));
        }
        let mut lease: [u8; 16] = [0; 16];
        lease.copy_from_slice(lease_bytes);

        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let extended = storage.extend_lease(&lease, lease_validity_secs).await?;
            results.get().init_resp().set_extended(extended);
            Ok(())
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
