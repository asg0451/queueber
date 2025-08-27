use capnp::capability::Promise;
use capnp::message::{Builder, HeapAllocator, TypedReader};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{oneshot, watch};
use tokio::time::Duration;

use crate::errors::Result;
type PolledItems = Vec<
    TypedReader<
        Builder<HeapAllocator>,
        crate::protocol::polled_item::Owned,
    >,
>;
type CoalescedPollResult = crate::errors::Result<([u8; 16], PolledItems)>;

use crate::protocol::queue::{
    AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
};
use crate::storage::{RetriedStorage, Storage};

#[derive(Clone, Copy)]
struct PollCoalescingConfig {
    max_batch_size: usize,
    max_batch_items: usize,
    batch_window_ms: u64,
}

impl Default for PollCoalescingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_batch_items: 512,
            batch_window_ms: 1,
        }
    }
}

struct PendingPoll {
    num_items: usize,
    lease_validity_secs: u64,
    tx: oneshot::Sender<CoalescedPollResult>,
}

struct PollCoalescer {
    storage: Arc<RetriedStorage<Storage>>,
    cfg: PollCoalescingConfig,
    state: Arc<tokio::sync::Mutex<Vec<PendingPoll>>>, // pending batch
    wake: Arc<Notify>,
    started: std::sync::atomic::AtomicBool,
}

impl PollCoalescer {
    fn new(storage: Arc<RetriedStorage<Storage>>) -> Self {
        Self {
            storage,
            cfg: PollCoalescingConfig::default(),
            state: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            wake: Arc::new(Notify::new()),
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn spawn_batcher(&self) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.cfg;
        let state = Arc::clone(&self.state);
        let wake = Arc::clone(&self.wake);
        tokio::task::Builder::new()
            .name("poll_coalescer")
            .spawn_local(async move {
                loop {
                    // Wait until there is at least one request
                    if state.lock().await.is_empty() {
                        wake.notified().await;
                    }

                    // Small window to gather more
                    tokio::time::sleep(Duration::from_millis(cfg.batch_window_ms)).await;

                    // Drain up to max_batch_size
                    let batch: Vec<PendingPoll> = {
                        let mut guard = state.lock().await;
                        if guard.is_empty() {
                            Vec::new()
                        } else {
                            let take_n = guard.len().min(cfg.max_batch_size);
                            guard.drain(0..take_n).collect()
                        }
                    };

                    if batch.is_empty() {
                        continue;
                    }

                    // Compute total items and effective lease duration (max across requests)
                    let mut total_items: usize = 0;
                    let mut lease_secs: u64 = 0;
                    for p in &batch {
                        total_items = total_items.saturating_add(p.num_items);
                        lease_secs = lease_secs.max(p.lease_validity_secs);
                    }
                    total_items = total_items.min(cfg.max_batch_items);

                    // If no items requested (shouldn't happen), respond empty
                    if total_items == 0 {
                        for p in batch {
                            let _ = p.tx.send(Ok(([0u8; 16], Vec::new())));
                        }
                        continue;
                    }

                    // Execute single storage poll for the sum
                    let (lease, items) = match storage
                        .get_next_available_entries_with_lease(total_items, lease_secs)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            // Propagate the error to all requesters by converting to capnp error string
                            let msg = e.to_string();
                            for p in batch {
                                let _ = p.tx.send(Err(crate::errors::Error::Other {
                                    source: Box::new(std::io::Error::other(
                                        msg.clone(),
                                    )),
                                    backtrace: std::backtrace::Backtrace::capture(),
                                }));
                            }
                            continue;
                        }
                    };

                    // Distribute round-robin up to each requester’s num_items
                    let mut distributed: Vec<PolledItems> =
                        (0..batch.len()).map(|_| Vec::new()).collect();
                    let mut idx = 0usize;
                    for item in items.into_iter() {
                        // Find next requester with remaining capacity
                        let mut attempts = 0usize;
                        loop {
                            let target = idx % batch.len();
                            if distributed[target].len() < batch[target].num_items {
                                distributed[target].push(item);
                                idx = idx.wrapping_add(1);
                                break;
                            } else {
                                idx = idx.wrapping_add(1);
                                attempts = attempts.saturating_add(1);
                                if attempts >= batch.len() {
                                    // all full
                                    break;
                                }
                            }
                        }
                    }

                    for (pos, p) in batch.into_iter().enumerate() {
                        let items_for_req = std::mem::take(&mut distributed[pos]);
                        // If requester received zero items, return empty set with nil lease to align with outer loop behavior
                        if items_for_req.is_empty() {
                            let _ = p.tx.send(Ok(([0u8; 16], Vec::new())));
                        } else {
                            let _ = p.tx.send(Ok((lease, items_for_req)));
                        }
                    }
                }
            })
            .expect("spawn poll coalescer");
    }

    async fn poll(
        &self,
        num_items: usize,
        lease_validity_secs: u64,
    ) -> crate::errors::Result<([u8; 16], PolledItems)> {
        // Ensure batcher is running; safe to call concurrently
        if !self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
            // Best-effort spawn; if this panics due to not being inside a LocalSet,
            // coalescing will be effectively disabled for this server instance.
            // In our server, RPC handlers run on a LocalSet, so this is fine.
            self.spawn_batcher();
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.state.lock().await;
            guard.push(PendingPoll {
                num_items: if num_items == 0 { 1 } else { num_items },
                lease_validity_secs,
                tx,
            });
        }
        self.wake.notify_one();
        match rx.await {
            Ok(res) => res,
            Err(_canceled) => Ok(([0u8; 16], Vec::new())),
        }
    }
}

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
}

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
            let mut ids_builder = results
                .get()
                .init_resp()
                .init_ids(ids_for_resp.len() as u32);
            for (i, id) in ids_for_resp.iter().enumerate() {
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

        // Own the id bytes so they can be moved across await boundaries.
        let id_owned = id.to_vec();

        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let removed = storage
                .remove_in_progress_item(id_owned.as_slice(), &lease)
                .await?;

            results.get().init_resp().set_removed(removed);
            Ok(())
        })
    }

    fn poll(&mut self, params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
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
                let (lease, items) = coalescer.poll(num_items, lease_validity_secs).await?;
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
