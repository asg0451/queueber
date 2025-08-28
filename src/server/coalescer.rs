use std::sync::Arc;
use tokio::sync::{Notify, oneshot, watch};
use tokio::time::Duration;

use capnp::message::{Builder, HeapAllocator, TypedReader};

use crate::storage::{RetriedStorage, Storage};
use std::collections::VecDeque;

pub type PolledItems =
    Vec<TypedReader<Builder<HeapAllocator>, crate::protocol::polled_item::Owned>>;
pub type CoalescedPollResult =
    std::result::Result<Option<([u8; 16], PolledItems)>, std::sync::Arc<crate::errors::Error>>;

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

pub struct PollCoalescer {
    pub(crate) storage: Arc<RetriedStorage<Storage>>,
    cfg: PollCoalescingConfig,
    state: Arc<tokio::sync::Mutex<VecDeque<PendingPoll>>>, // pending batch
    wake: Arc<Notify>,
    shutdown: watch::Receiver<bool>,
}

impl PollCoalescer {
    pub fn new(storage: Arc<RetriedStorage<Storage>>, shutdown: watch::Receiver<bool>) -> Self {
        let this = Self {
            storage,
            cfg: PollCoalescingConfig::default(),
            state: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
            shutdown,
        };
        this.spawn_batcher();
        this
    }

    fn spawn_batcher(&self) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.cfg;
        let state = Arc::clone(&self.state);
        let wake = Arc::clone(&self.wake);
        let mut shutdown = self.shutdown.clone();
        tokio::task::Builder::new()
            .name("poll_coalescer")
            .spawn(async move {
                loop {
                    if state.lock().await.is_empty() {
                        tokio::select! {
                            _ = wake.notified() => {}
                            _ = async { if *shutdown.borrow() { } else { let _ = shutdown.changed().await; } } => { break; }
                        }
                    }

                    // Wait the batch window or abort on shutdown
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(cfg.batch_window_ms)) => {}
                        _ = async { if *shutdown.borrow() { } else { let _ = shutdown.changed().await; } } => { break; }
                    }

                    let mut batch: Vec<PendingPoll> = Vec::new();
                    {
                        let mut guard = state.lock().await;
                        let take_n = guard.len().min(cfg.max_batch_size);
                        for _ in 0..take_n {
                            if let Some(p) = guard.pop_front() {
                                batch.push(p);
                            }
                        }
                    }

                    if batch.is_empty() {
                        continue;
                    }

                    let mut total_items: usize = 0;
                    let mut lease_secs: u64 = 0;
                    for p in &batch {
                        total_items = total_items.saturating_add(p.num_items);
                        lease_secs = lease_secs.max(p.lease_validity_secs);
                    }
                    total_items = total_items.min(cfg.max_batch_items);

                    if total_items == 0 {
                        for p in batch {
                            let _ = p.tx.send(Ok(None));
                        }
                        continue;
                    }

                    let (lease, items) = match storage
                        .get_next_available_entries_with_lease(total_items, lease_secs)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            let shared = std::sync::Arc::new(e);
                            for p in batch {
                                let _ = p.tx.send(Err(shared.clone()));
                            }
                            continue;
                        }
                    };

                    let mut distributed: Vec<PolledItems> =
                        (0..batch.len()).map(|_| Vec::new()).collect();
                    let mut remaining: Vec<usize> = batch.iter().map(|p| p.num_items).collect();
                    let mut idx = 0usize;
                    for item in items.into_iter() {
                        let mut placed = false;
                        for _ in 0..batch.len() {
                            let target = idx % batch.len();
                            if remaining[target] > 0 {
                                distributed[target].push(item);
                                remaining[target] -= 1;
                                idx = idx.wrapping_add(1);
                                placed = true;
                                break;
                            }
                            idx = idx.wrapping_add(1);
                        }
                        if !placed {
                            break;
                        }
                    }

                    for (pos, p) in batch.into_iter().enumerate() {
                        let items_for_req = std::mem::take(&mut distributed[pos]);
                        let _ = if items_for_req.is_empty() {
                            p.tx.send(Ok(None))
                        } else {
                            p.tx.send(Ok(Some((lease, items_for_req))))
                        };
                    }
                }

                // Shutdown: drain any pending polls with an empty result to unblock waiters
                let mut guard = state.lock().await;
                while let Some(p) = guard.pop_front() {
                    let _ = p.tx.send(Ok(None));
                }
            })
            .expect("spawn poll coalescer");
    }

    pub async fn poll(&self, num_items: usize, lease_validity_secs: u64) -> CoalescedPollResult {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.state.lock().await;
            guard.push_back(PendingPoll {
                num_items: if num_items == 0 { 1 } else { num_items },
                lease_validity_secs,
                tx,
            });
        }
        self.wake.notify_one();
        match rx.await {
            Ok(res) => res,
            Err(_canceled) => Ok(None),
        }
    }
}
