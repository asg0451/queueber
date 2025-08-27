use std::sync::Arc;
use tokio::sync::{Notify, oneshot};
use tokio::time::Duration;

use capnp::message::{Builder, HeapAllocator, TypedReader};

use crate::storage::{RetriedStorage, Storage};

pub type PolledItems =
    Vec<TypedReader<Builder<HeapAllocator>, crate::protocol::polled_item::Owned>>;
pub type CoalescedPollResult = crate::errors::Result<([u8; 16], PolledItems)>;

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
    state: Arc<tokio::sync::Mutex<Vec<PendingPoll>>>, // pending batch
    wake: Arc<Notify>,
    started: std::sync::atomic::AtomicBool,
}

impl PollCoalescer {
    pub fn new(storage: Arc<RetriedStorage<Storage>>) -> Self {
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
                    if state.lock().await.is_empty() {
                        wake.notified().await;
                    }

                    tokio::time::sleep(Duration::from_millis(cfg.batch_window_ms)).await;

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

                    let mut total_items: usize = 0;
                    let mut lease_secs: u64 = 0;
                    for p in &batch {
                        total_items = total_items.saturating_add(p.num_items);
                        lease_secs = lease_secs.max(p.lease_validity_secs);
                    }
                    total_items = total_items.min(cfg.max_batch_items);

                    if total_items == 0 {
                        for p in batch {
                            let _ = p.tx.send(Ok(([0u8; 16], Vec::new())));
                        }
                        continue;
                    }

                    let (lease, items) = match storage
                        .get_next_available_entries_with_lease(total_items, lease_secs)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = e.to_string();
                            for p in batch {
                                let _ = p.tx.send(Err(crate::errors::Error::Other {
                                    source: Box::new(std::io::Error::other(msg.clone())),
                                    backtrace: std::backtrace::Backtrace::capture(),
                                }));
                            }
                            continue;
                        }
                    };

                    let mut distributed: Vec<PolledItems> =
                        (0..batch.len()).map(|_| Vec::new()).collect();
                    let mut idx = 0usize;
                    for item in items.into_iter() {
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
                                    break;
                                }
                            }
                        }
                    }

                    for (pos, p) in batch.into_iter().enumerate() {
                        let items_for_req = std::mem::take(&mut distributed[pos]);
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

    pub async fn poll(
        &self,
        num_items: usize,
        lease_validity_secs: u64,
    ) -> crate::errors::Result<([u8; 16], PolledItems)> {
        if !self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
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
