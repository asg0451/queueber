use std::sync::Arc;
use tokio::sync::{Notify, oneshot};
use tokio::time::Duration;

use capnp::message::{Builder, HeapAllocator, TypedReader};

use crate::storage::{RetriedStorage, Storage};
use std::collections::VecDeque;

pub type PolledItems =
    Vec<TypedReader<Builder<HeapAllocator>, crate::protocol::polled_item::Owned>>;
pub type CoalescedPollResult =
    std::result::Result<Option<([u8; 16], PolledItems)>, std::sync::Arc<crate::errors::Error>>;

#[derive(Clone, Copy, Debug)]
pub struct PollCoalescingConfig {
    pub max_batch_size: usize,
    pub max_batch_items: usize,
    pub batch_window_ms: u64,
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

impl PollCoalescingConfig {
    pub fn new(max_batch_size: usize, max_batch_items: usize, batch_window_ms: u64) -> Self {
        Self {
            max_batch_size,
            max_batch_items,
            batch_window_ms,
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
    pending: Arc<tokio::sync::Mutex<VecDeque<PendingPoll>>>, // pending batch
    wake: Arc<Notify>,
}

impl PollCoalescer {
    pub fn new(storage: Arc<RetriedStorage<Storage>>) -> Self {
        let this = Self {
            storage,
            cfg: PollCoalescingConfig::default(),
            pending: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
        };
        this.spawn_batcher();
        this
    }

    pub fn with_config(storage: Arc<RetriedStorage<Storage>>, cfg: PollCoalescingConfig) -> Self {
        let this = Self {
            storage,
            cfg,
            pending: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
        };
        this.spawn_batcher();
        this
    }

    fn spawn_batcher(&self) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.cfg;
        let pending = Arc::clone(&self.pending);
        let wake = Arc::clone(&self.wake);
        tokio::task::Builder::new()
            .name("poll_coalescer")
            .spawn(async move {
                let mut batch: Vec<PendingPoll> = Vec::with_capacity(cfg.max_batch_size);

                loop {
                    // wait until there are pending polls
                    if pending.lock().await.is_empty() {
                        wake.notified().await;
                    }

                    // wait until the batch window is elapsed to increase batch size
                    tokio::time::sleep(Duration::from_millis(cfg.batch_window_ms)).await;

                    // get a batch of pending polls
                    batch.clear();
                    {
                        let mut pending = pending.lock().await;
                        let take_n = pending.len().min(cfg.max_batch_size);
                        for _ in 0..take_n {
                            if let Some(p) = pending.pop_front() {
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
                        for p in batch.drain(..) {
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
                            for p in batch.drain(..) {
                                let _ = p.tx.send(Err(Arc::clone(&shared)));
                            }
                            continue;
                        }
                    };

                    // distribute the items to the pending polls.
                    // TODO: simplify this
                    // TODO: reuse across loops
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

                    for (pos, p) in batch.drain(..).enumerate() {
                        let items_for_req = std::mem::take(&mut distributed[pos]);
                        let _ = if items_for_req.is_empty() {
                            p.tx.send(Ok(None))
                        } else {
                            p.tx.send(Ok(Some((lease, items_for_req))))
                        };
                    }
                }
            })
            .expect("spawn poll coalescer");
    }

    pub async fn poll(&self, num_items: usize, lease_validity_secs: u64) -> CoalescedPollResult {
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.push_back(PendingPoll {
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

    #[cfg(test)]
    pub(crate) fn config(&self) -> PollCoalescingConfig {
        self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn with_config_sets_values() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RetriedStorage::new(Storage::new(dir.path()).unwrap()));
        let cfg = PollCoalescingConfig::new(10, 100, 5);
        let c = PollCoalescer::with_config(Arc::clone(&storage), cfg);
        let got = c.config();
        assert_eq!(got.max_batch_size, 10);
        assert_eq!(got.max_batch_items, 100);
        assert_eq!(got.batch_window_ms, 5);
    }

    #[tokio::test]
    async fn default_config_is_reasonable() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(RetriedStorage::new(Storage::new(dir.path()).unwrap()));
        let c = PollCoalescer::new(storage);
        let got = c.config();
        assert!(got.max_batch_size >= 1);
        assert!(got.max_batch_items >= 1);
    }
}
