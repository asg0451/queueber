use capnp_rpc::RpcSystem;
use capnp_rpc::rpc_twoparty_capnp;
use capnp_rpc::twoparty;
use crate::protocol::queue;
use futures::AsyncReadExt;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Public worker configuration for processing polled jobs with timed extensions.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// How many items to request per poll.
    pub poll_batch_size: u32,
    /// Requested lease validity in seconds for each poll.
    pub lease_validity_secs: u64,
    /// Lower bound (inclusive) of simulated per-job processing time in milliseconds.
    pub process_time_min_ms: u64,
    /// Upper bound (inclusive) of simulated per-job processing time in milliseconds.
    pub process_time_max_ms: u64,
    /// Poll long-poll timeout in seconds.
    pub poll_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_batch_size: 16,
            lease_validity_secs: 30,
            process_time_min_ms: 10,
            process_time_max_ms: 100,
            poll_timeout_secs: 5,
        }
    }
}

/// Compute how often to extend a lease, given requested validity.
/// We extend at 50% of the requested validity, but never less than 1s and (for very small leases)
/// not more often than every 200ms.
pub fn compute_extend_interval(lease_validity_secs: u64) -> Duration {
    if lease_validity_secs == 0 {
        return Duration::from_millis(200);
    }
    let half_secs = lease_validity_secs / 2;
    Duration::from_secs(half_secs.max(1))
}

/// One batch of work: poll up to `poll_batch_size`, process each job concurrently with
/// randomized durations within the configured bounds, extend the batch lease periodically,
/// and remove each job when its processing completes.
///
/// Returns the number of jobs processed in this batch.
pub async fn run_worker_batch(
    queue_client: queue::Client,
    cfg: &WorkerConfig,
) -> Result<usize, capnp::Error> {
    // 1) Poll for up to N items
    let mut poll_req = queue_client.poll_request();
    {
        let mut req = poll_req.get().init_req();
        req.set_lease_validity_secs(cfg.lease_validity_secs);
        req.set_num_items(cfg.poll_batch_size);
        req.set_timeout_secs(cfg.poll_timeout_secs);
    }
    let poll_reply = match poll_req.send().promise.await {
        Ok(r) => r,
        Err(e) => return Err(e),
    };
    let poll_resp = poll_reply.get()?.get_resp()?;
    let lease = poll_resp.get_lease()?;
    let items = poll_resp.get_items()?;
    if items.is_empty() {
        return Ok(0);
    }

    // Snapshot lease bytes into fixed-size array
    let mut lease_arr = [0u8; 16];
    if lease.len() == 16 {
        lease_arr.copy_from_slice(lease);
    } else {
        // No valid lease → nothing we can safely remove; skip this batch.
        return Ok(0);
    }

    // 2) Spawn a periodic extender for this lease that runs until all work completes
    let stop_extender: Arc<Notify> = Arc::new(Notify::new());
    let stop_extender_clone = Arc::clone(&stop_extender);
    let extend_interval = compute_extend_interval(cfg.lease_validity_secs);
    let extender_client = queue_client.clone();
    let extender_lease = lease_arr;
    let extender_validity = cfg.lease_validity_secs;
    let extender = tokio::task::Builder::new()
        .name("worker_lease_extender")
        .spawn_local(async move {
            let mut interval = tokio::time::interval(extend_interval);
            // First tick happens immediately; we want to wait a full interval
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = stop_extender_clone.notified() => {
                        break;
                    }
                    _ = interval.tick() => {
                        let mut req = extender_client.extend_request();
                        {
                            let mut r = req.get().init_req();
                            r.set_lease(&extender_lease);
                            r.set_lease_validity_secs(extender_validity);
                        }
                        let _ = req.send().promise.await; // Ignore Busy/unknown; best-effort
                    }
                }
            }
        })
        .expect("spawn lease extender");

    // 3) Process and remove all items concurrently
    let mut rng = rand::thread_rng();
    let min_ms = cfg.process_time_min_ms.min(cfg.process_time_max_ms);
    let max_ms = cfg.process_time_max_ms.max(cfg.process_time_min_ms);

    let mut tasks = Vec::with_capacity(items.len() as usize);
    for i in 0..items.len() {
        let id = items.get(i).get_id()?.to_vec();
        let lease_bytes = lease.to_vec();
        let client = queue_client.clone();
        let sleep_ms: u64 = if max_ms == min_ms {
            min_ms
        } else {
            rng.gen_range(min_ms..=max_ms)
        };
        tasks.push(
            tokio::task::Builder::new()
                .name("worker_job")
                .spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    let mut req = client.remove_request();
                    let mut r = req.get().init_req();
                    r.set_id(&id);
                    r.set_lease(&lease_bytes);
                    let _ = req.send().promise.await; // Ignore Busy; best-effort
                })?,
        );
    }

    // Await all jobs
    for t in tasks {
        let _ = t.await;
    }

    // Stop extender and await it
    stop_extender.notify_waiters();
    let _ = extender.await;

    Ok(items.len() as usize)
}

/// Utility: connect to a server and produce a queue client (convenience for the worker binary)
pub async fn connect_queue_client(addr: std::net::SocketAddr) -> queue::Client {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
    let rpc_network = Box::new(twoparty::VatNetwork::new(
        futures::io::BufReader::new(reader),
        futures::io::BufWriter::new(writer),
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(rpc_network, None);
    let queue_client: queue::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::LocalSet::new()
        .run_until(async move {
            let _jh = tokio::task::Builder::new()
                .name("rpc_system")
                .spawn_local(rpc_system)
                .unwrap();
            queue_client
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::compute_extend_interval;
    use std::time::Duration;

    #[test]
    fn extend_interval_has_reasonable_bounds() {
        assert_eq!(compute_extend_interval(10), Duration::from_secs(5));
        assert!(compute_extend_interval(1) >= Duration::from_secs(1));
        // Very small leases are floored to 200ms
        assert_eq!(compute_extend_interval(0), Duration::from_millis(200));
    }
}
