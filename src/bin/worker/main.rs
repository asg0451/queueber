use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::Parser;
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::protocol::queue;
use rand::Rng;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "queueber-worker",
    version,
    about = "Run a single worker that polls and processes jobs"
)]
struct Args {
    /// Server address (host:port)
    #[arg(short = 'a', long = "addr", default_value = "127.0.0.1:9090")]
    addr: String,

    /// Items per poll
    #[arg(long = "batch", default_value_t = 16)]
    batch: u32,

    /// Lease validity seconds per poll
    #[arg(long = "lease", default_value_t = 30)]
    lease: u64,

    /// Long poll timeout seconds
    #[arg(long = "timeout", default_value_t = 5)]
    timeout: u64,

    /// Min per-job processing time in ms
    #[arg(long = "min-ms", default_value_t = 10)]
    min_ms: u64,

    /// Max per-job processing time in ms
    #[arg(long = "max-ms", default_value_t = 100)]
    max_ms: u64,

    /// Optional run duration; if 0, runs indefinitely
    #[arg(long = "duration", default_value_t = 0)]
    duration_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let addr = SocketAddr::from_str(&args.addr)?;

    let cfg = WorkerConfig {
        poll_batch_size: args.batch,
        lease_validity_secs: args.lease,
        process_time_min_ms: args.min_ms,
        process_time_max_ms: args.max_ms,
        poll_timeout_secs: args.timeout,
    };

    tokio::task::LocalSet::new()
        .run_until(async move {
            let client = connect_queue_client(addr).await;
            let deadline = if args.duration_secs > 0 {
                Some(Instant::now() + Duration::from_secs(args.duration_secs))
            } else {
                None
            };

            loop {
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    break;
                }
                match run_worker_batch(client.clone(), &cfg).await {
                    Ok(0) => {
                        // No items; backoff briefly to avoid tight loop
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(_n) => {
                        // processed _n items in this batch; immediately continue
                    }
                    Err(e) => {
                        eprintln!("worker batch error: {e}");
                        // brief backoff on error
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        })
        .await;

    Ok(())
}

// ================== Worker implementation (binary-scoped) ==================

#[derive(Clone, Debug)]
struct WorkerConfig {
    poll_batch_size: u32,
    lease_validity_secs: u64,
    process_time_min_ms: u64,
    process_time_max_ms: u64,
    poll_timeout_secs: u64,
}

fn compute_extend_interval(lease_validity_secs: u64) -> Duration {
    if lease_validity_secs == 0 {
        return Duration::from_millis(200);
    }
    let half_secs = lease_validity_secs / 2;
    Duration::from_secs(half_secs.max(1))
}

async fn connect_queue_client(addr: std::net::SocketAddr) -> queue::Client {
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

async fn run_worker_batch(
    queue_client: queue::Client,
    cfg: &WorkerConfig,
) -> Result<usize, capnp::Error> {
    // Poll
    let mut poll_req = queue_client.poll_request();
    {
        let mut req = poll_req.get().init_req();
        req.set_lease_validity_secs(cfg.lease_validity_secs);
        req.set_num_items(cfg.poll_batch_size);
        req.set_timeout_secs(cfg.poll_timeout_secs);
    }
    let poll_reply = poll_req.send().promise.await?; // use '?'
    let poll_resp = poll_reply.get()?.get_resp()?;
    let lease = poll_resp.get_lease()?;
    let items = poll_resp.get_items()?;
    if items.is_empty() {
        return Ok(0);
    }

    // Lease bytes
    let mut lease_arr = [0u8; 16];
    if lease.len() != 16 {
        return Ok(0);
    }
    lease_arr.copy_from_slice(lease);

    // Cancellation token for extender
    let token = tokio_util::sync::CancellationToken::new();
    let child = token.child_token();
    let extender_client = queue_client.clone();
    let extender_lease = lease_arr;
    let extender_validity = cfg.lease_validity_secs;
    let extend_interval = compute_extend_interval(cfg.lease_validity_secs);
    let extender = tokio::task::Builder::new()
        .name("worker_lease_extender")
        .spawn_local(async move {
            let mut interval = tokio::time::interval(extend_interval);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = child.cancelled() => break,
                    _ = interval.tick() => {
                        let mut req = extender_client.extend_request();
                        let mut r = req.get().init_req();
                        r.set_lease(&extender_lease);
                        r.set_lease_validity_secs(extender_validity);
                        let _ = req.send().promise.await; // best-effort
                    }
                }
            }
        })
        .expect("spawn extender");

    // Process jobs concurrently and remove
    let min_ms = cfg.process_time_min_ms.min(cfg.process_time_max_ms);
    let max_ms = cfg.process_time_max_ms.max(cfg.process_time_min_ms);
    let mut tasks = Vec::with_capacity(items.len() as usize);
    for i in 0..items.len() {
        let id = items.get(i).get_id()?.to_vec();
        let lease_bytes = lease.to_vec();
        let client = queue_client.clone();
        let sleep_ms = if max_ms == min_ms {
            min_ms
        } else {
            rand::thread_rng().gen_range(min_ms..=max_ms)
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
                    let _ = req.send().promise.await;
                })?,
        );
    }

    for t in tasks {
        let _ = t.await;
    }

    // Shutdown extender via token
    token.cancel();
    let _ = extender.await;
    Ok(items.len() as usize)
}
