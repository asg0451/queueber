use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::{Parser, Subcommand};
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::protocol::queue;
use std::{
    collections::HashSet,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Mutex, atomic},
    time::Duration,
};
use rand::Rng;
use tokio::{
    runtime::Handle,
    time::{Instant, MissedTickBehavior},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

// Busy error tracking helper: counts Busy errors and total requests, ignores Busy while propagating others
mod busy_tracker {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TOTAL: OnceLock<AtomicU64> = OnceLock::new();
    static BUSY: OnceLock<AtomicU64> = OnceLock::new();

    fn counters() -> (&'static AtomicU64, &'static AtomicU64) {
        (
            TOTAL.get_or_init(|| AtomicU64::new(0)),
            BUSY.get_or_init(|| AtomicU64::new(0)),
        )
    }

    pub trait IsBusyError {
        fn is_busy(&self) -> bool;
    }

    impl<E> IsBusyError for E
    where
        E: std::fmt::Display,
    {
        fn is_busy(&self) -> bool {
            self.to_string().to_lowercase().contains("busy")
        }
    }

    pub fn track_and_ignore_busy_error<T, E: IsBusyError>(
        res: std::result::Result<T, E>,
    ) -> std::result::Result<(), E> {
        let (total, busy) = counters();
        total.fetch_add(1, Ordering::Relaxed);
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.is_busy() {
                    busy.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn report_and_reset(context: &str) {
        let (total, busy) = counters();
        let t = total.swap(0, Ordering::Relaxed);
        let b = busy.swap(0, Ordering::Relaxed);
        if t > 0 {
            let pct = (b as f64) * 100.0 / (t as f64);
            println!(
                "[{}] Busy errors tolerated: {} / {} ({:.2}%)",
                context, b, t, pct
            );
        }
    }
}

// Connection limiter - limits concurrent connections instead of pooling non-Send clients
struct ConnectionLimiter {
    semaphore: tokio::sync::Semaphore,
    addr: SocketAddr,
}

impl ConnectionLimiter {
    fn new(addr: SocketAddr, max_concurrent: usize) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(max_concurrent),
            addr,
        }
    }

    async fn with_client<F, Fut, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(queue::Client) -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        // Acquire permit to limit concurrent connections
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            color_eyre::eyre::eyre!("Failed to acquire connection permit: {}", e)
        })?;

        // Create client connection
        let stream = tokio::net::TcpStream::connect(self.addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let rpc_network = Box::new(twoparty::VatNetwork::new(
            futures::io::BufReader::new(reader),
            futures::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(rpc_network, None);
        let queue_client: queue::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        // Use the LocalSet pattern like the original with_client function
        Ok(tokio::task::LocalSet::new()
            .run_until(async move {
                let _jh = tokio::task::Builder::new()
                    .name("rpc_system_limited")
                    .spawn_local(rpc_system);
                f(queue_client).await
            })
            .await)
    }
}

fn compute_batch_interval(rate_per_client: u32, batch_size: u32) -> Option<Duration> {
    if rate_per_client == 0 {
        return None;
    }
    let secs_per_batch = (batch_size as f64) / (rate_per_client as f64);
    let duration = Duration::from_secs_f64(secs_per_batch);
    Some(duration.max(Duration::from_millis(1)))
}

#[cfg(test)]
mod tests {
    use super::compute_batch_interval;
    use std::time::Duration;

    #[test]
    fn interval_none_when_rate_zero() {
        assert_eq!(compute_batch_interval(0, 10), None);
    }

    #[test]
    fn interval_scales_with_batch_and_rate() {
        let i1 = compute_batch_interval(10, 10).unwrap();
        assert!(i1 > Duration::from_millis(0));

        let i2 = compute_batch_interval(100, 10).unwrap();
        assert!(i2 < i1);

        let i3 = compute_batch_interval(1000, 10).unwrap();
        assert!(i3 < i2);
    }
}

#[derive(Parser, Debug)]
#[command(name = "queueber", version, about = "Queueber client")]
struct Cli {
    /// Server address (host:port)
    #[arg(short = 'a', long = "addr", default_value = "127.0.0.1:9090")]
    addr: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Add one item to the queue
    Add {
        /// Item contents (bytes as string)
        #[arg(short = 'c', long = "contents")]
        contents: String,

        /// Visibility timeout in seconds
        #[arg(short = 'v', long = "visibility", default_value_t = 10)]
        visibility_timeout_secs: u64,
    },
    /// Poll for available items
    Poll {
        /// How long the lease should be valid on the server (seconds)
        #[arg(short = 'l', long = "lease", default_value_t = 30)]
        lease_validity_secs: u64,
        /// Maximum number of items to return
        #[arg(short = 'n', long = "num", default_value_t = 1)]
        num_items: u32,
        /// How long to wait for items before returning (seconds)
        #[arg(short = 't', long = "timeout", default_value_t = 0)]
        timeout_secs: u64,
    },
    /// Remove an item by id under a lease
    Remove {
        /// The item id (UUID string) to remove
        #[arg(short = 'i', long = "id")]
        id: String,
        /// The lease id (UUID string) that owns the item
        #[arg(short = 'l', long = "lease")]
        lease: String,
    },
    /// Extend a lease by some seconds
    Extend {
        /// The lease id (UUID string) to extend
        #[arg(short = 'l', long = "lease")]
        lease: String,
        /// New lease validity in seconds from now
        #[arg(short = 'v', long = "validity", default_value_t = 30)]
        lease_validity_secs: u64,
    },
    /// Stress test the server
    Stress {
        /// The number of concurrent polling clients to spawn
        #[arg(short = 'p', long = "polling-clients", default_value_t = 1)]
        polling_clients: u32,
        /// The number of concurrent adding clients to spawn
        #[arg(short = 'a', long = "adding-clients", default_value_t = 1)]
        adding_clients: u32,
        /// Target message add rate per client (messages/second)
        #[arg(short = 'r', long = "rate", default_value_t = 100)]
        rate: u32,
        /// How long to run the stress test for (seconds)
        #[arg(short = 'd', long = "duration", default_value_t = 30)]
        duration_secs: u64,
    },
    /// Realistic workload simulator with N workers processing M tasks each
    WorkloadSim {
        /// Number of concurrent workers
        #[arg(short = 'w', long = "workers", default_value_t = 4)]
        workers: u32,
        /// Maximum tasks each worker polls for at once
        #[arg(short = 'm', long = "tasks-per-worker", default_value_t = 5)]
        tasks_per_worker: u32,
        /// Number of concurrent task producers
        #[arg(short = 'p', long = "producers", default_value_t = 2)]
        producers: u32,
        /// Tasks produced per second per producer
        #[arg(short = 'r', long = "production-rate", default_value_t = 10)]
        production_rate: u32,
        /// Minimum task execution time in milliseconds
        #[arg(long = "min-exec-time", default_value_t = 100)]
        min_execution_time_ms: u64,
        /// Maximum task execution time in milliseconds
        #[arg(long = "max-exec-time", default_value_t = 2000)]
        max_execution_time_ms: u64,
        /// Probability (0.0-1.0) that a worker extends a lease during processing
        #[arg(long = "extend-prob", default_value_t = 0.3)]
        extend_probability: f64,
        /// Probability (0.0-1.0) that a task is allowed to expire
        #[arg(long = "expiry-prob", default_value_t = 0.05)]
        expiry_probability: f64,
        /// How long to run the simulation in seconds
        #[arg(short = 'd', long = "duration", default_value_t = 60)]
        duration_secs: u64,
        /// Lease validity in seconds for polled tasks
        #[arg(short = 'l', long = "lease-validity", default_value_t = 30)]
        lease_validity_secs: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let addr = SocketAddr::from_str(&cli.addr)?;

    match cli.command {
        Commands::Add {
            contents,
            visibility_timeout_secs,
        } => {
            with_client(addr, |queue_client| async move {
                let res: Result<(), capnp::Error> = async move {
                    let mut request = queue_client.add_request();
                    let req = request.get().init_req();
                    let items = req.init_items(1);
                    let mut item = items.get(0);
                    item.set_contents(contents.as_bytes());
                    item.set_visibility_timeout_secs(visibility_timeout_secs);

                    let reply = request.send().promise.await?;
                    let ids = reply.get()?.get_resp()?.get_ids()?;

                    let parsed_ids = ids
                        .iter()
                        .map(|id_res| -> std::result::Result<Uuid, capnp::Error> {
                            let bytes = id_res?;
                            match Uuid::from_slice(bytes) {
                                Ok(u) => Ok(u),
                                Err(e) => Err(capnp::Error::failed(e.to_string())),
                            }
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    println!("received {:?} ids: {:?}", ids.len(), parsed_ids);
                    Ok(())
                }
                .await;
                busy_tracker::track_and_ignore_busy_error(res)
                    .map_err::<Box<dyn std::error::Error>, _>(Into::into)?;
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
            busy_tracker::report_and_reset("client-add");
        }
        Commands::Poll {
            lease_validity_secs,
            num_items,
            timeout_secs,
        } => {
            with_client(addr, |queue_client| async move {
                let res: Result<(), capnp::Error> = async move {
                    let mut request = queue_client.poll_request();
                    let mut req = request.get().init_req();
                    req.set_lease_validity_secs(lease_validity_secs);
                    req.set_num_items(num_items);
                    req.set_timeout_secs(timeout_secs);

                    let reply = request.send().promise.await?;
                    let resp = reply.get()?.get_resp()?;
                    let lease = resp.get_lease()?;
                    let items = resp.get_items()?;

                    println!(
                        "lease: {}",
                        Uuid::from_slice(lease)
                            .map(|u| u.to_string())
                            .unwrap_or_else(|_| format!("{:?}", lease))
                    );
                    if items.is_empty() {
                        println!("no items available");
                    } else {
                        for i in 0..items.len() {
                            let item = items.get(i);
                            let id = item.get_id()?;
                            let contents = item.get_contents()?;
                            println!(
                                "item {}: id={}, contents=",
                                i,
                                Uuid::from_slice(id)
                                    .map(|u| u.to_string())
                                    .unwrap_or_else(|_| format!("{:?}", id)),
                            );
                            println!("{}", String::from_utf8_lossy(contents));
                        }
                    }
                    Ok(())
                }
                .await;
                busy_tracker::track_and_ignore_busy_error(res)
                    .map_err::<Box<dyn std::error::Error>, _>(Into::into)?;
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
            busy_tracker::report_and_reset("client-poll");
        }
        Commands::Remove { id, lease } => {
            with_client(addr, |queue_client| async move {
                let id_bytes = uuid::Uuid::parse_str(&id)?.into_bytes();
                let lease_bytes = uuid::Uuid::parse_str(&lease)?.into_bytes();

                let mut request = queue_client.remove_request();
                let mut req = request.get().init_req();
                req.set_id(&id_bytes);
                req.set_lease(&lease_bytes);

                let res: Result<(), capnp::Error> = async move {
                    let reply = request.send().promise.await?;
                    let removed = reply.get()?.get_resp()?.get_removed();
                    println!("removed: {}", removed);
                    Ok(())
                }
                .await;
                busy_tracker::track_and_ignore_busy_error(res)
                    .map_err::<Box<dyn std::error::Error>, _>(Into::into)?;
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
            busy_tracker::report_and_reset("client-remove");
        }
        Commands::Extend {
            lease,
            lease_validity_secs,
        } => {
            with_client(addr, |queue_client| async move {
                let lease_bytes = uuid::Uuid::parse_str(&lease)?.into_bytes();
                let mut request = queue_client.extend_request();
                let mut req = request.get().init_req();
                req.set_lease(&lease_bytes);
                req.set_lease_validity_secs(lease_validity_secs);
                let res: Result<(), capnp::Error> = async move {
                    let reply = request.send().promise.await?;
                    let extended = reply.get()?.get_resp()?.get_extended();
                    println!("extended: {}", extended);
                    Ok(())
                }
                .await;
                busy_tracker::track_and_ignore_busy_error(res)
                    .map_err::<Box<dyn std::error::Error>, _>(Into::into)?;
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
            busy_tracker::report_and_reset("client-extend");
        }
        Commands::Stress {
            polling_clients,
            adding_clients,
            rate,
            duration_secs,
        } => {
            let start_time = Instant::now();
            let end_time = start_time
                .checked_add(Duration::from_secs(duration_secs))
                .unwrap();

            let add_count = Arc::new(atomic::AtomicU64::new(0));
            let poll_count = Arc::new(atomic::AtomicU64::new(0));
            let remove_count = Arc::new(atomic::AtomicU64::new(0));
            let extend_count = Arc::new(atomic::AtomicU64::new(0));
            let unique_leases: Arc<Mutex<HashSet<[u8; 16]>>> = Arc::new(Mutex::new(HashSet::new()));

            // periodic metrics reporter
            tokio::task::Builder::new()
                .name("metrics_reporter")
                .spawn({
                    let add_count = Arc::clone(&add_count);
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    let extend_count = Arc::clone(&extend_count);
                    async move {
                        let mut last_time = Instant::now();
                        while Instant::now() < end_time {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let now = Instant::now();
                            let adds = add_count.swap(0, atomic::Ordering::Relaxed);
                            let polls = poll_count.swap(0, atomic::Ordering::Relaxed);
                            let removes = remove_count.swap(0, atomic::Ordering::Relaxed);
                            let extends = extend_count.swap(0, atomic::Ordering::Relaxed);
                            let duration = now.duration_since(last_time);
                            last_time = now;
                            let secs = duration.as_secs_f64().max(1.0);
                            println!(
                                "add: {} ({:.1}/s), poll: {} ({:.1}/s), remove: {} ({:.1}/s), extend: {} ({:.1}/s)",
                                adds,
                                adds as f64 / secs,
                                polls,
                                polls as f64 / secs,
                                removes,
                                removes as f64 / secs,
                                extends,
                                extends as f64 / secs
                            );
                        }
                    }
                })
                .unwrap();

            std::thread::scope(|s| {
                // spawn polling clients
                for _ in 0..polling_clients {
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    let extend_count = Arc::clone(&extend_count);
                    let unique_leases = Arc::clone(&unique_leases);
                    let handle = Handle::current();
                    s.spawn(move || {
                        handle.block_on(async move {
                            tokio::task::LocalSet::new()
                                .run_until(async move {
                                    let _ = with_client(addr, |queue_client| async move {
                                        let mut current_lease: Option<[u8; 16]> = None;
                                        let mut last_extend = Instant::now();
                                        while Instant::now() < end_time {
                                            let mut request = queue_client.poll_request();
                                            let mut req = request.get().init_req();
                                            req.set_lease_validity_secs(30);
                                            req.set_num_items(10);
                                            req.set_timeout_secs(5);
                                            let poll_res: Result<(), capnp::Error> = async {
                                                let reply = request.send().promise.await?;
                                                let resp = reply.get()?.get_resp()?;
                                                let items = resp.get_items()?;
                                                poll_count.fetch_add(
                                                    items.len() as u64,
                                                    atomic::Ordering::Relaxed,
                                                );

                                                let lease = resp.get_lease()?;
                                                if lease.len() == 16 {
                                                    let mut lease_arr = [0u8; 16];
                                                    lease_arr.copy_from_slice(lease);
                                                    current_lease = Some(lease_arr);
                                                    if !items.is_empty()
                                                        && let Ok(mut s) = unique_leases.lock()
                                                    {
                                                        s.insert(lease_arr);
                                                    }
                                                }

                                                let promises = items.iter().map(|i| {
                                                    let mut request = queue_client.remove_request();
                                                    let mut req = request.get().init_req();
                                                    req.set_id(i.get_id().unwrap());
                                                    req.set_lease(lease);
                                                    request.send().promise
                                                });
                                                let _ = futures::future::join_all(promises).await;
                                                remove_count.fetch_add(
                                                    items.len() as u64,
                                                    atomic::Ordering::Relaxed,
                                                );
                                                Ok(())
                                            }
                                            .await;
                                            let _ =
                                                busy_tracker::track_and_ignore_busy_error(poll_res);

                                            // Occasionally extend the current lease to exercise extend under load.
                                            if last_extend.elapsed() > Duration::from_secs(2) {
                                                if let Some(lease_arr) = current_lease {
                                                    let mut request = queue_client.extend_request();
                                                    let mut req = request.get().init_req();
                                                    req.set_lease(&lease_arr);
                                                    req.set_lease_validity_secs(30);
                                                    let res =
                                                        request.send().promise.await.map(|_| ());
                                                    let _ =
                                                        busy_tracker::track_and_ignore_busy_error(
                                                            res,
                                                        );
                                                    extend_count
                                                        .fetch_add(1, atomic::Ordering::Relaxed);
                                                }
                                                last_extend = Instant::now();
                                            }
                                        }
                                    })
                                    .await;
                                })
                                .await;
                        });
                    });
                }

                // spawn adding clients
                for _ in 0..adding_clients {
                    let add_count = Arc::clone(&add_count);
                    let handle = Handle::current();
                    s.spawn(move || {
                        handle.block_on(async move {
                            tokio::task::LocalSet::new()
                                .run_until(async move {
                                    let _ = with_client(addr, |queue_client| async move {
                                        let batch_size: u32 = 10;
                                        let mut ticker = compute_batch_interval(rate, batch_size)
                                            .map(tokio::time::interval);
                                        if let Some(ref mut t) = ticker {
                                            t.set_missed_tick_behavior(MissedTickBehavior::Delay);
                                        }
                                        while Instant::now() < end_time {
                                            if let Some(t) = &mut ticker {
                                                t.tick().await;
                                            }
                                            let mut request = queue_client.add_request();
                                            let req = request.get().init_req();
                                            let mut items = req.init_items(batch_size);
                                            for i in 0..batch_size as usize {
                                                let mut item = items.reborrow().get(i as u32);
                                                item.set_contents(format!("test {}", i).as_bytes());
                                                item.set_visibility_timeout_secs(3);
                                            }
                                            let res = request.send().promise.await.map(|_| ());
                                            let _ = busy_tracker::track_and_ignore_busy_error(res);
                                            add_count.fetch_add(
                                                batch_size as u64,
                                                atomic::Ordering::Relaxed,
                                            );
                                        }
                                    })
                                    .await;
                                })
                                .await;
                        });
                    });
                }
            });
            println!("stress test completed");
            // Report unique leases observed across all polling clients
            if let Ok(s) = unique_leases.lock() {
                println!(
                    "Unique leases observed (proxy for coalesced polls): {}",
                    s.len()
                );
            }
            busy_tracker::report_and_reset("client-stress");
        }
        Commands::WorkloadSim {
            workers,
            tasks_per_worker,
            producers,
            production_rate,
            min_execution_time_ms,
            max_execution_time_ms,
            extend_probability,
            expiry_probability,
            duration_secs,
            lease_validity_secs,
        } => {
            run_workload_simulation(
                addr,
                workers,
                tasks_per_worker,
                producers,
                production_rate,
                min_execution_time_ms,
                max_execution_time_ms,
                extend_probability,
                expiry_probability,
                duration_secs,
                lease_validity_secs,
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_workload_simulation(
    addr: SocketAddr,
    workers: u32,
    tasks_per_worker: u32,
    producers: u32,
    production_rate: u32,
    min_execution_time_ms: u64,
    max_execution_time_ms: u64,
    extend_probability: f64,
    expiry_probability: f64,
    duration_secs: u64,
    lease_validity_secs: u64,
) -> Result<()> {
    let start_time = Instant::now();
    let end_time = start_time
        .checked_add(Duration::from_secs(duration_secs))
        .unwrap();

    // Shared metrics counters
    let tasks_produced = Arc::new(atomic::AtomicU64::new(0));
    let tasks_polled = Arc::new(atomic::AtomicU64::new(0));
    let tasks_processed = Arc::new(atomic::AtomicU64::new(0));
    let tasks_removed = Arc::new(atomic::AtomicU64::new(0));
    let tasks_expired = Arc::new(atomic::AtomicU64::new(0));
    let leases_extended = Arc::new(atomic::AtomicU64::new(0));
    let unique_leases: Arc<Mutex<HashSet<[u8; 16]>>> = Arc::new(Mutex::new(HashSet::new()));
    let execution_time_sum_ms = Arc::new(atomic::AtomicU64::new(0));

    println!(
        "Starting workload simulation: {} workers (max {} tasks each), {} producers ({} tasks/s each)",
        workers, tasks_per_worker, producers, production_rate
    );
    println!(
        "Task execution: {}-{}ms, extend prob: {:.1}%, expiry prob: {:.1}%, duration: {}s",
        min_execution_time_ms,
        max_execution_time_ms,
        extend_probability * 100.0,
        expiry_probability * 100.0,
        duration_secs
    );

    // Create separate connection limiters for workers and producers to prevent starvation
    let total_connections = std::cmp::min(200, (workers + producers) / 4).max(20) as usize;
    let producer_connections = std::cmp::max(producers as usize, total_connections / 5); // At least 20% or producer count
    let worker_connections = total_connections - producer_connections;

    let worker_connection_limiter = Arc::new(ConnectionLimiter::new(addr, worker_connections));
    let producer_connection_limiter = Arc::new(ConnectionLimiter::new(addr, producer_connections));

    println!(
        "Using connection limiters: {} for workers, {} for producers (total: {})",
        worker_connections, producer_connections, total_connections
    );

    // Periodic metrics reporter
    tokio::task::Builder::new()
        .name("metrics_reporter")
        .spawn({
            let tasks_produced = Arc::clone(&tasks_produced);
            let tasks_polled = Arc::clone(&tasks_polled);
            let tasks_processed = Arc::clone(&tasks_processed);
            let tasks_removed = Arc::clone(&tasks_removed);
            let tasks_expired = Arc::clone(&tasks_expired);
            let leases_extended = Arc::clone(&leases_extended);
            let execution_time_sum_ms = Arc::clone(&execution_time_sum_ms);
            async move {
                let mut last_time = Instant::now();
                while Instant::now() < end_time {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    let now = Instant::now();
                    let produced = tasks_produced.swap(0, atomic::Ordering::Relaxed);
                    let polled = tasks_polled.swap(0, atomic::Ordering::Relaxed);
                    let processed = tasks_processed.swap(0, atomic::Ordering::Relaxed);
                    let removed = tasks_removed.swap(0, atomic::Ordering::Relaxed);
                    let expired = tasks_expired.swap(0, atomic::Ordering::Relaxed);
                    let extended = leases_extended.swap(0, atomic::Ordering::Relaxed);
                    let exec_sum = execution_time_sum_ms.swap(0, atomic::Ordering::Relaxed);

                    let duration = now.duration_since(last_time);
                    last_time = now;
                    let secs = duration.as_secs_f64().max(1.0);

                    let avg_exec_time = if processed > 0 { exec_sum / processed } else { 0 };

                    println!(
                        "produced: {} ({:.1}/s), polled: {} ({:.1}/s), processed: {} ({:.1}/s), removed: {} ({:.1}/s), expired: {} ({:.1}/s), extended: {} ({:.1}/s), avg exec: {}ms",
                        produced, produced as f64 / secs,
                        polled, polled as f64 / secs,
                        processed, processed as f64 / secs,
                        removed, removed as f64 / secs,
                        expired, expired as f64 / secs,
                        extended, extended as f64 / secs,
                        avg_exec_time
                    );
                }
            }
        })
        .unwrap();

    // Use LocalSet with local tasks to support 10k+ workers without OS thread limits
    let local_set = tokio::task::LocalSet::new();

    // Clone all Arc references before moving them into the async closure
    let tasks_produced_clone = Arc::clone(&tasks_produced);
    let tasks_polled_clone = Arc::clone(&tasks_polled);
    let tasks_processed_clone = Arc::clone(&tasks_processed);
    let tasks_removed_clone = Arc::clone(&tasks_removed);
    let tasks_expired_clone = Arc::clone(&tasks_expired);
    let leases_extended_clone = Arc::clone(&leases_extended);
    let unique_leases_clone = Arc::clone(&unique_leases);
    let execution_time_sum_ms_clone = Arc::clone(&execution_time_sum_ms);

    local_set.run_until(async move {
        let mut worker_tasks = Vec::new();

        // Spawn local worker tasks
        for worker_id in 0..workers {
            let tasks_polled = Arc::clone(&tasks_polled_clone);
            let tasks_processed = Arc::clone(&tasks_processed_clone);
            let tasks_removed = Arc::clone(&tasks_removed_clone);
            let tasks_expired = Arc::clone(&tasks_expired_clone);
            let leases_extended = Arc::clone(&leases_extended_clone);
            let unique_leases = Arc::clone(&unique_leases_clone);
            let execution_time_sum_ms = Arc::clone(&execution_time_sum_ms_clone);
            let worker_connection_limiter = Arc::clone(&worker_connection_limiter);

            let worker_task = tokio::task::Builder::new()
                .name(&format!("worker_{}", worker_id))
                .spawn_local(async move {
                    while Instant::now() < end_time {
                        let tasks_polled = Arc::clone(&tasks_polled);
                        let tasks_expired = Arc::clone(&tasks_expired);
                        let leases_extended = Arc::clone(&leases_extended);
                        let tasks_processed = Arc::clone(&tasks_processed);
                        let execution_time_sum_ms = Arc::clone(&execution_time_sum_ms);
                        let tasks_removed = Arc::clone(&tasks_removed);
                        let unique_leases = Arc::clone(&unique_leases);

                        let _ = worker_connection_limiter
                            .with_client(|queue_client| async move {
                                let mut rng = rand::thread_rng();
                                let poll_result: Result<(), capnp::Error> = async {
                                    // Poll for tasks
                                    let mut request = queue_client.poll_request();
                                    let mut req = request.get().init_req();
                                    req.set_lease_validity_secs(lease_validity_secs);
                                    req.set_num_items(tasks_per_worker);
                                    req.set_timeout_secs(5); // Wait up to 5 seconds for tasks

                                    let reply = request.send().promise.await?;
                                    let resp = reply.get()?.get_resp()?;
                                    let items = resp.get_items()?;
                                    let lease = resp.get_lease()?;

                                    if items.is_empty() {
                                        // No tasks available, continue polling
                                        return Ok(());
                                    }

                                    tasks_polled.fetch_add(items.len() as u64, atomic::Ordering::Relaxed);

                                    // Track unique leases
                                    if lease.len() == 16 {
                                        let mut lease_arr = [0u8; 16];
                                        lease_arr.copy_from_slice(lease);
                                        if let Ok(mut s) = unique_leases.lock() {
                                            s.insert(lease_arr);
                                        }
                                    }

                                    // Process each task
                                    for item in items.iter() {
                                        let item_id = item.get_id()?;
                                        let _contents = item.get_contents()?;

                                        // Decide if this task should expire
                                        if rng.gen_range(0.0..1.0) < expiry_probability {
                                            // Let this task expire - don't process or remove it
                                            tasks_expired.fetch_add(1, atomic::Ordering::Relaxed);
                                            continue;
                                        }

                                        // Simulate task processing time
                                        let exec_time_ms = rng.gen_range(min_execution_time_ms..=max_execution_time_ms);
                                        let exec_duration = Duration::from_millis(exec_time_ms);

                                        // Start processing
                                        let process_start = Instant::now();

                                        // Check if we should extend the lease during processing
                                        let should_extend = rng.gen_range(0.0..1.0) < extend_probability;
                                        let extend_at = if should_extend {
                                            Some(process_start.checked_add(exec_duration / 2).unwrap())
                                        } else {
                                            None
                                        };

                                        // Simulate processing with potential lease extension
                                        while process_start.elapsed() < exec_duration {
                                            if let Some(extend_time) = extend_at {
                                                if Instant::now() >= extend_time {
                                                    // Extend the lease
                                                    let mut extend_request = queue_client.extend_request();
                                                    let mut extend_req = extend_request.get().init_req();
                                                    extend_req.set_lease(lease);
                                                    extend_req.set_lease_validity_secs(lease_validity_secs);

                                                    let res = extend_request.send().promise.await.map(|_| ());
                                                    let _ = busy_tracker::track_and_ignore_busy_error(res);
                                                    leases_extended.fetch_add(1, atomic::Ordering::Relaxed);
                                                    break; // Only extend once
                                                }
                                            }

                                            // Small sleep to simulate work
                                            tokio::time::sleep(Duration::from_millis(50)).await;
                                        }

                                        tasks_processed.fetch_add(1, atomic::Ordering::Relaxed);
                                        execution_time_sum_ms.fetch_add(exec_time_ms, atomic::Ordering::Relaxed);

                                        // Remove the completed task
                                        let mut remove_request = queue_client.remove_request();
                                        let mut remove_req = remove_request.get().init_req();
                                        remove_req.set_id(item_id);
                                        remove_req.set_lease(lease);

                                        let res = remove_request.send().promise.await.map(|_| ());
                                        let _ = busy_tracker::track_and_ignore_busy_error(res);
                                        tasks_removed.fetch_add(1, atomic::Ordering::Relaxed);
                                    }

                                    Ok(())
                                }.await;

                                let _ = busy_tracker::track_and_ignore_busy_error(poll_result);
                                Ok::<(), Box<dyn std::error::Error>>(())
                            })
                            .await;
                    }
                });

            worker_tasks.push(worker_task);
        }

        // Spawn local producer tasks
        let mut producer_tasks = Vec::new();
        for producer_id in 0..producers {
            let tasks_produced = Arc::clone(&tasks_produced_clone);
            let producer_connection_limiter = Arc::clone(&producer_connection_limiter);

            let producer_task = tokio::task::Builder::new()
                .name(&format!("producer_{}", producer_id))
                .spawn_local(async move {
                    let batch_size: u32 = 10;
                    let mut ticker = compute_batch_interval(production_rate, batch_size)
                        .map(tokio::time::interval);
                    if let Some(ref mut t) = ticker {
                        t.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    }

                    while Instant::now() < end_time {
                        if let Some(t) = &mut ticker {
                            t.tick().await;
                        }

                        let tasks_produced = Arc::clone(&tasks_produced);
                        let _ = producer_connection_limiter
                            .with_client(|queue_client| async move {
                                let mut request = queue_client.add_request();
                                let req = request.get().init_req();
                                let mut items = req.init_items(batch_size);

                                for i in 0..batch_size as usize {
                                    let mut item = items.reborrow().get(i as u32);
                                    item.set_contents(format!("workload_task_{}", i).as_bytes());
                                    item.set_visibility_timeout_secs(5); // 5 second initial visibility timeout
                                }

                                let res = request.send().promise.await.map(|_| ());
                                let _ = busy_tracker::track_and_ignore_busy_error(res);
                                tasks_produced.fetch_add(batch_size as u64, atomic::Ordering::Relaxed);

                                Ok::<(), Box<dyn std::error::Error>>(())
                            })
                            .await;
                    }
                });

            producer_tasks.push(producer_task);
        }

        // Wait for all tasks to complete
        for task in worker_tasks {
            if let Ok(handle) = task {
                let _ = handle.await;
            }
        }
        for task in producer_tasks {
            if let Ok(handle) = task {
                let _ = handle.await;
            }
        }
    }).await;

    println!("Workload simulation completed");

    // Final report
    if let Ok(s) = unique_leases.lock() {
        println!("Unique leases observed: {}", s.len());
    }

    // Final metrics (may have some remaining counts)
    let final_produced = tasks_produced.load(atomic::Ordering::Relaxed);
    let final_polled = tasks_polled.load(atomic::Ordering::Relaxed);
    let final_processed = tasks_processed.load(atomic::Ordering::Relaxed);
    let final_removed = tasks_removed.load(atomic::Ordering::Relaxed);
    let final_expired = tasks_expired.load(atomic::Ordering::Relaxed);
    let final_extended = leases_extended.load(atomic::Ordering::Relaxed);
    let final_exec_sum = execution_time_sum_ms.load(atomic::Ordering::Relaxed);

    let final_avg_exec = if final_processed > 0 { final_exec_sum / final_processed } else { 0 };

    println!("Final totals:");
    println!("  Tasks produced: {}", final_produced);
    println!("  Tasks polled: {}", final_polled);
    println!("  Tasks processed: {}", final_processed);
    println!("  Tasks removed: {}", final_removed);
    println!("  Tasks expired: {}", final_expired);
    println!("  Leases extended: {}", final_extended);
    println!("  Average execution time: {}ms", final_avg_exec);

    busy_tracker::report_and_reset("workload-simulation");
    Ok(())
}

async fn with_client<F, Fut, R>(addr: SocketAddr, f: F) -> Result<R>
where
    F: FnOnce(queue::Client) -> Fut,
    Fut: std::future::Future<Output = R>,
{
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
    Ok(tokio::task::LocalSet::new()
        .run_until(async move {
            let _jh = tokio::task::Builder::new()
                .name("rpc_system")
                .spawn_local(rpc_system);
            f(queue_client).await
        })
        .await)
}
