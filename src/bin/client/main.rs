use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::{Parser, Subcommand};
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::protocol::queue;
use std::{
    collections::HashSet,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::{
    time::{Instant, MissedTickBehavior},
    sync::Semaphore,
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

// Async stress testing with LocalSet to work around Cap'n Proto threading constraints
async fn run_async_stress_worker(
    addr: SocketAddr,
    end_time: Instant,
    polling_clients: u32,
    adding_clients: u32,
    rate: u32,
    max_concurrent_ops: u32,
    add_count: Arc<atomic::AtomicU64>,
    poll_count: Arc<atomic::AtomicU64>,
    remove_count: Arc<atomic::AtomicU64>,
    extend_count: Arc<atomic::AtomicU64>,
    unique_leases: Arc<tokio::sync::Mutex<HashSet<[u8; 16]>>>,
) -> Result<()> {
    // Use LocalSet to work with Cap'n Proto's single-threaded design
    let local_set = tokio::task::LocalSet::new();
    
    local_set.run_until(async move {
        let semaphore = Arc::new(Semaphore::new(max_concurrent_ops as usize));
        let mut tasks = Vec::new();
        
        // spawn polling tasks
        for _ in 0..polling_clients {
            let poll_count = Arc::clone(&poll_count);
            let remove_count = Arc::clone(&remove_count);
            let extend_count = Arc::clone(&extend_count);
            let unique_leases = Arc::clone(&unique_leases);
            let semaphore = Arc::clone(&semaphore);
            
            let task = tokio::task::spawn_local(async move {
                let _ = with_client(addr, |queue_client| async move {
                    let mut current_lease: Option<[u8; 16]> = None;
                    let mut last_extend = Instant::now();
                    
                    while Instant::now() < end_time {
                        let _permit = semaphore.acquire().await.unwrap();
                        
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
                                if !items.is_empty() {
                                    let mut s = unique_leases.lock().await;
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
                        }.await;
                        
                        let _ = busy_tracker::track_and_ignore_busy_error(poll_res);

                        // Occasionally extend the current lease to exercise extend under load.
                        if last_extend.elapsed() > Duration::from_secs(2) {
                            if let Some(lease_arr) = current_lease {
                                let mut request = queue_client.extend_request();
                                let mut req = request.get().init_req();
                                req.set_lease(&lease_arr);
                                req.set_lease_validity_secs(30);
                                let res = request.send().promise.await.map(|_| ());
                                let _ = busy_tracker::track_and_ignore_busy_error(res);
                                extend_count.fetch_add(1, atomic::Ordering::Relaxed);
                                last_extend = Instant::now();
                            }
                        }
                    }
                }).await;
            });
            tasks.push(task);
        }

        // spawn adding tasks
        for _ in 0..adding_clients {
            let add_count = Arc::clone(&add_count);
            let semaphore = Arc::clone(&semaphore);
            
            let task = tokio::task::spawn_local(async move {
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
                        
                        let _permit = semaphore.acquire().await.unwrap();
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
                        add_count.fetch_add(batch_size as u64, atomic::Ordering::Relaxed);
                    }
                }).await;
            });
            tasks.push(task);
        }

        // Wait for all tasks to complete
        for task in tasks {
            if let Err(e) = task.await {
                eprintln!("Local task failed: {}", e);
            }
        }
    }).await;
    
    Ok(())
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
        /// Connection pool size (max concurrent connections)
        #[arg(short = 'c', long = "connections", default_value_t = 100)]
        connection_pool_size: u32,
        /// Maximum concurrent operations per connection pool
        #[arg(short = 'm', long = "max-concurrent", default_value_t = 1000)]
        max_concurrent_ops: u32,
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
            connection_pool_size: _,
            max_concurrent_ops,
        } => {
            let start_time = Instant::now();
            let end_time = start_time
                .checked_add(Duration::from_secs(duration_secs))
                .unwrap();

            let add_count = Arc::new(atomic::AtomicU64::new(0));
            let poll_count = Arc::new(atomic::AtomicU64::new(0));
            let remove_count = Arc::new(atomic::AtomicU64::new(0));
            let extend_count = Arc::new(atomic::AtomicU64::new(0));
            let unique_leases: Arc<tokio::sync::Mutex<HashSet<[u8; 16]>>> = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

            // Calculate optimal number of worker threads 
            // Use fewer threads but each handles many async operations via LocalSet
            let num_workers = std::cmp::min(
                std::thread::available_parallelism().map(|p| p.get()).unwrap_or(4),
                ((polling_clients + adding_clients) / 1000).max(1) as usize // 1000+ clients per thread
            ).max(1);
            
            println!("Starting scalable async stress test with {} worker threads...", num_workers);
            println!("Total: {} polling clients, {} adding clients", polling_clients, adding_clients);
            println!("Max concurrent operations per worker: {}", max_concurrent_ops / num_workers as u32);
            println!("Each worker can handle thousands of concurrent operations via async tasks!");

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

            // Use scoped threads for better efficiency than the old approach
            std::thread::scope(|s| {
                for worker_id in 0..num_workers {
                    let add_count = Arc::clone(&add_count);
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    let extend_count = Arc::clone(&extend_count);
                    let unique_leases = Arc::clone(&unique_leases);
                    
                    // Distribute clients across workers
                    let worker_polling_clients = if worker_id < polling_clients as usize % num_workers {
                        polling_clients / num_workers as u32 + 1
                    } else {
                        polling_clients / num_workers as u32
                    };
                    
                    let worker_adding_clients = if worker_id < adding_clients as usize % num_workers {
                        adding_clients / num_workers as u32 + 1
                    } else {
                        adding_clients / num_workers as u32
                    };
                    
                    let worker_max_concurrent = max_concurrent_ops / num_workers as u32;
                    
                    s.spawn(move || {
                        tokio::runtime::Handle::current().block_on(async move {
                            if let Err(e) = run_async_stress_worker(
                                addr,
                                end_time,
                                worker_polling_clients,
                                worker_adding_clients,
                                rate,
                                worker_max_concurrent,
                                add_count,
                                poll_count,
                                remove_count,
                                extend_count,
                                unique_leases,
                            ).await {
                                eprintln!("Worker {} failed: {}", worker_id, e);
                            }
                        });
                    });
                }
            });
            
            println!("stress test completed");
            // Report unique leases observed across all polling clients
            let s = unique_leases.lock().await;
            println!(
                "Unique leases observed (proxy for coalesced polls): {}",
                s.len()
            );
            busy_tracker::report_and_reset("client-stress");
        }
    }
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
