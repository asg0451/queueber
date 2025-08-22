use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::{Parser, Subcommand};
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::protocol::queue;
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    time::{Instant, MissedTickBehavior},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

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
                let mut request = queue_client.add_request();
                let req = request.get().init_req();
                let items = req.init_items(1);
                let mut item = items.get(0);
                item.set_contents(contents.as_bytes());
                item.set_visibility_timeout_secs(visibility_timeout_secs);

                let reply = request.send().promise.await?;
                let ids = reply.get()?.get_resp()?.get_ids()?;

                println!(
                    "received {:?} ids: {:?}",
                    ids.len(),
                    ids.iter()
                        .map(|id| -> Result<Uuid> { Ok(Uuid::from_slice(id?)?) })
                        .collect::<Result<Vec<_>, _>>()?
                );
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
        }
        Commands::Poll {
            lease_validity_secs,
            num_items,
            timeout_secs,
        } => {
            with_client(addr, |queue_client| async move {
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
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
        }
        Commands::Remove { id, lease } => {
            with_client(addr, |queue_client| async move {
                let id_bytes = uuid::Uuid::parse_str(&id)?.into_bytes();
                let lease_bytes = uuid::Uuid::parse_str(&lease)?.into_bytes();

                let mut request = queue_client.remove_request();
                let mut req = request.get().init_req();
                req.set_id(&id_bytes);
                req.set_lease(&lease_bytes);

                let reply = request.send().promise.await?;
                let removed = reply.get()?.get_resp()?.get_removed();
                println!("removed: {}", removed);
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
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
                let reply = request.send().promise.await?;
                let extended = reply.get()?.get_resp()?.get_extended();
                println!("extended: {}", extended);
                Ok::<(), Box<dyn std::error::Error>>(())
            })
            .await?
            .unwrap();
        }
        Commands::Stress {
            polling_clients,
            adding_clients,
            rate,
        } => {
            let add_count = Arc::new(atomic::AtomicU64::new(0));
            let poll_count = Arc::new(atomic::AtomicU64::new(0));
            let remove_count = Arc::new(atomic::AtomicU64::new(0));

            // periodic metrics reporter
            tokio::task::Builder::new()
                .name("metrics_reporter")
                .spawn({
                    let add_count = Arc::clone(&add_count);
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    async move {
                        let mut last_time = Instant::now();
                        loop {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let now = Instant::now();
                            let adds = add_count.swap(0, atomic::Ordering::Relaxed);
                            let polls = poll_count.swap(0, atomic::Ordering::Relaxed);
                            let removes = remove_count.swap(0, atomic::Ordering::Relaxed);
                            let duration = now.duration_since(last_time);
                            last_time = now;
                            let secs = duration.as_secs_f64().max(1.0);
                            println!(
                                "add: {} ({:.1}/s), poll: {} ({:.1}/s), remove: {} ({:.1}/s)",
                                adds,
                                adds as f64 / secs,
                                polls,
                                polls as f64 / secs,
                                removes,
                                removes as f64 / secs
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
                    let handle = Handle::current();
                    s.spawn(move || {
                        handle.block_on(async move {
                            tokio::task::LocalSet::new()
                                .run_until(async move {
                                    let _ = with_client(addr, |queue_client| async move {
                                        loop {
                                            let mut request = queue_client.poll_request();
                                            let mut req = request.get().init_req();
                                            req.set_lease_validity_secs(30);
                                            req.set_num_items(10);
                                            req.set_timeout_secs(5);
                                            let reply = request.send().promise.await.unwrap();
                                            let resp = reply.get().unwrap().get_resp().unwrap();
                                            let items = resp.get_items().unwrap();
                                            poll_count.fetch_add(
                                                items.len() as u64,
                                                atomic::Ordering::Relaxed,
                                            );

                                            let lease = resp.get_lease().unwrap();

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
                                        loop {
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
                                            let _ = request.send().promise.await.unwrap();
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
