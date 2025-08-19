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
use tokio::{runtime::Handle, time::Instant};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

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
        .unwrap_or_else(|| EnvFilter::new("info,queueber=info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(console_subscriber::spawn())
        .init();

    let cli = Cli::parse();
    let addr = &cli.addr;
    let addr = SocketAddr::from_str(addr)?;

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
                            "item {}: id={}, contents={}",
                            i,
                            Uuid::from_slice(id)
                                .map(|u| u.to_string())
                                .unwrap_or_else(|_| format!("{:?}", id)),
                            String::from_utf8_lossy(contents)
                        );
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
        Commands::Stress {
            polling_clients,
            adding_clients,
            rate: _, // TODO: use this
        } => {
            // TODO: clean this shit up jesus fucking christ

            std::thread::scope(|s| {
                // bookkeeping
                let add_count = Arc::new(atomic::AtomicU64::new(0));
                let poll_count = Arc::new(atomic::AtomicU64::new(0));
                let remove_count = Arc::new(atomic::AtomicU64::new(0));

                tokio::spawn({
                    let add_count = Arc::clone(&add_count);
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    async move {
                        let mut last_time = Instant::now();
                        loop {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let now = Instant::now();
                            let adds = add_count.load(atomic::Ordering::Relaxed);
                            let polls = poll_count.load(atomic::Ordering::Relaxed);
                            let removes = remove_count.load(atomic::Ordering::Relaxed);
                            let duration = now.duration_since(last_time);
                            last_time = now;
                            println!(
                                "add: {} ({}/s), poll: {} ({}/s), remove: {} ({}/s)",
                                adds / duration.as_secs(),
                                adds,
                                polls / duration.as_secs(),
                                polls,
                                removes / duration.as_secs(),
                                removes
                            );

                            add_count.store(0, atomic::Ordering::Relaxed);
                            poll_count.store(0, atomic::Ordering::Relaxed);
                            remove_count.store(0, atomic::Ordering::Relaxed);
                        }
                    }
                });

                // spawn polling clients
                for _ in 0..polling_clients {
                    let poll_count = Arc::clone(&poll_count);
                    let remove_count = Arc::clone(&remove_count);
                    let handle = Handle::current();
                    s.spawn(move || {
                        handle.block_on(async move {
                            tokio::task::LocalSet::new()
                                .run_until(async move {
                                    with_client(addr, |queue_client| async move {
                                        loop {
                                            let mut request = queue_client.poll_request();
                                            let mut req = request.get().init_req();
                                            req.set_lease_validity_secs(30);
                                            req.set_num_items(10);
                                            req.set_timeout_secs(5);
                                            let reply = request.send().promise.await.unwrap();
                                            let items = reply
                                                .get()
                                                .unwrap()
                                                .get_resp()
                                                .unwrap()
                                                .get_items()
                                                .unwrap();
                                            poll_count.fetch_add(
                                                items.len() as u64,
                                                atomic::Ordering::Relaxed,
                                            );

                                            let lease = reply
                                                .get()
                                                .unwrap()
                                                .get_resp()
                                                .unwrap()
                                                .get_lease()
                                                .unwrap();

                                            // then remove the items
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
                                    .await
                                    .unwrap();
                                })
                                .await;
                        });
                    });
                }

                for _ in 0..adding_clients {
                    let add_count = Arc::clone(&add_count);
                    let handle = Handle::current();
                    s.spawn(move || {
                        handle.block_on(async move {
                            with_client(addr, |queue_client| async move {
                                loop {
                                    let mut request = queue_client.add_request();
                                    let req = request.get().init_req();
                                    let mut items = req.init_items(10);
                                    for i in 0..10 {
                                        let mut item = items.reborrow().get(i);
                                        item.set_contents(format!("test {}", i).as_bytes());
                                        item.set_visibility_timeout_secs(3);
                                    }
                                    let _ = request.send().promise.await.unwrap();
                                    add_count.fetch_add(10, atomic::Ordering::Relaxed);
                                }
                            })
                            .await
                            .unwrap();
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
            let _jh = tokio::task::spawn_local(rpc_system);
            f(queue_client).await
        })
        .await)
}
