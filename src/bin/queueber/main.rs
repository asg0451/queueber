use std::{net::ToSocketAddrs, path::PathBuf};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::Parser;
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::{server::Server, storage::Storage};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

// see https://github.com/capnproto/capnproto-rust/blob/master/example/addressbook_send/addressbook_send.rs
// for how to send stuff across threads; so we can parallelize the work..?

#[derive(Parser, Debug)]
#[command(name = "queueber-server", version, about = "Queueber server")]
struct Args {
    /// Address to listen on (host:port)
    #[arg(short = 'l', long = "listen", default_value = "127.0.0.1:9090")]
    listen: String,

    /// Data directory for RocksDB
    #[arg(short = 'd', long = "data-dir", default_value = "/tmp/queueber/data")]
    data_dir: PathBuf,

    /// Wipe the data directory before starting
    #[arg(short = 'w', long = "wipe")]
    wipe: bool,
}

// NOTE: to use the console you need "RUST_LOG=tokio=trace,runtime=trace"

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with both console and fmt subscribers
    let env_filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(console_subscriber::spawn())
        .init();

    let args = Args::parse();
    let addr = args.listen.to_socket_addrs()?.next().unwrap();

    if args.wipe {
        std::fs::remove_dir_all(&args.data_dir)?;
    }

    let storage = Arc::new(Storage::new(&args.data_dir)?);
    let notify = Arc::new(Notify::new());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Build a small pool of RPC workers. Each worker runs a single-threaded runtime with a LocalSet
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    tracing::info!("using {} worker threads", worker_count);
    let mut senders = Vec::with_capacity(worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, mut rx) = mpsc::channel::<tokio::net::TcpStream>(1024);
        senders.push(tx);

        let storage_cloned = Arc::clone(&storage);
        let notify_cloned = Arc::clone(&notify);
        let shutdown_tx_cloned = shutdown_tx.clone();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build worker runtime");
            rt.block_on(async move {
                let server = Server::new(storage_cloned, notify_cloned, shutdown_tx_cloned);
                let queue_client: queueber::protocol::queue::Client = capnp_rpc::new_client(server);
                // TODO: give this one a name
                let local = tokio::task::LocalSet::new();
                local
                    .run_until(async move {
                        while let Some(stream) = rx.recv().await {
                            let client = queue_client.clone();
                            let _jh = tokio::task::Builder::new()
                                .name("rpc_server")
                                .spawn_local(async move {
                                    let (reader, writer) =
                                        tokio_util::compat::TokioAsyncReadCompatExt::compat(stream)
                                            .split();
                                    let network = twoparty::VatNetwork::new(
                                        futures::io::BufReader::new(reader),
                                        futures::io::BufWriter::new(writer),
                                        rpc_twoparty_capnp::Side::Server,
                                        Default::default(),
                                    );
                                    let rpc_system =
                                        RpcSystem::new(Box::new(network), Some(client.client));
                                    let _jh2 = tokio::task::Builder::new()
                                        .name("rpc_system")
                                        .spawn_local(rpc_system)
                                        .unwrap();
                                })
                                .unwrap();
                        }
                    })
                    .await;
            });
        });
        worker_handles.push(handle);
    }

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let accept_outcome: Result<()> = 'accept: {
        let mut next = 0usize;
        loop {
            tokio::select! {
                _ = async { if *shutdown_rx.borrow() { } else { let _ = shutdown_rx.changed().await; } } => {
                    break 'accept Ok(())
                }
                accept = listener.accept() => {
                    let (stream, _)= accept?;
                    stream.set_nodelay(true)?;
                    let idx = next % senders.len();
                    next = next.wrapping_add(1);
                    senders[idx].send(stream).await?;
                }
            }
        }
    };

    drop(senders);
    for handle in worker_handles {
        let _ = handle.join();
    }
    accept_outcome
}
