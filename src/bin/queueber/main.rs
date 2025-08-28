use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::Parser;
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::{
    metrics,
    metrics_server::MetricsServer,
    server::Server,
    storage::{RetriedStorage, Storage},
};
use socket2::{Domain, Protocol, Socket, Type};
use std::sync::Arc;
use tokio::sync::Notify;
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

    /// Number of RPC worker threads. Defaults to available_parallelism.
    #[arg(long = "workers")]
    workers: Option<usize>,

    /// Metrics server port. Defaults to disabled metrics server.
    #[arg(long = "metrics-port")]
    metrics_port: Option<u16>,
}

// NOTE: to use the console you need "RUST_LOG=tokio=trace,runtime=trace"

fn compute_worker_count(arg_workers: Option<usize>) -> usize {
    match arg_workers {
        Some(n) if n > 0 => n,
        _ => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    }
}

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

    // Initialize metrics subsystem
    metrics::init();
    tracing::info!("Metrics initialized");

    // Start metrics server if requested
    if let Some(metrics_port) = args.metrics_port {
        let metrics_addr = SocketAddr::new(addr.ip(), metrics_port);
        let metrics_server = MetricsServer::new(metrics_addr);
        tokio::spawn(async move {
            if let Err(e) = metrics_server.run().await {
                tracing::error!("Metrics server error: {}", e);
            }
        });
        tracing::info!("Metrics server started on {}", metrics_addr);
    } else {
        tracing::info!("Metrics server disabled (use --metrics-port to enable)");
    }

    std::fs::create_dir_all(&args.data_dir)?;
    if args.wipe {
        std::fs::remove_dir_all(&args.data_dir)?;
    }

    let storage = Arc::new(RetriedStorage::new(Storage::new(&args.data_dir)?));
    let notify = Arc::new(Notify::new());
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn deduplicated background tasks once
    queueber::server::spawn_background_tasks(
        Arc::clone(&storage),
        Arc::clone(&notify),
        shutdown_tx.clone(),
    );

    // Build a small pool of RPC workers. Each worker runs a single-threaded runtime with a LocalSet
    let worker_count = compute_worker_count(args.workers);
    tracing::info!("using {} worker threads", worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for i in 0..worker_count {
        let storage_cloned = Arc::clone(&storage);
        let notify_cloned = Arc::clone(&notify);
        let shutdown_tx_cloned = shutdown_tx.clone();
        let mut shutdown_rx_worker = shutdown_rx.clone();
        let addr_for_worker: SocketAddr = addr;

        let thread_name = format!("rpc-worker-{}", i);
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build worker runtime");
                rt.block_on(async move {
                    let server = Server::new(storage_cloned, notify_cloned, shutdown_tx_cloned);
                    let queue_client: queueber::protocol::queue::Client =
                        capnp_rpc::new_client(server);
                    // Each worker owns one Server; clone client per-connection.
                    let local = tokio::task::LocalSet::new();
                    local
                        .run_until(async move {
                            // Per-worker listener using SO_REUSEPORT
                            let std_listener = bind_reuseport(addr_for_worker).expect("bind reuseport listener");
                            std_listener
                                .set_nonblocking(true)
                                .expect("set nonblocking");
                            let listener = tokio::net::TcpListener::from_std(std_listener)
                                .expect("tokio listener from std");

                            let mut conn_id: u64 = 0;
                            loop {
                                tokio::select! {
                                    _ = async { if *shutdown_rx_worker.borrow() { } else { let _ = shutdown_rx_worker.changed().await; } } => {
                                        break;
                                    }
                                    accept_res = listener.accept() => {
                                        let (stream, _) = accept_res.expect("accept ok");
                                        let _ = stream.set_nodelay(true);
                                        let client = queue_client.clone();
                                        let this_conn = conn_id;
                                        conn_id = conn_id.wrapping_add(1);
                                        let _jh = tokio::task::Builder::new()
                                            .name("rpc_server")
                                            .spawn_local(async move {
                                                let (reader, writer) =
                                                    tokio_util::compat::TokioAsyncReadCompatExt::compat(
                                                        stream,
                                                    )
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
                                                let _ = this_conn; // reserved for future naming/metrics
                                            })
                                            .unwrap();
                                    }
                                }
                            }
                        })
                        .await;
                });
                tracing::info!("worker thread {} exiting", i);
            })
            .expect("spawn worker thread");
        worker_handles.push(handle);
    }

    // Wait for shutdown signal, then join workers
    let _ = async {
        if *shutdown_rx.borrow() {
        } else {
            let _ = shutdown_rx.changed().await;
        }
    }
    .await;
    tracing::info!("shutdown signal received; joining workers");
    for handle in worker_handles {
        let _ = handle.join();
    }
    tracing::info!("workers joined; main loop exiting");
    Ok(())
}

fn bind_reuseport(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    // SO_REUSEPORT for per-worker accept
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    // Larger backlog can help under high accept rates
    socket.listen(4096)?;
    Ok(socket.into())
}

#[cfg(test)]
mod tests {
    use super::{bind_reuseport, compute_worker_count};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn compute_workers_uses_arg_when_provided() {
        assert_eq!(compute_worker_count(Some(4)), 4);
    }

    #[test]
    fn compute_workers_falls_back_to_available_parallelism() {
        let n = compute_worker_count(None);
        assert!(n >= 1);
    }

    #[test]
    fn bind_reuseport_allows_multiple_binds() {
        // Bind first on ephemeral port 0
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let l1 = bind_reuseport(addr1).expect("first bind");
        let port = l1.local_addr().unwrap().port();
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        // Second bind on same port should succeed due to SO_REUSEPORT
        let _l2 = bind_reuseport(addr2).expect("second bind");
    }
}
