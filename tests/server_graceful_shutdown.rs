use std::net::TcpListener;
use std::time::Duration;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio::sync::watch;

#[tokio::test(flavor = "current_thread")]
async fn server_drains_inflight_requests_on_shutdown() {
    // Bind ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Start minimal server like in other tests and signal readiness
    let data_dir = tempfile::TempDir::new().unwrap();
    let data_path = data_dir.path().to_path_buf();
    let (shutdown_tx, _rx_unused) = watch::channel(false);
    let mut shutdown_rx_for_thread = shutdown_tx.subscribe();
    let shutdown_tx_for_later = shutdown_tx.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(1);

    let server_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            use queueber::protocol::queue::Client as QueueClient;
            use queueber::server::Server;
            use queueber::storage::{RetriedStorage, Storage};
            use std::sync::Arc;
            use tokio::sync::Notify;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let storage = Arc::new(RetriedStorage::new(Storage::new(&data_path).unwrap()));
            let notify = Arc::new(Notify::new());
            queueber::server::spawn_background_tasks(
                Arc::clone(&storage),
                Arc::clone(&notify),
                shutdown_tx.clone(),
            );
            let server = Server::new(storage, notify, shutdown_tx.clone());
            let queue_client: QueueClient = capnp_rpc::new_client(server);

            tokio::task::LocalSet::new()
                .run_until(async move {
                    let _ = ready_tx.send(());
                    loop {
                        tokio::select! {
                            _ = async { if *shutdown_rx_for_thread.borrow() { } else { let _ = shutdown_rx_for_thread.changed().await; } } => { break; }
                            accept_res = listener.accept() => {
                                let (stream, _) = accept_res.unwrap();
                                stream.set_nodelay(true).unwrap();
                                let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                                let network = twoparty::VatNetwork::new(
                                    futures::io::BufReader::new(reader),
                                    futures::io::BufWriter::new(writer),
                                    rpc_twoparty_capnp::Side::Server,
                                    Default::default(),
                                );
                                let rpc_system = RpcSystem::new(Box::new(network), Some(queue_client.clone().client));
                                let _jh = tokio::task::Builder::new().name("rpc_system").spawn_local(rpc_system).unwrap();
                            }
                        }
                    }
                })
                .await;
        });
    });

    // Wait until server is ready to accept
    let _ = ready_rx.recv();

    // Create a client and start a long poll that should be in-flight during shutdown
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
    let queue_client: queueber::protocol::queue::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            tokio::task::spawn_local(rpc_system);
            tokio::task::spawn_local(async move {
                let mut poll = queue_client.poll_request();
                {
                    let mut req = poll.get().init_req();
                    req.set_lease_validity_secs(30);
                    req.set_num_items(1);
                    req.set_timeout_secs(2); // longish request
                }
                let _ = poll.send().promise.await; // ignore result
                let _ = done_tx.send(());
            });

            // Let the request run for a short time then initiate shutdown and ensure it drains
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = shutdown_tx_for_later.send(true);

            tokio::time::timeout(Duration::from_secs(5), async move {
                let _ = done_rx.await;
            })
            .await
            .expect("poll finished");
        })
        .await;

    // Join the server thread within a reasonable timeout
    let (tx_join, rx_join) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = server_thread.join();
        let _ = tx_join.send(());
    });
    rx_join
        .recv_timeout(Duration::from_secs(5))
        .expect("server exited");
}
