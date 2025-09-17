use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::sync_channel;
use std::time::Duration;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use queueber::protocol::queue;
use queueber::storage::{RetriedStorage, Storage};
use tokio::sync::watch;

#[test]
fn storage_durability_reopen_persists_available_items() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Open, add items, then drop
    {
        let storage = Storage::new(tmp.path()).expect("storage open");
        storage
            .add_available_item_from_parts(b"id1", b"a", 0)
            .expect("add 1");
        storage
            .add_available_item_from_parts(b"id2", b"b", 0)
            .expect("add 2");
        // Drop storage by leaving scope
    }

    // Reopen and verify items are still present
    let storage = Storage::new(tmp.path()).expect("storage reopen");
    let (_lease, items) = storage
        .get_next_available_entries_with_lease(2, 30)
        .expect("poll after reopen");
    assert_eq!(items.len(), 2, "expected two items after reopen");
}

struct TestServerHandle {
    _thread: Option<std::thread::JoinHandle<()>>,
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl Drop for TestServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(h) = self._thread.take() {
            let _ = h.join();
        }
    }
}

fn start_server_on(data_dir: std::path::PathBuf, addr: SocketAddr) -> TestServerHandle {
    // Signal readiness
    let (ready_tx, ready_rx) = sync_channel::<()>(1);

    let (shutdown_tx, _shutdown_rx_unused) = watch::channel(false);
    let shutdown_tx_for_handle = shutdown_tx.clone();
    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async move {
            use queueber::protocol::queue::Client as QueueClient;
            use queueber::server::Server;
            use std::sync::Arc;
            use tokio::sync::Notify;
            let mut shutdown_rx = shutdown_tx.subscribe();
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let storage = Arc::new(RetriedStorage::new(Storage::new(&data_dir).unwrap()));
            let notify = Arc::new(Notify::new());
            queueber::server::spawn_background_tasks(
                Arc::clone(&storage),
                Arc::clone(&notify),
                shutdown_tx.clone(),
            );
            let server = Server::new(storage, notify, shutdown_tx.clone());
            let queue_client: QueueClient = capnp_rpc::new_client(server);

            let _ = ready_tx.send(());

            tokio::task::LocalSet::new()
                .run_until(async move {
                    loop {
                        tokio::select! {
                            _ = async {
                                if *shutdown_rx.borrow() { } else { let _ = shutdown_rx.changed().await; }
                            } => { break; }
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
                                let _jh = tokio::task::Builder::new()
                                    .name("rpc_system")
                                    .spawn_local(rpc_system)
                                    .unwrap();
                            }
                        }
                    }
                })
                .await;
        });
    });

    let _ = ready_rx.recv();
    TestServerHandle {
        _thread: Some(thread),
        addr,
        shutdown_tx: shutdown_tx_for_handle,
    }
}

async fn with_client<F, Fut, R>(addr: SocketAddr, f: F) -> R
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
    tokio::task::LocalSet::new()
        .run_until(async move {
            let _jh = tokio::task::Builder::new()
                .name("rpc_system")
                .spawn_local(rpc_system)
                .unwrap();
            f(queue_client).await
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn server_restart_requeues_after_lease_expiry() {
    // Choose an address and hold it for inspection
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();

    // Start server #1
    let handle1 = start_server_on(data_dir.clone(), addr);

    // Add an item and poll it with a short lease so it becomes in-progress
    let lease = with_client(handle1.addr, |client| async move {
        // Add visible item
        let mut add = client.add_request();
        {
            let req = add.get().init_req();
            let mut items = req.init_items(1);
            let mut item = items.reborrow().get(0);
            item.set_contents(b"persist-me");
            item.set_visibility_timeout_secs(0);
        }
        let _ = add.send().promise.await.unwrap();

        // Poll with 1s lease
        let mut poll = client.poll_request();
        {
            let mut req = poll.get().init_req();
            req.set_lease_validity_secs(1);
            req.set_num_items(1);
            req.set_timeout_secs(0);
        }
        let reply = poll.send().promise.await.unwrap();
        let resp = reply.get().unwrap().get_resp().unwrap();
        resp.get_lease().unwrap().to_vec()
    })
    .await;

    assert!(!lease.is_empty());

    // Drop server #1
    drop(handle1);

    // Restart server #2 on the same addr and data dir
    let _handle2 = start_server_on(data_dir.clone(), addr);

    // Wait enough for the original lease to expire and background sweeper to run
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Now a poll should return the item again (re-queued after expiry)
    let got = with_client(addr, |client| async move {
        let mut poll = client.poll_request();
        {
            let mut req = poll.get().init_req();
            req.set_lease_validity_secs(30);
            req.set_num_items(1);
            req.set_timeout_secs(1);
        }
        let reply = poll.send().promise.await.unwrap();
        let resp = reply.get().unwrap().get_resp().unwrap();
        resp.get_items().unwrap().len()
    })
    .await;

    assert_eq!(
        got, 1,
        "expected item to be re-queued after restart + expiry"
    );
}
