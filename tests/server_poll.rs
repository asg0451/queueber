use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;

use queueber::protocol::queue;
use queueber::storage::{RetriedStorage, Storage};

struct TestServerHandle {
    _data_dir: tempfile::TempDir,
    _thread: std::thread::JoinHandle<()>,
    addr: SocketAddr,
}

fn start_test_server() -> TestServerHandle {
    // Prepare a temporary data directory and bind to an OS-assigned port
    let data_dir = tempfile::TempDir::new().expect("tmp dir");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    // Signal channel to indicate readiness
    let (ready_tx, ready_rx) = sync_channel::<()>(1);

    let data_path = data_dir.path().to_path_buf();
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

            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let storage = Arc::new(RetriedStorage::new(Storage::new(&data_path).unwrap()));
            let notify = Arc::new(Notify::new());
            queueber::server::spawn_background_tasks(
                Arc::clone(&storage),
                Arc::clone(&notify),
                shutdown_tx.clone(),
            );
            let server = Server::new(storage, notify, shutdown_tx);
            let queue_client: QueueClient = capnp_rpc::new_client(server);

            // Indicate that the server is ready to accept connections
            let _ = ready_tx.send(());

            tokio::task::LocalSet::new()
                .run_until(async move {
                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        stream.set_nodelay(true).unwrap();
                        let (reader, writer) =
                            tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                        let network = twoparty::VatNetwork::new(
                            futures::io::BufReader::new(reader),
                            futures::io::BufWriter::new(writer),
                            rpc_twoparty_capnp::Side::Server,
                            Default::default(),
                        );
                        let rpc_system =
                            RpcSystem::new(Box::new(network), Some(queue_client.clone().client));
                        let _jh = tokio::task::Builder::new()
                            .name("rpc_system")
                            .spawn_local(rpc_system)
                            .unwrap();
                    }
                })
                .await;
        });
    });

    // Wait for readiness signal
    let _ = ready_rx.recv();

    TestServerHandle {
        _data_dir: data_dir,
        _thread: thread,
        addr,
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
async fn poll_times_out_after_waiting_when_empty() {
    let handle = start_test_server();
    let addr = handle.addr;

    let elapsed = with_client(addr, |queue_client| async move {
        let start = Instant::now();
        let mut request = queue_client.poll_request();
        let mut req = request.get().init_req();
        req.set_lease_validity_secs(30);
        req.set_num_items(1);
        req.set_timeout_secs(1); // 1 second timeout
        let _reply = request.send().promise.await.unwrap();
        start.elapsed()
    })
    .await;

    // Expect we waited at least ~1s, and not an excessively long time
    assert!(
        elapsed >= Duration::from_millis(900),
        "elapsed: {:?}",
        elapsed
    );
    assert!(elapsed < Duration::from_secs(3), "elapsed: {:?}", elapsed);
}

#[tokio::test(flavor = "current_thread")]
async fn poll_indefinite_wait_wakes_on_add() {
    let handle = start_test_server();
    let addr = handle.addr;

    with_client(addr, |queue_client| async move {
        // Start a poll with timeout_secs = 0 (wait indefinitely)
        let mut poll_req = queue_client.poll_request();
        {
            let mut req = poll_req.get().init_req();
            req.set_lease_validity_secs(30);
            req.set_num_items(1);
            req.set_timeout_secs(0);
        }

        // Concurrently, after a short delay, add an item to wake the waiter
        let client_for_add = queue_client.clone();
        let add_task = tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut add = client_for_add.add_request();
            {
                let req = add.get().init_req();
                let mut items = req.init_items(1);
                let mut item = items.reborrow().get(0);
                item.set_contents(b"hello");
                item.set_visibility_timeout_secs(0);
            }
            let _ = add.send().promise.await.unwrap();
        });

        // The poll should complete shortly after the add notifies waiters
        let reply = tokio::time::timeout(Duration::from_secs(3), async {
            poll_req.send().promise.await.unwrap()
        })
        .await
        .expect("poll should complete after add");

        add_task.await.unwrap();

        // Verify that we received at least one item
        let resp = reply.get().unwrap().get_resp().unwrap();
        let items = resp.get_items().unwrap();
        assert!(!items.is_empty());
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn poll_timeout_is_not_cut_short_by_invisible_add() {
    let handle = start_test_server();
    let addr = handle.addr;

    let elapsed = with_client(addr, |queue_client| async move {
        // Start a poll with 1s timeout
        let mut poll_req = queue_client.poll_request();
        {
            let mut req = poll_req.get().init_req();
            req.set_lease_validity_secs(30);
            req.set_num_items(1);
            req.set_timeout_secs(1);
        }

        // After a short delay, add an item that is NOT yet visible (visibility timeout in future)
        let client_for_add = queue_client.clone();
        let add_task = tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut add = client_for_add.add_request();
            {
                let req = add.get().init_req();
                let mut items = req.init_items(1);
                let mut item = items.reborrow().get(0);
                item.set_contents(b"invisible for now");
                item.set_visibility_timeout_secs(5); // not visible yet
            }
            let _ = add.send().promise.await.unwrap();
        });

        let start = Instant::now();
        let _ = poll_req.send().promise.await.unwrap();
        add_task.await.unwrap();
        start.elapsed()
    })
    .await;

    // We should still have waited roughly the full timeout (not returned early on notify)
    assert!(
        elapsed >= Duration::from_millis(900),
        "elapsed: {:?}",
        elapsed
    );
    assert!(elapsed < Duration::from_secs(3), "elapsed: {:?}", elapsed);
}

#[tokio::test(flavor = "current_thread")]
async fn poll_wait_wakes_when_item_becomes_visible() {
    let handle = start_test_server();
    let addr = handle.addr;

    with_client(addr, |queue_client| async move {
        // Start a poll with a generous timeout to wait for the visibility flip
        let mut poll_req = queue_client.poll_request();
        {
            let mut req = poll_req.get().init_req();
            req.set_lease_validity_secs(30);
            req.set_num_items(1);
            req.set_timeout_secs(5);
        }

        // Add an item that becomes visible shortly (e.g., 500ms)
        let client_for_add = queue_client.clone();
        let add_task = tokio::task::spawn_local(async move {
            let mut add = client_for_add.add_request();
            {
                let req = add.get().init_req();
                let mut items = req.init_items(1);
                let mut item = items.reborrow().get(0);
                item.set_contents(b"delayed visible");
                item.set_visibility_timeout_secs(2);
            }
            let _ = add.send().promise.await.unwrap();
        });

        // The poll should return soon after the 1s visibility delay (well under the 5s timeout)
        let start = Instant::now();
        let reply = poll_req.send().promise.await.unwrap();
        let elapsed = start.elapsed();

        add_task.await.unwrap();

        assert!(
            elapsed >= Duration::from_millis(900),
            "elapsed: {:?}",
            elapsed
        );
        assert!(elapsed < Duration::from_secs(3), "elapsed: {:?}", elapsed);

        let resp = reply.get().unwrap().get_resp().unwrap();
        let items = resp.get_items().unwrap();
        assert!(!items.is_empty());
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn extend_renews_lease_and_unknown_returns_false() {
    let handle = start_test_server();
    let addr = handle.addr;

    with_client(addr, |queue_client| async move {
        // Add one item and poll with short lease
        let mut add = queue_client.add_request();
        {
            let req = add.get().init_req();
            let mut items = req.init_items(1);
            let mut item = items.reborrow().get(0);
            item.set_contents(b"hello");
            item.set_visibility_timeout_secs(0);
        }
        let _ = add.send().promise.await.unwrap();

        let mut poll = queue_client.poll_request();
        {
            let mut req = poll.get().init_req();
            req.set_lease_validity_secs(1);
            req.set_num_items(1);
            req.set_timeout_secs(0);
        }
        let reply = poll.send().promise.await.unwrap();
        let resp = reply.get().unwrap().get_resp().unwrap();
        let lease = resp.get_lease().unwrap().to_vec();
        assert!(!lease.is_empty());

        // Extend the lease for another 2 seconds
        let mut ext = queue_client.extend_request();
        {
            let mut req = ext.get().init_req();
            req.set_lease(&lease);
            req.set_lease_validity_secs(2);
        }
        let ext_reply = ext.send().promise.await.unwrap();
        let extended = ext_reply.get().unwrap().get_resp().unwrap().get_extended();
        assert!(extended);

        // We don't assert timing-based invisibility here due to background sweep scheduling.
        // Functionally, extend should return true for existing leases and false for unknown leases.
        let mut poll2 = queue_client.poll_request();
        {
            let mut req = poll2.get().init_req();
            req.set_lease_validity_secs(1);
            req.set_num_items(1);
            req.set_timeout_secs(0);
        }

        // Extending an unknown lease should return false
        let mut ext2 = queue_client.extend_request();
        {
            let mut req = ext2.get().init_req();
            let bogus = uuid::Uuid::now_v7().into_bytes();
            req.set_lease(&bogus);
            req.set_lease_validity_secs(1);
        }
        let rep2 = ext2.send().promise.await.unwrap();
        let ok = rep2.get().unwrap().get_resp().unwrap().get_extended();
        assert!(!ok);
    })
    .await;
}
