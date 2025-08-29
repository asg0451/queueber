use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::sync_channel;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use proptest::prelude::ProptestConfig;
use proptest::prelude::*;
use proptest::strategy::Strategy;
use queueber::protocol::queue;
use queueber::storage::{RetriedStorage, Storage};
use tokio::sync::watch;

// Lightweight RPC server harness copied from `tests/server_poll.rs` with minimal changes.
struct TestServerHandle {
    _data_dir: tempfile::TempDir,
    _thread: Option<std::thread::JoinHandle<()>>,
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl Drop for TestServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self._thread.take() {
            let _ = handle.join();
        }
    }
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
            let storage = Arc::new(RetriedStorage::new(Storage::new(&data_path).unwrap()));
            let notify = Arc::new(Notify::new());
            queueber::server::spawn_background_tasks(
                Arc::clone(&storage),
                Arc::clone(&notify),
                shutdown_tx.clone(),
            );
            let server = Server::new(storage, notify, shutdown_tx.clone());
            let queue_client: QueueClient = capnp_rpc::new_client(server);

            // Indicate that the server is ready to accept connections
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
                        }
                    }
                })
                .await;
        });
    });

    // Wait for readiness signal
    let _ = ready_rx.recv();

    TestServerHandle {
        _data_dir: data_dir,
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

#[derive(Clone, Debug)]
enum Op {
    Add {
        contents: Vec<u8>,
        vis_secs: u64,
    },
    Poll {
        n: u32,
        lease_secs: u64,
        timeout_secs: u64,
    },
    Extend {
        lease_secs: u64,
    },
    Remove {
        bogus: bool,
    },
    InvalidExtendZero,
    InvalidPollZeroLease,
}

fn arb_op() -> impl Strategy<Value = Op> {
    let add = (proptest::collection::vec(any::<u8>(), 0..16), (0u64..3u64))
        .prop_map(|(contents, vis_secs)| Op::Add { contents, vis_secs });

    let poll =
        ((1u32..4u32), (1u64..5u64), (0u64..2u64)).prop_map(|(n, lease_secs, timeout_secs)| {
            Op::Poll {
                n,
                lease_secs,
                timeout_secs,
            }
        });

    let extend = (1u64..5u64).prop_map(|lease_secs| Op::Extend { lease_secs });
    let remove = any::<bool>().prop_map(|bogus| Op::Remove { bogus });
    let invalid_ext = Just(Op::InvalidExtendZero);
    let invalid_poll = Just(Op::InvalidPollZeroLease);

    prop_oneof![add, poll, extend, remove, invalid_ext, invalid_poll]
}

const RPC_CALL_TIMEOUT_MS: u64 = 1500;

proptest! {
    #![proptest_config(ProptestConfig { cases: 6, max_shrink_iters: 64, .. ProptestConfig::default() })]
    #[test]
    fn randomized_rpc_ops(op_sequences in proptest::collection::vec(proptest::collection::vec(arb_op(), 10..20), 2..3)) {
        let handle = start_test_server();
        let addr = handle.addr;

        // Each sequence runs on its own thread to introduce concurrency pressure.
        let mut handles = Vec::new();
        for seq in op_sequences {
            let addr_this = addr;
            handles.push(std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                rt.block_on(async move {
                    let current_lease: std::sync::Arc<std::sync::Mutex<Option<Vec<u8>>>> =
                        std::sync::Arc::new(std::sync::Mutex::new(None));
                    for op in seq.into_iter() {
                        match op {
                            Op::Add { contents, vis_secs } => {
                                let _ = with_client(addr_this, |client| async move {
                                    let mut add = client.add_request();
                                    {
                                        let req = add.get().init_req();
                                        let mut items = req.init_items(1);
                                        let mut item = items.reborrow().get(0);
                                        item.set_contents(&contents);
                                        item.set_visibility_timeout_secs(vis_secs);
                                    }
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        add.send().promise.await
                                    }).await;
                                }).await;
                            }
                            Op::Poll { n, lease_secs, timeout_secs } => {
                                let lease_slot = std::sync::Arc::clone(&current_lease);
                                let _ = with_client(addr_this, |client| async move {
                                    let mut poll = client.poll_request();
                                    {
                                        let mut req = poll.get().init_req();
                                        req.set_lease_validity_secs(lease_secs);
                                        req.set_num_items(n);
                                        req.set_timeout_secs(timeout_secs);
                                    }
                                    let res = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        poll.send().promise.await
                                    }).await;
                                    if let Ok(Ok(reply)) = res
                                        && let Ok(resp_reader) = reply.get()
                                        && let Ok(resp) = resp_reader.get_resp()
                                        && let Ok(items) = resp.get_items()
                                        && !items.is_empty()
                                        && let Ok(lease) = resp.get_lease()
                                    {
                                        let mut guard = lease_slot.lock().unwrap();
                                        *guard = Some(lease.to_vec());
                                    }
                                }).await;
                            }
                            Op::Extend { lease_secs } => {
                                let lease = {
                                    let guard = current_lease.lock().unwrap();
                                    guard.clone().unwrap_or_else(|| uuid::Uuid::now_v7().into_bytes().to_vec())
                                };
                                let _ = with_client(addr_this, |client| async move {
                                    let mut ext = client.extend_request();
                                    {
                                        let mut req = ext.get().init_req();
                                        req.set_lease(&lease);
                                        req.set_lease_validity_secs(lease_secs);
                                    }
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        ext.send().promise.await
                                    }).await;
                                }).await;
                            }
                            Op::Remove { bogus } => {
                                let id = uuid::Uuid::now_v7().into_bytes().to_vec();
                                let lease = if bogus {
                                    uuid::Uuid::now_v7().into_bytes().to_vec()
                                } else {
                                    let guard = current_lease.lock().unwrap();
                                    guard.clone().unwrap_or_else(|| uuid::Uuid::now_v7().into_bytes().to_vec())
                                };
                                let _ = with_client(addr_this, |client| async move {
                                    let mut rm = client.remove_request();
                                    {
                                        let mut req = rm.get().init_req();
                                        req.set_id(&id);
                                        req.set_lease(&lease);
                                    }
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        rm.send().promise.await
                                    }).await;
                                }).await;
                            }
                            Op::InvalidExtendZero => {
                                let lease = uuid::Uuid::now_v7().into_bytes().to_vec();
                                let _ = with_client(addr_this, |client| async move {
                                    let mut ext = client.extend_request();
                                    {
                                        let mut req = ext.get().init_req();
                                        req.set_lease(&lease);
                                        req.set_lease_validity_secs(0);
                                    }
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        ext.send().promise.await
                                    }).await;
                                }).await;
                            }
                            Op::InvalidPollZeroLease => {
                                let _ = with_client(addr_this, |client| async move {
                                    let mut poll = client.poll_request();
                                    {
                                        let mut req = poll.get().init_req();
                                        req.set_lease_validity_secs(0);
                                        req.set_num_items(1);
                                        req.set_timeout_secs(0);
                                    }
                                    let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                                        poll.send().promise.await
                                    }).await;
                                }).await;
                            }
                        }
                    }

                    // After sequence, try a short poll to ensure server is still responsive
                    let _ = with_client(addr_this, |client| async move {
                        let mut poll = client.poll_request();
                        {
                            let mut req = poll.get().init_req();
                            req.set_lease_validity_secs(1);
                            req.set_num_items(1);
                            req.set_timeout_secs(0);
                        }
                        let _ = tokio::time::timeout(std::time::Duration::from_millis(RPC_CALL_TIMEOUT_MS), async move {
                            poll.send().promise.await
                        }).await;
                    }).await;
                });
            }));
        }

        for h in handles { let _ = h.join(); }
    }
}
