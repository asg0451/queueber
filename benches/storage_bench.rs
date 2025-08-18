use capnp::message::Builder as CapnpBuilder;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use queueber::protocol;
use queueber::storage::Storage;
use std::net::{SocketAddr, TcpListener};
use std::sync::OnceLock;
use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use queueber::protocol::queue;

fn bench_add_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_add");

    // Keep it simple and reasonably fast for CI
    let num_items: usize = 500;

    group.bench_function(format!("add_available_{}", num_items), |b| {
        b.iter_batched(
            || {
                let dir = tempfile::TempDir::new().expect("tmp dir");
                let storage = Storage::new(dir.path()).expect("storage");
                // Keep dir alive with storage by returning it as part of the state
                (dir, storage)
            },
            |(_dir, storage)| {
                // Build the item once and reuse it; only IDs change per insert
                let mut msg = CapnpBuilder::new_default();
                let mut item = msg.init_root::<protocol::item::Builder>();
                item.set_contents(b"hello");
                item.set_visibility_timeout_secs(0);
                let item_reader = item.into_reader();

                for i in 0..num_items {
                    let id = format!("id_{}", i);
                    storage
                        .add_available_item((id.as_bytes(), item_reader.reborrow()))
                        .expect("add_available_item");
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_remove_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_remove");

    // Keep it simple and reasonably fast for CI
    let num_items: usize = 500;

    group.bench_function(format!("remove_in_progress_{}", num_items), |b| {
        b.iter_batched(
            || {
                // Setup a fresh DB, insert N items, poll them to move to in_progress
                let dir = tempfile::TempDir::new().expect("tmp dir");
                let storage = Storage::new(dir.path()).expect("storage");

                // Build one reusable item
                let mut msg = CapnpBuilder::new_default();
                let mut item = msg.init_root::<protocol::item::Builder>();
                item.set_contents(b"hello");
                item.set_visibility_timeout_secs(0);
                let item_reader = item.into_reader();

                for i in 0..num_items {
                    let id = format!("id_{}", i);
                    storage
                        .add_available_item((id.as_bytes(), item_reader.reborrow()))
                        .expect("add_available_item");
                }

                let (lease, polled) = storage
                    .get_next_available_entries(num_items)
                    .expect("poll");

                let mut ids: Vec<Vec<u8>> = Vec::with_capacity(polled.len());
                for typed in polled.into_iter() {
                    let reader = typed.get().expect("typed reader");
                    ids.push(reader.get_id().expect("id").to_vec());
                }

                (dir, storage, lease, ids)
            },
            |(_dir, storage, lease, ids)| {
                for id in ids {
                    let removed = storage
                        .remove_in_progress_item(&id, &lease)
                        .expect("remove_in_progress_item");
                    assert!(removed, "expected removal for id");
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_poll_messages_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_poll");

    let num_items: usize = 500;

    group.bench_function(format!("get_next_available_entries_{}", num_items), |b| {
        b.iter_batched(
            || {
                // Fresh DB with preloaded items
                let dir = tempfile::TempDir::new().expect("tmp dir");
                let storage = Storage::new(dir.path()).expect("storage");
                let mut msg = CapnpBuilder::new_default();
                let mut item = msg.init_root::<protocol::item::Builder>();
                item.set_contents(b"hello");
                item.set_visibility_timeout_secs(0);
                let item_reader = item.into_reader();
                for i in 0..num_items {
                    let id = format!("id_{}", i);
                    storage
                        .add_available_item((id.as_bytes(), item_reader.reborrow()))
                        .expect("add_available_item");
                }
                (dir, storage)
            },
            |(_dir, storage)| {
                let _ = storage
                    .get_next_available_entries(num_items)
                    .expect("poll");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// =============== End-to-end benches (server + RPC client) ===============

struct ServerHandle {
    _thread: JoinHandle<()>,
    _data_dir: tempfile::TempDir,
    addr: SocketAddr,
}

static SERVER: OnceLock<ServerHandle> = OnceLock::new();

fn ensure_server_started() -> &'static ServerHandle {
    SERVER.get_or_init(|| {
        // Prepare data dir and pick an OS-assigned port by binding first
        let data_dir = tempfile::TempDir::new().expect("tmp dir");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);

        // Spawn server on a separate thread
        let data_path = data_dir.path().to_path_buf();
        let (ready_tx, ready_rx) = sync_channel::<()>(1);
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async move {
                use queueber::server::Server;
                use queueber::protocol::queue::Client as QueueClient;
                use std::sync::Arc;
                use tokio::sync::Notify;
                let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
                let storage = Arc::new(Storage::new(&data_path).unwrap());
                let notify = Arc::new(Notify::new());
                let server = Server::new(storage, notify);
                let queue_client: QueueClient = capnp_rpc::new_client(server);
                // Signal readiness once bound and client is created
                let _ = ready_tx.send(());

                tokio::task::LocalSet::new()
                    .run_until(async move {
                        loop {
                            let (stream, _) = listener.accept().await.unwrap();
                            stream.set_nodelay(true).unwrap();
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
                                RpcSystem::new(Box::new(network), Some(queue_client.clone().client));
                            let _jh = tokio::task::spawn_local(rpc_system);
                        }
                    })
                    .await;
            });
        });
        // Wait until the server thread signals readiness
        let _ = ready_rx.recv();
        ServerHandle { _thread: thread, _data_dir: data_dir, addr }
    })
}

fn with_client<F, R>(addr: SocketAddr, f: F) -> R
where
    F: FnOnce(queue::Client) -> R,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    rt.block_on(async move {
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
                let _jh = tokio::task::spawn_local(rpc_system);
                f(queue_client)
            })
            .await
    })
}

fn bench_e2e_add_poll_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_rpc");
    let num_items: u32 = 200;
    let handle = ensure_server_started();
    let addr = handle.addr;

    // Add batch
    group.bench_function(format!("rpc_add_batch_{}", num_items), |b| {
        b.iter(|| {
            with_client(addr, |queue_client| {
                let mut request = queue_client.add_request();
                let req = request.get().init_req();
                let mut items = req.init_items(num_items);
                for i in 0..num_items {
                    let mut item = items.reborrow().get(i);
                    item.set_contents(b"hello");
                    item.set_visibility_timeout_secs(0);
                }
                // Block until reply
                let _ = futures::executor::block_on(async move {
                    let reply = request.send().promise.await.unwrap();
                    let _ids = reply.get().unwrap().get_resp().unwrap().get_ids().unwrap();
                    Ok::<(), ()>(())
                });
            })
        });
    });

    // Poll batch (setup inserts first)
    group.bench_function(format!("rpc_poll_batch_{}", num_items), |b| {
        b.iter_batched(
            || {
                // preload N items
                with_client(addr, |queue_client| {
                    let mut request = queue_client.add_request();
                    let req = request.get().init_req();
                    let mut items = req.init_items(num_items);
                    for i in 0..num_items {
                        let mut item = items.reborrow().get(i);
                        item.set_contents(b"hello");
                        item.set_visibility_timeout_secs(0);
                    }
                    futures::executor::block_on(async move {
                        let _ = request.send().promise.await.unwrap();
                    });
                });
            },
            |_| {
                with_client(addr, |queue_client| {
                    let mut request = queue_client.poll_request();
                    let mut req = request.get().init_req();
                    req.set_lease_validity_secs(30);
                    req.set_num_items(num_items);
                    req.set_timeout_secs(0);
                    let _ = futures::executor::block_on(async move {
                        let reply = request.send().promise.await.unwrap();
                        let _resp = reply.get().unwrap().get_resp().unwrap();
                        Ok::<(), ()>(())
                    });
                });
            },
            BatchSize::SmallInput,
        );
    });

    // Remove all individually (setup: add + poll to get lease and ids)
    group.bench_function(format!("rpc_remove_each_{}", num_items), |b| {
        b.iter_batched(
            || {
                // Preload and poll to get lease and ids
                with_client(addr, |queue_client| {
                    let mut add = queue_client.add_request();
                    let req = add.get().init_req();
                    let mut items = req.init_items(num_items);
                    for i in 0..num_items {
                        let mut item = items.reborrow().get(i);
                        item.set_contents(b"hello");
                        item.set_visibility_timeout_secs(0);
                    }
                    let (lease, ids) = {
                        let client_for_poll = queue_client.clone();
                        futures::executor::block_on(async move {
                            let _ = add.send().promise.await.unwrap();
                            let mut poll = client_for_poll.poll_request();
                            let mut preq = poll.get().init_req();
                            preq.set_lease_validity_secs(30);
                            preq.set_num_items(num_items);
                            preq.set_timeout_secs(0);
                            let reply = poll.send().promise.await.unwrap();
                            let resp = reply.get().unwrap().get_resp().unwrap();
                            let lease = resp.get_lease().unwrap().to_vec();
                            let items = resp.get_items().unwrap();
                            let mut ids: Vec<Vec<u8>> =
                                Vec::with_capacity(items.len() as usize);
                            for i in 0..items.len() {
                                ids.push(items.get(i).get_id().unwrap().to_vec());
                            }
                            (lease, ids)
                        })
                    };
                    (queue_client, lease, ids)
                })
            },
            |(queue_client, lease, ids)| {
                for id in ids {
                    let mut req = queue_client.remove_request();
                    let mut r = req.get().init_req();
                    r.set_id(&id);
                    r.set_lease(&lease);
                    let _ = futures::executor::block_on(async move {
                        let _ = req.send().promise.await.unwrap();
                        Ok::<(), ()>(())
                    });
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_add_messages,
    bench_remove_messages,
    bench_poll_messages_storage,
    bench_e2e_add_poll_remove
);
criterion_main!(benches);

