use capnp::message::Builder as CapnpBuilder;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use queueber::protocol;
use queueber::protocol::queue;
use queueber::storage::{RetriedStorage, Storage};
// rand no longer needed; retries removed
use std::net::{SocketAddr, TcpListener};
use std::sync::OnceLock;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, atomic};
use std::thread::JoinHandle;

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
            |(_dir, storage)| -> Result<(), queueber::errors::Error> {
                // Build the item once and reuse it; only IDs change per insert
                let mut msg = CapnpBuilder::new_default();
                let mut item = msg.init_root::<protocol::item::Builder>();
                item.set_contents(b"hello");
                item.set_visibility_timeout_secs(0);
                let item_reader = item.into_reader();

                for i in 0..num_items {
                    let id = format!("id_{}", i);
                    track_and_ignore_busy_error(
                        storage.add_available_item((id.as_bytes(), item_reader.reborrow())),
                    )?;
                }
                Ok(())
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

                let (lease, polled) = storage.get_next_available_entries(num_items).expect("poll");

                let mut ids: Vec<Vec<u8>> = Vec::with_capacity(polled.len());
                for typed in polled.into_iter() {
                    let reader = typed.get().expect("typed reader");
                    ids.push(reader.get_id().expect("id").to_vec());
                }

                (dir, storage, lease, ids)
            },
            |(_dir, storage, lease, ids)| -> Result<(), queueber::errors::Error> {
                for id in ids {
                    track_and_ignore_busy_error(storage.remove_in_progress_item(&id, &lease))?;
                }
                Ok(())
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
            |(_dir, storage)| -> Result<(), queueber::errors::Error> {
                let _ = track_and_ignore_busy_error(storage.get_next_available_entries(num_items))?;
                Ok(())
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
                use queueber::protocol::queue::Client as QueueClient;
                use queueber::server::Server;
                use std::sync::Arc;

                let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
                let storage = Arc::new(RetriedStorage::new(Storage::new(&data_path).unwrap()));
                let (shutdown_tx, _rx) = tokio::sync::watch::channel(false);
                let notify = Arc::new(tokio::sync::Notify::new());
                let server = Server::new(storage, notify, shutdown_tx);
                let queue_client: QueueClient = capnp_rpc::new_client(server);
                // Signal readiness once bound and client is created
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
                            let rpc_system = RpcSystem::new(
                                Box::new(network),
                                Some(queue_client.clone().client),
                            );
                            let _jh = tokio::task::Builder::new()
                                .name("rpc_system")
                                .spawn_local(rpc_system);
                        }
                    })
                    .await;
            });
        });
        // Wait until the server thread signals readiness
        let _ = ready_rx.recv();
        ServerHandle {
            _thread: thread,
            _data_dir: data_dir,
            addr,
        }
    })
}

fn with_client<F, Fut, R>(addr: SocketAddr, f: F) -> R
where
    F: FnOnce(queue::Client) -> Fut,
    Fut: std::future::Future<Output = R>,
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
                let _jh = tokio::task::Builder::new()
                    .name("rpc_system")
                    .spawn_local(rpc_system);
                f(queue_client).await
            })
            .await
    })
}

fn bench_e2e_add_poll_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_rpc");
    group.measurement_time(std::time::Duration::from_secs(15));
    let num_items: u32 = 200;
    let handle = ensure_server_started();
    let addr = handle.addr;

    // Add batch
    group.bench_function(format!("rpc_add_batch_{}", num_items), |b| {
        b.iter(|| {
            with_client(addr, |queue_client| async move {
                let mut request = queue_client.add_request();
                let req = request.get().init_req();
                let mut items = req.init_items(num_items);
                for i in 0..num_items {
                    let mut item = items.reborrow().get(i);
                    item.set_contents(b"hello");
                    item.set_visibility_timeout_secs(0);
                }
                let reply = request.send().promise.await.unwrap();
                let _ids = reply.get().unwrap().get_resp().unwrap().get_ids().unwrap();
            })
        });
    });

    // Poll batch (setup inserts first)
    group.bench_function(format!("rpc_poll_batch_{}", num_items), |b| {
        b.iter_batched(
            || {
                // preload N items
                with_client(addr, |queue_client| async move {
                    let mut request = queue_client.add_request();
                    let req = request.get().init_req();
                    let mut items = req.init_items(num_items);
                    for i in 0..num_items {
                        let mut item = items.reborrow().get(i);
                        item.set_contents(b"hello");
                        item.set_visibility_timeout_secs(0);
                    }
                    let _ =
                        track_capnp_and_ignore_busy(request.send().promise.await, "rpc_add_batch");
                });
            },
            |_| {
                with_client(addr, |queue_client| async move {
                    let mut request = queue_client.poll_request();
                    let mut req = request.get().init_req();
                    req.set_lease_validity_secs(30);
                    req.set_num_items(num_items);
                    req.set_timeout_secs(0);
                    let _ =
                        track_capnp_and_ignore_busy(request.send().promise.await, "rpc_poll_batch");
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
                with_client(addr, |queue_client| async move {
                    let mut add = queue_client.add_request();
                    let req = add.get().init_req();
                    let mut items = req.init_items(num_items);
                    for i in 0..num_items {
                        let mut item = items.reborrow().get(i);
                        item.set_contents(b"hello");
                        item.set_visibility_timeout_secs(0);
                    }
                    let _ = track_capnp_and_ignore_busy(
                        add.send().promise.await,
                        "rpc_remove_setup_add",
                    );

                    let mut poll = queue_client.poll_request();
                    let mut preq = poll.get().init_req();
                    preq.set_lease_validity_secs(30);
                    preq.set_num_items(num_items);
                    preq.set_timeout_secs(0);
                    let reply = match poll.send().promise.await {
                        Ok(r) => r,
                        Err(_) => return (vec![], vec![]),
                    };
                    let resp = reply.get().unwrap().get_resp().unwrap();
                    let lease = resp.get_lease().unwrap().to_vec();
                    let items = resp.get_items().unwrap();
                    let mut ids: Vec<Vec<u8>> = Vec::with_capacity(items.len() as usize);
                    for i in 0..items.len() {
                        ids.push(items.get(i).get_id().unwrap().to_vec());
                    }
                    (lease, ids)
                })
            },
            |(lease, ids)| {
                with_client(addr, |queue_client| async move {
                    for id in ids {
                        let mut req = queue_client.remove_request();
                        let mut r = req.get().init_req();
                        r.set_id(&id);
                        r.set_lease(&lease);
                        let _ = track_capnp_and_ignore_busy(
                            req.send().promise.await,
                            "rpc_remove_each",
                        );
                    }
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_e2e_stress_like(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_rpc");
    group.measurement_time(std::time::Duration::from_secs(15));
    // Parameters inspired by stress.sh defaults but with bounded total work per iteration.
    let adding_clients: u32 = 2;
    let polling_clients: u32 = 2;
    let total_items: u32 = 200;
    let batch_size: u32 = 10;

    let handle = ensure_server_started();
    let addr = handle.addr;

    group.bench_function(
        format!(
            "rpc_stress_like_p{}_a{}_items_{}",
            polling_clients, adding_clients, total_items
        ),
        |b| {
            b.iter(|| {
                // Work counters
                let remaining_to_add = Arc::new(atomic::AtomicU32::new(total_items));
                let removed_count = Arc::new(atomic::AtomicU32::new(0));

                std::thread::scope(|s| {
                    // Spawn polling clients
                    for _ in 0..polling_clients {
                        let removed_count = Arc::clone(&removed_count);
                        s.spawn(move || {
                            with_client(addr, |queue_client| async move {
                                // Keep polling/removing until we've removed all items for this run
                                let mut current_lease: Option<[u8; 16]> = None;
                                let mut last_extend = std::time::Instant::now();
                                loop {
                                    let done = removed_count.load(atomic::Ordering::Relaxed)
                                        >= total_items;
                                    if done {
                                        break;
                                    }

                                    let mut request = queue_client.poll_request();
                                    let mut req = request.get().init_req();
                                    req.set_lease_validity_secs(30);
                                    req.set_num_items(batch_size);
                                    req.set_timeout_secs(1);
                                    let reply = match request.send().promise.await {
                                        Ok(r) => r,
                                        Err(_) => continue,
                                    };
                                    let resp = reply.get().unwrap().get_resp().unwrap();
                                    let items = resp.get_items().unwrap();
                                    let lease = resp.get_lease().unwrap();
                                    let items_len = items.len();
                                    let lease_vec = lease.to_vec();
                                    let lease = &lease_vec;
                                    if lease.len() == 16 {
                                        let mut lease_arr = [0u8; 16];
                                        lease_arr.copy_from_slice(lease);
                                        current_lease = Some(lease_arr);
                                    }

                                    if items_len == 0 {
                                        continue;
                                    }

                                    // Remove up to batch_size items by issuing batch_size remove calls.
                                    // We don't have the ids from the fast path above, so poll again immediately with num_items=batch_size
                                    let mut preq = queue_client.poll_request();
                                    let mut pr = preq.get().init_req();
                                    pr.set_lease_validity_secs(30);
                                    pr.set_num_items(batch_size);
                                    pr.set_timeout_secs(0);
                                    if let Ok(reply) = preq.send().promise.await {
                                        let resp = reply.get().unwrap().get_resp().unwrap();
                                        let items = resp.get_items().unwrap();
                                        let lease2 = resp.get_lease().unwrap();
                                        let promises = items.iter().map(|i| {
                                            let mut request = queue_client.remove_request();
                                            let mut r = request.get().init_req();
                                            r.set_id(i.get_id().unwrap());
                                            r.set_lease(lease2);
                                            request.send().promise
                                        });
                                        let _ = futures::future::join_all(promises).await;
                                        removed_count
                                            .fetch_add(items.len(), atomic::Ordering::Relaxed);
                                    }

                                    // Occasionally extend the current lease to exercise extend path
                                    if last_extend.elapsed() > std::time::Duration::from_secs(2) {
                                        if let Some(lease_arr) = current_lease {
                                            let mut xreq = queue_client.extend_request();
                                            let mut xr = xreq.get().init_req();
                                            xr.set_lease(&lease_arr);
                                            xr.set_lease_validity_secs(30);
                                            let _ = track_capnp_and_ignore_busy(
                                                xreq.send().promise.await,
                                                "rpc_stress_extend",
                                            );
                                        }
                                        last_extend = std::time::Instant::now();
                                    }
                                }
                            });
                        });
                    }

                    // Spawn adding clients
                    for _ in 0..adding_clients {
                        let remaining_to_add = Arc::clone(&remaining_to_add);
                        s.spawn(move || {
                            with_client(addr, |queue_client| async move {
                                loop {
                                    // Reserve a batch to add
                                    let batch = loop {
                                        let prev = remaining_to_add.load(atomic::Ordering::Relaxed);
                                        if prev == 0 {
                                            break 0;
                                        }
                                        let take = prev.min(batch_size);
                                        if remaining_to_add
                                            .compare_exchange(
                                                prev,
                                                prev - take,
                                                atomic::Ordering::Relaxed,
                                                atomic::Ordering::Relaxed,
                                            )
                                            .is_ok()
                                        {
                                            break take;
                                        }
                                    };

                                    if batch == 0 {
                                        break;
                                    }

                                    let mut request = queue_client.add_request();
                                    let req = request.get().init_req();
                                    let mut items = req.init_items(batch);
                                    for i in 0..batch as usize {
                                        let mut item = items.reborrow().get(i as u32);
                                        // Small payload, immediately visible
                                        item.set_contents(b"p");
                                        item.set_visibility_timeout_secs(0);
                                    }
                                    let _ = track_capnp_and_ignore_busy(
                                        request.send().promise.await,
                                        "rpc_stress_add",
                                    );
                                }
                            });
                        });
                    }
                });
            })
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_add_messages,
    bench_remove_messages,
    bench_poll_messages_storage,
    bench_e2e_add_poll_remove,
    bench_e2e_stress_like
);
criterion_main!(benches);

// retry_capnp_busy removed; ignoring Busy errors inline instead

fn track_and_ignore_busy_error<T>(
    res: Result<T, queueber::errors::Error>,
) -> Result<Option<T>, queueber::errors::Error> {
    match res {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            if let queueber::errors::Error::Rocksdb { ref source, .. } = e
                && source.kind() == rocksdb::ErrorKind::Busy
            {
                eprintln!("busy (ignored)");
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

fn track_capnp_and_ignore_busy<T>(res: Result<T, capnp::Error>, label: &str) -> Option<T> {
    match res {
        Ok(v) => Some(v),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Busy") || msg.contains("busy") || msg.contains("resource busy") {
                eprintln!("{}: busy (ignored)", label);
                None
            } else {
                panic!("{}", e);
            }
        }
    }
}
