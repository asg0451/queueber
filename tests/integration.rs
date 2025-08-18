use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use queueber::protocol::queue;
use queueber::storage::Storage;
use std::net::{SocketAddr, TcpListener};
use std::sync::OnceLock;
use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;

#[test]
fn add_poll_block() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let handle = ensure_server_started();
    let addr = handle.addr;

    with_client(
        addr,
        |queue_client| -> Result<(), Box<dyn std::error::Error>> {
            let mut request = queue_client.add_request();
            let req = request.get().init_req();
            let mut items = req.init_items(5);
            for i in 0..5 {
                let mut item = items.reborrow().get(i);
                item.set_contents(format!("hello {}", i).as_bytes());
                item.set_visibility_timeout_secs(0);
            }
            futures::executor::block_on(async move {
                // TODO: hangs here. same issue as in storage_bench.rs/bench_e2e_add_poll_remove prob
                let reply = request.send().promise.await?;
                let _ids = reply.get()?.get_resp()?.get_ids()?;
                Ok::<_, capnp::Error>(())
            })?;
            Ok(())
        },
    )?;

    // Poll to see the ones we just added
    with_client(
        addr,
        |queue_client| -> Result<(), Box<dyn std::error::Error>> {
            let mut request = queue_client.poll_request();
            let mut req = request.get().init_req();
            req.set_lease_validity_secs(30);
            req.set_num_items(5);
            req.set_timeout_secs(0);
            let _ = futures::executor::block_on(async move {
                let reply = request.send().promise.await?;
                let items = reply.get()?.get_resp()?.get_items()?;
                assert_eq!(items.len(), 5);
                Ok::<_, capnp::Error>(())
            })?;
            Ok(())
        },
    )?;

    // Poll again to block
    std::thread::scope(|s| {
        s.spawn(|| {
            with_client(
                addr,
                |queue_client| -> Result<(), color_eyre::eyre::Report> {
                    let mut request = queue_client.poll_request();
                    let mut req = request.get().init_req();
                    req.set_lease_validity_secs(30);
                    req.set_num_items(5);
                    req.set_timeout_secs(0);
                    let _ = futures::executor::block_on(async move {
                        tracing::info!("polling for new item");
                        let reply = request.send().promise.await?;
                        let items = reply.get()?.get_resp()?.get_items()?;
                        assert_eq!(items.len(), 1);
                        tracing::info!("got new item");
                        Ok::<_, capnp::Error>(())
                    })?;
                    Ok(())
                },
            )
        });

        s.spawn(move || {
            // Sleep for 1 second then add a new item
            std::thread::sleep(std::time::Duration::from_secs(1));
            with_client(
                addr,
                |queue_client| -> Result<(), color_eyre::eyre::Report> {
                    let mut request = queue_client.add_request();
                    let req = request.get().init_req();
                    let mut items = req.init_items(1);
                    let mut item = items.reborrow().get(0);
                    item.set_contents(b"hello (new)");
                    item.set_visibility_timeout_secs(0);
                    let _ = futures::executor::block_on(async move {
                        let reply = request.send().promise.await?;
                        let _ids = reply.get()?.get_resp()?.get_ids()?;
                        tracing::info!("added new item");
                        Ok::<_, capnp::Error>(())
                    })?;
                    Ok(())
                },
            )
        });
    });

    Ok(())
}

// copied from storage_bench.rs. TODO: move to a shared module
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
                            let _jh = tokio::task::spawn_local(rpc_system);
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
