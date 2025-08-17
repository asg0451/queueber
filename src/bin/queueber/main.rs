use std::net::ToSocketAddrs;

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::server::Server;

// see https://github.com/capnproto/capnproto-rust/blob/master/example/addressbook_send/addressbook_send.rs
// for how to send stuff across threads; so we can parallelize the work..?

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9090".to_socket_addrs()?.next().unwrap();

    let server = Server::new();

    tokio::task::LocalSet::new()
        .run_until(async move {
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let queue_client: queueber::protocol::queue::Client = capnp_rpc::new_client(server);

            loop {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
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

                tokio::task::spawn_local(rpc_system);
            }
        })
        .await
}
