use std::{net::ToSocketAddrs, path::PathBuf};

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::Parser;
use color_eyre::Result;
use futures::AsyncReadExt;
use queueber::{server::Server, storage::Storage};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = args.listen.to_socket_addrs()?.next().unwrap();

    let storage = std::sync::Arc::new(Storage::new(&args.data_dir)?);
    let server = Server::new(storage);

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