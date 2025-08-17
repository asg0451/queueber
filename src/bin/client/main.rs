use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use color_eyre::{Result, eyre::eyre};
use uuid::Uuid;

use futures::AsyncReadExt;

use queueber::protocol::queue;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "queueber", version, about = "Queueber client")] 
struct Cli {
    /// Server address (host:port)
    #[arg(short = 'a', long = "addr", default_value = "localhost:9090")]
    addr: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Add one item to the queue
    Add {
        /// Item contents (bytes as string)
        #[arg(short = 'c', long = "contents")]
        contents: String,

        /// Visibility timeout in seconds
        #[arg(short = 'v', long = "visibility", default_value_t = 10)]
        visibility_timeout_secs: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = &cli.addr;
    tokio::task::LocalSet::new()
        .run_until(async move {
            let stream = tokio::net::TcpStream::connect(&addr).await?;
            stream.set_nodelay(true)?;
            let (reader, writer) =
                tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
            let rpc_network = Box::new(twoparty::VatNetwork::new(
                futures::io::BufReader::new(reader),
                futures::io::BufWriter::new(writer),
                rpc_twoparty_capnp::Side::Client,
                Default::default(),
            ));
            let mut rpc_system = RpcSystem::new(rpc_network, None);
            let queue_client: queue::Client =
                rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

            tokio::task::spawn_local(rpc_system);

            match cli.command {
                Commands::Add { contents, visibility_timeout_secs } => {
                    let mut request = queue_client.add_request();
                    let req = request.get().init_req();
                    let items = req.init_items(1);
                    let mut item = items.get(0);
                    item.set_contents(contents.as_bytes());
                    item.set_visibility_timeout_secs(visibility_timeout_secs);

                    let reply = request.send().promise.await?;
                    let ids = reply.get()?.get_resp()?.get_ids()?;

                    println!(
                        "received {:?} ids: {:?}",
                        ids.len(),
                        ids.iter()
                            .map(|id| id
                                .map_err(|e| eyre!("some error idk: {:?}", e))
                                .and_then(
                                    |id| Uuid::from_slice(id).map_err(|e| eyre!("invalid uuid: {:?}", e))
                                ))
                            .collect::<Result<Vec<_>, _>>()?
                    );
                    Ok(())
                }
            }
        })
        .await
}
