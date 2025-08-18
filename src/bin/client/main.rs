use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use clap::{Parser, Subcommand};
use color_eyre::{Result, eyre::eyre};
use futures::AsyncReadExt;
use queueber::protocol::queue;
use uuid::Uuid;

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
    /// Poll for available items
    Poll {
        /// How long the lease should be valid on the server (seconds)
        #[arg(short = 'l', long = "lease", default_value_t = 30)]
        lease_validity_secs: u64,
        /// Maximum number of items to return
        #[arg(short = 'n', long = "num", default_value_t = 1)]
        num_items: u32,
        /// How long to wait for items before returning (seconds)
        #[arg(short = 't', long = "timeout", default_value_t = 0)]
        timeout_secs: u64,
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
                Commands::Add {
                    contents,
                    visibility_timeout_secs,
                } => {
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
                            .map(|id| {
                                id.map_err(|e| eyre!("some error idk: {:?}", e))
                                    .and_then(|id| {
                                        Uuid::from_slice(id)
                                            .map_err(|e| eyre!("invalid uuid: {:?}", e))
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    );
                    Ok(())
                }
                Commands::Poll {
                    lease_validity_secs,
                    num_items,
                    timeout_secs,
                } => {
                    let mut request = queue_client.poll_request();
                    let mut req = request.get().init_req();
                    req.set_lease_validity_secs(lease_validity_secs);
                    req.set_num_items(num_items);
                    req.set_timeout_secs(timeout_secs);

                    let reply = request.send().promise.await?;
                    let resp = reply.get()?.get_resp()?;
                    let lease = resp.get_lease()?;
                    let items = resp.get_items()?;

                    println!(
                        "lease: {}",
                        Uuid::from_slice(lease)
                            .map(|u| u.to_string())
                            .unwrap_or_else(|_| format!("{:?}", lease))
                    );
                    if items.is_empty() {
                        println!("no items available");
                    } else {
                        for i in 0..items.len() {
                            let item = items.get(i);
                            let id = item.get_id()?;
                            let contents = item.get_contents()?;
                            println!(
                                "item {}: id={}, contents={}",
                                i,
                                Uuid::from_slice(id)
                                    .map(|u| u.to_string())
                                    .unwrap_or_else(|_| format!("{:?}", id)),
                                String::from_utf8_lossy(contents)
                            );
                        }
                    }
                    Ok(())
                }
            }
        })
        .await
}
