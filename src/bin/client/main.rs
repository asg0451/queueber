use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use color_eyre::{Result, eyre::eyre};
use uuid::Uuid;

use futures::AsyncReadExt;

use queueber::protocol::queue;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "localhost:9090";
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

            let mut request = queue_client.add_request();
            let req = request.get().init_req();
            let items = req.init_items(1);
            let mut item = items.get(0);
            item.set_contents(b"hello");
            item.set_visibility_timeout_secs(10);

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
        })
        .await
}
