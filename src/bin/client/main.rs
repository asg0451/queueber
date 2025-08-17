use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use color_eyre::Result;

use futures::AsyncReadExt;

use queueber::protocol::{self, queue};

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

            let mut item_builder = capnp::message::Builder::new_default();
            let mut item = item_builder.init_root::<protocol::item::Builder>();
            item.set_contents(b"hello");
            item.set_visibility_timeout_secs(10);
            let an_item = item.into_reader();

            let mut request = queue_client.add_request();
            let req = request.get().init_req();
            let mut items = req.get_items()?;
            // the caveats are basically that if the schema version of the req is older than the schema version of the item, we'll lose the new data in the item.
            // that doesnt sound realistic though.
            items.set_with_caveats(0, an_item)?;

            let reply = request.send().promise.await?;

            println!(
                "received: {:?}",
                reply
                    .get()?
                    .get_resp()?
                    .get_ids()?
                    .iter()
                    .collect::<Vec<_>>()
            );
            Ok(())
        })
        .await
}
