use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use color_eyre::Result;

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
            request.get().init_req().set_contents(b"hello");
            request.get().init_req().set_visibility_timeout_secs(10);

            let reply = request.send().promise.await?;

            println!("received: {:?}", reply.get()?.get_resp()?.get_id());
            Ok(())
        })
        .await
}
