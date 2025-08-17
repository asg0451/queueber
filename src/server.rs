
use capnp::capability::Promise;

use crate::{
    protocol::{
        item,
        queue::{AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults},
    },
    storage::Storage,
};

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: Storage,
}

impl Server {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), ::capnp::Error> {
        let req = params.get()?.get_req()?;
        let contents = req.get_contents()?;
        let visibility_timeout_secs = req.get_visibility_timeout_secs();

        let id = uuid::Uuid::now_v7();
        let id = id.as_bytes();

        // make an item
        let mut message = capnp::message::Builder::new_default();
        let mut item = message.init_root::<item::Builder>();
        item.set_id(id);
        item.set_contents(contents);
        item.set_visibility_timeout_secs(visibility_timeout_secs);

        let mut buf = Vec::new();
        capnp::serialize_packed::write_message(&mut buf, &message)?;

        self.storage
            .put(id, &buf)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        results.get().init_resp().set_id(id);

        Promise::ok(())
    }

    fn remove(&mut self, _: RemoveParams, _: RemoveResults) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "method queue::Server::remove not implemented".to_string(),
        ))
    }

    fn poll(&mut self, _: PollParams, _: PollResults) -> Promise<(), ::capnp::Error> {
        // Promise::from_future(req.send().promise.and_then(move |response| {
        //     results.get().set_y(response.get()?.get_y());
        //     Ok(())
        // }))
        Promise::err(::capnp::Error::unimplemented(
            "method queue::Server::poll not implemented".to_string(),
        ))
    }
}
