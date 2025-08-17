use std::collections::HashMap;

use capnp::capability::Promise;

use crate::protocol::queue::{
    AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
};

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    dummy_store: HashMap<u64, Vec<u8>>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            dummy_store: HashMap::new(),
        }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), ::capnp::Error> {
        let req = params.get()?.get_req()?;
        let contents = req.get_contents()?;
        let _visibility_timeout_secs = req.get_visibility_timeout_secs();

        let id = self.dummy_store.len() as u64;

        self.dummy_store.insert(id, contents.to_vec());

        results.get().init_resp().set_id(id);

        Promise::ok(())
    }

    fn remove(&mut self, _: RemoveParams, _: RemoveResults) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "method queue::Server::remove not implemented".to_string(),
        ))
    }

    fn poll(&mut self, _: PollParams, _: PollResults) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "method queue::Server::poll not implemented".to_string(),
        ))
    }
}
