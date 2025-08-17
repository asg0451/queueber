use capnp::capability::Promise;

use crate::{
    protocol::queue::{AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults},
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
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        // let contents = req.get_contents()?;
        // let visibility_timeout_secs = req.get_visibility_timeout_secs();

        let items = req.get_items()?;

        let ids = items
            .iter()
            .map(|_| uuid::Uuid::now_v7().as_bytes().to_vec())
            .collect::<Vec<_>>();

        self.storage
            .add_available_items(ids.iter().map(AsRef::as_ref).zip(items.iter()))
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        // build the response
        let mut ids_builder = results.get().init_resp().get_ids()?;
        for (i, id) in ids.iter().enumerate() {
            ids_builder.set(i as u32, id);
        }

        Promise::ok(())
    }

    fn remove(&mut self, _: RemoveParams, _: RemoveResults) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "method queue::Server::remove not implemented".to_string(),
        ))
    }

    fn poll(&mut self, _: PollParams, _: PollResults) -> Promise<(), capnp::Error> {
        // Promise::from_future(req.send().promise.and_then(move |response| {
        //     results.get().set_y(response.get()?.get_y());
        //     Ok(())
        // }))
        Promise::err(capnp::Error::unimplemented(
            "method queue::Server::poll not implemented".to_string(),
        ))
    }
}
