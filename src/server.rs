use capnp::capability::Promise;

use crate::{
    protocol::queue::{
        AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
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
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        // let contents = req.get_contents()?;
        // let visibility_timeout_secs = req.get_visibility_timeout_secs();

        let items = req.get_items()?;

        let ids = items
            .iter()
            .map(|_| uuid::Uuid::now_v7().as_bytes().to_vec())
            .collect::<Vec<_>>();

        // TODO: do we need to run this in an io thread pool or smth?
        self.storage
            .add_available_items(ids.iter().map(AsRef::as_ref).zip(items.iter()))
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        // build the response
        let mut ids_builder = results.get().init_resp().init_ids(ids.len() as u32);
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

    fn poll(&mut self, _params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        // For now, ignore the requested lease validity and return up to 1 item.
        // Later we can thread lease validity through the storage layer.
        let (lease, items) = self
            .storage
            .get_next_available_entries(1)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut resp = results.get().init_resp();
        resp.set_lease(&lease);

        let mut items_builder = resp.init_items(items.len() as u32);
        for (i, typed_polled_item) in items.into_iter().enumerate() {
            let item_reader = typed_polled_item.get().map_err(|e| capnp::Error::failed(e.to_string()))?;
            let mut out_item = items_builder.reborrow().get(i as u32);
            out_item
                .set_contents(item_reader.get_contents().map_err(|e| capnp::Error::failed(e.to_string()))?);
            out_item
                .set_id(item_reader.get_id().map_err(|e| capnp::Error::failed(e.to_string()))?);
        }

        Promise::ok(())
    }
}
