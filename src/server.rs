use capnp::capability::Promise;

use crate::{
    protocol::queue::{
        AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
    },
    storage::Storage,
};
use std::sync::Arc;

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: Arc<Storage>,
}

impl Server {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let req = match params.get() {
            Ok(r) => r,
            Err(e) => return Promise::err(e),
        };
        let req = match req.get_req() {
            Ok(r) => r,
            Err(e) => return Promise::err(e),
        };

        let items = match req.get_items() {
            Ok(it) => it,
            Err(e) => return Promise::err(e),
        };

        // Generate ids upfront and copy request data into owned memory so we can move
        // it into a blocking task (capnp readers are not Send).
        let ids: Vec<Vec<u8>> = items
            .iter()
            .map(|_| uuid::Uuid::now_v7().as_bytes().to_vec())
            .collect();

        let items_owned: Vec<(Vec<u8>, u64)> = items
            .iter()
            .map(|item| {
                let contents = item
                    .get_contents()
                    .unwrap_or_default()
                    .to_vec();
                let vis = item.get_visibility_timeout_secs();
                (contents, vis)
            })
            .collect();

        let storage = Arc::clone(&self.storage);
        let ids_for_resp = ids.clone();

        Promise::from_future(async move {
            // Offload RocksDB work to the blocking thread pool.
            tokio::task::spawn_blocking(move || {
                // Zip ids with item parts, pass as slices to avoid extra allocations.
                let iter = ids
                    .iter()
                    .map(|id| id.as_slice())
                    .zip(items_owned.iter().map(|(c, v)| (c.as_slice(), *v)));
                storage
                    .add_available_items_from_parts(iter)
            })
            .await
            .map_err(|e| capnp::Error::failed(format!("join error: {}", e)))?
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

            // Build the response on the RPC thread.
            let mut ids_builder = results.get().init_resp().init_ids(ids_for_resp.len() as u32);
            for (i, id) in ids_for_resp.iter().enumerate() {
                ids_builder.set(i as u32, id);
            }
            Ok(())
        })
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
