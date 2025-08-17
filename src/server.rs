use capnp::capability::Promise;
use std::sync::Arc;
use tokio::task;

use crate::{
    protocol::queue::{
        AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
    },
    storage::Storage,
};

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: Arc<Storage>,
}

impl Server {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        // Extract everything we need from non-Send capnp params on this thread.
        let req = match params.get() {
            Ok(p) => p.get_req(),
            Err(e) => return Promise::err(e),
        };
        let req = match req {
            Ok(r) => r,
            Err(e) => return Promise::err(e),
        };

        let items = match req.get_items() {
            Ok(i) => i,
            Err(e) => return Promise::err(e),
        };

        // Generate IDs here (still on this thread) and copy item data we need to move to a blocking thread.
        let mut ids: Vec<Vec<u8>> = Vec::with_capacity(items.len() as usize);
        let mut owned_items: Vec<(Vec<u8>, u64)> = Vec::with_capacity(items.len() as usize);
        for it in items.iter() {
            ids.push(uuid::Uuid::now_v7().as_bytes().to_vec());
            // Copy out contents and visibility timeout so we can move work off-thread safely.
            let contents = match it.get_contents() {
                Ok(c) => c.to_vec(),
                Err(e) => return Promise::err(e),
            };
            let visibility = it.get_visibility_timeout_secs();
            owned_items.push((contents, visibility));
        }

        let storage = Arc::clone(&self.storage);
        // Clone ids for the blocking thread so we can still use the originals to build the response.
        let ids_for_storage = ids.clone();

        // Perform the blocking RocksDB writes on a dedicated blocking thread, and await here without blocking the local reactor.
        Promise::from_future(async move {
            // Move data for storage: pair each generated id with a capnp item we rebuild locally per entry.
            let storage_task = task::spawn_blocking(move || -> Result<(), crate::errors::Error> {
                for (id, (contents, visibility)) in ids_for_storage
                    .iter()
                    .map(AsRef::as_ref)
                    .zip(owned_items)
                {
                    // Rebuild a protocol::item from owned data for the storage API.
                    let mut msg = capnp::message::Builder::new_default();
                    {
                        let mut item = msg.init_root::<crate::protocol::item::Builder>();
                        item.set_contents(&contents);
                        item.set_visibility_timeout_secs(visibility);
                    }
                    let item_reader = msg.into_typed().into_reader();
                    storage.add_available_item((id, item_reader.get()?))?;
                }
                Ok(())
            });

            // Propagate any blocking-thread error back into capnp error space.
            match storage_task.await {
                Ok(Ok(())) => {
                    // Build the response (must be done on this thread, results is !Send).
                    let mut ids_builder = results.get().init_resp().init_ids(ids.len() as u32);
                    for (i, id) in ids.iter().enumerate() {
                        ids_builder.set(i as u32, id);
                    }
                    Ok(())
                }
                Ok(Err(e)) => Err(capnp::Error::failed(e.to_string())),
                Err(join_err) => Err(capnp::Error::failed(format!(
                    "spawn_blocking join error: {}",
                    join_err
                ))),
            }
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
