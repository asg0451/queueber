use capnp::capability::Promise;
use capnp::message::{Builder, HeapAllocator, TypedReader};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::{
    protocol::queue::{
        AddParams, AddResults, PollParams, PollResults, RemoveParams, RemoveResults,
    },
    storage::Storage,
};

// https://github.com/capnproto/capnproto-rust/tree/master/capnp-rpc
// https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/examples/hello-world/server.rs
pub struct Server {
    storage: std::sync::Arc<Storage>,
    notify: Arc<Notify>,
}

impl Server {
    pub fn new(storage: std::sync::Arc<Storage>) -> Self {
        Self { storage, notify: Arc::new(Notify::new()) }
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
        self
            .storage
            .add_available_items(ids.iter().map(AsRef::as_ref).zip(items.iter()))
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        // Notify any waiters that new items may be available
        self.notify.notify_waiters();

        // build the response
        let mut ids_builder = results.get().init_resp().init_ids(ids.len() as u32);
        for (i, id) in ids.iter().enumerate() {
            ids_builder.set(i as u32, id);
        }

        Promise::ok(())
    }

    fn remove(&mut self, params: RemoveParams, mut results: RemoveResults) -> Promise<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let id = req.get_id()?;
        let lease_bytes = req.get_lease()?;

        if lease_bytes.len() != 16 {
            return Promise::err(capnp::Error::failed("invalid lease length".to_string()));
        }
        let mut lease: [u8; 16] = [0; 16];
        lease.copy_from_slice(lease_bytes);

        let removed = self
            .storage
            .remove_in_progress_item(id, &lease)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        results.get().init_resp().set_removed(removed);
        Promise::ok(())
    }

    fn poll(&mut self, params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        let storage = Arc::clone(&self.storage);
        let notify = Arc::clone(&self.notify);

        Promise::from_future(async move {
            let req = params.get()?.get_req()?;
            let _lease_validity_secs = req.get_lease_validity_secs();
            let num_items = match req.get_num_items() { 0 => 1, n => n as usize };
            let timeout_secs = req.get_timeout_secs();

            // Fast path: try immediately
            if let Ok((lease, items)) = storage.get_next_available_entries(num_items) {
                write_poll_response(&lease, items, &mut results)?;
                return Ok(());
            }

            // Otherwise, wait for notification or timeout, then try again once
            if timeout_secs > 0 {
                let timeout = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs));
                tokio::select! {
                    _ = notify.notified() => {},
                    _ = timeout => {},
                }
            } else {
                // Wait indefinitely until something is added
                notify.notified().await;
            }

            let (lease, items) = storage
                .get_next_available_entries(num_items)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            write_poll_response(&lease, items, &mut results)
        })
    }
}

fn write_poll_response(
    lease: &[u8; 16],
    items: Vec<TypedReader<Builder<HeapAllocator>, crate::protocol::polled_item::Owned>>, 
    results: &mut crate::protocol::queue::PollResults,
) -> Result<(), capnp::Error> {
    let mut resp = results.get().init_resp();
    resp.set_lease(lease);

    let mut items_builder = resp.init_items(items.len() as u32);
    for (i, typed_polled_item) in items.into_iter().enumerate() {
        let item_reader = typed_polled_item.get()?;
        let mut out_item = items_builder.reborrow().get(i as u32);
        out_item.set_contents(item_reader.get_contents()?);
        out_item.set_id(item_reader.get_id()?);
    }
    Ok(())
}
