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
    storage: Arc<Storage>,
    notify: Arc<Notify>,
}

impl Server {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage, notify: Arc::new(Notify::new()) }
    }
}

impl crate::protocol::queue::Server for Server {
    fn add(&mut self, params: AddParams, mut results: AddResults) -> Promise<(), capnp::Error> {
        let req = match params.get() { Ok(r) => r, Err(e) => return Promise::err(e) };
        let req = match req.get_req() { Ok(r) => r, Err(e) => return Promise::err(e) };
        let items = match req.get_items() { Ok(it) => it, Err(e) => return Promise::err(e) };

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
        let notify = Arc::clone(&self.notify);
        let ids_for_resp = ids.clone();

        Promise::from_future(async move {
            // Offload RocksDB work to the blocking thread pool.
            tokio::task::spawn_blocking(move || {
                let iter = ids
                    .iter()
                    .map(|id| id.as_slice())
                    .zip(items_owned.iter().map(|(c, v)| (c.as_slice(), *v)));
                storage.add_available_items_from_parts(iter)
            })
            .await
            .map_err(|e| crate::errors::Error::from(e))? // Join error -> our Error
            .map_err(|e| capnp::Error::from(e))?;        // our Error -> capnp

            // Notify any waiters that new items may be available
            notify.notify_waiters();

            // Build the response on the RPC thread.
            let mut ids_builder = results.get().init_resp().init_ids(ids_for_resp.len() as u32);
            for (i, id) in ids_for_resp.iter().enumerate() {
                ids_builder.set(i as u32, id);
            }
            Ok(())
        })
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
