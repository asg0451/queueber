use capnp::message::Builder as CapnpBuilder;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use queueber::protocol;
use queueber::storage::Storage;

fn bench_add_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_add");

    // Keep it simple and reasonably fast for CI
    let num_items: usize = 500;

    group.bench_function(format!("add_available_{}", num_items), |b| {
        b.iter_batched(
            || {
                let dir = tempfile::TempDir::new().expect("tmp dir");
                let storage = Storage::new(dir.path()).expect("storage");
                // Keep dir alive with storage by returning it as part of the state
                (dir, storage)
            },
            |(_dir, storage)| {
                for i in 0..num_items {
                    let mut msg = CapnpBuilder::new_default();
                    let mut item = msg.init_root::<protocol::item::Builder>();
                    item.set_contents(b"hello");
                    item.set_visibility_timeout_secs(0);

                    let id = format!("id_{}", i);
                    let reader = item.into_reader();
                    storage
                        .add_available_item((id.as_bytes(), reader))
                        .expect("add_available_item");
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_remove_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_remove");

    // Keep it simple and reasonably fast for CI
    let num_items: usize = 500;

    group.bench_function(format!("remove_in_progress_{}", num_items), |b| {
        b.iter_batched(
            || {
                // Setup a fresh DB, insert N items, poll them to move to in_progress
                let dir = tempfile::TempDir::new().expect("tmp dir");
                let storage = Storage::new(dir.path()).expect("storage");

                for i in 0..num_items {
                    let mut msg = CapnpBuilder::new_default();
                    let mut item = msg.init_root::<protocol::item::Builder>();
                    item.set_contents(b"hello");
                    item.set_visibility_timeout_secs(0);
                    let id = format!("id_{}", i);
                    let reader = item.into_reader();
                    storage
                        .add_available_item((id.as_bytes(), reader))
                        .expect("add_available_item");
                }

                let (lease, polled) = storage
                    .get_next_available_entries(num_items)
                    .expect("poll");

                let mut ids: Vec<Vec<u8>> = Vec::with_capacity(polled.len());
                for typed in polled.into_iter() {
                    let reader = typed.get().expect("typed reader");
                    ids.push(reader.get_id().expect("id").to_vec());
                }

                (dir, storage, lease, ids)
            },
            |(_dir, storage, lease, ids)| {
                for id in ids {
                    let removed = storage
                        .remove_in_progress_item(&id, &lease)
                        .expect("remove_in_progress_item");
                    assert!(removed, "expected removal for id");
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_add_messages, bench_remove_messages);
criterion_main!(benches);

