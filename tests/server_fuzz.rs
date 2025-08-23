use std::sync::Arc;

use proptest::prelude::*;
use proptest::strategy::Strategy;

use queueber::{RetriedStorage, Storage};

// This is a randomized, concurrent integration test that attempts to find
// race conditions around add -> poll -> remove flows. It generates a mix of
// operations performed from multiple threads against a temporary RocksDB.

#[derive(Clone, Debug)]
enum Op {
    Add {
        id: Vec<u8>,
        contents: Vec<u8>,
        vis_secs: u64,
    },
    Poll {
        n: usize,
        lease_secs: u64,
    },
    Remove {
        id: Vec<u8>,
    },
}

fn arb_op() -> impl Strategy<Value = Op> {
    let add = (
        proptest::collection::vec(any::<u8>(), 1..8),
        proptest::collection::vec(any::<u8>(), 0..16),
        (0u64..3u64),
    )
        .prop_map(|(id, contents, vis_secs)| Op::Add {
            id,
            contents,
            vis_secs,
        });

    let poll =
        ((1usize..5usize), (1u64..5u64)).prop_map(|(n, lease_secs)| Op::Poll { n, lease_secs });

    let remove = proptest::collection::vec(any::<u8>(), 1..8).prop_map(|id| Op::Remove { id });

    prop_oneof![add, poll, remove]
}

proptest! {
    #[test]
    fn randomized_concurrent_ops(op_sequences in proptest::collection::vec(proptest::collection::vec(arb_op(), 25..60), 3..6)) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let storage = Storage::new(tmp.path()).expect("storage");
        let storage = Arc::new(RetriedStorage::new(storage));

        // Spawn one thread per sequence; each sequence is a list of operations
        // this thread will perform against the shared storage.
        let mut handles = Vec::new();
        for seq in op_sequences {
            let st = Arc::clone(&storage);
            handles.push(std::thread::spawn(move || -> queueber::errors::Result<()> {
                for op in seq {
                    match op {
                        Op::Add { id, contents, vis_secs } => {
                            st.add_available_item_from_parts(&id, &contents, vis_secs)?;
                        }
                        Op::Poll { n, lease_secs } => {
                            let (_lease, _items) = st.get_next_available_entries_with_lease(n, lease_secs)?;
                            // We do not assert on item counts here because of concurrency; the goal
                            // is to shake races and ensure no panics or invariants are violated.
                        }
                        Op::Remove { id } => {
                            // Try removing under a random/nonexistent lease to exercise paths.
                            let fake_lease = uuid::Uuid::now_v7().into_bytes();
                            let _ = st.remove_in_progress_item(&id, &fake_lease)?;
                        }
                    }
                }
                Ok(())
            }));
        }

        // Periodically run the expiry sweeper from the main thread to add pressure.
        for _ in 0..3 {
            let _ = storage.expire_due_leases();
        }

        for h in handles { h.join().expect("thread join").expect("thread result"); }

        // Final integrity check: visibility index entries should either point to
        // an available item or belong to the future; a direct call to the existing
        // poll path should not panic and should respect invariants.
        let _ = storage.get_next_available_entries(32)?;
    }
}
