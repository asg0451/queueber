use std::collections::HashSet;

#[test]
fn generates_batch_with_expected_size_and_uniqueness() {
    let n = 4096;
    let ids = queueber::uuid_utils::generate_uuidv7_batch(n);
    assert_eq!(ids.len(), n);
    let mut set: HashSet<[u8; 16]> = HashSet::with_capacity(n);
    for id in ids {
        assert!(set.insert(id), "duplicate id detected");
    }
}
