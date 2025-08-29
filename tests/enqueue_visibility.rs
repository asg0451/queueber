use queueber::storage::Storage;

#[test]
fn enqueue_uses_batched_time_for_visibility_index() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = Storage::new(tmp.path()).expect("storage open");

    // Enqueue three items with increasing visibility offsets
    storage
        .add_available_items_from_parts([
            (b"id-a".as_ref(), (b"a".as_ref(), 0u64)),
            (b"id-b".as_ref(), (b"b".as_ref(), 1u64)),
            (b"id-c".as_ref(), (b"c".as_ref(), 2u64)),
        ])
        .expect("enqueue batch");

    // Peek the earliest visibility timestamp; it should be >= now and <= now + 2
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let first_ts = storage
        .peek_next_visibility_ts_secs()
        .expect("peek ok")
        .expect("some ts");

    // Allow small slop since the test itself takes a little time
    assert!(
        first_ts >= now_secs && first_ts <= now_secs + 2,
        "first_ts={}, now={}",
        first_ts,
        now_secs
    );
}
