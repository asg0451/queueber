use queueber::errors::Result;
use queueber::storage::Storage;
use std::sync::Arc;

#[test]
fn concurrent_adds_and_poll_integration() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("concurrency=gdebug")
        .try_init();

    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(Storage::new(tmp.path()).expect("storage"));

    let writers = 4;
    let per_writer = 16;
    let mut handles = Vec::new();
    for w in 0..writers {
        tracing::debug!("starting writer {w}");
        let st = Arc::clone(&storage);
        handles.push(std::thread::spawn(move || -> Result<()> {
            for i in 0..per_writer {
                let id = format!("w{w}-i{i}");
                st.add_available_item_from_parts(id.as_bytes(), b"payload", 0)?;
                tracing::debug!("writer {w} added item {id}");
            }
            Ok(())
        }));
    }
    tracing::debug!("waiting for writers to finish");
    for h in handles {
        h.join().expect("thread join").expect("writer result");
    }

    tracing::debug!("getting next available entries");
    let total = (writers * per_writer) as usize;
    let (_lease, items) = storage.get_next_available_entries(total)?;
    assert_eq!(items.len(), total);

    Ok(())
}
