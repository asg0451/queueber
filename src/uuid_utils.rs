use uuid::Uuid;

/// Generate `count` UUIDv7 identifiers as raw 16-byte arrays.
///
/// Optimizations:
/// - Pre-allocates the full vector capacity
/// - Tight for-loop to minimize iterator overhead
///
/// Note: We rely on `Uuid::now_v7()` for correctness and monotonicity semantics.
/// If further optimization is needed, consider constructing v7s from a cached
/// timestamp source once the `uuid` crate exposes a stable builder API for v7.
pub fn generate_uuidv7_batch(count: usize) -> Vec<[u8; 16]> {
    let mut ids: Vec<[u8; 16]> = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(Uuid::now_v7().into_bytes());
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn batch_generates_correct_count_and_unique_ids() {
        let n = 1024;
        let ids = generate_uuidv7_batch(n);
        assert_eq!(ids.len(), n);
        let mut set: HashSet<[u8; 16]> = HashSet::with_capacity(n);
        for id in ids {
            assert!(set.insert(id), "duplicate id generated in batch");
        }
    }
}
