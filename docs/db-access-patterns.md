## Queueber DB access patterns (current state)

This document describes how the RocksDB storage layer is organized and the access patterns used for enqueue, poll, ack, and lease expiry. It reflects the current implementation in `src/storage.rs` and `src/dbkeys.rs`.

### Namespaces and keys

- `available/<id>`: Main record for an item that is visible to pollers. Value is a Cap'n Proto `stored_item` containing:
  - `contents`: opaque payload bytes
  - `id`: item id bytes (same as `<id>`)
  - `visibility_ts_index_key`: the exact visibility-index key bytes for this item at its visible-at time

- `visibility_index/<u64:visible_ts_be>/<id>`: Secondary index ordered by big-endian timestamp, then `id`. Value is the corresponding `available/<id>` key. Enables efficient prefix scans over ready items.

- `in_progress/<id>`: Main record for an item that has been polled and is currently leased. Value is the same `stored_item` payload as in `available` (including the original `visibility_ts_index_key`).

- `leases/<lease_bytes>`: A lease entry. Value is a Cap'n Proto `lease_entry` with a list of item ids (`keys`) currently owned by the lease.

- `lease_expiry/<u64:expiry_ts_be>/<lease_bytes>`: Index from expiry timestamp to `leases/<lease_bytes>` key for the sweeper.

Key helpers and parsers are in `src/dbkeys.rs`. Timestamps are encoded as 8-byte big-endian to preserve lexicographic ordering. A RocksDB `SliceTransform` is configured to treat the namespace segment up to and including the first `/` as the prefix, allowing efficient `prefix_iterator` scans across all namespaces.

### Write shapes

- Enqueue (`add_available_item_from_parts`):
  - Build `stored_item` with precomputed `visibility_ts_index_key` for visible-at time.
  - Write batch with:
    - `put available/<id> -> stored_item`
    - `put visibility_index/<ts>/<id> -> available/<id>`

- Poll (`get_next_available_entries*`):
  - Compute `now_secs`. Iterate `prefix_iterator("visibility_index/")` in lexicographic order.
  - For each entry with `visible_at_secs <= now_secs` attempt to claim atomically using a transaction:
    - `get_for_update(idx_key)` and `get_for_update(main_key)` to lock both.
    - If `main_key` exists: read `stored_item` and move to `in_progress/<id>`, then delete `available` and `visibility_index` inside the same transaction, and commit.
    - If `main_key` is missing:
      - If `in_progress/<id>` exists, another poller already claimed; skip.
      - If the index entry (`idx_key`) is already gone, skip.
      - Otherwise treat it as a stale/orphaned index and self-heal by deleting `idx_key` within the same transaction and commit; skip the item.
  - Accumulate up to `n` items. If any were claimed, write a lease entry:
    - `put leases/<lease> -> lease_entry(keys=[ids...])`
    - `put lease_expiry/<expiry>/<lease> -> leases/<lease>`
  - If zero items were claimed, no lease is written.

- Ack (`remove_in_progress_item`):
  - Validate `in_progress/<id>` exists; if not, return false.
  - Validate `leases/<lease>` exists and contains `id`; if not, return false.
  - Write batch:
    - `delete in_progress/<id>`
    - If `lease` has other ids: rewrite `leases/<lease>` with remaining ids; else delete `leases/<lease>`.

- Lease expiry sweep (`expire_due_leases`):
  - Iterate `prefix_iterator("lease_expiry/")` until `expiry_ts <= now`.
  - Load `leases/<lease>` if present. For each `id` still in `in_progress/<id>`:
    - Rebuild `stored_item` with `visibility_ts_index_key` set to now and move back to `available/<id>`.
    - Restore `visibility_index/now/<id>`.
    - Delete `in_progress/<id>`.
  - Delete `leases/<lease>` and its `lease_expiry` entry.
  - If `leases/<lease>` is missing, delete the `lease_expiry` entry and continue.

### Concurrency and locking

- Polling uses RocksDB transactions with `get_for_update` on both the index entry and the main key to ensure only one poller can claim an item.
- Stale/benign races are handled gracefully:
  - If `available/<id>` is gone and `in_progress/<id>` exists, skip (another poller claimed it).
  - If both the main and index are gone by the time we lock, skip.
  - If the index exists but the main is gone and not in-progress, we delete the orphaned index within the same transaction and continue.
- Other write paths (`enqueue`, `ack`, `sweep`) use write batches. The move operations in poll are done inside a single transaction to guarantee exclusivity.

### Invariants (current)

- At steady state, every `visibility_index/<ts>/<id>` should point at an existing `available/<id>`; violations can occur transiently under races and are now self-healed during polling.
- An item exists in exactly one of: `available/<id>` or `in_progress/<id>`.
- A lease lists zero or more `id`s. If it becomes empty it is deleted.

### Notes and future improvements

- Clock is non-mockable; tests rely on real time for lease expiry. Consider injecting a clock for deterministic tests.
- Consider batching multiple visibility-index deletions in a single transaction iteration if many orphans are encountered.
- Evaluate using column families per namespace for more explicit isolation.

