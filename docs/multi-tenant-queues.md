## Multi-tenant queues with per-tenant isolation

This document proposes and specifies support for multiple queues across multiple tenants with strong isolation, minimal performance regression, and operational simplicity.

### Goals
- **Multi-tenant isolation**: each tenant has isolated queues, limits, auth, and metrics.
- **No performance regressions**: maintain current single-queue throughput; avoid cross-tenant contention.
- **Operationally simple**: no explosion of RocksDB column families; efficient prefix scans; easy recovery.

### Non-Goals
- Cross-queue transactions are out of scope.
- Cross-tenant routing or federation across clusters is out of scope.

## API and Authentication

All RPCs that address a queue will carry a `QueueRef` envelope identifying the tenant and queue. Servers authorize the client to the bound `tenant_id` derived from mTLS/JWT and deny cross-tenant access.

Backward compatibility: when `tenant`/`queue` is omitted by older clients, default to `tenant="default"`, `queue="default"`.

### Cap'n Proto sketches

These are illustrative only; exact integration will follow existing RPC files.

```capnp
struct QueueRef {
  tenant @0 :Text;   # external tenant name, e.g. "acme"
  queue  @1 :Text;   # queue name within tenant, e.g. "payments"
}

struct AddRequest {
  ref    @0 :QueueRef;
  items  @1 :List(Data);
  # ... existing fields ...
}

struct PollRequest {
  ref        @0 :QueueRef;
  numItems   @1 :UInt16;
  waitMs     @2 :UInt32;   # optional long poll timeout
  # ... existing fields ...
}

struct AckRequest {
  ref       @0 :QueueRef;
  messageId @1 :Data;  # uuidv7 bytes
}

struct ExtendRequest {
  ref       @0 :QueueRef;
  messageId @1 :Data;
  extendMs  @2 :UInt32;
}

struct RemoveRequest {
  ref       @0 :QueueRef;
  messageId @1 :Data;
}
```

### AuthZ and admission control
- Map client identity (mTLS SAN or JWT claims) → `tenant_id`.
- Enforce per-tenant authorization for all RPCs.
- Apply per-tenant and per-queue rate limits and quotas (see Limits).

## Namespace and ID model

External names are mapped to compact numeric IDs to keep keys small and comparisons fast.

- `tenant_name → tenant_id (u32)`
- `queue_name (scoped under tenant) → queue_id (u32)`

Persist these in a small metadata column family (or namespace) and cache them in memory. Load on startup and lazily create missing entries on first use (subject to tenant auth and optional admin-only creation).

Metadata keys (all big-endian numeric encodings):

```text
meta/tenant/by_name/{tenant_name}               -> {tenant_id:be32}
meta/tenant/by_id/{tenant_id:be32}              -> {tenant_name}
meta/queue/by_name/{tenant_id:be32}/{queue_name} -> {queue_id:be32}
meta/queue/by_id/{tenant_id:be32}/{queue_id:be32} -> {queue_name}
```

## Storage schema (RocksDB)

We keep the current set of column families; we do not create a CF per tenant/queue. Instead, we add a queue prefix to all keys and leverage RocksDB prefix/upper-bound iterators for efficient scans. See also: [RocksDB column families](./rocksdb-column-families.md) and [DB access patterns](./db-access-patterns.md).

All numeric encodings below are fixed-width big-endian to preserve ordering and support iterator upper bounds.

Queue key prefix:

```text
P = {tenant_id:be32}/{queue_id:be32}
```

Main key/value and indices:

```text
# Main value: message body and metadata
m/{P}/{message_id:uuidv7_be128}                                  -> StoredItem

# Visibility index: next-visible time ordering
vi/{P}/{visible_at:be64}/{message_id:uuidv7_be128}               -> ()

# Lease expiry index: lease-expiration time ordering
li/{P}/{expiry_at:be64}/{message_id:uuidv7_be128}                -> ()

# Lease entry: per-message lease details (contains cached li-key)
le/{P}/{message_id:uuidv7_be128}                                 -> LeaseEntry

# Optional: per-queue counters/attributes
qc/{P}/{attr_name:bytes}                                         -> bytes
```

Iterators must use a lower bound at the queue prefix and an upper bound at the next lexicographic key after the prefix to tightly scope scans. Example for visibility scan up to now:

```text
lower = "vi/" + P + "/" + be64(0)
upper = "vi/" + P + "/" + be64(now)
```

## Concurrency model and sharding

Route RPCs to an internal shard derived from the queue identity to maximize locality and reduce cross-queue contention:

```text
shard_index = hash64(tenant_id, queue_id) % NUM_SHARDS
```

Each shard owns:
- A worker runtime that handles queue-local RPCs (add, poll, ack, extend, remove).
- A poll-wakeup channel and a per-shard poll coalescer (see [poll coalescing sketch](./poll_coalescing_sketch.md)).
- Background tasks bound to that shard, including the lease-expiry sweeper.

Storage I/O continues to use the I/O thread pool. Within a shard, bound parallelism prevents write-write amplification.

## Request handling

- **Add**: write `m/*` and `vi/*` under `P/…`; trigger queue-local wakeups.
- **Poll**:
  - Coalesce outstanding polls per shard.
  - Range-scan `vi/{P}/[0..now]` using upper bounds.
  - Use `multi_get` to batch main-value reads for selected message_ids.
  - Respect per-queue `num_items` and per-tenant limits.
- **Ack/Remove**: remove `m/*`, `le/*`, and any existing index entries under `P/…`.
- **Extend**: update `li/*` in-place using the cached lease-expiry index key in `LeaseEntry`, scoped under `P/…`.

## Background tasks

- **Lease expiry sweeper**: per shard, scan `li/{P}/[0..now]`, update visibility or delete as appropriate. Ensure there is exactly one sweeper per shard process-wide.
- **Queue-local wakeups**: each shard maintains an independent condition variable to avoid waking unrelated tenants/queues.

## Isolation: limits and quotas

Enforce isolation at ingress and within scheduling:

- **Rate limits**: per-tenant and per-queue token buckets for ops/sec and bytes/sec; independent read/write buckets; configurable bursts.
- **Concurrency limits**: max concurrent `poll` and `add` per queue; excess requests are queued fairly across tenants.
- **Quotas**: optional per-tenant maximum stored bytes and message count across all queues; reject new writes when over quota.
- **Message constraints**: per-tenant maximum message size and default TTLs.

## Metrics and observability

Emit metrics labeled with `tenant` and `queue` (or numeric IDs with a side-channel map to names to cap cardinality). Sample under load to control cardinality.

Key metrics:
- RPC latencies and QPS per operation.
- Queue depth (visible count), in-flight leases, expirations/sec.
- Rate-limit decisions and queueing durations.
- RocksDB stats per CF; where feasible, attribute hot prefixes by sampling.

Structured logs include `tenant_id` and `queue_id` fields.

## Admin operations

Introduce admin RPCs (tenant-scoped):
- `CreateQueue(tenant, queue)` → creates `queue_id` under `tenant_id`.
- `DeleteQueue(tenant, queue)` → two-phase delete (see below).
- `ListQueues(tenant)` → enumerate queues.

Deletion safety: mark queue as `draining`, stop accepting new adds/polls, wait for in-flight leases to settle or force-timeout, then purge all keys under `P/…`.

## Migration and compatibility

- New writes always include the queue prefix.
- Readers can support dual-mode: if prefix is missing, interpret as `default/default` for legacy data.
- Optional one-time migration tool to rewrite legacy keys into prefixed form with minimal downtime.
- Version the RPC schema to add `QueueRef`; default to `default/default` when absent.

## Testing strategy

- Property tests over multiple tenants/queues exercising isolation: deliveries, leases, and indices must not cross prefixes.
- Stress tests with concurrent `poll/add/extend/ack/remove` across many queues under high contention; verify no duplicate deliveries.
- Fault injection during expiry and extend; ensure recovery is correct within a queue prefix after restart.
- Performance benchmarks: single-tenant single-queue baseline vs same-tenant multi-queue and cross-tenant parallel loads.

## Rollout plan (increments)

1. Add RPC `QueueRef` and auth mapping to `tenant_id` with defaults for legacy clients.
2. Introduce metadata maps/caching for tenant/queue IDs.
3. Add key prefixing to all CFs and bound iterators.
4. Shard routing by `(tenant_id, queue_id)` and per-shard poll coalescer.
5. Enforce per-tenant/queue rate limits and concurrency caps.
6. Add metrics/log labels for tenant/queue.
7. Admin RPCs and safe deletion.
8. Optional migration tooling and rollout.

## Worked examples

### Key bounds for a queue

```text
tenant_id = 0x0000002A   # 42
queue_id  = 0x00000007   # 7
P = "\x00\x00\x00\x2A\x00\x00\x00\x07"

# Visibility range for now=1699999999999 (example)
lower = b"vi/" + P + b"/" + be64(0)
upper = b"vi/" + P + b"/" + be64(1699999999999)
```

### RPC example

```json
{
  "ref": {"tenant": "acme", "queue": "payments"},
  "numItems": 32,
  "waitMs": 2000
}
```

### Authorization mapping

```text
mTLS: SAN = queueber.acme.com → tenant_id = 42
JWT:  sub = user-123, claim tenant=acme → tenant_id = 42
```

## Open questions

- Do we require admin RPCs for queue creation, or allow first-write creation based on auth policy?
- Do we export name→ID mappings for metrics to cap cardinality, or emit names directly with sampling?

## References

- [poll_coalescing_sketch.md](./poll_coalescing_sketch.md)
- [rocksdb-column-families.md](./rocksdb-column-families.md)
- [perf-notes.md](./perf-notes.md)
- [db-access-patterns.md](./db-access-patterns.md)

