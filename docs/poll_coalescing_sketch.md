# Poll Request Coalescing Implementation Sketch

## Problem

Under high concurrency, multiple clients polling simultaneously create database contention:
- Each poll acquires a RocksDB transaction
- Scans visibility index and locks items
- Creates lease entries
- Leads to high lock contention, reduced throughput, and increased latency

## Solution

Batch multiple concurrent poll requests into a single database operation, then distribute results back to original requesters.

### Architecture

```
Client Poll Requests → Poll Batcher → Database Transaction → Result Router → Client Responses
```

### Key Components

1. **Request Queue**: Thread-safe queue with configurable batching window (1-10ms)
2. **Batch Processor**: Aggregates requests and executes single database transaction
3. **Result Distribution**: Fair distribution of items back to requesters

### Implementation Approach

- **Data Structures**: `PollRequest`, `PollBatch`, `PollResult` for managing batched operations
- **Batching Logic**: Configurable batch sizes and windows, round-robin distribution
- **Server Integration**: Transparent integration with existing RPC server using oneshot channels

### Configuration

```rust
struct PollCoalescingConfig {
    max_batch_size: usize,      // Max requests per batch
    max_batch_items: usize,     // Max total items per batch  
    batch_window_ms: u64,       // Max wait time
    enabled: bool,
}
```

### Benefits

- **Reduced Contention**: Single transaction instead of N concurrent
- **Improved Throughput**: Better resource utilization under load
- **Lower Latency**: Reduced lock wait times
- **Fair Distribution**: All clients get fair share
- **Backward Compatible**: No API changes needed

### Challenges

- **Complexity**: Adds significant complexity to polling logic
- **Latency Trade-offs**: Batching delay may increase individual request latency
- **Fairness**: Need fair distribution algorithms
- **Timeout Handling**: Complex semantics when requests are batched
- **Error Handling**: Partial failures and response guarantees

### Implementation Phases

1. **Phase 1**: Basic batching with simple distribution
2. **Phase 2**: Configurable windows, fairness algorithms, timeout handling
3. **Phase 3**: Performance tuning and monitoring integration

### Testing & Metrics

- Unit tests for batching logic
- Stress tests for performance improvements
- Fairness tests across multiple clients
- Track: batch sizes, latency, distribution fairness, transaction conflicts