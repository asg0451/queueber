# Poll Request Coalescing Implementation Sketch

## Problem Statement

Under high concurrency, multiple clients polling simultaneously create contention on the database as each poll request:
1. Acquires a RocksDB transaction
2. Scans the visibility index 
3. Locks and moves items from available to in-progress
4. Creates lease entries

This leads to:
- High database lock contention
- Reduced throughput under load
- Increased latency for individual poll requests
- Potential for transaction conflicts and retries

## Proposed Solution: Poll Request Coalescing

### Overview

Implement a batching mechanism that coalesces multiple concurrent poll requests into a single database operation, then distributes the results back to the original requesters.

### Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client Poll   │───▶│  Poll Batcher    │───▶│  Database       │
│   Requests      │    │  (Coalescer)     │    │  Transaction    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       ▼                       │
         │              ┌──────────────────┐             │
         │              │  Result Router   │             │
         │              │  (Distributor)   │             │
         │              └──────────────────┘             │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Client Poll   │◀───│  Poll Batcher    │◀───│  Database       │
│   Responses     │    │  (Coalescer)     │    │  Results        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Key Components

#### 1. Poll Request Queue
- Thread-safe queue to collect incoming poll requests
- Configurable batching window (e.g., 1-10ms)
- Maximum batch size limit (e.g., 100 requests)

#### 2. Batch Processor
- Processes queued requests in batches
- Aggregates total items needed across all requests
- Executes single database transaction for the entire batch
- Distributes results back to original requesters

#### 3. Result Distribution
- Fair distribution algorithm (round-robin, weighted, etc.)
- Handles partial fulfillment when insufficient items available
- Maintains request ordering within batches

### Implementation Details

#### Data Structures

```rust
#[derive(Debug)]
struct PollRequest {
    id: Uuid,
    num_items: usize,
    lease_validity_secs: u64,
    timeout_secs: u64,
    response_tx: oneshot::Sender<PollResult>,
    created_at: Instant,
}

#[derive(Debug)]
struct PollBatch {
    requests: Vec<PollRequest>,
    total_items_needed: usize,
    max_lease_validity: u64,
    created_at: Instant,
}

#[derive(Debug)]
struct PollResult {
    lease: [u8; 16],
    items: Vec<PolledItemOwnedReader>,
    partial: bool, // true if fewer items than requested
}
```

#### Batching Logic

```rust
struct PollBatcher {
    request_queue: Arc<Mutex<VecDeque<PollRequest>>>,
    batch_window: Duration,
    max_batch_size: usize,
    max_batch_items: usize,
    storage: Arc<RetriedStorage<Storage>>,
}

impl PollBatcher {
    async fn add_request(&self, request: PollRequest) {
        let mut queue = self.request_queue.lock().await;
        queue.push_back(request);
        
        // Trigger batch processing if queue is full
        if queue.len() >= self.max_batch_size {
            self.process_batch().await;
        }
    }
    
    async fn process_batch(&self) -> Result<()> {
        let mut queue = self.request_queue.lock().await;
        if queue.is_empty() {
            return Ok(());
        }
        
        // Group requests into batches
        let mut current_batch = PollBatch::new();
        let mut batches = Vec::new();
        
        while let Some(request) = queue.pop_front() {
            if current_batch.can_add(&request, self.max_batch_items) {
                current_batch.add(request);
            } else {
                if !current_batch.is_empty() {
                    batches.push(current_batch);
                }
                current_batch = PollBatch::new();
                current_batch.add(request);
            }
        }
        
        if !current_batch.is_empty() {
            batches.push(current_batch);
        }
        
        drop(queue);
        
        // Process each batch
        for batch in batches {
            self.process_single_batch(batch).await?;
        }
        
        Ok(())
    }
    
    async fn process_single_batch(&self, batch: PollBatch) -> Result<()> {
        // Execute single database transaction for entire batch
        let (lease, items) = self.storage
            .get_next_available_entries_with_lease(
                batch.total_items_needed, 
                batch.max_lease_validity
            )
            .await?;
            
        // Distribute items fairly among requests
        self.distribute_results(batch, lease, items).await;
        
        Ok(())
    }
    
    async fn distribute_results(
        &self, 
        batch: PollBatch, 
        lease: [u8; 16], 
        items: Vec<PolledItemOwnedReader>
    ) {
        let mut item_iter = items.into_iter();
        
        for request in batch.requests {
            let mut request_items = Vec::new();
            let mut partial = false;
            
            // Distribute items round-robin style
            for _ in 0..request.num_items {
                if let Some(item) = item_iter.next() {
                    request_items.push(item);
                } else {
                    partial = true;
                    break;
                }
            }
            
            let result = PollResult {
                lease,
                items: request_items,
                partial,
            };
            
            // Send result back to original requester
            let _ = request.response_tx.send(result);
        }
    }
}
```

#### Integration with Server

```rust
impl crate::protocol::queue::Server for Server {
    fn poll(&mut self, params: PollParams, mut results: PollResults) -> Promise<(), capnp::Error> {
        let (response_tx, response_rx) = oneshot::channel();
        
        let request = PollRequest {
            id: Uuid::now_v7(),
            num_items: params.get()?.get_req()?.get_num_items() as usize,
            lease_validity_secs: params.get()?.get_req()?.get_lease_validity_secs(),
            timeout_secs: params.get()?.get_req()?.get_timeout_secs(),
            response_tx,
            created_at: Instant::now(),
        };
        
        // Add to batcher
        let batcher = Arc::clone(&self.poll_batcher);
        let _ = batcher.add_request(request);
        
        Promise::from_future(async move {
            // Wait for response from batcher
            let poll_result = response_rx.await
                .map_err(|_| capnp::Error::failed("poll request cancelled".to_string()))?;
                
            // Build response
            write_poll_response(&poll_result.lease, poll_result.items, &mut results)?;
            Ok(())
        })
    }
}
```

### Configuration Options

```rust
#[derive(Debug, Clone)]
pub struct PollCoalescingConfig {
    /// Maximum number of requests to batch together
    pub max_batch_size: usize,
    /// Maximum total items to request in a single batch
    pub max_batch_items: usize,
    /// Maximum time to wait before processing a batch
    pub batch_window_ms: u64,
    /// Whether coalescing is enabled
    pub enabled: bool,
}
```

### Benefits

1. **Reduced Database Contention**: Single transaction instead of N concurrent transactions
2. **Improved Throughput**: Better resource utilization under high load
3. **Lower Latency**: Reduced lock wait times and transaction conflicts
4. **Fair Distribution**: Ensures all clients get their fair share of items
5. **Backward Compatibility**: Transparent to clients, no API changes needed

### Potential Challenges

1. **Complexity**: Adds significant complexity to the polling logic
2. **Latency Trade-offs**: May increase latency for individual requests due to batching delay
3. **Fairness**: Need to ensure fair distribution of items among batched requests
4. **Timeout Handling**: Complex timeout semantics when requests are batched
5. **Error Handling**: Need to handle partial failures and ensure all clients get responses

### Implementation Phases

#### Phase 1: Basic Batching
- Implement simple request queue and batch processor
- Basic round-robin distribution
- Minimal configuration options

#### Phase 2: Advanced Features
- Configurable batching windows and sizes
- Fair distribution algorithms
- Timeout handling improvements
- Metrics and monitoring

#### Phase 3: Optimization
- Performance tuning based on real-world usage
- Advanced distribution strategies
- Integration with existing monitoring

### Testing Strategy

1. **Unit Tests**: Test batching logic, distribution algorithms
2. **Integration Tests**: Test with real database transactions
3. **Stress Tests**: Verify performance improvements under high concurrency
4. **Fairness Tests**: Ensure fair distribution across multiple clients
5. **Timeout Tests**: Verify proper timeout handling in batched scenarios

### Metrics to Track

- Average batch size
- Batch processing latency
- Items per batch
- Distribution fairness
- Database transaction conflicts
- Overall poll throughput improvement