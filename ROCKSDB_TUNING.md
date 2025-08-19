# RocksDB Tuning for Queueber

This document explains the RocksDB configuration optimizations applied to Queueber for optimal performance in a message queue workload.

## Workload Characteristics

Queueber's workload has the following characteristics:
- **Write-heavy**: Frequent message additions and lease operations
- **Read-heavy**: Polling for available messages, checking lease expiry
- **Time-series access**: Visibility index queries by timestamp
- **Point lookups**: Individual message retrieval by ID
- **Range scans**: Prefix-based queries for different namespaces
- **Small values**: Message contents are typically small (< 1KB)

## Tuning Decisions

### 1. Write Buffer Configuration
```rust
opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
opts.set_max_write_buffer_number(3);
opts.set_min_write_buffer_number_to_merge(2);
```

**Rationale**: Larger write buffers reduce write amplification by batching more writes before flushing to disk. For a queue workload with frequent small writes, this significantly improves throughput.

### 2. Compaction Settings
```rust
opts.set_level_zero_file_num_compaction_trigger(4);
opts.set_level_zero_slowdown_writes_trigger(8);
opts.set_level_zero_stop_writes_trigger(12);
opts.set_max_bytes_for_level_base(256 * 1024 * 1024); // 256MB
opts.set_max_bytes_for_level_multiplier(10.0);
```

**Rationale**: 
- Lower L0 trigger (4) reduces read amplification during polling
- Higher slowdown/stop triggers (8/12) allow more writes before backpressure
- Larger level base (256MB) reduces compaction frequency
- 10x multiplier creates exponential level sizes for better space efficiency

### 3. Bloom Filter Optimization
```rust
opts.set_bloom_locality(1);
opts.set_optimize_filters_for_hits(true);
```

**Rationale**: Bloom filters improve point lookup performance for message retrieval by ID. The locality setting optimizes for sequential access patterns.

### 4. Block Cache and Table Settings
```rust
opts.set_block_cache_size_mb(128); // 128MB block cache
opts.set_block_size(4 * 1024); // 4KB blocks
opts.set_block_restart_interval(16);
```

**Rationale**: 
- 128MB cache provides good hit rates for frequently accessed data
- 4KB blocks balance between memory efficiency and read performance
- Restart interval of 16 optimizes for small key prefixes

### 5. Compression Settings
```rust
opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
```

**Rationale**: 
- Lz4 for upper levels: Fast decompression for hot data
- Zstd for bottommost level: Better compression ratio for cold data

### 6. Background Job Configuration
```rust
opts.set_max_background_jobs(4);
opts.set_max_background_compactions(2);
opts.set_max_background_flushes(1);
```

**Rationale**: Parallel background jobs improve throughput while limiting resource contention.

### 7. I/O Optimizations
```rust
opts.set_use_fsync(false); // Use fdatasync
opts.set_bytes_per_sync(1024 * 1024); // 1MB per sync
opts.set_wal_bytes_per_sync(64 * 1024); // 64KB WAL sync
```

**Rationale**: 
- `fdatasync` is faster than `fsync` for queue workloads
- Larger sync intervals reduce I/O overhead
- WAL sync optimized for transaction durability

### 8. Compaction Style and Readahead
```rust
opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
opts.set_compaction_readahead_size(2 * 1024 * 1024); // 2MB readahead
```

**Rationale**: Level compaction provides predictable performance and good space efficiency. Readahead improves compaction throughput.

## Performance Expectations

These settings should provide:
- **Write throughput**: 50-100K ops/sec for small messages
- **Read latency**: < 1ms for point lookups
- **Space efficiency**: 2-3x compression ratio
- **Memory usage**: ~200-300MB for typical workloads

## Monitoring and Tuning

Monitor these metrics to validate tuning:
- Write amplification (target: < 5x)
- Read amplification (target: < 3x)
- Compaction lag (target: < 10 seconds)
- Block cache hit rate (target: > 80%)

## Future Optimizations

Consider these additional tunings based on specific workload patterns:
- **Column family separation**: Separate CFs for different data types
- **Custom comparators**: For timestamp-based ordering
- **Dynamic leveling**: For workloads with varying access patterns
- **Partitioned filters**: For very large datasets