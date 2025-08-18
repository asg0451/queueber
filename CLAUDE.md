# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Queueber is a message queue implementation in Rust using Cap'n Proto RPC for communication and RocksDB for persistence. It provides basic queue operations: add items, poll for items with leases, and remove items.

## Common Commands

### Building
```bash
cargo build
cargo build --release
```

### Testing
```bash
cargo test
```

### Running the Server
```bash
cargo run --bin queueber
```
Server runs on `127.0.0.1:9090` and stores data in `/tmp/queueber/data`

### Running the Client
```bash
cargo run --bin client
```
Connects to server at `localhost:9090` and demonstrates adding an item

## Architecture

### Core Components

- **Storage Layer** (`src/storage.rs`): RocksDB-based persistence with key prefixes:
  - `available/` + id → stored items ready for polling
  - `in_progress/` + id → items currently leased
  - `visibility_index/` + timestamp → visibility timeout index
  - `leases/` + lease_id → lease entries with item keys

- **Server** (`src/server.rs`): Cap'n Proto RPC server implementing the Queue interface with methods:
  - `add()`: Stores items in the available queue
  - `poll()`: Not yet implemented - should move items to in_progress with lease and return them to the client
  - `remove()`: Not yet implemented - should remove items from in_progress

- **Protocol** (`src/protocol.rs`): Cap'n Proto generated code from `queueber.capnp` schema

- **Error Handling** (`src/errors.rs`): Centralized error types using thiserror

### Key Design Patterns

- Items use UUID v7 for time-ordered IDs
- Visibility timeouts implemented via timestamp-based index keys
- Leases track which items are currently being processed
- All database operations use atomic batches for consistency

### Schema Generation

The Cap'n Proto schema (`queueber.capnp`) is compiled during build via `build.rs`. Uses Rust nightly toolchain as specified in `rust-toolchain.toml`.

## Code Style Notes

- Prefer iterator combinators (`iter`, `map`, `filter`, `filter_map`, `collect`) over index-based loops when collecting or transforming data.
- Avoid `map_err` whenever possible; use `?` and convert errors at boundaries.
- When sharing `Arc<T>`, prefer `Arc::clone(&arc_value)` over calling `.clone()` directly on the variable for clarity and to avoid accidental inner clones.

## Current State

This is an early-stage implementation. Key unimplemented features noted in TODO.md:
- Lease expiry mechanism
- Complete poll implementation
- Remove functionality
- Extend lease functionality