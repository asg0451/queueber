Queueber
========

A simple queue server and client built with Cap'n Proto RPC and RocksDB.

Prerequisites
-------------

- Rust (nightly toolchain; e.g., `rustup default nightly`)
- Cap'n Proto CLI (`capnp`) version 0.5.2 or newer
  - Install instructions: https://capnproto.org/install.html
  - Ensure `capnp` is on your PATH; the build script will fail without it.

Build
-----

```
cargo build
```

If you see a schema compilation error from the build script, install the Cap'n Proto CLI as noted above and rebuild.

Running
-------

- Server:
  - Defaults: `127.0.0.1:9090`, data dir `/tmp/queueber/data`
  - Example:
    ```
    cargo run --bin queueber -- --listen 0.0.0.0:9090 --data-dir /var/lib/queueber
    ```

- Client (add one item):
  ```
  cargo run --bin client -- --addr localhost:9090 add --contents "hello" --visibility 10
  ```
Queueber
========

A simple queue server and client built with Cap'n Proto RPC and RocksDB.

Prerequisites
-------------

- Rust (nightly toolchain; e.g., `rustup default nightly`)
- Cap'n Proto CLI (`capnp`) version 0.5.2 or newer
  - Install instructions: https://capnproto.org/install.html
  - Ensure `capnp` is on your PATH; the build script will fail without it.

Build
-----

```
cargo build
```

If you see a schema compilation error from the build script, install the Cap'n Proto CLI as noted above and rebuild.

Running
-------

- Server:
  - Defaults: `127.0.0.1:9090`, data dir `/tmp/queueber/data`
  - Example:
    ```
    cargo run --bin queueber -- --listen 0.0.0.0:9090 --data-dir /var/lib/queueber
    ```

- Client (add one item):
  ```
  cargo run --bin client -- --addr localhost:9090 add --contents "hello" --visibility 10
  ```
