# --- Builder stage ---
FROM rust:bookworm AS builder

# Install dependencies for building (Cap'n Proto CLI, compilers, and C/C++ libs)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        capnproto \
        libclang-19-dev \
        clang \
        cmake \
        pkg-config \
        make \
        g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Leverage Docker layer caching: copy manifests first
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs queueber.capnp rust.capnp ./
COPY src ./src

# Build release binary (rust-toolchain.toml pins nightly)
RUN cargo build --release --bin queueber

# --- Runtime stage ---
FROM debian:bookworm-slim AS runtime

# Create non-root user and data dir
RUN useradd -u 10001 -r -m -d /home/queueber queueber && \
    mkdir -p /data && \
    chown -R queueber:queueber /data

# Copy binary
COPY --from=builder /app/target/release/queueber /usr/local/bin/queueber

USER queueber
ENV RUST_LOG=info
EXPOSE 9090
# Tokio console default port
EXPOSE 6669

# Default entrypoint; can be overridden
ENTRYPOINT ["/usr/local/bin/queueber", "--listen", "0.0.0.0:9090", "--data-dir", "/data"]