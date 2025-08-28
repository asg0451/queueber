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

# Minimal runtime deps and healthcheck tool
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user and data dir
RUN useradd -u 10001 -r -m -d /home/queueber queueber && \
    mkdir -p /var/lib/queueber && \
    chown -R queueber:queueber /var/lib/queueber

# Copy binary
COPY --from=builder /app/target/release/queueber /usr/local/bin/queueber

USER queueber
ENV RUST_LOG=info
EXPOSE 9090

# Basic TCP healthcheck on the Cap'n Proto port
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
  CMD nc -z 127.0.0.1 9090 || exit 1

# Default entrypoint; can be overridden
ENTRYPOINT ["/usr/local/bin/queueber", "--listen", "0.0.0.0:9090", "--data-dir", "/var/lib/queueber"]