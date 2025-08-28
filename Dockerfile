# --- Builder stage ---
FROM rust:trixie AS builder

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
        binutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Leverage Docker layer caching: copy manifests first
COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs queueber.capnp rust.capnp .cargo ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests

ENV RUSTFLAGS="--cfg tokio_unstable"
ENV RUST_BACKTRACE=1

# Build optimized binary (rust-toolchain.toml pins nightly)
# Use the custom fastrelease profile defined in Cargo.toml
RUN cargo build --profile fastrelease --bin queueber
RUN strip target/fastrelease/queueber || true

# --- Runtime stage ---
FROM debian:trixie-slim AS runtime

# Create non-root user and data dir
RUN useradd -u 10001 -r -m -d /home/queueber queueber && \
    mkdir -p /data && \
    chown -R queueber:queueber /data

# Copy binary
# Copy binary built with fastrelease profile
COPY --from=builder /app/target/fastrelease/queueber /usr/local/bin/queueber

USER queueber
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

EXPOSE 9090
# Tokio console default port
EXPOSE 6669

ENTRYPOINT ["/usr/local/bin/queueber", "--listen", "0.0.0.0:9090", "--data-dir", "/data"]
