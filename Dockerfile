# syntax=docker/dockerfile:1.7

# Multi-stage build: compile with Rust nightly and required native deps, run on slim Debian as non-root.

FROM debian:bookworm-slim AS builder
ARG RUST_TOOLCHAIN=nightly
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_TERM_COLOR=always \
    RUST_BACKTRACE=1 \
    RUSTFLAGS="--cfg tokio_unstable"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl \
        build-essential pkg-config cmake \
        clang libclang-dev \
        capnproto libcapnp-dev \
        zlib1g-dev liblz4-dev libzstd-dev libsnappy-dev libbz2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install rustup and Rust nightly toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain ${RUST_TOOLCHAIN}
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Pre-copy manifests to leverage Docker layer caching
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY build.rs ./

# Copy sources
COPY src ./src
COPY queueber.capnp ./
COPY rust.capnp ./
COPY benches ./benches

# Build release binary
RUN cargo build --release \
 && strip target/release/queueber


FROM debian:bookworm-slim AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates bash \
        libgcc-s1 libstdc++6 \
        zlib1g liblz4-1 libzstd1 libsnappy1v5 libbz2-1.0 \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Create non-root user and data directory
RUN useradd -r -u 10001 -g users queueber \
    && mkdir -p /var/lib/queueber/data \
    && chown -R queueber:users /var/lib/queueber

COPY --from=builder /app/target/release/queueber /usr/local/bin/queueber

USER queueber
EXPOSE 9090/tcp
ENV RUST_LOG=info

# Simple TCP healthcheck against Cap'n Proto RPC listener
HEALTHCHECK --interval=10s --timeout=2s --retries=5 \
  CMD bash -c '</dev/tcp/127.0.0.1/9090' || exit 1

ENTRYPOINT ["/usr/local/bin/queueber"]
CMD ["--listen", "0.0.0.0:9090", "--data-dir", "/var/lib/queueber/data"]