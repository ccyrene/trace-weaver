# syntax=docker/dockerfile:1
#
# Production image for the `trace-weaver` data-lineage compiler CLI.
#
# Multi-stage build:
#   1. `builder`  — compiles a stripped release binary on the full Rust toolchain.
#   2. `runtime`  — minimal Debian slim image carrying only the binary + CA roots.
#
# Build context is the repository root. Example DAGs are intentionally NOT baked
# in: at runtime the user mounts their own DAGs as a volume under /work.
#
#   docker build -t trace-weaver:0.3.0 .
#   docker run --rm -v "$PWD/dags:/work/dags" trace-weaver:0.3.0 scan dags -o /work/out.weave.json

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM rust:1.94-bookworm AS builder

WORKDIR /src

# Copy only what the build needs so unrelated context changes don't bust the
# layer cache. Lockfile is copied for a reproducible, pinned dependency graph.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the release binary for just the CLI crate, then strip symbols.
# (The release profile already sets strip = true, but we strip explicitly so
# the image stays small even if that profile setting ever changes.)
RUN cargo build --release --locked -p trace-weaver-cli \
    && strip target/release/trace-weaver

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates is REQUIRED: the OpenMetadata exporter talks to real
# OpenMetadata instances over HTTPS. Without the CA trust store, TLS handshakes
# fail and live export is impossible. Clean the apt lists to keep the layer small.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run as a dedicated, unprivileged user. Fixed uid:gid 10001 keeps file
# ownership predictable for mounted volumes across hosts.
RUN groupadd --gid 10001 traceweaver \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin traceweaver \
    && mkdir -p /work \
    && chown traceweaver:traceweaver /work

# Install the compiled binary onto the default PATH.
COPY --from=builder /src/target/release/trace-weaver /usr/local/bin/trace-weaver

# /work is where the user mounts their DAGs and where outputs are written.
WORKDIR /work
USER traceweaver:traceweaver

# No HEALTHCHECK: `trace-weaver` is a one-shot, run-to-completion CLI, not a long-lived
# service. There is no process to probe, so a HEALTHCHECK would be meaningless.

# OCI image metadata.
LABEL org.opencontainers.image.title="trace-weaver" \
      org.opencontainers.image.description="trace-weaver data-lineage compiler: scans annotated Python Airflow DAGs into the weave universal JSON format and exports to OpenMetadata / OpenLineage / DOT." \
      org.opencontainers.image.source="https://github.com/ccyrene/trace-weaver" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="0.3.0"

ENTRYPOINT ["trace-weaver"]
CMD ["--help"]
