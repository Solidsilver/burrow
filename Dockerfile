# syntax=docker/dockerfile:1

# burrow container image.
#
# Two final targets share ONE runtime definition so they can't drift apart:
#
#   * source   (default) — compiles burrow from this checkout. Plain
#                          `docker build .` works anywhere, no prebuilt binary
#                          needed. This is what end users get.
#
#   * prebuilt           — release/CI target. Copies an already-compiled binary
#                          from the build context so the published image ships
#                          the exact same binary as the tarball/.deb/.rpm.
#                          Build it with:
#                            cp target/release/burrow ./burrow
#                            docker build --target prebuilt -t burrow .
#
# Only the single "where does the binary come from" line differs between the
# two; everything about HOW the container runs lives once in the `runtime`
# stage below, so there is nothing to keep in sync by hand.

# ---- build from source ------------------------------------------------------
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
# The release profile already sets thin LTO + strip (see Cargo.toml). rusqlite
# is compiled bundled and blake3 builds C/SIMD, but the rust image ships a C
# compiler, so no extra apt packages are required.
RUN cargo build --release -p burrow

# ---- shared runtime definition ----------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 burrow \
 && useradd  --system --uid 10001 --gid burrow \
      --home-dir /var/lib/burrow --no-create-home \
      --shell /usr/sbin/nologin burrow \
 && mkdir -p /etc/burrow /var/lib/burrow /run/burrow \
 && chown burrow:burrow /etc/burrow /var/lib/burrow /run/burrow \
 && chmod 0750 /etc/burrow \
 && chmod 0700 /var/lib/burrow
# Pin every path so the daemon never depends on HOME/XDG. Mirrors
# contrib/burrow.service: config (incl. the secret repo key) in /etc/burrow,
# data + blobs in /var/lib/burrow, control socket on ephemeral /run/burrow.
ENV BURROW_CONFIG_DIR=/etc/burrow \
    BURROW_DATA_DIR=/var/lib/burrow \
    BURROW_SOCKET=/run/burrow/daemon.sock
LABEL org.opencontainers.image.source="https://github.com/solidsilver/burrow" \
      org.opencontainers.image.description="Distributed backup among friends, over iroh" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
WORKDIR /var/lib/burrow
VOLUME ["/etc/burrow", "/var/lib/burrow"]
# `burrow` is the entrypoint, so `docker run IMG` starts the daemon while
# `docker run IMG init` / `docker run IMG status` run one-off CLI commands
# against the same pinned env.
ENTRYPOINT ["burrow"]
CMD ["daemon", "run"]

# ---- final: prebuilt binary (release/CI target) -----------------------------
FROM runtime AS prebuilt
COPY burrow /usr/local/bin/burrow
USER burrow

# ---- final: from source (default target — keep last) ------------------------
FROM runtime AS source
COPY --from=builder /src/target/release/burrow /usr/local/bin/burrow
USER burrow
