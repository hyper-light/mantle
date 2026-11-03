# syntax=docker/dockerfile:1.7
# The mantle image (docs/design/kubernetes.md): the `mantle` binary alone, the workspace's release
# profile, on a distroless base with no shell, no package manager and a non-root user. One binary
# is the node (`mantle serve`), its install step (`mantle install`), its readiness check (`mantle
# status`) and the cell's clients. `mantle serve` is PID 1 and spawns nothing, so no init shim is
# needed to reap children; SIGTERM begins its orderly stop, bounded by its `stop_ms`
# (docs/design/node.md §1.7).
#
# Built for the builder's own architecture; `docker build` from the repository root. The cache
# mounts keep the registry, the git checkouts and the target directory across builds, so a rebuild
# after a source edit is incremental.

FROM rust:1.98.1 AS build
WORKDIR /work
# The caller can bound build concurrency.
ARG CARGO_BUILD_JOBS
COPY . .
# `--locked`: the lock file is the contract. The image's installed toolchain is selected by name, so
# the build does not download the components rust-toolchain.toml lists for development.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/work/target,sharing=locked \
    RUSTUP_TOOLCHAIN=1.98.1 cargo build --release --locked -p mantle \
    && cp target/release/mantle /mantle

# The runtime base is the builder's own Debian release (13): the binary links the builder's glibc,
# which a distroless image of an older release refuses at exec (slates' Dockerfile, measured
# 2026-09-14). Bump both together.
FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /mantle /mantle
USER nonroot:nonroot
ENTRYPOINT ["/mantle"]
