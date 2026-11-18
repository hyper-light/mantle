# The VM lane's image, mantle-bench:1.98.1 (scripts/doors-trials.sh, MACHINE=vm, VM_IMAGE): every
# case of the lane runs in a container of it on the Mac's Linux VM (linux/arm64), and
# scripts/compare/setup.sh vm builds the lane's msquic and quinn in it, so those binaries link the
# glibc they run with. Its base is the mantle image's builder (the repository's Dockerfile), so the
# lane's mantle arms build and run on the same toolchain and Debian release; the repository pins
# its bases by tag, as here.
#
# What runs in it, and the package that brings each (Debian 13):
#   doors-trials.sh's case: python3 (the rusage wrapper), timeout, seq and sleep (coreutils), curl
#     (the S3 readiness probe), grep, pgrep (procps: a comparator's server pid), ss (iproute2: the
#     QUIC server's bound port), bash (the case, the busy loops); lsof beside ss and taskset
#     (util-linux) beside the box's binding, for a case run by hand;
#   the comparators' start.sh: bash, head, od, tr, cut, cat and seq (coreutils), curl (SeaweedFS's
#     filer probe);
#   setup.sh vm's msquic build: python3-venv (the cmake and ninja the box's build pinned), cmake's
#     compilers, make and perl (quictls's Configure) from the base.
# perf is left out: Docker's default seccomp profile refuses perf_event_open to a container without
# CAP_PERFMON or CAP_SYS_ADMIN (docs.docker.com/engine/security/seccomp, "Significant syscalls
# blocked by the default profile"), which the lane's `docker run` does not grant, and
# doors-trials.sh passes --perf wherever a perf binary is found.
#
#   docker build --platform linux/arm64 -t mantle-bench:1.98.1 -f scripts/compare/mantle-bench.Dockerfile scripts/compare

FROM rust:1.98.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash ca-certificates coreutils curl grep iproute2 lsof procps python3 python3-venv util-linux \
    && rm -rf /var/lib/apt/lists/*
