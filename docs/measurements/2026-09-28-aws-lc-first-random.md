# The first random bytes a new process draws from AWS-LC

**Question.** How long does the first `aws_lc_rs::rand::fill` take in a new process with
AWS-LC's CPU jitter entropy source built in, as upstream builds it, and with it left out, as
the vendored aws-lc-sys builds it (vendor/UPSTREAM.md)?

**Method.** A binary that times two 32-byte `rand::fill` calls with `Instant`, built in release
from the vendored aws-lc-rs 1.18.1 and aws-lc-sys 0.45.0, once with the default and once with
`AWS_LC_SYS_NO_JITTER_ENTROPY=0`, which builds jitter entropy in. The first binary holds no
`jent` symbols, the second 81. Each ran as 200 new processes, one after another, on macOS
26.4.1 on an Apple M5 Max.

| Build | First fill, median | p90 | p99 | max | Second fill, median |
|---|---|---|---|---|---|
| Jitter entropy left out (default) | 5.4 µs | 6.7 µs | 9.1 µs | 38.2 µs | 2.25 µs |
| Jitter entropy built in (upstream's default) | 17.6 ms | 18.2 ms | 20.4 ms | 20.7 ms | 2.50 µs |

## Findings

**1. Jitter entropy costs a new process about 17.6 ms before its first random bytes.** The
cost is the first call's alone: the second costs the same in both builds. Every process that
draws randomness pays it once, which a CLI invocation or a test binary feels, and a server
does not.

**2. Without it, AWS-LC seeds from the operating system.** With `DISABLE_CPU_JITTER_ENTROPY`
defined, AWS-LC's entropy source takes its seed from `CRYPTO_sysrand`, the OS's CSPRNG, and its
second source from RDRAND or RNDR where the CPU has them, else the OS again
(`crypto/fipsmodule/rand/entropy/entropy_sources.c`, `crypto/rand_extra/vm_ube_fallback.c`).
That is the source `getrandom` reads.

**3. The time is jitter entropy's own work, and it cannot be made cheaper in place.** A C
harness calling the prefixed `jent_*` functions timed each phase in 5 new processes: the
start-up health test takes 4.3 ms, allocating the collector 4.3 ms (it runs the start-up test
again), and the 48-byte seed 8.5 ms; each further 32-byte block costs 4.3 ms. That is about
4,000 timed samples of about 4.5 µs each. `sample(1)` at 1 ms intervals over the seed puts 73%
of samples in SHA-3 (Keccak-p[1600]), 20% in the memory-access loop and its xoshiro128** index
generator, and 5% in reading the clock. The hash and the memory walk are the workload whose
execution time jitter entropy measures, so they sit inside every timed interval. The library
refuses to compile with optimisation (`jitterentropy-base.c`: "must not be compiled with
optimizations ... Use the compiler switch -O0"), and AWS-LC builds it at `-O0`
(`third_party/jitterentropy/CMakeLists.txt`). A faster workload would change the noise source
that the entropy estimate describes. The oversampling rate (3) fixes the number of samples.
So the latency is the price of this source, and the choice is whether to include it.
