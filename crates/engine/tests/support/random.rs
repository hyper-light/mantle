//! RocksDB's `util/random.h` `Random` and `test::RandomSeed`, for the ported tests that draw
//! their inputs from them: the Lehmer generator x ← 16807·x mod (2^31 − 1) (Park and Miller,
//! "Random number generators: good ones are hard to find", CACM 31(10), 1988).

pub struct Random(u32);

const M: u32 = 2_147_483_647;
const A: u64 = 16_807;

impl Random {
    pub fn new(s: u32) -> Self {
        Self(if s & M != 0 { s & M } else { 1 })
    }

    pub fn next(&mut self) -> u32 {
        let product = u64::from(self.0) * A;
        self.0 = ((product >> 31) + (product & u64::from(M))) as u32;
        if self.0 > M {
            self.0 -= M;
        }
        self.0
    }

    pub fn uniform(&mut self, n: u32) -> u32 {
        self.next() % n
    }

    pub fn one_in(&mut self, n: u32) -> bool {
        self.uniform(n) == 0
    }
}

/// `test::RandomSeed` [R test_util/testharness.cc]: RocksDB's seed when `TEST_RANDOM_SEED` is unset,
/// 301. The port's tests read no environment, so every run is the same run.
pub fn random_seed() -> u32 {
    301
}
