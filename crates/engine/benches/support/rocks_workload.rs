//! The key stream of RocksDB 11.8.1's one-thread `fillrandom,readrandom,seekrandom`.
//! `util/random.h` uses `std::mt19937_64`; `tools/db_bench_tool.cc` seeds each benchmark
//! with `seed + total_thread_count` and consumes a database-selection draw before each
//! fill and read key, even with one database. `seekrandom` consumes only its key draw.

pub struct Rng {
    state: [u64; 312],
    at: usize,
    database_draw: bool,
}

impl Rng {
    pub fn new(seed: u64, phase: u64) -> Self {
        // MT19937-64's published parameters (Matsumoto & Nishimura's mt19937-64.c),
        // also the parameters of C++'s std::mt19937_64. These are algorithm constants.
        let mut state = [0; 312];
        state[0] = seed.wrapping_add(phase);
        for i in 1..state.len() {
            let last = state[i - 1];
            state[i] = 6364136223846793005u64
                .wrapping_mul(last ^ (last >> 62))
                .wrapping_add(i as u64);
        }
        Self {
            state,
            at: 312,
            database_draw: phase != 3,
        }
    }

    fn draw(&mut self) -> u64 {
        if self.at == self.state.len() {
            for i in 0..self.state.len() {
                let x =
                    (self.state[i] & 0xffffffff80000000) | (self.state[(i + 1) % 312] & 0x7fffffff);
                self.state[i] = self.state[(i + 156) % 312]
                    ^ (x >> 1)
                    ^ if x & 1 == 0 { 0 } else { 0xb5026f5aa96619e9 };
            }
            self.at = 0;
        }
        let mut x = self.state[self.at];
        self.at += 1;
        x ^= (x >> 29) & 0x5555555555555555;
        x ^= (x << 17) & 0x71d67fffeda60000;
        x ^= (x << 37) & 0xfff7eee000000000;
        x ^ (x >> 43)
    }

    pub fn next(&mut self) -> u64 {
        if self.database_draw {
            self.draw();
        }
        self.draw()
    }
}

/// GenerateKeyFromInt writes the numeric prefix big endian and pads with ASCII '0'.
pub fn key(n: u64) -> [u8; 16] {
    let mut key = [b'0'; 16];
    key[..8].copy_from_slice(&n.to_be_bytes());
    key
}
