//! The corpus the codecs' oracles compress (`tests/golden/{snappy,lz4}/*_oracle.cc`), made alike
//! here: text with repeats at many distances, random bytes, one byte repeated, a short alphabet,
//! and the empty input, at lengths around each format's thresholds and the 64 KiB fragment.

/// A SplitMix64 stream, as the oracle's.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The corpus, in `snappy_oracle.cc`'s order and from its stream.
pub fn corpus() -> Vec<(String, Vec<u8>)> {
    let mut rng = Rng(7);
    let words: Vec<&[u8]> = vec![
        b"the ",
        b"quick ",
        b"brown ",
        b"fox ",
        b"jumps ",
        b"over ",
        b"lazy ",
        b"dog ",
        b"mantle ",
        b"engine ",
        b"range ",
        b"replica ",
        b"\n",
        b"{\"key\": ",
        b"}, ",
    ];
    let mut out = vec![("empty".to_owned(), Vec::new())];
    for len in [
        1usize, 2, 3, 7, 16, 60, 61, 100, 1000, 4096, 65535, 65536, 65537, 131_073, 200_000,
    ] {
        let mut text = Vec::new();
        while text.len() < len {
            text.extend_from_slice(words[rng.below(words.len() as u64) as usize]);
        }
        text.truncate(len);
        let random: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
        let same = vec![0x5Au8; len];
        let acgt: Vec<u8> = (0..len).map(|_| b"ACGT"[rng.below(4) as usize]).collect();
        out.push((format!("text-{len}"), text));
        out.push((format!("random-{len}"), random));
        out.push((format!("same-{len}"), same));
        out.push((format!("acgt-{len}"), acgt));
    }
    out
}

pub fn fnv(data: &[u8]) -> u64 {
    data.iter().fold(0xCBF2_9CE4_8422_2325u64, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3)
    })
}
