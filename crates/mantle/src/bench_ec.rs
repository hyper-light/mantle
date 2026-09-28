//! `mantle bench ec`: erasure-coding throughput on one core, for each code and chunk size.
//!
//! For each code, a block of `data` chunks is encoded, and rebuilt from its chunks after
//! losing one data chunk and after losing as many data chunks as the code tolerates, the
//! most expensive rebuild. Throughput counts the block's bytes, so it compares directly with
//! the rate at which blocks are written and read. The codes are those research note 04 §0
//! weighs for mantle's durability profiles; the sizes run from a small object's chunk to an
//! 8 MiB shard of a large block.

use std::io::Write;
use std::time::{Duration, Instant};

use mantle_disk::measure::SplitMix64;
use mantle_ec::{Code, EcError};

use crate::display;

#[derive(Debug)]
pub enum Error {
    Output(std::io::Error),
    Ec(EcError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(e) => write!(f, "writing output: {e}"),
            Self::Ec(e) => write!(f, "erasure coding: {e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Output(e)
    }
}

impl From<EcError> for Error {
    fn from(e: EcError) -> Self {
        Self::Ec(e)
    }
}

/// The codes docs/research/04 §0 considers: RS(6,3) for 9–11 failure domains, RS(8,4) for 12
/// and more, RS(10,4) for capacity, RS(9,6) for durability.
pub const CODES: [(usize, usize); 4] = [(6, 3), (8, 4), (10, 4), (9, 6)];

pub fn ec(
    out: &mut impl Write,
    codes: &[(usize, usize)],
    sizes: &[usize],
    step: Duration,
) -> Result<(), Error> {
    writeln!(
        out,
        "  {:<10} {:>8} {:>12} {:>16} {:>18}",
        "code", "chunk", "encode", "rebuild 1 lost", "rebuild most lost"
    )?;
    for &(data, parity) in codes {
        let code = Code::new(data, parity)?;
        for &size in sizes {
            let len = size.saturating_mul(data);
            let mut block = vec![0u8; len];
            SplitMix64::new(u64::try_from(len).unwrap_or(0)).fill(&mut block);
            let encode = rate(len, step, || code.encode(&block).map(|_| ()))?;
            let chunks = code.encode(&block)?;
            let without = |lost: usize| -> Vec<(usize, &[u8])> {
                chunks
                    .iter()
                    .enumerate()
                    .skip(lost)
                    .map(|(i, c)| (i, c.as_slice()))
                    .collect()
            };
            let one = without(1);
            let most = without(parity);
            let rebuild_one = rate(len, step, || code.decode(&one, len).map(|_| ()))?;
            let rebuild_most = rate(len, step, || code.decode(&most, len).map(|_| ()))?;
            writeln!(
                out,
                "  {:<10} {:>8} {:>12} {:>16} {:>18}",
                format!("RS({data},{parity})"),
                display::size(size),
                display::rate(encode),
                display::rate(rebuild_one),
                display::rate(rebuild_most),
            )?;
            out.flush()?;
        }
    }
    Ok(())
}

/// Block bytes per second over repeated runs of `f` for `step`.
fn rate(
    bytes: usize,
    step: Duration,
    mut f: impl FnMut() -> Result<(), EcError>,
) -> Result<f64, Error> {
    let started = Instant::now();
    let mut runs = 0u64;
    while runs == 0 || started.elapsed() < step {
        f()?;
        runs = runs.saturating_add(1);
    }
    let secs = started.elapsed().as_secs_f64();
    // u64 -> f64 rounds above 2^53, far beyond any count a bounded run produces.
    Ok(runs as f64 * bytes as f64 / secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_and_size_is_reported() {
        let mut out = Vec::new();
        ec(&mut out, &CODES, &[4096], Duration::from_millis(1)).unwrap();
        let text = String::from_utf8(out).unwrap();
        for (data, parity) in CODES {
            assert!(text.contains(&format!("RS({data},{parity})")), "{text}");
        }
    }
}
