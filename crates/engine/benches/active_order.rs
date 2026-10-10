//! Deterministic public HashMem work reproduction, not a timing benchmark.
//! The same puts pay their normal chunk work, then either fully tidy idle order or pay only
//! already-admitted work. Final full tidying and every lookup/walk are checked identically.
//! `cargo bench -p mantle-engine --bench active_order -- [N ...] [--chunk=64]`.
#![allow(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

use std::collections::BTreeMap;
use std::io;

use mantle_engine::Error;
use mantle_engine::branch::Op;
use mantle_engine::error::Malformed;
use mantle_engine::memtable::hashed::HashMem;

type BenchError = Box<dyn std::error::Error>;

#[derive(Clone, Copy)]
enum Idle {
    Tidy,
    Pay,
}

impl Idle {
    fn name(self) -> &'static str {
        match self {
            Self::Tidy => "complete-tidy",
            Self::Pay => "admitted-pay",
        }
    }
}

#[derive(Default)]
struct Work {
    put: u64,
    idle: u64,
    finish: u64,
}

fn add(total: &mut u64, work: usize) -> Result<(), BenchError> {
    *total = total
        .checked_add(u64::try_from(work)?)
        .ok_or_else(|| io::Error::other("returned work count overflow"))?;
    Ok(())
}

fn key(n: u64) -> [u8; 16] {
    let mut key = [b'0'; 16];
    key[..8].copy_from_slice(&n.to_be_bytes());
    key
}

fn mismatch() -> Error {
    Error::Corruption {
        what: "the reproduction's public output oracle",
        why: Malformed::CountMismatch,
    }
}

fn tidy(mem: &mut HashMem, work: &mut u64) -> Result<(), BenchError> {
    // These inputs are unique: one tail sort and at most one merge per input entry.
    // Refuse an unexpected lack of progress rather than spin without an input-derived bound.
    for _ in 0..=mem.len() {
        if !mem.untidy() {
            return Ok(());
        }
        add(work, mem.tidy(usize::MAX))?;
    }
    if mem.untidy() {
        Err(mismatch().into())
    } else {
        Ok(())
    }
}

fn run(n: usize, chunk: usize, idle: Idle) -> Result<Work, BenchError> {
    // HashMem::insert's documented conservative record head bound is 9 bytes, plus this
    // workload's 16-byte key and 8-byte value. The arena fits the declared inputs without rotation.
    let limit = n
        .checked_mul(9 + key(0).len() + size_of::<u64>())
        .ok_or_else(|| io::Error::other("reproduction arena size overflow"))?;
    let mut mem = HashMem::new(limit)?;
    let mut oracle = BTreeMap::new();
    let mut out = Vec::new();
    let mut work = Work::default();
    for i in 0..n {
        let number = u64::try_from(n - i)?;
        let key = key(number);
        let value = u64::try_from(i)?.to_le_bytes();
        mem.insert(&key, Op::Put, &value)?;
        oracle.insert(key.to_vec(), value.to_vec());
        let room = mem.room();
        add(&mut work.put, mem.order_put(1, chunk, room))?;
        match idle {
            Idle::Tidy => tidy(&mut mem, &mut work.idle)?,
            Idle::Pay => add(&mut work.idle, mem.pay(usize::MAX))?,
        }
        if mem.get(&key, &mut out)? != Some(Op::Put) || out != value {
            return Err(mismatch().into());
        }
    }
    // Deferred order is charged, not hidden in a free final seal or dropped at process exit.
    tidy(&mut mem, &mut work.finish)?;
    mem.seal();
    for (key, expected) in &oracle {
        if mem.get(key, &mut out)? != Some(Op::Put) || &out != expected {
            return Err(mismatch().into());
        }
    }
    let mut expected = oracle.iter();
    mem.walk(|key, op, value| match expected.next() {
        Some((want_key, want_value)) if key == want_key && value == want_value && op == Op::Put => {
            Ok(())
        }
        _ => Err(mismatch()),
    })?;
    if expected.next().is_some() {
        return Err(mismatch().into());
    }
    Ok(work)
}

fn main() -> Result<(), BenchError> {
    let mut sizes = Vec::new();
    // Reproduction input shape, not a production tuning change; matches the public ordering
    // fixture's chunk parameter and is printed beside every curve.
    let mut chunk = 64usize;
    for arg in std::env::args().skip(1).filter(|arg| arg != "--bench") {
        if let Some(value) = arg.strip_prefix("--chunk=") {
            chunk = value.parse()?;
        } else {
            sizes.push(arg.parse::<usize>()?);
        }
    }
    if sizes.is_empty() {
        sizes.extend([128, 256, 512, 1024]);
    }
    if chunk == 0 || sizes.contains(&0) {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "positive N and chunk required").into(),
        );
    }
    println!(
        "public returned-work reproduction; no timing/allocation claim; final full order paid"
    );
    for n in sizes {
        for idle in [Idle::Tidy, Idle::Pay] {
            let work = run(n, chunk, idle)?;
            let total = work
                .put
                .checked_add(work.idle)
                .and_then(|n| n.checked_add(work.finish))
                .ok_or_else(|| io::Error::other("total returned work overflow"))?;
            println!(
                "active_order n {n} chunk {chunk} idle {} put_work {} idle_work {} finish_work {} paid_work {total} point_and_walk_oracle exact",
                idle.name(),
                work.put,
                work.idle,
                work.finish
            );
        }
    }
    Ok(())
}
