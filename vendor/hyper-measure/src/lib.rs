//! What a run costs, counted where it happens: every allocation, reallocation
//! and free the process makes ([`alloc`]), and the page faults the operating
//! system charged it ([`faults`]). The benchmarks and the end-to-end tests of
//! this repository report both per operation (`CLAUDE.md` §1a,
//! `docs/benchmarks.md`).
//!
//! It is measurement only: no shipped crate depends on it. A benchmark or a
//! test installs [`alloc::Counting`] as its global allocator and switches the
//! counting on around what it measures.
//!
//! A test or a benchmark that hands a call a `Waker` takes a counting one from
//! [`wake`].
//!
//! The `unsafe` this crate needs is in three files that
//! `scripts/check-contracts.py` lists: `src/alloc.rs` (the allocator forwards
//! to the system's), `src/faults.rs` (the OS calls that read the faults) and
//! `src/wake.rs` (a waker built over a leaked slot).

pub mod alloc;
pub mod faults;
pub mod stats;
pub mod wake;
