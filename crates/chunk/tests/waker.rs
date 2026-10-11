//! A caller's waker is the caller's code, run on the volume's writer thread once its answer is
//! sent: one that panics leaves the writer answering every later request (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

mod common;

use std::sync::Arc;
use std::task::{Wake, Waker};

use common::{SIZE, config, data, key, sim};
use hyper_block::issuer::{self, Issuer};
use mantle_chunk::Volume;

/// A waker that panics when woken. `std` makes a safe `Waker` only from an `Arc<impl Wake>`.
struct Panics;

impl Wake for Panics {
    fn wake(self: Arc<Self>) {
        panic!("a caller's waker panicked");
    }
}

#[test]
fn a_panicking_waker_leaves_the_writer_answering() {
    // A simulated device reports no queue and calibration has measured none: mantle runs it one
    // transfer at a time (`issuer::depth`).
    let issuer = Issuer::start(
        std::path::Path::new("simulated device"),
        issuer::depth(None, None),
    )
    .unwrap();
    let v = Volume::format(&issuer, sim(5), SIZE, config()).unwrap();
    let answer = v
        .put_waking(key(1), &data(1, 3000), Waker::from(Arc::new(Panics)))
        .unwrap();
    answer.wait().unwrap();
    v.put(key(2), &data(2, 3000)).unwrap();
    assert_eq!(v.read(&key(1), 0, 3000).unwrap(), data(1, 3000));
    assert_eq!(v.read(&key(2), 0, 3000).unwrap(), data(2, 3000));
}
