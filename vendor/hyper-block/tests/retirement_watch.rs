//! Public physical watches retain their cold registration and terminal receipt.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_block::buf::{AlignedBuf, Alignment};
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use hyper_rt::{Runtime, RuntimeConfig};
use std::sync::mpsc::sync_channel;

/// The filesystem page shape used by the existing native issuer allocation fixture.
const PAGE: usize = 4096;

fn runtime() -> Runtime {
    // One admitted context-probe task; timing fields match native retirement fixtures.
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 0,
        interests_per_shard: hyper_rt::runtime::interests_for(1),
        ring_entries: 1,
        batch: 1,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        page_bytes: PAGE,
        spin_ns: 0,
        pin: false,
        cores: Vec::new(),
        wake_tracking: None,
    })
    .unwrap()
}

fn page(fill: u8) -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(PAGE, Alignment::new(PAGE).unwrap()).unwrap();
    buf.extend_from_slice(&[fill; PAGE]).unwrap();
    buf
}

fn verify(file: &DeviceFile, fill: u8) {
    let mut buf = page(0);
    file.read_exact_at(buf.as_mut_slice(), 0).unwrap();
    assert!(buf.as_slice().iter().all(|byte| *byte == fill));
}

#[test]
fn entered_wait_refusal_preserves_an_already_ready_physical_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    let mut attached = issuer.attach(&file).unwrap();
    let mut watch = attached.prepare_retirement_watch().unwrap();
    drop(attached.write(vec![(page(0x5a), 0)], true).unwrap());
    attached.retire_blocking().unwrap();
    // The detach has physically completed before the invalid context can poll this watch.
    let rt = runtime();
    let (back, returned) = sync_channel(1);
    rt.spawn_on(rt.shard_ids()[0], async move {
        let refused = watch.wait_blocking().is_err();
        let untouched = !watch.is_retired();
        back.try_send((watch, refused, untouched)).unwrap();
    })
    .unwrap();
    let (mut watch, refused, untouched) = returned.recv().unwrap();
    watch.wait_blocking().unwrap();
    let retired = watch.is_retired();
    rt.shutdown().unwrap();
    drop(issuer);
    verify(&file, 0x5a);
    assert!(refused && untouched && retired);
}

#[test]
fn entered_registration_refusal_keeps_the_one_cold_registration_usable() {
    let dir = tempfile::tempdir().unwrap();
    let file = DeviceFile::open(
        &dir.path().join("store"),
        true,
        CachingRequest::Buffered,
        Alignment::new(PAGE).unwrap(),
    )
    .unwrap();
    let issuer = Issuer::start(dir.path(), 1).unwrap();
    let mut attached = issuer.attach(&file).unwrap();
    let rt = runtime();
    let (back, returned) = sync_channel(1);
    rt.spawn_on(rt.shard_ids()[0], async move {
        let refused = attached.prepare_retirement_watch().is_err();
        back.try_send((attached, refused)).unwrap();
    })
    .unwrap();
    let (mut attached, refused) = returned.recv().unwrap();
    let mut watch = attached.prepare_retirement_watch().unwrap();
    let duplicate_refused = attached.prepare_retirement_watch().is_err();
    drop(attached.write(vec![(page(0xa5), 0)], true).unwrap());
    attached.retire_blocking().unwrap();
    watch.wait_blocking().unwrap();
    let retired = watch.is_retired();
    rt.shutdown().unwrap();
    drop(issuer);
    verify(&file, 0xa5);
    assert!(refused && duplicate_refused && retired);
}
