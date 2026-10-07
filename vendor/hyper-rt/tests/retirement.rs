//! §4.3, mantle's review of hyper-rt, finding 10b: retiring a registry slot never waits for its foreign
//! readers. The last reader frees the entry; until then the slot stays claimed, so no registration
//! takes it while a reader holds its entry. A retirement from inside a reader's own call ends too.
//! Before the fix, retirement spun with `yield_now` until no reader was counted: both cases below
//! never ended.
//!
//! This fixture owns its process: it reads which slot a registration takes, so another test's
//! registrations must not share the table.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::mpsc::channel;

use hyper_rt::driver::Kick;
use hyper_rt::registry::{RegisterKick, Registration, register, with_entry};

fn registration() -> Registration {
    register(4, 2, RegisterKick::Kick(Kick::none())).unwrap().0
}

/// Do: a reader stays inside its call on another thread while the registration drops, and only then
/// is let go. Expect: the drop returns with the reader still inside, the slot stays claimed, so a new
/// registration takes another one, and the reader's end frees it, so the next takes it again. Then do:
/// drop a registration from inside a read of its own slot. Expect: the read returns, and the slot is
/// free after it.
#[test]
fn retirement_never_waits_for_a_reader() {
    let first = registration();
    let shard = first.shard();

    let (inside, entered) = channel();
    let (release, released) = channel::<()>();
    let reader = std::thread::spawn(move || {
        with_entry(shard, |_| {
            inside.send(()).unwrap();
            released.recv().unwrap();
        })
    });
    entered.recv().unwrap();
    drop(first);
    assert!(
        with_entry(shard, |_| ()).is_none(),
        "the retired entry is out of lookup"
    );
    let other = registration();
    assert_ne!(
        other.shard(),
        shard,
        "the slot stays claimed while its reader holds the entry"
    );
    release.send(()).unwrap();
    assert!(
        reader.join().unwrap().is_some(),
        "the reader read the entry"
    );
    drop(other);

    let again = registration();
    assert_eq!(again.shard(), shard, "the last reader freed the slot");

    let mut held = Some(again);
    let read = with_entry(shard, |_| drop(held.take()));
    assert!(read.is_some(), "the read ran and returned");
    assert!(with_entry(shard, |_| ()).is_none());
    assert_eq!(
        registration().shard(),
        shard,
        "the reader's own end freed the slot"
    );
}
