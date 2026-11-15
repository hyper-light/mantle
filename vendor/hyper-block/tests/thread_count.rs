//! The thread count leaves out a thread once it is joined. Linux's `Threads:` still counted a
//! joined thread at once in 81,088 of 200,000 joins on adacachy; a bench that read it after
//! dropping what it ran then counted one thread more (mantle `tests/clients.rs`, 2026-11-11). This
//! file holds one test, so its process runs no thread but the harness's and the ones it starts.
//! Linux alone: macOS's count (`proc_pidinfo`'s `threadnum`) still held a joined thread in about
//! 152,000 of 200,000 joins, and no list it gives leaves out one exiting (`task_threads` less the
//! threads `thread_info` cannot read: 4 or 5 of 200,000).
#![allow(
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::disallowed_methods
)]

#[cfg(target_os = "linux")]
use hyper_block::threads::count;

#[cfg(target_os = "linux")]
#[test]
fn a_joined_thread_is_not_counted() {
    let before = count().unwrap();
    for _ in 0..10_000 {
        std::thread::spawn(|| ()).join().unwrap();
        assert_eq!(count().unwrap(), before);
    }
}
