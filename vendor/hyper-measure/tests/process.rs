//! A process count sees what every thread asked of the allocator, the thread that began it and
//! any other. One test in its own binary, so that no other test's allocations share the count.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_macros)]

use hyper_measure::alloc;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

#[test]
fn another_threads_allocations_are_in_the_process_count_and_not_in_this_threads() {
    assert!(alloc::installed());
    let (send, receive) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
    std::thread::scope(|s| {
        let worker = s.spawn(move || {
            let block: Vec<u8> = std::hint::black_box(Vec::with_capacity(4096));
            send.send(block).unwrap();
        });
        alloc::begin_process();
        alloc::begin();
        let mine: Vec<u8> = std::hint::black_box(Vec::with_capacity(100));
        let received = receive.recv().unwrap();
        worker.join().unwrap();
        drop(mine);
        drop(received);
        let thread = alloc::end();
        let process = alloc::end_process();
        // This thread asked for its 100 bytes and what receiving and joining take, never the
        // worker's 4096; the process count has both.
        assert!(thread.bytes >= 100 && thread.bytes < 4096, "{thread:?}");
        assert!(
            process.bytes >= thread.bytes + 4096,
            "{process:?} {thread:?}"
        );
        assert!(
            process.allocations > thread.allocations,
            "{process:?} {thread:?}"
        );
        assert!(process.frees >= thread.frees, "{process:?} {thread:?}");
    });
}
