//! A span read's wait prices the kernel's page cache only where the file can tell which pages the
//! OS holds (docs/research/65 §8): what a read the OS did not hold waited for the device. A file
//! that cannot tell, as the simulated device cannot, counts no wait whatever it reads, so a budget
//! on it leaves the kernel no share.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 32,
    max_extents: 4096,
};

#[test]
fn a_file_that_cannot_tell_what_the_os_holds_counts_no_read_wait() {
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 5).unwrap();
    assert!(
        !file.reads_resident(),
        "the simulated device tells residency"
    );
    let mut store = Store::create(file, CONFIG).unwrap();
    let mut run = store.run().unwrap();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let extent = store.allocate_extent().unwrap();
        for n in 0..CONFIG.extent_pages {
            let a = store.address(extent, n).unwrap();
            store.queue_page(&mut run, a, &[7u8; 100]).unwrap();
            addresses.push(a);
        }
    }
    store.write_run(&mut run).unwrap();
    store.drain().unwrap();
    let before = store.io_stats();
    let mut span = store.span_sequential().unwrap();
    let mut out = Vec::new();
    for &a in &addresses {
        out.clear();
        store.read_page_ahead(&mut span, a, &mut out).unwrap();
        assert_eq!(&out[..], &[7u8; 100][..], "page {a}");
    }
    store.give_span(span);
    let after = store.io_stats();
    assert!(after.reads > before.reads, "no read went to the file");
    assert_eq!(after.read_wait_ns, 0);
    assert_eq!(store.read_wait_ns(), 0);
}
