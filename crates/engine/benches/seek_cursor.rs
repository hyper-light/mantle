//! Focused seek CPU work on one immutable resident branch, through Branch and ScanMerge.
//! `cargo bench -p mantle-engine --bench seek_cursor -- DIR KEYS SEEKS [NEXTS]`
//! Sixteen-byte keys and 100-byte values match shard_db; even-numbered keys exercise absent
//! starts too. A 256MiB cache matches that comparison. Each case warms with the same stream,
//! then reports whole-process CPU/instructions, allocation and physical-read counts. This
//! isolates cursor/merge work; it is not a replacement for the full fill/drain comparison.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::disallowed_macros
)]

use std::path::PathBuf;
use std::time::Instant;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_measure::{alloc, usage};
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::rows::Rows;
use mantle_engine::scan::ScanMerge;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::Source;

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

fn run<'a>(
    store: &mut Store<DeviceFile>,
    branch: &'a Branch,
    keys: u64,
    seeks: u64,
    nexts: usize,
    use_merge: bool,
    buffers: (&mut Rows, &mut ScanMerge<'a>),
) -> u64 {
    let (rows, merge) = buffers;
    let mut rng = rocks_workload::Rng::new(301, 3);
    let sources = [Source::Branch(branch)];
    let mut count = 0u64;
    for _ in 0..seeks {
        let from = rocks_workload::key(rng.next() % (keys * 2));
        rows.clear();
        if use_merge {
            merge
                .open(store, &sources, &from, None, false, false)
                .unwrap();
            for _ in 0..nexts {
                let Some((key, _, value)) = merge.entry() else {
                    break;
                };
                rows.push(key, value);
                merge.next(store).unwrap();
            }
        } else {
            let mut cursor = branch.seek(store, &from).unwrap();
            for _ in 0..nexts {
                if !cursor.valid() {
                    break;
                }
                rows.push(cursor.key(), cursor.value());
                cursor.next(branch, store).unwrap();
            }
            cursor.give_back(store);
        }
        count += rows.len() as u64;
        std::hint::black_box(&*rows);
    }
    merge.close(store);
    count
}

fn main() -> Result<(), mantle_engine::Error> {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    let dir = PathBuf::from(args.first().expect("DIR"));
    let keys: u64 = args.get(1).expect("KEYS").parse().unwrap();
    let seeks: u64 = args.get(2).expect("SEEKS").parse().unwrap();
    let nexts: usize = args.get(3).map_or(10, |arg| arg.parse().unwrap());
    assert!(keys > 0 && seeks > 0 && nexts > 0);
    let path = dir.join("seek_cursor.store");
    let align = Alignment::new(4096).unwrap();
    let file = DeviceFile::open(&path, true, CachingRequest::Buffered, align).unwrap();
    let mut store = Store::create(
        file,
        Config {
            page_size: 4096,
            extent_pages: 32,
            max_extents: 1 << 24,
        },
    )
    .unwrap();
    store.set_cache((256 << 20) / store.page_size());
    let mut builder = Builder::new(&mut store, Keys::Exactly(keys)).unwrap();
    for n in 0..keys {
        builder
            .add(
                &mut store,
                &rocks_workload::key(n * 2),
                Op::Put,
                &[b'v'; 100],
            )
            .unwrap();
    }
    let branch = builder.finish(&mut store).unwrap();
    store.checkpoint(Some(branch.root), 1).unwrap();
    println!(
        "fixture keys {keys} seeks {seeks} nexts {nexts} cache_mib 256 seed 301 read_only true resident_work_diagnostic true"
    );
    for use_merge in [false, true] {
        let name = if use_merge { "merge" } else { "branch" };
        let mut rows = Rows::new();
        let mut merge = ScanMerge::new();
        let warmed_rows = run(
            &mut store,
            &branch,
            keys,
            seeks,
            nexts,
            use_merge,
            (&mut rows, &mut merge),
        );
        let before_io = store.io_stats();
        let before = usage::this().ok();
        alloc::begin_process();
        let start = Instant::now();
        let read = run(
            &mut store,
            &branch,
            keys,
            seeks,
            nexts,
            use_merge,
            (&mut rows, &mut merge),
        );
        let elapsed = start.elapsed().as_secs_f64();
        let allocations = alloc::end_process();
        let account = usage::this()
            .ok()
            .zip(before)
            .map(|(now, before)| now.since(&before));
        let after_io = store.io_stats();
        assert_eq!(read, warmed_rows);
        let per_op = |count: Option<u64>| {
            count.map_or_else(
                || "unavailable".to_string(),
                |count| format!("{:.3}", count as f64 / seeks as f64),
            )
        };
        println!(
            "{name} ops_per_s {:.0} rows {read} cpu_ns/op {} instructions/op {} allocations {} reallocations {} reads {} pages {} span_hits {}",
            seeks as f64 / elapsed,
            per_op(account.and_then(|account| account.user_ns.checked_add(account.system_ns))),
            per_op(account.and_then(|account| account.instructions)),
            allocations.allocations,
            allocations.reallocations,
            after_io.reads - before_io.reads,
            after_io.pages_read - before_io.pages_read,
            after_io.span_cache_hits - before_io.span_cache_hits
        );
    }
    let (file, landed) = match store.into_file() {
        mantle_engine::store::IntoFile::Finished { file, result } => (file, result),
        mantle_engine::store::IntoFile::Refused { error, .. } => return Err(error),
    };
    landed.unwrap();
    drop(file);
    // Retain the immutable image so the runner can compare dataset hashes after the timings.
    Ok(())
}
