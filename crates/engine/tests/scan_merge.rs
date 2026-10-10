//! A scan merge's one-head and many-head walks through the same bounded ranges, with its
//! buffers reused across opens: branch tombstones and newest values agree with an ordered map.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/into_file.rs"]
mod file_outcome;
use file_outcome::finished_file;

use std::collections::BTreeMap;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};
use mantle_engine::Error;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::remix::View;
use mantle_engine::scan::ScanMerge;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::Source;

type Entries = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;

fn build(store: &mut Store<SimFile>, entries: &Entries) -> Branch {
    let mut builder = Builder::new(store, Keys::Exactly(entries.len() as u64)).unwrap();
    for (key, (op, value)) in entries {
        builder.add(store, key, *op, value).unwrap();
    }
    builder.finish(store).unwrap()
}

#[test]
fn reused_single_and_multiple_heads_keep_bounds_versions_and_deletions() {
    let config = Config {
        page_size: 4096,
        extent_pages: 8,
        max_extents: 4096,
    };
    for cache in [0, 8] {
        let align = Alignment::new(4096).unwrap();
        let file = SimFile::new(align, Alignment::new(512).unwrap(), 41).unwrap();
        let mut store = Store::create(file, config).unwrap();
        store.set_cache(cache);
        let older: Entries = (0..96u16)
            .map(|n| {
                (
                    format!("shared/{n:04}").into_bytes(),
                    (Op::Put, vec![3; 100]),
                )
            })
            .collect();
        let newest: Entries = older
            .iter()
            .enumerate()
            .filter(|(n, _)| n % 3 != 0)
            .map(|(n, (key, _))| {
                let entry = if n % 7 == 0 {
                    (Op::Delete, Vec::new())
                } else {
                    (Op::Put, vec![9; 100 + n])
                };
                (key.clone(), entry)
            })
            .collect();
        let branches = [build(&mut store, &newest), build(&mut store, &older)];
        let view = View::build(&mut store, &branches, b"", None).unwrap();
        let mut all = older.clone();
        all.extend(newest.clone());
        let mut merge = ScanMerge::new();
        for (sources, oracle) in [
            (vec![Source::Branch(&branches[0])], &newest),
            (
                vec![Source::Branch(&branches[0]), Source::Branch(&branches[1])],
                &all,
            ),
            (vec![Source::View(&view, &branches)], &all),
            (vec![Source::Branch(&branches[1])], &older),
        ] {
            for from in [
                b"".as_slice(),
                b"shared",
                b"shared/0000",
                b"shared/0000\0",
                b"shared/0031\0",
                b"shared/9999",
                b"\xff",
            ] {
                for end in [None, Some(b"shared/0048".as_slice())] {
                    let want: Vec<_> = oracle
                        .iter()
                        .filter(|(key, _)| {
                            key.as_slice() >= from && end.is_none_or(|end| key.as_slice() < end)
                        })
                        .map(|(key, (op, value))| (key.clone(), *op, value.clone()))
                        .collect();
                    merge
                        .open(&mut store, &sources, from, end, false, false)
                        .unwrap();
                    let mut got = Vec::new();
                    while let Some((key, op, value)) = merge.entry() {
                        assert!(got.len() <= want.len(), "a merge that does not end");
                        got.push((key.to_vec(), op, value.to_vec()));
                        merge.next(&mut store).unwrap();
                    }
                    assert_eq!(got, want, "from {from:?}, end {end:?}");
                    merge.next(&mut store).unwrap();
                    assert!(merge.entry().is_none());
                }
            }
        }
        merge.close(&mut store);
    }
}

#[test]
fn a_failed_single_head_step_can_reopen_over_another_run() {
    let config = Config {
        page_size: 4096,
        extent_pages: 8,
        max_extents: 4096,
    };
    let align = Alignment::new(4096).unwrap();
    let file = SimFile::new(align, Alignment::new(512).unwrap(), 43).unwrap();
    let mut store = Store::create(file, config).unwrap();
    let entries: Entries = (0..200u16)
        .map(|n| (format!("a/{n:04}").into_bytes(), (Op::Put, vec![4; 100])))
        .collect();
    let good: Entries = (0..80u16)
        .map(|n| (format!("z/{n:04}").into_bytes(), (Op::Put, vec![8; 100])))
        .collect();
    let bad_branch = build(&mut store, &entries);
    let good_branch = build(&mut store, &good);
    let page = u64::from(bad_branch.leaf_of(b"a/0100").unwrap());
    let address = bad_branch.page_address(&store, page).unwrap();
    store.checkpoint(None, 1).unwrap();
    let (file, landed) = finished_file(store.into_file());
    landed.unwrap();
    file.inject(Fault::ReadError {
        offset: address * config.page_size as u64,
        len: config.page_size as u64,
    })
    .unwrap();
    let (mut store, _) = Store::open(file, config).unwrap();
    let mut merge = ScanMerge::new();
    merge
        .open(
            &mut store,
            &[Source::Branch(&bad_branch)],
            b"",
            None,
            false,
            false,
        )
        .unwrap();
    let mut got = Vec::new();
    let mut failed = false;
    for _ in 0..=entries.len() {
        let (key, op, value) = merge.entry().unwrap();
        got.push((key.to_vec(), op, value.to_vec()));
        match merge.next(&mut store) {
            Ok(()) => {}
            Err(Error::Io { .. }) => {
                failed = true;
                break;
            }
            Err(error) => panic!("unexpected refusal: {error:?}"),
        }
    }
    assert!(failed);
    let want: Vec<_> = entries
        .iter()
        .take(got.len())
        .map(|(key, (op, value))| (key.clone(), *op, value.clone()))
        .collect();
    assert_eq!(got, want);
    merge
        .open(
            &mut store,
            &[Source::Branch(&good_branch)],
            b"",
            None,
            false,
            false,
        )
        .unwrap();
    got.clear();
    while let Some((key, op, value)) = merge.entry() {
        assert!(got.len() <= good.len(), "a merge that does not end");
        got.push((key.to_vec(), op, value.to_vec()));
        merge.next(&mut store).unwrap();
    }
    let want: Vec<_> = good
        .iter()
        .map(|(key, (op, value))| (key.clone(), *op, value.clone()))
        .collect();
    assert_eq!(got, want);
    merge.close(&mut store);
    assert!(matches!(
        bad_branch.seek(&mut store, b"a/0100"),
        Err(Error::Io { .. })
    ));
}
