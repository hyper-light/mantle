//! The store's crash recovery (docs/design/engine-structure.md §7, step E1) on hyper-block's
//! simulated device: power is cut after every write and flush a workload of checkpoints makes, the
//! sectors written since the last flush then each lost, all lost, or all kept, and the store is
//! reopened. The checkpoint recovered must be the last one acknowledged or the one in flight; every
//! page it names must read back exactly; the allocator must hold exactly the extents it names; and
//! the store must go on working from it without writing over a page it names.
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
use hyper_block::sim::{Crash, Fault, SimFile};
use mantle_engine::store::{Config, Store};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 256,
};
/// Checkpoints the workload makes.
const STEPS: u64 = 12;

/// The tree a step leaves: data extents, each with its page count and the step that wrote it,
/// and the extent of the root page naming them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Model {
    data: Vec<(u64, u32, u64)>,
    root: Option<u64>,
}

fn page_payload(step: u64, page: u32) -> Vec<u8> {
    let mut v = format!("step {step} page {page} ").into_bytes();
    v.resize(
        100 + (step as usize * 37 + page as usize * 11) % 900,
        step as u8,
    );
    v
}

fn root_payload(model: &Model) -> Vec<u8> {
    model
        .data
        .iter()
        .flat_map(|&(e, p, s)| {
            [
                e.to_le_bytes().to_vec(),
                u64::from(p).to_le_bytes().to_vec(),
                s.to_le_bytes().to_vec(),
            ]
            .concat()
        })
        .collect()
}

/// One step: a new data extent, every fourth step the oldest released, and a new root.
fn step<F: BlockFile>(
    store: &mut Store<F>,
    model: &mut Model,
    step: u64,
) -> Result<(), mantle_engine::error::Error> {
    let extent = store.allocate_extent()?;
    let pages = 1 + (step % 3) as u32;
    let mut run = store.run()?;
    for p in 0..pages {
        store.queue_page(&mut run, store.address(extent, p)?, &page_payload(step, p))?;
    }
    let mut next = model.clone();
    next.data.push((extent, pages, step));
    if step.is_multiple_of(4) {
        let (old, _, _) = next.data.remove(0);
        store.release(old)?;
    }
    let root_extent = store.allocate_extent()?;
    let root = store.address(root_extent, 0)?;
    store.queue_page(&mut run, root, &root_payload(&next))?;
    store.write_run(&mut run)?;
    if let Some(old_root) = model.root {
        store.release(old_root)?;
    }
    next.root = Some(root_extent);
    store.checkpoint(Some(root), step)?;
    *model = next;
    Ok(())
}

/// Checks the recovered store against the model of the checkpoint it names.
fn check<F: BlockFile>(store: &mut Store<F>, model: &Model, root: Option<u64>) {
    match model.root {
        None => assert_eq!(root, None),
        Some(root_extent) => {
            let root = root.expect("a root");
            assert_eq!(store.extent_of(root), root_extent);
            let mut got = Vec::new();
            store.read_page(root, &mut got).unwrap();
            assert_eq!(got, root_payload(model));
            for &(extent, pages, step) in &model.data {
                for p in 0..pages {
                    let mut got = Vec::new();
                    store
                        .read_page(store.address(extent, p).unwrap(), &mut got)
                        .unwrap();
                    assert_eq!(got, page_payload(step, p), "extent {extent} page {p}");
                }
            }
        }
    }
    // Exactly the named extents are held: the superblocks', the map's, the root's, the data's.
    let mut held: Vec<u64> = vec![0];
    held.extend(store.map_extents());
    held.extend(model.root);
    held.extend(model.data.iter().map(|&(e, _, _)| e));
    held.sort_unstable();
    let refs: Vec<u64> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(e, _)| e as u64)
        .collect();
    assert_eq!(refs, held);
    assert!(store.refs().iter().all(|&c| c <= 1));
}

fn sim(seed: u64) -> SimFile {
    let align = Alignment::new(4096).unwrap();
    SimFile::new(align, Alignment::new(512).unwrap(), seed).unwrap()
}

/// Runs the workload until it fails or ends; the models of every checkpoint acknowledged.
fn run(file: SimFile) -> (Vec<Model>, Option<Store<SimFile>>, Option<SimFile>) {
    let mut acked = Vec::new();
    let mut store = match Store::create(file, CONFIG) {
        Ok(store) => store,
        Err(_) => return (acked, None, None),
    };
    let mut model = Model::default();
    acked.push(model.clone());
    for s in 1..=STEPS {
        if step(&mut store, &mut model, s).is_err() {
            return (acked, None, Some(store.into_file().0));
        }
        acked.push(model.clone());
    }
    (acked, Some(store), None)
}

#[test]
fn every_crash_point_recovers_an_acknowledged_or_in_flight_checkpoint() {
    // The workload's writes and flushes, counted on a run without faults.
    let (acked, store, _) = run(sim(1));
    assert_eq!(acked.len() as u64, STEPS + 1);
    let stats = store.unwrap().into_file().0.stats().unwrap();
    let ops = stats.writes + stats.syncs;
    assert!(ops > STEPS * 4);
    let mut cases = 0;
    for cut in 0..ops {
        for (mode, seed) in [
            (Crash::Random, 11),
            (Crash::Random, 12),
            (Crash::Random, 13),
            (Crash::LoseAll, 0),
            (Crash::KeepAll, 0),
        ] {
            let file = sim(seed ^ cut);
            file.inject(Fault::PowerCut { ops: cut }).unwrap();
            let (acked, store, failed) = run(file);
            assert!(
                store.is_none(),
                "cut {cut}: the power cut stopped the workload"
            );
            let Some(file) = failed else {
                // Cut while creating: nothing was acknowledged, so an empty or unreadable file
                // is the honest outcome.
                assert!(acked.is_empty());
                continue;
            };
            file.crash(mode).unwrap();
            file.clear_faults().unwrap();
            let (mut store, recovered) =
                Store::open(file, CONFIG).unwrap_or_else(|e| panic!("cut {cut} {mode:?}: {e}"));
            let last = acked.len() as u64 - 1;
            assert!(
                recovered.applied == last || recovered.applied == last + 1,
                "cut {cut} {mode:?}: recovered {} with {last} acknowledged",
                recovered.applied
            );
            // The model of the recovered checkpoint: acknowledged, or the one in flight (the
            // clean run's).
            let model = if recovered.applied == last {
                acked[last as usize].clone()
            } else {
                let (all, _, _) = run(sim(1));
                all[recovered.applied as usize].clone()
            };
            check(&mut store, &model, recovered.root);
            // The store goes on from there without writing over a page the checkpoint names.
            let mut model = model;
            for s in recovered.applied + 1..=recovered.applied + 3 {
                step(&mut store, &mut model, s).unwrap();
                let root = store.address(model.root.unwrap(), 0).unwrap();
                check(&mut store, &model, Some(root));
            }
            cases += 1;
        }
    }
    assert!(cases > 0);
}

#[test]
fn a_store_reopens_at_its_last_checkpoint() {
    let (acked, store, _) = run(sim(3));
    let file = store.unwrap().into_file().0;
    let (mut store, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.applied, STEPS);
    check(&mut store, &acked[STEPS as usize], recovered.root);
}

#[test]
fn a_page_flipped_on_the_medium_is_a_typed_corruption() {
    let (acked, store, _) = run(sim(5));
    let file = store.unwrap().into_file().0;
    let model = acked[STEPS as usize].clone();
    let (extent, _, _) = model.data[0];
    let offset = extent * u64::from(CONFIG.extent_pages) * CONFIG.page_size as u64 + 100;
    file.inject(Fault::BitFlip {
        offset,
        bit: 3,
        stored: true,
    })
    .unwrap();
    let (mut store, _) = Store::open(file, CONFIG).unwrap();
    let mut out = Vec::new();
    let err = store
        .read_page(store.address(extent, 0).unwrap(), &mut out)
        .unwrap_err();
    assert!(
        matches!(err, mantle_engine::error::Error::Corruption { .. }),
        "{err}"
    );
}
