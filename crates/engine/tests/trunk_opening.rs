//! Paced opening on a real file: held input reads and writes leave maintenance resumable,
//! and a required read failure can be repaired before checkpointing the exact newest values.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_block::issuer::Issuer;
use mantle_engine::Error;
use mantle_engine::branch::filter::Keys;
use mantle_engine::branch::{Branch, Builder, Op};
use mantle_engine::scan::ScanMerge;
use mantle_engine::store::{Config, Store};
use mantle_engine::trunk::{Trunk, TrunkConfig};

const CONFIG: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 4096,
};

type Entries = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

struct Gate {
    owner: std::thread::ThreadId,
    deny_owner_reads: AtomicBool,
    owner_reads: AtomicUsize,
    hold_reads: AtomicBool,
    reads_entered: AtomicUsize,
    reject_reads: AtomicBool,
    hold_writes: AtomicBool,
    writes_entered: AtomicUsize,
    held_write_offset: AtomicU64,
}

impl Gate {
    fn new() -> Self {
        Self {
            owner: std::thread::current().id(),
            deny_owner_reads: AtomicBool::new(false),
            owner_reads: AtomicUsize::new(0),
            hold_reads: AtomicBool::new(false),
            reads_entered: AtomicUsize::new(0),
            reject_reads: AtomicBool::new(false),
            hold_writes: AtomicBool::new(false),
            writes_entered: AtomicUsize::new(0),
            held_write_offset: AtomicU64::new(u64::MAX),
        }
    }
}

struct Gated {
    file: DeviceFile,
    gate: Arc<Gate>,
}

fn refusal(op: &'static str) -> DiskError {
    DiskError::Io {
        op,
        path: std::path::PathBuf::new(),
        source: std::io::Error::other(op),
    }
}

impl BlockFile for Gated {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        if std::thread::current().id() == self.gate.owner {
            if self.gate.deny_owner_reads.load(Ordering::SeqCst) {
                self.gate.owner_reads.fetch_add(1, Ordering::SeqCst);
                return Err(refusal("paced opening read on the owner thread"));
            }
        } else {
            self.gate.reads_entered.fetch_add(1, Ordering::SeqCst);
            while self.gate.hold_reads.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        if self.gate.reject_reads.load(Ordering::SeqCst) {
            return Err(refusal("required input read rejected by test"));
        }
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        if self.gate.hold_writes.load(Ordering::SeqCst) {
            let _ = self.gate.held_write_offset.compare_exchange(
                u64::MAX,
                offset,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            self.gate.writes_entered.fetch_add(1, Ordering::SeqCst);
            while self.gate.hold_writes.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
        }
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            gate: Arc::clone(&self.gate),
        })
    }
}

/// Gates open before the issuer is dropped, including during an assertion's unwind.
struct OpenOnDrop(Arc<Gate>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.hold_reads.store(false, Ordering::SeqCst);
        self.0.hold_writes.store(false, Ordering::SeqCst);
        self.0.reject_reads.store(false, Ordering::SeqCst);
        self.0.deny_owner_reads.store(false, Ordering::SeqCst);
    }
}

fn native() -> (tempfile::TempDir, Arc<Gate>, Store<Gated>) {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::new());
    let file = Gated {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap(),
        gate: Arc::clone(&gate),
    };
    let store = Store::create(file, CONFIG).unwrap();
    (dir, gate, store)
}

fn entries(items: &[(&[u8], Option<&[u8]>)]) -> Entries {
    items
        .iter()
        .map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec)))
        .collect()
}

fn build(store: &mut Store<Gated>, entries: &Entries) -> Branch {
    let mut builder = Builder::new(store, Keys::Exactly(entries.len() as u64)).unwrap();
    for (key, value) in entries {
        match value {
            Some(v) => builder.add(store, key, Op::Put, v).unwrap(),
            None => builder.add(store, key, Op::Delete, b"").unwrap(),
        }
    }
    builder.finish(store).unwrap()
}

fn check_refs(store: &Store<Gated>, trunk: &Trunk) {
    let mut expected = BTreeSet::from([0]);
    let mut branches = BTreeSet::new();
    for branch in trunk.branches() {
        for extent in branch.extents {
            assert!(branches.insert(extent), "branch extent named twice");
        }
    }
    expected.extend(branches);
    expected.extend(store.map_extents());
    expected.extend(trunk.image_extents());
    expected.extend(trunk.view_extents());
    let held: BTreeSet<_> = store
        .refs()
        .iter()
        .enumerate()
        .filter(|(_, count)| **count > 0)
        .map(|(extent, _)| extent as u64)
        .collect();
    assert_eq!(held, expected);
}

fn scan(
    store: &mut Store<Gated>,
    trunk: &Trunk,
    from: &[u8],
    end: Option<&[u8]>,
    keys: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut got = Vec::new();
    let mut from = from.to_vec();
    let mut sources = Vec::new();
    let mut boundary = Vec::new();
    let mut merge = ScanMerge::new();
    // Every segment advances past its lower fence; no scan visits more leaves than exist.
    for _ in 0..=trunk.shape().unwrap().2 {
        if end.is_some_and(|e| from.as_slice() >= e) {
            return got;
        }
        let bounded = trunk
            .segment_at(&from, &mut sources, &mut boundary, &mut Vec::new())
            .unwrap();
        let hi = match (bounded, end) {
            (true, Some(e)) if e < boundary.as_slice() => Some(e),
            (true, _) => Some(boundary.as_slice()),
            (false, e) => e,
        };
        merge
            .open(store, &sources, &from, hi, end.is_some(), false)
            .unwrap();
        while let Some((key, op, value)) = merge.entry() {
            if op == Op::Put {
                got.push((key.to_vec(), value.to_vec()));
                assert!(got.len() <= keys, "a scan repeated a key");
            }
            merge.next(store).unwrap();
        }
        merge.close(store);
        if !bounded {
            return got;
        }
        assert!(boundary > from, "a segment must advance");
        from.clone_from(&boundary);
    }
    panic!("a scan did not end within the trunk's leaf count");
}

fn check(store: &mut Store<Gated>, trunk: &mut Trunk, oracle: &Entries) {
    let mut value = Vec::new();
    let mut starts = vec![Vec::new(), b"absent-between".to_vec(), b"zzzz".to_vec()];
    for (key, expected) in oracle {
        let op = trunk.get(store, key, &mut value).unwrap();
        match expected {
            Some(v) => {
                assert_eq!(op, Some(Op::Put), "{key:?}");
                assert_eq!(&value, v, "{key:?}");
            }
            None => assert_ne!(op, Some(Op::Put), "deleted {key:?}"),
        }
        starts.push(key.clone());
        let mut past = key.clone();
        past.push(0);
        starts.push(past);
    }
    assert_eq!(trunk.get(store, b"not-present", &mut value).unwrap(), None);
    for from in &starts {
        for end in [None, Some(b"m0".as_slice()), Some(b"z0".as_slice())] {
            let want: Vec<_> = oracle
                .iter()
                .filter(|(k, _)| k.as_slice() >= from.as_slice())
                .filter(|(k, _)| end.is_none_or(|e| k.as_slice() < e))
                .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
                .collect();
            assert_eq!(scan(store, trunk, from, end, oracle.len()), want);
        }
    }
    check_refs(store, trunk);
}

fn checkpoint_and_reopen(mut store: Store<Gated>, mut trunk: Trunk, oracle: &Entries) {
    trunk.drain(&mut store).unwrap();
    check(&mut store, &mut trunk, oracle);
    let root = trunk.save(&mut store).unwrap();
    let applied = oracle.len() as u64;
    store.checkpoint(Some(root), applied).unwrap();
    let (file, drained) = store.into_file();
    drained.unwrap();
    let (mut store, recovered) = Store::open(file, CONFIG).unwrap();
    assert_eq!(recovered.applied, applied);
    assert_eq!(recovered.root, Some(root));
    let mut trunk = Trunk::load(&mut store, recovered.root.unwrap()).unwrap();
    check(&mut store, &mut trunk, oracle);
}

#[test]
fn paced_opening_returns_while_its_first_input_read_is_held() {
    let (dir, gate, mut store) = native();
    let older = entries(&[(b"a", Some(b"old-a")), (b"m", Some(b"old-m"))]);
    let newer = entries(&[(b"a", None), (b"m", Some(b"new-m")), (b"z", Some(b"new-z"))]);
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 2,
        leaf_entries: 2,
    })
    .unwrap();
    trunk.add(build(&mut store, &older));
    trunk.add(build(&mut store, &newer));
    store.drain().unwrap();
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    store.set_write_budget(store.run_bytes());
    let _open = OpenOnDrop(Arc::clone(&gate));
    gate.hold_reads.store(true, Ordering::SeqCst);
    gate.deny_owner_reads.store(true, Ordering::SeqCst);
    assert_eq!(trunk.step_paced(&mut store, u64::MAX).unwrap(), 0);
    while gate.reads_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    assert!(!trunk.is_idle());
    assert_eq!(trunk.step_paced(&mut store, 1).unwrap(), 0);
    // The sole issuer slot still holds the input read. A write can enter its bounded memory
    // queue and its sealed payload can be read without waiting for that device operation.
    let unrelated = store.allocate_extent().unwrap();
    let address = store.address(unrelated, 0).unwrap();
    let mut run = store.run().unwrap();
    let payload = b"unrelated write accepted while the input read is held";
    store.queue_page(&mut run, address, payload).unwrap();
    store.write_run(&mut run).unwrap();
    let mut got = Vec::new();
    store.read_page(address, &mut got).unwrap();
    assert_eq!(got, payload);
    assert_eq!(gate.owner_reads.load(Ordering::SeqCst), 0);
    gate.hold_reads.store(false, Ordering::SeqCst);
    gate.deny_owner_reads.store(false, Ordering::SeqCst);
    store.drain().unwrap();
    store.give_run(run);
    store.release(unrelated).unwrap();
    let mut oracle = older;
    oracle.extend(newer);
    checkpoint_and_reopen(store, trunk, &oracle);
}

#[test]
fn a_failed_required_opening_read_can_be_repaired_and_retried() {
    let (dir, gate, mut store) = native();
    let older = entries(&[(b"a", Some(b"old")), (b"m", Some(b"deleted"))]);
    let newer = entries(&[(b"a", Some(b"new")), (b"m", None), (b"z", Some(b"last"))]);
    let old = build(&mut store, &older);
    let new = build(&mut store, &newer);
    store.drain().unwrap();
    // The first, newest input opens from memory. The second requires a device read.
    store.set_cache(CONFIG.extent_pages as usize);
    let first = new
        .page_address(&store, u64::from(new.leaf_of(b"").unwrap()))
        .unwrap();
    store.read_page(first, &mut Vec::new()).unwrap();
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 2,
        leaf_entries: 2,
    })
    .unwrap();
    trunk.add(old);
    trunk.add(new);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let _open = OpenOnDrop(Arc::clone(&gate));
    gate.reject_reads.store(true, Ordering::SeqCst);
    gate.deny_owner_reads.store(true, Ordering::SeqCst);
    let first = trunk.step_paced(&mut store, u64::MAX);
    let error = match first {
        Err(error) => error,
        Ok(_) => {
            // Await the submitted read's actual answer, rather than poll a guessed number of times.
            store.drain().unwrap();
            trunk.step_paced(&mut store, u64::MAX).unwrap_err()
        }
    };
    assert!(
        matches!(
            error,
            Error::Io {
                op: "read a store page span ahead",
                ..
            }
        ),
        "{error}"
    );
    assert!(
        !trunk.is_idle(),
        "failed opening must retain its maintenance"
    );
    assert!(trunk.save(&mut store).is_err());
    assert_eq!(gate.owner_reads.load(Ordering::SeqCst), 0);
    gate.reject_reads.store(false, Ordering::SeqCst);
    gate.deny_owner_reads.store(false, Ordering::SeqCst);
    let mut oracle = older;
    oracle.extend(newer);
    checkpoint_and_reopen(store, trunk, &oracle);
}

#[test]
fn a_pivot_lower_bound_in_a_leaf_gap_keeps_the_next_leaf_and_end_fence() {
    let (dir, gate, mut store) = native();
    let baseline = entries(&[
        (b"a0", Some(b"old-a")),
        (b"m0", Some(b"middle")),
        (b"z0", Some(b"old-z")),
    ]);
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 2,
        leaf_entries: 2,
    })
    .unwrap();
    let initial = build(&mut store, &baseline);
    trunk.incorporate(&mut store, initial).unwrap();
    let incoming: Entries = [b"a0", b"a1", b"a2", b"a3", b"z0", b"z1"]
        .into_iter()
        .map(|k| (k.to_vec(), Some(vec![k[1]; 2500])))
        .collect();
    let branch = build(&mut store, &incoming);
    store.drain().unwrap();
    // These public seeks establish that m0 routes to a leaf ending before it, and must cross
    // to another leaf. The crossed leaf lies beyond the middle pivot's exclusive z0 fence.
    let before = u64::from(branch.leaf_of(b"m0").unwrap());
    let cursor = branch.run_at(&mut store, b"m0").unwrap();
    assert_eq!(cursor.key(), b"z0");
    assert_ne!(cursor.position().0, before);
    cursor.give_back(&mut store);
    trunk.add(branch);
    store.set_cache(0);
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    let _open = OpenOnDrop(Arc::clone(&gate));
    gate.deny_owner_reads.store(true, Ordering::SeqCst);
    assert_eq!(trunk.step_paced(&mut store, 1).unwrap(), 0);
    store.drain().unwrap();
    // A finite schedule through the incoming keys lets each actual read answer between
    // paced slices, including the middle pivot's gap, without allowing an owner-thread read.
    for _ in incoming.keys() {
        trunk.step_paced(&mut store, u64::MAX).unwrap();
        store.drain().unwrap();
    }
    assert_eq!(gate.owner_reads.load(Ordering::SeqCst), 0);
    gate.deny_owner_reads.store(false, Ordering::SeqCst);
    let mut oracle = baseline;
    oracle.extend(incoming);
    checkpoint_and_reopen(store, trunk, &oracle);
}

#[test]
fn a_first_input_write_above_the_answered_end_does_not_block_paced_opening() {
    let (dir, gate, mut store) = native();
    let landed = std::fs::metadata(dir.path().join("store")).unwrap().len();
    let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
    store.attach(&issuer, 1).unwrap();
    // Every legally allocated extent can remain queued while this fixture holds the device.
    store.set_write_budget(usize::try_from(CONFIG.max_extents).unwrap() * store.run_bytes());
    store.set_cache(1);
    let _open = OpenOnDrop(Arc::clone(&gate));
    gate.hold_writes.store(true, Ordering::SeqCst);
    let oracle = entries(&[
        (b"a", Some(b"first")),
        (b"m", Some(b"middle")),
        (b"z", Some(b"last")),
    ]);
    let branch = build(&mut store, &oracle);
    let first = branch
        .page_address(&store, u64::from(branch.leaf_of(b"").unwrap()))
        .unwrap();
    assert!(first * CONFIG.page_size as u64 >= landed);
    while gate.writes_entered.load(Ordering::SeqCst) == 0 {
        std::thread::yield_now();
    }
    assert_eq!(
        gate.held_write_offset.load(Ordering::SeqCst),
        first * CONFIG.page_size as u64
    );
    // Clear even a cache entry refreshed by the builder; the first run is held at the device.
    store.set_cache(0);
    gate.deny_owner_reads.store(true, Ordering::SeqCst);
    let mut probe = store.span_sequential().unwrap();
    let ready = store.ready(&mut probe, first).unwrap();
    store.give_span(probe);
    // Refuse the old past-end policy before it could block in a paced cursor's settle.
    assert!(!ready);
    let mut trunk = Trunk::new(TrunkConfig {
        fanout: 2,
        leaf_entries: 2,
    })
    .unwrap();
    trunk.add(branch);
    assert_eq!(trunk.step_paced(&mut store, u64::MAX).unwrap(), 0);
    assert!(!trunk.is_idle());
    assert_eq!(gate.owner_reads.load(Ordering::SeqCst), 0);
    gate.hold_writes.store(false, Ordering::SeqCst);
    gate.deny_owner_reads.store(false, Ordering::SeqCst);
    checkpoint_and_reopen(store, trunk, &oracle);
}
