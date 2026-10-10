//! Whole maplet/view outputs through held native reads, cancellation, failure and replacement.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::branch::filter::{self, Keys};
    use crate::branch::{Builder, Op};
    use crate::scan::ScanMerge;
    use crate::store::Config;
    use hyper_block::DiskError;
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use hyper_block::issuer::Issuer;
    use hyper_rt::combine::{Either, race2};
    use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    // Config::check permits any buffered page >=4 KiB aligned to this file. The extra byte
    // makes the payload cross a format u32, independently of metadata prefix alignment.
    const CONFIG: Config = Config {
        page_size: 4096 + 1,
        extent_pages: 4,
        max_extents: 4096,
    };
    // The existing public branch_test hash-stream oracle uses this finite dataset.
    const ROWS: usize = 20_000;

    #[derive(Debug)]
    enum Notice {
        Entered,
        Deadline,
    }

    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
        armed: AtomicBool,
        // Let the first metadata page complete; hold the next required page.
        skip: AtomicUsize,
        failed: AtomicBool,
        notice: hyper_rt::sync::Sender<Notice>,
        first_read: AtomicU64,
        old_page_written: AtomicBool,
    }
    impl Gate {
        fn release(&self) {
            *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.changed.notify_all();
        }
    }
    struct OpenOnDrop(&'static Gate);
    impl Drop for OpenOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct Deadline {
        expired: Arc<AtomicBool>,
        stopped: Arc<(Mutex<bool>, Condvar)>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Deadline {
        fn new(gate: &'static Gate) -> Self {
            let stopped = Arc::new((Mutex::new(false), Condvar::new()));
            let stop = Arc::clone(&stopped);
            let expired = Arc::new(AtomicBool::new(false));
            let timed_out = Arc::clone(&expired);
            let thread = std::thread::spawn(move || {
                let (lock, changed) = &*stop;
                let done = lock.lock().unwrap_or_else(|e| e.into_inner());
                // A failure deadline, never a gate-release ordering or success condition.
                let (done, result) = changed
                    .wait_timeout_while(done, Duration::from_secs(5), |done| !*done)
                    .unwrap_or_else(|e| e.into_inner());
                if result.timed_out() && !*done {
                    timed_out.store(true, Ordering::SeqCst);
                    let _ = gate.notice.try_send(Notice::Deadline);
                    gate.release();
                }
            });
            Self {
                expired,
                stopped,
                thread: Some(thread),
            }
        }
    }
    impl Drop for Deadline {
        fn drop(&mut self) {
            let (lock, changed) = &*self.stopped;
            *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
            changed.notify_all();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    struct Gated {
        file: DeviceFile,
        gate: &'static Gate,
    }
    struct Owner {
        store: Option<Store<Gated>>,
        gate: &'static Gate,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.gate.release();
        }
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
        fn read_exact_at(&self, out: &mut [u8], at: u64) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash read on the runtime owner"));
            }
            if self.gate.armed.load(Ordering::SeqCst)
                && self
                    .gate
                    .skip
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_err()
                && self.gate.armed.swap(false, Ordering::SeqCst)
            {
                let _ = self.gate.notice.try_send(Notice::Entered);
                let mut open = self.gate.open.lock().unwrap();
                while !*open {
                    open = self.gate.changed.wait(open).unwrap();
                }
                if self.gate.failed.swap(false, Ordering::SeqCst) {
                    return Err(refusal("the required hash page failed"));
                }
            }
            let result = self.file.read_exact_at(out, at);
            if result.is_ok() && self.gate.armed.load(Ordering::SeqCst) {
                let _ = self.gate.first_read.compare_exchange(
                    0,
                    at,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
            result
        }
        fn write_all_at(&self, out: &[u8], at: u64) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash write on the runtime owner"));
            }
            let first = self.gate.first_read.load(Ordering::SeqCst);
            if first != 0 && at <= first && at + u64::try_from(out.len()).unwrap() > first {
                self.gate.old_page_written.store(true, Ordering::SeqCst);
            }
            self.file.write_all_at(out, at)
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash flush on the runtime owner"));
            }
            self.file.sync_data()
        }
        fn try_clone(&self) -> Result<Self, DiskError> {
            Ok(Self {
                file: self.file.try_clone()?,
                gate: self.gate,
            })
        }
    }
    fn key(i: usize) -> Vec<u8> {
        let mut key = vec![0, 255];
        key.extend_from_slice(&u32::try_from(i).unwrap().to_be_bytes());
        key.extend_from_slice(&[0, 255]);
        key
    }

    type Entries = BTreeMap<Vec<u8>, (Op, Vec<u8>)>;

    // Three versions fit the configured fanout and entry bound; the fourth forces
    // actual public leaf settlement and frees the former input extents at checkpoint.
    const VERSIONS: usize = 3;

    fn version(n: usize) -> Entries {
        (0..ROWS)
            .map(|i| {
                let op = if (i + n).is_multiple_of(7) {
                    Op::Delete
                } else {
                    Op::Put
                };
                let value = if op == Op::Put {
                    vec![u8::try_from((i + n) % 251).unwrap(); 100]
                } else {
                    Vec::new()
                };
                (key(i), (op, value))
            })
            .collect()
    }

    fn add_version(store: &mut Store<Gated>, trunk: &mut Trunk, entries: &Entries) {
        let mut builder =
            Builder::new(store, Keys::Exactly(u64::try_from(entries.len()).unwrap())).unwrap();
        for (key, (op, value)) in entries {
            builder.add(store, key, *op, value).unwrap();
        }
        let branch = builder.finish(store).unwrap();
        trunk.incorporate(store, branch).unwrap();
    }

    fn native_config() -> RuntimeConfig {
        // The same two roles as the native point/hash fixture: owner and progress task.
        RuntimeConfig {
            shards: 1,
            tasks_per_shard: 2,
            timers_per_shard: 2,
            interests_per_shard: 4,
            ring_entries: 2,
            batch: 2,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            spin_ns: 0,
            page_bytes: 4096,
            wake_tracking: None,
            pin: false,
            cores: Vec::new(),
        }
    }

    fn setup(
        path: &std::path::Path,
        fail: bool,
    ) -> (
        Store<Gated>,
        Trunk,
        Entries,
        &'static Gate,
        hyper_rt::sync::ChannelReceiver<Notice>,
    ) {
        let (notice, heard) = hyper_rt::sync::channel(1).unwrap();
        let gate = Box::leak(Box::new(Gate {
            open: Mutex::new(true),
            changed: Condvar::new(),
            armed: AtomicBool::new(false),
            skip: AtomicUsize::new(1),
            failed: AtomicBool::new(fail),
            notice,
            first_read: AtomicU64::new(0),
            old_page_written: AtomicBool::new(false),
        }));
        let file = Gated {
            file: DeviceFile::open(
                path,
                true,
                CachingRequest::Buffered,
                Alignment::new(1).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let mut store = Store::create(file, CONFIG).unwrap();
        store.set_cache(0);
        let mut trunk = Trunk::new(TrunkConfig {
            fanout: VERSIONS,
            leaf_entries: u64::try_from(VERSIONS * ROWS).unwrap(),
        })
        .unwrap();
        trunk.set_consolidation(Consolidation::Never);
        trunk.set_view_choice(ViewChoice::Build);
        let mut oracle = Entries::new();
        for n in 0..VERSIONS {
            let entries = version(n);
            add_version(&mut store, &mut trunk, &entries);
            oracle.extend(entries);
        }
        (store, trunk, oracle, gate, heard)
    }

    async fn maplet_product(
        store: &mut Store<Gated>,
        trunk: &mut Trunk,
    ) -> Result<crate::maplet::Maplet, Error> {
        let budget = u64::try_from(store.page_capacity() / size_of::<u32>()).unwrap();
        loop {
            // The public routing product is inspected before measured keep/decline;
            // correctness must not depend on the host deciding to keep an accelerator.
            if let Some(trial) = trunk.maplet_job.as_ref().and_then(|job| job.trial.as_ref()) {
                return Ok(trial.maplet.clone());
            }
            if !trunk.maplets_owed() {
                return Err(Error::InvalidArgument {
                    what: "the prepared bundle produced no maplet",
                });
            }
            trunk.clear_io_wait();
            trunk.maplet_step_paced(store, budget)?;
            if trunk.waiting_for_io() && !store.wait_completion().await? {
                return Err(Error::InvalidArgument {
                    what: "a maplet wait without a completion owner",
                });
            }
        }
    }

    async fn maplets_finish(store: &mut Store<Gated>, trunk: &mut Trunk) -> Result<(), Error> {
        let budget = u64::try_from(store.page_capacity() / size_of::<u32>()).unwrap();
        while trunk.maplets_owed() {
            trunk.clear_io_wait();
            trunk.maplet_step_paced(store, budget)?;
            if trunk.waiting_for_io() && !store.wait_completion().await? {
                return Err(Error::InvalidArgument {
                    what: "a maplet wait without a completion owner",
                });
            }
        }
        Ok(())
    }

    async fn views_finish(store: &mut Store<Gated>, trunk: &mut Trunk) -> Result<(), Error> {
        let budget = u64::try_from(store.page_capacity() / size_of::<u32>()).unwrap();
        while trunk.views_owed() {
            trunk.clear_io_wait();
            trunk.view_step_paced(store, budget)?;
            if trunk.waiting_for_io() && !store.wait_completion().await? {
                return Err(Error::InvalidArgument {
                    what: "a view wait without a completion owner",
                });
            }
        }
        Ok(())
    }

    fn check(store: &mut Store<Gated>, trunk: &mut Trunk, oracle: &Entries) {
        let mut value = Vec::new();
        for (key, (op, expected)) in oracle {
            value.clear();
            let got = trunk.get(store, key, &mut value).unwrap();
            if *op == Op::Put {
                assert_eq!(got, Some(Op::Put));
                assert_eq!(&value, expected);
            } else {
                assert_ne!(got, Some(Op::Put));
            }
        }
        for absent in [b"".as_slice(), b"\0\xfe", b"\xff"] {
            value.clear();
            assert_eq!(trunk.get(store, absent, &mut value).unwrap(), None);
        }
        // The actual View/Branch source interface, over exact starts and missing starts,
        // including an exclusive bound inside the binary-key prefix.
        let per = store.page_capacity() / 100;
        for (from, end, limits) in [
            (Vec::new(), key(per), vec![1, VERSIONS]),
            (key(0), key(per), vec![1, VERSIONS]),
            (
                [key(ROWS / 2), vec![0]].concat(),
                key(ROWS / 2 + per),
                vec![1, VERSIONS],
            ),
            (vec![255], vec![255], vec![1]),
            (Vec::new(), vec![255], vec![per]),
        ] {
            for limit in limits {
                let want: Vec<_> = oracle
                    .iter()
                    .filter(|(key, (op, _))| {
                        key.as_slice() >= from.as_slice()
                            && key.as_slice() < end.as_slice()
                            && *op == Op::Put
                    })
                    .map(|(key, (_, value))| (key.clone(), value.clone()))
                    .collect();
                let mut all = Vec::new();
                let mut start = from.clone();
                loop {
                    let mut sources = Vec::new();
                    let mut segment_end = Vec::new();
                    let mut path = Vec::new();
                    let bounded = trunk
                        .segment_at(&start, &mut sources, &mut segment_end, &mut path)
                        .unwrap();
                    let bound = if bounded && segment_end < end {
                        &segment_end
                    } else {
                        &end
                    };
                    let mut merge = ScanMerge::new();
                    merge
                        .open(store, &sources, &start, Some(bound), true, false)
                        .unwrap();
                    let mut page = 0usize;
                    let mut continuation = None;
                    while let Some((key, op, value)) = merge.entry() {
                        if op == Op::Put && page == limit {
                            continuation = Some(key.to_vec());
                            break;
                        }
                        if op == Op::Put {
                            all.push((key.to_vec(), value.to_vec()));
                            page += 1;
                        }
                        merge.next(store).unwrap();
                    }
                    merge.close(store);
                    if let Some(next) = continuation {
                        assert!(next > start, "a scan page did not advance");
                        start = next;
                    } else if bounded && segment_end < end {
                        assert!(segment_end > start, "a segment did not advance");
                        start = segment_end;
                    } else {
                        break;
                    }
                    assert!(all.len() <= want.len(), "a scan did not end");
                }
                assert_eq!(all, want);
            }
        }
    }

    fn reopen(
        store: Store<Gated>,
        path: &std::path::Path,
        gate: &'static Gate,
        applied: u64,
    ) -> (Store<Gated>, Trunk) {
        let (file, landed) = crate::store::cold_file(store.into_file());
        landed.unwrap();
        drop(file);
        let file = Gated {
            file: DeviceFile::open(
                path,
                false,
                CachingRequest::Buffered,
                Alignment::new(1).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let (mut store, checkpoint) = Store::open(file, CONFIG).unwrap();
        assert_eq!(checkpoint.applied, applied);
        let trunk = Trunk::load(&mut store, checkpoint.root.unwrap()).unwrap();
        (store, trunk)
    }

    fn whole_maplet_case(fail: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("maplet-job");
        let (mut store, mut trunk, oracle, gate, mut heard) = setup(&path, fail);
        let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        let mut runtime = LocalRuntime::new(&native_config()).unwrap();
        let _open = OpenOnDrop(gate);
        *gate.open.lock().unwrap() = false;
        gate.armed.store(true, Ordering::SeqCst);
        let deadline = Deadline::new(gate);
        let mut owner = Owner {
            store: Some(store),
            gate,
        };
        let (mut store, mut trunk, product) = runtime
            .block_on(async move {
                let store = owner.store.as_mut().unwrap();
                {
                    let build = maplet_product(store, &mut trunk);
                    let mut build = std::pin::pin!(build);
                    match race2(build.as_mut(), heard.recv()).await {
                        Either::Second(Ok(Notice::Entered)) => {}
                        other => panic!("required maplet page was not held: {other:?}"),
                    }
                }
                let (sent, mut progressed) = hyper_rt::sync::channel(1).unwrap();
                hyper_rt::futures::spawn_detached(async move {
                    sent.try_send(()).unwrap();
                    gate.release();
                })
                .unwrap();
                let result = maplet_product(store, &mut trunk).await;
                progressed.recv().await.unwrap();
                let product = if fail {
                    assert!(matches!(result, Err(Error::Io { .. })));
                    assert_eq!(store.io_stats().buffers_out, 0);
                    maplet_product(store, &mut trunk).await.unwrap()
                } else {
                    result.unwrap()
                };
                maplets_finish(store, &mut trunk).await.unwrap();
                assert_eq!(store.io_stats().buffers_out, 0);
                (owner.store.take().unwrap(), trunk, product)
            })
            .unwrap();
        let expired = Arc::clone(&deadline.expired);
        drop(deadline);
        assert!(
            !expired.load(Ordering::SeqCst),
            "the failure deadline opened the callback"
        );
        // Every key's same hash occurs in all three versions, including newest tombstones.
        // Exactly these three ages exist; a duplicate must keep every possible branch.
        let ages = (1u64 << VERSIONS) - 1;
        for key in oracle.keys() {
            assert_eq!(product.route(filter::hash(key)).unwrap(), ages);
        }
        check(&mut store, &mut trunk, &oracle);
        let head = trunk.save(&mut store).unwrap();
        store.checkpoint(Some(head), 41).unwrap();
        let (mut store, mut trunk) = reopen(store, &path, gate, 41);
        check(&mut store, &mut trunk, &oracle);
    }

    #[test]
    fn whole_maplet_merge_preserves_duplicate_hash_ages_and_newest_values_after_cancel() {
        whole_maplet_case(false);
    }

    #[test]
    fn whole_maplet_required_read_failure_returns_owners_and_rebuilds_exactly() {
        whole_maplet_case(true);
    }

    fn view_replacement_case(fail: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("view-replacement");
        let (mut store, mut trunk, mut oracle, gate, mut heard) = setup(&path, fail);
        let original: BTreeSet<_> = trunk
            .branches()
            .into_iter()
            .flat_map(|branch| branch.extents)
            .collect();
        let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        let mut runtime = LocalRuntime::new(&native_config()).unwrap();
        let _open = OpenOnDrop(gate);
        *gate.open.lock().unwrap() = false;
        gate.armed.store(true, Ordering::SeqCst);
        let deadline = Deadline::new(gate);
        let mut owner = Owner {
            store: Some(store),
            gate,
        };
        let (mut store, mut trunk) = runtime
            .block_on(async move {
                let store = owner.store.as_mut().unwrap();
                {
                    let build = views_finish(store, &mut trunk);
                    let mut build = std::pin::pin!(build);
                    match race2(build.as_mut(), heard.recv()).await {
                        Either::Second(Ok(Notice::Entered)) => {}
                        other => panic!("required view page was not held: {other:?}"),
                    }
                }
                // One actual required read completed before the held callback: the existing
                // View builder therefore owns old page bytes, not merely empty cursors.
                assert_ne!(gate.first_read.load(Ordering::SeqCst), 0);
                let (sent, mut progressed) = hyper_rt::sync::channel(1).unwrap();
                hyper_rt::futures::spawn_detached(async move {
                    sent.try_send(()).unwrap();
                    gate.release();
                })
                .unwrap();
                progressed.recv().await.unwrap();
                assert!(store.wait_completion().await.unwrap());
                if fail {
                    let result = views_finish(store, &mut trunk).await;
                    assert!(matches!(result, Err(Error::Io { .. })));
                    assert_eq!(store.io_stats().buffers_out, 0);
                    views_finish(store, &mut trunk).await.unwrap();
                }
                (owner.store.take().unwrap(), trunk)
            })
            .unwrap();
        let expired = Arc::clone(&deadline.expired);
        drop(deadline);
        assert!(
            !expired.load(Ordering::SeqCst),
            "the failure deadline opened the callback"
        );
        let dropped = trunk.stats().views_dropped;
        let entries = version(VERSIONS);
        add_version(&mut store, &mut trunk, &entries);
        oracle.extend(entries);
        if !fail {
            assert!(
                trunk.stats().views_dropped > dropped,
                "a loaded optional view survived source replacement"
            );
        }
        // Incorporation may leave its newly accepted replacement write out. Retire
        // that physical loan before testing the abandoned cursor's ownership: drain
        // cannot reclaim a still-owned Span, so zero still detects a cursor leak.
        let before_drain = store.io_stats();
        store.drain().unwrap();
        assert_eq!(
            store.io_stats().buffers_out,
            0,
            "after retiring replacement writes; before {before_drain:?}"
        );
        let head = trunk.save(&mut store).unwrap();
        store.checkpoint(Some(head), 42).unwrap();
        // New source writes may now reuse the physical addresses the abandoned view
        // read. Require real public extent reuse and the adapter's actual write fact.
        for n in VERSIONS + 1..=VERSIONS + 2 {
            let entries = version(n);
            add_version(&mut store, &mut trunk, &entries);
            oracle.extend(entries);
        }
        let now: BTreeSet<_> = trunk
            .branches()
            .into_iter()
            .flat_map(|branch| branch.extents)
            .collect();
        assert!(
            !original.is_disjoint(&now),
            "fixture did not reuse a released input extent"
        );
        assert!(
            gate.old_page_written.load(Ordering::SeqCst),
            "fixture did not overwrite the actual old loaded page"
        );
        while trunk.views_owed() {
            trunk.view_step(&mut store, u64::MAX).unwrap();
        }
        assert!(trunk.views() > 0, "replacement bundle produced no view");
        check(&mut store, &mut trunk, &oracle);
        let head = trunk.save(&mut store).unwrap();
        store.checkpoint(Some(head), 43).unwrap();
        let (mut store, mut trunk) = reopen(store, &path, gate, 43);
        assert!(trunk.views() > 0);
        check(&mut store, &mut trunk, &oracle);
    }

    #[test]
    fn a_cancelled_loaded_view_is_returned_before_actual_source_page_reuse() {
        view_replacement_case(false);
    }

    #[test]
    fn a_required_view_page_error_returns_loaded_owners_before_healthy_rebuild() {
        view_replacement_case(true);
    }
}
