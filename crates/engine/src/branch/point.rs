//! A point lookup retains its resolved leaf and demand span across a device wait. The
//! branch's leaf index is in memory: there is no index-page I/O to replay on resumption.

use super::{LEAF, Op, View, corrupt};
use crate::error::{Error, Malformed};
use crate::store::{Span, Store};
use hyper_block::block::BlockFile;
use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PointStep {
    Waiting,
    Done(Option<Op>),
}

/// The caller keeps the request key and branch references stable until this cursor is
/// finished or returned. Its span owns any pending read number; canceling a borrowed wait
/// changes neither the page nor the caller's candidate position.
#[derive(Debug)]
pub(crate) struct PointRead {
    address: u64,
    span: Option<Span>,
    page: Vec<u8>,
    loaded: bool,
    finished: bool,
}

impl PointRead {
    pub(super) fn new(address: u64) -> Self {
        Self {
            address,
            span: None,
            page: Vec::new(),
            loaded: false,
            finished: false,
        }
    }

    /// The same request is stepped until ready; value is changed only after a verified
    /// leaf is present. A demand miss uses the issuer, including with a zero-page cache.
    pub(crate) fn step<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<PointStep, Error> {
        if self.finished {
            return Err(Error::InvalidArgument {
                what: "a point cursor stepped after its lookup ended",
            });
        }
        if !self.loaded {
            if self.span.is_none() {
                match store.with_resident_page(self.address, |page| read_value(page, key, value)) {
                    Ok(Some(found)) => {
                        self.finished = true;
                        return Ok(PointStep::Done(found));
                    }
                    Err(error) => {
                        self.finished = true;
                        return Err(error);
                    }
                    Ok(None) => {}
                }
                self.span = Some(store.span()?);
                self.page = store.take_page();
            }
            let span = self.span.as_mut().ok_or(Error::InvalidArgument {
                what: "a point cursor stepped after its owners were returned",
            })?;
            if !store.read_page_paced(span, self.address, &mut self.page)? {
                return Ok(PointStep::Waiting);
            }
            self.loaded = true;
        }
        // A malformed leaf is terminal too: retrying never appends/re-reads its bytes.
        self.finished = true;
        read_value(&self.page, key, value).map(PointStep::Done)
    }

    /// Gives back the cursor's memory and releases, rather than consumes, an unfinished
    /// read. Store still owns its numbered answer until physical completion/retirement.
    pub(crate) fn give_back(&mut self, store: &mut Store<impl BlockFile>) {
        if let Some(span) = self.span.take() {
            store.give_span(span);
            store.give_page(std::mem::take(&mut self.page));
        }
        self.finished = true;
    }
}

/// A lookup future borrows the owner Store for its complete lifetime. Dropping that
/// future returns its span and scratch, orphaning any numbered read without waiting.
pub(super) struct BorrowedPoint<'a, F: BlockFile> {
    read: PointRead,
    store: &'a mut Store<F>,
}

impl<'a, F: BlockFile> BorrowedPoint<'a, F> {
    pub(super) fn new(read: PointRead, store: &'a mut Store<F>) -> Self {
        Self { read, store }
    }

    pub(super) async fn read(
        &mut self,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        loop {
            match self.read.step(self.store, key, value)? {
                PointStep::Done(found) => return Ok(found),
                PointStep::Waiting => {
                    if self.store.io_outstanding() {
                        self.store.wait_completion().await?;
                    } else {
                        return Err(Error::InvalidArgument {
                            what: "a pending point read without an outstanding completion",
                        });
                    }
                }
            }
        }
    }
}

impl<F: BlockFile> Drop for BorrowedPoint<'_, F> {
    fn drop(&mut self) {
        self.read.give_back(self.store);
    }
}

pub(super) fn read_value(
    page: &[u8],
    key: &[u8],
    value: &mut Vec<u8>,
) -> Result<Option<Op>, Error> {
    let view = View::new(page)?;
    if view.kind != LEAF {
        return Err(corrupt(Malformed::CountMismatch));
    }
    let Some(i) = view.floor(key)? else {
        return Ok(None);
    };
    if view.compare(key, i)? != Ordering::Equal {
        return Ok(None);
    }
    let (_, rest) = view.entry(i)?;
    let tag = rest.first().copied().unwrap_or(0);
    let op = Op::from_byte(tag).ok_or(corrupt(Malformed::UnknownTag(tag)))?;
    value.clear();
    value.extend_from_slice(rest.get(3..).ok_or(corrupt(Malformed::Truncated))?);
    Ok(Some(op))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::filter::Keys;
    use crate::branch::{Branch, Builder};
    use crate::store::Config;
    use hyper_block::DiskError;
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use hyper_block::issuer::Issuer;
    use hyper_rt::combine::{Either, race2};
    use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
    use std::future::{Future, poll_fn};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::task::Poll;

    const CONFIG: Config = Config {
        page_size: 4096,
        extent_pages: 4,
        max_extents: 4096,
    };
    // The existing branch_test binary/index crossing oracle uses this finite dataset size.
    const ROWS: usize = 2049;

    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
        first: AtomicBool,
        fail: AtomicBool,
        reads: AtomicUsize,
        entered: hyper_rt::sync::Sender<()>,
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

    struct Gated {
        file: DeviceFile,
        gate: &'static Gate,
    }
    // Opens held device work before a failed/canceled root drops its attachment.
    struct ReadOwner {
        store: Option<Store<Gated>>,
        gate: &'static Gate,
    }
    impl Drop for ReadOwner {
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
                return Err(refusal("a demand page read on the runtime owner"));
            }
            let mut open = self.gate.open.lock().unwrap();
            if !*open {
                self.gate.reads.fetch_add(1, Ordering::SeqCst);
                if !self.gate.first.swap(true, Ordering::SeqCst) {
                    self.gate.entered.blocking_send(()).unwrap();
                }
                while !*open {
                    open = self.gate.changed.wait(open).unwrap();
                }
                if self.gate.fail.swap(false, Ordering::SeqCst) {
                    return Err(refusal("the required demand page read failed"));
                }
            }
            drop(open);
            self.file.read_exact_at(out, at)
        }
        fn write_all_at(&self, out: &[u8], at: u64) -> Result<(), DiskError> {
            self.file.write_all_at(out, at)
        }
        fn sync_data(&self) -> Result<(), DiskError> {
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
        format!("bucket/shared/path/to/object/{i:08}\0").into_bytes()
    }
    fn expected(i: usize) -> Vec<u8> {
        vec![u8::try_from(i % 251).unwrap(); 100]
    }

    fn held_read(fail: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let (entered, mut heard) = hyper_rt::sync::channel(1).unwrap();
        let gate: &'static Gate = Box::leak(Box::new(Gate {
            open: Mutex::new(true),
            changed: Condvar::new(),
            first: AtomicBool::new(false),
            fail: AtomicBool::new(fail),
            reads: AtomicUsize::new(0),
            entered,
        }));
        let file = Gated {
            file: DeviceFile::open(
                &path,
                true,
                CachingRequest::Buffered,
                Alignment::new(CONFIG.page_size).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let mut store = Store::create(file, CONFIG).unwrap();
        store.set_cache(0);
        let mut builder =
            Builder::new(&mut store, Keys::Exactly(u64::try_from(ROWS).unwrap())).unwrap();
        for i in 0..ROWS {
            let op = if i % 7 == 0 { Op::Delete } else { Op::Put };
            builder
                .add(
                    &mut store,
                    &key(i),
                    op,
                    &if op == Op::Put {
                        expected(i)
                    } else {
                        Vec::new()
                    },
                )
                .unwrap();
        }
        let branch = builder.finish(&mut store).unwrap();
        assert!(
            branch.height > 1,
            "the public descriptor must span index and leaf levels"
        );
        let mut descriptor = Vec::new();
        branch.encode(&mut descriptor).unwrap();
        store.checkpoint(None, 7).unwrap();
        let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        let chosen = ROWS / 2;
        let query = key(chosen);
        let mut cursor = branch.prepare_get_routed(&mut store, &query).unwrap();
        // Two actual roles: the root read and the independent callback-gate opener.
        let config = RuntimeConfig {
            shards: 1,
            tasks_per_shard: 2,
            timers_per_shard: 2,
            interests_per_shard: 4,
            ring_entries: 2,
            batch: 2,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            spin_ns: 0,
            page_bytes: CONFIG.page_size,
            wake_tracking: None,
            pin: false,
            cores: Vec::new(),
        };
        let mut runtime = LocalRuntime::new(&config).unwrap();
        let _open = OpenOnDrop(gate);
        *gate.open.lock().unwrap() = false;
        let mut owner = ReadOwner {
            store: Some(store),
            gate,
        };
        let store = runtime
            .block_on(async move {
                let store = owner.store.as_mut().unwrap();
                let mut value = vec![99];
                assert_eq!(
                    cursor.step(store, &query, &mut value).unwrap(),
                    PointStep::Waiting
                );
                assert_eq!(value, [99]);
                {
                    let mut wait = std::pin::pin!(store.wait_completion());
                    poll_fn(|cx| {
                        assert!(wait.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    assert!(matches!(
                        race2(wait.as_mut(), async {}).await,
                        Either::Second(())
                    ));
                }
                let (sent, mut progressed) = hyper_rt::sync::channel(1).unwrap();
                hyper_rt::futures::spawn_detached(async move {
                    heard.recv().await.unwrap();
                    sent.try_send(()).unwrap();
                    gate.release();
                })
                .unwrap();
                let result = loop {
                    match cursor.step(store, &query, &mut value) {
                        Ok(PointStep::Done(op)) => break Ok(op),
                        Ok(PointStep::Waiting) => {
                            store.wait_completion().await.unwrap();
                        }
                        Err(error) => break Err(error),
                    }
                };
                progressed.recv().await.unwrap();
                assert_eq!(
                    gate.reads.load(Ordering::SeqCst),
                    1,
                    "canceling the borrowed wait must not replay a device page"
                );
                cursor.give_back(store);
                if fail {
                    assert!(matches!(result, Err(Error::Io { .. })));
                    assert_eq!(value, [99]);
                    let mut repaired = branch.prepare_get_routed(store, &query).unwrap();
                    loop {
                        match repaired.step(store, &query, &mut value).unwrap() {
                            PointStep::Done(op) => {
                                assert_eq!(op, Some(Op::Put));
                                break;
                            }
                            PointStep::Waiting => {
                                store.wait_completion().await.unwrap();
                            }
                        }
                    }
                    repaired.give_back(store);
                } else {
                    assert_eq!(result.unwrap(), Some(Op::Put));
                }
                assert_eq!(value, expected(chosen));
                store.retire_async().await.unwrap();
                owner.store.take().unwrap()
            })
            .unwrap();
        let (file, landed) = crate::store::cold_file(store.into_file());
        landed.unwrap();
        drop(file);
        let file = Gated {
            file: DeviceFile::open(
                &path,
                false,
                CachingRequest::Buffered,
                Alignment::new(CONFIG.page_size).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let (mut reopened, recovered) = Store::open(file, CONFIG).unwrap();
        assert_eq!(recovered.applied, 7);
        reopened.set_cache(0);
        let (branch, used) = Branch::decode(&mut reopened, &descriptor).unwrap();
        assert_eq!(used, descriptor.len());
        let mut value = Vec::new();
        for i in 0..ROWS {
            let op = branch.get(&mut reopened, &key(i), &mut value).unwrap();
            if i % 7 == 0 {
                assert_eq!(op, Some(Op::Delete));
                assert!(value.is_empty());
            } else {
                assert_eq!(op, Some(Op::Put));
                assert_eq!(value, expected(i));
            }
        }
        assert_eq!(
            branch.get(&mut reopened, &key(ROWS), &mut value).unwrap(),
            None
        );
    }

    #[test]
    fn zero_cache_held_point_read_survives_borrowed_wait_cancellation_and_reopen() {
        held_read(false);
    }
    #[test]
    fn required_point_read_failure_can_be_repaired_then_reopened_with_exact_values() {
        held_read(true);
    }
}
