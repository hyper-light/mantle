//! A range's rows in mantle's engine (`mantle_engine::shard_db::ShardDb`, docs/design/
//! engine-structure.md §2), as the metadata layer's [`Engine`]: the state machine a replicated
//! range applies its entries into on a node.
//!
//! The engine keeps no write-ahead log of its own: the Raft log is the only one (metadata.md §4;
//! engine-structure.md §2). An entry's writes go into the engine's memtable; [`Engine::persist`]
//! is a checkpoint that records the last applied index, durable when it returns; and an engine
//! opened again stands at its last checkpoint's index, from which the log is replayed. A crash
//! between checkpoints loses what was applied since the last, which the log still holds.
//!
//! A batch is applied write by write. Nothing reads the engine between them (its owner applies
//! the batch whole on its own thread), and a crash part way returns to the last checkpoint, from
//! which the entry is applied again whole: a put and a delete applied twice leave what applying
//! them once leaves, so the batch is atomic as the trait asks.
//!
//! A replicated range applies its entries with [`Engine::apply_now`], on a node's shard that
//! serves its consensus and transport too: the room the whole batch takes in the memtable is made
//! first, waiting on no maintenance worker, and the batch applied only once it is
//! (`ShardDb::room_now`). With none, nothing of the batch is applied, so no read sees part of an
//! entry, and the entry waits committed and unapplied, its range driven again when the worker's
//! message wakes the shard's task (docs/design/replica.md §2). The rows an entry's commands read
//! are read so too ([`Engine::get_now`], [`Engine::next_now`]): from memory, or refused with the
//! page they need loading on a maintenance worker (`ShardDb::get_ready`), the entry waiting as it
//! waits for room.

use hyper_block::block::BlockFile;
use mantle_engine::memtable::hashed::entry_bytes;
use mantle_engine::rows::Rows as EngineRows;
use mantle_engine::shard_db::{FlushStats, ShardDb};
use mantle_engine::store::{Config, IoStats};
use mantle_engine::trunk::{TrunkConfig, TrunkStats};
use mantle_meta::engine::{Engine, EngineError, Row, Rows, Write};
use mantle_meta::session::Rules;

/// Whether an engine of pages of `page_size` bytes and a memtable of `memtable_bytes` holds a row
/// at the bounds `rules` sets on a key and a value whole: a branch page holds one alone
/// (`mantle_engine::branch::fits`), and so does the memtable, which rotates only when it holds
/// something (`mantle_engine::memtable::hashed::entry_bytes`). The apply layer refuses a command
/// whose writes hold a row past the bounds (`mantle_meta::apply`), so an engine that holds a row
/// at them holds every row its range writes. One that does not takes such a row into its memtable
/// and fences at its next checkpoint, as the branch refuses it, on every member at once, and the
/// log gives the row again at each restart (docs/design/node.md §1.4).
pub fn holds_rows(page_size: usize, memtable_bytes: usize, rules: &Rules) -> bool {
    let (key, value) = (rules.max_key_bytes, rules.max_value_bytes);
    let page = mantle_engine::store::page::capacity(page_size);
    mantle_engine::branch::fits(page, key, value)
        && mantle_engine::memtable::hashed::entry_bytes(key, value)
            .is_some_and(|bytes| bytes <= memtable_bytes)
}

/// A range's rows in mantle's engine.
pub struct Stored<F: BlockFile> {
    db: ShardDb<F>,
    applied: u64,
    durable: u64,
    /// Whether anything was applied or installed since the last checkpoint: an image installed
    /// at the index the engine stands at, a range's founding rows at 0, changes it all the same.
    unpersisted: bool,
    /// The checkpoints taken since the engine opened: each `persist` that found something to make
    /// durable, two flushes of the store's device apiece (`Store::checkpoint`).
    checkpoints: u64,
    /// The index of the checkpoint [`Engine::persist_now`] began and has not yet seen durable.
    out: Option<u64>,
    /// A get's value and a scan's rows: the engine's buffers, kept across calls.
    value: Vec<u8>,
    rows: EngineRows,
    /// The engine's own error that fenced it, the first: an [`EngineError::Fenced`] says only
    /// that one did, and its owner reports this one ([`Engine::fault`]).
    fault: Option<mantle_engine::Error>,
}

impl<F: BlockFile + 'static> Stored<F> {
    /// A new engine in `file`, which must be empty, before any entry.
    pub fn create(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<Self, mantle_engine::Error> {
        Ok(Self::of(ShardDb::create(file, store, mem_limit, trunk)?, 0))
    }

    /// The engine in `file` at its newest checkpoint: applied and durable through the index that
    /// checkpoint holds.
    pub fn open(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<Self, mantle_engine::Error> {
        let (db, applied) = ShardDb::open(file, store, mem_limit, trunk)?;
        Ok(Self::of(db, applied))
    }

    fn of(db: ShardDb<F>, applied: u64) -> Self {
        Self {
            db,
            applied,
            durable: applied,
            unpersisted: false,
            checkpoints: 0,
            out: None,
            value: Vec::new(),
            rows: EngineRows::new(),
            fault: None,
        }
    }

    /// Runs the engine's packings and compactions on at most `most` maintenance workers, each
    /// writing through its own handle on the store's file that `open` gives
    /// (`ShardDb::set_workers_within`): an apply then hands a full memtable or a due compaction to
    /// a worker and takes its result back, where it wrote every page of it on its own thread.
    /// [`Engine::persist`] still returns only once the checkpoint is durable, waiting for the
    /// workers' jobs out first. The owner counts the workers' threads and memory
    /// (docs/design/node.md §1.4). Their threads carry `name`, the range's
    /// (`ShardDb::set_workers_within`).
    pub fn set_workers(
        &mut self,
        open: impl FnMut() -> Result<F, mantle_engine::Error> + Send + 'static,
        most: usize,
        name: &str,
    ) -> Result<(), mantle_engine::Error>
    where
        F: Send,
    {
        self.db.set_workers_within(open, most, name)
    }

    /// Whether the engine is at its store's extent bound (`ShardDb::at_extent_bound`): every
    /// entry waits for room ([`Engine::apply_now`] answers false) until extents are freed.
    pub fn at_extent_bound(&self) -> bool {
        self.db.at_extent_bound()
    }

    /// Gives gets and lookups a cache of `pages` of the store's pages, each verified as it was
    /// read or written, replacing any it had; none at 0 (`ShardDb::set_cache`). Without one,
    /// every page a get or a lookup needs is read from the device again.
    pub fn set_cache(&mut self, pages: usize) {
        self.db.set_cache(pages);
    }

    /// The store's reads since the engine opened: the pages read from the device, and the reads
    /// its cache served and missed (`ShardDb::stats`).
    pub fn io_stats(&self) -> IoStats {
        self.db.stats().2
    }

    /// The trunk's maintenance since the engine opened: its compactions, and the most that held
    /// their inputs on the engine's own thread at once (`ShardDb::stats`).
    pub fn trunk_stats(&self) -> TrunkStats {
        self.db.stats().1
    }

    /// The engine's maintenance outside the trunk since it opened: its packings, and its idle
    /// steps, the units they did and those that left their wait to a worker's message
    /// (`ShardDb::stats`).
    pub fn flush_stats(&self) -> FlushStats {
        self.db.stats().0
    }

    /// The rows of `[from, to)` (to the end with no `to`), all of them, into the scan buffer.
    fn scan_all(&mut self, from: &[u8], to: Option<&[u8]>) -> Result<(), EngineError> {
        self.rows.clear();
        self.db
            .scan_rows(from, to, usize::MAX, &mut self.rows)
            .map_err(|e| fenced(&mut self.fault, e))?;
        Ok(())
    }

    /// The pages loaded and loading for reads that may not wait, and the loads out on a worker
    /// (`ShardDb::loads`).
    pub fn loads(&self) -> (usize, usize) {
        self.db.loads()
    }

    /// The checkpoint begun at `at` ([`Engine::persist_now`]) is durable.
    fn durable_at(&mut self, at: u64) {
        self.out = None;
        self.durable = at;
        self.checkpoints = self.checkpoints.saturating_add(1);
    }

    /// The bytes `writes` take in the engine's memtable at most, each entry's head at its largest
    /// (`mantle_engine::memtable::hashed::entry_bytes`): a clear's deletes as the rows its span
    /// holds now, which reads the span, and the batch's own puts into it. Saturated: a sum past the
    /// address space is more than any memtable holds, which rotates as it fills.
    fn batch_bytes(&mut self, writes: &[Write]) -> Result<usize, EngineError> {
        let entry = |key: usize, value: usize| entry_bytes(key, value).unwrap_or(usize::MAX);
        let mut bytes = 0usize;
        for write in writes {
            let more = match write {
                Write::Put(k, v) => entry(k.len(), v.len()),
                Write::Delete(k) => entry(k.len(), 0),
                Write::Clear(from, to) if from < to => {
                    self.scan_all(from, Some(to))?;
                    let held = self.rows.iter().map(|(k, _)| entry(k.len(), 0));
                    let own = writes.iter().filter_map(|w| match w {
                        Write::Put(k, _) if k >= from && k < to => Some(entry(k.len(), 0)),
                        _ => None,
                    });
                    held.chain(own).fold(0usize, usize::saturating_add)
                }
                Write::Clear(..) => 0,
            };
            bytes = bytes.saturating_add(more);
        }
        Ok(bytes)
    }

    /// Deletes every row of `[from, to)` (to the end with no `to`).
    fn clear(&mut self, from: &[u8], to: Option<&[u8]>) -> Result<(), EngineError> {
        self.scan_all(from, to)?;
        let Self {
            db, rows, fault, ..
        } = self;
        for (key, _) in rows.iter() {
            db.delete(key).map_err(|e| fenced(fault, e))?;
        }
        Ok(())
    }
}

/// An engine that failed a read, a write or a flush: nothing more is applied until the range
/// recovers from its checkpoint and the log, or from its peers. Its error is kept in `fault`
/// unless one is already, the first being the cause.
fn fenced(fault: &mut Option<mantle_engine::Error>, e: mantle_engine::Error) -> EngineError {
    fault.get_or_insert(e);
    EngineError::Fenced
}

impl<F: BlockFile + 'static> Rows for Stored<F> {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.value.clear();
        let found = self
            .db
            .get(key, &mut self.value)
            .map_err(|e| fenced(&mut self.fault, e))?;
        // The value's buffer goes with the answer, not a copy of it.
        Ok(found.then(|| std::mem::take(&mut self.value)))
    }

    fn next(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        self.rows.clear();
        // The row alone: no continuation is found past it.
        self.db
            .scan_rows(from, Some(to), 1, &mut self.rows)
            .map_err(|e| fenced(&mut self.fault, e))?;
        Ok(self.rows.get(0).map(|(k, v)| (k.to_vec(), v.to_vec())))
    }

    fn apply(&mut self, index: u64, writes: Vec<Write>) -> Result<(), EngineError> {
        if index <= self.applied {
            return Err(EngineError::OutOfOrder {
                index,
                applied: self.applied,
            });
        }
        for write in &writes {
            match write {
                Write::Put(k, v) => self.db.put(k, v).map_err(|e| fenced(&mut self.fault, e))?,
                Write::Delete(k) => self.db.delete(k).map_err(|e| fenced(&mut self.fault, e))?,
                Write::Clear(from, to) => {
                    if from < to {
                        self.clear(from, Some(to))?;
                    }
                }
            }
        }
        self.applied = index;
        self.unpersisted = true;
        Ok(())
    }
}

impl<F: BlockFile + 'static> Engine for Stored<F> {
    fn applied(&self) -> u64 {
        self.applied
    }

    fn durable(&self) -> u64 {
        self.durable
    }

    /// A checkpoint begun without waiting and still out is finished first, waiting on its worker
    /// (`ShardDb::checkpoint`): nothing applied since it began leaves it unpersisted. With
    /// nothing applied since the last, one is made all the same while the engine waits for
    /// extents the next checkpoint frees (`ShardDb::wants_checkpoint`), at the index already
    /// durable.
    fn persist(&mut self) -> Result<(), EngineError> {
        if self.unpersisted || self.out.is_some() || self.db.wants_checkpoint() {
            self.db
                .checkpoint(self.applied)
                .map_err(|e| fenced(&mut self.fault, e))?;
            self.durable = self.applied;
            self.unpersisted = false;
            self.out = None;
            self.checkpoints = self.checkpoints.saturating_add(1);
        }
        Ok(())
    }

    /// The checkpoint begun at the index applied now and made without waiting
    /// (`ShardDb::checkpoint_now`): its memtables packed and its pages written by the engine's
    /// maintenance workers and the shard in slices, its two flushes of the device on a worker's
    /// own handle, the worker's message waking the task. That index is durable once it
    /// completes; entries applied meanwhile are past it, not covered by it, and leave the engine
    /// unpersisted.
    fn persist_now(&mut self) -> Result<bool, EngineError> {
        if self.fault.is_some() {
            return Err(EngineError::Fenced);
        }
        let at = match self.out {
            // Completed meanwhile by a put that wanted the memtable's room, which steps it
            // (`ShardDb::room_now`); one that failed there fenced the engine.
            Some(at) if self.db.checkpointing().is_none() => {
                self.durable_at(at);
                return Ok(true);
            }
            Some(at) => at,
            // With nothing applied since the last, the engine may still wait for extents only
            // the next checkpoint frees (`ShardDb::wants_checkpoint`): one at the index applied,
            // durable already, so the log's start stays where it may.
            None if !self.unpersisted && !self.db.wants_checkpoint() => return Ok(true),
            None => {
                // Begun here: what is applied from now on is the next checkpoint's.
                self.unpersisted = false;
                self.out = Some(self.applied);
                self.applied
            }
        };
        let done = self
            .db
            .checkpoint_now(at)
            .map_err(|e| fenced(&mut self.fault, e))?;
        if done {
            self.durable_at(at);
        }
        Ok(done)
    }

    fn image(&mut self) -> Result<Vec<Row>, EngineError> {
        self.scan_all(&[], None)?;
        Ok(self
            .rows
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect())
    }

    /// The engine counts no rows apart from reading them.
    fn image_size(&self) -> Option<(u64, u64)> {
        None
    }

    fn checkpoints(&self) -> (u64, u64) {
        (self.checkpoints, self.db.syncs())
    }

    fn fault(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.fault
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }

    /// The batch applied once the memtable has room for all of it with no wait on a maintenance
    /// worker (`ShardDb::room_now`), the room `batch_bytes` counts: so a read between the drives
    /// that apply the range's entries sees each entry's writes all or none, and the shard waits on
    /// no worker. Room is made before any write rather than reads held while an entry is part
    /// applied: a range's reads go to its engine from several paths of the node, and none then
    /// needs to know of an entry in progress. A batch larger than a whole memtable rotates inline
    /// as [`Rows::apply`] does, nothing reading between its writes. The shard's own share of the
    /// rotation's work is a unit a write, as its puts pay theirs as they go.
    fn apply_now(&mut self, index: u64, writes: Vec<Write>) -> Result<bool, EngineError> {
        if index <= self.applied {
            return Err(EngineError::OutOfOrder {
                index,
                applied: self.applied,
            });
        }
        let bytes = self.batch_bytes(&writes)?;
        let keys = u64::try_from(writes.len()).unwrap_or(u64::MAX).max(1);
        let room = self.db.room_now(bytes, keys);
        if !room.map_err(|e| fenced(&mut self.fault, e))? {
            return Ok(false);
        }
        self.apply(index, writes)?;
        // The entry's reads are done: the pages loaded for them go to the cache.
        self.db.release_loaded();
        Ok(true)
    }

    /// The value from memory (`ShardDb::get_ready`), or [`EngineError::Unloaded`] with its page
    /// loading on a maintenance worker, whose message wakes the task.
    fn get_now(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        self.value.clear();
        let found = self
            .db
            .get_ready(key, &mut self.value)
            .map_err(|e| fenced(&mut self.fault, e))?
            .ok_or(EngineError::Unloaded)?;
        Ok(found.then(|| std::mem::take(&mut self.value)))
    }

    /// The row from memory (`ShardDb::scan_rows_ready`), or [`EngineError::Unloaded`] as
    /// [`Engine::get_now`].
    fn next_now(&mut self, from: &[u8], to: &[u8]) -> Result<Option<Row>, EngineError> {
        if from >= to {
            return Ok(None);
        }
        self.rows.clear();
        self.db
            .scan_rows_ready(from, Some(to), 1, &mut self.rows)
            .map_err(|e| fenced(&mut self.fault, e))?
            .ok_or(EngineError::Unloaded)?;
        Ok(self.rows.get(0).map(|(k, v)| (k.to_vec(), v.to_vec())))
    }

    /// `ShardDb::loads_sent`.
    fn loads_sent(&self) -> u64 {
        self.db.loads_sent()
    }

    /// `ShardDb::loads_most`.
    fn loads_most(&self) -> usize {
        self.db.loads_most()
    }

    /// The pages loaded for the reader go to the cache (`ShardDb::release_loaded`).
    fn reads_done(&mut self) {
        self.db.release_loaded();
    }

    /// The engine's idle step that waits for nothing (`ShardDb::idle_step_now`), in SILK's order:
    /// the packing first, then the trunk's work and its workers' results, then the consolidations
    /// seeks paid for, then the cache's. A puts-only or reads-only range otherwise paid it only at
    /// its next put or checkpoint. Work that waits on a job out on a worker answers none done and
    /// work owed, the owner's task woken by the worker's next message. One that failed fences the
    /// engine, as a failed apply does.
    fn upkeep(&mut self, budget: u64) -> Result<(u64, bool), EngineError> {
        if self.fault.is_some() {
            return Err(EngineError::Fenced);
        }
        self.db
            .idle_step_now(budget)
            .map_err(|e| fenced(&mut self.fault, e))
    }

    fn install(&mut self, index: u64, rows: Vec<Row>) -> Result<(), EngineError> {
        // The entry that waited on them is passed by the image.
        self.db.release_loaded();
        self.clear(&[], None)?;
        for (k, v) in &rows {
            self.db.put(k, v).map_err(|e| fenced(&mut self.fault, e))?;
        }
        self.applied = index;
        self.unpersisted = true;
        Ok(())
    }

    /// Its cache's limit set to the pages `bytes` hold, whole: lowered at once, the pages past it
    /// dropped and none kept at none (`ShardDb::shrink_cache`), or raised keeping its pages, a
    /// cache made where it had none (`ShardDb::grow_cache`); a limit of as many pages is left as
    /// it is, so a part that moves by less than a page costs nothing.
    fn lend_cache(&mut self, bytes: u64) -> u64 {
        let (now, page) = self.db.cache_limit();
        let page = u64::try_from(page).unwrap_or(u64::MAX).max(1);
        let whole = bytes.checked_div(page).unwrap_or(0);
        let pages = usize::try_from(whole).unwrap_or(usize::MAX);
        if pages < now {
            self.db.shrink_cache(pages);
        } else if pages > now {
            self.db.grow_cache(pages);
        }
        u64::try_from(pages)
            .ok()
            .and_then(|pages| pages.checked_mul(page))
            .unwrap_or(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use std::path::Path;

    /// The engine's own small test configuration (crates/engine/tests/pool_startup.rs).
    const CONFIG: Config = Config {
        page_size: 4096,
        extent_pages: 4,
        max_extents: 256,
    };
    const TRUNK: TrunkConfig = TrunkConfig {
        fanout: 3,
        leaf_entries: 96,
    };

    fn file(path: &Path, create: bool) -> DeviceFile {
        DeviceFile::open(
            path,
            create,
            CachingRequest::Buffered,
            Alignment::new(CONFIG.page_size).unwrap(),
        )
        .unwrap()
    }

    fn put(k: &str, v: &str) -> Write {
        Write::Put(k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    fn rows(e: &mut Stored<DeviceFile>) -> Vec<(String, String)> {
        e.image()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()))
            .collect()
    }

    /// Rules whose bounds on a row are `key` and `value`.
    fn bounds(key: usize, value: usize) -> Rules {
        Rules {
            lifetime_ns: u64::MAX,
            max_sessions: 1,
            expiries_per_entry: 1,
            max_key_bytes: key,
            max_value_bytes: value,
        }
    }

    /// An engine that holds a row at the bounds keeps one through a checkpoint. The engine's
    /// small page, which does not, takes the row into its memtable and fences at the checkpoint
    /// that packs it, as a range would on every member: a node refuses to start on it.
    #[test]
    fn an_engine_that_holds_a_row_at_the_bounds_keeps_one_and_one_that_does_not_fences() {
        let (key, value) = (100, 8_000);
        let rules = bounds(key, value);
        let page = mantle_engine::branch::least_page(CONFIG.page_size, key, value).unwrap();
        let store = Config {
            page_size: page,
            ..CONFIG
        };
        let memtable = mantle_engine::memtable::hashed::entry_bytes(key, value).unwrap();
        assert!(holds_rows(store.page_size, memtable, &rules));
        assert!(!holds_rows(store.page_size, memtable - 1, &rules));
        assert!(!holds_rows(CONFIG.page_size, memtable, &rules));
        let row = Write::Put(vec![b'k'; key], vec![7; value]);
        for (config, kept) in [(store, true), (CONFIG, false)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("range.store");
            let mut e = Stored::create(file(&path, true), config, memtable, TRUNK).unwrap();
            e.apply(1, vec![row.clone()]).unwrap();
            if kept {
                e.persist().unwrap();
                assert_eq!(e.get(&vec![b'k'; key]).unwrap(), Some(vec![7; value]));
            } else {
                assert_eq!(e.persist(), Err(EngineError::Fenced));
            }
        }
    }

    #[test]
    fn writes_read_back_in_order_and_a_clear_takes_its_span_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        e.apply(
            1,
            vec![put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")],
        )
        .unwrap();
        assert_eq!(e.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(e.get(b"x").unwrap(), None);
        assert_eq!(
            e.next(b"b", b"d").unwrap(),
            Some((b"b".to_vec(), b"2".to_vec()))
        );
        assert_eq!(e.next(b"bb", b"c").unwrap(), None);
        e.apply(
            2,
            vec![
                Write::Delete(b"a".to_vec()),
                Write::Clear(b"b".to_vec(), b"d".to_vec()),
            ],
        )
        .unwrap();
        assert_eq!(rows(&mut e), vec![("d".into(), "4".into())]);
        assert_eq!(e.applied(), 2);
        assert_eq!(
            e.apply(2, vec![put("e", "5")]),
            Err(EngineError::OutOfOrder {
                index: 2,
                applied: 2
            })
        );
    }

    #[test]
    fn opened_again_it_stands_at_its_last_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        e.apply(1, vec![put("a", "1")]).unwrap();
        e.apply(2, vec![put("b", "2")]).unwrap();
        e.persist().unwrap();
        assert_eq!(e.durable(), 2);
        // Applied past the checkpoint, then lost with the process: the log holds it again.
        e.apply(3, vec![put("c", "3"), Write::Delete(b"a".to_vec())])
            .unwrap();
        drop(e);
        let mut e = Stored::open(file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        assert_eq!((e.applied(), e.durable()), (2, 2));
        assert_eq!(
            rows(&mut e),
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
    }

    #[test]
    fn rows_installed_at_index_zero_are_kept_by_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        e.install(0, vec![(b"f".to_vec(), b"0".to_vec())]).unwrap();
        e.persist().unwrap();
        drop(e);
        let mut e = Stored::open(file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        assert_eq!(rows(&mut e), vec![("f".into(), "0".into())]);
    }

    /// A row as a Name range's are sized: a version's key and its record.
    fn row(i: u64, generation: u8) -> Row {
        (
            format!("v/b/k{i:06}/{}", "0".repeat(24)).into_bytes(),
            vec![generation; 120],
        )
    }

    #[test]
    fn an_image_installs_over_a_store_of_as_many_rows() {
        const ROWS: u64 = 1_100;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        for i in 0..ROWS {
            let (k, v) = row(i, 1);
            e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
            e.persist().unwrap();
        }
        let image: Vec<Row> = (0..ROWS).map(|i| row(i, 2)).collect();
        e.install(ROWS + 10, image.clone()).unwrap();
        e.persist().unwrap();
        assert_eq!(e.image().unwrap(), image);
    }

    /// A range's engine flushes its store's device only at a checkpoint, twice (the map, then the
    /// superblock naming it, `Store::checkpoint`): a burst whose memtables its maintenance worker
    /// packs and whose trunk it compacts flushes nothing of its own, every output made durable by
    /// the checkpoint that first names it, and a checkpoint asked with nothing applied since, as a
    /// range that only serves reads asks it, flushes nothing. Opened again, it stands at the
    /// checkpoint with every row of the burst.
    #[test]
    fn its_store_flushes_only_at_a_checkpoint_and_twice_there() {
        const ROWS: u64 = 2_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        // A new store's first checkpoint, of nothing, is its creation's.
        assert_eq!(e.checkpoints(), (0, 2));
        let reopen = path.clone();
        e.set_workers(move || Ok(file(&reopen, false)), 1, "")
            .unwrap();
        for i in 0..ROWS {
            let (k, v) = row(i, 1);
            e.apply(i + 1, vec![Write::Put(k, v)]).unwrap();
        }
        let trunk = e.trunk_stats();
        assert!(trunk.flushes > 0, "the burst packed no memtable: {trunk:?}");
        assert_eq!(e.checkpoints(), (0, 2), "maintenance flushed on its own");
        e.persist().unwrap();
        let trunk = e.trunk_stats();
        assert!(
            trunk.pivot_compactions + trunk.leaf_compactions > 0,
            "the burst compacted nothing: {trunk:?}"
        );
        assert_eq!(e.checkpoints(), (1, 4));
        // Quiet: asked again with nothing applied, as a compaction of a range serving reads asks.
        for _ in 0..3 {
            e.persist().unwrap();
        }
        assert_eq!(e.checkpoints(), (1, 4), "a quiet checkpoint flushed");
        assert_eq!(e.durable(), ROWS);
        drop(e);
        let mut e = Stored::open(file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        assert_eq!(
            (e.applied(), e.durable(), e.checkpoints()),
            (ROWS, ROWS, (0, 0))
        );
        let image = e.image().unwrap();
        assert_eq!(image.len(), usize::try_from(ROWS).unwrap());
        assert_eq!(image, (0..ROWS).map(|i| row(i, 1)).collect::<Vec<_>>());
    }

    #[test]
    fn an_installed_image_replaces_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.store");
        let mut e = Stored::create(file(&path, true), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        e.apply(1, vec![put("a", "1"), put("z", "26")]).unwrap();
        e.install(9, vec![(b"m".to_vec(), b"13".to_vec())]).unwrap();
        e.persist().unwrap();
        assert_eq!(rows(&mut e), vec![("m".into(), "13".into())]);
        drop(e);
        let mut e = Stored::open(file(&path, false), CONFIG, CONFIG.page_size, TRUNK).unwrap();
        assert_eq!(e.durable(), 9);
        assert_eq!(rows(&mut e), vec![("m".into(), "13".into())]);
    }
}
