//! A volume: format it, open it (recovering from whatever state it was left in), write and
//! delete chunks through its group-commit writer, and read them back verified.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

use mantle_disk::DiskError;
use mantle_disk::block::BlockFile;
use mantle_disk::buf::Pool;

use crate::clean::{CleanReport, Cleaner};
use crate::error::ChunkError;
use crate::frame::SegmentState;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::layout::{Config, Geometry};
use crate::read;
use crate::record::Payload;
use crate::recover::{self, RecoveryReport, read_span};
use crate::scrub::{Findings, Scrubber, SharedFindings, scrub_all};
use crate::superblock::{OFFSET_A, OFFSET_B_COMPACT, OFFSET_B_STANDARD, Superblock};
use crate::writer::{Op, Request, Shared, Writer, write_superblock};

/// A chunk's size and state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkStat {
    pub len: u64,
    pub sealed: bool,
    pub fragments: usize,
}

/// How the volume's segments are used.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub segments: u32,
    pub free: u32,
    pub open: u32,
    pub sealed: u32,
    /// Bytes of records some chunk still references.
    pub live_bytes: u64,
    /// Checkpoints written since the volume opened.
    pub checkpoints: u64,
}

pub struct Volume<F: BlockFile + 'static> {
    shared: Arc<Shared<F>>,
    sender: Option<SyncSender<Request>>,
    writer: Option<JoinHandle<()>>,
    /// Wakes the cleaner, which stops once `stopping` is set.
    wake_cleaner: Option<SyncSender<()>>,
    cleaner: Option<JoinHandle<()>>,
    wake_scrubber: Option<SyncSender<()>>,
    scrubber: Option<JoinHandle<()>>,
    findings: SharedFindings,
    superblock: Superblock,
    watermarks: (usize, usize),
    limits: crate::layout::Limits,
}

impl<F: BlockFile + 'static> Volume<F> {
    /// Lays out a new, empty volume of `size` bytes in `file` and opens it. Whatever `file`
    /// held before is ignored: every structure is bound to the new volume's random id.
    pub fn format(file: F, size: u64, config: Config) -> Result<Self, ChunkError> {
        let geometry = Geometry::plan(size, file.alignment(), &config)?;
        let volume = random_id()?;
        let created_ns = now_ns();
        let base = Superblock {
            block: u32::try_from(geometry.block).map_err(|_| ChunkError::Config("block".into()))?,
            volume,
            sequence: 0,
            created_ns,
            segment_size: geometry.segment_size,
            segments: geometry.segments,
            checksum_shift: config.checksum_shift,
            offset_b: geometry.offset_b,
            log_offset: geometry.log_offset,
            log_size: geometry.log_size,
            data_offset: geometry.data_offset,
            start_lsn: 1,
            start_pos: 0,
            // Nothing issued, nothing reserved: the first batch reserves.
            sequence_limit: 0,
            incarnation_limit: 0,
        };
        let a = base.clone();
        let b = Superblock {
            sequence: 1,
            ..base
        };
        write_superblock(&file, &a)?;
        write_superblock(&file, &b)?;
        file.sync_data().map_err(ChunkError::Device)?;
        let (volume, _) = Self::start(file, b, geometry, config)?;
        Ok(volume)
    }

    /// Opens a volume, recovering from a clean shutdown or a crash alike.
    pub fn open(file: F, config: Config) -> Result<(Self, RecoveryReport), ChunkError> {
        let superblock = read_superblock(&file)?;
        let geometry = Geometry {
            block: u64::from(superblock.block),
            segment_size: superblock.segment_size,
            segments: superblock.segments,
            offset_b: superblock.offset_b,
            log_offset: superblock.log_offset,
            log_size: superblock.log_size,
            data_offset: superblock.data_offset,
        };
        if !file.alignment().is_aligned_u64(geometry.block) {
            return Err(ChunkError::Format(format!(
                "volume block {} is not a multiple of the file's alignment {}",
                geometry.block,
                file.alignment().get()
            )));
        }
        let config = Config {
            segment_size: geometry.segment_size,
            checksum_shift: superblock.checksum_shift,
            ..config
        };
        Self::start(file, superblock, geometry, config)
    }

    fn start(
        file: F,
        superblock: Superblock,
        geometry: Geometry,
        config: Config,
    ) -> Result<(Self, RecoveryReport), ChunkError> {
        // Read buffers: at most one batch's worth kept free, none larger than a batch.
        let batch = config.limits.batch_bytes;
        let pool = Pool::new(file.alignment(), batch, batch);
        let recovered = recover::recover(&file, &pool, &superblock, &geometry, &config)?;
        let shared = Arc::new(Shared {
            pool,
            file,
            geometry,
            volume: superblock.volume,
            checksum_shift: superblock.checksum_shift,
            index: RwLock::new(recovered.index),
            fenced: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            dead_bytes: std::sync::atomic::AtomicU64::new(0),
            submitted: std::sync::atomic::AtomicU64::new(0),
            checkpoints: std::sync::atomic::AtomicU64::new(0),
            usage: RwLock::new(recovered.segments.clone()),
        });
        let (sender, rx) = sync_channel(config.limits.queue.max(1));
        let (wake, wakes) = sync_channel(1);
        // Clean below one sixteenth of the segments free, up to one eighth; never below the
        // two a relocation needs (one being cleaned, one to write into).
        let segments = usize::try_from(geometry.segments).unwrap_or(usize::MAX);
        let low = (segments / 16).max(2);
        let high = (segments / 8).max(low.saturating_add(1));
        let report = recovered.report;
        let mut opens = recovered.open.into_iter();
        let client = opens.next();
        let clean = opens.next();
        let mut writer = Writer {
            shared: Arc::clone(&shared),
            config,
            rx,
            segments: recovered.segments,
            opens: [client, clean],
            stale: opens.collect(),
            incarnation: recovered.incarnation,
            sequence: recovered.sequence,
            cursor: recovered.cursor,
            superblock: superblock.clone(),
            fragments: recovered.fragments,
            poke: Some(wake.clone()),
            low_water: low,
            // Two batches' buffers kept free, each up to twice a batch.
            pool: Pool::new(
                shared.file.alignment(),
                batch.saturating_mul(2),
                batch.saturating_mul(2),
            ),
            service_ns: 0,
            return_rate: crate::writer::RATE_ONE / 2,
            received: 0,
        };
        // Recovery changed the index relative to the log: it dropped records whose flush
        // never completed, or indexed records the log never named. Both decisions live only
        // in memory until a checkpoint records them; without one, a later replay would bring
        // the dropped records back and forget the rolled-forward ones.
        if report.unflushed_dropped > 0 || report.rolled_forward > 0 {
            writer.checkpoint()?;
        }
        let superblock = writer.superblock.clone();
        let spawn_error = |e| {
            ChunkError::Device(DiskError::Io {
                op: "spawn thread",
                path: std::path::PathBuf::new(),
                source: e,
            })
        };
        let handle = std::thread::Builder::new()
            .name("mantle-chunk-writer".into())
            .spawn(move || writer.run())
            .map_err(spawn_error)?;
        let cleaner = Cleaner {
            shared: Arc::clone(&shared),
            submit: sender.clone(),
            wake: wakes,
            low,
            high,
            batch_bytes: config.limits.batch_bytes,
            batch_moves: config.limits.batch_requests,
        };
        let cleaner = std::thread::Builder::new()
            .name("mantle-chunk-cleaner".into())
            .spawn(move || cleaner.run())
            .map_err(spawn_error)?;
        let findings: SharedFindings = Arc::new(std::sync::Mutex::new(Findings::default()));
        let (wake_scrubber, scrubber) = match config.scrub_period {
            Some(period) => {
                let (wake, wakes) = sync_channel(1);
                let scrubber = Scrubber {
                    shared: Arc::clone(&shared),
                    findings: Arc::clone(&findings),
                    wake: wakes,
                    period,
                };
                let handle = std::thread::Builder::new()
                    .name("mantle-chunk-scrubber".into())
                    .spawn(move || scrubber.run())
                    .map_err(spawn_error)?;
                (Some(wake), Some(handle))
            }
            None => (None, None),
        };
        Ok((
            Self {
                shared,
                sender: Some(sender),
                writer: Some(handle),
                wake_cleaner: Some(wake),
                cleaner: Some(cleaner),
                wake_scrubber,
                scrubber,
                findings,
                superblock,
                watermarks: (low, high),
                limits: config.limits,
            },
            report,
        ))
    }

    pub fn volume_id(&self) -> u128 {
        self.superblock.volume
    }

    /// The bytes of the file or device that hold segments.
    pub fn data_span(&self) -> std::ops::Range<u64> {
        let g = &self.shared.geometry;
        let end = u64::from(g.segments)
            .saturating_mul(g.segment_size)
            .saturating_add(g.data_offset);
        g.data_offset..end
    }

    pub fn is_fenced(&self) -> bool {
        self.shared.fenced.load(Ordering::Acquire)
    }

    /// Writes a whole chunk and seals it. Returns once it is durable.
    pub fn put(&self, key: ChunkKey, data: &[u8]) -> Result<(), ChunkError> {
        self.write(key, 0, data, true, None)
    }

    /// Writes a whole chunk like [`Volume::put`], after checking it against the CRC-32C its
    /// sender computed: bytes changed on the way are refused before anything is written.
    pub fn put_checked(&self, key: ChunkKey, data: &[u8], crc32c: u32) -> Result<(), ChunkError> {
        self.write(key, 0, data, true, Some(crc32c))
    }

    /// Appends to a chunk at `offset`, which must be where the chunk ends (or an exact
    /// repeat of a fragment already there). `seal` makes this the chunk's last fragment.
    pub fn append(
        &self,
        key: ChunkKey,
        offset: u64,
        data: &[u8],
        seal: bool,
    ) -> Result<(), ChunkError> {
        self.write(key, offset, data, seal, None)
    }

    /// Appends like [`Volume::append`], after checking the bytes against the CRC-32C their
    /// sender computed.
    pub fn append_checked(
        &self,
        key: ChunkKey,
        offset: u64,
        data: &[u8],
        seal: bool,
        crc32c: u32,
    ) -> Result<(), ChunkError> {
        self.write(key, offset, data, seal, Some(crc32c))
    }

    /// Checksums the payload in the calling thread, verifies it against `expected` when the
    /// sender supplied one, and submits it.
    fn write(
        &self,
        key: ChunkKey,
        offset: u64,
        data: &[u8],
        seal: bool,
        expected: Option<u32>,
    ) -> Result<(), ChunkError> {
        let payload = Payload::new(data.to_vec(), self.shared.checksum_shift)
            .ok_or_else(|| ChunkError::Config("checksum block size".into()))?;
        if let Some(expected) = expected
            && expected != payload.crc
        {
            return Err(ChunkError::Checksum {
                key,
                expected,
                actual: payload.crc,
            });
        }
        self.submit(Op::Write {
            key,
            offset,
            payload,
            seal,
        })
    }

    /// Deletes a chunk; deleting one that does not exist succeeds.
    pub fn delete(&self, key: ChunkKey) -> Result<(), ChunkError> {
        self.submit(Op::Delete { key })
    }

    /// Writes the whole index into the log and points the superblock at it, so the next open
    /// replays nothing written before now. Returns once it is durable.
    pub fn checkpoint(&self) -> Result<(), ChunkError> {
        self.submit(Op::Checkpoint)
    }

    fn submit(&self, op: Op) -> Result<(), ChunkError> {
        if self.is_fenced() {
            return Err(ChunkError::Fenced);
        }
        let sender = self.sender.as_ref().ok_or(ChunkError::Closed)?;
        let (reply, answer) = sync_channel(1);
        self.shared.submitted.fetch_add(1, Ordering::AcqRel);
        sender
            .send(Request { op, reply })
            .map_err(|_| ChunkError::Closed)?;
        answer.recv().map_err(|_| ChunkError::Closed)?
    }

    pub fn stat(&self, key: &ChunkKey) -> Result<Option<ChunkStat>, ChunkError> {
        let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
        Ok(index.get(key).map(|e| ChunkStat {
            len: e.len(),
            sealed: e.sealed,
            fragments: e.fragments.len(),
        }))
    }

    /// Every chunk key, sorted.
    pub fn keys(&self) -> Result<Vec<ChunkKey>, ChunkError> {
        let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
        let mut keys: Vec<ChunkKey> = index.iter().map(|(k, _)| *k).collect();
        keys.sort_unstable();
        Ok(keys)
    }

    pub fn usage(&self) -> Result<Usage, ChunkError> {
        let segments = self.shared.usage.read().map_err(|_| ChunkError::Fenced)?;
        let mut usage = Usage {
            segments: u32::try_from(segments.len()).unwrap_or(u32::MAX),
            checkpoints: self.shared.checkpoints.load(Ordering::Relaxed),
            ..Usage::default()
        };
        for s in segments.iter() {
            match s.state {
                SegmentState::Free => usage.free = usage.free.saturating_add(1),
                SegmentState::Open => usage.open = usage.open.saturating_add(1),
                SegmentState::Sealed => usage.sealed = usage.sealed.saturating_add(1),
            }
            usage.live_bytes = usage.live_bytes.saturating_add(s.live);
        }
        Ok(usage)
    }

    /// Reads `len` bytes of a chunk from `offset`, verifying every byte returned against the
    /// record's checksums and every record against the identity the index expects.
    pub fn read(&self, key: &ChunkKey, offset: u64, len: u64) -> Result<Vec<u8>, ChunkError> {
        let mut out = Vec::new();
        self.read_into(key, offset, len, &mut out)?;
        Ok(out)
    }

    /// Reads like [`Volume::read`] into `out`, replacing what it held. A caller that reads in a
    /// loop with the same `out` allocates nothing once `out` is large enough.
    pub fn read_into(
        &self,
        key: &ChunkKey,
        offset: u64,
        len: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), ChunkError> {
        out.clear();
        let fragments = {
            let index = self.shared.index.read().map_err(|_| ChunkError::Fenced)?;
            let entry = index.get(key).ok_or(ChunkError::NotFound(*key))?;
            let end = offset.checked_add(len);
            if end.is_none_or(|e| e > entry.len()) {
                return Err(ChunkError::Range {
                    key: *key,
                    offset,
                    len,
                    chunk_len: entry.len(),
                });
            }
            let end = offset.saturating_add(len);
            entry
                .covering(offset, end)
                .copied()
                .collect::<Vec<Fragment>>()
        };
        let capacity =
            usize::try_from(len).map_err(|_| ChunkError::TooLarge { len, max: u64::MAX })?;
        out.reserve(capacity);
        let end = offset.saturating_add(len);
        for fragment in &fragments {
            let from = offset
                .max(fragment.chunk_offset)
                .saturating_sub(fragment.chunk_offset);
            let to = end
                .min(fragment.end())
                .saturating_sub(fragment.chunk_offset);
            read::fragment(&self.shared, key, fragment, from, to, out)?;
        }
        Ok(())
    }

    /// Verifies every stored fragment now; returns the number that failed. Failed chunks are
    /// listed by `damaged`.
    pub fn scrub(&self) -> Result<u64, ChunkError> {
        scrub_all(&self.shared, &self.findings)
    }

    /// Chunks with a fragment that failed verification, found by reads of the scrubber, for
    /// repair from another copy.
    pub fn damaged(&self) -> Result<Vec<ChunkKey>, ChunkError> {
        let findings = self.findings.lock().map_err(|_| ChunkError::Fenced)?;
        Ok(findings.damaged.iter().copied().collect())
    }

    /// Whether damage has been found since the volume opened: errors cluster, so a volume
    /// with one is scrubbed continuously and should be repaired or drained.
    pub fn at_risk(&self) -> bool {
        self.findings
            .lock()
            .map(|f| f.at_risk_since.is_some())
            .unwrap_or(true)
    }

    /// Whether the volume has more damage than repair chunk by chunk should chase, and is to
    /// be drained whole (`scrub::MAX_DAMAGED`).
    pub fn failing(&self) -> bool {
        self.findings.lock().map(|f| f.failing).unwrap_or(true)
    }

    /// Forgets a chunk once it has been repaired or its copy here deleted.
    pub fn repaired(&self, key: &ChunkKey) -> Result<(), ChunkError> {
        self.findings
            .lock()
            .map_err(|_| ChunkError::Fenced)?
            .damaged
            .remove(key);
        Ok(())
    }

    /// Cleans the `n` segments with the best cost-benefit ratio now, whatever the free
    /// space, and waits for their space to be freed.
    pub fn clean(&self, n: u32) -> Result<CleanReport, ChunkError> {
        let submit = self.sender.clone().ok_or(ChunkError::Closed)?;
        let (_, wakes) = sync_channel(1);
        let cleaner = Cleaner {
            shared: Arc::clone(&self.shared),
            submit,
            wake: wakes,
            low: self.watermarks.0,
            high: self.watermarks.1,
            batch_bytes: self.limits.batch_bytes,
            batch_moves: self.limits.batch_requests,
        };
        cleaner.clean_best(n)
    }

    /// Stops the writer after it answers what is queued. Everything acknowledged is already
    /// durable, so closing writes nothing.
    pub fn close(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        // The cleaner holds a sender to the writer, so it stops first. The writer holds a
        // wake sender of its own, so the cleaner is told to stop and then woken.
        self.shared.stopping.store(true, Ordering::Release);
        if let Some(wake) = self.wake_cleaner.take() {
            let _ = wake.try_send(());
        }
        if let Some(handle) = self.cleaner.take() {
            // A thread that unwound has already stopped; there is nothing more to wait for.
            let _ = handle.join();
        }
        if let Some(wake) = self.wake_scrubber.take() {
            let _ = wake.try_send(());
        }
        if let Some(handle) = self.scrubber.take() {
            let _ = handle.join();
        }
        self.sender.take();
        if let Some(handle) = self.writer.take() {
            let _ = handle.join();
        }
    }
}

impl<F: BlockFile + 'static> Drop for Volume<F> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Reads both superblock copies (and both possible places for B) and returns the valid one
/// with the highest sequence.
fn read_superblock<F: BlockFile>(file: &F) -> Result<Superblock, ChunkError> {
    let mut best: Option<Superblock> = None;
    let len = file.len().map_err(ChunkError::Device)?;
    let pool = Pool::new(file.alignment(), 64 << 10, 64 << 10);
    for offset in [OFFSET_A, OFFSET_B_COMPACT, OFFSET_B_STANDARD] {
        for block in [4096u64, 8192, 16384, 32768, 65536] {
            if offset.saturating_add(block) > len || !file.alignment().is_aligned_u64(block) {
                continue;
            }
            let base = Geometry {
                block,
                segment_size: 0,
                segments: 0,
                offset_b: 0,
                log_offset: 0,
                log_size: 0,
                data_offset: 0,
            };
            let Ok(Some(span)) = read_span(file, &pool, &base, 0, offset, block) else {
                continue;
            };
            if let Some(sb) = Superblock::decode(span.bytes())
                && sb.offset_of(sb.slot()) == offset
                && best.as_ref().is_none_or(|b| sb.sequence > b.sequence)
            {
                best = Some(sb);
            }
        }
    }
    best.ok_or_else(|| ChunkError::Format("no valid superblock".into()))
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// A random 128-bit volume id from the OS's generator.
fn random_id() -> Result<u128, ChunkError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| ChunkError::Config(format!("random id: {e}")))?;
    Ok(u128::from_le_bytes(bytes))
}

impl<F: BlockFile + 'static> Volume<F> {
    /// Each segment's state, incarnation, write position and live bytes.
    pub fn segments(&self) -> Result<Vec<(SegmentState, u64, u32, u64)>, ChunkError> {
        let usage = self.shared.usage.read().map_err(|_| ChunkError::Fenced)?;
        Ok(usage
            .iter()
            .map(|s| (s.state, s.incarnation, s.write_pos, s.live))
            .collect())
    }
}
