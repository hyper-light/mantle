//! A volume: format it, open it (recovering from whatever state it was left in), write and
//! delete chunks through its group-commit writer, and read them back verified.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

use mantle_disk::DiskError;
use mantle_disk::block::BlockFile;

use crate::error::ChunkError;
use crate::frame::SegmentState;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::layout::{Config, Geometry};
use crate::record;
use crate::recover::{self, RecoveryReport, read_span};
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
}

pub struct Volume<F: BlockFile + 'static> {
    shared: Arc<Shared<F>>,
    sender: Option<SyncSender<Request>>,
    writer: Option<JoinHandle<()>>,
    superblock: Superblock,
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
        let recovered = recover::recover(&file, &superblock, &geometry, &config)?;
        let shared = Arc::new(Shared {
            file,
            geometry,
            volume: superblock.volume,
            checksum_shift: superblock.checksum_shift,
            index: RwLock::new(recovered.index),
            fenced: AtomicBool::new(false),
            usage: RwLock::new(recovered.segments.clone()),
        });
        let (sender, rx) = sync_channel(config.limits.queue.max(1));
        let report = recovered.report;
        let mut writer = Writer {
            shared: Arc::clone(&shared),
            config,
            rx,
            segments: recovered.segments,
            open: recovered.open,
            incarnation: recovered.incarnation,
            sequence: recovered.sequence,
            cursor: recovered.cursor,
            superblock: superblock.clone(),
            fragments: recovered.fragments,
        };
        // Recovery changed the index relative to the log: it dropped records whose flush
        // never completed, or indexed records the log never named. Both decisions live only
        // in memory until a checkpoint records them; without one, a later replay would bring
        // the dropped records back and forget the rolled-forward ones.
        if report.unflushed_dropped > 0 || report.rolled_forward > 0 {
            writer.checkpoint()?;
        }
        let superblock = writer.superblock.clone();
        let handle = std::thread::Builder::new()
            .name("mantle-chunk-writer".into())
            .spawn(move || writer.run())
            .map_err(|e| {
                ChunkError::Device(DiskError::Io {
                    op: "spawn writer",
                    path: std::path::PathBuf::new(),
                    source: e,
                })
            })?;
        Ok((
            Self {
                shared,
                sender: Some(sender),
                writer: Some(handle),
                superblock,
            },
            report,
        ))
    }

    pub fn volume_id(&self) -> u128 {
        self.superblock.volume
    }

    pub fn is_fenced(&self) -> bool {
        self.shared.fenced.load(Ordering::Acquire)
    }

    /// Writes a whole chunk and seals it. Returns once it is durable.
    pub fn put(&self, key: ChunkKey, data: &[u8]) -> Result<(), ChunkError> {
        self.submit(Op::Write {
            key,
            offset: 0,
            data: data.to_vec(),
            seal: true,
        })
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
        self.submit(Op::Write {
            key,
            offset,
            data: data.to_vec(),
            seal,
        })
    }

    /// Deletes a chunk; deleting one that does not exist succeeds.
    pub fn delete(&self, key: ChunkKey) -> Result<(), ChunkError> {
        self.submit(Op::Delete { key })
    }

    fn submit(&self, op: Op) -> Result<(), ChunkError> {
        if self.is_fenced() {
            return Err(ChunkError::Fenced);
        }
        let sender = self.sender.as_ref().ok_or(ChunkError::Closed)?;
        let (reply, answer) = sync_channel(1);
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
        let mut out = Vec::with_capacity(capacity);
        let end = offset.saturating_add(len);
        for fragment in &fragments {
            let from = offset
                .max(fragment.chunk_offset)
                .saturating_sub(fragment.chunk_offset);
            let to = end
                .min(fragment.end())
                .saturating_sub(fragment.chunk_offset);
            self.read_fragment(key, fragment, from, to, &mut out)?;
        }
        Ok(out)
    }

    /// Appends payload bytes `[from, to)` of one fragment to `out`, verified.
    fn read_fragment(
        &self,
        key: &ChunkKey,
        fragment: &Fragment,
        from: u64,
        to: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), ChunkError> {
        let corrupt = |detail: &str| ChunkError::Corrupt {
            key: *key,
            detail: detail.to_owned(),
        };
        let geometry = &self.shared.geometry;
        let shift = self.shared.checksum_shift;
        let block_size = 1u64.checked_shl(u32::from(shift)).unwrap_or(u64::MAX);
        let prefix_len = record::prefix_len(fragment.payload_len, shift)
            .and_then(|p| u64::try_from(p).ok())
            .ok_or_else(|| corrupt("record size"))?;
        let base = geometry
            .segment_offset(fragment.segment)
            .ok_or_else(|| corrupt("segment"))?;
        let first_block = from.checked_div(block_size).unwrap_or(0);
        let last_block_end = to
            .div_ceil(block_size)
            .saturating_mul(block_size)
            .min(u64::from(fragment.payload_len));
        let payload_from = first_block.saturating_mul(block_size);
        // One read covering the header and the needed checksum blocks.
        let span_len = prefix_len.saturating_add(last_block_end);
        let bytes = match read_span(
            &self.shared.file,
            geometry,
            base,
            u64::from(fragment.offset),
            span_len,
        ) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Err(corrupt("record extends past the end of the volume")),
            Err(ChunkError::Device(e)) => return Err(corrupt(&format!("read failed: {e}"))),
            Err(e) => return Err(e),
        };
        let prefix =
            record::decode_prefix(&bytes).ok_or_else(|| corrupt("record header did not verify"))?;
        let h = &prefix.header;
        if h.key != *key
            || h.volume != self.shared.volume
            || h.segment != fragment.segment
            || h.incarnation != fragment.incarnation
            || h.sequence != fragment.sequence
            || h.chunk_offset != fragment.chunk_offset
            || h.payload_len != fragment.payload_len
        {
            return Err(corrupt("record identity does not match the index"));
        }
        let start = usize::try_from(prefix_len.saturating_add(payload_from))
            .map_err(|_| corrupt("offset"))?;
        let stop = usize::try_from(prefix_len.saturating_add(last_block_end))
            .map_err(|_| corrupt("offset"))?;
        let blocks = bytes
            .get(start..stop)
            .ok_or_else(|| corrupt("short record"))?;
        let first = u32::try_from(first_block).map_err(|_| corrupt("offset"))?;
        if !record::verify(&prefix, first, blocks) {
            return Err(corrupt("payload checksum mismatch"));
        }
        let skip =
            usize::try_from(from.saturating_sub(payload_from)).map_err(|_| corrupt("offset"))?;
        let take = usize::try_from(to.saturating_sub(from)).map_err(|_| corrupt("offset"))?;
        let wanted = blocks
            .get(skip..skip.saturating_add(take))
            .ok_or_else(|| corrupt("short record"))?;
        out.extend_from_slice(wanted);
        Ok(())
    }

    /// Stops the writer after it answers what is queued. Everything acknowledged is already
    /// durable, so closing writes nothing.
    pub fn close(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(handle) = self.writer.take() {
            // A writer that unwound has already stopped; there is nothing more to wait for.
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
            let Ok(Some(bytes)) = read_span(file, &base, 0, offset, block) else {
                continue;
            };
            if let Some(sb) = Superblock::decode(&bytes)
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
