//! Reads for an owner on a runtime task (docs/design/node.md §1.3, chunk-store.md §7): each read
//! handed to the device's issuer as one batch and taken once its answer comes, verified exactly as
//! [`Volume::read`] verifies one, so the owner's thread never waits on the device. A storage node
//! serves its chunks' reads this way from the shard that also runs its transport, where a read
//! that waited would hold every connection the shard carries.
//!
//! A [`Reader`] holds its reads to the volume's measured [`Reads`], as the volume's gate holds
//! reads made on threads: at most `depth` at the device and their buffers within `bytes` (or one
//! read alone), `waiting` more kept in the order they came and sent as turns come back, and any
//! past them refused with `Busy`, before anything is taken for them, so the caller reads another
//! copy.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment, Pool};
use hyper_block::issuer::Attached;

use crate::error::ChunkError;
use crate::index::Fragment;
use crate::key::ChunkKey;
use crate::layout::{Geometry, Reads};
use crate::read::{self, Plan, ReadStats};
use crate::volume::Volume;

/// A read asked of a [`Reader`], named again in its answer.
pub type ReadId = u64;

/// A read's answer: its id, and its bytes or why not.
pub type ReadAnswer = (ReadId, Result<Vec<u8>, ChunkError>);

/// A volume's reads for an owner on a runtime task ([`Volume::reader`]).
#[derive(Debug)]
pub struct Reader {
    attached: Attached,
    pool: Pool,
    reads: Reads,
    next: ReadId,
    /// Reads at the device, and the bytes their buffers hold.
    out: Vec<Out>,
    bytes: u64,
    /// Reads asked for while the device held its depth or its bytes, in the order they came.
    waiting: VecDeque<Asked>,
    /// Reads answered without the device: a chunk a waiting read found gone when its turn came.
    answered: VecDeque<ReadAnswer>,
    stats: ReadStats,
}

#[derive(Debug, Clone, Copy)]
struct Asked {
    id: ReadId,
    key: ChunkKey,
    offset: u64,
    len: u64,
}

/// A read at the device: the issuer's batch it went as, and how its transfers verify.
#[derive(Debug)]
struct Out {
    id: ReadId,
    batch: u64,
    key: ChunkKey,
    len: u64,
    cost: u64,
    pieces: Vec<Piece>,
}

/// One fragment's part of a read: the payload range it gives and where its spans lie among the
/// batch's transfers.
#[derive(Debug)]
struct Piece {
    fragment: Fragment,
    plan: Plan,
    from: u64,
    to: u64,
    head: Spot,
    apart: Option<Spot>,
}

/// A span's bytes within its transfer's buffer.
#[derive(Debug, Clone, Copy)]
struct Spot {
    transfer: usize,
    skip: usize,
    len: usize,
}

/// A span to read: from `start`, `len` bytes, read as the device's blocks around it.
#[derive(Debug, Clone, Copy)]
struct Aligned {
    at: u64,
    span: usize,
    skip: usize,
    len: usize,
}

impl Reader {
    pub(crate) fn new(
        attached: Attached,
        align: Alignment,
        batch_bytes: usize,
        reads: Reads,
    ) -> Self {
        Self {
            attached,
            pool: Pool::new(align, batch_bytes, batch_bytes),
            reads,
            next: 0,
            out: Vec::new(),
            bytes: 0,
            waiting: VecDeque::new(),
            answered: VecDeque::new(),
            stats: ReadStats::default(),
        }
    }

    /// Asks for bytes `[offset, offset + len)` of `key`'s chunk on `volume`, the volume this
    /// reader was made by: sent to the device now when a turn is free and its buffers fit, or it
    /// goes alone; kept behind the reads waiting when one more may wait; refused with `Busy`
    /// otherwise. `NotFound` and `Range` are answered at once. Its bytes come from
    /// [`Self::poll_next`], under the id returned.
    pub fn submit<F: BlockFile + Sync + 'static>(
        &mut self,
        volume: &Volume<F>,
        key: &ChunkKey,
        offset: u64,
        len: u64,
    ) -> Result<ReadId, ChunkError> {
        let fragments = volume.fragments(key, offset, len)?;
        let id = self.next;
        let asked = Asked {
            id,
            key: *key,
            offset,
            len,
        };
        if self.waiting.is_empty() {
            let planned = plan(volume, &asked, &fragments)?;
            if self.fits(planned.cost) {
                self.send(asked, planned)?;
                self.next = id.wrapping_add(1);
                return Ok(id);
            }
        }
        if self.waiting.len() < self.reads.waiting {
            self.waiting.push_back(asked);
            self.next = id.wrapping_add(1);
            return Ok(id);
        }
        self.stats.refused = self.stats.refused.saturating_add(1);
        Err(ChunkError::Busy)
    }

    /// The next read answered, verified: its id and its bytes, or why not (`Corrupt` for any
    /// device error or failed check, as [`Volume::read`] answers, so the caller reads another
    /// copy). `Ready(None)` when no read is out or waiting. `Pending` registers the caller's task
    /// to be woken when one is answered.
    pub fn poll_next<F: BlockFile + Sync + 'static>(
        &mut self,
        volume: &Volume<F>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ReadAnswer>> {
        loop {
            if let Some(answered) = self.answered.pop_front() {
                return Poll::Ready(Some(answered));
            }
            if self.out.is_empty() {
                if self.waiting.is_empty() {
                    return Poll::Ready(None);
                }
                self.send_waiting(volume);
                continue;
            }
            let numbered = {
                let mut wait = pin!(self.attached.answer_async());
                match wait.as_mut().poll(cx) {
                    Poll::Ready(numbered) => numbered,
                    Poll::Pending => return Poll::Pending,
                }
            };
            match numbered {
                Ok((batch, answer)) => {
                    if let Some(taken) = self.take(volume, batch, answer) {
                        self.send_waiting(volume);
                        return Poll::Ready(Some(taken));
                    }
                }
                // The attachment answers nothing more: every read out or waiting fails, as a
                // device error does, and the caller reads other copies.
                Err(error) => {
                    let detail = format!("read failed: {error}");
                    for out in std::mem::take(&mut self.out) {
                        self.answered
                            .push_back((out.id, Err(corrupt(&out.key, &detail))));
                    }
                    for asked in std::mem::take(&mut self.waiting) {
                        self.answered
                            .push_back((asked.id, Err(corrupt(&asked.key, &detail))));
                    }
                    self.bytes = 0;
                }
            }
        }
    }

    /// The depth, bytes and waiting this reader holds its reads to: the volume's.
    pub fn reads(&self) -> Reads {
        self.reads
    }

    /// Reads at the device now.
    pub fn out(&self) -> usize {
        self.out.len()
    }

    /// Reads kept waiting for a turn.
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    /// The most reads and bytes this reader has had at the device, and the reads it refused.
    pub fn stats(&self) -> ReadStats {
        self.stats
    }

    /// Whether a read whose buffers hold `cost` bytes may go to the device now.
    fn fits(&self, cost: u64) -> bool {
        self.out.len() < self.reads.depth
            && (self.out.is_empty() || self.bytes.saturating_add(cost) <= self.reads.bytes)
    }

    /// Hands a planned read to the device as one batch.
    fn send(&mut self, asked: Asked, planned: Planned) -> Result<(), ChunkError> {
        let mut transfers: Vec<(AlignedBuf, u64)> = Vec::with_capacity(planned.spans.len());
        for span in &planned.spans {
            let taken = self.pool.take(span.span).and_then(|mut buf| {
                buf.set_len(span.span)?;
                Ok(buf)
            });
            match taken {
                Ok(buf) => transfers.push((buf, span.at)),
                Err(error) => {
                    for (buf, _) in transfers {
                        self.pool.give(buf);
                    }
                    return Err(ChunkError::Device(DiskError::from(error)));
                }
            }
        }
        let batch = self
            .attached
            .submit_reads(transfers)
            .map_err(ChunkError::Device)?;
        self.bytes = self.bytes.saturating_add(planned.cost);
        self.stats.most_at_device = self
            .stats
            .most_at_device
            .max(self.out.len().saturating_add(1));
        self.stats.most_bytes = self.stats.most_bytes.max(self.bytes);
        self.out.push(Out {
            id: asked.id,
            batch,
            key: asked.key,
            len: asked.len,
            cost: planned.cost,
            pieces: planned.pieces,
        });
        Ok(())
    }

    /// Sends the reads waiting, in order, while the head fits; one that finds its chunk gone or
    /// its range past the end, or fails to go, is answered with why.
    fn send_waiting<F: BlockFile + Sync + 'static>(&mut self, volume: &Volume<F>) {
        while let Some(asked) = self.waiting.front().copied() {
            let planned = volume
                .fragments(&asked.key, asked.offset, asked.len)
                .and_then(|fragments| plan(volume, &asked, &fragments));
            let planned = match planned {
                Ok(planned) => planned,
                Err(error) => {
                    self.waiting.pop_front();
                    self.answered.push_back((asked.id, Err(error)));
                    continue;
                }
            };
            if !self.fits(planned.cost) {
                return;
            }
            self.waiting.pop_front();
            if let Err(error) = self.send(asked, planned) {
                self.answered.push_back((asked.id, Err(error)));
            }
        }
    }

    /// Takes a batch's answer: the read it carried, verified, its buffers given back to the pool.
    fn take<F: BlockFile + Sync + 'static>(
        &mut self,
        volume: &Volume<F>,
        batch: u64,
        answer: Result<Vec<(AlignedBuf, u64)>, DiskError>,
    ) -> Option<ReadAnswer> {
        let at = self.out.iter().position(|o| o.batch == batch)?;
        let out = self.out.swap_remove(at);
        self.bytes = self.bytes.saturating_sub(out.cost);
        let result = match answer {
            Ok(transfers) => {
                let checked = check(volume, &out, &transfers);
                for (buf, _) in transfers {
                    self.pool.give(buf);
                }
                checked
            }
            Err(DiskError::ShortRead { .. }) => Err(corrupt(
                &out.key,
                "record extends past the end of the volume",
            )),
            Err(error) => Err(corrupt(&out.key, &format!("read failed: {error}"))),
        };
        Some((out.id, result))
    }
}

/// A read's plan: its pieces, the spans its batch reads, and the bytes their buffers hold.
#[derive(Debug)]
struct Planned {
    pieces: Vec<Piece>,
    spans: Vec<Aligned>,
    cost: u64,
}

/// Plans the read of `asked` from `fragments`, the fragments that hold its range: each
/// fragment's header and checksum table, with its checksum blocks or apart from them, as
/// [`Volume::read`] reads them.
fn plan<F: BlockFile + Sync + 'static>(
    volume: &Volume<F>,
    asked: &Asked,
    fragments: &[Fragment],
) -> Result<Planned, ChunkError> {
    let shared = volume.shared();
    let geometry = &shared.geometry;
    let end = asked.offset.saturating_add(asked.len);
    let mut pieces = Vec::with_capacity(fragments.len());
    let mut spans: Vec<Aligned> = Vec::with_capacity(fragments.len().saturating_mul(2));
    let mut cost = 0u64;
    for fragment in fragments {
        let bad = |detail: &str| corrupt(&asked.key, detail);
        let from = asked
            .offset
            .max(fragment.chunk_offset)
            .saturating_sub(fragment.chunk_offset);
        let to = end
            .min(fragment.end())
            .saturating_sub(fragment.chunk_offset);
        let plan = Plan::new(
            shared.checksum_shift,
            fragment,
            from,
            to,
            shared.reads.gap(),
        )
        .ok_or_else(|| bad("record size"))?;
        let base = geometry
            .segment_offset(fragment.segment)
            .ok_or_else(|| bad("segment"))?;
        let record_at = u64::from(fragment.offset);
        let head_len = if plan.apart {
            plan.prefix
        } else {
            plan.bytes()
        };
        let mut add = |offset: u64, len: u64| -> Result<Spot, ChunkError> {
            let aligned = align(geometry, base, offset, len).ok_or_else(|| bad("offset"))?;
            cost = cost.saturating_add(u64::try_from(aligned.span).unwrap_or(u64::MAX));
            spans.push(aligned);
            Ok(Spot {
                transfer: spans.len().saturating_sub(1),
                skip: aligned.skip,
                len: aligned.len,
            })
        };
        let head = add(record_at, head_len)?;
        let apart = if plan.apart {
            let (first, last) = plan.blocks;
            let at = record_at
                .checked_add(plan.prefix)
                .and_then(|at| at.checked_add(first))
                .ok_or_else(|| bad("offset"))?;
            Some(add(at, last.saturating_sub(first))?)
        } else {
            None
        };
        pieces.push(Piece {
            fragment: *fragment,
            plan,
            from,
            to,
            head,
            apart,
        });
    }
    Ok(Planned {
        pieces,
        spans,
        cost,
    })
}

/// The device blocks around `len` bytes at `offset` within the segment at `base`.
fn align(geometry: &Geometry, base: u64, offset: u64, len: u64) -> Option<Aligned> {
    let block = geometry.block;
    let start = base.checked_add(offset)?;
    let end = start.checked_add(len)?;
    let at = start.checked_div(block)?.checked_mul(block)?;
    let stop = end.checked_next_multiple_of(block)?;
    Some(Aligned {
        at,
        span: usize::try_from(stop.checked_sub(at)?).ok()?,
        skip: usize::try_from(start.checked_sub(at)?).ok()?,
        len: usize::try_from(len).ok()?,
    })
}

/// A read's bytes from its batch's transfers, every piece verified against the index and the
/// record's checksums.
fn check<F: BlockFile + Sync + 'static>(
    volume: &Volume<F>,
    out: &Out,
    transfers: &[(AlignedBuf, u64)],
) -> Result<Vec<u8>, ChunkError> {
    let len = usize::try_from(out.len).map_err(|_| ChunkError::TooLarge {
        len: out.len,
        max: u64::MAX,
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| ChunkError::TooLarge {
            len: out.len,
            max: 0,
        })?;
    let spot = |spot: Spot| -> Result<&[u8], ChunkError> {
        transfers
            .get(spot.transfer)
            .and_then(|(buf, _)| {
                buf.as_slice()
                    .get(spot.skip..spot.skip.saturating_add(spot.len))
            })
            .ok_or_else(|| corrupt(&out.key, "short record"))
    };
    for piece in &out.pieces {
        let head = spot(piece.head)?;
        let apart = piece.apart.map(spot).transpose()?;
        read::check(
            volume.shared(),
            &out.key,
            &piece.fragment,
            &piece.plan,
            (piece.from, piece.to),
            (head, apart),
            &mut bytes,
        )?;
    }
    Ok(bytes)
}

fn corrupt(key: &ChunkKey, detail: &str) -> ChunkError {
    ChunkError::Corrupt {
        key: *key,
        detail: detail.to_owned(),
    }
}
