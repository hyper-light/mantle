//! Repair and rebalancing of one block (docs/design/repair.md): what the block needs as the cell's
//! facts stand ([`plan`]), and the rebuilding of one of its chunks on another volume
//! ([`Repairer`]): its sources read, the chunk rebuilt and checked against the block's CRC-32C
//! before anything is written, written to a volume placement offers, and named there by the Block
//! range's compare-and-swap, which a block changed meanwhile refuses. As the put and get drivers
//! do, a repairer names its requests and moves on with their answers, doing no I/O.
//!
//! A chunk is rebuilt only from sources that verify: a copy is the whole block, so its bytes are
//! checked against the block's CRC-32C; a coded chunk is cut or coded from the block a decode of
//! `data` chunks gives, once that block matches the CRC, never taken from bytes read, whose padding
//! past the block the CRC does not cover. A decode that fails though every chunk read verified at its volume
//! reads every readable chunk and tries every set of `data` of them, at most `C(read, data)`, the
//! first whose block matches the CRC being one with no wrong chunk; where none does, the repair is
//! refused, and nothing is written (node.md §5.6: repair never spreads corruption).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use mantle_ec::{Code, EcError};
use mantle_meta::block;
use mantle_meta::record::{BlockHeader, ChunkPlace};

/// How a volume stands to a block's repair, as the cell's facts judge it (docs/design/repair.md
/// §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// Its node is seen and the volume holds its chunks: read from it.
    Readable,
    /// Readable, its node leaving the cell's storage: what it holds is moved off.
    Leaving,
    /// Its node is away. What it holds waits for it, and is rebuilt elsewhere once `due`, the
    /// volume keeping its copy for when its node is back.
    Away { due: bool },
    /// Written off, or not in the table: what it held is gone.
    Gone,
}

impl Reach {
    fn readable(self) -> bool {
        matches!(self, Self::Readable | Self::Leaving)
    }
}

/// A volume as a block's plan judges it: its failure domain, a number the cell's facts give each
/// domain, and its reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Volume {
    pub volume: u128,
    pub domain: u32,
    pub reach: Reach,
}

/// The cell's volumes as the plans judge them, and the domains that take a chunk now: those with a
/// volume that is active and whose node is seen.
#[derive(Debug, Clone, Default)]
pub struct Judged {
    volumes: Vec<Volume>,
    takers: BTreeSet<u32>,
}

impl Judged {
    pub fn new(mut volumes: Vec<Volume>, takers: BTreeSet<u32>) -> Self {
        volumes.sort_unstable_by_key(|v| v.volume);
        Self { volumes, takers }
    }

    /// `volume` as judged; none for one the table does not hold, which is gone.
    pub fn of(&self, volume: u128) -> Option<Volume> {
        let at = self
            .volumes
            .binary_search_by_key(&volume, |v| v.volume)
            .ok()?;
        self.volumes.get(at).copied()
    }
}

/// What a block needs (docs/design/repair.md §3–§4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Every chunk reachable, none on a leaving volume, its domains as the rule allows.
    Whole,
    /// A chunk waits: its every place is away, none of them due yet, or no volume that takes
    /// chunks is free of the block's places to rebuild it on, and no place another of its chunk
    /// holds makes room: it waits for its node, or for a volume to join.
    Waiting,
    /// Fewer chunks reachable than rebuild the block: until a node is back, or for good, `lost`,
    /// when every place of a chunk it lacks is gone.
    Unreadable { lost: bool },
    /// A change of the block's rows alone: a kept place on a gone volume forgotten, or a chunk's
    /// own place on a gone volume moved onto a readable place kept beside it.
    Command(block::Command),
    /// Chunk `index` rebuilt on another volume and named there by `commit`; where `spread`, on a
    /// volume of a domain holding no place of the block.
    Rebuild {
        index: u16,
        commit: Commit,
        spread: bool,
    },
}

/// How a rebuilt chunk is named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commit {
    /// The chunk's own place `from` is out of reach: the chunk lives where it was rebuilt, and
    /// `from` keeps it, reachable again once its node is back (`block::Command::Repair`).
    Keep { from: u128 },
    /// The place on `from`, gone, leaving, kept where another place is wanted, or sharing a
    /// domain with another chunk, moves (`block::Command::Move`). Where it does not spread, a
    /// place of the chunk on a volume that stays, read and found to be the chunk, takes it without
    /// a write.
    Move { from: u128 },
}

/// A block's plan, with how many of its chunks are reachable and its margin: those past the data
/// chunks rebuilding it needs, which orders repair (Ford et al., OSDI 2010, §5.1: "These queues
/// prioritize reconstruction of stripes which have lost the most chunks").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub plan: Plan,
    pub reachable: usize,
    pub margin: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RepairError {
    /// The block's row is not one a block's chunks can have.
    #[error("block {0:032x}: a row that is not a block's")]
    Inconsistent(u128),
    /// Fewer of the block's chunks read than rebuild it.
    #[error("block {0:032x}: too few of its chunks could be read")]
    Unreadable(u128),
    /// No set of the chunks read rebuilds a block that matches its CRC-32C, or a rebuilt parity
    /// chunk does not decode back to it: nothing is written.
    #[error("block {0:032x}: no chunks read rebuild it whole")]
    Corrupt(u128),
    /// No volume offered took the rebuilt chunk; `full` where every one refused it for want of
    /// room cleaning cannot make.
    #[error("block {block:032x}: no volume took its rebuilt chunk")]
    Unplaced { block: u128, full: bool },
    /// The block holds as many places as the cell's volumes.
    #[error("block {0:032x}: as many places as the cell holds volumes")]
    Full(u128),
    #[error("the Block range answered {0:?}")]
    Block(block::Outcome),
    #[error("an answer of another kind than its request")]
    Mismatch,
    #[error("an answer to no request in flight: {0}")]
    Unknown(Id),
    #[error("a length past what the repair can address")]
    Overflow,
    #[error(transparent)]
    Code(#[from] EcError),
}

/// What a block's plan judges of its places: each place's volume judged, and each chunk's places.
struct Seen<'a> {
    places: &'a [ChunkPlace],
    judged: Vec<Option<Volume>>,
    width: usize,
}

impl<'a> Seen<'a> {
    fn new(
        header: &BlockHeader,
        places: &'a [ChunkPlace],
        cell: &Judged,
        block: u128,
    ) -> Result<Self, RepairError> {
        let width = usize::from(header.data)
            .checked_add(usize::from(header.parity))
            .ok_or(RepairError::Overflow)?;
        let fits = places.len() >= width
            && places.iter().enumerate().all(|(p, place)| {
                place.key.block == block
                    && usize::from(place.key.index) < width
                    && (p >= width || usize::from(place.key.index) == p)
            });
        if header.data == 0 || !fits {
            return Err(RepairError::Inconsistent(block));
        }
        let judged = places.iter().map(|p| cell.of(p.volume)).collect();
        Ok(Self {
            places,
            judged,
            width,
        })
    }

    fn index(&self, p: usize) -> usize {
        self.places
            .get(p)
            .map_or(usize::MAX, |pl| usize::from(pl.key.index))
    }

    fn reach(&self, p: usize) -> Reach {
        self.judged
            .get(p)
            .copied()
            .flatten()
            .map_or(Reach::Gone, |v| v.reach)
    }

    /// The domain of its volume; none for a volume the table does not hold.
    fn domain(&self, p: usize) -> Option<u32> {
        self.judged.get(p).copied().flatten().map(|v| v.domain)
    }

    fn volume(&self, p: usize) -> u128 {
        self.places.get(p).map_or(0, |pl| pl.volume)
    }

    /// The places of chunk `i`: its own first, then those kept beside it.
    fn of(&self, i: usize) -> impl Iterator<Item = usize> + '_ {
        (0..self.places.len()).filter(move |&p| self.index(p) == i)
    }

    fn readable(&self, i: usize) -> bool {
        self.of(i).any(|p| self.reach(p).readable())
    }
}

/// What `places`, `header`'s block's places (`block::places`: each chunk's own in index order,
/// then those kept), need as `cell` judges their volumes (docs/design/repair.md §3–§4), in this
/// order: a kept place on a gone volume forgotten; a chunk's own place on a gone volume moved onto
/// a readable one kept beside it; nothing more while too few chunks are reachable to rebuild any;
/// a chunk none of whose places is reachable rebuilt, at once where each of its places is gone,
/// and where one is away once that one is due; a place on a leaving volume moved onto another of
/// its chunk checked to hold it, or rebuilt elsewhere; and a chunk sharing its domain with another
/// moved to a domain that takes chunks and holds none of the block's, while there is one. A
/// rebuild that may write, every volume that takes chunks holding a place of the block, first lets
/// go a kept place another place of its chunk is checked to hold, and with none to let go waits,
/// unless a place of its chunk that stays may take it with no write.
pub fn plan(
    block: u128,
    header: &BlockHeader,
    places: &[ChunkPlace],
    cell: &Judged,
) -> Result<Planned, RepairError> {
    let seen = Seen::new(header, places, cell, block)?;
    let width = seen.width;
    let data = usize::from(header.data);
    let reachable = (0..width).filter(|&i| seen.readable(i)).count();
    let margin = i64::try_from(reachable)
        .ok()
        .zip(i64::try_from(data).ok())
        .and_then(|(r, d)| r.checked_sub(d))
        .ok_or(RepairError::Overflow)?;
    let planned = |plan| {
        Ok(Planned {
            plan,
            reachable,
            margin,
        })
    };
    // A kept place on a gone volume keeps nothing.
    if let Some(p) = (width..places.len()).find(|&p| seen.reach(p) == Reach::Gone) {
        return planned(Plan::Command(block::Command::Unkeep {
            block,
            volume: seen.volume(p),
        }));
    }
    // A chunk's own place gone while a place kept beside it reads: the kept place becomes its own.
    for i in 0..width {
        if seen.reach(i) != Reach::Gone {
            continue;
        }
        if let Some(p) = seen.of(i).find(|&p| p >= width && seen.reach(p).readable())
            && let Some(own) = places.get(i)
        {
            return planned(Plan::Command(block::Command::Move {
                chunk: own.key,
                from: own.volume,
                to: seen.volume(p),
            }));
        }
    }
    if reachable < data {
        let lost = (0..width)
            .filter(|&i| !seen.readable(i))
            .any(|i| seen.of(i).all(|p| seen.reach(p) == Reach::Gone));
        return planned(Plan::Unreadable { lost });
    }
    // A rebuild that may write needs a volume that takes chunks and holds no place of the block.
    // Where every one holds a place already, a place kept beside another of its chunk on a volume
    // that takes chunks makes room first: that other read and found to be the chunk, the kept
    // place is let go (`Commit::Move`'s keepers), never one whose chunk is held nowhere checked.
    let free = cell
        .volumes
        .iter()
        .any(|v| v.reach == Reach::Readable && !places.iter().any(|p| p.volume == v.volume));
    let room = (width..places.len()).find(|&p| {
        seen.reach(p) == Reach::Readable
            && seen
                .of(seen.index(p))
                .any(|q| q != p && seen.reach(q) == Reach::Readable)
    });
    // `checks`: a place of the chunk that stays may take it with no write (`Commit::Move`'s
    // keepers), so the rebuild is worth beginning with nowhere to write.
    let writing = |plan: Plan, checks: bool| -> Result<Planned, RepairError> {
        match room {
            _ if free => planned(plan),
            Some(p) => planned(Plan::Rebuild {
                index: index_u16(seen.index(p))?,
                commit: Commit::Move {
                    from: seen.volume(p),
                },
                spread: false,
            }),
            None if checks => planned(plan),
            None => planned(Plan::Waiting),
        }
    };
    let mut waiting = false;
    for i in 0..width {
        if seen.readable(i) {
            continue;
        }
        let gone = seen.of(i).all(|p| seen.reach(p) == Reach::Gone);
        let due = gone
            || seen
                .of(i)
                .any(|p| seen.reach(p) == Reach::Away { due: true });
        if !due {
            waiting = true;
            continue;
        }
        let from = seen.volume(i);
        let commit = match seen.reach(i) {
            Reach::Away { .. } => Commit::Keep { from },
            _ => Commit::Move { from },
        };
        return writing(
            Plan::Rebuild {
                index: index_u16(i)?,
                commit,
                spread: false,
            },
            false,
        );
    }
    // A place on a leaving volume moves: onto another place of its chunk that stays, once that is
    // read and found to be the chunk, or onto a volume the chunk is rebuilt and checked on. A copy
    // let go unchecked could be its chunk's only sound one where another place's bytes went wrong
    // in a way their volume cannot see.
    for i in 0..width {
        if let Some(first) = seen.of(i).find(|&p| seen.reach(p) == Reach::Leaving) {
            // Its other places may all hold bytes that are not the chunk: the rebuild may write.
            let stays = seen.of(i).any(|p| seen.reach(p) == Reach::Readable);
            return writing(
                Plan::Rebuild {
                    index: index_u16(i)?,
                    commit: Commit::Move {
                        from: seen.volume(first),
                    },
                    spread: false,
                },
                stays,
            );
        }
    }
    // The domain rule (architecture §7; research/04 R2): no domain the only one to hold two of
    // the block's chunks while a domain that takes chunks holds none of them.
    let held: BTreeSet<u32> = (0..places.len())
        .filter(|&p| seen.reach(p) != Reach::Gone)
        .filter_map(|p| seen.domain(p))
        .collect();
    if cell.takers.iter().any(|d| !held.contains(d)) {
        let mut sole: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        for i in 0..width {
            let domains: BTreeSet<u32> = seen
                .of(i)
                .filter(|&p| seen.reach(p).readable())
                .filter_map(|p| seen.domain(p))
                .collect();
            if let (1, Some(&d)) = (domains.len(), domains.first()) {
                sole.entry(d).or_default().push(i);
            }
        }
        if let Some(chunks) = sole.values().find(|chunks| chunks.len() >= 2)
            && let Some(&i) = chunks.last()
            && let Some(from) = seen.of(i).find(|&p| seen.reach(p).readable())
        {
            return planned(Plan::Rebuild {
                index: index_u16(i)?,
                commit: Commit::Move {
                    from: seen.volume(from),
                },
                spread: true,
            });
        }
    }
    planned(if waiting { Plan::Waiting } else { Plan::Whole })
}

fn index_u16(i: usize) -> Result<u16, RepairError> {
    u16::try_from(i).map_err(|_| RepairError::Overflow)
}

/// A request's name among those in flight.
pub type Id = u64;

/// A repairer's request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// The whole of a chunk, from its volume, which verifies it.
    Chunk {
        volume: u128,
        key: mantle_chunk::ChunkKey,
        len: u64,
    },
    /// Volumes that take a chunk of `block`, in the order to try them, none of `avoid`, those of
    /// domains holding none of `avoid` first; only those where `spread`.
    Place {
        block: u128,
        avoid: Vec<u128>,
        spread: bool,
    },
    /// The rebuilt chunk to its volume, with its CRC-32C, which the volume checks before it
    /// writes.
    Put {
        volume: u128,
        key: mantle_chunk::ChunkKey,
        bytes: Bytes,
        crc: u32,
    },
    /// The compare-and-swap that names the rebuilt chunk, to the Block range.
    Block(block::Command),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Chunk(Bytes),
    /// The volume did not give the bytes: it failed, did not answer, or found them corrupt.
    Unreadable,
    Volumes(Vec<u128>),
    /// The chunk is durable on its volume.
    Stored,
    /// The volume refused it, or did not answer; `full` where for want of room cleaning cannot
    /// make.
    Refused {
        full: bool,
    },
    Block(block::Outcome),
}

/// What became of a repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repaired {
    /// The rebuilt chunk is named where it was written.
    Done { volume: u128 },
    /// Nothing written: a place of the chunk, read and found to be it, took the place let go, or a
    /// place found not to be the chunk was let go for `volume`'s, found to be it.
    Verified { volume: u128 },
    /// The block changed since it was read: it is planned again.
    Changed,
    /// The block was released or removed meanwhile: nothing to keep.
    Released,
}

#[derive(Debug, Clone, Copy)]
enum Asked {
    /// A source chunk, read from the place at this position.
    Source(usize),
    Place,
    Put(usize),
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Reading,
    Placing,
    Writing,
    Committing,
    Over,
}

/// The rebuilding of one chunk of one block (docs/design/repair.md §3).
pub struct Repairer {
    block: u128,
    header: BlockHeader,
    places: Vec<ChunkPlace>,
    /// The positions of the places sources are read from, in the order to try them.
    sources: Vec<usize>,
    index: u16,
    commit: Commit,
    spread: bool,
    /// The most places the block may hold: the cell's volumes.
    most: u32,
    phase: Phase,
    /// Chunks read, by index, and the places tried.
    got: BTreeMap<usize, Bytes>,
    /// Copies of a chunk `got` holds, read from its other places once a decode failed, where they
    /// hold other bytes: a chunk's places are candidates apart, one of them perhaps wrong.
    others: Vec<(usize, Bytes)>,
    tried: BTreeSet<usize>,
    /// Places of the chunk, other than the one a move lets go, on volumes that stay: read first,
    /// and one found to be the chunk takes the place let go without a write.
    keepers: Vec<usize>,
    /// The bytes read from the chunk's own places, by position: each is then known to be the
    /// chunk or not.
    reads: Vec<(usize, Bytes)>,
    /// The volume the chunk is named on, once committing, and whether it was written there.
    to: Option<u128>,
    stored: bool,
    /// Source reads out.
    reading: usize,
    /// Whether a decode of `data` chunks failed the block's CRC-32C: every readable chunk is read
    /// then, and each left out in turn.
    suspect: bool,
    /// Whether a copy read whole was not the block: refused for its CRC-32C, never a source.
    mismatched: bool,
    rebuilt: Option<(Bytes, u32)>,
    offered: Vec<u128>,
    next: usize,
    full: bool,
    ready: VecDeque<(Id, Request)>,
    asked: BTreeMap<Id, Asked>,
    next_id: Id,
    outcome: Option<Result<Repaired, RepairError>>,
}

impl Repairer {
    /// The rebuilding of chunk `index` of `block`, whose header and places (`block::places`) the
    /// plan read and judged, named by `commit`, on a volume of a domain holding none of the block's
    /// places where `spread`; the block holding at most `most` places.
    pub fn new(
        block: u128,
        header: BlockHeader,
        places: Vec<ChunkPlace>,
        cell: &Judged,
        (index, commit, spread): (u16, Commit, bool),
        most: u32,
    ) -> Result<Self, RepairError> {
        let seen = Seen::new(&header, &places, cell, block)?;
        // Sources: readable places, the chunk itself first where it reads (a copy, or the coded
        // chunk read whole), its places on volumes that stay first where a move lets one go.
        let target = usize::from(index);
        let keepers: Vec<usize> = match commit {
            Commit::Move { from } if !spread => seen
                .of(target)
                .filter(|&p| seen.volume(p) != from && seen.reach(p) == Reach::Readable)
                .collect(),
            _ => Vec::new(),
        };
        let mut sources: Vec<usize> = (0..places.len())
            .filter(|&p| seen.reach(p).readable())
            .collect();
        sources.sort_by_key(|&p| {
            (
                !keepers.contains(&p),
                seen.index(p) != target,
                seen.index(p),
                p,
            )
        });
        drop(seen);
        let mut repairer = Self {
            block,
            header,
            places,
            sources,
            index,
            commit,
            spread,
            most,
            phase: Phase::Reading,
            got: BTreeMap::new(),
            others: Vec::new(),
            tried: BTreeSet::new(),
            keepers,
            reads: Vec::new(),
            to: None,
            stored: false,
            reading: 0,
            suspect: false,
            mismatched: false,
            rebuilt: None,
            offered: Vec::new(),
            next: 0,
            full: true,
            ready: VecDeque::new(),
            asked: BTreeMap::new(),
            next_id: 0,
            outcome: None,
        };
        let started = repairer.read_more();
        repairer.guard(started);
        Ok(repairer)
    }

    /// The next request to send, if any.
    pub fn poll(&mut self) -> Option<(Id, Request)> {
        self.ready.pop_front()
    }

    /// Takes the answer to request `id`.
    pub fn answer(&mut self, id: Id, answer: Answer) -> Result<(), RepairError> {
        if self.outcome.is_some() {
            return Err(RepairError::Unknown(id));
        }
        let asked = self.asked.remove(&id).ok_or(RepairError::Unknown(id))?;
        let stepped = self.answered(asked, answer);
        self.guard(stepped);
        Ok(())
    }

    /// The repair's end, once it has one.
    pub fn outcome(&self) -> Option<&Result<Repaired, RepairError>> {
        self.outcome.as_ref()
    }

    /// The block and chunk it rebuilds.
    pub fn chunk(&self) -> (u128, u16) {
        (self.block, self.index)
    }

    /// The bytes of the chunk it rebuilds.
    pub fn chunk_len(&self) -> u64 {
        self.header.chunk_len
    }

    fn guard(&mut self, stepped: Result<(), RepairError>) {
        if let Err(e) = stepped {
            self.end(Err(e));
        }
    }

    fn end(&mut self, outcome: Result<Repaired, RepairError>) {
        self.phase = Phase::Over;
        self.ready.clear();
        self.asked.clear();
        self.outcome.get_or_insert(outcome);
    }

    fn ask(&mut self, asked: Asked, request: Request) {
        let id = self.next_id;
        self.next_id = id.wrapping_add(1);
        self.asked.insert(id, asked);
        self.ready.push_back((id, request));
    }

    /// The chunks read and asked, by index.
    fn have(&self) -> usize {
        self.got.len().saturating_add(self.reading)
    }

    /// How many distinct chunks a rebuild reads: a copy one, a code its data chunks, or, once a
    /// decode failed, every one that reads.
    fn wanted(&self) -> usize {
        if self.header.data == 1 {
            1
        } else if self.suspect {
            usize::MAX
        } else {
            usize::from(self.header.data)
        }
    }

    /// Asks as many more sources as the rebuild still wants, each of a chunk not yet read or out,
    /// from its first place not yet tried; done, the chunk is rebuilt, and with none left to ask
    /// and too few read, the block is unreadable.
    fn read_more(&mut self) -> Result<(), RepairError> {
        let out: BTreeSet<usize> = self
            .asked
            .values()
            .filter_map(|a| match a {
                Asked::Source(p) => self.places.get(*p).map(|pl| usize::from(pl.key.index)),
                _ => None,
            })
            .collect();
        let mut asks = Vec::new();
        for &p in &self.sources {
            if self.have().saturating_add(asks.len()) >= self.wanted() {
                break;
            }
            let Some(place) = self.places.get(p) else {
                continue;
            };
            let index = usize::from(place.key.index);
            // A copy is the whole block in any place; a coded chunk is read once, until a decode
            // failed, when each of its places is read.
            let taken = self.header.data > 1
                && !self.suspect
                && (self.got.contains_key(&index)
                    || out.contains(&index)
                    || asks.iter().any(|&(_, q): &(usize, usize)| q == index));
            if self.tried.contains(&p) || taken {
                continue;
            }
            asks.push((p, index));
        }
        for (p, _) in asks {
            let Some(place) = self.places.get(p).copied() else {
                continue;
            };
            self.tried.insert(p);
            self.reading = self.reading.saturating_add(1);
            self.ask(
                Asked::Source(p),
                Request::Chunk {
                    volume: place.volume,
                    key: place.key,
                    len: self.header.chunk_len,
                },
            );
        }
        if self.reading == 0 {
            return self.rebuild();
        }
        Ok(())
    }

    fn answered(&mut self, asked: Asked, answer: Answer) -> Result<(), RepairError> {
        match (asked, answer) {
            (Asked::Source(p), answer) if self.phase == Phase::Reading => {
                self.reading = self.reading.checked_sub(1).ok_or(RepairError::Mismatch)?;
                if let Answer::Chunk(bytes) = answer {
                    let index = self
                        .places
                        .get(p)
                        .map(|pl| usize::from(pl.key.index))
                        .ok_or(RepairError::Mismatch)?;
                    let fits = u64::try_from(bytes.len()).ok() == Some(self.header.chunk_len);
                    // A copy is the block: one whose bytes are not the block's is no source.
                    let copy_fits =
                        self.header.data > 1 || mantle_crc::crc32c(&bytes) == self.header.crc32c;
                    if !fits {
                        return Err(RepairError::Inconsistent(self.block));
                    }
                    if index == usize::from(self.index) {
                        self.reads.push((p, bytes.clone()));
                    }
                    if !copy_fits {
                        self.mismatched = true;
                    } else if let Some(held) = self.got.get(&index) {
                        if *held != bytes
                            && !self.others.iter().any(|(i, b)| *i == index && *b == bytes)
                        {
                            self.others.push((index, bytes));
                        }
                    } else {
                        self.got.insert(index, bytes);
                    }
                } else if !matches!(answer, Answer::Unreadable) {
                    return Err(RepairError::Mismatch);
                }
                self.read_more()
            }
            (Asked::Place, Answer::Volumes(volumes)) if self.phase == Phase::Placing => {
                let mut seen = BTreeSet::new();
                self.offered = volumes
                    .into_iter()
                    .filter(|v| seen.insert(*v) && !self.places.iter().any(|p| p.volume == *v))
                    .collect();
                self.next = 0;
                self.phase = Phase::Writing;
                self.put_next()
            }
            (Asked::Put(at), Answer::Stored) if self.phase == Phase::Writing => {
                let volume = *self.offered.get(at).ok_or(RepairError::Mismatch)?;
                let command = self.command(volume)?;
                self.stored = true;
                self.commit_to(volume, command);
                Ok(())
            }
            (Asked::Put(_), Answer::Refused { full }) if self.phase == Phase::Writing => {
                self.full &= full;
                self.put_next()
            }
            (Asked::Commit, Answer::Block(outcome)) if self.phase == Phase::Committing => {
                let done = match (outcome, self.to) {
                    (block::Outcome::Moved, Some(volume)) if self.stored => {
                        Repaired::Done { volume }
                    }
                    (block::Outcome::Moved, Some(volume)) => Repaired::Verified { volume },
                    (block::Outcome::Conflict, _) => Repaired::Changed,
                    (block::Outcome::Expired | block::Outcome::NoSuchBlock, _) => {
                        Repaired::Released
                    }
                    (block::Outcome::Full, _) => return Err(RepairError::Full(self.block)),
                    (other, _) => return Err(RepairError::Block(other)),
                };
                self.end(Ok(done));
                Ok(())
            }
            _ => Err(RepairError::Mismatch),
        }
    }

    /// The compare-and-swap that names the chunk on `volume`, asked.
    fn commit_to(&mut self, volume: u128, command: block::Command) {
        self.to = Some(volume);
        self.phase = Phase::Committing;
        self.ask(Asked::Commit, Request::Block(command));
    }

    /// With the chunk's bytes known, a change of the block's rows that needs no write: a place of
    /// the chunk that stays, read and found to be it, takes the place a move lets go; else a place
    /// of the chunk read and found not to be it is let go for one found to be it. A place is let
    /// go only for another known to hold the chunk, so none is the last that held it.
    fn verified(&self, chunk: &Bytes) -> Option<(u128, block::Command)> {
        let key = self.places.get(usize::from(self.index))?.key;
        let volume = |p: usize| self.places.get(p).map(|pl| pl.volume);
        let holds = |p: usize| self.reads.iter().any(|(q, b)| *q == p && b == chunk);
        if let Commit::Move { from } = self.commit
            && let Some(to) = self
                .keepers
                .iter()
                .find(|&&q| holds(q))
                .and_then(|&q| volume(q))
        {
            return Some((
                to,
                block::Command::Move {
                    chunk: key,
                    from,
                    to,
                },
            ));
        }
        let wrong = self.reads.iter().find(|(_, b)| b != chunk)?.0;
        let right = self.reads.iter().find(|(_, b)| b == chunk)?.0;
        let (from, to) = (volume(wrong)?, volume(right)?);
        Some((
            to,
            block::Command::Move {
                chunk: key,
                from,
                to,
            },
        ))
    }

    fn command(&self, to: u128) -> Result<block::Command, RepairError> {
        let own = self
            .places
            .get(usize::from(self.index))
            .ok_or(RepairError::Inconsistent(self.block))?;
        Ok(match self.commit {
            Commit::Keep { from } => block::Command::Repair {
                chunk: own.key,
                from,
                to,
                most: self.most,
            },
            Commit::Move { from } => block::Command::Move {
                chunk: own.key,
                from,
                to,
            },
        })
    }

    /// With the rebuilt chunk written nowhere, and none of its places read found to hold it, one
    /// found not to be it let go for another place of the chunk: it held nothing of the chunk, and
    /// its volume is then free to take a rebuild, as a cell with no volume to spare needs.
    fn release_wrong(&self) -> Option<(u128, block::Command)> {
        let (chunk, _) = self.rebuilt.as_ref()?;
        let target = usize::from(self.index);
        let key = self.places.get(target)?.key;
        let wrong = self.reads.iter().find(|(_, b)| b != chunk)?.0;
        let other = (0..self.places.len()).find(|&p| {
            p != wrong
                && self
                    .places
                    .get(p)
                    .is_some_and(|pl| usize::from(pl.key.index) == target)
        })?;
        let from = self.places.get(wrong)?.volume;
        let to = self.places.get(other)?.volume;
        Some((
            to,
            block::Command::Move {
                chunk: key,
                from,
                to,
            },
        ))
    }

    /// The rebuilt chunk to the next volume offered; with none left, a place of the chunk found
    /// not to be it let go, or the repair unplaced.
    fn put_next(&mut self) -> Result<(), RepairError> {
        let Some(&volume) = self.offered.get(self.next) else {
            if let Some((to, command)) = self.release_wrong() {
                self.commit_to(to, command);
                return Ok(());
            }
            return Err(RepairError::Unplaced {
                block: self.block,
                full: self.full && !self.offered.is_empty(),
            });
        };
        let at = self.next;
        self.next = self.next.checked_add(1).ok_or(RepairError::Overflow)?;
        let (bytes, crc) = self.rebuilt.clone().ok_or(RepairError::Mismatch)?;
        let key = self
            .places
            .get(usize::from(self.index))
            .ok_or(RepairError::Inconsistent(self.block))?
            .key;
        self.ask(
            Asked::Put(at),
            Request::Put {
                volume,
                key,
                bytes,
                crc,
            },
        );
        Ok(())
    }

    /// With every source read, the chunk rebuilt and checked, and its volume asked for.
    fn rebuild(&mut self) -> Result<(), RepairError> {
        let rebuilt = if self.header.data == 1 {
            // A copy read is the block itself, checked as it came.
            match self.got.values().next() {
                Some(copy) => copy.clone(),
                None if self.mismatched => return Err(RepairError::Corrupt(self.block)),
                None => return Err(RepairError::Unreadable(self.block)),
            }
        } else {
            match self.decoded()? {
                Some(chunk) => chunk,
                // A decode failed: every readable chunk read, each left out in turn.
                None if !self.suspect => {
                    self.suspect = true;
                    return self.read_more();
                }
                None => return Err(RepairError::Corrupt(self.block)),
            }
        };
        if let Some((volume, command)) = self.verified(&rebuilt) {
            self.commit_to(volume, command);
            return Ok(());
        }
        let crc = mantle_crc::crc32c(&rebuilt);
        self.rebuilt = Some((rebuilt, crc));
        self.phase = Phase::Placing;
        let avoid = self.places.iter().map(|p| p.volume).collect();
        self.ask(
            Asked::Place,
            Request::Place {
                block: self.block,
                avoid,
                spread: self.spread,
            },
        );
        Ok(())
    }

    /// The chunk rebuilt from the coded chunks read: cut or coded from the block of the first set
    /// of `data` of them, or of every set of copies of distinct chunks among those read once a
    /// decode has failed, that matches its CRC-32C. `None` when no set decodes to the block.
    fn decoded(&self) -> Result<Option<Bytes>, RepairError> {
        let data = usize::from(self.header.data);
        let code = Code::new(data, usize::from(self.header.parity))?;
        let len = usize::try_from(self.header.length).map_err(|_| RepairError::Overflow)?;
        // Every copy read, by its chunk's index: one of each chunk first, then the others a failed
        // decode had read.
        let mut read: Vec<(usize, &[u8])> =
            self.got.iter().map(|(&i, b)| (i, b.as_ref())).collect();
        read.extend(self.others.iter().map(|(i, b)| (*i, b.as_ref())));
        if self.got.len() < data {
            return Err(RepairError::Unreadable(self.block));
        }
        // Without suspicion one set, the chunks read; with it, every set of `data` copies of
        // distinct chunks among those read, in turn, at most `C(read, data)`: the block's CRC-32C
        // finds the first set none of whose copies is wrong, however many are.
        let target = usize::from(self.index);
        let mut at: Vec<usize> = (0..data).collect();
        let mut first = true;
        loop {
            if !first && !(self.suspect && next_set(&mut at, read.len())) {
                return Ok(None);
            }
            first = false;
            let set: Vec<(usize, &[u8])> =
                at.iter().filter_map(|&k| read.get(k).copied()).collect();
            let distinct = set
                .iter()
                .enumerate()
                .all(|(n, (i, _))| set.iter().take(n).all(|(j, _)| j != i));
            if set.len() < data || !distinct {
                continue;
            }
            let block = code.decode(&set, len)?;
            if mantle_crc::crc32c(&block) != self.header.crc32c {
                continue;
            }
            // The chunk comes out of the block the CRC-32C checked, never out of bytes read: a
            // data chunk's bytes past the block, zeros as written, are no part of what the
            // CRC-32C covers. A data chunk is its span of the block, padded; a parity chunk is
            // coded from the block (`Code::parity_of`).
            if target < data {
                let c =
                    usize::try_from(self.header.chunk_len).map_err(|_| RepairError::Overflow)?;
                let mut chunk = Vec::new();
                chunk
                    .try_reserve_exact(c)
                    .map_err(|_| RepairError::Overflow)?;
                chunk
                    .extend_from_slice(block.get(code.data_span(len, target)?).unwrap_or_default());
                chunk.resize(c, 0);
                return Ok(Some(Bytes::from(chunk)));
            }
            let at = target.checked_sub(data).ok_or(RepairError::Overflow)?;
            let parity = code
                .parity_of(&block)?
                .into_iter()
                .nth(at)
                .ok_or(RepairError::Inconsistent(self.block))?;
            return Ok(Some(Bytes::from(parity)));
        }
    }
}

/// Moves `at`, `k` increasing positions of `0..n`, to the next such set in lexicographic order:
/// whether there is one.
fn next_set(at: &mut [usize], n: usize) -> bool {
    let k = at.len();
    // The last position that can still move right: position j can hold at most n − k + j.
    let movable = (0..k).rev().find(|&j| {
        at.get(j).is_some_and(|&v| {
            n.checked_sub(k)
                .and_then(|room| room.checked_add(j))
                .is_some_and(|most| v < most)
        })
    });
    let Some(j) = movable else {
        return false;
    };
    let Some(start) = at.get(j).and_then(|v| v.checked_add(1)) else {
        return false;
    };
    for (offset, slot) in at.iter_mut().skip(j).enumerate() {
        *slot = start.saturating_add(offset);
    }
    true
}

/// The most bytes a repairer holds for a block of `header`'s shape with `places` places: a chunk
/// from each place, once a decode has failed; and beside them the larger of a decode, the block
/// and the decoder's work (`Code::decode_bytes`), and the block and the coding of a parity chunk
/// from it (`Code::encode_bytes`), the decoder's work gone once it returns the block; a data chunk
/// cut from the block is less than the coding.
pub fn most_bytes(header: &BlockHeader, places: usize) -> Option<u64> {
    let read = u64::try_from(places).ok()?.checked_mul(header.chunk_len)?;
    let working = if header.data > 1 {
        let code = Code::new(usize::from(header.data), usize::from(header.parity)).ok()?;
        let len = usize::try_from(header.length).ok()?;
        let coding = u64::try_from(len)
            .ok()?
            .checked_add(code.encode_bytes(len)?)?;
        code.decode_bytes(len)?.max(coding)
    } else {
        header.chunk_len
    };
    read.checked_add(working)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every set of `k` of `0..n` once, in lexicographic order: `C(n, k)` of them.
    #[test]
    fn next_set_walks_every_set_once_in_order() {
        for n in 0..8usize {
            for k in 1..=n {
                let mut at: Vec<usize> = (0..k).collect();
                let mut seen = vec![at.clone()];
                while next_set(&mut at, n) {
                    assert!(at.windows(2).all(|w| w[0] < w[1]) && at.iter().all(|&v| v < n));
                    assert!(seen.last().unwrap() < &at);
                    seen.push(at.clone());
                }
                let choose = (0..k).fold(1usize, |c, i| c * (n - i) / (i + 1));
                assert_eq!(seen.len(), choose, "C({n}, {k})");
            }
        }
    }
}
