#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

//! Repair and rebalancing's control logic (docs/design/repair.md §7) under schedules proptest
//! chooses: a cell of volumes in failure domains, a Block range on the model engine, and blocks of
//! copies and of Reed–Solomon codes placed one chunk to a domain; then nodes go away and come back,
//! volumes go for good and their nodes start again with fresh ones, nodes leave, chunks rot at rest
//! in ways their volumes do not see, time passes, and the repair service plans a block and runs its
//! repairer, a repairer stops part way and is never resumed, two repairers rebuild one chunk at once
//! as two leaders' would, and reads and writes at volumes fail.
//!
//! After every step: no repairer wrote bytes that are not the chunk's (checked at every write); every
//! place a block names on a volume not gone holds the chunk, as written or as it rotted there; and no
//! command a plan or a repairer sent left a chunk without a place holding its bytes on a volume not
//! gone that had one before. Once the faults stop and every node is back, the service brings every
//! block with as many sound chunks as its data to whole: every chunk reachable, nothing kept on a
//! gone volume, nothing held by a leaving volume, and no domain the only one to hold two of its
//! chunks while a domain that takes chunks holds none; and it says whole of no block lacking a
//! chunk. A chunk rotted where its volume cannot see it reads as the chunk to the plan, as it would
//! to a GET, whose decode then fails the block's CRC-32C: repair never writes from it.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use mantle_chunk::ChunkKey;
use mantle_ec::Code;
use mantle_gateway::repair::{self, Answer, Judged, Plan, Reach, Repairer, Request, Volume};
use mantle_meta::block;
use mantle_meta::engine::Model;
use mantle_meta::record::{BlockHeader, ChunkPlace};
use proptest::prelude::*;

/// Steps a node must be away before its volumes' chunks fall due: the simulation's stand-in for
/// the durability allowance, in its own clock, which moves one a tick.
const ALLOW: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Present,
    Away { since: u64 },
    Gone,
}

struct Vol {
    node: usize,
    state: State,
    leaving: bool,
    chunks: BTreeMap<ChunkKey, Bytes>,
}

/// A block as it was written: its header and each chunk's bytes, by index.
struct Truth {
    block: u128,
    header: BlockHeader,
    chunks: Vec<Bytes>,
}

struct Cell {
    vols: BTreeMap<u128, Vol>,
    /// Each node's domain.
    domains: Vec<u32>,
    blocks: Vec<Truth>,
    rows: Model,
    index: u64,
    now: u64,
    next_volume: u128,
    rng: u64,
    /// Each block's last repair's end, as the assertions name it.
    last: BTreeMap<u128, String>,
}

impl Cell {
    fn draw(&mut self) -> u64 {
        // SplitMix64 (Steele, Lea and Flood, OOPSLA 2014).
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn pick(&mut self, n: usize) -> usize {
        usize::try_from(self.draw() % u64::try_from(n.max(1)).unwrap()).unwrap()
    }

    fn new(
        seed: u64,
        nodes: usize,
        two_volumes: &[bool],
        schemes: &[(u8, u8)],
        sizes: &[usize],
    ) -> Self {
        let mut cell = Cell {
            vols: BTreeMap::new(),
            domains: (0..nodes).map(|n| u32::try_from(n).unwrap()).collect(),
            blocks: Vec::new(),
            rows: Model::default(),
            index: 0,
            now: 0,
            next_volume: 100,
            rng: seed,
            last: BTreeMap::new(),
        };
        for node in 0..nodes {
            cell.fresh_volume(node);
            if two_volumes.get(node).copied().unwrap_or(false) {
                cell.fresh_volume(node);
            }
        }
        for (b, (&(data, parity), &size)) in schemes.iter().zip(sizes).enumerate() {
            cell.write_block(u128::try_from(b).unwrap() + 1, data, parity, size);
        }
        cell
    }

    fn fresh_volume(&mut self, node: usize) -> u128 {
        let id = self.next_volume;
        self.next_volume += 1;
        self.vols.insert(
            id,
            Vol {
                node,
                state: State::Present,
                leaving: false,
                chunks: BTreeMap::new(),
            },
        );
        id
    }

    /// A block of `size` bytes in `(data, parity)`, one chunk to a domain, as a put writes it.
    fn write_block(&mut self, block: u128, data: u8, parity: u8, size: usize) {
        let bytes: Vec<u8> = (0..size).map(|_| self.draw().to_le_bytes()[0]).collect();
        let chunks: Vec<Bytes> = if data == 1 {
            (0..=parity).map(|_| Bytes::from(bytes.clone())).collect()
        } else {
            let code = Code::new(data.into(), parity.into()).unwrap();
            code.encode(&bytes)
                .unwrap()
                .into_iter()
                .map(Bytes::from)
                .collect()
        };
        let header = BlockHeader {
            length: u64::try_from(size).unwrap(),
            data,
            parity,
            chunk_len: u64::try_from(chunks[0].len()).unwrap(),
            crc32c: mantle_crc::crc32c(&bytes),
        };
        // One chunk to a domain, each on one of its node's volumes.
        let mut nodes: Vec<usize> = (0..self.domains.len()).collect();
        for i in (1..nodes.len()).rev() {
            let j = self.pick(i + 1);
            nodes.swap(i, j);
        }
        let mut places = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            let node = nodes[i];
            let on: Vec<u128> = self
                .vols
                .iter()
                .filter(|(_, v)| v.node == node && v.state == State::Present)
                .map(|(&id, _)| id)
                .collect();
            let volume = on[self.pick(on.len())];
            let key = ChunkKey {
                block,
                epoch: 1,
                index: u16::try_from(i).unwrap(),
            };
            self.vols
                .get_mut(&volume)
                .unwrap()
                .chunks
                .insert(key, chunk.clone());
            places.push(ChunkPlace { volume, key });
        }
        let write = block::Command::Write {
            block,
            header,
            chunks: places,
            file: 1,
            handover_ns: u64::MAX / 2,
            at_ns: 0,
        };
        self.index += 1;
        let written = block::apply(&mut self.rows, self.index, &write).unwrap();
        assert!(
            matches!(written, block::Outcome::Written { .. }),
            "{written:?}"
        );
        self.blocks.push(Truth {
            block,
            header,
            chunks,
        });
    }

    fn judged(&self) -> Judged {
        let mut volumes = Vec::new();
        let mut takers = BTreeSet::new();
        for (&volume, v) in &self.vols {
            let domain = self.domains[v.node];
            let reach = match v.state {
                State::Gone => Reach::Gone,
                State::Away { since } => Reach::Away {
                    due: self.now.saturating_sub(since) >= ALLOW,
                },
                State::Present if v.leaving => Reach::Leaving,
                State::Present => {
                    takers.insert(domain);
                    Reach::Readable
                }
            };
            volumes.push(Volume {
                volume,
                domain,
                reach,
            });
        }
        Judged::new(volumes, takers)
    }

    fn places(&mut self, block: u128) -> Option<(BlockHeader, Vec<ChunkPlace>)> {
        block::places(&mut self.rows, block).unwrap()
    }

    fn truth(&self, block: u128) -> &Truth {
        self.blocks.iter().find(|t| t.block == block).unwrap()
    }

    /// The indices of `block` with a place on a volume not gone that holds the chunk's bytes.
    fn sound(&mut self, block: u128) -> BTreeSet<u16> {
        let Some((_, places)) = self.places(block) else {
            return BTreeSet::new();
        };
        let truth = self.truth(block);
        let mut sound = BTreeSet::new();
        for p in places {
            let v = &self.vols[&p.volume];
            if v.state != State::Gone
                && v.chunks.get(&p.key) == Some(&truth.chunks[usize::from(p.key.index)])
            {
                sound.insert(p.key.index);
            }
        }
        sound
    }

    /// Applies a command a plan or a repairer sent, checking that no chunk lost its last sound
    /// place to it.
    fn command(&mut self, command: &block::Command) -> block::Outcome {
        let block = match command {
            block::Command::Move { chunk, .. } | block::Command::Repair { chunk, .. } => {
                chunk.block
            }
            block::Command::Unkeep { block, .. } => *block,
            other => panic!("repair sent {other:?}"),
        };
        let before = self.sound(block);
        self.index += 1;
        let outcome = block::apply(&mut self.rows, self.index, command).unwrap();
        let after = self.sound(block);
        assert!(
            before.is_subset(&after),
            "{command:?} took chunks {:?} of block {block} off every sound place",
            before.difference(&after).collect::<Vec<_>>()
        );
        outcome
    }

    /// The answer the cell gives a repairer's request.
    fn serve(&mut self, request: Request, faults: bool) -> Answer {
        match request {
            Request::Chunk { volume, key, len } => {
                let fails = faults && self.pick(8) == 0;
                match self.vols.get(&volume) {
                    Some(v) if v.state == State::Present && !fails => match v.chunks.get(&key) {
                        Some(bytes) => {
                            assert_eq!(u64::try_from(bytes.len()).unwrap(), len);
                            Answer::Chunk(bytes.clone())
                        }
                        None => Answer::Unreadable,
                    },
                    _ => Answer::Unreadable,
                }
            }
            Request::Place {
                block: _,
                avoid,
                spread,
            } => {
                let held: BTreeSet<u32> = avoid
                    .iter()
                    .filter_map(|v| self.vols.get(v))
                    .filter(|v| v.state != State::Gone)
                    .map(|v| self.domains[v.node])
                    .collect();
                let mut free = Vec::new();
                let mut rest = Vec::new();
                let ids: Vec<u128> = self.vols.keys().copied().collect();
                for volume in ids {
                    let v = &self.vols[&volume];
                    if v.state != State::Present || v.leaving || avoid.contains(&volume) {
                        continue;
                    }
                    if held.contains(&self.domains[v.node]) {
                        rest.push(volume);
                    } else {
                        free.push(volume);
                    }
                }
                for list in [&mut free, &mut rest] {
                    for i in (1..list.len()).rev() {
                        let j = self.pick(i + 1);
                        list.swap(i, j);
                    }
                }
                if !spread {
                    free.extend(rest);
                }
                Answer::Volumes(free)
            }
            Request::Put {
                volume,
                key,
                bytes,
                crc,
            } => {
                let truth = self.truth(key.block);
                assert_eq!(
                    bytes,
                    truth.chunks[usize::from(key.index)],
                    "repair wrote bytes that are not chunk {key:?}'s"
                );
                assert_eq!(mantle_crc::crc32c(&bytes), crc);
                let refuses = faults && self.pick(10) == 0;
                let v = self.vols.get_mut(&volume).unwrap();
                if v.state != State::Present || refuses {
                    return Answer::Refused { full: false };
                }
                // A copy held under the key is the chunk's or one rotted since, which no longer
                // verifies at the volume and is written again (chunk-store.md §4).
                v.chunks.insert(key, bytes);
                Answer::Stored
            }
            Request::Block(command) => Answer::Block(self.command(&command)),
        }
    }

    /// The service plans `block`; a plan's command is applied, and a rebuild's repairer runs,
    /// stopping after `stop` answers when one is given, faults striking between its answers.
    fn repair(&mut self, block: u128, stop: Option<usize>, faults: bool) -> Option<Repairer> {
        let (header, places) = self.places(block)?;
        let judged = self.judged();
        let planned = repair::plan(block, &header, &places, &judged).unwrap();
        match planned.plan {
            Plan::Command(command) => {
                self.command(&command);
                None
            }
            Plan::Rebuild {
                index,
                commit,
                spread,
            } => {
                let most = u32::try_from(self.vols.len()).unwrap();
                let mut repairer = Repairer::new(
                    block,
                    header,
                    places,
                    &judged,
                    (index, commit, spread),
                    most,
                )
                .unwrap();
                let mut answered = 0;
                loop {
                    if stop == Some(answered) && repairer.outcome().is_none() {
                        return Some(repairer);
                    }
                    let Some((id, request)) = repairer.poll() else {
                        break;
                    };
                    if faults && self.pick(16) == 0 {
                        self.strike();
                    }
                    let answer = self.serve(request, faults);
                    repairer.answer(id, answer).unwrap();
                    answered += 1;
                }
                self.ended(&repairer);
                self.last.insert(block, format!("{:?}", repairer.outcome()));
                None
            }
            Plan::Whole | Plan::Waiting | Plan::Unreadable { .. } => None,
        }
    }

    fn ended(&self, repairer: &Repairer) {
        match repairer.outcome() {
            Some(Ok(_))
            | Some(Err(
                repair::RepairError::Unreadable(_)
                | repair::RepairError::Corrupt(_)
                | repair::RepairError::Unplaced { .. }
                | repair::RepairError::Full(_),
            )) => {}
            other => panic!("a repairer ended {other:?}"),
        }
    }

    /// A fault between two steps of a repair: a node away or back.
    fn strike(&mut self) {
        let node = self.pick(self.domains.len());
        let now = self.now;
        let away = self
            .vols
            .values()
            .any(|v| v.node == node && matches!(v.state, State::Away { .. }));
        for v in self.vols.values_mut().filter(|v| v.node == node) {
            v.state = match v.state {
                State::Present if !away => State::Away { since: now },
                State::Away { .. } => State::Present,
                s => s,
            };
        }
    }

    /// Every node back, and every leaving volume that holds nothing gone.
    fn all_back(&mut self) {
        for v in self.vols.values_mut() {
            if matches!(v.state, State::Away { .. }) {
                v.state = State::Present;
            }
        }
    }

    fn retire_empty(&mut self) {
        let leaving: Vec<u128> = self
            .vols
            .iter()
            .filter(|(_, v)| v.leaving && v.state != State::Gone)
            .map(|(&id, _)| id)
            .collect();
        for volume in leaving {
            let holds = self
                .blocks
                .iter()
                .map(|t| t.block)
                .collect::<Vec<_>>()
                .into_iter()
                .any(|b| {
                    block::places(&mut self.rows, b)
                        .unwrap()
                        .is_some_and(|(_, places)| places.iter().any(|p| p.volume == volume))
                });
            if !holds {
                self.vols.get_mut(&volume).unwrap().state = State::Gone;
            }
        }
    }

    /// Every place a block names on a volume not gone holds its chunk.
    fn check(&mut self) {
        for b in 0..self.blocks.len() {
            let block = self.blocks[b].block;
            let Some((_, places)) = self.places(block) else {
                continue;
            };
            for p in places {
                let v = &self.vols[&p.volume];
                if v.state != State::Gone {
                    assert!(
                        v.chunks.contains_key(&p.key),
                        "block {block} names {:x} for chunk {}, which holds nothing of it",
                        p.volume,
                        p.key.index
                    );
                }
            }
        }
    }

    /// With the faults stopped: every node back, the service run until no plan changes anything,
    /// and each block judged.
    fn finish(&mut self) {
        self.all_back();
        let blocks: Vec<u128> = self.blocks.iter().map(|t| t.block).collect();
        // Each block takes at most one step a chunk to rebuild and one a place to forget or move
        // for each volume: past that a plan that still acts is not converging.
        let most = blocks.len() * (self.vols.len() + 16) * 4;
        let mut passes = 0;
        loop {
            self.retire_empty();
            let mut acted = false;
            for &block in &blocks {
                let (header, places) = self.places(block).unwrap();
                let judged = self.judged();
                let planned = repair::plan(block, &header, &places, &judged).unwrap();
                if matches!(planned.plan, Plan::Command(_) | Plan::Rebuild { .. }) {
                    // A rebuild that finds no source or volume acts no more than once a pass.
                    let before = self.places(block);
                    self.repair(block, None, false);
                    acted |= self.places(block) != before;
                }
                self.check();
            }
            passes += 1;
            if !acted {
                break;
            }
            assert!(passes < most, "repair did not settle in {most} passes");
        }
        for &block in &blocks {
            let (header, places) = self.places(block).unwrap();
            let judged = self.judged();
            let planned = repair::plan(block, &header, &places, &judged).unwrap();
            let sound = self.sound(block);
            if sound.len() >= usize::from(header.data) {
                let volumes: Vec<(u128, usize, State, bool)> = self
                    .vols
                    .iter()
                    .map(|(&id, v)| (id, v.node, v.state, v.leaving))
                    .collect();
                assert_eq!(
                    planned.plan,
                    Plan::Whole,
                    "block {block}: sound chunks {sound:?}, places {places:?}, last repair {:?}, \
                     volumes {volumes:?}",
                    self.last.get(&block)
                );
            }
            if planned.plan == Plan::Whole {
                // Whole is never said of a block lacking a chunk: each has a place on a volume not
                // gone that holds bytes under its key, sound or rotted where its volume cannot see,
                // and none is left on a leaving volume.
                let width = usize::from(header.data) + usize::from(header.parity);
                for index in 0..width {
                    let held = places.iter().any(|p| {
                        usize::from(p.key.index) == index
                            && self.vols[&p.volume].state != State::Gone
                            && self.vols[&p.volume].chunks.contains_key(&p.key)
                    });
                    assert!(
                        held,
                        "block {block} planned whole lacking chunk {index}: {places:?}"
                    );
                }
                for p in &places {
                    assert!(
                        !self.vols[&p.volume].leaving,
                        "block {block} whole with a place on a leaving volume: {places:?}"
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Action {
    Away(usize),
    Back(usize),
    /// A node's volume goes for good, its node starting again with a fresh one.
    Lose(usize),
    Leave(usize),
    /// A chunk rots at rest where its volume cannot see it.
    Rot {
        block: usize,
        place: usize,
    },
    Tick,
    Repair(usize),
    /// A repairer that stops after this many answers, never resumed.
    Stop {
        block: usize,
        after: usize,
    },
    /// Two repairers of one block, one stopped before its last answers while the other runs.
    Race {
        block: usize,
        after: usize,
    },
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        (0usize..8).prop_map(Action::Away),
        (0usize..8).prop_map(Action::Back),
        (0usize..8).prop_map(Action::Lose),
        (0usize..8).prop_map(Action::Leave),
        (0usize..16, 0usize..16).prop_map(|(block, place)| Action::Rot { block, place }),
        Just(Action::Tick),
        (0usize..16).prop_map(Action::Repair),
        (0usize..16).prop_map(Action::Repair),
        (0usize..16).prop_map(Action::Repair),
        (0usize..16, 0usize..12).prop_map(|(block, after)| Action::Stop { block, after }),
        (0usize..16, 0usize..12).prop_map(|(block, after)| Action::Race { block, after }),
    ]
}

impl Cell {
    fn act(&mut self, action: &Action) {
        let nodes = self.domains.len();
        let block = |i: usize, cell: &Cell| cell.blocks[i % cell.blocks.len()].block;
        match *action {
            Action::Away(n) => {
                let now = self.now;
                for v in self.vols.values_mut().filter(|v| v.node == n % nodes) {
                    if v.state == State::Present {
                        v.state = State::Away { since: now };
                    }
                }
            }
            Action::Back(n) => {
                for v in self.vols.values_mut().filter(|v| v.node == n % nodes) {
                    if matches!(v.state, State::Away { .. }) {
                        v.state = State::Present;
                    }
                }
            }
            Action::Lose(n) => {
                let node = n % nodes;
                // A node starts again, with a fresh volume in the lost one's place, only once back.
                if self
                    .vols
                    .values()
                    .any(|v| v.node == node && matches!(v.state, State::Away { .. }))
                {
                    return;
                }
                let lost = self
                    .vols
                    .iter()
                    .find(|(_, v)| v.node == node && v.state != State::Gone)
                    .map(|(&id, _)| id);
                if let Some(id) = lost {
                    let v = self.vols.get_mut(&id).unwrap();
                    v.state = State::Gone;
                    v.chunks.clear();
                    let leaving = v.leaving;
                    let fresh = self.fresh_volume(node);
                    self.vols.get_mut(&fresh).unwrap().leaving = leaving;
                }
            }
            Action::Leave(n) => {
                // A node leaves only where the domains that stay still hold every block's width.
                let widest = self
                    .blocks
                    .iter()
                    .map(|t| usize::from(t.header.data) + usize::from(t.header.parity))
                    .max()
                    .unwrap_or(0);
                let leaving: BTreeSet<usize> = self
                    .vols
                    .values()
                    .filter(|v| v.leaving)
                    .map(|v| v.node)
                    .collect();
                if nodes - leaving.len() > widest {
                    for v in self.vols.values_mut().filter(|v| v.node == n % nodes) {
                        v.leaving = true;
                    }
                }
            }
            Action::Rot { block: b, place } => {
                let block = block(b, self);
                if let Some((_, places)) = self.places(block) {
                    let p = places[place % places.len()];
                    if let Some(bytes) =
                        self.vols.get_mut(&p.volume).unwrap().chunks.get_mut(&p.key)
                    {
                        let mut rotted = bytes.to_vec();
                        rotted[0] ^= 1;
                        *bytes = Bytes::from(rotted);
                    }
                }
            }
            Action::Tick => self.now += 1,
            Action::Repair(b) => {
                let block = block(b, self);
                self.repair(block, None, true);
            }
            Action::Stop { block: b, after } => {
                let block = block(b, self);
                self.repair(block, Some(after), true);
            }
            Action::Race { block: b, after } => {
                let block = block(b, self);
                // The first stops short; the second runs whole; the first then goes on, its
                // block changed under it or not.
                if let Some(mut first) = self.repair(block, Some(after), true) {
                    self.repair(block, None, true);
                    while let Some((id, request)) = first.poll() {
                        let answer = self.serve(request, true);
                        first.answer(id, answer).unwrap();
                    }
                    self.ended(&first);
                }
            }
        }
    }
}

/// The schemes a cell of `nodes` domains holds blocks in: copies and codes no wider than it.
fn schemes(nodes: usize) -> Vec<(u8, u8)> {
    [(1, 1), (1, 2), (2, 1), (3, 2), (2, 2)]
        .into_iter()
        .filter(|&(d, p)| usize::from(d) + usize::from(p) <= nodes)
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn repair_never_writes_wrong_bytes_or_loses_a_chunk_and_settles_once_faults_stop(
        seed in any::<u64>(),
        nodes in 3usize..7,
        two in prop::collection::vec(any::<bool>(), 7),
        picks in prop::collection::vec((0usize..5, 1usize..600), 1..10),
        actions in prop::collection::vec(action(), 1..120),
    ) {
        let available = schemes(nodes);
        let chosen: Vec<(u8, u8)> =
            picks.iter().map(|(s, _)| available[s % available.len()]).collect();
        let sizes: Vec<usize> = picks.iter().map(|(_, size)| *size).collect();
        let mut cell = Cell::new(seed, nodes, &two, &chosen, &sizes);
        for action in &actions {
            cell.act(action);
            cell.check();
        }
        cell.finish();
    }
}

/// A node away is waited for until its chunks fall due, and then each is rebuilt on a volume of a
/// domain holding none of the block's and kept where it was: back, its copy counts again.
#[test]
fn an_away_node_is_waited_for_then_its_chunks_rebuilt_and_kept() {
    let mut cell = Cell::new(7, 4, &[false; 4], &[(1, 2), (2, 1)], &[500, 501]);
    let held: Vec<u128> = cell.blocks.iter().map(|t| t.block).collect();
    // The node holding block 1's first chunk goes away.
    let (_, places) = cell.places(held[0]).unwrap();
    let away = cell.vols[&places[0].volume].node;
    cell.act(&Action::Away(away));
    for &block in &held {
        cell.repair(block, None, false);
    }
    assert_eq!(
        cell.places(held[0]).unwrap().1,
        places,
        "nothing moves before it is due"
    );
    for _ in 0..ALLOW {
        cell.act(&Action::Tick);
    }
    for &block in &held {
        cell.repair(block, None, false);
    }
    let (_, now) = cell.places(held[0]).unwrap();
    assert_eq!(
        now.len(),
        places.len() + 1,
        "rebuilt, and kept where it was: {now:?}"
    );
    assert_eq!(now.last().unwrap().volume, places[0].volume);
    cell.act(&Action::Back(away));
    cell.finish();
    let (_, end) = cell.places(held[0]).unwrap();
    assert_eq!(
        end.len(),
        places.len() + 1,
        "the copy it kept counts again: {end:?}"
    );
}

/// A volume gone is rebuilt at once, from copies or by decoding, and a chunk two places of whose
/// block share a domain moves to a domain that takes chunks and holds none of them.
#[test]
fn a_gone_volume_is_rebuilt_at_once_and_a_shared_domain_spreads() {
    let mut cell = Cell::new(
        11,
        4,
        &[true, false, false, false],
        &[(2, 1), (1, 2)],
        &[700, 33],
    );
    for t in 0..cell.blocks.len() {
        let block = cell.blocks[t].block;
        let (_, places) = cell.places(block).unwrap();
        let node = cell.vols[&places[1].volume].node;
        cell.act(&Action::Lose(node));
        cell.repair(block, None, false);
        let (_, after) = cell.places(block).unwrap();
        assert!(
            after
                .iter()
                .all(|p| cell.vols[&p.volume].state != State::Gone),
            "block {block} rebuilt: {after:?}"
        );
        assert_eq!(cell.sound(block).len(), after.len());
    }
    cell.finish();
}
