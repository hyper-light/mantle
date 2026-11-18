//! The cell's repair and rebalancing (docs/design/repair.md §5), run by the node whose member of the
//! Block range leads it, as the collector runs beside its range's leader (metadata.md §2): one
//! service a range, which a change of leader moves. And the facts it judges by: the node that leads
//! the root range states, from its own failure detectors and connections, which nodes the cell
//! sees (`root::Seen`), so every node places and repairs by the same record.
//!
//! The service walks the blocks with a place on a volume that needs it, its reverse rows (metadata.md
//! §1): a volume gone, leaving, or whose node has been away long enough that its chunks fall due; and
//! every block of the range once the domains that take chunks grow, or once it begins, for the
//! domain rule. It walks them in passes by margin, the blocks with the fewest chunks to spare first
//! (Ford et al., OSDI 2010, §5.1), and plans each (`mantle_gateway::repair::plan`): a change of rows
//! alone it asks of the Block range, and a chunk to rebuild it hands a repairer, whose reads, puts
//! and compare-and-swap the node asks as it asks a gateway's. Nothing of it is kept but in the Block
//! range: a service begun on a new leader walks again, every rebuild it repeats writes the same bytes
//! under the same key, and every command it repeats is refused or answered as done.
//!
//! A chunk whose every place is away waits for its node until it falls due: the time the
//! durability model gives a lost chunk to be rebuilt in (`durability.repair_rate`, durability.md
//! §5), less the time rebuilding what the away volume holds takes: the blocks it holds, as a pass
//! counted them, times the longest one block's rebuild has taken, over the repairs run at once, so
//! the last rebuilt chunk is in place by then. Until it has timed a rebuild it cannot plan a wait,
//! and rebuilds one block at once to time one. A volume whose node started without it is gone, and
//! its chunks are rebuilt at once.

use bytes::Bytes;
use mantle_gateway::repair::{
    self as fix, Answer, Judged, Plan, Reach, RepairError, Repaired, Repairer, Request, Volume,
};
use mantle_meta::apply::Layer;
use mantle_meta::block;
use mantle_meta::engine::Engine;
use mantle_meta::record::{BlockHeader, ChunkPlace, VolumeStanding};
use mantle_meta::root::{self, Fact, Seen};
use mantle_meta::wire::{Answer as RangeAnswer, Command};

use super::host::Pending;
use super::{ChunkOutcome, Node, NodeError, NodeLog, take_done};
use crate::ask::{CommandAsk, CommandOutcome, Wait};
use crate::chunk;
use crate::place;

/// What the service has done and where it stands, as `mantle node` says it (`repair` lines).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepairStatus {
    /// Whether this node runs the cell's repair: its member leads the Block range.
    pub leading: bool,
    /// Whether its last pass ended finding nothing it could do, and the root and Block ranges'
    /// applied indexes that pass began at.
    pub settled: bool,
    pub root_applied: u64,
    pub block_applied: u64,
    /// Chunks rebuilt and named, of them those rebuilt only to time a repair, rows changed alone,
    /// and repairs refused for want of a volume, a source, or a block whole.
    pub rebuilt: u64,
    pub timing: u64,
    pub commands: u64,
    pub unplaced: u64,
    pub unreadable: u64,
    pub corrupt: u64,
    /// Of the last pass: blocks whose chunks wait for away nodes, blocks unreadable now, and blocks
    /// lost for good, every place of a chunk they lack gone.
    pub waiting: u64,
    pub short: u64,
    pub lost: u64,
    /// Repairs out now.
    pub running: usize,
    /// When, on the wall clock, the last pass ended.
    pub ended_ns: u64,
}

/// A repair out: its repairer, what it waits on, each with the repairer's request it answers and
/// whether that is a chunk's read, and the requests the node had no room to ask yet.
struct Running {
    repairer: Repairer,
    pending: Vec<(Pending, u64, bool)>,
    held: Vec<(u64, Request)>,
    /// When it began, on the wall clock: its rebuild's time, once its chunk is named.
    began_ns: u64,
    /// The bytes it holds of the operations' memory while it runs: an operation slot for each width
    /// of its block's places, a chunk from each of which it holds at most (`repair::most_bytes`),
    /// each slot the most an operation holds (`Charges::most`), taken from the host's admission and
    /// given back as it ends.
    charge: u64,
    /// Whether it is the rebuild that times one.
    timing: bool,
}

/// A pass over the blocks the facts point at.
#[derive(Debug, Clone)]
struct Pass {
    /// The margin it acts at: blocks with no more chunks to spare than this.
    level: i64,
    /// The volumes whose blocks it walks, and where in them it is.
    volumes: Vec<u128>,
    at: usize,
    after: Option<u128>,
    /// Whether it walks every block of the range, and where; done once the walk passes the last.
    /// A pass that sweeps sweeps again at each level it walks.
    sweep: Option<Option<u128>>,
    sweeps: bool,
    /// Whether it acted on a block, and the least margin above its level it found to act on.
    acted: bool,
    higher: Option<i64>,
    /// The root and Block ranges' applied indexes it began at.
    root_applied: u64,
    block_applied: u64,
    /// What it found: blocks waiting, short of chunks, lost.
    waiting: u64,
    short: u64,
    lost: u64,
    /// The blocks with a place on each volume it walks, counted at its first level.
    counting: std::collections::BTreeMap<u128, u64>,
    /// Whether its walk is over and it waits for its repairs and commands to be answered.
    walked: bool,
}

/// The service, beside the Block range's leader.
pub(crate) struct Repair {
    pass: Option<Pass>,
    running: Vec<Running>,
    /// Row changes asked, by the block each is of.
    commanding: Vec<(CommandAsk, u128)>,
    /// Volumes written off or taken out of the table asked of the root range, by volume.
    retiring: Vec<(CommandAsk, u128)>,
    /// The domains that took chunks at the last pass's start: a pass sweeps every block when more
    /// do.
    takers: Vec<String>,
    /// Whether the service has swept every block since it began.
    swept: bool,
    /// The longest one block's rebuild has taken, from its repairer's start to its chunk named:
    /// none until the service has timed one, which it then rebuilds at once to time.
    per_block_ns: Option<u64>,
    /// Whether the rebuild that times one has begun.
    timing: bool,
    /// The blocks each away, leaving or gone volume held a place of, as the last pass at the
    /// lowest margin counted them: the rebuilds its node's absence would cost. At most the table's
    /// volumes.
    counts: std::collections::BTreeMap<u128, u64>,
    /// The repairs it runs at once: one, the least that moves the work, until a rebuild has been
    /// timed; then as many as rebuild every block the away volumes hold within the time the
    /// durability model allows, at the time a rebuild took (Little's law, research/11 §4).
    concurrency: usize,
    /// A block the walk reached whose plan waits for room, with the operation slots its repair
    /// takes (none for a change of rows): it goes first once there is room, the walk held there.
    deferred: Option<(u128, usize)>,
    status: RepairStatus,
    dbg_takers: Vec<String>,
}

impl Repair {
    fn new() -> Self {
        Self {
            pass: None,
            running: Vec::new(),
            commanding: Vec::new(),
            retiring: Vec::new(),
            takers: Vec::new(),
            swept: false,
            per_block_ns: None,
            timing: false,
            counts: std::collections::BTreeMap::new(),
            concurrency: 1,
            deferred: None,
            dbg_takers: Vec::new(),
            status: RepairStatus {
                leading: true,
                ..RepairStatus::default()
            },
        }
    }
}

/// The time the durability model gives a lost chunk to be rebuilt in: one over its repair rate, per
/// hour (durability.md §5), in nanoseconds; none where the rate gives none.
fn allowance_ns(repair_per_hour: f64) -> Option<u64> {
    let seconds = 3_600.0 / repair_per_hour;
    let allowance = std::time::Duration::try_from_secs_f64(seconds).ok()?;
    u64::try_from(allowance.as_nanos()).ok()
}

/// What an away volume's chunks fall due by: the time the durability model allows a lost chunk's
/// rebuild (`allowance`), the longest one block's rebuild has taken (`per_block_ns`), the blocks
/// each volume held a place of as last counted, and the repairs run at once.
struct Due<'a> {
    allowance: Option<u64>,
    per_block_ns: Option<u64>,
    counts: &'a std::collections::BTreeMap<u128, u64>,
    concurrency: usize,
    /// Whether, untimed, a rebuild may begin at once to time one.
    untimed: bool,
}

impl Due<'_> {
    /// When `fact`'s chunks fall due, on the wall clock: the allowance past the time its node went
    /// away, less the time rebuilding the blocks it holds takes at the repairs run at once; none
    /// where no allowance is given, or no rebuild has been timed (then at once, to time one, where
    /// that is `untimed`'s to say).
    fn at(&self, fact: &Fact) -> Option<u64> {
        let (allowance, per) = (self.allowance?, self.per_block_ns?);
        let blocks = self.counts.get(&fact.placed.volume).copied().unwrap_or(0);
        let concurrency = u64::try_from(self.concurrency.max(1)).unwrap_or(u64::MAX);
        let rebuilding = per
            .saturating_mul(blocks)
            .checked_div(concurrency)
            .unwrap_or(u64::MAX);
        Some(
            fact.presence
                .since_ns
                .saturating_add(allowance.saturating_sub(rebuilding)),
        )
    }

    fn due(&self, fact: &Fact, now_ns: u64) -> bool {
        match (self.allowance, self.per_block_ns) {
            (None, _) => false,
            (Some(_), None) => self.untimed,
            (Some(_), Some(_)) => self.at(fact).is_some_and(|at| at <= now_ns),
        }
    }
}

/// The volumes as a block's plan judges them at `now_ns`: a volume gone is gone; one whose node is
/// away is due as `due` says; one whose node is seen is leaving or readable; and the domains with
/// a volume that takes chunks.
fn judge(facts: &[Fact], now_ns: u64, due: &Due<'_>) -> (Judged, Vec<String>) {
    let mut domains: Vec<&str> = facts.iter().map(|f| f.placed.domain.as_str()).collect();
    domains.sort_unstable();
    domains.dedup();
    let number = |domain: &str| {
        domains
            .binary_search(&domain)
            .ok()
            .and_then(|at| u32::try_from(at).ok())
    };
    let mut volumes = Vec::with_capacity(facts.len());
    let mut takers = std::collections::BTreeSet::new();
    let mut taking: Vec<String> = Vec::new();
    for fact in facts {
        // Every fact's domain is among those just numbered, at most a `u32` of them.
        let Some(domain) = number(&fact.placed.domain) else {
            continue;
        };
        let reach = match (fact.standing, fact.presence.present) {
            (VolumeStanding::Gone, _) => Reach::Gone,
            (_, false) => Reach::Away {
                due: due.due(fact, now_ns),
            },
            (VolumeStanding::Leaving, true) => Reach::Leaving,
            (VolumeStanding::Active, true) => {
                takers.insert(domain);
                if !taking.contains(&fact.placed.domain) {
                    taking.push(fact.placed.domain.clone());
                }
                Reach::Readable
            }
        };
        volumes.push(Volume {
            volume: fact.placed.volume,
            domain,
            reach,
        });
    }
    taking.sort_unstable();
    (Judged::new(volumes, takers), taking)
}

/// When the soonest away volume of `facts` whose chunks fall due after `after_ns` does, on the wall
/// clock: none where none is away, or no rebuild has been timed.
fn next_due_after(facts: &[Fact], due: &Due<'_>, after_ns: u64) -> Option<u64> {
    facts
        .iter()
        .filter(|f| f.standing != VolumeStanding::Gone && !f.presence.present)
        .filter_map(|f| due.at(f))
        .filter(|&at| at > after_ns)
        .min()
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// What the cell's repair on this node has done and where it stands.
    pub fn repair_status(&self) -> RepairStatus {
        match &self.repair {
            Some(repair) => RepairStatus {
                running: repair.running.len(),
                ..repair.status
            },
            None => RepairStatus::default(),
        }
    }

    /// What an away volume's chunks fall due by, as the service stands.
    fn due_of<'a>(&self, repair: &'a Repair) -> Due<'a> {
        Due {
            allowance: self
                .gateway
                .as_ref()
                .and_then(|g| allowance_ns(g.rates.repair)),
            per_block_ns: repair.per_block_ns,
            counts: &repair.counts,
            concurrency: repair.concurrency,
            untimed: !repair.timing,
        }
    }

    /// The handle of this node's member of the range of `layer`.
    fn handle_of(&self, layer: Layer) -> Option<hyper_durable::Handle> {
        self.ranges
            .iter()
            .find(|k| k.layer == layer)
            .map(|k| k.handle)
    }

    /// The facts the root range keeps, as this node's member applied them: each volume, its
    /// standing and its node's presence, and the index read at.
    fn facts(&mut self) -> Result<(Vec<Fact>, u64), NodeError> {
        let table = self.table()?;
        Ok((table.facts.clone(), table.applied()))
    }

    /// The node that leads the root range states what the cell sees of each node it holds
    /// (docs/design/repair.md §2): seen, where its detectors trust it over a keyed connection, or
    /// itself; not seen, where they suspect it, it announced its stop, or a connection to it this
    /// run ended and none followed. A node it holds no link to, or none yet, it says nothing of.
    pub(super) fn observe_presence(&mut self) -> Result<(), NodeError> {
        // Words asked earlier, answered or not, are let go as their answers come.
        let Self {
            seeing,
            commands_done,
            ..
        } = self;
        seeing.retain(|(_, ask)| take_done(commands_done, *ask).is_none());
        let Some(root) = self.group_of(Layer::Root) else {
            return Ok(());
        };
        if !self.is_leader(root) || self.stopping() {
            return Ok(());
        }
        // The table's volumes read in place, a node's word asked once however many volumes it
        // holds: nothing is copied or allocated at a turn that changes nothing.
        let volumes = self.table()?.facts.len();
        for at in 0..volumes {
            let Some((node, recorded)) = self
                .table()?
                .facts
                .get(at)
                .map(|f| (f.placed.node, f.presence.present))
            else {
                break;
            };
            self.state_presence(root, node, recorded)?;
        }
        // A member holding no volume is no volume's node, and its presence is the cell's fact all
        // the same: the record membership judges a member gone by (docs/design/membership.md §8).
        let members = self.cell.as_ref().map_or(0, |c| c.members().len());
        for at in 0..members {
            let Some((node, recorded)) = self.cell.as_ref().and_then(|c| {
                let (id, _) = c.members().get(at)?;
                if c.holds(*id) {
                    return None;
                }
                Some((*id, c.presence(*id)?.present))
            }) else {
                continue;
            };
            self.state_presence(root, node, recorded)?;
        }
        Ok(())
    }

    /// Asks the root range of `root` the word that `node` is seen or not, where this node's links
    /// say otherwise than `recorded` and none is out for it.
    fn state_presence(&mut self, root: u128, node: u64, recorded: bool) -> Result<(), NodeError> {
        let seen = if node == self.id {
            Some(true)
        } else {
            self.remotes.iter().find(|r| r.id == node).and_then(|r| {
                if r.suspect {
                    Some(false)
                } else if r.keyed {
                    Some(true)
                } else if r.ended > 0 {
                    Some(false)
                } else {
                    None
                }
            })
        };
        let Some(present) = seen else {
            return Ok(());
        };
        if present == recorded
            || self.seeing.iter().any(|(n, _)| *n == node)
            || !self.command_room()
        {
            return Ok(());
        }
        let word = Command::Root(root::Command::Seen(Seen {
            node,
            present,
            // The entry that carries it gives it its time.
            at_ns: 0,
        }));
        let ask = self.command(root, word, u64::MAX, Wait::Request)?;
        self.seeing.push((node, ask));
        Ok(())
    }

    /// When the repair next has something to do on the shard's clock: now while a pass walks with
    /// room for a repair or a repair's answer waits, or when an away volume's chunks fall due.
    pub(super) fn repair_due(&self) -> Option<u64> {
        let repair = self.repair.as_ref()?;
        let shard = hyper_rt::futures::now_ns();
        let answered = |pending: &Pending| match pending {
            Pending::Command(ask) => self.commands_done.iter().any(|(a, _)| a == ask),
            Pending::Read(ask) => self.reads_done.iter().any(|(a, _)| a == ask),
            Pending::Chunk(ask) => self.chunks_done.iter().any(|(a, _)| a == ask),
        };
        let waiting = repair
            .running
            .iter()
            .any(|r| r.pending.iter().any(|(p, _, _)| answered(p)) || !r.held.is_empty())
            || repair
                .commanding
                .iter()
                .chain(&repair.retiring)
                .any(|(ask, _)| self.commands_done.iter().any(|(a, _)| a == ask));
        // A pass whose walk is over waits for its answers, which `waiting` says.
        let resumes = match repair.deferred {
            Some((_, 0)) => self.command_room(),
            Some((_, slots)) => self.starts(repair, slots),
            None => repair.pass.as_ref().is_some_and(|p| !p.walked) && self.starts(repair, 1),
        };
        if waiting || resumes {
            return Some(shard);
        }
        let facts = &self.table.as_ref()?.facts;
        let due = next_due_after(facts, &self.due_of(repair), repair.status.ended_ns)?;
        let wall = self.wall.now_ns();
        Some(shard.saturating_add(due.saturating_sub(wall)))
    }

    /// What a repair taking `slots` holds of the operations' memory: each slot the most an
    /// operation holds, a block of its width's (`Charges::most`); none for a node with no gateway.
    fn repair_charge(&self, slots: usize) -> u64 {
        self.gateway.as_ref().map_or(0, |gateway| {
            u64::try_from(slots)
                .ok()
                .and_then(|slots| slots.checked_mul(gateway.charges.most()))
                // Past what any memory holds: never begun.
                .unwrap_or(u64::MAX)
        })
    }

    /// Whether a repair taking `slots` begins now: fewer out than the service's concurrency, and
    /// its bytes held by the operations' memory beside what the host's operations and the repairs
    /// out hold (`crate::admit`), so the host's clients and the cell's repair share one budget.
    fn starts(&self, repair: &Repair, slots: usize) -> bool {
        let charge = self.repair_charge(slots);
        repair.running.len() < repair.concurrency
            && self
                .admission
                .as_ref()
                .is_some_and(|admission| admission.holds(charge))
    }

    /// The repair at a turn: on the node leading the Block range, its repairs' answers taken, their
    /// requests asked, and its pass walked as far as its room goes; on any other, let go.
    pub(super) fn step_repair(&mut self) -> Result<(), NodeError> {
        let leads = self.gateway.is_some()
            && !self.stopping()
            && self
                .group_of(Layer::Block)
                .is_some_and(|block| self.is_leader(block))
            && self.group_of(Layer::Root).is_some();
        if !leads {
            if let Some(repair) = self.repair.take() {
                eprintln!("REPAIRDBG {} service-abandon status={:?}", self.id, repair.status);
                self.abandon(repair);
            }
            return Ok(());
        }
        if self.repair.is_none() {
            eprintln!("REPAIRDBG {} service-new", self.id);
        }
        let mut repair = self.repair.take().unwrap_or_else(Repair::new);
        let stepped = self.step_service(&mut repair);
        self.repair = Some(repair);
        stepped
    }

    /// What a service let go of asked: its outcomes dropped as they come.
    fn abandon(&mut self, repair: Repair) {
        for running in repair.running {
            if let Some(admission) = self.admission.as_mut() {
                admission.gave(running.charge);
            }
            self.abandoned
                .extend(running.pending.into_iter().map(|(p, _, _)| p));
        }
        self.abandoned.extend(
            repair
                .commanding
                .into_iter()
                .chain(repair.retiring)
                .map(|(ask, _)| Pending::Command(ask)),
        );
    }

    fn step_service(&mut self, repair: &mut Repair) -> Result<(), NodeError> {
        let Some(block_group) = self.group_of(Layer::Block) else {
            return Ok(());
        };
        let block_applied = self.applied(block_group);
        let now = self.wall.now_ns();
        // Settled, with nothing out, the facts where the last pass began and no away volume fallen
        // due since: nothing to do, and nothing read or copied to find so.
        let idle = repair.pass.is_none()
            && repair.running.is_empty()
            && repair.commanding.is_empty()
            && repair.retiring.is_empty()
            && repair.status.settled
            && repair.status.block_applied == block_applied;
        let ended = repair.status.ended_ns;
        let fallen = {
            let due = self.due_of(repair);
            let table = self.table()?;
            let fallen = next_due_after(&table.facts, &due, ended).is_some_and(|at| at <= now);
            if idle && table.applied() == repair.status.root_applied && !fallen {
                return Ok(());
            }
            fallen
        };
        let (facts, root_applied) = self.facts()?;
        // A block holds at most one place on each of the cell's volumes, a count the table's
        // `u32` census bounds.
        let most = u32::try_from(facts.len())
            .map_err(|_| NodeError::Bounds("the table's volumes".into()))?;
        let (judged, takers) = judge(&facts, now, &self.due_of(repair));
        if takers != repair.dbg_takers {
            eprintln!(
                "REPAIRDBG {} takers {:?} -> {:?} root={} block={} pass={}",
                self.id,
                repair.dbg_takers,
                takers,
                root_applied,
                block_applied,
                repair.pass.is_some()
            );
            repair.dbg_takers = takers.clone();
        }
        self.take_repairs(repair, block_group)?;
        self.take_commands(repair);
        self.ask_repairs(repair, block_group, &facts)?;
        // A pass begins once the facts moved since the last began, or the domains that take
        // chunks grew, which sweeps every block for the domain rule.
        let moved = repair.pass.is_none()
            && (!repair.status.settled
                || repair.status.root_applied != root_applied
                || repair.status.block_applied != block_applied
                || fallen);
        if moved {
            let grew = takers.iter().any(|d| !repair.takers.contains(d));
            let sweep = !repair.swept || grew;
            eprintln!(
                "REPAIRDBG {} pass-begin root={} block={} takers={:?} old={:?} grew={} swept={} sweep={} was_settled={} st=({},{}) fallen={}",
                self.id,
                root_applied,
                block_applied,
                takers,
                repair.takers,
                grew,
                repair.swept,
                sweep,
                repair.status.settled,
                repair.status.root_applied,
                repair.status.block_applied,
                fallen
            );
            repair.swept |= sweep;
            repair.takers = takers;
            let mut volumes: Vec<u128> = facts
                .iter()
                .filter(|f| match f.standing {
                    VolumeStanding::Gone | VolumeStanding::Leaving => true,
                    VolumeStanding::Active => !f.presence.present,
                })
                .map(|f| f.placed.volume)
                .collect();
            volumes.sort_unstable();
            repair.pass = Some(Pass {
                level: 0,
                volumes,
                at: 0,
                after: None,
                sweep: if sweep { Some(None) } else { None },
                sweeps: sweep,
                acted: false,
                higher: None,
                root_applied,
                block_applied,
                waiting: 0,
                short: 0,
                lost: 0,
                counting: std::collections::BTreeMap::new(),
                walked: false,
            });
            repair.status.settled = false;
        }
        // A block whose plan waited for room goes first, the walk held at it meanwhile.
        if let Some((block, _)) = repair.deferred.take() {
            repair.deferred = self
                .consider(repair, block, &judged, most, block_group)?
                .map(|slots| (block, slots));
        }
        // One block a turn for each repair the room leaves: the least that moves the pass.
        let mut budget = repair.concurrency.saturating_sub(repair.running.len());
        while budget > 0 && repair.deferred.is_none() && self.starts(repair, 1) {
            budget = budget.saturating_sub(1);
            let Some(block) = self.next_block(repair, (root_applied, block_applied))? else {
                break;
            };
            repair.deferred = self
                .consider(repair, block, &judged, most, block_group)?
                .map(|slots| (block, slots));
        }
        if repair.pass.is_none() && repair.status.settled {
            self.retire_empty(repair, &facts)?;
        }
        Ok(())
    }

    /// Each leaving volume no block names any more is gone, and each gone one the table forgets
    /// (docs/design/repair.md §4): asked of the root range once a pass settled, one ask a volume.
    fn retire_empty(&mut self, repair: &mut Repair, facts: &[Fact]) -> Result<(), NodeError> {
        let Some(root_group) = self.group_of(Layer::Root) else {
            return Ok(());
        };
        let Some(handle) = self.handle_of(Layer::Block) else {
            return Ok(());
        };
        for fact in facts {
            let volume = fact.placed.volume;
            let word = match fact.standing {
                VolumeStanding::Active => continue,
                VolumeStanding::Leaving => root::Command::Retire {
                    volume,
                    // The entry that carries it gives it its time.
                    at_ns: 0,
                },
                VolumeStanding::Gone => root::Command::Forget { volume },
            };
            if repair.retiring.iter().any(|(_, v)| *v == volume) || !self.command_room() {
                continue;
            }
            let Some(shell) = self.owner.get_mut(handle) else {
                return Ok(());
            };
            let named = block::on_volume(shell.machine_mut().engine_mut(), volume, None, 1)
                .map_err(|e| NodeError::Bounds(e.to_string()))?;
            if !named.is_empty() {
                eprintln!(
                    "REPAIRDBG {} retire-held volume={volume:x} standing={:?} named={:?}",
                    self.id, fact.standing, named
                );
                continue;
            }
            eprintln!(
                "REPAIRDBG {} retire-ask volume={volume:x} standing={:?}",
                self.id, fact.standing
            );
            let ask = self.command(root_group, Command::Root(word), u64::MAX, Wait::Request)?;
            repair.retiring.push((ask, volume));
        }
        Ok(())
    }

    /// The next block the pass walks, or none: a walk that found a higher margin to act on walks
    /// again at it; one that found none waits for its repairs and commands, and ends. The service
    /// is settled where it ended with the facts, `now` the root and Block ranges' applied indexes,
    /// where they were when it began: a repair that changed a block moved them, and the next pass
    /// begins at the lowest margin again.
    fn next_block(
        &mut self,
        repair: &mut Repair,
        now: (u64, u64),
    ) -> Result<Option<u128>, NodeError> {
        let Some(handle) = self.handle_of(Layer::Block) else {
            return Ok(None);
        };
        let Some(shell) = self.owner.get_mut(handle) else {
            return Ok(None);
        };
        let engine = shell.machine_mut().engine_mut();
        let meta = |e: mantle_meta::error::MetaError| NodeError::Bounds(e.to_string());
        let Some(pass) = repair.pass.as_mut() else {
            return Ok(None);
        };
        loop {
            if let Some(after) = pass.sweep {
                match block::blocks(engine, after, 1).map_err(meta)?.first() {
                    Some(&block) => {
                        pass.sweep = Some(Some(block));
                        return Ok(Some(block));
                    }
                    None => pass.sweep = None,
                }
                continue;
            }
            if let Some(&volume) = pass.volumes.get(pass.at) {
                match block::on_volume(engine, volume, pass.after, 1)
                    .map_err(meta)?
                    .first()
                {
                    Some(&(block, _)) => {
                        pass.after = Some(block);
                        if pass.level == 0 {
                            let count = pass.counting.entry(volume).or_insert(0);
                            *count = count.saturating_add(1);
                        }
                        return Ok(Some(block));
                    }
                    None => {
                        pass.at = pass.at.saturating_add(1);
                        pass.after = None;
                    }
                }
                continue;
            }
            // The walk is over.
            if let Some(level) = pass.higher.take() {
                pass.level = level;
                pass.at = 0;
                pass.after = None;
                if pass.sweeps {
                    pass.sweep = Some(None);
                }
                pass.acted = false;
                pass.waiting = 0;
                pass.short = 0;
                pass.lost = 0;
                continue;
            }
            // Its repairs and commands answered first, so their changes are the next pass's.
            if !repair.running.is_empty() || !repair.commanding.is_empty() {
                pass.walked = true;
                return Ok(None);
            }
            // The blocks each volume held, as this pass counted them, and the repairs at once that
            // rebuild them all within the allowance at the time a rebuild took.
            repair.counts = std::mem::take(&mut pass.counting);
            if let (Some(per), Some(allowance)) = (
                repair.per_block_ns,
                self.gateway
                    .as_ref()
                    .and_then(|g| allowance_ns(g.rates.repair))
                    .filter(|a| *a > 0),
            ) {
                let blocks: u64 = repair.counts.values().fold(0, |a, b| a.saturating_add(*b));
                let wanted = u128::from(per)
                    .saturating_mul(u128::from(blocks))
                    .div_ceil(u128::from(allowance));
                repair.concurrency = usize::try_from(wanted).unwrap_or(usize::MAX).max(1);
            }
            let ended_ns = self.wall.now_ns();
            let status = &mut repair.status;
            status.ended_ns = ended_ns;
            status.settled = (pass.root_applied, pass.block_applied) == now;
            status.root_applied = now.0;
            status.block_applied = now.1;
            status.waiting = pass.waiting;
            status.short = pass.short;
            status.lost = pass.lost;
            eprintln!(
                "REPAIRDBG {} pass-end settled={} began=({},{}) now=({},{}) waiting={} short={} lost={} rebuilt={} commands={}",
                self.id,
                status.settled,
                pass.root_applied,
                pass.block_applied,
                now.0,
                now.1,
                pass.waiting,
                pass.short,
                pass.lost,
                status.rebuilt,
                status.commands
            );
            repair.pass = None;
            return Ok(None);
        }
    }

    /// Plans `block` and acts on it where its margin is the pass's level: a change of rows asked,
    /// or a repairer begun; or, wanting room for either, the operation slots it waits for (none for
    /// a change of rows).
    fn consider(
        &mut self,
        repair: &mut Repair,
        block: u128,
        judged: &Judged,
        most: u32,
        block_group: u128,
    ) -> Result<Option<usize>, NodeError> {
        // A block a repair or a command already works on waits for it.
        if repair.running.iter().any(|r| r.repairer.chunk().0 == block)
            || repair.commanding.iter().any(|(_, b)| *b == block)
        {
            eprintln!("REPAIRDBG {} busy-skip block={block:x}", self.id);
            return Ok(None);
        }
        let Some((header, places)) = self.block_places(block)? else {
            return Ok(None);
        };
        // A block the walk reached already at its level, by the sweep or through a volume walked
        // before, was planned and counted then: once a walk.
        let again = repair.pass.as_ref().is_some_and(|pass| {
            pass.sweep.is_none()
                && (pass.sweeps
                    || places.iter().any(|p| {
                        pass.volumes
                            .get(..pass.at)
                            .is_some_and(|walked| walked.binary_search(&p.volume).is_ok())
                    }))
        });
        if again {
            return Ok(None);
        }
        let Ok(planned) = fix::plan(block, &header, &places, judged) else {
            eprintln!("REPAIRDBG {} plan-error block={block:x}", self.id);
            return Ok(None);
        };
        eprintln!(
            "REPAIRDBG {} plan block={block:x} level={:?} sweeping={:?} margin={} plan={:?}",
            self.id,
            repair.pass.as_ref().map(|p| p.level),
            repair.pass.as_ref().map(|p| p.sweep.is_some()),
            planned.margin,
            planned.plan
        );
        // A repair holds a chunk from each of the block's places at most, once a decode failed
        // (`repair::most_bytes`): an operation slot for each width of them, the memory of one
        // slot holding a block of its width's (`derive`'s test of it). A block holds at most as
        // many places as every slot holds.
        let width = usize::from(header.data).saturating_add(usize::from(header.parity));
        let slots = places.len().div_ceil(width.max(1));
        let objects = self.gateway.as_ref().map_or(0, |g| g.objects);
        let most = most.min(u32::try_from(objects.saturating_mul(width)).unwrap_or(u32::MAX));
        let starts = self.starts(repair, slots);
        let Some(pass) = repair.pass.as_mut() else {
            return Ok(None);
        };
        match planned.plan {
            Plan::Whole => {}
            Plan::Waiting => pass.waiting = pass.waiting.saturating_add(1),
            Plan::Unreadable { lost } => {
                pass.short = pass.short.saturating_add(1);
                if lost {
                    pass.lost = pass.lost.saturating_add(1);
                }
            }
            Plan::Command(_) | Plan::Rebuild { .. } if planned.margin > pass.level => {
                pass.higher = Some(
                    pass.higher
                        .map_or(planned.margin, |h| h.min(planned.margin)),
                );
            }
            Plan::Command(command) => {
                if !self.command_room() {
                    return Ok(Some(0));
                }
                pass.acted = true;
                let ask = self.command(
                    block_group,
                    Command::Block(command),
                    u64::MAX,
                    Wait::Request,
                )?;
                repair.commanding.push((ask, block));
            }
            Plan::Rebuild {
                index,
                commit,
                spread,
            } => {
                pass.acted = true;
                // Untimed, the service rebuilds one block at once to time a rebuild, and the rest
                // wait for its measure.
                let away = matches!(commit, fix::Commit::Keep { .. });
                let timing = away && repair.per_block_ns.is_none();
                if timing && repair.timing {
                    pass.waiting = pass.waiting.saturating_add(1);
                    return Ok(None);
                }
                if slots > objects {
                    // More places than every slot holds: past what any repair may read.
                    repair.status.unplaced = repair.status.unplaced.saturating_add(1);
                    return Ok(None);
                }
                if !starts {
                    return Ok(Some(slots));
                }
                match Repairer::new(block, header, places, judged, (index, commit, spread), most) {
                    Ok(repairer) => {
                        if timing {
                            repair.timing = true;
                            repair.status.timing = repair.status.timing.saturating_add(1);
                        }
                        let charge = self.repair_charge(slots);
                        if let Some(admission) = self.admission.as_mut() {
                            admission.took(charge);
                        }
                        repair.running.push(Running {
                            repairer,
                            pending: Vec::new(),
                            held: Vec::new(),
                            began_ns: self.wall.now_ns(),
                            charge,
                            timing,
                        });
                    }
                    Err(_) => repair.status.unreadable = repair.status.unreadable.saturating_add(1),
                }
            }
        }
        Ok(None)
    }

    /// `block`'s header and every place of its chunks, as this node's member of the Block range
    /// applied them.
    fn block_places(
        &mut self,
        block: u128,
    ) -> Result<Option<(BlockHeader, Vec<ChunkPlace>)>, NodeError> {
        let Some(handle) = self.handle_of(Layer::Block) else {
            return Ok(None);
        };
        let Some(shell) = self.owner.get_mut(handle) else {
            return Ok(None);
        };
        block::places(shell.machine_mut().engine_mut(), block)
            .map_err(|e| NodeError::Bounds(e.to_string()))
    }

    /// The row changes the service asked, as their answers come: each lets its block be planned
    /// again.
    fn take_commands(&mut self, repair: &mut Repair) {
        let Self { commands_done, .. } = self;
        repair
            .commanding
            .retain(|(ask, _)| match take_done(commands_done, *ask) {
                Some(CommandOutcome::Answered(RangeAnswer::Block(_))) => {
                    repair.status.commands = repair.status.commands.saturating_add(1);
                    false
                }
                Some(_) => false,
                None => true,
            });
        // A word to the root range answered, whatever it answered, is let go: the next settled
        // pass reads the table again and asks what it still needs, a retired volume's forgetting.
        repair
            .retiring
            .retain(|(ask, _)| take_done(commands_done, *ask).is_none());
    }

    /// Each repair's answers given it; a repair ended is counted and let go.
    fn take_repairs(&mut self, repair: &mut Repair, _block_group: u128) -> Result<(), NodeError> {
        let mut at = 0;
        while let Some(running) = repair.running.get_mut(at) {
            let mut answers = Vec::new();
            {
                let Self {
                    chunks_done,
                    commands_done,
                    reads_done,
                    ..
                } = &mut *self;
                running.pending.retain(|(pending, id, get)| {
                    let answer = match pending {
                        Pending::Chunk(ask) => {
                            take_done(chunks_done, *ask).map(|o| chunk_answer(o, *get))
                        }
                        Pending::Command(ask) => take_done(commands_done, *ask).map(command_answer),
                        Pending::Read(ask) => {
                            take_done(reads_done, *ask).map(|_| Answer::Unreadable)
                        }
                    };
                    match answer {
                        Some(answer) => {
                            answers.push((*id, answer));
                            false
                        }
                        None => true,
                    }
                });
            }
            for (id, answer) in answers {
                // An answer the repairer refuses ends it, as its outcome says.
                let _ = running.repairer.answer(id, answer);
            }
            if let Some(outcome) = running.repairer.outcome() {
                eprintln!(
                    "REPAIRDBG {} repair-end block={:x} index={} outcome={:?}",
                    self.id,
                    running.repairer.chunk().0,
                    running.repairer.chunk().1,
                    outcome
                );
                // A timing rebuild that named no chunk times none: the next pass begins another.
                if running.timing && !matches!(outcome, Ok(Repaired::Done { .. })) {
                    repair.timing = false;
                }
                let status = &mut repair.status;
                match outcome {
                    Ok(Repaired::Done { .. }) => {
                        status.rebuilt = status.rebuilt.saturating_add(1);
                        let took = self.wall.now_ns().saturating_sub(running.began_ns);
                        repair.per_block_ns =
                            Some(repair.per_block_ns.map_or(took, |per| per.max(took)));
                    }
                    // Rows changed alone, a place checked to hold the chunk taking another's.
                    Ok(Repaired::Verified { .. }) => {
                        status.commands = status.commands.saturating_add(1);
                    }
                    Ok(Repaired::Changed | Repaired::Released) => {}
                    Err(RepairError::Unplaced { .. } | RepairError::Full(_)) => {
                        status.unplaced = status.unplaced.saturating_add(1);
                    }
                    Err(RepairError::Corrupt(_)) => {
                        status.corrupt = status.corrupt.saturating_add(1);
                    }
                    Err(_) => status.unreadable = status.unreadable.saturating_add(1),
                }
                let ended = repair.running.swap_remove(at);
                if let Some(admission) = self.admission.as_mut() {
                    admission.gave(ended.charge);
                }
                self.abandoned
                    .extend(ended.pending.into_iter().map(|(p, _, _)| p));
            } else {
                at = at.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Each repair's requests asked as the node has room: a read or a put of a chunk on its volume's
    /// node, a placement from the facts, a command of the Block range.
    fn ask_repairs(
        &mut self,
        repair: &mut Repair,
        block_group: u128,
        facts: &[Fact],
    ) -> Result<(), NodeError> {
        let now = self.wall.now_ns();
        for running in &mut repair.running {
            let mut held = std::mem::take(&mut running.held);
            while let Some((id, request)) = running.repairer.poll() {
                held.push((id, request));
            }
            for (id, request) in held {
                let node_of = |volume: u128| {
                    facts
                        .iter()
                        .find(|f| f.placed.volume == volume)
                        .map(|f| f.placed.node)
                };
                let get = matches!(request, Request::Chunk { .. });
                let asked = match &request {
                    Request::Chunk { volume, key, len } => match node_of(*volume) {
                        None => {
                            let _ = running.repairer.answer(id, Answer::Unreadable);
                            continue;
                        }
                        Some(node) => self
                            .get_chunk(node, *volume, *key, (0, *len), 0)
                            .map(Pending::Chunk),
                    },
                    Request::Place {
                        block,
                        avoid,
                        spread,
                    } => {
                        let volumes = place::offer(*block, facts, avoid, *spread, now);
                        let _ = running.repairer.answer(id, Answer::Volumes(volumes));
                        continue;
                    }
                    Request::Put {
                        volume,
                        key,
                        bytes,
                        crc,
                    } => match node_of(*volume) {
                        None => {
                            let _ = running.repairer.answer(id, Answer::Refused { full: false });
                            continue;
                        }
                        Some(node) => self
                            .put_chunk(node, *volume, *key, bytes.clone(), (*crc, 0))
                            .map(Pending::Chunk),
                    },
                    Request::Block(command) => {
                        if !self.command_room() {
                            Err(NodeError::Asks)
                        } else {
                            self.command(
                                block_group,
                                Command::Block(command.clone()),
                                u64::MAX,
                                Wait::Request,
                            )
                            .map(Pending::Command)
                        }
                    }
                };
                match asked {
                    Ok(pending) => running.pending.push((pending, id, get)),
                    Err(NodeError::Asks) => running.held.push((id, request)),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }
}

/// A chunk's outcome as a repairer takes it: a read's bytes, or none; a put's storing, or its
/// refusal, a volume's `Full` told apart.
fn chunk_answer(outcome: ChunkOutcome, get: bool) -> Answer {
    match (outcome, get) {
        (ChunkOutcome::Read(bytes), true) => Answer::Chunk(Bytes::from(bytes)),
        (_, true) => Answer::Unreadable,
        (ChunkOutcome::Stored, false) => Answer::Stored,
        (ChunkOutcome::Refused(chunk::Status::Full), false) => Answer::Refused { full: true },
        (_, false) => Answer::Refused { full: false },
    }
}

/// A command's outcome as a repairer takes it: one that never applied is a block read again.
fn command_answer(outcome: CommandOutcome) -> Answer {
    match outcome {
        CommandOutcome::Answered(RangeAnswer::Block(outcome)) => Answer::Block(outcome),
        _ => Answer::Block(block::Outcome::Conflict),
    }
}
