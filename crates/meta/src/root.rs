//! The root range's volume table (docs/design/node.md §1.5, §5.6; architecture §4): the cell's
//! nodes, each with its failure domain, and its volumes, each with the node that holds it and its
//! bytes, which placement reads (node.md §7); and each node's history, the time it has run and its
//! runs that ended without an orderly stop, which every node's failure detectors start from
//! (node.md §2.4, item 7).
//!
//! A node enlists itself and its volumes as it starts. What the table does not hold is recorded;
//! what it holds otherwise, a node in another domain or a volume on another node, is a conflict
//! that changes nothing, since a device moved between nodes is a move to make, never a silent
//! overwrite. The table holds at most the nodes and volumes the cell states, as every map keyed by
//! a value a peer chooses is bounded (CLAUDE.md §2), and an enlistment past them is refused.
//!
//! The table also keeps what repair and placement judge by (docs/design/repair.md §2): each node's
//! presence as the cell has seen it, a fact the node that leads the range states from its own
//! failure detectors (`Seen`), with the time the node was seen and not seen over its enlisted life,
//! its measured availability; and each volume's standing. A node's enlistment names every volume
//! it holds, so a volume the table gave it and it no longer holds, its node having started without
//! it, is gone for good, a fact its own node stated; a node that leaves names its volumes leaving,
//! and each is gone once repair has moved every chunk it held. No volume is ever written off for
//! the time its node has been away.

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::key;
use crate::record::{Census, History, Member, Presence, VolumeRow, VolumeStanding, VolumeState};

/// The longest failure domain a node names, in bytes: a label such as a rack's or a zone's name.
pub const MAX_DOMAIN: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Enlist(Enlist),
    Lived(Lived),
    Seen(Seen),
    /// `node` leaves the cell's storage: each of its active volumes takes no new chunk, and
    /// repair moves what each holds to the others (docs/design/repair.md §4). From the node
    /// itself.
    Leave {
        node: u64,
        at_ns: u64,
    },
    /// A leaving volume that holds no chunk any block names is gone: its asker, the cell's repair,
    /// found none.
    Retire {
        volume: u128,
        at_ns: u64,
    },
    /// A gone volume no block names any more leaves the table, which holds the volumes a cell
    /// makes over its life no longer than they matter (CLAUDE.md §2): its asker, the cell's
    /// repair, found it named nowhere.
    Forget {
        volume: u128,
    },
}

/// Whether the cell sees `node` now, as the node that leads the root range judges it by its
/// failure detectors and its connection to it (docs/design/repair.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seen {
    pub node: u64,
    pub present: bool,
    /// The entry's time, which the command takes.
    pub at_ns: u64,
}

/// A node's history as the node states it: the totals over all its runs (node.md §2.4, item 7).
/// The table keeps the greater of what it holds and what is stated, for each, so a statement
/// applied late or twice takes nothing back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lived {
    pub node: u64,
    /// The time it has run, in nanoseconds.
    pub exposure_ns: u64,
    /// Its runs that ended without an orderly stop.
    pub crashes: u64,
}

/// A node and the volumes it holds, as it starts (node.md §5.6): every one, so a volume the table
/// gave the node and this does not name is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enlist {
    pub node: u64,
    /// The node's failure domain, at most [`MAX_DOMAIN`] bytes.
    pub domain: String,
    /// Each volume's id and its bytes.
    pub volumes: Vec<(u128, u64)>,
    /// The most nodes and volumes the table holds, as the cell states them.
    pub max_nodes: u32,
    pub max_volumes: u32,
    /// The entry's time, which the command takes.
    pub at_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The table holds the node and each volume named as the enlistment states them.
    Enlisted,
    /// The table holds the node in another domain, or a volume on another node or of other
    /// bytes, or one gone, or the enlistment names a volume twice or a domain longer than
    /// [`MAX_DOMAIN`]; or a volume retired is not leaving: nothing changed.
    Conflict,
    /// The table would hold more nodes or volumes than the cell states: nothing changed.
    Full,
    /// The node's history holds at least what was stated.
    Kept,
    /// The table holds no such node, whose history it would keep: nothing changed.
    Unknown,
}

/// A volume as the table holds it, with its node's failure domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub volume: u128,
    pub node: u64,
    pub domain: String,
    pub bytes: u64,
}

/// A volume as placement and repair judge it: where it is, its standing, and its node's presence
/// as the cell has seen it (docs/design/repair.md §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    pub placed: Placed,
    pub standing: VolumeStanding,
    pub presence: Presence,
}

impl Fact {
    /// Whether its node is seen now and the volume holds its chunks: what a read of it or a
    /// write to it can reach.
    pub fn reachable(&self) -> bool {
        self.presence.present && self.standing != VolumeStanding::Gone
    }

    /// Whether it takes new chunks now: active, and its node seen.
    pub fn takes(&self) -> bool {
        self.presence.present && self.standing == VolumeStanding::Active
    }
}

/// The time `presence`'s node was seen and not seen over its enlisted life to `now_ns`, the
/// stretch under way counted: its measured availability is the first over both.
pub fn seen(presence: &Presence, now_ns: u64) -> (u64, u64) {
    let stretch = now_ns.saturating_sub(presence.since_ns);
    if presence.present {
        (
            presence.present_ns.saturating_add(stretch),
            presence.away_ns,
        )
    } else {
        (
            presence.present_ns,
            presence.away_ns.saturating_add(stretch),
        )
    }
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Enlist(e) => enlist(engine, e)?,
        Command::Lived(l) => lived(engine, l)?,
        Command::Seen(s) => seen_now(engine, s)?,
        Command::Leave { node, at_ns } => leave(engine, *node, *at_ns)?,
        Command::Retire { volume, at_ns } => retire(engine, *volume, *at_ns)?,
        Command::Forget { volume } => forget(engine, *volume)?,
    };
    engine.apply(index, writes)?;
    Ok(outcome)
}

fn enlist<E: Rows>(engine: &mut E, e: &Enlist) -> Result<(Outcome, Vec<Write>), MetaError> {
    let refused = |outcome| Ok((outcome, Vec::new()));
    if e.domain.len() > MAX_DOMAIN {
        return refused(Outcome::Conflict);
    }
    let mut census = census(engine)?;
    let mut writes = reserved(e.volumes.len().saturating_add(2))?;
    match engine.get(&key::member(e.node))? {
        Some(bytes) if Member::decode(&bytes)?.domain != e.domain => {
            return refused(Outcome::Conflict);
        }
        Some(_) => {}
        None => {
            census.nodes = census.nodes.checked_add(1).ok_or(MetaError::Corrupt)?;
            let member = Member {
                domain: e.domain.clone(),
            };
            writes.push(Write::Put(key::member(e.node), member.encode()?));
        }
    }
    for (at, (volume, bytes)) in e.volumes.iter().enumerate() {
        if e.volumes
            .get(..at)
            .unwrap_or_default()
            .iter()
            .any(|(v, _)| v == volume)
        {
            return refused(Outcome::Conflict);
        }
        let row = VolumeRow {
            node: e.node,
            bytes: *bytes,
        };
        match engine.get(&key::volume(*volume))? {
            Some(held) if VolumeRow::decode(&held)? != row => return refused(Outcome::Conflict),
            // A volume the cell wrote off does not come back with what it held then: its
            // chunks were rebuilt elsewhere, and its node starts again without it.
            Some(_) if standing(engine, *volume)? == VolumeStanding::Gone => {
                return refused(Outcome::Conflict);
            }
            Some(_) => {}
            None => {
                census.volumes = census.volumes.checked_add(1).ok_or(MetaError::Corrupt)?;
                writes.push(Write::Put(key::volume(*volume), row.encode()));
            }
        }
    }
    if census.nodes > e.max_nodes || census.volumes > e.max_volumes {
        return refused(Outcome::Full);
    }
    let (now, clock) = clock::tick(engine, e.at_ns)?;
    // The node states every volume it holds: one the table gave it and it does not name, it
    // started without, and the volume is gone. The table's volumes are at most its census's.
    let most = usize::try_from(census.volumes).map_err(|_| MetaError::Corrupt)?;
    for placed in volumes(engine, None, most)? {
        if placed.node == e.node
            && !e.volumes.iter().any(|(v, _)| *v == placed.volume)
            && standing(engine, placed.volume)? != VolumeStanding::Gone
        {
            let gone = VolumeState {
                state: VolumeStanding::Gone,
                at_ns: now,
            };
            writes.push(Write::Put(key::volume_state(placed.volume), gone.encode()));
        }
    }
    // The node speaks, so the cell sees it.
    if let Some(presence) = seen_at(engine, e.node, true, now)? {
        writes.push(Write::Put(key::presence(e.node), presence.encode()));
    }
    writes.push(clock);
    writes.push(Write::Put(key::census(), census.encode()));
    Ok((Outcome::Enlisted, writes))
}

/// A volume's standing: active unless its row says otherwise.
pub fn standing<E: Rows>(engine: &mut E, volume: u128) -> Result<VolumeStanding, MetaError> {
    match engine.get(&key::volume_state(volume))? {
        None => Ok(VolumeStanding::Active),
        Some(bytes) => Ok(VolumeState::decode(&bytes)?.state),
    }
}

/// A node's presence as the table holds it: seen since its enlistment where it holds none, a
/// table older than presence rows having enlisted it.
pub fn presence<E: Rows>(engine: &mut E, node: u64) -> Result<Presence, MetaError> {
    match engine.get(&key::presence(node))? {
        Some(bytes) => Ok(Presence::decode(&bytes)?),
        None => Ok(Presence {
            present: true,
            ..Presence::default()
        }),
    }
}

/// `node`'s presence once the cell sees it, or not, at `now`: the stretch that ends counted to
/// the side it was on. `None` when it is already so, which changes nothing.
fn seen_at<E: Rows>(
    engine: &mut E,
    node: u64,
    present: bool,
    now: u64,
) -> Result<Option<Presence>, MetaError> {
    let held = match engine.get(&key::presence(node))? {
        Some(bytes) => Presence::decode(&bytes)?,
        // First seen: present from now.
        None if present => {
            return Ok(Some(Presence {
                present: true,
                since_ns: now,
                ..Presence::default()
            }));
        }
        None => Presence {
            present: true,
            since_ns: now,
            ..Presence::default()
        },
    };
    if held.present == present {
        return Ok(None);
    }
    let stretch = now.saturating_sub(held.since_ns);
    let mut next = Presence {
        present,
        since_ns: now,
        ..held
    };
    if held.present {
        next.present_ns = held.present_ns.saturating_add(stretch);
        next.absences = held.absences.saturating_add(1);
    } else {
        next.away_ns = held.away_ns.saturating_add(stretch);
        next.longest_away_ns = held.longest_away_ns.max(stretch);
    }
    Ok(Some(next))
}

/// The cell's word on whether it sees a node: kept for an enlisted node only.
fn seen_now<E: Rows>(engine: &mut E, s: &Seen) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::member(s.node))?.is_none() {
        return Ok((Outcome::Unknown, Vec::new()));
    }
    let (now, clock) = clock::tick(engine, s.at_ns)?;
    let mut writes = reserved(2)?;
    if let Some(presence) = seen_at(engine, s.node, s.present, now)? {
        writes.push(Write::Put(key::presence(s.node), presence.encode()));
        writes.push(clock);
    }
    Ok((Outcome::Kept, writes))
}

/// A node leaving: each of its active volumes is leaving from now.
fn leave<E: Rows>(
    engine: &mut E,
    node: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::member(node))?.is_none() {
        return Ok((Outcome::Unknown, Vec::new()));
    }
    let census = census(engine)?;
    let most = usize::try_from(census.volumes).map_err(|_| MetaError::Corrupt)?;
    let (now, clock) = clock::tick(engine, at_ns)?;
    let mut writes = Vec::new();
    for placed in volumes(engine, None, most)? {
        if placed.node == node && standing(engine, placed.volume)? == VolumeStanding::Active {
            let leaving = VolumeState {
                state: VolumeStanding::Leaving,
                at_ns: now,
            };
            writes.push(Write::Put(
                key::volume_state(placed.volume),
                leaving.encode(),
            ));
        }
    }
    if !writes.is_empty() {
        writes.push(clock);
    }
    Ok((Outcome::Kept, writes))
}

/// A leaving volume written off, now that repair has moved what it held: gone. A volume gone
/// already is kept so; any other is a conflict.
fn retire<E: Rows>(
    engine: &mut E,
    volume: u128,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::volume(volume))?.is_none() {
        return Ok((Outcome::Unknown, Vec::new()));
    }
    match standing(engine, volume)? {
        VolumeStanding::Gone => Ok((Outcome::Kept, Vec::new())),
        VolumeStanding::Active => Ok((Outcome::Conflict, Vec::new())),
        VolumeStanding::Leaving => {
            let (now, clock) = clock::tick(engine, at_ns)?;
            let gone = VolumeState {
                state: VolumeStanding::Gone,
                at_ns: now,
            };
            Ok((
                Outcome::Kept,
                vec![Write::Put(key::volume_state(volume), gone.encode()), clock],
            ))
        }
    }
}

/// A node's history kept: only an enlisted node's, so the histories are as many as the table's
/// nodes.
fn lived<E: Rows>(engine: &mut E, l: &Lived) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::member(l.node))?.is_none() {
        return Ok((Outcome::Unknown, Vec::new()));
    }
    let held = match engine.get(&key::history(l.node))? {
        Some(bytes) => History::decode(&bytes)?,
        None => History::default(),
    };
    let kept = History {
        exposure_ns: held.exposure_ns.max(l.exposure_ns),
        crashes: held.crashes.max(l.crashes),
    };
    let mut writes = reserved(1)?;
    if kept != held {
        writes.push(Write::Put(key::history(l.node), kept.encode()));
    }
    Ok((Outcome::Kept, writes))
}

/// The cell's history: every node's time run and crashes, summed, read at most `max` nodes'. Sums
/// saturate: past `u64::MAX` nanoseconds (584 years) of node time or that many crashes, more
/// changes no estimate made from them.
pub fn history<E: Rows>(engine: &mut E, max: usize) -> Result<History, MetaError> {
    let (mut from, to) = key::history_rows();
    let mut sum = History::default();
    for _ in 0..max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let row = History::decode(&v)?;
        sum.exposure_ns = sum.exposure_ns.saturating_add(row.exposure_ns);
        sum.crashes = sum.crashes.saturating_add(row.crashes);
        from = k;
        from.push(0);
    }
    Ok(sum)
}

/// Node `node`'s history as the table holds it: none before it stated one.
pub fn history_of<E: Rows>(engine: &mut E, node: u64) -> Result<History, MetaError> {
    match engine.get(&key::history(node))? {
        Some(bytes) => Ok(History::decode(&bytes)?),
        None => Ok(History::default()),
    }
}

/// How many nodes and volumes the table holds.
pub fn census<E: Rows>(engine: &mut E) -> Result<Census, MetaError> {
    match engine.get(&key::census())? {
        None => Ok(Census::default()),
        Some(bytes) => Ok(Census::decode(&bytes)?),
    }
}

/// The table's volumes after `after`, or from the first, in the order of their ids, at most
/// `max`: each with its node's failure domain.
pub fn volumes<E: Rows>(
    engine: &mut E,
    after: Option<u128>,
    max: usize,
) -> Result<Vec<Placed>, MetaError> {
    let (mut from, to) = key::volume_rows(after);
    let mut out: Vec<Placed> = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let volume = key::decode_volume(&k).ok_or(MetaError::Corrupt)?;
        let row = VolumeRow::decode(&v)?;
        // A node's volumes are enlisted together, so its domain is usually the last one read.
        let domain = match out.last().filter(|p| p.node == row.node) {
            Some(p) => p.domain.clone(),
            None => {
                let member = engine
                    .get(&key::member(row.node))?
                    .ok_or(MetaError::Corrupt)?;
                Member::decode(&member)?.domain
            }
        };
        out.push(Placed {
            volume,
            node: row.node,
            domain,
            bytes: row.bytes,
        });
        from = k;
        from.push(0);
    }
    Ok(out)
}

/// A gone volume taken out of the table, its count with it; one the table does not hold is forgotten
/// already, and any other is a conflict.
fn forget<E: Rows>(engine: &mut E, volume: u128) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::volume(volume))?.is_none() {
        return Ok((Outcome::Kept, Vec::new()));
    }
    if standing(engine, volume)? != VolumeStanding::Gone {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let mut census = census(engine)?;
    census.volumes = census.volumes.checked_sub(1).ok_or(MetaError::Corrupt)?;
    Ok((
        Outcome::Kept,
        vec![
            Write::Delete(key::volume(volume)),
            Write::Delete(key::volume_state(volume)),
            Write::Put(key::census(), census.encode()),
        ],
    ))
}

/// The table's volumes after `after`, or from the first, in the order of their ids, at most
/// `max`, each with its standing and its node's presence: what placement and repair judge by.
pub fn facts<E: Rows>(
    engine: &mut E,
    after: Option<u128>,
    max: usize,
) -> Result<Vec<Fact>, MetaError> {
    let placed = volumes(engine, after, max)?;
    let mut out = reserved(placed.len())?;
    // A node's volumes are enlisted together and read in a run, so its presence is read once.
    let mut last: Option<(u64, Presence)> = None;
    for p in placed {
        let seen = match last {
            Some((node, held)) if node == p.node => held,
            _ => presence(engine, p.node)?,
        };
        last = Some((p.node, seen));
        out.push(Fact {
            standing: standing(engine, p.volume)?,
            presence: seen,
            placed: p,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;

    fn enlist(node: u64, domain: &str, volumes: &[(u128, u64)]) -> Command {
        Command::Enlist(Enlist {
            node,
            domain: domain.into(),
            volumes: volumes.to_vec(),
            max_nodes: 3,
            max_volumes: 4,
            at_ns: 0,
        })
    }

    fn placed(volume: u128, node: u64, domain: &str, bytes: u64) -> Placed {
        Placed {
            volume,
            node,
            domain: domain.into(),
            bytes,
        }
    }

    #[test]
    fn nodes_and_volumes_enlisted_read_back_with_their_domains_in_order() {
        let mut m = Model::default();
        let first = enlist(2, "rack-b", &[(30, 1 << 30), (10, 1 << 20)]);
        assert_eq!(apply(&mut m, 1, &first).unwrap(), Outcome::Enlisted);
        let second = enlist(1, "rack-a", &[(20, 1 << 25)]);
        assert_eq!(apply(&mut m, 2, &second).unwrap(), Outcome::Enlisted);
        assert_eq!(
            volumes(&mut m, None, 10).unwrap(),
            vec![
                placed(10, 2, "rack-b", 1 << 20),
                placed(20, 1, "rack-a", 1 << 25),
                placed(30, 2, "rack-b", 1 << 30),
            ]
        );
        // A page at a time, each after the last one read.
        assert_eq!(
            volumes(&mut m, Some(10), 1).unwrap(),
            vec![placed(20, 1, "rack-a", 1 << 25)]
        );
        assert!(volumes(&mut m, Some(30), 10).unwrap().is_empty());
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 2,
                volumes: 3
            }
        );
    }

    #[test]
    fn an_enlistment_again_changes_nothing_and_a_new_volume_joins_its_node() {
        let mut m = Model::default();
        let first = enlist(1, "rack-a", &[(10, 100)]);
        assert_eq!(apply(&mut m, 1, &first).unwrap(), Outcome::Enlisted);
        assert_eq!(apply(&mut m, 2, &first).unwrap(), Outcome::Enlisted);
        let more = enlist(1, "rack-a", &[(10, 100), (11, 200)]);
        assert_eq!(apply(&mut m, 3, &more).unwrap(), Outcome::Enlisted);
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 1,
                volumes: 2
            }
        );
    }

    #[test]
    fn a_node_in_another_domain_or_a_volume_held_otherwise_conflicts_and_changes_nothing() {
        let mut m = Model::default();
        assert_eq!(
            apply(&mut m, 1, &enlist(1, "rack-a", &[(10, 100)])).unwrap(),
            Outcome::Enlisted
        );
        let before = volumes(&mut m, None, 10).unwrap();
        for (index, command) in [
            // The node, in another domain.
            enlist(1, "rack-z", &[]),
            // The volume, on another node.
            enlist(2, "rack-b", &[(12, 5), (10, 100)]),
            // The volume, of other bytes.
            enlist(1, "rack-a", &[(10, 101)]),
            // A volume named twice.
            enlist(3, "rack-c", &[(13, 1), (13, 1)]),
            // A domain past its bound.
            enlist(4, &"d".repeat(MAX_DOMAIN + 1), &[]),
        ]
        .iter()
        .enumerate()
        {
            let index = 2 + index as u64;
            assert_eq!(apply(&mut m, index, command).unwrap(), Outcome::Conflict);
        }
        assert_eq!(volumes(&mut m, None, 10).unwrap(), before);
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 1,
                volumes: 1
            }
        );
    }

    #[test]
    fn an_enlistment_past_the_cells_nodes_or_volumes_is_full_and_changes_nothing() {
        let mut m = Model::default();
        for node in 1..=3 {
            let volume = u128::from(node);
            let command = enlist(node, "d", &[(volume, 1)]);
            assert_eq!(apply(&mut m, node, &command).unwrap(), Outcome::Enlisted);
        }
        // A fourth node.
        assert_eq!(
            apply(&mut m, 4, &enlist(4, "d", &[])).unwrap(),
            Outcome::Full
        );
        // A fourth volume fits; a fifth and sixth do not, and neither is kept.
        assert_eq!(
            apply(&mut m, 5, &enlist(1, "d", &[(5, 1), (6, 1)])).unwrap(),
            Outcome::Full
        );
        assert_eq!(
            apply(&mut m, 6, &enlist(1, "d", &[(5, 1)])).unwrap(),
            Outcome::Enlisted
        );
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 3,
                volumes: 4
            }
        );
        assert_eq!(volumes(&mut m, None, 10).unwrap().len(), 4);
    }

    fn lived(node: u64, exposure_ns: u64, crashes: u64) -> Command {
        Command::Lived(Lived {
            node,
            exposure_ns,
            crashes,
        })
    }

    #[test]
    fn a_nodes_history_keeps_the_greater_of_each_total_and_the_cells_is_their_sum() {
        let mut m = Model::default();
        assert_eq!(
            apply(&mut m, 1, &enlist(1, "d", &[])).unwrap(),
            Outcome::Enlisted
        );
        assert_eq!(
            apply(&mut m, 2, &enlist(2, "d", &[])).unwrap(),
            Outcome::Enlisted
        );
        assert_eq!(apply(&mut m, 3, &lived(1, 100, 1)).unwrap(), Outcome::Kept);
        // Stated late or twice: a smaller time takes nothing back, a larger count is kept.
        assert_eq!(apply(&mut m, 4, &lived(1, 50, 2)).unwrap(), Outcome::Kept);
        assert_eq!(apply(&mut m, 5, &lived(1, 50, 2)).unwrap(), Outcome::Kept);
        assert_eq!(apply(&mut m, 6, &lived(2, 30, 0)).unwrap(), Outcome::Kept);
        assert_eq!(
            history_of(&mut m, 1).unwrap(),
            History {
                exposure_ns: 100,
                crashes: 2
            }
        );
        assert_eq!(history_of(&mut m, 3).unwrap(), History::default());
        assert_eq!(
            history(&mut m, 10).unwrap(),
            History {
                exposure_ns: 130,
                crashes: 2
            }
        );
    }

    #[test]
    fn the_history_of_a_node_the_table_does_not_hold_is_refused_and_changes_nothing() {
        let mut m = Model::default();
        assert_eq!(
            apply(&mut m, 1, &enlist(1, "d", &[])).unwrap(),
            Outcome::Enlisted
        );
        assert_eq!(
            apply(&mut m, 2, &lived(9, 100, 1)).unwrap(),
            Outcome::Unknown
        );
        assert_eq!(history(&mut m, 10).unwrap(), History::default());
        assert_eq!(history_of(&mut m, 9).unwrap(), History::default());
    }

    #[test]
    fn the_cells_history_reads_at_most_the_nodes_asked_and_its_sums_saturate() {
        let mut m = Model::default();
        for node in 1..=3 {
            let index = 2 * node;
            let enlisted = apply(&mut m, index - 1, &enlist(node, "d", &[])).unwrap();
            assert_eq!(enlisted, Outcome::Enlisted);
            let kept = apply(&mut m, index, &lived(node, u64::MAX, node)).unwrap();
            assert_eq!(kept, Outcome::Kept);
        }
        assert_eq!(
            history(&mut m, 2).unwrap(),
            History {
                exposure_ns: u64::MAX,
                crashes: 3
            }
        );
        assert_eq!(history(&mut m, 0).unwrap(), History::default());
        assert_eq!(history(&mut m, 3).unwrap().crashes, 6);
    }

    fn at(command: Command, at_ns: u64) -> Command {
        match command {
            Command::Enlist(e) => Command::Enlist(Enlist { at_ns, ..e }),
            other => other,
        }
    }

    fn seen_command(node: u64, present: bool, at_ns: u64) -> Command {
        Command::Seen(Seen {
            node,
            present,
            at_ns,
        })
    }

    fn standings(m: &mut Model) -> Vec<(u128, VolumeStanding)> {
        facts(m, None, 10)
            .unwrap()
            .into_iter()
            .map(|f| (f.placed.volume, f.standing))
            .collect()
    }

    /// A node states every volume it holds as it starts: one the table gave it that it no longer
    /// names is gone, and a gone volume named again is refused, changing nothing.
    #[test]
    fn a_volume_its_node_started_without_is_gone_and_never_comes_back() {
        let mut m = Model::default();
        let first = at(enlist(1, "a", &[(10, 100), (11, 100)]), 1_000);
        assert_eq!(apply(&mut m, 1, &first).unwrap(), Outcome::Enlisted);
        assert_eq!(
            standings(&mut m),
            [(10, VolumeStanding::Active), (11, VolumeStanding::Active)]
        );
        // Started again without volume 10, and with a fresh one.
        let again = at(enlist(1, "a", &[(11, 100), (12, 100)]), 2_000);
        assert_eq!(apply(&mut m, 2, &again).unwrap(), Outcome::Enlisted);
        assert_eq!(
            standings(&mut m),
            [
                (10, VolumeStanding::Gone),
                (11, VolumeStanding::Active),
                (12, VolumeStanding::Active)
            ]
        );
        // The gone volume named again: refused whole, nothing changed.
        let back = at(enlist(1, "a", &[(10, 100), (11, 100), (12, 100)]), 3_000);
        assert_eq!(apply(&mut m, 3, &back).unwrap(), Outcome::Conflict);
        assert_eq!(standings(&mut m)[0], (10, VolumeStanding::Gone));
        // Another node's volumes are never its to write off.
        let other = at(enlist(2, "b", &[(20, 100)]), 4_000);
        assert_eq!(apply(&mut m, 4, &other).unwrap(), Outcome::Enlisted);
        assert_eq!(standings(&mut m).len(), 4);
        assert!(standings(&mut m).contains(&(11, VolumeStanding::Active)));
    }

    /// The cell's word on a node: each change counts the stretch that ends to the side it was on,
    /// the absences and the longest of them, so the node's availability is measured; a word that
    /// changes nothing writes nothing, and a node the table does not hold is unknown.
    #[test]
    fn presence_counts_the_time_seen_and_not_seen() {
        let mut m = Model::default();
        assert_eq!(
            apply(&mut m, 1, &at(enlist(1, "a", &[(10, 1)]), 1_000)).unwrap(),
            Outcome::Enlisted
        );
        let p = presence(&mut m, 1).unwrap();
        assert!(p.present);
        assert_eq!(p.since_ns, 1_000);
        assert_eq!(
            apply(&mut m, 2, &seen_command(1, true, 1_500)).unwrap(),
            Outcome::Kept
        );
        assert_eq!(
            presence(&mut m, 1).unwrap(),
            p,
            "seen again changes nothing"
        );
        apply(&mut m, 3, &seen_command(1, false, 3_000)).unwrap();
        apply(&mut m, 4, &seen_command(1, true, 3_400)).unwrap();
        apply(&mut m, 5, &seen_command(1, false, 4_000)).unwrap();
        apply(&mut m, 6, &seen_command(1, true, 5_000)).unwrap();
        let p = presence(&mut m, 1).unwrap();
        assert_eq!(
            p,
            Presence {
                present: true,
                since_ns: 5_000,
                present_ns: 2_000 + 600,
                away_ns: 400 + 1_000,
                absences: 2,
                longest_away_ns: 1_000,
            }
        );
        assert_eq!(seen(&p, 6_000), (3_600, 1_400));
        assert_eq!(
            apply(&mut m, 7, &seen_command(9, false, 6_000)).unwrap(),
            Outcome::Unknown
        );
        // A word proposed behind the range's clock takes the clock's next instant.
        apply(&mut m, 8, &seen_command(1, false, 10)).unwrap();
        let p = presence(&mut m, 1).unwrap();
        assert!(!p.present);
        assert!(p.since_ns > 5_000);
    }

    /// A node that leaves names its active volumes leaving; a leaving volume retired is gone, and
    /// an active one retired is a conflict.
    #[test]
    fn a_leaving_volume_is_retired_once_and_an_active_one_is_not() {
        let mut m = Model::default();
        apply(&mut m, 1, &at(enlist(1, "a", &[(10, 1), (11, 1)]), 1)).unwrap();
        apply(&mut m, 2, &at(enlist(2, "b", &[(20, 1)]), 2)).unwrap();
        let retire = |volume| Command::Retire { volume, at_ns: 5 };
        assert_eq!(apply(&mut m, 3, &retire(20)).unwrap(), Outcome::Conflict);
        let leave = Command::Leave { node: 1, at_ns: 3 };
        assert_eq!(apply(&mut m, 4, &leave).unwrap(), Outcome::Kept);
        assert_eq!(
            standings(&mut m),
            [
                (10, VolumeStanding::Leaving),
                (11, VolumeStanding::Leaving),
                (20, VolumeStanding::Active)
            ]
        );
        assert_eq!(apply(&mut m, 5, &retire(10)).unwrap(), Outcome::Kept);
        assert_eq!(apply(&mut m, 6, &retire(10)).unwrap(), Outcome::Kept);
        assert_eq!(standings(&mut m)[0], (10, VolumeStanding::Gone));
        assert_eq!(apply(&mut m, 7, &retire(99)).unwrap(), Outcome::Unknown);
        let stranger = Command::Leave { node: 9, at_ns: 7 };
        assert_eq!(apply(&mut m, 8, &stranger).unwrap(), Outcome::Unknown);
        // A leaving node that starts again keeps its volumes leaving.
        apply(&mut m, 9, &at(enlist(1, "a", &[(11, 1)]), 9)).unwrap();
        assert_eq!(standings(&mut m)[1], (11, VolumeStanding::Leaving));
    }

    /// A gone volume leaves the table once no block names it, and the count with it, so the
    /// volumes a cell makes over its life take no room past those that matter; an active one
    /// stays.
    #[test]
    fn a_gone_volume_is_forgotten_and_an_active_one_is_not() {
        let mut m = Model::default();
        apply(&mut m, 1, &at(enlist(1, "a", &[(10, 1)]), 1)).unwrap();
        apply(&mut m, 2, &at(enlist(1, "a", &[(11, 1)]), 2)).unwrap();
        assert_eq!(census(&mut m).unwrap().volumes, 2);
        let forget = |volume| Command::Forget { volume };
        assert_eq!(apply(&mut m, 3, &forget(11)).unwrap(), Outcome::Conflict);
        assert_eq!(apply(&mut m, 4, &forget(10)).unwrap(), Outcome::Kept);
        assert_eq!(apply(&mut m, 5, &forget(10)).unwrap(), Outcome::Kept);
        assert_eq!(standings(&mut m), [(11, VolumeStanding::Active)]);
        assert_eq!(census(&mut m).unwrap().volumes, 1);
    }
}
