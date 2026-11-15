//! The root range's volume table (docs/design/node.md §1.5, §5.6; architecture §4): the cell's
//! nodes, each with its failure domain, and its volumes, each with the node that holds it and its
//! bytes, which placement reads (node.md §7); each node's history, the time it has run and its
//! runs that ended without an orderly stop, which every node's failure detectors start from
//! (node.md §2.4, item 7); and each member's standing and the grant window (docs/design/membership.md
//! §1–§5).
//!
//! A node enlists itself and its volumes as it starts. What the table does not hold is recorded;
//! what it holds otherwise, a node in another domain or a volume on another node, is a conflict
//! that changes nothing, since a device moved between nodes is a move to make, never a silent
//! overwrite. The table holds at most the nodes and volumes the cell states, as every map keyed by
//! a value a peer chooses is bounded (CLAUDE.md §2), and an enlistment past them is refused.
//!
//! A node whose certificate carries a grant joins instead (membership.md §2–§3): its grant spent in
//! the window, its standing `Serving`. It leaves in order (`Leaving`), or is judged or stated
//! `Gone`, and is removed once no volume of it is left, which repair retires (membership.md §8):
//! `Removed`, its identity refused, its row kept until no range's configuration names it, then
//! delisted. Every member's absence is recorded at its entry's time, and cleared at its return
//! (membership.md §5): the fact a judgment of gone counts from.

use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::key;
use crate::record::{
    Census, FINGERPRINT, Grants, History, Member, MemberState as State, Membership, VolumeRow,
};

/// The longest failure domain a node names, in bytes: a label such as a rack's or a zone's name.
pub const MAX_DOMAIN: usize = 255;

/// The longest place a member is dialed at, `HOST:PORT`, in bytes: a host of at most 255 octets
/// (RFC 1035 §2.3.4, as [`MAX_DOMAIN`]) in brackets where it is an IPv6 address (RFC 3986 §3.2.2),
/// a colon, and a port of at most five digits (a `u16`).
pub const MAX_REACH: usize = MAX_DOMAIN + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Enlist(Enlist),
    Lived(Lived),
    /// A node joins under its grant, or restates where it is dialed (membership.md §3).
    Join(Join),
    /// A member asks to leave in order (membership.md §4).
    Leave {
        node: u64,
    },
    /// A member gone for good (membership.md §5): judged so by the root range's leader, its
    /// recorded absence past what the cell's durability model lets a chunk stay lost, or stated so
    /// by an operator, a supervisor or repair.
    Gone {
        node: u64,
    },
    /// A member leaving or gone, with no volume left, removed (membership.md §4, §5).
    Remove {
        node: u64,
    },
    /// A removed member that no range's configuration names, delisted: its rows go
    /// (membership.md §1).
    Delist {
        node: u64,
    },
    /// A volume of a member leaving or gone, emptied by repair, taken out of the table
    /// (membership.md §8).
    Retire {
        volume: u128,
    },
    /// A member's absence, recorded at its entry's time `at_ns` (membership.md §5).
    Absent {
        node: u64,
        at_ns: u64,
    },
    /// A member back: its recorded absence cleared.
    Returned {
        node: u64,
    },
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

/// A node and the volumes it holds, as it starts (node.md §5.6).
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
}

/// A node joining under its grant, as it starts (membership.md §3): an enlistment, the grant its
/// certificate carries and that certificate's SHA-256, and where its peers dial it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub node: u64,
    pub grant: u64,
    pub fingerprint: [u8; FINGERPRINT],
    /// Its QUIC endpoint's and its plane's places, `HOST:PORT`, at most [`MAX_REACH`] bytes each.
    pub quic: String,
    pub plane: String,
    /// Its failure domain, at most [`MAX_DOMAIN`] bytes.
    pub domain: String,
    /// Each volume's id and its bytes.
    pub volumes: Vec<(u128, u64)>,
    /// The most nodes and volumes the table holds, as the cell states them: the grant window is
    /// `max_nodes` wide (membership.md §2).
    pub max_nodes: u32,
    pub max_volumes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The table holds the node and each volume named as the enlistment states them.
    Enlisted,
    /// The table holds the node in another domain, or a volume on another node or of other
    /// bytes, or the enlistment names a volume twice or a domain longer than [`MAX_DOMAIN`]; or a
    /// join names a member under another grant or certificate, or a place past [`MAX_REACH`]; or a
    /// volume retired is a serving member's: nothing changed.
    Conflict,
    /// The table would hold more nodes or volumes than the cell states: nothing changed.
    Full,
    /// The node's history holds at least what was stated.
    Kept,
    /// The table holds no such node, whose history it would keep: nothing changed.
    Unknown,
    /// The member serves, as its join states it.
    Joined,
    /// The member leaves in order: a leave taken, or a join of a member leaving, which resumes it.
    Leaving,
    /// The member is gone: judged or stated so, or a join of a gone member, refused.
    Gone,
    /// The member is removed: its identity refused, its row kept until no range's
    /// configuration names it.
    Removed,
    /// The removed member's rows are gone, its history added to the removed nodes' totals.
    Delisted,
    /// The volume is out of the table.
    Retired,
    /// A join under a grant the window has spent or passed: nothing changed; the window's top
    /// is the highest grant spent.
    Stale { top: u64 },
    /// A removal of a member serving, or with volumes left, or a delisting of a member not
    /// removed: nothing changed.
    Holding,
    /// The absence or the return is recorded, or was already.
    Recorded,
}

/// A volume as the table holds it, with its node's failure domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub volume: u128,
    pub node: u64,
    pub domain: String,
    pub bytes: u64,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    let (outcome, writes) = match command {
        Command::Enlist(e) => enlist(engine, e)?,
        Command::Lived(l) => lived(engine, l)?,
        Command::Join(j) => join(engine, j)?,
        Command::Leave { node } => leave(engine, *node)?,
        Command::Gone { node } => gone(engine, *node)?,
        Command::Remove { node } => remove(engine, *node)?,
        Command::Delist { node } => delist(engine, *node)?,
        Command::Retire { volume } => retire(engine, *volume)?,
        Command::Absent { node, at_ns } => absent(engine, *node, *at_ns)?,
        Command::Returned { node } => returned(engine, *node)?,
    };
    engine.apply(index, writes)?;
    Ok(outcome)
}

fn enlist<E: Rows>(engine: &mut E, e: &Enlist) -> Result<(Outcome, Vec<Write>), MetaError> {
    match enroll(
        engine,
        e.node,
        &e.domain,
        &e.volumes,
        (e.max_nodes, e.max_volumes),
    )? {
        Ok((writes, _)) => Ok((Outcome::Enlisted, writes)),
        Err(refused) => Ok((refused, Vec::new())),
    }
}

/// What an enlistment of `node` in `domain` with `volumes` writes, within the cell's bounds on
/// nodes and volumes, and whether it adds the node to the table; or why it changes nothing.
fn enroll<E: Rows>(
    engine: &mut E,
    node: u64,
    domain: &str,
    volumes: &[(u128, u64)],
    (max_nodes, max_volumes): (u32, u32),
) -> Result<Result<(Vec<Write>, bool), Outcome>, MetaError> {
    if domain.len() > MAX_DOMAIN {
        return Ok(Err(Outcome::Conflict));
    }
    let mut census = census(engine)?;
    let mut writes = reserved(volumes.len().saturating_add(2))?;
    let mut added = false;
    match engine.get(&key::member(node))? {
        Some(bytes) if Member::decode(&bytes)?.domain != domain => {
            return Ok(Err(Outcome::Conflict));
        }
        Some(_) => {}
        None => {
            census.nodes = census.nodes.checked_add(1).ok_or(MetaError::Corrupt)?;
            added = true;
            let member = Member {
                domain: domain.to_owned(),
            };
            writes.push(Write::Put(key::member(node), member.encode()?));
        }
    }
    for (at, (volume, bytes)) in volumes.iter().enumerate() {
        if volumes
            .get(..at)
            .unwrap_or_default()
            .iter()
            .any(|(v, _)| v == volume)
        {
            return Ok(Err(Outcome::Conflict));
        }
        let row = VolumeRow {
            node,
            bytes: *bytes,
        };
        match engine.get(&key::volume(*volume))? {
            Some(held) if VolumeRow::decode(&held)? != row => return Ok(Err(Outcome::Conflict)),
            Some(_) => {}
            None => {
                census.volumes = census.volumes.checked_add(1).ok_or(MetaError::Corrupt)?;
                writes.push(Write::Put(key::volume(*volume), row.encode()));
            }
        }
    }
    if census.nodes > max_nodes || census.volumes > max_volumes {
        return Ok(Err(Outcome::Full));
    }
    if !writes.is_empty() {
        writes.push(Write::Put(key::census(), census.encode()));
    }
    Ok(Ok((writes, added)))
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

/// Whether grant `grant` may yet be spent in `grants`, a window `width` wide: above the highest
/// spent, or within the window, at or above the lowest grant it ever covered, and not spent (RFC
/// 4303 §3.4.3). Grant 0 names no grant.
pub fn admits(grants: &Grants, grant: u64, width: u32) -> bool {
    if grant == 0 {
        return false;
    }
    let Some(below) = grants.top.checked_sub(grant) else {
        return true;
    };
    grant >= grants.floor && below < u64::from(width) && !bit(&grants.seen, below)
}

/// Whether bit `at` of `seen` is set.
fn bit(seen: &[u8], at: u64) -> bool {
    let (byte, shift) = (at / 8, at % 8);
    usize::try_from(byte)
        .ok()
        .and_then(|byte| seen.get(byte))
        .is_some_and(|b| b & (1u8 << shift) != 0)
}

/// `grants` with `grant` spent, the window `width` wide: a grant above the top moves the window up
/// to it, the bits it passes dropped and its floor raised to the lowest grant it now covers; one
/// within it sets its bit. The caller checked [`admits`].
fn spend(grants: &Grants, grant: u64, width: u32) -> Result<Grants, MetaError> {
    let bits = u64::from(width);
    let bytes = (bits / 8)
        .checked_add(u64::from(bits % 8 != 0))
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(MetaError::Memory)?;
    let mut seen = reserved(bytes)?;
    seen.resize(bytes, 0u8);
    let (top, shift) = match grant.checked_sub(grants.top) {
        Some(up) => (grant, up),
        None => (grants.top, 0),
    };
    for at in 0..u64::from(width) {
        // The grant `top - at` was spent before if the old window held it.
        let held = at
            .checked_sub(shift)
            .is_some_and(|old| grants.top > 0 && bit(&grants.seen, old));
        let now = top.checked_sub(at) == Some(grant);
        if held || now {
            let byte = usize::try_from(at / 8).map_err(|_| MetaError::Memory)?;
            if let Some(b) = seen.get_mut(byte) {
                *b |= 1u8 << (at % 8);
            }
        }
    }
    let covered = top.saturating_sub(u64::from(width)).saturating_add(1);
    Ok(Grants {
        top,
        floor: grants.floor.max(covered),
        seen,
    })
}

/// A node joining under its grant, or at a later start restating where it is dialed and that it is
/// present (membership.md §3, §5).
fn join<E: Rows>(engine: &mut E, j: &Join) -> Result<(Outcome, Vec<Write>), MetaError> {
    let refused = |outcome| Ok((outcome, Vec::new()));
    if j.quic.len() > MAX_REACH || j.plane.len() > MAX_REACH {
        return refused(Outcome::Conflict);
    }
    let held = membership(engine, j.node)?;
    if let Some(s) = &held {
        if s.grant != j.grant || s.fingerprint != j.fingerprint {
            return refused(Outcome::Conflict);
        }
        match s.state {
            State::Gone => return refused(Outcome::Gone),
            State::Removed => return refused(Outcome::Removed),
            State::Serving | State::Leaving => {}
        }
    } else {
        let window = grants(engine)?;
        if !admits(&window, j.grant, j.max_nodes) {
            return refused(Outcome::Stale { top: window.top });
        }
    }
    let (mut writes, _) = match enroll(
        engine,
        j.node,
        &j.domain,
        &j.volumes,
        (j.max_nodes, j.max_volumes),
    )? {
        Ok(enrolled) => enrolled,
        Err(refusal) => return refused(refusal),
    };
    let standing = match held {
        // A member's own start: where it listens now, and present, as its command came.
        Some(s) => Membership {
            quic: j.quic.clone(),
            plane: j.plane.clone(),
            absent_since_ns: 0,
            ..s
        },
        None => {
            let window = spend(&grants(engine)?, j.grant, j.max_nodes)?;
            writes.push(Write::Put(key::grants(), window.encode()?));
            Membership {
                state: State::Serving,
                grant: j.grant,
                fingerprint: j.fingerprint,
                quic: j.quic.clone(),
                plane: j.plane.clone(),
                absent_since_ns: 0,
            }
        }
    };
    let outcome = match standing.state {
        State::Leaving => Outcome::Leaving,
        _ => Outcome::Joined,
    };
    writes.push(Write::Put(key::standing(j.node), standing.encode()?));
    Ok((outcome, writes))
}

/// A member asking to leave in order: `Serving` to `Leaving`; a member leaving already stays so.
fn leave<E: Rows>(engine: &mut E, node: u64) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    match s.state {
        State::Gone => Ok((Outcome::Gone, Vec::new())),
        State::Removed => Ok((Outcome::Removed, Vec::new())),
        State::Leaving => Ok((Outcome::Leaving, Vec::new())),
        State::Serving => {
            s.state = State::Leaving;
            Ok((
                Outcome::Leaving,
                vec![Write::Put(key::standing(node), s.encode()?)],
            ))
        }
    }
}

/// A member gone for good (membership.md §5): serving or leaving, it is gone from now on.
fn gone<E: Rows>(engine: &mut E, node: u64) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    match s.state {
        State::Gone => return Ok((Outcome::Gone, Vec::new())),
        State::Removed => return Ok((Outcome::Removed, Vec::new())),
        State::Serving | State::Leaving => {}
    }
    s.state = State::Gone;
    Ok((
        Outcome::Gone,
        vec![Write::Put(key::standing(node), s.encode()?)],
    ))
}

/// A member leaving or gone, with no volume of it left in the table, removed: its identity
/// refused from now on, its row kept until no range's configuration names it.
fn remove<E: Rows>(engine: &mut E, node: u64) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    match s.state {
        State::Removed => return Ok((Outcome::Removed, Vec::new())),
        State::Serving => return Ok((Outcome::Holding, Vec::new())),
        State::Leaving | State::Gone => {}
    }
    if holds_volumes(engine, node)? {
        return Ok((Outcome::Holding, Vec::new()));
    }
    s.state = State::Removed;
    Ok((
        Outcome::Removed,
        vec![Write::Put(key::standing(node), s.encode()?)],
    ))
}

/// A removed member delisted: its node, standing and history rows go, its history added to the
/// removed nodes' totals, its grant spent for good.
fn delist<E: Rows>(engine: &mut E, node: u64) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    if s.state != State::Removed {
        return Ok((Outcome::Holding, Vec::new()));
    }
    let mut census = census(engine)?;
    census.nodes = census.nodes.checked_sub(1).ok_or(MetaError::Corrupt)?;
    let own = history_of(engine, node)?;
    let gone = retired_history(engine)?;
    let retired = History {
        exposure_ns: gone.exposure_ns.saturating_add(own.exposure_ns),
        crashes: gone.crashes.saturating_add(own.crashes),
    };
    Ok((
        Outcome::Delisted,
        vec![
            Write::Delete(key::member(node)),
            Write::Delete(key::standing(node)),
            Write::Delete(key::history(node)),
            Write::Put(key::retired(), retired.encode()),
            Write::Put(key::census(), census.encode()),
        ],
    ))
}

/// Whether any volume in the table is `node`'s: a scan of the volume rows, at most the cell's.
fn holds_volumes<E: Rows>(engine: &mut E, node: u64) -> Result<bool, MetaError> {
    let (mut from, to) = key::volume_rows(None);
    let most = census(engine)?.volumes;
    for _ in 0..most {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        if VolumeRow::decode(&v)?.node == node {
            return Ok(true);
        }
        from = k;
        from.push(0);
    }
    Ok(false)
}

/// A volume of a member leaving or gone taken out of the table, as repair asks once it holds no
/// chunk a block names (membership.md §8). A serving member's volume is refused, `Conflict`.
fn retire<E: Rows>(engine: &mut E, volume: u128) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(row) = engine.get(&key::volume(volume))? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    let node = VolumeRow::decode(&row)?.node;
    let leaving =
        membership(engine, node)?.is_some_and(|s| matches!(s.state, State::Leaving | State::Gone));
    if !leaving {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let mut census = census(engine)?;
    census.volumes = census.volumes.checked_sub(1).ok_or(MetaError::Corrupt)?;
    Ok((
        Outcome::Retired,
        vec![
            Write::Delete(key::volume(volume)),
            Write::Put(key::census(), census.encode()),
        ],
    ))
}

/// A member's absence recorded at `at_ns`, unless one is recorded already.
fn absent<E: Rows>(
    engine: &mut E,
    node: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    if s.absent_since_ns != 0 || at_ns == 0 {
        return Ok((Outcome::Recorded, Vec::new()));
    }
    s.absent_since_ns = at_ns;
    Ok((
        Outcome::Recorded,
        vec![Write::Put(key::standing(node), s.encode()?)],
    ))
}

/// A member back: its recorded absence cleared.
fn returned<E: Rows>(engine: &mut E, node: u64) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    if s.absent_since_ns == 0 {
        return Ok((Outcome::Recorded, Vec::new()));
    }
    s.absent_since_ns = 0;
    Ok((
        Outcome::Recorded,
        vec![Write::Put(key::standing(node), s.encode()?)],
    ))
}

/// The cell's history: every node's time run and crashes, and the removed nodes' totals, summed,
/// read at most `max` nodes'. Sums saturate: past `u64::MAX` nanoseconds (584 years) of node time
/// or that many crashes, more changes no estimate made from them.
pub fn history<E: Rows>(engine: &mut E, max: usize) -> Result<History, MetaError> {
    let (mut from, to) = key::history_rows();
    let mut sum = retired_history(engine)?;
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

/// The removed nodes' history, their totals added together.
fn retired_history<E: Rows>(engine: &mut E) -> Result<History, MetaError> {
    match engine.get(&key::retired())? {
        Some(bytes) => Ok(History::decode(&bytes)?),
        None => Ok(History::default()),
    }
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

/// Node `node`'s standing, if it joined under a grant and is not removed.
pub fn membership<E: Rows>(engine: &mut E, node: u64) -> Result<Option<Membership>, MetaError> {
    match engine.get(&key::standing(node))? {
        Some(bytes) => Ok(Some(Membership::decode(&bytes)?)),
        None => Ok(None),
    }
}

/// Every member's standing, in the order of their ids, at most `max`.
pub fn memberships<E: Rows>(
    engine: &mut E,
    max: usize,
) -> Result<Vec<(u64, Membership)>, MetaError> {
    let (mut from, to) = key::standing_rows();
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let node = key::decode_standing(&k).ok_or(MetaError::Corrupt)?;
        out.push((node, Membership::decode(&v)?));
        from = k;
        from.push(0);
    }
    Ok(out)
}

/// The grant window: empty before any grant is spent.
pub fn grants<E: Rows>(engine: &mut E) -> Result<Grants, MetaError> {
    match engine.get(&key::grants())? {
        Some(bytes) => Ok(Grants::decode(&bytes)?),
        None => Ok(Grants::default()),
    }
}

/// The table's volumes after `after`, or from the first, in the order of their ids, at most
/// `max`: each with its node's failure domain.
pub fn volumes<E: Rows>(
    engine: &mut E,
    after: Option<u128>,
    max: usize,
) -> Result<Vec<Placed>, MetaError> {
    volumes_where(engine, after, max, Which::All)
}

/// The volumes a new block's scheme is chosen over, in the order of their ids, at most `max`: those
/// of serving members, present or away, and of nodes enlisted with no standing (a cell made before
/// grants). An absence is no change of the cell's shape (docs/design/repair.md §5), so a block
/// written while a member sleeps is as whole as one written with it there; a member leaving or
/// gone is (membership.md §4, §5).
pub fn shaping<E: Rows>(engine: &mut E, max: usize) -> Result<Vec<Placed>, MetaError> {
    volumes_where(engine, None, max, Which::Serving)
}

/// The volumes placement may offer a new block's chunks, in the order of their ids, at most `max`:
/// those [`shaping`] names whose node is present, so no chunk is sent to a member away
/// (membership.md §5), and a block its scheme cannot place on those is refused rather than
/// written short of it (CLAUDE.md §6).
pub fn placeable<E: Rows>(engine: &mut E, max: usize) -> Result<Vec<Placed>, MetaError> {
    volumes_where(engine, None, max, Which::Present)
}

/// Which of the table's volumes a reading takes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Which {
    All,
    /// Of serving members, present or away, and of nodes with no standing.
    Serving,
    /// Of those, the ones whose node is present.
    Present,
}

fn volumes_where<E: Rows>(
    engine: &mut E,
    after: Option<u128>,
    max: usize,
    which: Which,
) -> Result<Vec<Placed>, MetaError> {
    let (mut from, to) = key::volume_rows(after);
    let mut out: Vec<Placed> = Vec::new();
    // The last node read and whether the reading takes it: a node's volumes are enlisted together.
    let mut last: Option<(u64, String, bool)> = None;
    for _ in 0..max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let volume = key::decode_volume(&k).ok_or(MetaError::Corrupt)?;
        let row = VolumeRow::decode(&v)?;
        from = k;
        from.push(0);
        let (domain, serving) = match last.as_ref().filter(|(node, ..)| *node == row.node) {
            Some((_, domain, serving)) => (domain.clone(), *serving),
            None => {
                let member = engine
                    .get(&key::member(row.node))?
                    .ok_or(MetaError::Corrupt)?;
                let domain = Member::decode(&member)?.domain;
                let standing = membership(engine, row.node)?;
                let serving = match which {
                    Which::All => true,
                    Which::Serving => standing.is_none_or(|s| s.state == State::Serving),
                    Which::Present => {
                        standing.is_none_or(|s| s.state == State::Serving && s.absent_since_ns == 0)
                    }
                };
                last = Some((row.node, domain.clone(), serving));
                (domain, serving)
            }
        };
        if !serving {
            continue;
        }
        out.push(Placed {
            volume,
            node: row.node,
            domain,
            bytes: row.bytes,
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

    fn join(node: u64, grant: u64, volumes: &[(u128, u64)]) -> Command {
        joined_as(
            node,
            grant,
            [u8::try_from(node).unwrap(); FINGERPRINT],
            volumes,
        )
    }

    fn joined_as(
        node: u64,
        grant: u64,
        fingerprint: [u8; FINGERPRINT],
        volumes: &[(u128, u64)],
    ) -> Command {
        Command::Join(Join {
            node,
            grant,
            fingerprint,
            quic: format!("10.0.0.{node}:7000"),
            plane: format!("10.0.0.{node}:7001"),
            domain: format!("rack-{node}"),
            volumes: volumes.to_vec(),
            max_nodes: 3,
            max_volumes: 6,
        })
    }

    /// Applies `commands` from index `first`, giving each outcome.
    fn applied(m: &mut Model, first: u64, commands: &[Command]) -> Vec<Outcome> {
        commands
            .iter()
            .zip(first..)
            .map(|(c, index)| apply(m, index, c).unwrap())
            .collect()
    }

    #[test]
    fn a_join_spends_its_grant_and_its_member_serves_with_its_volumes() {
        let mut m = Model::default();
        assert_eq!(
            apply(&mut m, 1, &join(1, 1, &[(10, 100)])).unwrap(),
            Outcome::Joined
        );
        let s = membership(&mut m, 1).unwrap().unwrap();
        assert_eq!(
            (s.state, s.grant, s.absent_since_ns),
            (State::Serving, 1, 0)
        );
        assert_eq!(
            placeable(&mut m, 10).unwrap(),
            vec![placed(10, 1, "rack-1", 100)]
        );
        // The same member again, its address moved: restated, nothing else.
        let mut moved = join(1, 1, &[(10, 100)]);
        if let Command::Join(j) = &mut moved {
            j.quic = "10.0.9.1:7000".into();
        }
        assert_eq!(apply(&mut m, 2, &moved).unwrap(), Outcome::Joined);
        assert_eq!(
            membership(&mut m, 1).unwrap().unwrap().quic,
            "10.0.9.1:7000"
        );
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 1,
                volumes: 1
            }
        );
        // Another certificate under its id, or another grant, conflicts.
        let other = joined_as(1, 1, [9; FINGERPRINT], &[(10, 100)]);
        assert_eq!(apply(&mut m, 3, &other).unwrap(), Outcome::Conflict);
        assert_eq!(
            apply(&mut m, 4, &join(1, 2, &[])).unwrap(),
            Outcome::Conflict
        );
        // A place past its bound conflicts.
        let mut long = join(2, 2, &[]);
        if let Command::Join(j) = &mut long {
            j.plane = "p".repeat(MAX_REACH + 1);
        }
        assert_eq!(apply(&mut m, 5, &long).unwrap(), Outcome::Conflict);
        assert_eq!(membership(&mut m, 2).unwrap(), None);
    }

    /// RFC 4303 §3.4.3's window over grants: one above the top is new, one within it new while
    /// unspent, one below the lowest it ever covered refused, whatever its width becomes.
    #[test]
    fn the_grant_window_spends_each_grant_once_and_refuses_what_it_passed() {
        let mut g = Grants::default();
        assert!(!admits(&g, 0, 3));
        for grant in [5, 3] {
            assert!(admits(&g, grant, 3), "{grant}");
            g = spend(&g, grant, 3).unwrap();
        }
        assert_eq!((g.top, g.floor), (5, 3));
        assert!(!admits(&g, 5, 3) && !admits(&g, 3, 3), "spent");
        assert!(admits(&g, 4, 3), "within, unspent");
        assert!(!admits(&g, 2, 3), "below the window");
        g = spend(&g, 4, 3).unwrap();
        assert!(!admits(&g, 4, 3));
        g = spend(&g, 9, 3).unwrap();
        assert_eq!((g.top, g.floor), (9, 7));
        assert!(admits(&g, 8, 3) && admits(&g, 7, 3));
        assert!(!admits(&g, 6, 3) && !admits(&g, 5, 3));
        // A wider window later reopens nothing below the floor.
        assert!(!admits(&g, 5, 10) && !admits(&g, 6, 10));
        assert!(admits(&g, 8, 10) && admits(&g, 100, 10));
    }

    #[test]
    fn a_join_under_a_spent_or_passed_grant_is_stale_and_changes_nothing() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[]),
                join(2, 1, &[]),
                join(3, 9, &[]),
                join(4, 2, &[]),
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Joined,
                Outcome::Stale { top: 1 },
                Outcome::Joined,
                Outcome::Stale { top: 9 }
            ]
        );
        assert_eq!(census(&mut m).unwrap().nodes, 2);
        assert_eq!(membership(&mut m, 4).unwrap(), None);
    }

    #[test]
    fn a_member_leaves_in_order_and_is_removed_once_repair_retired_its_volumes() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[(10, 100)]),
                join(2, 2, &[(20, 100), (21, 100)]),
                Command::Lived(Lived {
                    node: 2,
                    exposure_ns: 50,
                    crashes: 2,
                }),
                Command::Leave { node: 2 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Joined,
                Outcome::Joined,
                Outcome::Kept,
                Outcome::Leaving
            ]
        );
        assert_eq!(
            membership(&mut m, 2).unwrap().unwrap().state,
            State::Leaving
        );
        // Placement offers its volumes no more; the table still names them, for repair.
        assert_eq!(
            placeable(&mut m, 10).unwrap(),
            vec![placed(10, 1, "rack-1", 100)]
        );
        assert_eq!(volumes(&mut m, None, 10).unwrap().len(), 3);
        let before = history(&mut m, 10).unwrap();
        let outcomes = applied(
            &mut m,
            5,
            &[
                Command::Leave { node: 2 },
                Command::Delist { node: 2 },
                Command::Remove { node: 2 },
                Command::Retire { volume: 20 },
                Command::Remove { node: 2 },
                Command::Retire { volume: 21 },
                Command::Remove { node: 2 },
                Command::Remove { node: 2 },
                Command::Retire { volume: 20 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Leaving,
                Outcome::Holding,
                Outcome::Holding,
                Outcome::Retired,
                Outcome::Holding,
                Outcome::Retired,
                Outcome::Removed,
                Outcome::Removed,
                Outcome::Unknown
            ]
        );
        // Removed: its row kept, its identity refused, until it is delisted.
        assert_eq!(
            membership(&mut m, 2).unwrap().unwrap().state,
            State::Removed
        );
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 2,
                volumes: 1
            }
        );
        let outcomes = applied(
            &mut m,
            14,
            &[
                join(2, 2, &[]),
                Command::Leave { node: 2 },
                Command::Delist { node: 2 },
                Command::Delist { node: 2 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Removed,
                Outcome::Removed,
                Outcome::Delisted,
                Outcome::Unknown
            ]
        );
        assert_eq!(membership(&mut m, 2).unwrap(), None);
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 1,
                volumes: 1
            }
        );
        // Its history is the removed nodes' now: the cell's total holds it still.
        assert_eq!(history(&mut m, 10).unwrap(), before);
        assert_eq!(history_of(&mut m, 2).unwrap(), History::default());
        // Its grant is spent for good.
        assert_eq!(
            apply(&mut m, 18, &join(2, 2, &[])).unwrap(),
            Outcome::Stale { top: 2 }
        );
    }

    #[test]
    fn a_serving_member_is_not_removed_nor_its_volume_retired() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[(10, 100)]),
                Command::Remove { node: 1 },
                Command::Retire { volume: 10 },
                Command::Retire { volume: 11 },
                Command::Leave { node: 7 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Joined,
                Outcome::Holding,
                Outcome::Conflict,
                Outcome::Unknown,
                Outcome::Unknown
            ]
        );
        assert_eq!(
            census(&mut m).unwrap(),
            Census {
                nodes: 1,
                volumes: 1
            }
        );
    }

    /// membership.md §5: a member's absence is recorded once, at its entry's time, and cleared at
    /// its return; gone, it joins and leaves no more, and is removed and delisted once it holds no
    /// volume.
    #[test]
    fn absences_are_recorded_and_a_gone_member_is_removed_once_it_holds_nothing() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[]),
                join(2, 2, &[(20, 100)]),
                Command::Absent {
                    node: 1,
                    at_ns: 100,
                },
                Command::Absent {
                    node: 1,
                    at_ns: 300,
                },
                Command::Returned { node: 2 },
                Command::Absent { node: 2, at_ns: 0 },
                Command::Absent { node: 9, at_ns: 5 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Joined,
                Outcome::Joined,
                Outcome::Recorded,
                Outcome::Recorded,
                Outcome::Recorded,
                Outcome::Recorded,
                Outcome::Unknown
            ]
        );
        assert_eq!(membership(&mut m, 1).unwrap().unwrap().absent_since_ns, 100);
        assert_eq!(membership(&mut m, 2).unwrap().unwrap().absent_since_ns, 0);
        // Placement offers an absent member's volumes no new chunk; the cell's shape keeps them.
        let absent = applied(&mut m, 8, &[Command::Absent { node: 2, at_ns: 7 }]);
        assert_eq!(absent, [Outcome::Recorded]);
        assert!(placeable(&mut m, 10).unwrap().is_empty());
        assert_eq!(shaping(&mut m, 10).unwrap().len(), 1);
        let back = applied(&mut m, 9, &[Command::Returned { node: 2 }]);
        assert_eq!(back, [Outcome::Recorded]);
        assert_eq!(
            placeable(&mut m, 10).unwrap(),
            vec![placed(20, 2, "rack-2", 100)]
        );
        let outcomes = applied(&mut m, 10, &[Command::Returned { node: 1 }]);
        assert_eq!(outcomes, [Outcome::Recorded]);
        assert_eq!(membership(&mut m, 1).unwrap().unwrap().absent_since_ns, 0);
        // Gone: it joins and leaves no more, its volume out of placement, held for repair.
        let outcomes = applied(
            &mut m,
            11,
            &[
                Command::Gone { node: 2 },
                Command::Gone { node: 2 },
                join(2, 2, &[(20, 100)]),
                Command::Leave { node: 2 },
                Command::Remove { node: 2 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Gone,
                Outcome::Gone,
                Outcome::Gone,
                Outcome::Gone,
                Outcome::Holding
            ]
        );
        assert!(placeable(&mut m, 10).unwrap().is_empty());
        assert!(shaping(&mut m, 10).unwrap().is_empty());
        assert_eq!(volumes(&mut m, None, 10).unwrap().len(), 1);
        let outcomes = applied(
            &mut m,
            16,
            &[
                Command::Retire { volume: 20 },
                Command::Remove { node: 2 },
                Command::Gone { node: 2 },
                Command::Delist { node: 2 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Retired,
                Outcome::Removed,
                Outcome::Removed,
                Outcome::Delisted
            ]
        );
        assert_eq!(membership(&mut m, 2).unwrap(), None);
    }

    /// The standings read back in the order of their ids, at most the number asked.
    #[test]
    fn standings_read_back_in_order() {
        let mut m = Model::default();
        applied(
            &mut m,
            1,
            &[join(3, 1, &[]), join(1, 2, &[]), join(2, 3, &[])],
        );
        let ids: Vec<u64> = memberships(&mut m, 10)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(memberships(&mut m, 2).unwrap().len(), 2);
    }
}
