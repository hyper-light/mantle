//! The root range's volume table (docs/design/node.md §1.5, §5.6; architecture §4): the cell's
//! nodes, each with its failure domain, and its volumes, each with the node that holds it and its
//! bytes, which placement reads (node.md §7); and each node's history, the time it has run and its
//! runs that ended without an orderly stop, which every node's failure detectors start from
//! (node.md §2.4, item 7).
//!
//! It also holds the account's principals and their access keys (`crate::principal`).
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
//!
//! A node whose certificate carries a grant joins instead (docs/design/membership.md §2–§3): its
//! grant spent in the window, its standing `Serving`. It leaves in order (`Leaving`, its volumes
//! leaving with it in the same entry), or is judged or stated `Gone` (every volume of it gone in the
//! same entry), and is removed once no volume of it is left, which repair forgets (membership.md
//! §8): `Removed`, its identity refused, its row kept until no range's configuration names it, then
//! delisted. Whether the cell sees a member is its presence, the one record of it (membership.md
//! §8): the fact a judgment of gone counts from.

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::record::{
    Census, FINGERPRINT, Grants, History, Member, MemberState as State, Membership, Presence,
    VolumeRow, VolumeStanding, VolumeState,
};
use crate::{key, principal};

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
    /// A command on the account's principals and their keys (`crate::principal`).
    Principal(principal::Command),
    Seen(Seen),
    /// `node` leaves in order (membership.md §4): a member's standing `Leaving`, and each of its
    /// active volumes leaving, taking no new chunk while repair moves what each holds to the others
    /// (docs/design/repair.md §4). From the node itself.
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
    /// A node joins under its grant, or restates where it is dialed (membership.md §3).
    Join(Join),
    /// A member gone for good (membership.md §5), every volume of it gone in the same entry:
    /// judged so by the root range's leader, its presence away past what the cell's durability
    /// model lets a chunk stay lost, or stated so by an operator, a supervisor or repair.
    Gone {
        node: u64,
        at_ns: u64,
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
    /// The entry's time, which the command takes.
    pub at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The table holds the node and each volume named as the enlistment states them.
    Enlisted,
    /// The table holds the node in another domain, or a volume on another node or of other
    /// bytes, or one gone, or the enlistment names a volume twice or a domain longer than
    /// [`MAX_DOMAIN`]; or a volume retired is not leaving; or a join names a member under another
    /// grant or certificate, or a place past [`MAX_REACH`]: nothing changed.
    Conflict,
    /// The table would hold more nodes or volumes than the cell states: nothing changed.
    Full,
    /// The node's history holds at least what was stated.
    Kept,
    /// The table holds no such node, whose history it would keep: nothing changed.
    Unknown,
    /// What became of a command on the principals.
    Principal(principal::Outcome),
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
    /// A join under a grant the window has spent or passed: nothing changed; the window's top
    /// is the highest grant spent.
    Stale { top: u64 },
    /// A removal of a member serving, or with volumes left, or a delisting of a member not
    /// removed: nothing changed.
    Holding,
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
        Command::Principal(c) => {
            let (outcome, writes) = principal::run(engine, c)?;
            (Outcome::Principal(outcome), writes)
        }
        Command::Seen(s) => seen_now(engine, s)?,
        Command::Leave { node, at_ns } => leave(engine, *node, *at_ns)?,
        Command::Retire { volume, at_ns } => retire(engine, *volume, *at_ns)?,
        Command::Forget { volume } => forget(engine, *volume)?,
        Command::Join(j) => join(engine, j)?,
        Command::Gone { node, at_ns } => gone(engine, *node, *at_ns)?,
        Command::Remove { node } => remove(engine, *node)?,
        Command::Delist { node } => delist(engine, *node)?,
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
        e.at_ns,
    )? {
        Ok(writes) => Ok((Outcome::Enlisted, writes)),
        Err(refused) => Ok((refused, Vec::new())),
    }
}

/// What an enlistment of `node` in `domain` with `volumes` at `at_ns` writes, within the cell's
/// bounds on nodes and volumes; or why it changes nothing. A volume the cell wrote off does not
/// come back; one the table gave the node and it does not name, it started without, and the volume
/// is gone; and the node speaks, so the cell sees it.
fn enroll<E: Rows>(
    engine: &mut E,
    node: u64,
    domain: &str,
    volumes: &[(u128, u64)],
    (max_nodes, max_volumes): (u32, u32),
    at_ns: u64,
) -> Result<Result<Vec<Write>, Outcome>, MetaError> {
    if domain.len() > MAX_DOMAIN {
        return Ok(Err(Outcome::Conflict));
    }
    let mut census = census(engine)?;
    let mut writes = reserved(volumes.len().saturating_add(2))?;
    match engine.get(&key::member(node))? {
        Some(bytes) if Member::decode(&bytes)?.domain != domain => {
            return Ok(Err(Outcome::Conflict));
        }
        Some(_) => {}
        None => {
            census.nodes = census.nodes.checked_add(1).ok_or(MetaError::Corrupt)?;
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
            // A volume the cell wrote off does not come back with what it held then: its
            // chunks were rebuilt elsewhere, and its node starts again without it.
            Some(_) if standing(engine, *volume)? == VolumeStanding::Gone => {
                return Ok(Err(Outcome::Conflict));
            }
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
    let (now, clock) = clock::tick(engine, at_ns)?;
    // The node states every volume it holds: one the table gave it and it does not name, it
    // started without, and the volume is gone. The table's volumes are at most its census's.
    let most = usize::try_from(census.volumes).map_err(|_| MetaError::Corrupt)?;
    for placed in self::volumes(engine, None, most)? {
        if placed.node == node
            && !volumes.iter().any(|(v, _)| *v == placed.volume)
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
    if let Some(presence) = seen_at(engine, node, true, now)? {
        writes.push(Write::Put(key::presence(node), presence.encode()));
    }
    writes.push(clock);
    writes.push(Write::Put(key::census(), census.encode()));
    Ok(Ok(writes))
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

/// A node joining under its grant, or at a later start restating where it is dialed, its
/// enrollment saying it seen (membership.md §3, §5).
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
    let mut writes = match enroll(
        engine,
        j.node,
        &j.domain,
        &j.volumes,
        (j.max_nodes, j.max_volumes),
        j.at_ns,
    )? {
        Ok(writes) => writes,
        Err(refusal) => return refused(refusal),
    };
    let standing = match held {
        // A member's own start: where it listens now; present, as its enrollment says it seen.
        Some(s) => Membership {
            quic: j.quic.clone(),
            plane: j.plane.clone(),
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

/// A member gone for good (membership.md §5): serving or leaving, it is gone from now on, and every
/// volume of it gone in the same entry, so repair rebuilds its chunks at once (membership.md §8).
fn gone<E: Rows>(
    engine: &mut E,
    node: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(mut s) = membership(engine, node)? else {
        return Ok((Outcome::Unknown, Vec::new()));
    };
    match s.state {
        State::Gone => return Ok((Outcome::Gone, Vec::new())),
        State::Removed => return Ok((Outcome::Removed, Vec::new())),
        State::Serving | State::Leaving => {}
    }
    s.state = State::Gone;
    let mut writes = vec![Write::Put(key::standing(node), s.encode()?)];
    writes.extend(volumes_written(engine, node, VolumeStanding::Gone, at_ns)?);
    Ok((Outcome::Gone, writes))
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

/// A node leaving in order: a member serving is `Leaving` from now (one gone or removed is refused,
/// one leaving stays so), and each of the node's active volumes is leaving, so repair drains them
/// while the node serves (membership.md §4 step 4; repair.md §4). A node enlisted with no standing
/// leaves its volumes alone.
fn leave<E: Rows>(
    engine: &mut E,
    node: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if engine.get(&key::member(node))?.is_none() {
        return Ok((Outcome::Unknown, Vec::new()));
    }
    let mut writes = Vec::new();
    let outcome = match membership(engine, node)? {
        Some(s) if s.state == State::Gone => return Ok((Outcome::Gone, Vec::new())),
        Some(s) if s.state == State::Removed => return Ok((Outcome::Removed, Vec::new())),
        Some(mut s) => {
            if s.state == State::Serving {
                s.state = State::Leaving;
                writes.push(Write::Put(key::standing(node), s.encode()?));
            }
            Outcome::Leaving
        }
        None => Outcome::Kept,
    };
    writes.extend(volumes_written(
        engine,
        node,
        VolumeStanding::Leaving,
        at_ns,
    )?);
    Ok((outcome, writes))
}

/// Each of `node`'s volumes that `to` moves on, `to` from the entry's time `at_ns`, and the clock
/// it ticked; nothing where none moves. Leaving takes the active ones; gone takes every one not
/// gone already.
fn volumes_written<E: Rows>(
    engine: &mut E,
    node: u64,
    to: VolumeStanding,
    at_ns: u64,
) -> Result<Vec<Write>, MetaError> {
    let census = census(engine)?;
    let most = usize::try_from(census.volumes).map_err(|_| MetaError::Corrupt)?;
    let (now, clock) = clock::tick(engine, at_ns)?;
    let mut writes = Vec::new();
    for placed in volumes(engine, None, most)? {
        if placed.node != node {
            continue;
        }
        let held = standing(engine, placed.volume)?;
        let moves = match to {
            VolumeStanding::Leaving => held == VolumeStanding::Active,
            VolumeStanding::Gone => held != VolumeStanding::Gone,
            VolumeStanding::Active => false,
        };
        if moves {
            let state = VolumeState {
                state: to,
                at_ns: now,
            };
            writes.push(Write::Put(key::volume_state(placed.volume), state.encode()));
        }
    }
    if !writes.is_empty() {
        writes.push(clock);
    }
    Ok(writes)
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

// The removed nodes' history, their totals added together.
fn retired_history<E: Rows>(engine: &mut E) -> Result<History, MetaError> {
    match engine.get(&key::retired())? {
        Some(bytes) => Ok(History::decode(&bytes)?),
        None => Ok(History::default()),
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
            at_ns: 0,
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

    /// The volumes placement offers a new block's chunks: active, their node seen
    /// (`Fact::takes`).
    fn taking(m: &mut Model) -> Vec<Placed> {
        facts(m, None, 10)
            .unwrap()
            .into_iter()
            .filter(Fact::takes)
            .map(|f| f.placed)
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
        assert_eq!((s.state, s.grant), (State::Serving, 1));
        assert!(presence(&mut m, 1).unwrap().present);
        assert_eq!(taking(&mut m), vec![placed(10, 1, "rack-1", 100)]);
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

    /// membership.md §4 with repair's volume standing (§8): a leave makes the member `Leaving` and
    /// its volumes leaving in one entry; repair retires each drained volume and forgets it, and
    /// only then is the member removed, delisted once no range names it.
    #[test]
    fn a_member_leaves_in_order_and_is_removed_once_repair_forgot_its_volumes() {
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
                Command::Leave { node: 2, at_ns: 10 },
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
        assert_eq!(standing(&mut m, 20).unwrap(), VolumeStanding::Leaving);
        assert_eq!(standing(&mut m, 21).unwrap(), VolumeStanding::Leaving);
        // Placement offers its volumes no more; the table still names them, for repair.
        assert_eq!(taking(&mut m), vec![placed(10, 1, "rack-1", 100)]);
        assert_eq!(volumes(&mut m, None, 10).unwrap().len(), 3);
        let before = history(&mut m, 10).unwrap();
        let outcomes = applied(
            &mut m,
            5,
            &[
                Command::Leave { node: 2, at_ns: 11 },
                Command::Delist { node: 2 },
                Command::Remove { node: 2 },
                Command::Retire {
                    volume: 20,
                    at_ns: 12,
                },
                Command::Forget { volume: 20 },
                Command::Remove { node: 2 },
                Command::Retire {
                    volume: 21,
                    at_ns: 13,
                },
                Command::Forget { volume: 21 },
                Command::Remove { node: 2 },
                Command::Remove { node: 2 },
                Command::Retire {
                    volume: 20,
                    at_ns: 14,
                },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Leaving,
                Outcome::Holding,
                Outcome::Holding,
                Outcome::Kept,
                Outcome::Kept,
                Outcome::Holding,
                Outcome::Kept,
                Outcome::Kept,
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
            16,
            &[
                join(2, 2, &[]),
                Command::Leave { node: 2, at_ns: 15 },
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
            apply(&mut m, 20, &join(2, 2, &[])).unwrap(),
            Outcome::Stale { top: 2 }
        );
    }

    #[test]
    fn a_serving_member_is_not_removed_nor_its_active_volume_retired() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[(10, 100)]),
                Command::Remove { node: 1 },
                Command::Retire {
                    volume: 10,
                    at_ns: 5,
                },
                Command::Retire {
                    volume: 11,
                    at_ns: 6,
                },
                Command::Leave { node: 7, at_ns: 7 },
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

    /// membership.md §5 over repair's presence (§8): a member away is its presence, kept at its
    /// entry's time once and cleared as it is seen again; placement offers an away member's volumes
    /// nothing while a block's scheme still counts them; gone, it joins and leaves no more, every
    /// volume of it gone in the same entry, and it is removed and delisted once repair forgot them.
    #[test]
    fn a_member_away_and_then_gone_is_removed_once_it_holds_nothing() {
        let mut m = Model::default();
        let outcomes = applied(
            &mut m,
            1,
            &[
                join(1, 1, &[]),
                join(2, 2, &[(20, 100)]),
                Command::Seen(Seen {
                    node: 1,
                    present: false,
                    at_ns: 100,
                }),
                Command::Seen(Seen {
                    node: 1,
                    present: false,
                    at_ns: 300,
                }),
                Command::Seen(Seen {
                    node: 9,
                    present: false,
                    at_ns: 301,
                }),
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Joined,
                Outcome::Joined,
                Outcome::Kept,
                Outcome::Kept,
                Outcome::Unknown
            ]
        );
        let away = presence(&mut m, 1).unwrap();
        assert_eq!((away.present, away.since_ns), (false, 100));
        assert!(presence(&mut m, 2).unwrap().present);
        // Placement offers an away member's volumes no new chunk; the cell's shape keeps them.
        let seen = |present, at_ns| {
            Command::Seen(Seen {
                node: 2,
                present,
                at_ns,
            })
        };
        assert_eq!(applied(&mut m, 6, &[seen(false, 400)]), [Outcome::Kept]);
        assert!(taking(&mut m).is_empty());
        let kept = |m: &mut Model| {
            facts(m, None, 10)
                .unwrap()
                .into_iter()
                .filter(|f| f.standing != VolumeStanding::Gone)
                .count()
        };
        assert_eq!(kept(&mut m), 1);
        assert_eq!(applied(&mut m, 7, &[seen(true, 500)]), [Outcome::Kept]);
        assert_eq!(taking(&mut m), vec![placed(20, 2, "rack-2", 100)]);
        // Gone: it joins and leaves no more, its volume gone with it, held for repair.
        let outcomes = applied(
            &mut m,
            8,
            &[
                Command::Gone {
                    node: 2,
                    at_ns: 600,
                },
                Command::Gone {
                    node: 2,
                    at_ns: 601,
                },
                join(2, 2, &[(20, 100)]),
                Command::Leave {
                    node: 2,
                    at_ns: 602,
                },
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
        assert_eq!(standing(&mut m, 20).unwrap(), VolumeStanding::Gone);
        assert!(taking(&mut m).is_empty());
        assert_eq!(kept(&mut m), 0);
        assert_eq!(volumes(&mut m, None, 10).unwrap().len(), 1);
        let outcomes = applied(
            &mut m,
            13,
            &[
                Command::Forget { volume: 20 },
                Command::Remove { node: 2 },
                Command::Gone {
                    node: 2,
                    at_ns: 700,
                },
                Command::Delist { node: 2 },
            ],
        );
        assert_eq!(
            outcomes,
            [
                Outcome::Kept,
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
