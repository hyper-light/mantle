//! The root range's volume table (docs/design/node.md §1.5, §5.6; architecture §4): the cell's
//! nodes, each with its failure domain, and its volumes, each with the node that holds it and its
//! bytes, which placement reads (node.md §7).
//!
//! A node enlists itself and its volumes as it starts. What the table does not hold is recorded;
//! what it holds otherwise, a node in another domain or a volume on another node, is a conflict
//! that changes nothing, since a device moved between nodes is a move to make, never a silent
//! overwrite. The table holds at most the nodes and volumes the cell states, as every map keyed by
//! a value a peer chooses is bounded (CLAUDE.md §2), and an enlistment past them is refused.

use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::key;
use crate::record::{Census, Member, VolumeRow};

/// The longest failure domain a node names, in bytes: a label such as a rack's or a zone's name.
pub const MAX_DOMAIN: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Enlist(Enlist),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The table holds the node and each volume named as the enlistment states them.
    Enlisted,
    /// The table holds the node in another domain, or a volume on another node or of other
    /// bytes, or the enlistment names a volume twice or a domain longer than [`MAX_DOMAIN`]:
    /// nothing changed.
    Conflict,
    /// The table would hold more nodes or volumes than the cell states: nothing changed.
    Full,
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
    };
    engine.apply(index, &writes)?;
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
    if !writes.is_empty() {
        writes.push(Write::Put(key::census(), census.encode()));
    }
    Ok((Outcome::Enlisted, writes))
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
}
