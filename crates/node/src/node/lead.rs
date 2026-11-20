//! Where each range's lead lies (docs/design/node.md §6.7; research/101): spread over the cell's
//! nodes by a rule every member of a range works out alike, and kept there by hand-overs (Raft
//! §3.10) that move each range's lead only up its ranking, one at a time from a node.
//!
//! Every range's member on a node draws its campaign delays from one seed, the node's id
//! (`Node::open_on`), so the node that draws the shortest delay wins every range's election at once,
//! at a cell's boot and at each failover: on the Mac Studio one node's first shard ran every
//! range's proposals, appends, ReadIndex rounds, reads and applies, 89–93% busy, while the other two
//! nodes' sat at 14–23%. Drawn apart, five ranges' elections over three nodes still leave a node
//! leading none in 38% of cells (3·(2/3)^5 − 3·(1/3)^5), so the lead is placed instead.
//!
//! **The rule.** A cell holds one range of each layer (`process::ranges`), so a range's layer is its
//! place in the cell, known to every member of it. Over a range's voters in ascending order, the
//! range at place `k` of `n` voters ranks first the voter at `k mod n`; past it, the rest in turn
//! upward where `k / n` is even and downward where it is odd. Three voters' five ranges first choose
//! two, two and one, and with one node down their second choices fall to the two others, three and
//! two. Every member derives the same ranking from the range's own configuration. The ranking says
//! where a lead goes; the counts of leads say whether, as PD's balance-leader scheduler and
//! CockroachDB's lease rebalancing move toward an even count: from any leads, five ranges over
//! three nodes settle two, two and one, each node leading at least one.
//!
//! **The hand-over.** A range's leader hands its lead to the best-ranked voter above itself that its
//! detectors trust, that has not announced its stop, whose disk is not judged slow, and that leads
//! two or more fewer ranges than this node by what the node knows (PD's balance-leader moves from
//! the store with the most leaders to the one with the fewest; one move narrows a gap of two or
//! more and none smaller, so one range alone never moves), once that
//! voter has been so for the range's patience and is caught up with everything committed at the
//! turn the hand-over begins (TiKV refuses a transfer to a lagging peer; Raft §3.10 has the leader
//! bring the target's log up to its own first). The patience is the node's own measure
//! (`Node::patience`, `Status::patience`): the longest a peer's evidence may go unmoved before it is
//! suspected, a judge's wait for its own flush, and the range's expected election and one more
//! round. A voter trusted that long has been heard within its detector's bound, not merely not yet
//! suspected; and a hand-over ends, done or given up by the core, within an election (Raft §3.10),
//! so one patience apart the last has ended and been seen. None begins while the node stops (its
//! stop hands every lead over itself), while the range's configuration changes (a learner, a joint
//! configuration, a change not yet applied, one whose commit not every voter's durable state
//! states, a replacement under way), while the range's own core has a transfer out, or while
//! another of the node's is out: one at a time from a node, and the next a patience after the last
//! began, CockroachDB's spacing of a store's lease transfers (research/101 §2) at the cell's own
//! measured time. Each hand-over raises the range's leader in its ranking, so while the cell's
//! health holds a range moves at most `n − 1` times and then stays: nothing flaps. A hand-over the
//! core gave up, or that ended with another voter leading, is failed: the range's next waits twice
//! as long, and after `n − 1` failures in a row, the most moves the rule asks of a range, none
//! begins until the range's term changes.

use mantle_meta::apply::Layer;
use mantle_meta::engine::Engine;
use mantle_range::LogStore;

use super::shard::Replica;
use super::{Node, NodeLog};

/// A range's place in the cell's order of ranges, one range of each layer.
fn place(layer: Layer) -> usize {
    match layer {
        Layer::Root => 0,
        Layer::Bucket => 1,
        Layer::Name => 2,
        Layer::File => 3,
        Layer::Block => 4,
    }
}

/// The position, in the voters' ascending order, of the voter of `rank` for the range at `place`
/// among `count` voters; none past them.
fn position(place: usize, count: usize, rank: usize) -> Option<usize> {
    if rank >= count {
        return None;
    }
    let first = place.checked_rem(count)?;
    let upward = place.checked_div(count)?.checked_rem(2)? == 0;
    if upward {
        first.checked_add(rank)?.checked_rem(count)
    } else {
        first
            .checked_add(count)?
            .checked_sub(rank)?
            .checked_rem(count)
    }
}

/// The voter of `rank` for the range of `layer` among `voters` (distinct), 0 the one it prefers.
pub(super) fn ranked(layer: Layer, voters: &[u64], rank: usize) -> Option<u64> {
    let at = position(place(layer), voters.len(), rank)?;
    voters
        .iter()
        .copied()
        .find(|voter| voters.iter().filter(|other| *other < voter).count() == at)
}

/// The rank of `voter` for the range of `layer` among `voters`; none for one not among them.
pub(super) fn rank(layer: Layer, voters: &[u64], voter: u64) -> Option<usize> {
    (0..voters.len()).find(|r| ranked(layer, voters, *r) == Some(voter))
}

/// The voter the range of `layer` among `voters`, led by `own`, should go to: the best-ranked above
/// `own` that `fit` passes. None where `own` ranks first, or none above it is fit.
pub(super) fn heir(
    layer: Layer,
    voters: &[u64],
    own: u64,
    fit: impl Fn(u64) -> bool,
) -> Option<u64> {
    let mine = rank(layer, voters, own)?;
    (0..mine)
        .filter_map(|r| ranked(layer, voters, r))
        .find(|voter| fit(*voter))
}

/// The heir of the range `replica` leads, of `layer` ([`heir`]): none while it leads not, its
/// configuration changes (`replacing` a replacement under way), its core has a transfer out, or it
/// cannot take part (waiting for room, fenced, its log uncertain). A voter is fit while the node's
/// detectors trust it, it has not announced its stop, its disk is not judged slow, and it leads at
/// least two fewer ranges than this node, by `leads`, the ranges the node knows each voter to lead:
/// a move of one lead narrows the gap between two nodes' counts only when it is two or more, so a
/// cell whose counts differ by one at most moves nothing, and one range alone never moves. Whether
/// the heir is caught up is asked as a hand-over begins ([`Node::balance_leads`]).
pub(super) fn heir_of<E: Engine, S: LogStore>(
    replica: &Replica<E, S>,
    layer: Layer,
    replacing: bool,
    leads: impl Fn(u64) -> usize,
) -> Option<u64> {
    // Asked at every turn: a follower, and a leader its range ranks first, the cell's steady state,
    // answer before anything else is looked at.
    if !replica.is_leader() {
        return None;
    }
    let conf = replica.configuration();
    if rank(layer, &conf.voters, replica.id()).is_none_or(|mine| mine == 0) {
        return None;
    }
    let raft = &replica.core().raft;
    let changing = replacing
        || !conf.learners.is_empty()
        || !conf.voters_outgoing.is_empty()
        || !conf.learners_next.is_empty()
        || raft.has_pending_conf()
        || !replica.configuration_known();
    if changing
        || raft.lead_transferee().is_some()
        || replica.is_stalled()
        || replica.fenced().is_some()
        || replica.mark().is_some()
    {
        return None;
    }
    let own = leads(replica.id());
    heir(layer, &conf.voters, replica.id(), |voter| {
        !raft.suspects(voter)
            && !raft.stops(voter)
            && !replica.disk_is_slow(voter)
            && leads(voter) < own.saturating_sub(1)
    })
}

/// What a range keeps of its heir: the voter, and since when it has been the range's heir.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Watch {
    heir: Option<(u64, u64)>,
    /// Hand-overs of the range that failed in a row in the range's term `term`, each doubling its
    /// wait; a new term starts them over.
    failures: u32,
    term: u64,
}

impl Watch {
    /// The range's heir now, at `now`, in `term`: one that stays keeps the time it became the heir.
    fn see(&mut self, heir: Option<u64>, now: u64, term: u64) {
        if term != self.term {
            self.term = term;
            self.failures = 0;
        }
        self.heir = match (self.heir, heir) {
            (Some((was, since)), Some(voter)) if was == voter => Some((was, since)),
            (_, Some(voter)) => Some((voter, now)),
            (_, None) => None,
        };
    }

    /// The heir due a hand-over at `now`, of a range of `voters` whose patience is `patience_ns`:
    /// the heir for its patience, doubled for each failure in a row, and fewer than `voters − 1`
    /// failures in a row.
    fn due(&self, now: u64, voters: usize, patience_ns: u64) -> Option<u64> {
        let (voter, since) = self.heir?;
        let spent = usize::try_from(self.failures)
            .map_or(true, |failures| failures >= voters.saturating_sub(1));
        let wait = 1u64
            .checked_shl(self.failures)
            .and_then(|doubled| patience_ns.checked_mul(doubled))?;
        if spent || now < since.saturating_add(wait) {
            return None;
        }
        Some(voter)
    }

    /// A hand-over of the range began: its heir let go.
    fn began(&mut self) {
        self.heir = None;
    }

    /// A hand-over of the range failed.
    fn failed(&mut self) {
        self.failures = self.failures.saturating_add(1);
    }
}

/// A hand-over out: its range's group and the voter it goes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Out {
    group: u128,
    to: u64,
}

/// The node's hand-overs that spread the leads: the one out, when the next may begin, and the counts
/// since it opened that its status says.
#[derive(Debug, Default)]
pub(super) struct Leads {
    out: Option<Out>,
    next: Option<u64>,
    /// Hand-overs begun, and of them those that failed (the core gave it up, or another voter
    /// took the lead), with those the core refused at once.
    pub(super) begun: u64,
    pub(super) failed: u64,
}

/// Where a hand-over out stands.
enum Followed {
    Out,
    Done,
    Failed,
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// A turn's step of the spread of the leads (module docs): follows the hand-over out, sees each
    /// led range's heir, and begins at most one hand-over. Nothing while the node stops.
    pub(super) fn balance_leads(&mut self, now: u64) {
        if self.stopping.is_some() {
            return;
        }
        if let Some(out) = self.leads.out {
            let followed = self.follow(out);
            let watch = self
                .ranges
                .iter_mut()
                .find(|k| k.group == out.group)
                .map(|k| &mut k.lead);
            match followed {
                Followed::Out => {}
                Followed::Done => self.leads.out = None,
                Followed::Failed => {
                    if let Some(watch) = watch {
                        watch.failed();
                    }
                    self.leads.failed = self.leads.failed.saturating_add(1);
                    self.leads.out = None;
                }
            }
        }
        for at in 0..self.ranges.len() {
            let Some(kept) = self.ranges.get(at) else {
                continue;
            };
            let Some(replica) = self.owner.get(kept.handle) else {
                continue;
            };
            let heir = heir_of(replica, kept.layer, kept.replacing.is_some(), |voter| {
                self.leads_of(voter)
            });
            let term = replica.term();
            if let Some(kept) = self.ranges.get_mut(at) {
                kept.lead.see(heir, now, term);
            }
        }
        let resting = self.leads.next.is_some_and(|next| now < next);
        if self.leads.out.is_some() || resting {
            return;
        }
        for at in 0..self.ranges.len() {
            // A range with no heir, the steady state, is passed before its patience is worked out.
            let Some(group) = self
                .ranges
                .get(at)
                .filter(|k| k.lead.heir.is_some())
                .map(|k| k.group)
            else {
                continue;
            };
            // Unmeasured, the range's patience times nothing yet: no hand-over until it does.
            let Some(patience_ns) = self
                .patience(group)
                .map(|p| u64::try_from(p.as_nanos()).unwrap_or(u64::MAX))
            else {
                continue;
            };
            let Some(kept) = self.ranges.get_mut(at) else {
                continue;
            };
            let Some(replica) = self.owner.get_mut(kept.handle) else {
                continue;
            };
            let voters = replica.configuration().voters.len();
            let Some(to) = kept.lead.due(now, voters, patience_ns) else {
                continue;
            };
            // A voter behind what the range committed waits for a turn that finds it caught up.
            if !replica.caught_up(to) {
                continue;
            }
            kept.lead.began();
            self.leads.next = Some(now.saturating_add(patience_ns));
            if replica.transfer(to).is_ok() {
                self.leads.begun = self.leads.begun.saturating_add(1);
                self.leads.out = Some(Out {
                    group: kept.group,
                    to,
                });
                self.owner.schedule(kept.handle);
            } else {
                self.leads.failed = self.leads.failed.saturating_add(1);
                kept.lead.failed();
            }
            return;
        }
    }

    /// Where the hand-over `out` stands: done once its voter leads, out while this node's core
    /// hands to it or the range elects, failed once the core gave it up or another voter leads.
    /// A range no longer driven here (moved to another shard) is done with as its own.
    fn follow(&self, out: Out) -> Followed {
        let Some(replica) = self
            .ranges
            .iter()
            .find(|k| k.group == out.group)
            .and_then(|k| self.owner.get(k.handle))
        else {
            return Followed::Done;
        };
        match replica.leader() {
            leader if leader == out.to => Followed::Done,
            0 => Followed::Out,
            leader if leader == self.id => {
                if replica.core().raft.lead_transferee() == Some(out.to) {
                    Followed::Out
                } else {
                    Followed::Failed
                }
            }
            _ => Followed::Failed,
        }
    }

    /// The ranges this node knows `voter` leads: those its own task drives by their replicas, and
    /// those another shard drives as it last showed them.
    fn leads_of(&self, voter: u64) -> usize {
        let own = self
            .ranges
            .iter()
            .filter_map(|k| self.owner.get(k.handle))
            .filter(|replica| replica.leader() == voter)
            .count();
        let far = self
            .far
            .iter()
            .filter(|far| far.seen.leader == voter)
            .count();
        own.saturating_add(far)
    }

    /// The hand-overs this node began to spread the leads, and of them those that failed.
    pub fn lead_moves(&self) -> (u64, u64) {
        (self.leads.begun, self.leads.failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LAYERS: [Layer; 5] = [
        Layer::Root,
        Layer::Bucket,
        Layer::Name,
        Layer::File,
        Layer::Block,
    ];

    /// The voters each range prefers, for `voters`.
    fn firsts(voters: &[u64]) -> Vec<u64> {
        LAYERS
            .iter()
            .map(|layer| ranked(*layer, voters, 0).unwrap())
            .collect()
    }

    /// Each range's ranking is a permutation of its voters, whatever order the configuration names
    /// them in: every member works out the same one.
    #[test]
    fn each_ranking_orders_every_voter_once_whatever_the_configurations_order() {
        for voters in [vec![1, 2, 3], vec![3, 1, 2], vec![9, 4, 7, 2, 5], vec![5]] {
            let mut sorted = voters.clone();
            sorted.sort_unstable();
            for layer in LAYERS {
                let mut ranking: Vec<u64> = (0..voters.len())
                    .map(|r| ranked(layer, &voters, r).unwrap())
                    .collect();
                assert_eq!(ranking, {
                    let mut shuffled = sorted.clone();
                    shuffled.sort_unstable_by_key(|v| rank(layer, &sorted, *v).unwrap());
                    shuffled
                });
                ranking.sort_unstable();
                assert_eq!(ranking, sorted, "{layer:?} over {voters:?}");
                assert_eq!(ranked(layer, &voters, voters.len()), None);
            }
        }
    }

    /// Three voters lead the five ranges two, two and one; five voters, one each.
    #[test]
    fn the_cells_ranges_are_led_evenly() {
        let three = firsts(&[11, 22, 33]);
        assert_eq!(three, vec![11, 22, 33, 11, 22]);
        let mut five = firsts(&[1, 2, 3, 4, 5]);
        five.sort_unstable();
        assert_eq!(five, vec![1, 2, 3, 4, 5]);
    }

    /// With any one of three voters unfit, its ranges' second choices fall to the two others, so
    /// the five ranges are led three and two, never four and one.
    #[test]
    fn a_voter_down_leaves_its_ranges_spread_over_the_others() {
        let voters = [1, 2, 3];
        for down in voters {
            let mut led = [0usize; 4];
            for layer in LAYERS {
                let lead = (0..voters.len())
                    .filter_map(|r| ranked(layer, &voters, r))
                    .find(|v| *v != down)
                    .unwrap();
                led[usize::try_from(lead).unwrap()] += 1;
            }
            let mut counts: Vec<usize> = voters
                .iter()
                .filter(|v| **v != down)
                .map(|v| led[usize::try_from(*v).unwrap()])
                .collect();
            counts.sort_unstable();
            assert_eq!(counts, vec![2, 3], "voter {down} down: {led:?}");
        }
    }

    /// A leader hands only up its ranking: to the best-ranked fit voter above it, never to one
    /// below, never to an unfit one, and the preferred voter keeps its lead.
    #[test]
    fn a_leader_hands_only_up_its_ranking_to_a_fit_voter() {
        let voters = [1, 2, 3];
        // The Name range prefers 3, then 1, then 2.
        assert_eq!(
            (0..3)
                .map(|r| ranked(Layer::Name, &voters, r).unwrap())
                .collect::<Vec<_>>(),
            vec![3, 1, 2]
        );
        assert_eq!(heir(Layer::Name, &voters, 2, |_| true), Some(3));
        assert_eq!(heir(Layer::Name, &voters, 2, |v| v != 3), Some(1));
        assert_eq!(heir(Layer::Name, &voters, 2, |_| false), None);
        assert_eq!(heir(Layer::Name, &voters, 1, |_| true), Some(3));
        assert_eq!(heir(Layer::Name, &voters, 1, |v| v != 3), None);
        assert_eq!(heir(Layer::Name, &voters, 3, |_| true), None);
        // A leader no voter of the configuration hands to none.
        assert_eq!(heir(Layer::Name, &voters, 4, |_| true), None);
    }

    /// Following the rule from any leaders, each range moves only up its ranking, so the cell
    /// settles on each range's first choice within `n − 1` hand-overs of it and moves no more.
    #[test]
    fn from_any_leaders_the_cell_settles_within_n_less_one_moves_a_range() {
        let voters = [1, 2, 3];
        for start in 1..=3u64 {
            for layer in LAYERS {
                let (mut lead, mut moves) = (start, 0);
                while let Some(next) = heir(layer, &voters, lead, |_| true) {
                    assert!(
                        rank(layer, &voters, next).unwrap() < rank(layer, &voters, lead).unwrap()
                    );
                    lead = next;
                    moves += 1;
                }
                assert!(moves < voters.len());
                assert_eq!(lead, ranked(layer, &voters, 0).unwrap());
            }
        }
    }

    /// From every assignment of the five ranges' leads over three voters, moving each range up its
    /// ranking to the best-ranked voter leading two or more fewer, a pass over the ranges at a time,
    /// settles within two moves a range on counts of two, two and one; one range alone never moves.
    #[test]
    fn from_every_assignment_the_counts_settle_two_two_and_one() {
        let voters = [1u64, 2, 3];
        for start in 0..3usize.pow(5) {
            let mut lead: Vec<u64> = (0..5)
                .map(|i| voters[(start / 3usize.pow(i)) % 3])
                .collect();
            let mut moves = vec![0usize; 5];
            loop {
                let mut moved = false;
                for (i, layer) in LAYERS.iter().enumerate() {
                    let count = |v: u64| lead.iter().filter(|l| **l == v).count();
                    let own = count(lead[i]);
                    if let Some(to) = heir(*layer, &voters, lead[i], |v| {
                        count(v) < own.saturating_sub(1)
                    }) {
                        lead[i] = to;
                        moves[i] += 1;
                        moved = true;
                    }
                }
                if !moved {
                    break;
                }
            }
            let mut counts: Vec<usize> = voters
                .iter()
                .map(|v| lead.iter().filter(|l| *l == v).count())
                .collect();
            counts.sort_unstable();
            assert_eq!(counts, vec![1, 2, 2], "from {start}: {lead:?}");
            assert!(moves.iter().all(|m| *m < voters.len()), "{moves:?}");
        }
        // One range alone: its leader leads one, and no voter leads fewer than none.
        for one in voters {
            let own = 1usize;
            let count = |v: u64| usize::from(v == one);
            assert_eq!(
                heir(Layer::Name, &voters, one, |v| count(v)
                    < own.saturating_sub(1)),
                None
            );
        }
    }

    /// An heir is due once it has stayed the heir for the range's patience, doubled for each
    /// failure in a row; a change of heir starts its wait again; after `n − 1` failures in a row none
    /// is due until the range's term changes, which starts the failures over.
    #[test]
    fn an_heir_is_due_after_its_patience_doubled_per_failure_until_n_less_one_failures() {
        let patience = 700;
        let mut watch = Watch::default();
        assert_eq!(watch.due(u64::MAX, 3, patience), None);
        watch.see(Some(3), 100, 4);
        watch.see(Some(3), 100 + patience / 2, 4);
        assert_eq!(watch.due(100 + patience - 1, 3, patience), None);
        assert_eq!(watch.due(100 + patience, 3, patience), Some(3));
        watch.see(Some(1), 100 + patience, 4);
        assert_eq!(watch.due(100 + patience, 3, patience), None);
        assert_eq!(watch.due(100 + 2 * patience, 3, patience), Some(1));
        watch.failed();
        assert_eq!(watch.due(100 + 2 * patience, 3, patience), None);
        assert_eq!(watch.due(100 + 3 * patience, 3, patience), Some(1));
        watch.failed();
        assert_eq!(
            watch.due(u64::MAX, 3, patience),
            None,
            "two failures of three voters"
        );
        assert_eq!(watch.due(100 + 5 * patience, 5, patience), Some(1));
        watch.see(Some(1), 100 + 5 * patience, 5);
        assert_eq!(
            watch.due(100 + 5 * patience, 3, patience),
            Some(1),
            "a new term"
        );
        watch.began();
        assert_eq!(
            watch.due(u64::MAX, 3, patience),
            None,
            "let go as it is handed the lead"
        );
    }
}
