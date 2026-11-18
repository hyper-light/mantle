//! Which of the node's shards runs each of its ranges (docs/design/node.md §2.9). A range is placed
//! when the node opens it, for its life in the process, on the shard past the first that holds the
//! fewest of the node's ranges, the first of those tied, as Redpanda assigns each partition's
//! replica to a core for an even count across a broker's cores (research/95 §2); a node of one
//! shard holds every range on it. The first shard is passed over because it carries the node's one
//! UDP endpoint, its plane, its liveness stream and the S3 door's accept. The rule counts and picks
//! no number, and nothing on disk names a shard, so a node started again with another CPU count
//! places its ranges afresh.

use std::num::NonZeroUsize;

/// The node's ranges by the shard each runs on: each range's shard by its place in the node's
/// table, the order the node opened them, and each shard's count of ranges, the rule's measure.
/// Bound: a place a range the node hosts, and an entry a shard the ranges may run on, both fixed
/// when the node opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Spread {
    shard_of: Vec<usize>,
    held: Vec<usize>,
}

impl Spread {
    /// A spread over `shards` shards for a table of `ranges` ranges, holding none yet.
    pub(super) fn over(shards: NonZeroUsize, ranges: usize) -> Self {
        Self {
            shard_of: Vec::with_capacity(ranges),
            held: vec![0; shards.get()],
        }
    }

    /// Places the table's next range and says its shard: the shard past the first holding the
    /// fewest ranges, the first of those tied (`min_by_key` keeps the first of equal keys); the
    /// first where it is the only shard. `None` where that shard's count would pass what a `usize`
    /// counts, which no table of ranges in memory reaches; nothing is placed then.
    pub(super) fn place(&mut self) -> Option<usize> {
        let shard = match self
            .held
            .iter()
            .enumerate()
            .skip(1)
            .min_by_key(|(_, held)| **held)
        {
            Some((shard, _)) => shard,
            None => 0,
        };
        let held = self.held.get_mut(shard)?;
        *held = held.checked_add(1)?;
        self.shard_of.push(shard);
        Some(shard)
    }

    /// The shards the ranges are placed over, the first among them.
    pub(super) fn shards(&self) -> usize {
        self.held.len()
    }

    /// The shards that run ranges once `ranges` ranges are placed over `shards` by the rule: the
    /// one where it is the only shard, and otherwise those past the first, each taking one before
    /// any takes a second, so at most one a range; none where there is no range.
    pub(super) fn running(shards: NonZeroUsize, ranges: usize) -> usize {
        match shards.get().checked_sub(1) {
            Some(0) | None => ranges.min(1),
            Some(past) => past.min(ranges),
        }
    }

    /// The shard that runs the range at `place` in the table.
    pub(super) fn shard(&self, place: usize) -> Option<usize> {
        self.shard_of.get(place).copied()
    }

    /// The places in the table of the ranges `shard` runs, in the table's order.
    pub(super) fn on(&self, shard: usize) -> impl Iterator<Item = usize> + '_ {
        self.shard_of
            .iter()
            .enumerate()
            .filter(move |(_, runs)| **runs == shard)
            .map(|(place, _)| place)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn over(shards: usize) -> Spread {
        Spread::over(NonZeroUsize::new(shards).unwrap(), 0)
    }

    #[test]
    fn a_node_of_one_shard_holds_every_range_on_it() {
        let mut spread = over(1);
        for _ in 0..5 {
            assert_eq!(spread.place(), Some(0));
        }
        assert_eq!(spread.held, vec![5]);
        assert_eq!(spread.on(0).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
        assert_eq!(spread.on(1).count(), 0);
    }

    #[test]
    fn a_range_goes_to_the_shard_past_the_first_with_the_fewest_the_first_of_those_tied() {
        let mut spread = over(4);
        let placed: Vec<usize> = (0..7).map(|_| spread.place().unwrap()).collect();
        assert_eq!(placed, vec![1, 2, 3, 1, 2, 3, 1]);
        assert_eq!(spread.held, vec![0, 3, 2, 2]);

        // Two shards: every range goes to the second, never the first.
        let mut spread = over(2);
        for _ in 0..3 {
            assert_eq!(spread.place(), Some(1));
        }
        assert_eq!(spread.held, vec![0, 3]);
    }

    #[test]
    fn the_table_says_each_ranges_shard_and_each_shards_ranges_in_its_order() {
        let mut spread = over(3);
        for _ in 0..5 {
            spread.place().unwrap();
        }
        let shards: Vec<Option<usize>> = (0..6).map(|place| spread.shard(place)).collect();
        assert_eq!(
            shards,
            vec![Some(1), Some(2), Some(1), Some(2), Some(1), None]
        );
        assert_eq!(spread.on(0).count(), 0);
        assert_eq!(spread.on(1).collect::<Vec<_>>(), vec![0, 2, 4]);
        assert_eq!(spread.on(2).collect::<Vec<_>>(), vec![1, 3]);
        assert_eq!(spread.on(3).count(), 0, "a shard past the spread's");
    }

    #[test]
    fn a_cells_five_ranges_take_a_shard_each_wherever_there_are_shards_for_them() {
        for shards in 6..=32 {
            let mut spread = over(shards);
            let placed: Vec<usize> = (0..5).map(|_| spread.place().unwrap()).collect();
            assert_eq!(placed, vec![1, 2, 3, 4, 5], "{shards} shards");
        }
    }

    #[test]
    fn the_shards_running_ranges_are_those_the_rule_fills_at_most_one_a_range() {
        for shards in 1..=9 {
            let mut spread = over(shards);
            for ranges in 0..=40 {
                if ranges > 0 {
                    spread.place().unwrap();
                }
                let filled = spread.held.iter().filter(|held| **held > 0).count();
                assert_eq!(
                    Spread::running(NonZeroUsize::new(shards).unwrap(), ranges),
                    filled,
                    "{shards} shards, {ranges} ranges"
                );
                assert_eq!(spread.shards(), shards);
            }
        }
    }

    #[test]
    fn every_shard_past_the_first_stays_within_one_range_of_every_other() {
        for shards in 1..=9 {
            let mut spread = over(shards);
            for ranges in 1..=40 {
                spread.place().unwrap();
                let rest = if shards == 1 {
                    &spread.held[..]
                } else {
                    assert_eq!(spread.held[0], 0, "{shards} shards, {ranges} ranges");
                    &spread.held[1..]
                };
                let most = rest.iter().max().unwrap();
                let least = rest.iter().min().unwrap();
                assert!(
                    most - least <= 1,
                    "{shards} shards, {ranges} ranges: {rest:?}"
                );
                assert_eq!(rest.iter().sum::<usize>(), ranges);
            }
        }
    }

    #[test]
    fn a_count_past_a_usizes_is_refused_and_places_nothing() {
        let mut spread = over(2);
        spread.held[1] = usize::MAX;
        assert_eq!(spread.place(), None);
        assert_eq!(spread.held, vec![0, usize::MAX]);
        assert_eq!(spread.shard(0), None);
    }
}
