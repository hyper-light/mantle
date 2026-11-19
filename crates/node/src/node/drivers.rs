//! Which of the node's shards drives each native request (docs/design/node.md §2.9, step 6). A
//! request the host admits is placed on the shard past the first running the fewest of the node's
//! native requests, the first of those tied, as the S3 listener places a connection on the shard
//! past the first with the fewest open (`listener::shards::least`), and counted there until its
//! job ends; a node of one shard drives every request on it. The first shard is passed over because
//! it carries the node's one UDP endpoint, its plane, its liveness stream and the S3 door's accept.
//! The rule counts and picks no number.

use std::num::NonZeroUsize;

/// The native requests running on each of the node's shards, numbered from the node's first: the
/// rule's measure. Bound: an entry a shard, fixed when the node opens; each count at most the jobs
/// the host holds, each request counted being one of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Drivers {
    running: Vec<usize>,
}

impl Drivers {
    /// Drivers over `shards` shards, running none.
    pub(super) fn over(shards: NonZeroUsize) -> Self {
        Self {
            running: vec![0; shards.get()],
        }
    }

    /// Places a request and says its shard: of the shards past the first that `takes` says take
    /// one now (a driving task's, its shard not held past the watchdog's limit), the one running
    /// the fewest, the first of those tied (`min_by_key` keeps the first of equal keys); the first
    /// where none does, or it is the only shard. `None` where that shard's count would pass what a
    /// `usize` counts, which no host's jobs reach; nothing is placed then.
    pub(super) fn place(&mut self, takes: impl Fn(usize) -> bool) -> Option<usize> {
        let shard = self
            .running
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(shard, _)| takes(*shard))
            .min_by_key(|(_, running)| **running)
            .map_or(0, |(shard, _)| shard);
        let running = self.running.get_mut(shard)?;
        *running = running.checked_add(1)?;
        Some(shard)
    }

    /// A request placed on `shard` ended: its place in the count given back. A shard running none,
    /// or past the drivers', counts nothing.
    pub(super) fn ended(&mut self, shard: usize) {
        if let Some(running) = self.running.get_mut(shard)
            && let Some(less) = running.checked_sub(1)
        {
            *running = less;
        }
    }

    /// A request placed on `from` runs on `to` instead, its lane having refused it (node.md §2.9
    /// step 6f): its place in the count moved there, so it is counted on the shard that runs it
    /// until its job ends. `None`, the count as it was, where `to` is past the drivers' or its
    /// count would pass what a `usize` counts.
    pub(super) fn moved(&mut self, from: usize, to: usize) -> Option<()> {
        let running = self.running.get_mut(to)?;
        *running = running.checked_add(1)?;
        self.ended(from);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn over(shards: usize) -> Drivers {
        Drivers::over(NonZeroUsize::new(shards).unwrap())
    }

    #[test]
    fn a_node_of_one_shard_drives_every_request_on_it() {
        let mut drivers = over(1);
        for _ in 0..5 {
            assert_eq!(drivers.place(|_| true), Some(0));
        }
        assert_eq!(drivers.running, vec![5]);
        drivers.ended(0);
        assert_eq!(drivers.running, vec![4]);
    }

    #[test]
    fn a_request_goes_to_the_shard_past_the_first_running_the_fewest_the_first_of_those_tied() {
        let mut drivers = over(4);
        let placed: Vec<usize> = (0..7).map(|_| drivers.place(|_| true).unwrap()).collect();
        assert_eq!(placed, vec![1, 2, 3, 1, 2, 3, 1]);
        assert_eq!(drivers.running, vec![0, 3, 2, 2]);

        // Two shards: every request goes to the second, never the first.
        let mut drivers = over(2);
        for _ in 0..3 {
            assert_eq!(drivers.place(|_| true), Some(1));
        }
        assert_eq!(drivers.running, vec![0, 3]);
    }

    #[test]
    fn a_request_that_ended_gives_its_place_back_to_the_rule() {
        let mut drivers = over(4);
        for _ in 0..7 {
            drivers.place(|_| true).unwrap();
        }
        drivers.ended(2);
        drivers.ended(2);
        assert_eq!(drivers.running, vec![0, 3, 0, 2]);
        let placed: Vec<usize> = (0..4).map(|_| drivers.place(|_| true).unwrap()).collect();
        assert_eq!(placed, vec![2, 2, 2, 3]);
        assert_eq!(drivers.running, vec![0, 3, 3, 3]);
    }

    #[test]
    fn a_shard_that_takes_none_now_is_passed_over() {
        let mut drivers = over(4);
        // The second shard stalled: the rest past the first take the requests.
        let placed: Vec<usize> = (0..4).map(|_| drivers.place(|s| s != 1).unwrap()).collect();
        assert_eq!(placed, vec![2, 3, 2, 3]);
        // It takes again: the rule fills it first, as it runs the fewest.
        assert_eq!(drivers.place(|_| true), Some(1));
        // None past the first takes one: the first runs it.
        assert_eq!(drivers.place(|_| false), Some(0));
        assert_eq!(drivers.running, vec![1, 1, 2, 2]);
    }

    #[test]
    fn a_request_a_lane_refused_is_counted_on_the_shard_that_runs_it() {
        let mut drivers = over(3);
        assert_eq!(
            (drivers.place(|_| true), drivers.place(|_| true)),
            (Some(1), Some(2))
        );
        // The second shard's lane refused its request: it runs on the first, counted there.
        assert_eq!(drivers.moved(1, 0), Some(()));
        assert_eq!(drivers.running, vec![1, 0, 1]);
        // The rule places the next on the shard that gave its place back.
        assert_eq!(drivers.place(|_| true), Some(1));
        // Its end gives the first shard's place back.
        drivers.ended(0);
        assert_eq!(drivers.running, vec![0, 1, 1]);
        // A shard past the drivers' counts nothing, the request's place kept where it was.
        assert_eq!(drivers.moved(1, 3), None);
        assert_eq!(drivers.running, vec![0, 1, 1]);
    }

    #[test]
    fn every_shard_past_the_first_stays_within_one_request_of_every_other() {
        for shards in 1..=9 {
            let mut drivers = over(shards);
            for requests in 1..=40 {
                drivers.place(|_| true).unwrap();
                let rest = if shards == 1 {
                    &drivers.running[..]
                } else {
                    assert_eq!(
                        drivers.running[0], 0,
                        "{shards} shards, {requests} requests"
                    );
                    &drivers.running[1..]
                };
                let most = rest.iter().max().unwrap();
                let least = rest.iter().min().unwrap();
                assert!(
                    most - least <= 1,
                    "{shards} shards, {requests} requests: {rest:?}"
                );
                assert_eq!(rest.iter().sum::<usize>(), requests);
            }
        }
    }

    #[test]
    fn an_end_where_none_runs_counts_nothing() {
        let mut drivers = over(2);
        drivers.ended(1);
        drivers.ended(2);
        assert_eq!(
            drivers.running,
            vec![0, 0],
            "a shard past the drivers' counts nothing"
        );
    }

    #[test]
    fn a_count_past_a_usizes_is_refused_and_places_nothing() {
        let mut drivers = over(2);
        drivers.running[1] = usize::MAX;
        assert_eq!(drivers.place(|_| true), None);
        assert_eq!(drivers.running, vec![0, usize::MAX]);
    }
}
