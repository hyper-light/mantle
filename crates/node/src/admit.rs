//! The operations a node admits at once, learned from the operations themselves (docs/design/
//! node.md §2.8, research/87 §3): the knee, Denning and Buzen's saturation point `N*`, past which
//! "queueing is certain to be observed", found as the point of greatest power, Kleinrock's
//! throughput over delay `P = X/R̄`, which rises with each operation admitted below the knee and
//! falls past it: "Keep the pipe just full, but no fuller".
//!
//! The node finds it as BBR probes its pipe (Linux `net/ipv4/tcp_bbr.c`), from above: it starts at
//! the ceiling its memory was split for, as a fixed limit ran, and halves while power does not fall
//! by a fifth, the reciprocal of BBR's startup's `5/4`; then it cycles its limit over eight phases,
//! a quarter past its base, a quarter below it, and six at it, and at each cycle's end takes the
//! limit whose power was greatest. A phase begins once the operations admitted until its first
//! completion have completed, BBR's packet-timed round, so it measures its own limit, and ends after
//! [`PHASE`] completions, enough to tell a quarter's change in power from chance.
//!
//! Power, not a bandwidth-delay product of a windowed maximum and minimum as BBR keeps: operations
//! of a hundredfold different lengths side by side make the least latency the smallest one's, and
//! a limit built on it sits far below the knee (research/87 §3, the model `power.py`); power's
//! latency is the phase's mean, every operation's.

/// The completions a phase measures: a quarter's change in power told from chance. A rate measured
/// over `n` completions has a relative error of `1/√n`, and the ratio of two such phases' powers a
/// log error of `√(2/n)`; a probe a quarter away changes the log of power by `ln(5/4)` at the
/// least (`5/4` below the knee, `4/5` above it), so `n ≥ 2·(z/ln(5/4))²` at `z` 2.576, the normal
/// distribution's two-sided 99% point: 267.
pub(crate) const PHASE: u64 = 267;

/// The phases' limits about the base, in quarters: a quarter past it, a quarter below, six at it,
/// BBR's `bbr_pacing_gain` cycle (`5/4`, `3/4` and six of one, `tcp_bbr.c`).
const CYCLE: [Quarter; 8] = [
    Quarter::Up,
    Quarter::Down,
    Quarter::At,
    Quarter::At,
    Quarter::At,
    Quarter::At,
    Quarter::At,
    Quarter::At,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quarter {
    Up,
    Down,
    At,
}

/// A phase's measure: its length and its completions' latencies summed, both in nanoseconds. Its
/// power is `PHASE² / (length · latency)`: phases of as many completions compare by that product,
/// the smaller the more powerful.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Measured {
    length_ns: u128,
    latency_ns: u128,
    /// Whether an operation was turned away, or left waiting at a door, in it: a phase in which
    /// none was ran what its clients asked, not what its limit let, and says nothing of a limit
    /// past it.
    turned_away: bool,
}

impl Measured {
    fn cost(&self) -> u128 {
        self.length_ns.saturating_mul(self.latency_ns)
    }
}

/// The node's learned limit on the operations it runs at once.
#[derive(Debug, Clone)]
pub(crate) struct Admission {
    /// The most the node's memory was split for (`gateway.objects`).
    ceiling: usize,
    /// The limit the cycle is about, and the limit now.
    base: usize,
    limit: usize,
    /// Halving from the ceiling: the measure at the base before this one, while it halves.
    descent: Option<Option<(usize, Measured)>>,
    /// The cycle's phase, and what each phase measured.
    phase: usize,
    cycle: [Measured; 8],
    admitted: u64,
    completed: u64,
    /// The completions the phase passes before it measures: those of the operations admitted until
    /// its first completion, before it began and in the burst its limit let in; none yet known.
    settle: Option<u64>,
    /// The phase under way: when it began measuring, its completions, their latencies, and whether
    /// an operation was turned away meanwhile.
    began_ns: u64,
    counted: u64,
    latency_ns: u128,
    turned_away: bool,
}

impl Admission {
    /// A limit within `ceiling`, starting at it, at `now_ns`: the node admits what its memory was
    /// split for until it has measured less to be more, so no client is turned away at its start
    /// that a fixed limit would have taken.
    pub(crate) fn new(ceiling: usize, now_ns: u64) -> Self {
        let ceiling = ceiling.max(1);
        Self {
            ceiling,
            base: ceiling,
            limit: ceiling,
            descent: Some(None),
            phase: 0,
            cycle: [Measured::default(); 8],
            admitted: 0,
            completed: 0,
            settle: None,
            began_ns: now_ns,
            counted: 0,
            latency_ns: 0,
            turned_away: false,
        }
    }

    /// The operations the node admits at once now.
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    /// Whether one more operation is admitted beside `running`.
    pub(crate) fn admits(&self, running: usize) -> bool {
        running < self.limit
    }

    /// An operation was turned away, or left waiting at a door, for want of room.
    pub(crate) fn turned_away(&mut self) {
        self.turned_away = true;
    }

    /// An operation was admitted.
    pub(crate) fn admitted(&mut self) {
        self.admitted = self.admitted.saturating_add(1);
    }

    /// An admitted operation ended at `now_ns`, `latency_ns` after it was admitted.
    pub(crate) fn completed(&mut self, latency_ns: u64, now_ns: u64) {
        self.completed = self.completed.saturating_add(1);
        let settle = *self.settle.get_or_insert(self.admitted);
        if self.completed <= settle {
            // An operation admitted before the phase: the phase measures from the next.
            self.began_ns = now_ns;
            self.turned_away = false;
            return;
        }
        self.counted = self.counted.saturating_add(1);
        self.latency_ns = self.latency_ns.saturating_add(u128::from(latency_ns));
        if self.counted < PHASE {
            return;
        }
        let measured = Measured {
            length_ns: u128::from(now_ns.saturating_sub(self.began_ns)).max(1),
            latency_ns: self.latency_ns.max(1),
            turned_away: self.turned_away,
        };
        self.counted = 0;
        self.latency_ns = 0;
        self.began_ns = now_ns;
        self.turned_away = false;
        match self.descent {
            Some(before) => self.halve(before, measured),
            None => self.cycled(measured),
        }
        self.settle = None;
    }

    /// The descent's step: halve while power does not fall a fifth, the reciprocal of BBR's startup's
    /// `5/4` (`bbr_full_bw_thresh`). Past the knee halving raises power, or leaves it where the limit
    /// did not bind; below it power halves with the limit, and the base goes back to the limit
    /// before.
    fn halve(&mut self, before: Option<(usize, Measured)>, measured: Measured) {
        // Power did not fall a fifth: `P ≥ 4/5 · P_before`, so `4 · cost ≤ 5 · cost_before`.
        let held = before
            .is_none_or(|(_, b)| measured.cost().saturating_mul(4) <= b.cost().saturating_mul(5));
        if held && self.base > 1 {
            self.descent = Some(Some((self.base, measured)));
            self.base = (self.base / 2).max(1);
            self.limit = self.base;
            return;
        }
        if !held && let Some((base, _)) = before {
            self.base = base;
        }
        self.descent = None;
        self.phase = 0;
        self.limit = self.at(CYCLE[0]);
    }

    /// The cycle's step: the phase's measure kept, and at the cycle's end the base moved to the
    /// limit of greatest power among its own six phases', pooled, and each probe its limit bound:
    /// one in which no operation was turned away measured what its clients asked, not its limit.
    fn cycled(&mut self, measured: Measured) {
        if let Some(slot) = self.cycle.get_mut(self.phase) {
            *slot = measured;
        }
        self.phase = self.phase.saturating_add(1);
        if self.phase >= CYCLE.len() {
            self.phase = 0;
            self.base = self.choose();
        }
        self.limit = self.at(CYCLE.get(self.phase).copied().unwrap_or(Quarter::At));
    }

    /// The limit of greatest power the cycle measured. The six phases at the base pool as one of
    /// six times the completions, `36 · PHASE² / (Σ length · Σ latency)`: a probe beats them where
    /// `36 · cost < Σ length · Σ latency`.
    fn choose(&self) -> usize {
        let (mut length, mut latency) = (0u128, 0u128);
        let mut probes = Vec::with_capacity(2);
        for (quarter, measured) in CYCLE.iter().zip(&self.cycle) {
            match quarter {
                Quarter::At => {
                    length = length.saturating_add(measured.length_ns);
                    latency = latency.saturating_add(measured.latency_ns);
                }
                Quarter::Up | Quarter::Down if measured.turned_away => {
                    probes.push((*quarter, measured.cost()));
                }
                Quarter::Up | Quarter::Down => {}
            }
        }
        let pooled = length.saturating_mul(latency);
        // Each probe and the pool, by cost scaled to the pool's 36: the least is the most power.
        let mut best = (pooled, Quarter::At);
        for (quarter, cost) in probes {
            let scaled = cost.saturating_mul(36);
            if scaled < best.0 {
                best = (scaled, quarter);
            }
        }
        self.at(best.1)
    }

    /// The limit a quarter past, a quarter below, or at the base, within one and the ceiling.
    fn at(&self, quarter: Quarter) -> usize {
        let quarters = self.base.saturating_mul(match quarter {
            Quarter::Up => 5,
            Quarter::Down => 3,
            Quarter::At => 4,
        });
        let limit = match quarter {
            Quarter::Up => quarters.div_ceil(4),
            Quarter::Down | Quarter::At => quarters.checked_div(4).unwrap_or(0),
        };
        limit.clamp(1, self.ceiling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    /// A closed system of `servers`, each serving one operation at a time first come first served,
    /// with service times drawn exponentially about a mean taken in turn from `means` (ns), as
    /// Denning and Buzen's bottleneck: its knee is `servers` operations. `clients` keep an
    /// operation asked each; the node admits up to its limit and turns the rest away until one
    /// ends. Runs `steps` completions: the limit after each, and the system's throughput over the
    /// second half as a share of its most.
    struct Closed {
        rng: u64,
        now: u64,
        free: Vec<u64>,
        running: BinaryHeap<Reverse<(u64, u64)>>,
        drawn: usize,
    }

    impl Closed {
        fn new(seed: u64) -> Self {
            Self {
                rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
                now: 0,
                free: Vec::new(),
                running: BinaryHeap::new(),
                drawn: 0,
            }
        }

        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        fn service(&mut self, means: &[u64]) -> u64 {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            let u = ((self.rng >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
            let mean = means[self.drawn % means.len()];
            self.drawn += 1;
            (-(mean as f64) * u.ln()).max(1.0) as u64
        }

        fn run(
            &mut self,
            admission: &mut Admission,
            servers: usize,
            means: &[u64],
            clients: usize,
            steps: usize,
        ) -> (Vec<usize>, f64) {
            self.free.resize(servers, self.now);
            self.free.truncate(servers);
            let mut limits = Vec::with_capacity(steps);
            let mut half = (0u64, 0usize);
            for step in 0..steps {
                while self.running.len() < clients {
                    if !admission.admits(self.running.len()) {
                        admission.turned_away();
                        break;
                    }
                    admission.admitted();
                    let at = (0..servers).min_by_key(|&s| self.free[s]).unwrap();
                    let end = self.free[at].max(self.now) + self.service(means);
                    self.free[at] = end;
                    self.running.push(Reverse((end, self.now)));
                }
                let Reverse((end, admitted)) = self.running.pop().unwrap();
                self.now = end;
                admission.completed(end - admitted, end);
                limits.push(admission.limit());
                if step == steps / 2 {
                    half = (self.now, step);
                }
            }
            let mean = means.iter().sum::<u64>() as f64 / means.len() as f64;
            let most = servers as f64 / mean;
            let done = (steps - half.1) as f64;
            (limits, done / (self.now - half.0) as f64 / most)
        }
    }

    fn mean(limits: &[usize]) -> f64 {
        limits.iter().sum::<usize>() as f64 / limits.len() as f64
    }

    /// From its ceiling, the limit comes down to a closed system's knee and keeps about it, and the
    /// system runs within a fifth of its most throughput, at ceilings near the knee and far past
    /// it; operations of a hundredfold different lengths side by side are learned alike.
    #[test]
    fn the_limit_learns_the_knee_and_keeps_about_it() {
        for (servers, means) in [
            (1usize, &[1_000_000u64][..]),
            (3, &[1_000_000]),
            (8, &[1_000_000]),
            (40, &[1_000_000]),
            (3, &[1_000_000, 100_000_000, 1_000_000, 1_000_000]),
            (16, &[1_000_000, 100_000_000, 1_000_000, 1_000_000]),
            (8, &[500_000, 64_000_000]),
        ] {
            for ceiling in [64, 1_024] {
                let mut admission = Admission::new(ceiling, 0);
                let (limits, share) =
                    Closed::new(1).run(&mut admission, servers, means, 1_000, 60_000);
                let settled = mean(&limits[limits.len() / 2..]);
                let knee = servers as f64;
                assert!(
                    settled >= 0.75 * knee && settled <= 1.5 * knee + 1.0,
                    "{servers} {means:?}: the limit kept {settled:.1}"
                );
                assert!(share >= 0.8, "{servers} {means:?}: {share:.2} of the most");
            }
        }
    }

    /// The limit never passes its ceiling, however much the system would take, and holds it.
    #[test]
    fn the_limit_stays_within_its_ceiling() {
        let mut admission = Admission::new(5, 0);
        let (limits, _) = Closed::new(2).run(&mut admission, 64, &[1_000_000], 1_000, 20_000);
        assert!(limits.iter().all(|&l| (1..=5).contains(&l)));
        assert!(mean(&limits[limits.len() / 2..]) >= 4.0);
    }

    /// A node whose clients ask fewer operations than its knee is limited by them: its limit never
    /// comes below what they ask, as a probe its limit did not bind moves nothing, and rests where
    /// it binds no one; once more clients come it finds the knee.
    #[test]
    fn a_limit_its_clients_bound_climbs_once_they_ask_more() {
        let mut admission = Admission::new(10_000, 0);
        let mut system = Closed::new(3);
        let (limits, _) = system.run(&mut admission, 16, &[1_000_000], 4, 20_000);
        let settled = &limits[limits.len() / 2..];
        assert!(settled.iter().all(|&l| l >= 4), "{:?}", &settled[..20]);
        let (limits, share) = system.run(&mut admission, 16, &[1_000_000], 1_000, 60_000);
        assert!(mean(&limits[limits.len() / 2..]) >= 12.0);
        assert!(share >= 0.8, "{share:.2}");
    }

    /// When the system's capacity falls, the limit follows it down, and when it rises, back up:
    /// the knee is learned again, never kept.
    #[test]
    fn the_limit_follows_the_knee_as_it_moves() {
        let mut admission = Admission::new(10_000, 0);
        let mut system = Closed::new(4);
        system.run(&mut admission, 32, &[1_000_000], 1_000, 40_000);
        let (limits, _) = system.run(&mut admission, 4, &[1_000_000], 1_000, 60_000);
        let settled = mean(&limits[limits.len() / 2..]);
        assert!((3.0..=7.0).contains(&settled), "down to {settled:.1}");
        let (limits, _) = system.run(&mut admission, 32, &[1_000_000], 1_000, 120_000);
        let settled = mean(&limits[limits.len() / 2..]);
        assert!(settled >= 24.0, "back up to {settled:.1}");
    }

    /// The phase's completions are the least that tell a quarter's change in power from chance at
    /// 99%, as its comment derives.
    #[test]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn a_phase_measures_enough_to_tell_a_quarter() {
        let z = 2.576f64;
        let n = (2.0 * (z / 1.25f64.ln()).powi(2)).ceil() as u64;
        assert_eq!(PHASE, n);
    }
}
