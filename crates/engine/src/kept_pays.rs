//! Whether a get's probe of the kept memtables pays for itself (docs/research/90 §6).
//!
//! A memtable is kept only once its branch is in the trunk ([`crate::shard_db`]'s `keep`), so the
//! trunk holds every record the kept memtables do, and a get may pass them by and ask the trunk.
//! A probe pays when the gets it answers save more than every probe costs: `p · s ≥ c`, `p` the
//! share of probes the kept memtables answer, `s` what a get spends past them when they do not
//! answer, and `c` what a probe takes, each measured while the kept memtables hold what they hold
//! now (an epoch: a memtable kept or given back, or an index generation closed, starts the next).
//! Where the measured mean says a probe does not pay, a get still probes while the optimistic
//! bound on `p` says it may: UCB1's `p̂ + √(2 ln t / n)` over the epoch's `t` gets and `n` probes
//! (Auer, Cesa-Bianchi and Fischer, "Finite-time analysis of the multiarmed bandit problem",
//! Machine Learning 47, 2002, Fig. 1), which probes a number of times logarithmic in the gets
//! (their Theorem 1, the order Lai and Robbins 1985 show no rule beats), so a rise in what the
//! kept memtables answer is found and a probe that never pays costs a vanishing share. The clock
//! is read only where the epoch's count of probes or passes reaches a power of two, so its
//! readings are logarithmic in the gets as the probes are; each sample has the clock's own cost,
//! read beside it, taken out.

use std::time::Instant;

/// What the kept memtables' probes paid this epoch, and the decision it gives.
#[derive(Debug, Default)]
pub(crate) struct KeptPays {
    /// The epoch's kept memtables: the next memtable number, the oldest kept, the closed index
    /// generations. Another value starts a new epoch.
    epoch: (u32, Option<u32>, usize),
    /// Gets that reached the kept memtables, probes made, probes the kept memtables answered.
    reached: u64,
    probes: u64,
    answered: u64,
    /// Timed probes and the nanoseconds they took; timed passes (a get past the kept memtables
    /// they did not answer, probed or not) and the nanoseconds the rest of the get took.
    probe_samples: u64,
    probe_ns: u64,
    pass_samples: u64,
    pass_ns: u64,
    /// Passes counted this epoch, for the doubling schedule of their samples.
    passes: u64,
    /// Whether the next get probes, made again whenever what it rests on changes.
    probe: bool,
}

/// A clock reading for a sample: the reading after a first, so the clock's own cost, the
/// interval between the two, is taken out of the sample.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sample {
    first: Instant,
    second: Instant,
}

impl Sample {
    fn start() -> Self {
        let first = Instant::now();
        Self {
            first,
            second: Instant::now(),
        }
    }

    /// The nanoseconds since the sample started, less the clock's own cost.
    fn ns(self) -> u64 {
        let clock = self.second.saturating_duration_since(self.first);
        let spent = self.second.elapsed().saturating_sub(clock);
        u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Whether `count`, counted from one, is on the doubling schedule: one, two, four, ...
fn doubling(count: u64) -> bool {
    count.is_power_of_two()
}

impl KeptPays {
    /// A get reached the kept memtables, as they are in `epoch`: whether it probes them, and the
    /// sample to time the probe with when one is due. A new epoch forgets what the last measured
    /// and probes until it has measured again.
    pub(crate) fn reach(&mut self, epoch: (u32, Option<u32>, usize)) -> (bool, Option<Sample>) {
        if epoch != self.epoch {
            *self = Self {
                epoch,
                probe: true,
                ..Self::default()
            };
        }
        self.reached = self.reached.saturating_add(1);
        // The bound grows with ln t, which the decision reads as t's binary logarithm times ln 2:
        // it changes only where t reaches a power of two, so a skip is weighed again there.
        if !self.probe && doubling(self.reached) {
            self.decide();
        }
        if !self.probe {
            return (false, None);
        }
        let due = doubling(self.probes.saturating_add(1));
        (true, due.then(Sample::start))
    }

    /// A probe's answer: whether the kept memtables answered it, and its sample when timed.
    pub(crate) fn probed(&mut self, answered: bool, sample: Option<Sample>) {
        self.probes = self.probes.saturating_add(1);
        if answered {
            self.answered = self.answered.saturating_add(1);
        }
        if let Some(s) = sample {
            self.probe_samples = self.probe_samples.saturating_add(1);
            self.probe_ns = self.probe_ns.saturating_add(s.ns());
        }
        self.decide();
    }

    /// A get goes past the kept memtables, probed and not answered or not probed: the sample to
    /// time the rest of it with when one is due.
    pub(crate) fn pass(&mut self) -> Option<Sample> {
        self.passes = self.passes.saturating_add(1);
        doubling(self.passes).then(Sample::start)
    }

    /// The rest of a get past the kept memtables, timed.
    pub(crate) fn passed(&mut self, sample: Sample) {
        self.pass_samples = self.pass_samples.saturating_add(1);
        self.pass_ns = self.pass_ns.saturating_add(sample.ns());
        self.decide();
    }

    /// Probes while nothing is measured; then when the measured share pays, `p̂ · s ≥ c`, or its
    /// UCB1 bound does, `(p* − p̂)² ≤ 2 ln t / n` with `p* = c / s`, in integers:
    /// `(c·n − a·s)² ≤ 2 ln t · s² · n`, the means `c` and `s` in nanoseconds.
    fn decide(&mut self) {
        let (Some(c), Some(s)) = (
            self.probe_ns.checked_div(self.probe_samples),
            self.pass_ns.checked_div(self.pass_samples),
        ) else {
            self.probe = true;
            return;
        };
        let (n, a, t) = (
            u128::from(self.probes),
            u128::from(self.answered),
            self.reached,
        );
        let (c, s) = (u128::from(c), u128::from(s));
        let (cost, saving) = (c.saturating_mul(n), a.saturating_mul(s));
        if n == 0 || saving >= cost {
            self.probe = true;
            return;
        }
        let gap = cost.saturating_sub(saving);
        // 2 ln t as a ratio of integers: ln t = log2 t · ln 2, and ln 2 is
        // `LN_2_BILLIONTHS / BILLION`, so both sides are scaled by `BILLION`.
        let log2 = u128::from(t.max(1).ilog2());
        let bound = log2
            .saturating_mul(LN_2_BILLIONTHS.saturating_mul(2))
            .saturating_mul(s.saturating_mul(s))
            .saturating_mul(n);
        self.probe = gap.saturating_mul(gap).saturating_mul(BILLION) <= bound;
    }

    /// Whether the next get that reaches the kept memtables probes them.
    #[cfg(test)]
    fn probes_next(&self) -> bool {
        self.probe
    }
}

/// Format: ln 2 in billionths, 0.693147180 (the natural logarithm of two, rounded to nine places):
/// a binary logarithm times it over [`BILLION`] is the natural one.
const LN_2_BILLIONTHS: u128 = 693_147_180;

/// Format: a billion, the denominator [`LN_2_BILLIONTHS`] is written over.
const BILLION: u128 = 1_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(p: &mut KeptPays, probe_ns: u64, pass_ns: u64) {
        p.probe_samples = 1;
        p.probe_ns = probe_ns;
        p.pass_samples = 1;
        p.pass_ns = pass_ns;
    }

    /// With nothing measured a get probes, as before any rule.
    #[test]
    fn a_new_epoch_probes() {
        let mut p = KeptPays::default();
        assert!(p.reach((1, Some(0), 0)).0);
    }

    /// Answers often enough to pay keep every get probing: a share of 0.6 against a probe a
    /// fifth of a pass.
    #[test]
    fn a_paying_share_keeps_probing() {
        let mut p = KeptPays::default();
        p.reach((1, Some(0), 0));
        measured(&mut p, 100, 500);
        for k in 0..10_000u32 {
            let (probe, _) = p.reach((1, Some(0), 0));
            assert!(probe, "get {k} passed the kept memtables by");
            p.probed(k % 5 < 3, None);
        }
    }

    /// A share of 0.05 against a probe a fifth of a pass does not pay: the gets stop probing
    /// but for UCB1's logarithmic share, and probe again once the kept memtables change.
    #[test]
    fn a_share_that_does_not_pay_is_probed_a_logarithmic_number_of_times() {
        let mut p = KeptPays::default();
        let epoch = (1, Some(0), 0);
        p.reach(epoch);
        measured(&mut p, 100, 500);
        // Three quarters of the way from one power of two to the next: the probes the bound's last
        // step called for are made, and the next step is not yet due.
        let gets = 3u64 << 18;
        let mut probes = 0u64;
        for k in 0..gets {
            if p.reach(epoch).0 {
                probes += 1;
                p.probed(k % 20 == 0, None);
            }
        }
        // UCB1 probes about 2 ln t / (p* − p)² times: 2 ln 2^19 / 0.15² ≈ 1,171.
        assert!(probes < gets / 100, "{probes} probes of {gets} gets");
        assert!(probes > 100, "{probes} probes: none explored");
        assert!(!p.probes_next());
        assert!(p.reach((2, Some(0), 0)).0, "a new epoch probes");
    }

    /// A share that rises past what pays turns the gets back to probing.
    #[test]
    fn a_share_that_rises_is_found() {
        let mut p = KeptPays::default();
        let epoch = (1, Some(0), 0);
        p.reach(epoch);
        measured(&mut p, 100, 500);
        for k in 0..100_000u64 {
            if p.reach(epoch).0 {
                p.probed(k % 20 == 0, None);
            }
        }
        assert!(!p.probes_next());
        let mut probed_late = 0u64;
        for k in 0..1_000_000u64 {
            if p.reach(epoch).0 {
                p.probed(k % 10 < 6, None);
                probed_late += 1;
            }
        }
        assert!(p.probes_next(), "the risen share was not found");
        assert!(probed_late > 100_000, "{probed_late}");
    }

    /// Samples come on the doubling schedule, and a sample's clock cost is taken out.
    #[test]
    fn samples_come_at_doubling_counts() {
        let mut p = KeptPays::default();
        let epoch = (1, Some(0), 0);
        let mut timed = Vec::new();
        for k in 1..=64u64 {
            let (probe, sample) = p.reach(epoch);
            assert!(probe);
            if sample.is_some() {
                timed.push(k);
            }
            p.probed(true, sample);
        }
        assert_eq!(timed, vec![1, 2, 4, 8, 16, 32, 64]);
        let passes: Vec<u64> = (1..=16u64).filter(|_| p.pass().is_some()).collect();
        assert_eq!(passes.len(), 5);
    }
}
