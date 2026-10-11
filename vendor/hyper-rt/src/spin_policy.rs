//! Whether an idle shard spins before it parks, decided by what its spins have been measured to save and to
//! waste (docs/runtime.md §3.4; mantle `docs/design/event-loop.md` D9).
//!
//! **What spinning buys.** A shard that spins and sees its next work after `t` has spent `t`; had it parked
//! at once it would have spent the cost of blocking `C` (its park and its waker's kick, measured:
//! `crate::park_cost`). So a spin that sees its work saves `C − t`, and one that runs out its budget `τ` and
//! parks anyway wastes `τ`. Spinning pays while the saving of the spins exceeds their waste: the expected
//! cost of a two-phase wait set against blocking at once, `∫₀^τ t dP + (τ + C)(1 − P(τ))` against `C` [A:
//! Karlin, Li, Manasse, Owicki, SOSP 1991; A: Lim and Agarwal, "Waiting algorithms for synchronization in
//! large-scale multiprocessors", ACM TOCS 11(3), 1993, eq. (1)], evaluated on the waits this shard actually
//! met rather than on an assumed distribution.
//!
//! **When it cannot pay.** The bound that makes a spin of `C` 2-competitive assumes the work arrives in its own
//! time while the shard spins. That holds only while the thread that sends it can run beside the spinner. Lim
//! and Agarwal call a program *matched* when its runnable threads never outnumber a processor's contexts; in an
//! unmatched one "polling admits the possibility of deadlock if non-preemptive scheduling is used [...]
//! polling could still suffer from poor performance", and their analysis of two-phase waiting "assumes that we
//! can always find a runnable thread to replace a blocked thread" [A: Lim and Agarwal, MIT-LCS-TR-498, §3 p.
//! 7, §4 p. 9]. Boguslavsky et al., modelling three threads on two processors, find immediate blocking optimal
//! over a region of the parameters, and "the best choice for all other parameters" as a context switch's cost
//! falls to zero [A: Boguslavsky, Harzallah, Kreinen, Sevcik, Vainshtein, "Optimal strategies for spinning
//! and blocking", Toronto CSRI-278, 1993, pp. 13-14]. Where the process can run only one thread at a time, the
//! sender waits for the very CPU the spin holds, so no spin can see its work before the scheduler takes the
//! CPU from it: the saving is zero, and the shard never spins. Where it can run more, whether the sender does
//! run beside the spin depends on what else the host runs, which no reading of the host's load answers for one
//! shard; the outcome of the spins themselves does.
//!
//! **Windows, not single spins.** The outcome of one shard's spins depends on its peers': two shards bouncing
//! a message see each other's work early only while both spin, so a shard that judged each spin as it came
//! stopped at the first misses, while its peer still parked, and the peer's spins then missed too — on this
//! Mac a ping-pong fell from 0.96 million round trips a second to 6,443 (mantle note 42 §14). So the shard
//! judges a window of [`MIN_SAMPLES`] consecutive spins: it spins on every wait until the window is full, and
//! goes on while the window's saving exceeds its waste. After a window that did not pay it rests that many
//! waits and then tries a new window, the rest doubling after each window that fails (exponential backoff, for
//! a load it cannot know, as for "a network of unknown topology and with an unknown, unknowable and constantly
//! changing population of competing conversations, only one scheme has any hope of working — exponential
//! backoff" [A: Jacobson and Karels, "Congestion Avoidance and Control", 1988, §2 p. 7]) up to `MIN_SAMPLES²`
//! waits, so a shard whose spins never pay spends at most one wait in `MIN_SAMPLES + 1` spinning. Peers that
//! wait in step (a ping-pong's two shards take turns) start their windows together, and so recover together.

use crate::machine::bench::MIN_SAMPLES;

/// The spin decision of one shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SpinPolicy {
    /// Whether the thread that ends a spin can run beside it: more than one of the process's threads can run
    /// at once.
    parallel: bool,
    /// The window of spins being judged: every wait spins until it holds [`MIN_SAMPLES`] spins.
    window: Window,
    /// Waits left without a spin, after a window that did not pay; 0 while spinning.
    resting: u64,
    /// The next rest's length in waits: [`MIN_SAMPLES`] after a window that paid, doubling after each one that
    /// did not, up to `MIN_SAMPLES²`.
    rest: u64,
}

/// The spins of one window: how many, and what they saved and wasted in all.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Window {
    spins: u64,
    saved_ns: u128,
    wasted_ns: u128,
}

impl SpinPolicy {
    /// A policy for a shard whose process can run `cpus_at_once` threads at once: it starts in a window.
    pub(crate) fn new(cpus_at_once: usize) -> SpinPolicy {
        SpinPolicy {
            parallel: cpus_at_once > 1,
            window: Window::default(),
            resting: 0,
            rest: u64::try_from(MIN_SAMPLES).unwrap_or(u64::MAX),
        }
    }

    /// Whether a spin can ever see its work: no where one thread runs at a time.
    pub(crate) const fn can_spin(&self) -> bool {
        self.parallel
    }

    /// Whether the wait about to start spins: never where no spin can see its work; not while resting after a
    /// window that did not pay; otherwise yes.
    pub(crate) fn should_spin(&mut self) -> bool {
        if !self.parallel {
            return false;
        }
        if self.resting > 0 {
            self.resting = self.resting.saturating_sub(1);
            return false;
        }
        true
    }

    /// A spin that saw its work after `spun_ns`, where parking would have cost `blocking_ns`.
    pub(crate) fn note_seen(&mut self, spun_ns: u64, blocking_ns: u64) {
        self.record(blocking_ns.saturating_sub(spun_ns), 0);
    }

    /// A spin that ran out `spun_ns` without its work, and then parked.
    pub(crate) fn note_missed(&mut self, spun_ns: u64) {
        self.record(0, spun_ns);
    }

    /// Folds one spin in; a full window is judged: spinning goes on while the window's saving exceeds its
    /// waste, and otherwise rests for `rest` waits, `rest` doubling up to its cap.
    fn record(&mut self, saved_ns: u64, wasted_ns: u64) {
        self.window.spins = self.window.spins.saturating_add(1);
        self.window.saved_ns = self.window.saved_ns.saturating_add(u128::from(saved_ns));
        self.window.wasted_ns = self.window.wasted_ns.saturating_add(u128::from(wasted_ns));
        let full = u64::try_from(MIN_SAMPLES).unwrap_or(u64::MAX);
        if self.window.spins < full {
            return;
        }
        let paid = self.window.saved_ns > self.window.wasted_ns;
        self.window = Window::default();
        if paid {
            self.rest = full;
        } else {
            self.resting = self.rest;
            self.rest = self.rest.saturating_mul(2).min(full.saturating_mul(full));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full() -> u64 {
        u64::try_from(MIN_SAMPLES).unwrap()
    }

    /// The waits that spin out of `waits`, each spin noted by `outcome`, in order.
    fn spins(
        policy: &mut SpinPolicy,
        waits: u64,
        mut outcome: impl FnMut(&mut SpinPolicy),
    ) -> Vec<u64> {
        (1..=waits)
            .filter(|_| {
                let spun = policy.should_spin();
                if spun {
                    outcome(policy);
                }
                spun
            })
            .collect()
    }

    #[test]
    fn a_shard_whose_process_runs_one_thread_at_a_time_never_spins() {
        let mut policy = SpinPolicy::new(1);
        assert!(!policy.can_spin());
        assert!(spins(&mut policy, 10_000, |p| p.note_seen(0, 5_000)).is_empty());
    }

    /// A window whose spins see their work goes on spinning, window after window.
    #[test]
    fn spins_that_pay_keep_it_spinning() {
        let mut policy = SpinPolicy::new(2);
        assert_eq!(
            spins(&mut policy, 1_000, |p| p.note_seen(200, 5_000)).len(),
            1_000
        );
    }

    /// A window of spins that miss rests for a window's length, then tries a new window; each window that
    /// misses again doubles the rest, up to `MIN_SAMPLES²` waits.
    #[test]
    fn a_window_that_does_not_pay_rests_and_the_rests_double() {
        let mut policy = SpinPolicy::new(2);
        let mut rests = Vec::new();
        let mut run = 0u64;
        let mut quiet = 0u64;
        for _ in 0..1_000_000 {
            if policy.should_spin() {
                if quiet > 0 {
                    rests.push(quiet);
                    quiet = 0;
                }
                run += 1;
                policy.note_missed(5_000);
            } else {
                quiet += 1;
            }
        }
        assert_eq!(run % full(), 0, "windows are whole");
        let cap = full() * full();
        let mut expected = Vec::new();
        let mut rest = full();
        while expected.len() < rests.len() {
            expected.push(rest);
            rest = (rest * 2).min(cap);
        }
        assert_eq!(rests, expected);
    }

    /// A window that pays after rests ends the backoff: the next window that fails rests a window's length.
    #[test]
    fn a_window_that_pays_resets_the_rest() {
        let mut policy = SpinPolicy::new(2);
        for _ in 0..full() {
            assert!(policy.should_spin());
            policy.note_missed(5_000);
        }
        assert_eq!(
            spins(&mut policy, full(), |_| {}).len(),
            0,
            "a window's rest"
        );
        for _ in 0..full() {
            assert!(policy.should_spin());
            policy.note_seen(0, 5_000);
        }
        for _ in 0..full() {
            assert!(policy.should_spin());
            policy.note_missed(5_000);
        }
        assert_eq!(
            spins(&mut policy, full(), |_| {}).len(),
            0,
            "back to a window's rest"
        );
        assert!(policy.should_spin());
    }
}
