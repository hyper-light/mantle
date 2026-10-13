//! The schedule tests' harnesses and their swarm configurations (`docs/sim.md` §3.8, step S-6): each
//! group kind's schedule length, rules and mix as the schedule tests run it, and a seed's own draw
//! within the harness's ranges (Groce et al.), which the schedule tests run by default and the
//! strategies' campaigns run past them (`tests/strategies.rs`).
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    unreachable_pub,
    missing_docs
)]

use hyper_check::strategy::swarm::Swarm;

use hyper_raft::Mutant;

use super::judge::{Core, Driver, Judged, drive};
use super::{Cluster, Lagged, Mix, New, Settings};

/// A group kind, with its schedule's length and mix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Harness {
    /// `tests/group.rs`'s: five members, three voters.
    Group,
    /// `tests/pipeline.rs`'s: members whose writes lag, two out at once.
    Pipelined,
    /// `tests/fast.rs`'s: five voters with the fast track.
    Fast,
    /// The rules seed 9843's defect was found under (`tests/fast.rs`).
    FastFirstRule,
}

impl Harness {
    pub fn steps(self) -> u64 {
        match self {
            Self::Pipelined => 2_000,
            _ => 4_000,
        }
    }

    pub fn settings(self) -> Settings {
        let settings = match self {
            Self::Group => Settings::focal(),
            Self::Pipelined => Settings {
                depth: 2,
                in_place: true,
                ..Settings::focal()
            },
            Self::Fast => Settings::fast(),
            Self::FastFirstRule => Settings {
                round_each: true,
                max_inflight_bytes: u64::MAX,
                bare_answers: true,
                refuse_ahead: true,
                ..Settings::fast()
            },
        };
        Settings {
            judged: true,
            ..settings
        }
    }

    pub fn mix(self) -> Mix {
        match self {
            Self::Group => Mix {
                leader_leaves: true,
                bursts: true,
                windows: true,
                ..Mix::everything()
            },
            Self::Pipelined => Mix {
                leader_leaves: true,
                bursts: true,
                windows: true,
                lag: 35,
                ..Mix::everything()
            },
            Self::Fast | Self::FastFirstRule => Mix {
                leader_leaves: true,
                fast: 60,
                ..Mix::everything()
            },
        }
    }

    pub fn fast(self) -> bool {
        matches!(self, Self::Fast | Self::FastFirstRule)
    }

    /// The group (members, voters) of the harness.
    pub fn members(self) -> (u64, Vec<u64>) {
        if self.fast() {
            (5, vec![1, 2, 3, 4, 5])
        } else {
            (5, vec![1, 2, 3])
        }
    }
}

/// A run's group, rules and mix: the harness's own, or a swarm's draw over its ranges.
#[derive(Clone, Debug)]
pub struct Configuration {
    pub count: u64,
    pub voters: Vec<u64>,
    pub settings: Settings,
    pub mix: Mix,
}

impl Configuration {
    pub fn of(harness: Harness) -> Self {
        let (count, voters) = harness.members();
        Self {
            count,
            voters,
            settings: harness.settings(),
            mix: harness.mix(),
        }
    }

    /// A swarm's configuration for `seed` within the harness's ranges (Groce et al.; `docs/sim.md`
    /// §3.8): each rule and each kind of step on or off with even odds, each rate uniform over the
    /// range the schedule tests run (losses and repeats to three in four, `tests/check.rs`'s
    /// hostile networks; the fast track's share of proposals to all; persistence steps to the
    /// pipelined schedules' bound), the windows and message sizes from the values the schedule
    /// tests use, and the group from the sizes they run. The group kind (fast, pipelined) is the
    /// harness's: a defect of the fast track is not caught by a group without it.
    pub fn swarm(harness: Harness, seed: u64) -> Self {
        let mut swarm = Swarm::of(seed);
        let base = harness.settings();
        let pre_vote = swarm.feature();
        let check_quorum = swarm.feature();
        let refuse_ahead = swarm.feature();
        let round_each = swarm.feature();
        let bare_answers = swarm.feature();
        // raft-rs's precedence is the differential tests' alone, no consumer's: a campaign runs
        // what a consumer can (`docs/raft.md` §3.3, swarm group seed 9,657). Its feature is still
        // drawn, so every draw after it, and every seed's schedule, is as it was.
        let _raft_rs_precedence = swarm.feature();
        let mut settings = Settings {
            pre_vote,
            check_quorum,
            refuse_ahead,
            round_each,
            bare_answers,
            by_length: false,
            max_inflight_bytes: swarm.one(&[256, 4096, u64::MAX]).unwrap(),
            max_size_per_msg: swarm.one(&[1, 64, base.max_size_per_msg]).unwrap(),
            ..base
        };
        if harness == Harness::Pipelined {
            settings.apply_unpersisted = swarm.feature();
        }
        let mix = Mix {
            changes: swarm.feature(),
            leader_leaves: swarm.feature(),
            restarts: swarm.feature(),
            compaction: swarm.feature(),
            partitions: swarm.feature(),
            priorities: swarm.feature(),
            windows: swarm.feature(),
            bursts: swarm.feature(),
            lose: swarm.within(0, 75),
            repeat: swarm.within(0, 75),
            fast: if harness.fast() {
                swarm.within(0, 100)
            } else {
                0
            },
            lag: if harness == Harness::Pipelined {
                swarm.within(1, 75)
            } else {
                0
            },
            ..harness.mix()
        };
        let (count, voters) = if harness.fast() {
            let count = swarm.within(3, 5);
            (count, (1..=count).collect())
        } else {
            let count = swarm.one(&[3, 5]).unwrap();
            let voters = if count == 5 && swarm.feature() { 5 } else { 3 };
            (count, (1..=voters).collect())
        };
        Self {
            count,
            voters,
            settings,
            mix,
        }
    }

    pub fn group<R: Core>(&self, seed: u64) -> Cluster<R> {
        let mut group = Cluster::new(self.count, &self.voters, self.settings, seed);
        group.stop_who_left = !self.settings.fast;
        group.liveness_bound = Some(LIVENESS);
        group
    }
}

/// The most operations a run's liveness phase may act: four times the most any of the swarm's
/// 15,000 seeds with no defect took (9,039, a group harness's,
/// `the_swarm_with_no_defect_keeps_every_oracle_and_the_model`, measured 2026-10-05), the rule
/// the trace's reservation is stated by (`docs/sim.md` §13).
/// Past it the run is reported unconverged (§4.2).
pub const LIVENESS: u64 = 4 * 9_039;

/// A group of `configuration` at `seed`, driven by `driver` for the harness's steps and judged.
pub fn run<D>(
    harness: Harness,
    configuration: &Configuration,
    seed: u64,
    mutant: Option<Mutant>,
    driver: &mut D,
) -> Judged
where
    D: Driver<New> + Driver<Lagged>,
{
    let steps = harness.steps();
    if harness == Harness::Pipelined {
        drive(
            configuration.group::<Lagged>(seed),
            steps,
            &configuration.mix,
            mutant,
            driver,
        )
    } else {
        drive(
            configuration.group::<New>(seed),
            steps,
            &configuration.mix,
            mutant,
            driver,
        )
    }
}
