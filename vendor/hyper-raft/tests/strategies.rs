//! The strategies of `docs/sim.md` §4.5 (step S-5) run on this core, each judged by every oracle of
//! §4.1 after every operation (`support::judge`):
//!
//! - **Random walk under swarm configurations**: each seed draws its own group, rules and mix,
//!   every feature on or off and every rate over its range (Groce et al.).
//! - **PCT over members**: priorities over the members, change points over the deliveries at
//!   which messages for two or more members race, and the confidence a campaign reaches per depth
//!   (Burckhardt et al., Theorem 9).
//! - **Coverage over the TLA+ abstraction, with the conformance check**: every step's members read
//!   as `FastTrack.tla`'s variables and held to the relations every action of the model keeps
//!   (`hyper_check::conform`); the abstract states' shapes are the coverage, and runs that reach
//!   new ones are mutated (Gulcan et al.).
//! - **Implementation-level exhaustive search at a tiny scope**: every sequence of a few rounds
//!   of three members, each round a leader, the partition its election crosses, the partition its
//!   replication crosses, whether it sends its new term's entries, and whether it takes a proposal
//!   (Twins §4.2), reduced by the state each round reaches.
//! - **Shrinking**: a failing run's decision tape shortened while it fails the same way.
//!
//! For each planted defect of §4.6 each strategy's runs to catch it are measured by the campaigns
//! here (`#[ignore]`, run in release), recorded in `docs/sim.md` §15, and each catch is then held
//! by a directed test at its seed, with the same seed clean without the defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    unreachable_pub,
    missing_docs
)]
mod support;

use hyper_check::conform::{Abstract, Member, conforms};
use hyper_check::explore::fingerprint;
use hyper_check::explore::rounds::{Played, System, rounds};
use hyper_check::oracle::Violation;
use hyper_check::search::Budget;
use hyper_check::strategy::guided::Guided;
use hyper_check::strategy::pct::{Pct, confidence};
use hyper_check::strategy::swarm::Swarm;
use hyper_check::strategy::tape::{Bounds, Player, Tape, shrink};
use hyper_measure::alloc::Counting;
use hyper_measure::cost::{Costs, measure};
use hyper_measure::usage;
use hyper_raft::Mutant;
use hyper_raft::proto::{Entry, MessageType};
use support::judge::{Core, Driver, Judge, Judged, Seed, drive, route};
use support::{Cluster, Draws, Lagged, Mix, New, Op, Replica, Seeded, Settings};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

// ---------------------------------------------------------------------------------------------
// The harnesses the planted defects are caught in: `tests/check.rs`'s, each the test's ranges.

/// A group kind, with its schedule's length and mix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Harness {
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
    fn steps(self) -> u64 {
        match self {
            Self::Pipelined => 2_000,
            _ => 4_000,
        }
    }

    fn settings(self) -> Settings {
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

    fn mix(self) -> Mix {
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

    fn fast(self) -> bool {
        matches!(self, Self::Fast | Self::FastFirstRule)
    }

    /// The group (members, voters) of the harness.
    fn members(self) -> (u64, Vec<u64>) {
        if self.fast() {
            (5, vec![1, 2, 3, 4, 5])
        } else {
            (5, vec![1, 2, 3])
        }
    }
}

/// A run's group, rules and mix: the harness's own, or a swarm's draw over its ranges.
#[derive(Clone, Debug)]
struct Configuration {
    count: u64,
    voters: Vec<u64>,
    settings: Settings,
    mix: Mix,
}

impl Configuration {
    fn of(harness: Harness) -> Self {
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
    fn swarm(harness: Harness, seed: u64) -> Self {
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

    fn group<R: Core>(&self, seed: u64) -> Cluster<R> {
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
const LIVENESS: u64 = 4 * 9_039;

/// A planted defect, the harness its catch is sought in, and what a random walk from seed 0 needed
/// to catch it there (`docs/sim.md` §14.6): its runs, or `None` where 2,096 runs never did.
#[derive(Clone, Copy, Debug)]
struct Case {
    mutant: Mutant,
    harness: Harness,
    random: Option<u64>,
}

const CASES: [Case; 5] = [
    Case {
        mutant: Mutant::OlderTermCommit,
        harness: Harness::Group,
        random: None,
    },
    Case {
        mutant: Mutant::ReadBeforeFirstCommit,
        harness: Harness::Group,
        random: Some(4),
    },
    Case {
        mutant: Mutant::VoteBeforeDurable,
        harness: Harness::Pipelined,
        random: Some(1),
    },
    Case {
        mutant: Mutant::FastBesideAnyTerm,
        harness: Harness::FastFirstRule,
        random: Some(102_775),
    },
    Case {
        mutant: Mutant::FastAnyConfiguration,
        harness: Harness::Fast,
        random: Some(8_868),
    },
];

/// A group of `configuration` at `seed`, driven by `driver` for the harness's steps and judged.
fn run<D>(
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

// ---------------------------------------------------------------------------------------------
// PCT over members.

/// The racing deliveries of a run, the `k` of Theorem 9 a PCT campaign is stated over: the most
/// any of the harness's 96 default random-walk seeds took (`racing_deliveries_are_counted`,
/// measured 2026-10-04 at this commit; a run that takes more is counted, and the confidence is
/// stated for the runs within it).
fn racing(harness: Harness) -> u64 {
    match harness {
        Harness::Group => 1_939,
        Harness::Pipelined => 377,
        Harness::Fast => 2_051,
        Harness::FastFirstRule => 2_035,
    }
}

/// The seed's schedule, every delivery chosen by PCT over the members the messages are for.
struct Prioritized {
    draws: Seeded,
    pct: Pct,
}

impl Prioritized {
    fn new(seed: u64, members: usize, depth: u32, steps: u64) -> Self {
        Self {
            draws: Seeded(seed),
            pct: Pct::of(seed, members, depth, steps).unwrap(),
        }
    }
}

impl<R: Replica> Driver<R> for Prioritized {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        let op = group.choose(&mut self.draws, mix);
        Some(match op {
            Op::Deliver { keep, lose, .. } => {
                let enabled: Vec<usize> = group.net.iter().map(|m| (m.to - 1) as usize).collect();
                Op::Deliver {
                    at: self.pct.pick(&enabled).unwrap_or(0),
                    keep,
                    lose,
                }
            }
            other => other,
        })
    }
}

/// Counts the deliveries at which messages for two or more members race, of a seed's schedule.
struct Racing {
    draws: Seeded,
    races: u64,
}

impl<R: Replica> Driver<R> for Racing {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        let op = group.choose(&mut self.draws, mix);
        if matches!(op, Op::Deliver { .. })
            && let Some(first) = group.net.first()
            && group.net.iter().any(|m| m.to != first.to)
        {
            self.races += 1;
        }
        Some(op)
    }
}

/// `k` for each harness: the most racing deliveries any default seed's run took, measured.
#[test]
#[ignore = "a measurement: the racing deliveries `racing` states and the tape's words, in release"]
fn racing_deliveries_are_counted() {
    let mut over = Vec::new();
    for harness in [
        Harness::Group,
        Harness::Pipelined,
        Harness::Fast,
        Harness::FastFirstRule,
    ] {
        let configuration = Configuration::of(harness);
        let mut most = 0;
        for seed in 0..96 {
            let mut driver = Racing {
                draws: Seeded(seed),
                races: 0,
            };
            run(harness, &configuration, seed, None, &mut driver);
            most = most.max(driver.races);
        }
        let mut words = 0;
        let mut liveness = 0;
        for seed in 0..96 {
            let mut taped = Taped {
                player: Player::record(seed, TAPE),
            };
            let judged = run(harness, &configuration, seed, None, &mut taped);
            words = words.max(taped.player.finish().words());
            liveness = liveness.max(judged.steps.saturating_sub(harness.steps()));
        }
        println!("{harness:?}: at most {liveness} operations a liveness phase");
        println!(
            "{harness:?}: at most {most} racing deliveries a run, stated {}; at most {words} words of tape a run",
            racing(harness)
        );
        if most > racing(harness) {
            over.push((harness, most));
        }
    }
    assert!(over.is_empty(), "{over:?}");
}

// ---------------------------------------------------------------------------------------------
// The decision tape, conformance and coverage.

/// The kind of a step, as the tape's mutations respect it: a delivery, a crash, anything else.
fn kind(op: &Op) -> u8 {
    match op {
        Op::Deliver { .. } => 0,
        Op::Restart(_) | Op::Corrupt(..) => 1,
        _ => 2,
    }
}

/// The tape's bound: a run's steps, the harness's 4,000 at most and a step for the group's seed
/// (the coverage campaign's tapes carry it), and four times the most words any of the harnesses'
/// 96 default seeds drew (25,358, the fast schedules', `racing_deliveries_are_counted`, measured
/// 2026-10-04) and the seed's word.
const TAPE: Bounds = Bounds {
    steps: 4_001,
    words: 4 * 25_358 + 1,
};

/// A schedule drawn through a decision tape: recorded from a seed, or played back.
struct Taped {
    player: Player,
}

impl Draws for Taped {
    fn next(&mut self) -> u64 {
        self.player
            .word()
            .expect("the tape's bound holds a schedule's draws")
    }
}

impl<R: Replica> Driver<R> for Taped {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        if self.player.left() == Some(0) {
            return None;
        }
        let op = group.choose(self, mix);
        self.player
            .end_step(kind(&op))
            .expect("the tape's bound holds a schedule");
        Some(op)
    }
}

/// What an entry states, for the abstraction: FNV-1a over its kind and data.
fn stated(entry: &Entry) -> u64 {
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    for byte in std::iter::once(entry.entry_type as u8).chain(entry.data.iter().copied()) {
        digest = (digest ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    digest
}

/// The group read as `FastTrack.tla`'s variables: each member's durable term, vote, log and
/// commit (what a restart opens on, as the model's members hold only what is durable), what it
/// holds beside its log, whether it runs, and whether it leads in a term its device holds.
fn abstraction<R: Core>(group: &Cluster<R>) -> Abstract {
    Abstract {
        members: group
            .ids()
            .into_iter()
            .map(|id| {
                let disk = group.disk(id);
                let node = group.peek(id);
                Member {
                    up: node.is_some(),
                    term: disk.hard_state.term,
                    vote: disk.hard_state.vote,
                    // A leader whose term is not yet durable sends nothing (I1) and leads in the
                    // model once the write that carries its election is durable
                    // (`docs/models/README.md`, "Readies ahead of their persistence").
                    leader: node.is_some_and(|node| {
                        node.view().role == 2 && node.view().term == disk.hard_state.term
                    }),
                    start: disk.snapshot_index(),
                    log: disk.entries.iter().map(|e| (e.term, stated(e))).collect(),
                    held: disk.proposals.iter().map(|e| e.index).collect(),
                    commit: disk.hard_state.commit,
                    classic: disk.released,
                }
            })
            .collect(),
    }
}

/// A driver whose every step is held to the model, and whose abstract states are counted as
/// coverage when a campaign keeps them.
struct Conformed<'g, D> {
    inner: D,
    previous: Option<Abstract>,
    lost: Vec<usize>,
    points: Option<&'g mut Guided>,
    new: u64,
    steps: u64,
}

impl<'g, D> Conformed<'g, D> {
    fn new(inner: D, points: Option<&'g mut Guided>) -> Self {
        Self {
            inner,
            previous: None,
            lost: Vec::new(),
            points,
            new: 0,
            steps: 0,
        }
    }
}

impl<R: Core, D: Driver<R>> Driver<R> for Conformed<'_, D> {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        if self.previous.is_none() {
            self.previous = Some(abstraction(group));
        }
        self.inner.choose(group, mix)
    }

    fn acted(&mut self, group: &Cluster<R>, op: &Op) -> Option<String> {
        if let Some(departure) = self.inner.acted(group, op) {
            return Some(departure);
        }
        self.steps += 1;
        if let Op::Corrupt(member, _) = op {
            self.lost.push((member - 1) as usize);
        }
        let now = abstraction(group);
        if let Some(before) = &self.previous
            && let Err(departure) = conforms(before, &now, &self.lost)
        {
            return Some(format!("{departure} at step {} ({op:?})", self.steps));
        }
        self.lost.clear();
        if let Some(points) = self.points.as_deref_mut()
            && points
                .reached(now.point())
                .expect("the coverage set's budget")
        {
            self.new += 1;
        }
        self.previous = Some(now);
        None
    }
}

/// Every step of the schedule tests' default seeds is a step of the model (`docs/sim.md` §4.5,
/// "The abstraction is also checked on every step"): the group, fast and pipelined schedules, each
/// seed's members read as `FastTrack.tla`'s variables after every operation, the liveness phase's
/// included, and held to the relations every action of the model keeps.
#[test]
fn every_step_of_the_default_schedules_is_a_step_of_the_model() {
    for (harness, seeds) in [
        (Harness::Group, 24),
        (Harness::Fast, 24),
        (Harness::Pipelined, 12),
    ] {
        let configuration = Configuration {
            settings: Settings {
                judged: false,
                ..harness.settings()
            },
            ..Configuration::of(harness)
        };
        let mut steps = 0;
        for seed in 0..seeds {
            let mut driver = Conformed::new(Seed(Seeded(seed)), None);
            let judged = run(harness, &configuration, seed, None, &mut driver);
            assert_eq!(judged.departure, None, "{harness:?}, seed {seed}");
            assert_eq!(judged.violation, None, "{harness:?}, seed {seed}");
            steps += driver.steps;
        }
        println!("{harness:?}: {seeds} seeds, {steps} steps, each a step of the model");
    }
}

/// Swarm group seed 9,657 drew raft-rs's precedence of length. Its schedule ends with member 1
/// holding the longest log, of an older last term (18), and the highest priority; member 3 a
/// shorter log of term 27 whose last change enters the joint configuration {1,3} / {1,2,3}, by
/// which it counts (`docs/raft.md` §3.4); member 2 the shortest. Member 1 refuses both
/// candidates for priority, though it can never be elected itself, member 3 refuses member 2 for
/// its log, and with every member up the group elected no one within the liveness bound, terms
/// climbing (`docs/raft.md` §3.3). By the log's precedence, the only rule a consumer can set, the
/// same schedule elects.
#[test]
fn swarm_group_seed_9657_elects_by_the_logs_precedence() {
    const SEED: u64 = 9_657;
    let harness = Harness::Group;
    let configuration = Configuration::swarm(harness, SEED);
    assert!(!configuration.settings.by_length);
    let judged = run(harness, &configuration, SEED, None, &mut Seed(Seeded(SEED)));
    assert_eq!(judged.violation, None);
    assert!(judged.settled, "by the log's precedence the group elects");
    // The same schedule by raft-rs's rule: the cause, replayed.
    let by_length = Configuration {
        settings: Settings {
            by_length: true,
            ..configuration.settings
        },
        ..configuration
    };
    let judged = run(harness, &by_length, SEED, None, &mut Seed(Seeded(SEED)));
    assert!(
        !judged.settled,
        "by raft-rs's precedence the group elects no one"
    );
}

// ---------------------------------------------------------------------------------------------
// The campaigns: for each defect and strategy, the runs to its first catch.

/// What caught a defect: the run's index in its campaign (from one), the seed, and the violation.
#[derive(Debug)]
struct Catch {
    runs: u64,
    seed: u64,
    violation: Violation,
    /// The catching run's tape, where the strategy runs by tapes.
    tape: Option<Tape>,
}

/// A campaign's result, or the runs it took without a catch.
type Campaign = Result<Catch, u64>;

fn catch(judged: &Judged) -> Option<Violation> {
    judged.violation.clone()
}

fn swarm_campaign(case: Case, budget: u64) -> Campaign {
    for seed in 0..budget {
        let configuration = Configuration::swarm(case.harness, seed);
        let judged = run(
            case.harness,
            &configuration,
            seed,
            Some(case.mutant),
            &mut Seed(Seeded(seed)),
        );
        if let Some(violation) = catch(&judged) {
            return Ok(Catch {
                runs: seed + 1,
                seed,
                violation,
                tape: None,
            });
        }
    }
    Err(budget)
}

fn pct_campaign(case: Case, depth: u32, budget: u64) -> Campaign {
    let configuration = Configuration::of(case.harness);
    let members = configuration.count as usize;
    for seed in 0..budget {
        let mut driver = Prioritized::new(seed, members, depth, racing(case.harness));
        let judged = run(
            case.harness,
            &configuration,
            seed,
            Some(case.mutant),
            &mut driver,
        );
        if let Some(violation) = catch(&judged) {
            return Ok(Catch {
                runs: seed + 1,
                seed,
                violation,
                tape: None,
            });
        }
    }
    Err(budget)
}

/// The coverage campaign: fresh seeds while the corpus has no energy, mutations of its entries
/// while it has (Gulcan et al.'s Algorithm 1); each run held to the model, a departure reported
/// as the campaign's catch. Its corpus holds 64 tapes (the corpus a campaign of the budgets here
/// fills, measured: at most 41 entries held energy at once over the group campaign's first 2,096
/// runs), and its coverage set the search's ceiling.
fn guided_campaign(case: Case, budget: u64) -> (Campaign, u64, Option<String>) {
    let configuration = Configuration::of(case.harness);
    let mut guided = Guided::new(Budget::default(), 64, 0).unwrap();
    let mut fresh = 0u64;
    for runs in 1..=budget {
        let mut player = match guided.mutated() {
            Some(tape) => Player::replay(&tape, runs, TAPE),
            None => {
                fresh += 1;
                Player::record(fresh - 1, TAPE)
            }
        };
        // A tape's first step is the group's seed (its members' own randomness), so a mutation
        // may draw another group as it draws another schedule.
        let seed = player.word().unwrap();
        player.end_step(3).unwrap();
        let mut taped = Taped { player };
        let (judged, new) = {
            let mut driver = Conformed::new(&mut taped, Some(&mut guided));
            let judged = run(
                case.harness,
                &configuration,
                seed,
                Some(case.mutant),
                &mut driver,
            );
            let new = driver.new;
            (judged, new)
        };
        if let Some(departure) = judged.departure {
            return (Err(runs), guided.points(), Some(departure));
        }
        if let Some(violation) = catch(&judged) {
            let tape = Some(taped.player.finish());
            return (
                Ok(Catch {
                    runs,
                    seed,
                    violation,
                    tape,
                }),
                guided.points(),
                None,
            );
        }
        guided.finished(taped.player.finish(), new);
    }
    (Err(budget), guided.points(), None)
}

impl<R: Replica> Driver<R> for &mut Taped {
    fn choose(&mut self, group: &mut Cluster<R>, mix: &Mix) -> Option<Op> {
        <Taped as Driver<R>>::choose(self, group, mix)
    }
}

/// The budget of a campaign for a defect: the random walk's runs (§14.6), so a strategy that does
/// not catch it within them has not beaten the random walk on it (`docs/sim.md` §11 item 1), and
/// at least the 2,096 runs the random walk was given for the defect it never caught, so a defect
/// the random walk caught at its first seed is given room to be caught at all.
fn budget(case: Case) -> u64 {
    case.random.unwrap_or(0).max(2_096)
}

fn report(case: Case, strategy: &str, campaign: &Campaign) {
    match campaign {
        Ok(catch) => println!(
            "{:?}: {strategy}: caught at run {} (seed {}): {}",
            case.mutant, catch.runs, catch.seed, catch.violation
        ),
        Err(runs) => println!("{:?}: {strategy}: not caught in {runs} runs", case.mutant),
    }
}

/// A strategy of the campaigns.
#[derive(Clone, Copy, Debug)]
enum Strategy {
    Swarm,
    Pct(u32),
    Guided,
}

/// One campaign: `strategy`'s runs to catch `CASES[case]`, reported.
fn campaign(strategy: Strategy, case: usize) {
    let case = CASES[case];
    match strategy {
        Strategy::Swarm => report(case, "swarm", &swarm_campaign(case, budget(case))),
        Strategy::Pct(depth) => {
            let campaign = pct_campaign(case, depth, budget(case));
            report(case, &format!("PCT at depth {depth}"), &campaign);
            let members = Configuration::of(case.harness).count;
            let runs = campaign
                .as_ref()
                .map_or_else(|runs| *runs, |catch| catch.runs);
            println!(
                "  {runs} runs reach, for a defect of depth 1, 2, 3: {:.4}, {:.3e}, {:.3e}",
                confidence(runs, members, racing(case.harness), 1),
                confidence(runs, members, racing(case.harness), 2),
                confidence(runs, members, racing(case.harness), 3)
            );
        }
        Strategy::Guided => {
            let (campaign, points, departure) = guided_campaign(case, budget(case));
            report(case, "coverage-guided", &campaign);
            println!("  {points} abstract states covered");
            assert_eq!(departure, None, "{:?}", case.mutant);
        }
    }
}

/// The campaigns, one test each so they run apart: each strategy against each planted defect of
/// `CASES`, in its order (PCT at depths one and two: TaxDC's bugs are 92 % triggered by one
/// untimely event and over 90 % by one to three messages, Findings #1 and #3, and at depth three
/// Theorem 9's bound at these `k` is below 10⁻⁷ a run, past what a campaign here runs).
macro_rules! campaigns {
    ($($name:ident: $strategy:expr, $case:expr;)*) => {$(
        #[test]
        #[ignore = "a campaign, in release"]
        fn $name() {
            campaign($strategy, $case);
        }
    )*};
}

campaigns! {
    swarm_older_term_commit: Strategy::Swarm, 0;
    swarm_read_before_first_commit: Strategy::Swarm, 1;
    swarm_vote_before_durable: Strategy::Swarm, 2;
    swarm_fast_beside_any_term: Strategy::Swarm, 3;
    swarm_fast_any_configuration: Strategy::Swarm, 4;
    pct1_older_term_commit: Strategy::Pct(1), 0;
    pct1_read_before_first_commit: Strategy::Pct(1), 1;
    pct1_vote_before_durable: Strategy::Pct(1), 2;
    pct1_fast_beside_any_term: Strategy::Pct(1), 3;
    pct1_fast_any_configuration: Strategy::Pct(1), 4;
    pct2_older_term_commit: Strategy::Pct(2), 0;
    pct2_read_before_first_commit: Strategy::Pct(2), 1;
    pct2_vote_before_durable: Strategy::Pct(2), 2;
    pct2_fast_beside_any_term: Strategy::Pct(2), 3;
    pct2_fast_any_configuration: Strategy::Pct(2), 4;
    guided_older_term_commit: Strategy::Guided, 0;
    guided_read_before_first_commit: Strategy::Guided, 1;
    guided_vote_before_durable: Strategy::Guided, 2;
    guided_fast_beside_any_term: Strategy::Guided, 3;
    guided_fast_any_configuration: Strategy::Guided, 4;
}

/// Each catch a campaign reports, held to be the defect's: the same run with the defect taken out
/// keeps every oracle. A coverage campaign's catch is found again by running the campaign to it,
/// and its tape played without the defect.
#[test]
fn each_catch_by_seed_is_its_defects() {
    check_catches(false);
}

/// The coverage campaigns' catches, each found again by running its campaign to it.
#[test]
#[ignore = "the coverage campaigns run to their catches, in release"]
fn each_catch_by_coverage_is_its_defects() {
    check_catches(true);
}

fn check_catches(guided: bool) {
    for (strategy, case, seed) in CATCHES
        .iter()
        .filter(|(strategy, ..)| matches!(strategy, Strategy::Guided) == guided)
    {
        let case = CASES[*case];
        let configuration = match strategy {
            Strategy::Swarm => Configuration::swarm(case.harness, *seed),
            _ => Configuration::of(case.harness),
        };
        let (caught, clean) = match strategy {
            Strategy::Swarm => (
                run(
                    case.harness,
                    &configuration,
                    *seed,
                    Some(case.mutant),
                    &mut Seed(Seeded(*seed)),
                ),
                run(
                    case.harness,
                    &configuration,
                    *seed,
                    None,
                    &mut Seed(Seeded(*seed)),
                ),
            ),
            Strategy::Pct(depth) => {
                let members = configuration.count as usize;
                let prioritized = |mutant| {
                    let mut driver = Prioritized::new(*seed, members, *depth, racing(case.harness));
                    run(case.harness, &configuration, *seed, mutant, &mut driver)
                };
                (prioritized(Some(case.mutant)), prioritized(None))
            }
            Strategy::Guided => {
                let (campaign, ..) = guided_campaign(case, *seed);
                let catch = campaign.expect("the coverage campaign catches the defect again");
                assert_eq!(catch.runs, *seed, "the coverage campaign's catch moved");
                let tape = catch
                    .tape
                    .expect("a coverage campaign's catch has its tape");
                let replay = |mutant| {
                    let mut player = Player::replay(&tape, 0, TAPE);
                    let seed = player.word().unwrap();
                    player.end_step(3).unwrap();
                    let mut taped = Taped { player };
                    run(case.harness, &configuration, seed, mutant, &mut taped)
                };
                (replay(Some(case.mutant)), replay(None))
            }
        };
        println!(
            "{strategy:?}, {:?}, {seed}: {:?}; without it: {:?}, settled {}",
            case.mutant,
            caught.violation.as_ref().map(ToString::to_string),
            clean.violation.as_ref().map(ToString::to_string),
            clean.settled
        );
        assert!(caught.violation.is_some(), "{strategy:?} {seed}");
        assert_eq!(clean.violation, None, "{strategy:?} {seed}");
    }
}

/// The catches the campaigns reported (2026-10-05, on the core that counts by the newest
/// configuration in its log and holds what it holds until a classic commit): strategy, case, and
/// the seed (for a coverage campaign, the run). `FastAnyConfiguration` is caught by the random
/// walk alone (`CASES`): swarm, PCT at either depth and the coverage-guided campaign each ran its
/// 8,868 runs without a catch; and the coverage-guided campaign ran `FastBesideAnyTerm`'s 102,775
/// without one.
const CATCHES: &[(Strategy, usize, u64)] = &[
    (Strategy::Swarm, 0, 171),
    (Strategy::Swarm, 1, 17),
    (Strategy::Swarm, 2, 0),
    (Strategy::Swarm, 3, 2_692),
    (Strategy::Pct(1), 1, 7),
    (Strategy::Pct(1), 2, 0),
    (Strategy::Pct(1), 3, 22_065),
    (Strategy::Pct(2), 1, 7),
    (Strategy::Pct(2), 2, 0),
    (Strategy::Pct(2), 3, 38_860),
    (Strategy::Guided, 1, 20),
    (Strategy::Guided, 2, 1),
];

// ---------------------------------------------------------------------------------------------
// Shrinking.

/// Whether `tape`, played in `case`'s harness at `seed`, fails with a violation of `violation`'s
/// kind.
fn fails_as(case: Case, seed: u64, tape: &Tape, violation: &Violation) -> bool {
    let configuration = Configuration::of(case.harness);
    let mut driver = Taped {
        player: Player::replay(tape, seed, TAPE),
    };
    let steps = tape.len() as u64;
    let judged = if case.harness == Harness::Pipelined {
        drive(
            configuration.group::<Lagged>(seed),
            steps,
            &configuration.mix,
            Some(case.mutant),
            &mut driver,
        )
    } else {
        drive(
            configuration.group::<New>(seed),
            steps,
            &configuration.mix,
            Some(case.mutant),
            &mut driver,
        )
    };
    judged
        .violation
        .is_some_and(|found| std::mem::discriminant(&found) == std::mem::discriminant(violation))
}

/// A seed's schedule recorded through a tape: the tape replays the seed's own run, the
/// harness's draws being SplitMix64 read by multiply-shift as the tape reads them.
fn recorded(case: Case, seed: u64) -> (Judged, Tape) {
    let configuration = Configuration::of(case.harness);
    let mut taped = Taped {
        player: Player::record(seed, TAPE),
    };
    let judged = run(
        case.harness,
        &configuration,
        seed,
        Some(case.mutant),
        &mut taped,
    );
    (judged, taped.player.finish())
}

/// The random walk's first catch of a read before the term's first commit among the group
/// schedules' 96 default seeds (`tests/check.rs`, `a_read_before_the_terms_first_commit_is_caught`),
/// recorded through a tape and shrunk while it fails with a stale read: the tape replays the
/// seed's run exactly, and the shrunk tape is 1-minimal and still fails.
#[test]
fn a_failing_run_shrinks_to_a_minimal_tape_that_fails_the_same_way() {
    let case = CASES[1];
    let (seed, judged, tape) = (0..96)
        .map(|seed| {
            let (judged, tape) = recorded(case, seed);
            (seed, judged, tape)
        })
        .find(|(_, judged, _)| judged.violation.is_some())
        .expect("a default seed catches the read before the first commit");
    let violation = judged.violation.expect("found by its violation");
    assert!(
        matches!(violation, Violation::StaleRead { .. }),
        "seed {seed}: {violation}"
    );
    // The tape is the seed's run: the same steps and the same violation.
    let plain = run(
        case.harness,
        &Configuration::of(case.harness),
        seed,
        Some(case.mutant),
        &mut Seed(Seeded(seed)),
    );
    assert_eq!(plain.violation, Some(violation.clone()));
    assert!(fails_as(case, seed, &tape, &violation));
    let shrunk = shrink(tape.clone(), 20_000, |candidate| {
        fails_as(case, seed, candidate, &violation)
    });
    println!(
        "a read before the first commit, seed {seed}: {} steps shrunk to {} in {} runs, minimal {}",
        tape.len(),
        shrunk.tape.len(),
        shrunk.runs,
        shrunk.minimal
    );
    assert!(shrunk.minimal && shrunk.tape.len() < tape.len());
    assert!(fails_as(case, seed, &shrunk.tape, &violation));
}

// ---------------------------------------------------------------------------------------------
// Implementation-level exhaustive search at a tiny scope.

/// Three members, all voters, on focal's rules with one entry an append (so a member is caught up
/// an entry at a time, as Figure 3.7 needs), no pre-vote and no check of quorum (a round's leader
/// is elected by its partition, as the thesis's figure has it).
fn tiny() -> Settings {
    Settings {
        pre_vote: false,
        check_quorum: false,
        max_size_per_msg: 1,
        refuse_ahead: true,
        judged: true,
        ..Settings::focal()
    }
}

/// Scenarios a round may play: a leader (3), the partition its election crosses (with each other
/// member alone or both: 3), the partition its replication crosses (none, either, both: 4), whether
/// it sends its new term's entries or only older ones (2), and whether it then takes a proposal it
/// sends to no one (2).
const SCENARIOS: usize = 3 * 3 * 4 * 2 * 2;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Scenario {
    leader: u64,
    elected_by: Vec<u64>,
    replicated_to: Vec<u64>,
    older_only: bool,
    proposes: bool,
}

fn scenario(choice: usize) -> Scenario {
    let leader = (choice % 3) as u64 + 1;
    let rest = choice / 3;
    let others: Vec<u64> = (1..=3).filter(|m| *m != leader).collect();
    let mut elected_by = vec![leader];
    match rest % 3 {
        0 => elected_by.push(others[0]),
        1 => elected_by.push(others[1]),
        _ => elected_by.extend(&others),
    }
    let rest = rest / 3;
    let mut replicated_to = vec![leader];
    match rest % 4 {
        0 => {}
        1 => replicated_to.push(others[0]),
        2 => replicated_to.push(others[1]),
        _ => replicated_to.extend(&others),
    }
    let rest = rest / 4;
    Scenario {
        leader,
        elected_by,
        replicated_to,
        older_only: rest % 2 == 1,
        proposes: rest / 2 == 1,
    }
}

/// Three members and their judge, a round at a time.
#[derive(Clone)]
struct Tiny {
    group: Cluster<New>,
    judge: Judge,
    depth: u32,
}

impl Tiny {
    fn new(mutant: Option<Mutant>, depth: u32) -> Self {
        let mut group: Cluster<New> = Cluster::new(3, &[1, 2, 3], tiny(), 0);
        group.plant(mutant);
        Self {
            group,
            judge: Judge::new(false, false, 1_000),
            depth,
        }
    }

    fn act(&mut self, op: &Op) {
        self.group.act_observed(&mut self.judge, op);
    }

    fn fault(&self) -> Result<(), String> {
        self.judge
            .violation
            .as_ref()
            .map_or(Ok(()), |v| Err(v.to_string()))
    }

    /// Every member's disk, a digest of all of them.
    fn disks(&self) -> u128 {
        fingerprint(&format!(
            "{:?}",
            self.group
                .ids()
                .iter()
                .map(|id| self.group.disk(*id))
                .collect::<Vec<_>>()
        ))
    }
}

impl System for Tiny {
    type Fault = String;

    fn choices(&self) -> usize {
        SCENARIOS
    }

    /// A round: every member restarts on what it holds (so a round's state is its members' disks
    /// and the judge's, which the key holds); the leader campaigns until it leads or a campaign
    /// fails twice over (a vote refused for a term already voted in is answered at the next term),
    /// its election's messages crossing only its partition; it replicates in rounds of its
    /// heartbeat until no disk changes, its messages crossing only their partition and, where the
    /// scenario says, no append that would give a member an entry of its term (one that follows
    /// what the member holds: an append past a hole is delivered, and refused, as the thesis's
    /// Figure 3.7 has the old entry reach a member while the new one does not); then it takes a proposal,
    /// sent to no one.
    fn play(&mut self, choice: usize) -> Result<(), String> {
        let round = scenario(choice);
        for id in 1..=3 {
            self.act(&Op::Restart(id));
        }
        self.fault()?;
        let leader = round.leader;
        for _ in 0..2 {
            self.act(&Op::Campaign(leader));
            let among = round.elected_by.clone();
            route(&mut self.group, &mut self.judge, |_, m| {
                among.contains(&m.from)
                    && among.contains(&m.to)
                    && matches!(
                        m.msg_type,
                        MessageType::MsgRequestVote | MessageType::MsgRequestVoteResponse
                    )
            });
            self.fault()?;
            if self.group.leaders_now().contains(&leader) {
                break;
            }
        }
        if !self.group.leaders_now().contains(&leader) {
            return Ok(());
        }
        let term = self.group.peek(leader).map_or(0, |node| node.view().term);
        // Each round of the heartbeat moves some disk or ends the replication; a disk takes at
        // most an entry a round and a probe a round for it, and the scope's logs hold at most an
        // entry and a proposal a round over its rounds: twice that many rounds, and one to see
        // nothing move.
        let bound = 2 * 2 * self.depth + 1;
        for _ in 0..bound {
            let before = self.disks();
            for _ in 0..self.group.settings.heartbeat_tick {
                self.act(&Op::Tick(leader));
            }
            let to = round.replicated_to.clone();
            let older = round.older_only;
            route(&mut self.group, &mut self.judge, |group, m| {
                to.contains(&m.from)
                    && to.contains(&m.to)
                    && !(older
                        && m.msg_type == MessageType::MsgAppend
                        && m.entries.iter().any(|e| e.term == term)
                        && m.index <= group.disk(m.to).last_index())
            });
            self.fault()?;
            if self.disks() == before {
                break;
            }
        }
        if round.proposes {
            self.act(&Op::Propose(leader, b"v".to_vec()));
            route(&mut self.group, &mut self.judge, |_, _| false);
        }
        self.fault()
    }

    /// The members' disks and the state of every oracle that judges a round (Election Safety,
    /// Log Matching, State Machine Safety, Leader Completeness, Same History): every member
    /// restarts at the next round, so these are all the futures depend on.
    fn key(&self) -> u128 {
        fingerprint(&format!(
            "{:?} {:?} {:?} {:?} {:?} {:?}",
            self.group
                .ids()
                .iter()
                .map(|id| self.group.disk(*id))
                .collect::<Vec<_>>(),
            self.judge.election,
            self.judge.matching,
            self.judge.machine,
            self.judge.complete,
            self.judge.same,
        ))
    }
}

/// Every sequence of two rounds of three members, played on the real core: no oracle breaks.
#[test]
fn every_two_rounds_of_three_members_keep_every_oracle() {
    let played = rounds(&Tiny::new(None, 2), 2, Budget::default());
    let Played::Exhausted(done) = played else {
        panic!("{played:?}");
    };
    println!("two rounds: {done:?}");
    assert!(done.pruned > 0);
}

/// The commit counted from an older term's replicas (`Mutant::OlderTermCommit`) is caught by the
/// exhaustive search of four rounds, which plays the thesis's Figure 3.7 among them; three rounds
/// keep every oracle with the defect and without it.
#[test]
#[ignore = "an exhaustive search of four rounds, in release"]
fn four_rounds_of_three_members_catch_the_older_term_commit() {
    for mutant in [None, Some(Mutant::OlderTermCommit)] {
        let played = rounds(&Tiny::new(mutant, 3), 3, Budget::default());
        let Played::Exhausted(done) = played else {
            panic!("{mutant:?}: {played:?}");
        };
        println!("{mutant:?}: three rounds: {done:?}");
    }
    let clean = rounds(&Tiny::new(None, 4), 4, Budget::default());
    println!("four rounds, no defect: {clean:?}");
    assert!(matches!(clean, Played::Exhausted(_)));
    let played = rounds(
        &Tiny::new(Some(Mutant::OlderTermCommit), 4),
        4,
        Budget::default(),
    );
    let Played::Fault {
        rounds,
        path,
        fault,
    } = played
    else {
        panic!("four rounds did not catch the older-term commit: {played:?}");
    };
    println!(
        "older-term commit caught after {} rounds played: {fault}\n  {:#?}",
        rounds.played,
        path.iter()
            .map(|choice| scenario(*choice))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------------------------
// What each strategy costs a run (`docs/benchmarks.md`, "hyper-check's strategies (S-5)").

fn costs_of(label: &str, mut each: impl FnMut(u64), seeds: u64) {
    let mut costs = Costs::new();
    for seed in 0..seeds {
        let ((), cost) = measure(|| each(seed));
        costs.add(&cost);
    }
    println!(
        "{label}, {seeds} runs, load {:.1}:\n{}",
        usage::load().unwrap_or(f64::NAN),
        costs.report()
    );
}

/// What a run of each strategy costs, p50 / p99 / max over 96 runs of the group and fast
/// harnesses with no defect, one test at a time (`--test-threads 1`) so the process's counts are
/// the run's: the random walk, the swarm's, PCT's at depth two, the conformance check alone, and
/// the coverage campaign's (each run held to the model, its points counted, mutations as its
/// corpus earns them); then a round of the exhaustive search, and a shrinking.
#[test]
#[ignore = "a measurement, in release, one test at a time"]
fn strategy_costs() {
    for harness in [Harness::Group, Harness::Fast] {
        let configuration = Configuration::of(harness);
        let members = configuration.count as usize;
        costs_of(
            &format!("{harness:?}: random walk"),
            |seed| {
                run(harness, &configuration, seed, None, &mut Seed(Seeded(seed)));
            },
            96,
        );
        costs_of(
            &format!("{harness:?}: swarm"),
            |seed| {
                let drawn = Configuration::swarm(harness, seed);
                run(harness, &drawn, seed, None, &mut Seed(Seeded(seed)));
            },
            96,
        );
        costs_of(
            &format!("{harness:?}: PCT, depth 2"),
            |seed| {
                let mut driver = Prioritized::new(seed, members, 2, racing(harness));
                run(harness, &configuration, seed, None, &mut driver);
            },
            96,
        );
        costs_of(
            &format!("{harness:?}: random walk, every step held to the model"),
            |seed| {
                let mut driver = Conformed::new(Seed(Seeded(seed)), None);
                assert_eq!(
                    run(harness, &configuration, seed, None, &mut driver).departure,
                    None
                );
            },
            96,
        );
        let mut guided = Guided::new(Budget::default(), 64, 0).unwrap();
        let mut fresh = 0u64;
        costs_of(
            &format!("{harness:?}: coverage-guided"),
            |runs| {
                let mut player = match guided.mutated() {
                    Some(tape) => Player::replay(&tape, runs, TAPE),
                    None => {
                        fresh += 1;
                        Player::record(fresh - 1, TAPE)
                    }
                };
                let seed = player.word().unwrap();
                player.end_step(3).unwrap();
                let mut taped = Taped { player };
                let new = {
                    let mut driver = Conformed::new(&mut taped, Some(&mut guided));
                    run(harness, &configuration, seed, None, &mut driver);
                    driver.new
                };
                guided.finished(taped.player.finish(), new);
            },
            96,
        );
        println!(
            "{harness:?}: coverage-guided: {} abstract states",
            guided.points()
        );
    }
    let mut played = 0;
    costs_of(
        "exhaustive, two rounds of three members (one search)",
        |_| {
            if let Played::Exhausted(done) = rounds(&Tiny::new(None, 2), 2, Budget::default()) {
                played = done.played;
            }
        },
        3,
    );
    println!("  {played} rounds played a search");
    let case = CASES[1];
    let (judged, tape) = recorded(case, 3);
    let violation = judged.violation.unwrap();
    costs_of(
        "shrinking seed 3's read before the first commit (one shrinking)",
        |_| {
            shrink(tape.clone(), 20_000, |candidate| {
                fails_as(case, 3, candidate, &violation)
            });
        },
        3,
    );
}

// ---------------------------------------------------------------------------------------------
// The swarm and the model over the core with no defect planted.

/// The seeds the swarm runs each harness with no defect planted: enough to see, with probability
/// 0.95, a defect that one seed in 2,693 reaches, ⌈ln 0.05 / ln(1 − 1/2,693)⌉: the rarest a planted
/// defect the swarm catches was for it, the first fast-track rule's catch at run 2,693 (`CATCHES`).
/// Run 2026-10-05 at 9,924 seeds (the count for the catch at 3,313 before raft-rs's precedence left
/// the swarm): no failure on any harness, at most 9,039, 8,472 and 3,396 operations a liveness
/// phase (group, fast, pipelined).
const NO_DEFECT_SEEDS: u64 = 8_067;

/// The swarm's configurations of each harness with no defect planted, every step held to the
/// model: every oracle keeps, every step is a model step, every group settles and every read it
/// confirmed is served. A failure here is a defect of the core, or of a judge, to trace to its
/// cause (`docs/sim.md` §15).
#[test]
#[ignore = "a campaign of the swarm with no defect, in release"]
fn the_swarm_with_no_defect_keeps_every_oracle_and_the_model() {
    for harness in [Harness::Group, Harness::Fast, Harness::Pipelined] {
        let mut failures = Vec::new();
        let mut most = 0;
        let seeds = NO_DEFECT_SEEDS;
        for seed in 0..seeds {
            let configuration = Configuration::swarm(harness, seed);
            let mut driver = Conformed::new(Seed(Seeded(seed)), None);
            let judged = run(harness, &configuration, seed, None, &mut driver);
            most = most.max(judged.steps.saturating_sub(harness.steps()));
            if judged.violation.is_some()
                || judged.departure.is_some()
                || !judged.settled
                || judged.hot.is_some()
            {
                failures.push((
                    seed,
                    judged.violation.map(|v| v.to_string()),
                    judged.departure,
                    judged.settled,
                    judged.hot,
                ));
            }
        }
        println!(
            "{harness:?}: {seeds} swarm seeds, at most {most} operations a liveness phase, {} failed: {failures:#?}",
            failures.len()
        );
        assert!(failures.is_empty(), "{harness:?}: {failures:#?}");
    }
}

/// The thesis's Figure 3.7 as four rounds of the round search: member 1 leads and replicates its
/// first entry, then takes one it sends to no one; member 3 leads by member 2 and takes two of its
/// own; member 1 leads again by member 2 and gives it the old entry but none of its term; member 3
/// leads again by member 2. With the commit counted from an older term's replicas, member 1 commits
/// the old entry and member 3 then leads without it; without the defect every oracle keeps.
#[test]
fn figure_three_seven_is_a_path_of_four_rounds() {
    let find = |want: &Scenario| (0..SCENARIOS).find(|c| scenario(*c) == *want).unwrap();
    let path = [
        Scenario {
            leader: 1,
            elected_by: vec![1, 2, 3],
            replicated_to: vec![1, 2, 3],
            older_only: false,
            proposes: true,
        },
        Scenario {
            leader: 3,
            elected_by: vec![3, 2],
            replicated_to: vec![3],
            older_only: false,
            proposes: true,
        },
        Scenario {
            leader: 1,
            elected_by: vec![1, 2],
            replicated_to: vec![1, 2],
            older_only: true,
            proposes: false,
        },
        Scenario {
            leader: 3,
            elected_by: vec![3, 2],
            replicated_to: vec![3, 2],
            older_only: false,
            proposes: false,
        },
    ];
    for mutant in [None, Some(Mutant::OlderTermCommit)] {
        let mut tiny = Tiny::new(mutant, 4);
        let played: Result<Vec<()>, String> =
            path.iter().map(|round| tiny.play(find(round))).collect();
        match mutant {
            None => assert_eq!(played, Ok(vec![(); 4])),
            Some(_) => {
                let fault = played.unwrap_err();
                assert!(fault.starts_with("Leader Completeness"), "{fault}");
            }
        }
    }
}

/// The swarm's seeds that found the judges wanting (`docs/sim.md` §15.9): at pipelined seeds 123,
/// 522 and 967 the durability oracle refused a candidate's request whose log its term's leader had
/// cut before the request's notice; at pipelined seeds 299, 585, 1,159, 1,580 and 1,633 the
/// abstraction read a member that led a term its device did not yet hold as the model's leader; at
/// fast seed 34,957 Log Matching refused a term falling after a member's committed prefix, which the
/// fast track keeps as the member holds it. Each now keeps every oracle and every step is a model
/// step.
#[test]
fn the_swarm_seeds_that_found_the_judges_wanting_keep_every_oracle_and_the_model() {
    let seeds = [123, 522, 967, 299, 585, 1_159, 1_580, 1_633]
        .map(|seed| (Harness::Pipelined, seed))
        .into_iter()
        .chain([(Harness::Fast, 34_957)]);
    for (harness, seed) in seeds {
        let configuration = Configuration::swarm(harness, seed);
        let mut driver = Conformed::new(Seed(Seeded(seed)), None);
        let judged = run(harness, &configuration, seed, None, &mut driver);
        assert_eq!(judged.violation, None, "{harness:?}, seed {seed}");
        assert_eq!(judged.departure, None, "{harness:?}, seed {seed}");
        assert!(judged.settled, "{harness:?}, seed {seed}");
    }
}

/// The swarm's fast seeds whose groups never converged (`docs/sim.md` §15.9): at 3,112 a leader
/// whose group's later configuration made it a learner led its old term for ever, no member
/// answering its appends without check-quorum or pre-vote (`Raft::step_older_term`); at 2,396 the
/// harness's network lost a snapshot at its bound without telling its sender, which waited on it
/// for ever (`Cluster::report`). Each now settles, and keeps every oracle and the model.
#[test]
fn the_swarm_seeds_whose_groups_never_converged_settle() {
    for seed in [2_396, 3_112] {
        let configuration = Configuration::swarm(Harness::Fast, seed);
        let mut driver = Conformed::new(Seed(Seeded(seed)), None);
        let judged = run(Harness::Fast, &configuration, seed, None, &mut driver);
        assert_eq!(judged.violation, None, "seed {seed}");
        assert_eq!(judged.departure, None, "seed {seed}");
        assert!(judged.settled, "seed {seed}: the group did not converge");
    }
}

/// The swarm's campaign against the fast track's second rule found, at fast seed 41,345 with no
/// defect planted (`docs/sim.md` §15.9), an entry committed by the fast quorum lost: four voters,
/// index 10 committed in term 13 with member 4's vote among the three; member 4 led term 18, took
/// the entry into its log and let its holding go (it held a proposal only above its log); term 23's
/// leader cut member 4's log below index 10, member 4 held another proposal there, and the election
/// of term 34 by members 1, 3 and 4 recovered the other value: members 1 and 3 had the entry in
/// their logs alone, from term 13's leader, which the fast quorum had counted and no election
/// reads. A member now holds what it holds by itself until it knows the index committed by a
/// classic quorum, and a fast quorum counts only such holdings (`docs/raft.md` §3.5). The seed's
/// run keeps every oracle.
#[test]
fn a_fast_committed_entry_outlives_a_vote_its_log_covered_and_then_lost() {
    let seed = 41_345;
    let configuration = Configuration::swarm(Harness::Fast, seed);
    let judged = run(
        Harness::Fast,
        &configuration,
        seed,
        None,
        &mut Seed(Seeded(seed)),
    );
    assert_eq!(judged.violation, None);
}
