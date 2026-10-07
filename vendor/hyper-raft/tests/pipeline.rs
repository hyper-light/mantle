//! Readies taken ahead of their persistence (core step R-4, `docs/durable.md`
//! §2.1). Every member of the group takes `Ready`s while earlier writes are
//! out, its writes become durable on its disk in the order they were issued
//! but when the schedule says, and its owner hears of them later still, a
//! notice at a time for one write or several. Those steps are drawn at
//! random among proposals, ticks, deliveries, elections, changes,
//! compactions and crashes, and a crash loses every write that was not
//! durable.
//!
//! Whatever the interleaving, the members are held to the invariants of
//! `docs/durable.md` §3 the core keeps, against their disks as they are at
//! each step (`support::lagged`, `Cluster::check_durable`):
//! - I1: no term, vote or vote request leaves before the disk holds it, and
//!   a leader sends at once only while its term and vote are durable;
//! - I2: no acknowledgement leaves before the disk holds what it
//!   acknowledges, nor the fast track's word that a member holds an entry;
//! - I3: a leader counts itself for no entry its disk does not hold, and
//!   commits nothing a majority of each half of its configuration does not
//!   hold durably;
//! - I4: nothing is given to apply that is not committed and durable here,
//!   but a leader's own entries where it applies before its write is durable
//!   (core step R-6, `docs/durable.md` §4.2);
//! - I5: a change of configuration applies only once the disk states a
//!   commit covering it, the member holding it and asked for nothing more
//!   to apply meanwhile (R-6's apply pause, §4.1);
//! - I7: once a write is durable the disk holds what the member held when
//!   it took the `Ready`;
//! - R-6: an answer to an append or a heartbeat states no commit its disk
//!   does not state when it leaves;
//!
//! and to what every schedule is held to (`Cluster::report`): no two members
//! commit different entries at one index, no term has two leaders, every
//! answered read saw what was committed when it was asked. Once the network
//! is whole and the members up, the group elects and commits.
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
    unreachable_pub
)]
mod support;

use hyper_measure::alloc::Counting;
use hyper_measure::cost::{Costs, measure};
use hyper_measure::usage;
use support::{Cluster, Coverage, Lagged, Mix, Op, Seeded, Settings, Step};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[allow(
    clippy::disallowed_methods,
    reason = "a soak sets the seed count from the environment; the default is the gate's"
)]
fn count(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Of a hundred steps, how many are persistence steps: enough that writes
/// pile up to the bound and notices cover several, few enough that the
/// group still elects and commits between them.
const LAG: u64 = 35;

/// Of a hundred chances to make a leader's write durable, how many are
/// taken where its disk is the slowest of its group. Measured: at a hundred
/// (a disk like the others') 24 schedules of the setting that applies before
/// durability never once committed a leader's entry by its followers before
/// its own write (no entry applied ahead); at a quarter they do at the
/// default size (10 entries) and at 1,000 schedules (303).
const SLOW_LEADER: u64 = 25;

fn mix(fast: u64) -> Mix {
    Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        fast,
        lag: LAG,
        ..Mix::everything()
    }
}

/// The schedules of `settings`: where a leader applies before its own write
/// is durable, its disk is the slowest of its group.
fn mix_for(settings: &Settings, fast: u64) -> Mix {
    Mix {
        leader_durable: if settings.apply_unpersisted {
            SLOW_LEADER
        } else {
            100
        },
        ..mix(fast)
    }
}

/// One group of members driven ahead of their persistence under the
/// schedule of `seed`; `crash` stops and reopens the member of the
/// `crash.0`-th persistence step that did something, right after it.
/// Returns the group and how many persistence steps did something.
fn schedule(
    settings: Settings,
    voters: &[u64],
    seed: u64,
    steps: u64,
    mix: &Mix,
    crash: Option<u64>,
) -> (Cluster<Lagged>, u64) {
    let mut group: Cluster<Lagged> = Cluster::new(5, voters, settings, seed);
    group.stop_who_left = true;
    let mut rng = Seeded(seed);
    let mut persisted = 0u64;
    for _ in 0..steps {
        if let Some(member) = persistence_step(&mut group, &mut rng, mix) {
            if crash == Some(persisted) {
                group.act(&Op::Restart(member));
            }
            persisted += 1;
        }
    }
    (settled(group, seed, crash), persisted)
}

/// One step of the schedule: the member whose persistence step it was, if it did something.
fn persistence_step(group: &mut Cluster<Lagged>, rng: &mut Seeded, mix: &Mix) -> Option<u64> {
    let op = group.choose(rng, mix);
    let reports = group.act(&op);
    match op {
        Op::Persist(member, _) if reports.iter().any(|report| report.accepted == Some(true)) => {
            Some(member)
        }
        _ => None,
    }
}

/// The group through its liveness phase, and held to what every schedule's end is held to.
fn settled(mut group: Cluster<Lagged>, seed: u64, crash: Option<u64>) -> Cluster<Lagged> {
    let by_length = group.settings.by_length;
    if group.settles() {
        group.check_kept();
    } else {
        // Only a group whose marks leave no member the election rule admits
        // may wait (`docs/durable.md` §5.2): it is counted, and is no failure.
        // The rule's argument is by the log's precedence. By raft-rs's
        // precedence of length a voter of higher priority refuses a candidate
        // whose log is shorter however much more current, and may be one the
        // group could not elect instead (`Config::raft_rs_precedence`): with marks, a
        // group with every member up may then wait with a member the rule
        // admits. Seed 740 of 960: the voter of the longest log, of an older
        // term, refused the candidate of the later term for priority, the
        // marked voter refused it for its mark, and neither could be elected.
        let electable = if by_length {
            Vec::new()
        } else {
            group.electable()
        };
        assert!(
            group.faults > 0 && electable.is_empty(),
            "seed {seed}, crash {crash:?}: the group did not settle, and {electable:?} could lead"
        );
        group.waits += 1;
    }
    assert_eq!(
        group.deposed, 0,
        "seed {seed}: a member led a group it left"
    );
    group
}

/// The schedule of `seed` run once, with a crash at each of its persistence steps that did
/// something enumerated by fork (`hyper_check::strategy::fork`, `docs/sim.md` §3.7): at each, the
/// group and its draws are cloned, the member crashed in the clone, and the clone run through the
/// rest of the schedule and its liveness phase, one clone alive at a time. Gives each crash
/// point's group, in order, and the trunk's steps.
fn forked(
    settings: Settings,
    voters: &[u64],
    seed: u64,
    steps: u64,
    mix: &Mix,
    mut each: impl FnMut(u64, Cluster<Lagged>),
) -> u64 {
    let mut group: Cluster<Lagged> = Cluster::new(5, voters, settings, seed);
    group.stop_who_left = true;
    let mut world = (group, Seeded(seed), 0u64);
    let mut point = 0u64;
    let trunk = hyper_check::strategy::fork::each_point(
        &mut world,
        steps,
        |(group, rng, taken)| {
            *taken += 1;
            Some(persistence_step(group, rng, mix))
        },
        |(mut group, mut rng, taken), member| {
            group.act(&Op::Restart(member));
            for _ in taken..steps {
                persistence_step(&mut group, &mut rng, mix);
            }
            let crash = point;
            point += 1;
            each(crash, settled(group, seed, Some(crash)));
        },
    );
    trunk.steps
}

/// What a crash point's run ended in, as the replay and the fork are compared: every member's
/// disk and view, what was committed, the persistence steps' coverage, and the faults, waits and
/// repairs counted.
fn outcome(group: &Cluster<Lagged>) -> String {
    let members: Vec<String> = group
        .ids()
        .into_iter()
        .map(|id| {
            format!(
                "{:?} {:?}",
                group.disk(id),
                group.peek(id).map(support::Replica::view)
            )
        })
        .collect();
    format!(
        "{members:?} {:?} {:?} {} {} {} {:?}",
        group.chosen,
        group.coverage(),
        group.faults,
        group.waits,
        group.repaired,
        group.incarnations
    )
}

/// Of a hundred chances to make a leader's write durable, how many are
/// taken in the long schedules where a leader applies before its write is
/// durable ([`schedules`]); the crashes' and faults' schedules keep
/// [`SLOW_LEADER`], at which each still holds a change behind the fence.
/// Measured on R16's core, each append charged its record, which sends a
/// member fewer appends ahead in the schedules' small windows, with a
/// campaign superseding its own requests (`d254f78`): at a quarter, 24
/// schedules by suspicion applied no entry ahead (22 of 240, where 61 before
/// R16); at a tenth, 15 at the default size (on ticks, 12 at a quarter and
/// 8 at a tenth).
const SCHEDULES_SLOW_LEADER: u64 = 10;

/// Schedules of `settings` at the depth it states; what they reached.
fn schedules(name: &str, settings: Settings, voters: &[u64], fast: u64) -> (Coverage, usize) {
    let mut mix = mix_for(&settings, fast);
    if settings.apply_unpersisted {
        mix.leader_durable = SCHEDULES_SLOW_LEADER;
    }
    schedules_of(name, settings, voters, &mix)
}

/// Schedules of `settings` drawn from `mix`; what they reached.
fn schedules_of(name: &str, settings: Settings, voters: &[u64], mix: &Mix) -> (Coverage, usize) {
    let seeds = count("HYPER_RAFT_SEEDS", 24);
    let steps = count("HYPER_RAFT_STEPS", 2_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mut coverage = Coverage::default();
    let mut committed = 0;
    let mut terms = 0;
    let mut faults = 0;
    let mut repaired = 0;
    let mut marked_steps = 0;
    let mut waits = 0;
    for seed in first..first + seeds {
        let (group, _) = schedule(settings, voters, seed, steps, mix, None);
        coverage.add(group.coverage());
        committed += group.chosen.len();
        terms += group.leaders.len();
        faults += group.faults;
        repaired += group.repaired;
        marked_steps += group.marked_steps;
        waits += group.waits;
    }
    println!(
        "{name}: {seeds} schedules of {steps} steps committed {committed} entries in {terms} terms led, {faults} faults at rest, {repaired} marks ended after {} steps each, {waits} groups left waiting on a mark; {coverage:?}",
        marked_steps.checked_div(repaired).unwrap_or(0)
    );
    if mix.corrupt > 0 {
        // Faults at rest struck, and marks ended: members were repaired in
        // place. Not every schedule draws a fault (seed 11 of the shell's
        // three writes out draws none).
        assert!(faults > 0 && repaired > 0, "{name}: {faults} {repaired}");
    }
    // Schedules that never committed, never had two writes out, never heard
    // of several at once and never lost one prove nothing of R-4. Each check
    // asks that the mechanism was reached; how often is reported above, not
    // judged against a picked count.
    assert!(committed > 0, "{name}: {committed}");
    assert!(
        coverage.behind > 0 && coverage.several > 0,
        "{name}: {coverage:?}"
    );
    assert!(
        coverage.refused > 0 && coverage.lost > 0,
        "{name}: {coverage:?}"
    );
    assert!(coverage.held_back > 0, "{name}: {coverage:?}");
    // R-6 reached: answers held to the disk's commit, changes held behind
    // the fence, and the commit stated for them.
    assert!(
        coverage.answers > 0 && coverage.fenced > 0 && coverage.stated > 0,
        "{name}: {coverage:?}"
    );
    // R17 reached: members that keep what arrives ahead of a hole took some
    // of it into their logs, each acknowledged only with the write that held
    // it (`check_message` holds every acknowledgement to the member's disk).
    if !settings.refuse_ahead {
        assert!(coverage.ahead > 0, "{name}: {coverage:?}");
    }
    // With faults at rest a leader's term is short, and its own entries are
    // rarely committed before its write of them: the schedules without
    // faults are the ones that hold applying before durability to its
    // coverage (24 schedules with two marks at once reached none).
    if settings.apply_unpersisted && mix.corrupt == 0 {
        assert!(coverage.unpersisted > 0, "{name}: {coverage:?}");
    }
    (coverage, committed)
}

/// The four settings the schedules run at.
fn settings() -> [(&'static str, Settings); 4] {
    [
        (
            "shell, three writes out",
            Settings {
                depth: 3,
                ..Settings::shell()
            },
        ),
        (
            "focal, two writes out, in place",
            Settings {
                depth: 2,
                in_place: true,
                ..Settings::focal()
            },
        ),
        (
            "focal, three writes out, narrow",
            Settings {
                depth: 3,
                max_inflight_msgs: 2,
                max_size_per_msg: 1,
                max_committed_size_per_ready: 64,
                ..Settings::focal()
            },
        ),
        (
            "focal, three writes out, in place, a leader applying before its write",
            Settings {
                depth: 3,
                in_place: true,
                apply_unpersisted: true,
                ..Settings::focal()
            },
        ),
    ]
}

/// Of ten thousand steps, how many are a fault at rest (`Mix::corrupt`):
/// one in as many steps as a mark lasts, so that faults come as often as the
/// rule of one marked member at a time lets them. Measured: at fifty, 200
/// schedules of each setting ended 1,194 marks after 640 steps each on
/// average (`faults_at_rest_lose_nothing_acknowledged`); 10,000 / 640 is
/// sixteen. `HYPER_RAFT_CORRUPT` sets it to measure again.
fn corrupt_rate() -> u64 {
    count("HYPER_RAFT_CORRUPT", 16)
}

/// The same schedules, with faults at rest besides (Ganesan et al., FAST
/// 2017, the checklist of `docs/durable.md` §12): a bit flipped in the last
/// write's entries or an earlier one's, and the last writes lost though
/// acknowledged, each found when the member opens (`Disk::verify`) and
/// marked (`hyper_raft::Lost`). The member judges votes by its mark and tells
/// a leader that counts what it lost (core step R-5), which sends it the
/// entries again. Held to every invariant above, with a leader's commit
/// counted by a mark where the copy was lost at rest, and once settled every
/// member holds every committed entry as it was committed: nothing
/// acknowledged is lost, and no damaged entry was applied or sent, or the
/// group would hold another at its index.
#[test]
fn faults_at_rest_lose_nothing_acknowledged() {
    faults_at_rest(1);
}

/// The same with two of the three voters' disks marked at once: the case
/// where the only logs as current as the group's may all be marked (Protocol-
/// Aware Recovery's Figure 4(b), Alagappan et al., FAST 2018, §3.4), which
/// a marked member's election (core step R-7) is for. What both marked
/// members acknowledged may be lost to both: such a group must wait, and
/// is counted.
#[test]
fn faults_at_rest_on_two_members_at_once_lose_nothing_acknowledged() {
    faults_at_rest(2);
}

fn faults_at_rest(marks: usize) {
    for suspicion in [false, true] {
        for (name, settings) in settings() {
            let settings = Settings {
                suspicion,
                ..settings
            };
            let mix = Mix {
                corrupt: corrupt_rate(),
                marks,
                ..mix_for(&settings, 0)
            };
            schedules_of(name, settings, &[1, 2, 3], &mix);
        }
    }
}

#[test]
fn random_interleavings_keep_every_invariant() {
    for (name, settings) in settings() {
        schedules(name, settings, &[1, 2, 3], 0);
    }
}

/// The same schedules with every member electing by suspicion (timing step
/// L-2, `docs/timing.md` §2.3): no member ticks; a tick of the schedule is
/// its clock moving, and its detectors suspect and trust, right nine times
/// in ten about a member that is down or cut off and wrong one time in ten
/// about one that is not. The invariants are the same, and the group
/// settles once the network is whole and the detectors trust every member.
#[test]
fn random_interleavings_by_suspicion_keep_every_invariant() {
    for (name, settings) in settings() {
        schedules(name, settings.by_suspicion(), &[1, 2, 3], 0);
    }
}

/// `docs/raft.md`'s fast-track schedules with readies persisted at random
/// lags: what a member says it holds beside its log it says once its disk
/// holds it (I2), and the fast quorum commits only what is durable.
#[test]
fn the_fast_track_with_readies_persisted_at_random_lags_is_safe_and_settles() {
    for suspicion in [false, true] {
        for in_place in [false, true] {
            schedules(
                "fast",
                Settings {
                    depth: 3,
                    in_place,
                    suspicion,
                    ..Settings::fast()
                },
                &[1, 2, 3, 4, 5],
                60,
            );
        }
    }
}

/// A crash at every persistence step of a schedule in turn: after a `Ready`
/// was taken and its write issued, after a write became durable and before
/// its owner heard, and after the owner heard and released what waited.
/// Each crash loses the writes out and nothing durable, and the group stays
/// safe and settles.
#[test]
fn a_crash_at_every_persistence_step_loses_nothing_durable() {
    crashes_at_every_persistence_step(false);
}

/// The same crashes, in schedules with faults at rest besides: a crash
/// between any two persistence steps of a member marked, or of its leader
/// while it repairs it, loses nothing acknowledged. A schedule here is
/// shorter than a mark lasts ([`corrupt_rate`]), so each suffers one fault in
/// its steps, as many as there are.
#[test]
fn a_crash_at_every_persistence_step_with_faults_at_rest_loses_nothing_acknowledged() {
    crashes_at_every_persistence_step(true);
}

/// The settings and mixes the crash enumerations run, and the seeds whose schedules they take:
/// with faults at rest, the first seeds whose schedule suffers one.
fn crash_campaigns(faults_at_rest: bool) -> Vec<(Settings, Mix, Vec<u64>)> {
    // Eleven: the fewest from seed zero at which every variant below, on ticks
    // and by suspicion, holds a change behind the fence at some crash
    // (measured 2026-10-04). It was four until a read asked of a new leader
    // waited for its term's first commit: the rounds of the reads held until
    // then moved the schedules, and the variant on ticks that applies only
    // what is durable first held a change behind the fence at seed ten.
    let seeds = count("HYPER_RAFT_CRASH_SEEDS", 11);
    let steps = count("HYPER_RAFT_CRASH_STEPS", 400);
    let corrupt = if faults_at_rest { 10_000 / steps } else { 0 };
    // The second has a leader apply its own entries before its write of them
    // is durable: a crash between the two (`docs/durable.md` §12). Each on
    // ticks and by suspicion.
    let mut campaigns = Vec::new();
    for (apply_unpersisted, suspicion) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let settings = Settings {
            depth: 3,
            apply_unpersisted,
            suspicion,
            ..Settings::focal()
        };
        let mix = Mix {
            corrupt,
            ..mix_for(&settings, 0)
        };
        // With faults at rest, the first seeds whose schedule suffers one:
        // a fault finds a disk that holds entries only between compactions,
        // and one schedule in twenty did (measured, from seed zero), so the
        // search is bounded at sixty-four times as many.
        let mut taken = Vec::new();
        for seed in 0..seeds * 64 {
            if taken.len() as u64 == seeds {
                break;
            }
            let (whole, _) = schedule(settings, &[1, 2, 3], seed, steps, &mix, None);
            if faults_at_rest && whole.faults == 0 {
                continue;
            }
            taken.push(seed);
        }
        assert_eq!(
            taken.len() as u64,
            seeds,
            "seeds whose schedule suffered a fault at rest"
        );
        campaigns.push((settings, mix, taken));
    }
    campaigns
}

fn crashes_at_every_persistence_step(faults_at_rest: bool) {
    let steps = count("HYPER_RAFT_CRASH_STEPS", 400);
    for (settings, mix, seeds) in crash_campaigns(faults_at_rest) {
        let mut crashes = 0u64;
        let mut lost = 0u64;
        let mut faults = 0u64;
        let mut reached = Coverage::default();
        for seed in seeds {
            forked(settings, &[1, 2, 3], seed, steps, &mix, |_, group| {
                crashes += 1;
                faults += group.faults;
                let coverage = group.coverage();
                lost += coverage.lost;
                reached.add(coverage);
            });
        }
        let (apply_unpersisted, suspicion) = (settings.apply_unpersisted, settings.suspicion);
        println!(
            "applying before durable {apply_unpersisted}, by suspicion {suspicion}: {crashes} crashes, one at each persistence step, lost {lost} writes out, {faults} faults at rest; {reached:?}"
        );
        assert!(!faults_at_rest || faults > 0, "{faults} {crashes}");
        assert!(crashes > 0 && lost > 0, "{crashes} {lost}");
        assert!(reached.answers > 0 && reached.fenced > 0, "{reached:?}");
        assert!(!apply_unpersisted || reached.unpersisted > 0, "{reached:?}");
    }
}

/// The gate of crash enumeration by fork (`docs/sim.md` §9, S-5): every crash point of every
/// schedule the enumerations above take ends, forked, exactly as the replay of its schedule with
/// the crash at that point ends (every member's disk and view, what was committed, the coverage,
/// the faults and waits), with and without faults at rest; and the fork takes the schedule's
/// steps once where the replay took them once a crash point.
#[test]
fn a_crash_point_forked_ends_as_its_replay_ends() {
    let steps = count("HYPER_RAFT_CRASH_STEPS", 400);
    for faults_at_rest in [false, true] {
        let mut points = 0u64;
        let (mut replayed, mut trunk) = (0u64, 0u64);
        for (settings, mix, seeds) in crash_campaigns(faults_at_rest) {
            for seed in seeds {
                trunk += forked(settings, &[1, 2, 3], seed, steps, &mix, |crash, group| {
                    let (replay, _) =
                        schedule(settings, &[1, 2, 3], seed, steps, &mix, Some(crash));
                    replayed += steps;
                    points += 1;
                    assert_eq!(
                        outcome(&group),
                        outcome(&replay),
                        "seed {seed}, crash {crash}: the fork and the replay part"
                    );
                });
            }
        }
        println!(
            "faults at rest {faults_at_rest}: {points} crash points, each forked as its replay ends; the forks' trunks took {trunk} steps, the replays' prefixes and rests {replayed}"
        );
        assert!(points > 0);
    }
}

/// A notice of a write heard after a crash is never given: the member opens
/// on what its disk holds, and a write durable before the crash and never
/// heard of is held all the same.
#[test]
fn a_write_durable_and_never_heard_of_is_held_after_a_crash() {
    let settings = Settings {
        depth: 3,
        ..Settings::focal()
    };
    let mut group: Cluster<Lagged> = Cluster::new(3, &[1, 2, 3], settings, 7);
    group.act(&Op::Campaign(1));
    for _ in 0..64 {
        for id in group.up() {
            group.act(&Op::Persist(id, Step::Flush));
        }
        if group.net.is_empty() {
            break;
        }
        while !group.net.is_empty() {
            group.act(&Op::Deliver {
                at: 0,
                keep: false,
                lose: false,
            });
        }
    }
    assert_eq!(group.leaders_now(), vec![1]);
    let before = group.disk(1).last_index();
    group.act(&Op::Propose(1, b"kept".to_vec()));
    group.act(&Op::Persist(1, Step::Take));
    group.act(&Op::Propose(1, b"lost".to_vec()));
    group.act(&Op::Persist(1, Step::Take));
    // The first is durable and never heard of; the second is out.
    group.act(&Op::Persist(1, Step::Durable));
    assert_eq!(group.disk(1).last_index(), before + 1);
    group.act(&Op::Restart(1));
    let disk = group.disk(1);
    assert_eq!(disk.last_index(), before + 1);
    assert_eq!(disk.entries.last().unwrap().data, b"kept");
    assert_eq!(group.coverage().lost, 1);
    assert!(group.settles());
}

/// What enumerating a schedule's crash points costs, by fork and by replay, p50 / p99 / max over
/// the schedules the enumerations take (each a seed of a variant, 44 in all), one test at a time:
/// `docs/benchmarks.md`, "hyper-check's strategies (S-5)".
#[test]
#[ignore = "a measurement, in release, one test at a time"]
fn crash_enumeration_costs() {
    let steps = count("HYPER_RAFT_CRASH_STEPS", 400);
    let (mut forks, mut replays) = (Costs::new(), Costs::new());
    let mut points = 0u64;
    for (settings, mix, seeds) in crash_campaigns(false) {
        for seed in seeds {
            let ((), cost) = measure(|| {
                forked(settings, &[1, 2, 3], seed, steps, &mix, |_, _| points += 1);
            });
            forks.add(&cost);
            let ((), cost) = measure(|| {
                let (_, events) = schedule(settings, &[1, 2, 3], seed, steps, &mix, None);
                for at in 0..events {
                    schedule(settings, &[1, 2, 3], seed, steps, &mix, Some(at));
                }
            });
            replays.add(&cost);
        }
    }
    println!(
        "{points} crash points; load {:.1}\nby fork, a schedule's enumeration:\n{}\nby replay:\n{}",
        usage::load().unwrap_or(f64::NAN),
        forks.report(),
        replays.report()
    );
}
