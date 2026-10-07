//! This core's schedules judged by hyper-check (`docs/sim.md` §4, step S-4): the same seeds the
//! schedule tests run (`tests/group.rs`, `tests/fast.rs`, `tests/pipeline.rs`), with every oracle of
//! §4.1 fed after every operation over the whole history, the clients' history recorded and judged
//! by both linearizability checkers, which must agree, and the paths the runs claim held to their
//! floors (§4.4). Then the planted defects of §4.6 (`src/mutant.rs`), each run with the harness's
//! own checks set aside (`Settings::judged`), and caught by the oracle that names it.
//!
//! The clients' history. A proposal a leader takes is a write of a register (one of two, by its
//! first byte), its value its own number: called when proposed, published when its entry is first
//! committed by any member, answered committed when its proposer applies that entry and refused when
//! its proposer applies another at its index. A proposer that restarts, or stops leading the term it
//! took the write in, answers it unknown (its owner gives up the term's waits, as focal's does), and
//! the client asks again under the same request, a session's question (thesis §6.3): that attempt
//! is answered once the index is decided, committed or refused. A read is a
//! read of a register (by its number): called when asked, answered when the member that confirmed it
//! has applied through the index it confirmed, with the register's value there, and unknown when
//! that member restarts first. The state machine whose registers these are applies the writes the
//! history recorded and nothing else, so a proposal a follower forwarded, which a duplicated message
//! can append twice (its owner keeps no session, thesis §6.3), changes no register.
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

use hyper_check::coverage::{Counters, Floor, Measured, Per, hold};
use hyper_check::oracle::{Rule, Violation};
use hyper_measure::alloc::Counting;
use hyper_raft::Mutant;

use support::judge::{
    Core, Judge, PATHS, Totals, agreed, between, elect, judged, replicate, route,
};
use support::{Cluster, Lagged, Mix, New, Op, Settings};

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

fn floors(name: &str, counters: &Counters, seeds: u64, floors: &[Floor]) {
    for (path, count) in counters.iter() {
        println!("{name}: {path}: {count}");
    }
    if let Err(fallen) = hold(floors, counters, seeds) {
        panic!("{name}: {fallen}");
    }
}

fn seed_floor(path: &'static str, count: u64, seeds: u64) -> Floor {
    Floor {
        path,
        per: Per::Seed,
        measured: Measured { count, seeds },
    }
}

fn campaign_floor(path: &'static str, count: u64, seeds: u64) -> Floor {
    Floor {
        path,
        per: Per::Campaign,
        measured: Measured { count, seeds },
    }
}

/// `tests/group.rs`'s schedules of this core, judged.
#[test]
fn the_group_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for seed in first..first + seeds {
        let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
        group.stop_who_left = true;
        let run = judged(group, seed, steps, &mix, None);
        assert!(
            run.violation.is_none(),
            "seed {seed}: {}",
            run.violation.unwrap()
        );
        assert!(run.settled, "seed {seed}: the group did not settle");
        assert!(
            run.hot.is_none(),
            "seed {seed}: {}",
            run.hot.unwrap_or_default()
        );
        agreed(seed, &run.events, &mut totals);
        counters.merge(&run.counters);
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("group", &counters, seeds, &group_floors());
}

/// The floors of the group schedules, each set from its count over the default 96 seeds of 4,000
/// steps (2026-10-05, this test at its commit): more than one a seed where the count was, at least
/// one a campaign where it was rarer.
fn group_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 911, 96),
        seed_floor("entries committed", 91775, 96),
        seed_floor("log entries held", 63578, 96),
        seed_floor("leaderships held to the committed entries", 911, 96),
        seed_floor("messages held to their senders' devices", 170297, 96),
        seed_floor("reads asked", 23507, 96),
        seed_floor("reads recorded", 4664, 96),
        seed_floor("read indexes answered", 9643, 96),
        seed_floor("reads served", 3069, 96),
        seed_floor("writes published", 4350, 96),
        seed_floor("writes answered committed", 4350, 96),
        seed_floor("writes failed, another entry at their index", 188, 96),
        campaign_floor("writes failed, their term ended past the commit", 20, 96),
        seed_floor("operations left unknown by a restart", 2826, 96),
    ]
}

/// The group schedules on hostile networks: of a hundred deliveries, a quarter, a half and three
/// quarters lost, and as many repeated later (the quartiles of the range of rates), beside the
/// schedules' reordering, partitions and restarts. Every oracle holds, and both checkers agree.
#[test]
fn the_group_schedules_on_hostile_networks_keep_every_oracle_and_their_histories_are_linearizable()
{
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    for rate in [25, 50, 75] {
        let mix = Mix {
            leader_leaves: true,
            bursts: true,
            windows: true,
            lose: rate,
            repeat: rate,
            ..Mix::everything()
        };
        let mut totals = Totals::default();
        for seed in first..first + seeds {
            let mut group: Cluster<New> = Cluster::new(5, &[1, 2, 3], Settings::focal(), seed);
            group.stop_who_left = true;
            let run = judged(group, seed, steps, &mix, None);
            assert!(
                run.violation.is_none(),
                "{rate}, seed {seed}: {}",
                run.violation.unwrap()
            );
            assert!(run.settled, "{rate}, seed {seed}: the group did not settle");
            assert!(
                run.hot.is_none(),
                "{rate}, seed {seed}: {}",
                run.hot.unwrap_or_default()
            );
            agreed(seed, &run.events, &mut totals);
        }
        println!(
            "{rate} in a hundred lost and repeated: {} histories of {} events agreed, {} configurations searched; {}",
            totals.histories,
            totals.operations,
            totals.configurations,
            totals.tails()
        );
    }
}

/// `tests/fast.rs`'s schedules of the fast track, judged.
#[test]
fn the_fast_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for seed in first..first + seeds {
        let group: Cluster<New> = Cluster::new(5, &[1, 2, 3, 4, 5], Settings::fast(), seed);
        let run = judged(group, seed, steps, &mix, None);
        assert!(
            run.violation.is_none(),
            "seed {seed}: {}",
            run.violation.unwrap()
        );
        assert!(run.settled, "seed {seed}: the group did not settle");
        assert!(
            run.hot.is_none(),
            "seed {seed}: {}",
            run.hot.unwrap_or_default()
        );
        agreed(seed, &run.events, &mut totals);
        counters.merge(&run.counters);
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("fast", &counters, seeds, &fast_floors());
}

/// The floors of the fast schedules, from their counts over the default 96 seeds of 4,000 steps
/// (2026-10-05, this test at its commit).
fn fast_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 446, 96),
        seed_floor("entries committed", 31804, 96),
        seed_floor("log entries held", 30450, 96),
        seed_floor("leaderships held to the committed entries", 446, 96),
        seed_floor("messages held to their senders' devices", 238721, 96),
        seed_floor("reads asked", 7718, 96),
        seed_floor("reads recorded", 638, 96),
        seed_floor("read indexes answered", 1083, 96),
        seed_floor("reads served", 281, 96),
        seed_floor("writes published", 686, 96),
        seed_floor("writes answered committed", 686, 96),
        seed_floor("writes failed, another entry at their index", 763, 96),
        seed_floor("operations left unknown by a restart", 1282, 96),
        seed_floor("fast votes cast", 79004, 96),
        seed_floor("indexes a fast quorum chose", 681, 96),
    ]
}

/// `tests/pipeline.rs`'s schedules of members driven ahead of their persistence, judged: the
/// durability oracle holds every message to its sender's disk as the schedule leaves it.
#[test]
fn the_pipelined_schedules_keep_every_oracle_and_their_histories_are_linearizable() {
    let seeds = count("HYPER_RAFT_SEEDS", 24);
    let steps = count("HYPER_RAFT_STEPS", 2_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mut counters = Counters::new(PATHS);
    let mut totals = Totals::default();
    for (depth, apply_unpersisted) in [(2, false), (3, true)] {
        let settings = Settings {
            depth,
            in_place: true,
            apply_unpersisted,
            ..Settings::focal()
        };
        let mix = Mix {
            leader_leaves: true,
            bursts: true,
            windows: true,
            lag: 35,
            leader_durable: if apply_unpersisted { 10 } else { 100 },
            ..Mix::everything()
        };
        for seed in first..first + seeds {
            let mut group: Cluster<Lagged> = Cluster::new(5, &[1, 2, 3], settings, seed);
            group.stop_who_left = true;
            let run = judged(group, seed, steps, &mix, None);
            assert!(
                run.violation.is_none(),
                "seed {seed}: {}",
                run.violation.unwrap()
            );
            assert!(run.settled, "seed {seed}: the group did not settle");
            assert!(
                run.hot.is_none(),
                "seed {seed}: {}",
                run.hot.unwrap_or_default()
            );
            agreed(seed, &run.events, &mut totals);
            counters.merge(&run.counters);
        }
    }
    println!(
        "{} histories of {} events agreed, {} configurations searched; {}",
        totals.histories,
        totals.operations,
        totals.configurations,
        totals.tails()
    );
    floors("pipelined", &counters, 2 * seeds, &pipelined_floors());
}

/// The floors of the pipelined schedules, from their counts over the default 2 × 24 seeds of 2,000
/// steps (2026-10-05, this test at its commit, the core counting by the newest configuration in its
/// log: `docs/raft.md` §3.4). A write another entry displaced is now reached once a seed on
/// average, so it is held to being reached, not to its rate.
fn pipelined_floors() -> Vec<Floor> {
    vec![
        seed_floor("terms led", 142, 48),
        seed_floor("entries committed", 4581, 48),
        seed_floor("log entries held", 4433, 48),
        seed_floor("leaderships held to the committed entries", 142, 48),
        seed_floor("messages held to their senders' devices", 15113, 48),
        seed_floor("leaders' commits held to their voters' devices", 333, 48),
        seed_floor("entries applied held to their members' devices", 4581, 48),
        seed_floor("reads asked", 3844, 48),
        seed_floor("reads recorded", 392, 48),
        seed_floor("read indexes answered", 693, 48),
        seed_floor("reads served", 170, 48),
        seed_floor("writes published", 291, 48),
        seed_floor("writes answered committed", 291, 48),
        campaign_floor("writes failed, another entry at their index", 53, 48),
        campaign_floor("writes failed, their term ended past the commit", 27, 48),
        seed_floor("operations left unknown by a restart", 403, 48),
    ]
}

/// The first violation a planted defect brings, over the schedules of `seeds` from `first`: the
/// seed and what caught it.
fn caught<R: Core>(
    make: impl Fn(u64) -> Cluster<R>,
    first: u64,
    seeds: u64,
    steps: u64,
    mix: &Mix,
    mutant: Mutant,
) -> Option<(u64, Violation)> {
    // A campaign that looks for more seeds that catch it sets where to start and how many.
    let first = count("HYPER_RAFT_MUTANT_SEED", first);
    let seeds = count("HYPER_RAFT_MUTANT_SEEDS", seeds);
    for seed in first..first + seeds {
        let run = judged(make(seed), seed, steps, mix, Some(mutant));
        if let Some(violation) = run.violation {
            return Some((seed, violation));
        }
    }
    None
}

fn judged_settings(settings: Settings) -> Settings {
    Settings {
        judged: true,
        ..settings
    }
}

/// The thesis's Figure 3.7, played by three members: member 1 leads, and takes an entry at index 2
/// that reaches no one; member 3 leads by member 2 and takes index 2 itself, which reaches no one;
/// member 1 leads again by member 2, and gives member 2 its old entry at 2, one entry an append and
/// nothing of its own term, so a quorum holds the old entry; then member 3 leads again by member 2,
/// its log of a later last term than member 2's. The first violation the judge found.
fn figure_eight(mutant: Option<Mutant>) -> Option<Violation> {
    let settings = Settings {
        pre_vote: false,
        check_quorum: false,
        max_size_per_msg: 1,
        // A follower keeps nothing that arrives ahead of a hole, or member 2 would take in member
        // 1's entry of its term with its old one (R17's `Ahead::Kept`).
        refuse_ahead: true,
        judged: true,
        ..Settings::focal()
    };
    let all = [1, 2, 3];
    let mut group: Cluster<New> = Cluster::new(3, &all, settings, 0);
    group.plant(mutant);
    let mut judge = Judge::new(false, false, 1_000);
    elect(&mut group, &mut judge, 1, &all);
    replicate(&mut group, &mut judge, 1, |_, _| true);
    group.act_observed(&mut judge, &Op::Propose(1, b"of 1".to_vec()));
    route(&mut group, &mut judge, |_, _| false);
    group.stop(1);
    elect(&mut group, &mut judge, 3, &[2, 3]);
    group.act_observed(&mut judge, &Op::Propose(3, b"of 3".to_vec()));
    route(&mut group, &mut judge, |_, _| false);
    group.stop(3);
    group.act_observed(&mut judge, &Op::Restart(1));
    elect(&mut group, &mut judge, 1, &[1, 2]);
    // Between 1 and 2, every message but an append past index 2 to member 2 once it holds index
    // 2, which it would take: while it does not, it refuses one and is probed back to index 2.
    replicate(&mut group, &mut judge, 1, |group, m| {
        let past = m.entries.iter().any(|entry| entry.index > 2);
        between(&[1, 2], m) && !(past && group.disk(m.to).last_index() >= 2)
    });
    group.stop(1);
    group.act_observed(&mut judge, &Op::Restart(3));
    elect(&mut group, &mut judge, 3, &[2, 3]);
    route(&mut group, &mut judge, |_, m| m.from != 1 && m.to != 1);
    judge.violation
}

/// A leader that commits by counting an older term's replicas (thesis §3.6.2, Figure 3.7) commits
/// an entry a later leader replaces. Played as the figure plays it: with the rule taken out, Leader
/// Completeness catches the later leader lacking the entry committed; with it in, nothing is
/// committed that a later leader lacks. 2,096 random schedules of the group test (seeds 0 to 2,095)
/// never reached the figure's interleaving (2026-10-04), so it is played.
#[test]
fn a_commit_counted_from_an_older_term_is_caught() {
    assert_eq!(figure_eight(None), None);
    let violation = figure_eight(Some(Mutant::OlderTermCommit))
        .expect("the commit of an older term's entry went uncaught");
    println!("older-term commit: Figure 3.7: {violation}");
    assert!(
        matches!(
            violation,
            Violation::LeaderLacks { index: 2, .. } | Violation::TwoCommitted { index: 2, .. }
        ),
        "{violation}"
    );
}

/// A new leader that answers a read before it commits an entry of its term answers it below a
/// commit already made: read safety catches it.
#[test]
fn a_read_before_the_terms_first_commit_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        ..Mix::everything()
    };
    let make = |seed| {
        let mut group: Cluster<New> =
            Cluster::new(5, &[1, 2, 3], judged_settings(Settings::focal()), seed);
        group.stop_who_left = true;
        group
    };
    let (seed, violation) = caught(make, 0, 96, 4_000, &mix, Mutant::ReadBeforeFirstCommit)
        .expect("no schedule caught a read before the term's first commit");
    println!("read before the first commit: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::StaleRead { .. }),
        "seed {seed}: {violation}"
    );
}

/// A vote sent before it is durable: the durability oracle catches it (I1).
#[test]
fn a_vote_sent_before_it_is_durable_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        bursts: true,
        windows: true,
        lag: 35,
        ..Mix::everything()
    };
    let settings = judged_settings(Settings {
        depth: 2,
        in_place: true,
        ..Settings::focal()
    });
    let make = |seed| {
        let mut group: Cluster<Lagged> = Cluster::new(5, &[1, 2, 3], settings, seed);
        group.stop_who_left = true;
        group
    };
    let (seed, violation) = caught(make, 0, 24, 2_000, &mix, Mutant::VoteBeforeDurable)
        .expect("no schedule caught a vote sent before it was durable");
    println!("vote before durable: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::Durability { rule: Rule::I1, .. }),
        "seed {seed}: {violation}"
    );
}

/// The fast track without its first rule (`docs/raft.md` §3): a member holding the entry beside its
/// log counted before its log holds an entry of the leader's term. Its original seed, 9843, no
/// longer reaches the defect on today's core (the reads' rounds and R17 moved its schedule); a
/// campaign from seed 0 found seed 47,818 first (2026-10-04), and on the core that counts by the
/// newest configuration in its log (`docs/raft.md` §3.4) seed 67,842 (2026-10-05, 400,000 seeds
/// asked): a member voted two values at one index in one term, the fast agreement oracle's catch of
/// what the leader's wrong count led to; and on the core whose fast quorum counts holdings alone,
/// kept until a classic commit (`docs/raft.md` §3.5), seed 102,774 (2026-10-05, 400,000 seeds asked):
/// a later leader lacks an entry the fast track committed. The same seed without the defect keeps
/// every oracle, so the catch is the defect's.
#[test]
fn the_fast_track_without_its_first_rule_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    // The rules its original seed was found under (`tests/fast.rs`).
    let settings = Settings {
        round_each: true,
        max_inflight_bytes: u64::MAX,
        bare_answers: true,
        refuse_ahead: true,
        ..Settings::fast()
    };
    let make = |seed| Cluster::<New>::new(5, &[1, 2, 3, 4, 5], judged_settings(settings), seed);
    assert_eq!(
        judged(make(102_774), 102_774, 4_000, &mix, None).violation,
        None
    );
    let (seed, violation) = caught(make, 102_774, 1, 4_000, &mix, Mutant::FastBesideAnyTerm)
        .expect("seed 102,774 did not catch the fast track without its first rule");
    println!("fast track without its first rule: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::LeaderLacks { .. }),
        "{violation}"
    );
}

/// The fast track without its second rule: a fast quorum counted of the configuration in force
/// alone. Its original seed, 54104, no longer reaches the defect; a campaign from seed 0 found seed
/// 121,040 first (2026-10-04, on the core with both fixes of `docs/sim.md` §14.5 and the final
/// oracles): a later leader lacks an entry the fast track committed. The same seed without the
/// defect keeps every oracle, so the catch is the defect's. On the core that counts by the newest
/// configuration in its log (`docs/raft.md` §3.4) the campaign from seed 0 finds seed 1,483 first
/// (2026-10-05), the same violation, and on the core whose fast quorum counts holdings alone
/// (`docs/raft.md` §3.5) seed 8,867.
#[test]
fn the_fast_track_without_its_second_rule_is_caught() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let settings = Settings::fast();
    let make = |seed| Cluster::<New>::new(5, &[1, 2, 3, 4, 5], judged_settings(settings), seed);
    assert_eq!(
        judged(make(8_867), 8_867, 4_000, &mix, None).violation,
        None
    );
    let (seed, violation) = caught(make, 8_867, 1, 4_000, &mix, Mutant::FastAnyConfiguration)
        .expect("seed 8,867 did not catch the fast track without its second rule");
    println!("fast track without its second rule: seed {seed}: {violation}");
    assert!(
        matches!(violation, Violation::LeaderLacks { .. }),
        "{violation}"
    );
}

/// A fast-track leader serves reads only once it commits the blank entry it began its term with
/// (`Raft::commit_to_current_term`). The judge found the cause at seed 15,761 of the fast schedules
/// (2026-10-04): index 9 committed by a fast quorum in term 1; member 4, elected in term 2 at a
/// commit of 2, recovered indexes 3 to 9 under its own term and appended its blank entry after them;
/// a read asked of it was served once index 5 committed, an entry of its term, and so answered
/// below the commit of 9 made before it was asked. Read safety catches the schedule if the rule is
/// an entry of the term rather than the term's blank entry.
#[test]
fn a_fast_leaders_reads_wait_for_the_entry_it_began_its_term_with() {
    let mix = Mix {
        leader_leaves: true,
        fast: 60,
        ..Mix::everything()
    };
    let group = Cluster::<New>::new(
        5,
        &[1, 2, 3, 4, 5],
        judged_settings(Settings::fast()),
        15_761,
    );
    let run = judged(group, 15_761, 4_000, &mix, None);
    assert_eq!(run.violation, None);
}
