//! This core and `raft-rs` on one schedule (27 §6, stage D): the same
//! members, the same messages delivered, lost, repeated and held back in
//! the same order, the same stops and compactions. After every step both
//! say what they persisted, sent, committed and answered, and what each
//! member knows of the others; all of it is equal, or the run fails naming
//! its seed and its step.
//!
//! The two differ by decision in four places a schedule reaches. A change
//! of the configuration counts here from the moment a log holds its entry,
//! and there from the moment its owner applies it (`docs/raft.md` §3.4:
//! raft-rs's rule elects two leaders of a term, `docs/models/Reconfig.tla`):
//! a run that comes to a change entering a log ends there, and what this
//! core does from there on is tested by itself (`group.rs`, `check.rs`,
//! `pipeline.rs` and hyper-check's model of the rule). A member told by
//! its leader to campaign while it asks whether it could be elected
//! campaigns here and ignores it there, where the leader then waits an
//! election timeout for nothing: the schedule loses that message for both,
//! and the run goes on. Without check-quorum and pre-vote, a member answers
//! an append or a heartbeat of an older term here and drops it there: the
//! schedule loses that message for both too. An answer to a read round sent
//! before the read it names was asked again under the same context confirms
//! nothing here and the read and every one before it there: the schedule
//! loses that message for both as well.
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

use std::collections::VecDeque;

use hyper_raft::StateRole;
use hyper_raft::proto::{Message, MessageType};
use support::{Cluster, Mix, New, Old, Op, Replica, Report, Seeded, Settings};

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

/// What the schedules came to: a comparison proves what it reached.
#[derive(Debug, Default)]
struct Reached {
    /// Members told to campaign while they asked whether they could.
    told_while_asking: u64,
    /// Appends and heartbeats of an older term than their recipients', without check-quorum or
    /// pre-vote: this core answers them, raft-rs does not, and the comparison loses them for both.
    stale_leaders: u64,
    /// Reads that would have reached a leader before its term's first commit, lost for both.
    held_reads: u64,
    /// Answers to a round sent before the read they name was asked again, lost for both.
    older_rounds: u64,
    terms: u64,
    committed: u64,
    changes: u64,
    refused_changes: u64,
    /// Runs that came to a change entering a log, where the cores part by decision.
    changes_written: u64,
    snapshots: u64,
    reads: u64,
    rejections: u64,
    transfers: u64,
    votes_refused: u64,
    restarts: u64,
    refusals: u64,
}
impl Reached {
    fn note(&mut self, op: &Op, reports: &[Report]) {
        if matches!(op, Op::Restart(_)) {
            self.restarts += 1;
        }
        for report in reports {
            let output = &report.output;
            self.committed += output.committed.len() as u64;
            self.changes += output.confs.len() as u64;
            self.refused_changes += output.refused.len() as u64;
            self.snapshots += output.snapshots.len() as u64;
            self.reads += output.reads.len() as u64;
            self.refusals += u64::from(report.accepted == Some(false));
            if output.committed.iter().any(|said| {
                said.3.is_empty() && said.2 == hyper_raft::proto::EntryType::EntryNormal
            }) {
                // A leader's first entry: a term that was led.
                self.terms += 1;
            }
            for message in &output.messages {
                match message.msg_type {
                    MessageType::MsgAppendResponse if message.reject => self.rejections += 1,
                    MessageType::MsgTimeoutNow => self.transfers += 1,
                    MessageType::MsgRequestVoteResponse
                    | MessageType::MsgRequestPreVoteResponse
                        if message.reject =>
                    {
                        self.votes_refused += 1
                    }
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum End {
    /// Every step of the schedule was taken and compared.
    Ran,
    /// A change of the configuration entered a log at this step: the cores count it differently
    /// from here by decision.
    Changed(u64),
}

/// Whether `op` tells a member to campaign that asks whether it could.
fn tells_one_that_asks(group: &Cluster<New>, op: &Op) -> bool {
    let Op::Deliver {
        at, lose: false, ..
    } = op
    else {
        return false;
    };
    let Some(message) = group.net.get(*at) else {
        return false;
    };
    message.msg_type == MessageType::MsgTimeoutNow
        && group.peek(message.to).is_some_and(|member| {
            let view = member.view();
            view.role == 3 && view.term == message.term
        })
}

/// Whether `op` delivers an append or a heartbeat of an older term than its recipient's to a group
/// without check-quorum or pre-vote: this core answers it, so that the leader of the older term
/// learns the newer one (thesis Figure 3.1), and raft-rs leaves that to vote requests, which never
/// reach a leader the newer term's configuration names no voter (`docs/sim.md` §15.9).
fn stale_leader(group: &Cluster<New>, op: &Op) -> bool {
    let Op::Deliver {
        at, lose: false, ..
    } = op
    else {
        return false;
    };
    let Some(message) = group.net.get(*at) else {
        return false;
    };
    !group.settings.check_quorum
        && !group.settings.pre_vote
        && matches!(
            message.msg_type,
            MessageType::MsgAppend | MessageType::MsgHeartbeat
        )
        && group
            .peek(message.to)
            .is_some_and(|member| member.view().term > message.term)
}

/// Whether `op` delivers to a leader an answer to a round sent before the read it names was asked
/// again under the same context: this core takes it to confirm nothing, as it says nothing of who
/// led when the read was asked; raft-rs takes it to confirm the read and every one before it, and
/// the comparison loses it for both (`docs/raft.md` §3.3).
fn answers_an_older_round(group: &Cluster<New>, op: &Op) -> bool {
    let Op::Deliver {
        at, lose: false, ..
    } = op
    else {
        return false;
    };
    let Some(message) = group.net.get(*at) else {
        return false;
    };
    if message.msg_type != MessageType::MsgHeartbeatResponse {
        return false;
    }
    let Some((context, round)) = hyper_raft::read::ReadOnly::of_round(&message.context) else {
        return false;
    };
    group.peek(message.to).is_some_and(|leader| {
        leader.raw.raft.state() == StateRole::Leader
            && leader.raw.raft.term() == message.term
            && leader.raw.raft.round_confirms(context, round) == Some(false)
    })
}

/// Whether `op` asks a read of a leader that has not committed an entry of its term, or delivers
/// to one a read a follower forwarded: this core holds the read until that commit (the thesis's
/// §6.4 step 1), raft-rs drops it, and the comparison loses it for both (`docs/raft.md` §3.3).
fn reads_before_first_commit(group: &Cluster<New>, op: &Op) -> bool {
    let holds = |id: u64| {
        group.peek(id).is_some_and(|member| {
            member.raw.raft.state() == StateRole::Leader
                && !member.raw.raft.commit_to_current_term()
        })
    };
    match op {
        Op::Read(id, _) | Op::Reads(id, _) => holds(*id),
        Op::Deliver {
            at, lose: false, ..
        } => group.net.get(*at).is_some_and(|message| {
            message.msg_type == MessageType::MsgReadIndex && holds(message.to)
        }),
        _ => false,
    }
}

fn brief(message: &hyper_raft::proto::Message) -> String {
    let entries: Vec<String> = message
        .entries
        .iter()
        .map(|entry| {
            format!(
                "{}@{}/{:?}:{}b",
                entry.index,
                entry.term,
                entry.entry_type,
                entry.data.len()
            )
        })
        .collect();
    format!(
        "{:?} {}->{} term {} log {}@{} commit {}@{} reject {} hint {} request {} context {:?} priority {} snapshot {:?} [{}]",
        message.msg_type,
        message.from,
        message.to,
        message.term,
        message.index,
        message.log_term,
        message.commit,
        message.commit_term,
        message.reject,
        message.reject_hint,
        message.request_snapshot,
        String::from_utf8_lossy(&message.context),
        message.priority,
        message
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.as_ref())
            .map(|metadata| (metadata.index, metadata.term)),
        entries.join(" "),
    )
}
/// Where two reports differ, and no more.
fn explain(old: &[Report], new: &[Report]) -> String {
    let mut lines = Vec::new();
    if old.len() != new.len() {
        lines.push(format!("reports: {} and {}", old.len(), new.len()));
    }
    for (old, new) in old.iter().zip(new) {
        let member = new.member;
        macro_rules! differ {
            ($what:literal, $old:expr, $new:expr) => {
                if $old != $new {
                    lines.push(format!(
                        "member {member} {}:\n  raft-rs:    {:?}\n  hyper-raft: {:?}",
                        $what, $old, $new
                    ));
                }
            };
        }
        differ!("member", old.member, new.member);
        differ!("accepted", old.accepted, new.accepted);
        differ!(
            "hard states",
            old.output.hard_states,
            new.output.hard_states
        );
        differ!(
            "persisted",
            old.output
                .persisted
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>(),
            new.output
                .persisted
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>()
        );
        differ!("snapshots", old.output.snapshots, new.output.snapshots);
        differ!(
            "committed",
            old.output
                .committed
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>(),
            new.output
                .committed
                .iter()
                .map(|said| (said.0, said.1, said.2, said.3.len()))
                .collect::<Vec<_>>()
        );
        differ!("reads", old.output.reads, new.output.reads);
        differ!("configurations", old.output.confs, new.output.confs);
        differ!("refused changes", old.output.refused, new.output.refused);
        if old.output.messages != new.output.messages {
            lines.push(format!(
                "member {member} messages:\n  raft-rs:\n{}\n  hyper-raft:\n{}",
                old.output
                    .messages
                    .iter()
                    .map(|m| format!("    {}", brief(m)))
                    .collect::<Vec<_>>()
                    .join("\n"),
                new.output
                    .messages
                    .iter()
                    .map(|m| format!("    {}", brief(m)))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }
        differ!("view", old.view, new.view);
    }
    lines.join("\n")
}

/// This core's read rounds carry their number after the read's context (`ReadOnly::round_context`),
/// raft-rs's the context alone: a decided difference the comparison takes out.
fn roundless(reports: &mut [Report]) {
    for report in reports {
        roundless_messages(&mut report.output.messages);
    }
}
fn roundless_messages(messages: &mut [hyper_raft::proto::Message]) {
    for message in messages.iter_mut().filter(|message| {
        matches!(
            message.msg_type,
            MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse
        )
    }) {
        if let Some((context, _)) = hyper_raft::read::ReadOnly::of_round(&message.context) {
            message.context = context.to_vec();
        }
    }
}

fn timeless(reports: &mut [Report]) {
    for report in reports {
        report.view.timeout = 0;
    }
}

/// This core's messages as raft-rs's say them: raft-rs states no classic commit
/// (`Message::classic`), which only the fast track reads, and none of these schedules runs it.
fn classless(reports: &mut [Report], net: &mut [Message]) {
    for report in reports {
        for message in &mut report.output.messages {
            message.classic = None;
        }
    }
    for message in net {
        message.classic = None;
    }
}

fn run(seed: u64, steps: u64, settings: Settings, mix: Mix, reached: &mut Reached) -> End {
    let voters = [1, 2, 3];
    let mut old: Cluster<Old> = Cluster::new(5, &voters, settings, seed);
    let mut new: Cluster<New> = Cluster::new(5, &voters, settings, seed);
    let mut rng = Seeded(seed);
    let mut trace: VecDeque<String> = VecDeque::new();
    let agree = |old: &mut Cluster<Old>, new: &Cluster<New>| {
        for id in new.up() {
            let timeout = new.peek(id).unwrap().view().timeout;
            old.node(id).unwrap().set_timeout(timeout);
        }
    };
    agree(&mut old, &new);
    for id in new.ids() {
        assert_eq!(
            old.peek(id).unwrap().view(),
            new.peek(id).unwrap().view(),
            "seed {seed}: member {id} as opened"
        );
    }
    for step in 0..steps {
        let op = new.choose(&mut rng, &mix);
        trace.push_back(format!("{step}: {op:?}"));
        if trace.len() > 24 {
            trace.pop_front();
        }
        let op = match op {
            Op::Deliver { at, keep, .. } if tells_one_that_asks(&new, &op) => {
                reached.told_while_asking += 1;
                Op::Deliver {
                    at,
                    keep,
                    lose: true,
                }
            }
            Op::Deliver { at, keep, .. } if stale_leader(&new, &op) => {
                reached.stale_leaders += 1;
                Op::Deliver {
                    at,
                    keep,
                    lose: true,
                }
            }
            Op::Deliver { at, keep, .. } if answers_an_older_round(&new, &op) => {
                reached.older_rounds += 1;
                Op::Deliver {
                    at,
                    keep,
                    lose: true,
                }
            }
            Op::Deliver { at, keep, .. } if reads_before_first_commit(&new, &op) => {
                reached.held_reads += 1;
                Op::Deliver {
                    at,
                    keep,
                    lose: true,
                }
            }
            Op::Read(..) | Op::Reads(..) if reads_before_first_commit(&new, &op) => {
                reached.held_reads += 1;
                if let Some(last) = trace.back_mut() {
                    last.push_str(" (lost for both: a read before the term's first commit)");
                }
                continue;
            }
            op => op,
        };
        let mut said_old = old.act(&op);
        let mut said_new = new.act(&op);
        timeless(&mut said_old);
        timeless(&mut said_new);
        classless(&mut said_new, &mut new.net);
        let changes = |reports: &[Report]| {
            reports.iter().any(|report| {
                report
                    .output
                    .persisted
                    .iter()
                    .any(|said| said.2 != hyper_raft::proto::EntryType::EntryNormal)
            })
        };
        if changes(&said_new) || changes(&said_old) {
            reached.changes_written += 1;
            return End::Changed(step);
        }
        roundless(&mut said_new);
        if said_old != said_new {
            let steps: Vec<&String> = trace.iter().collect();
            panic!(
                "seed {seed}, step {step}: the cores differ\n\
                 the last steps: {steps:#?}\n{}",
                explain(&said_old, &said_new)
            );
        }
        let mut net = new.net.clone();
        roundless_messages(&mut net);
        assert!(
            old.net.iter().eq(net.iter()),
            "seed {seed}, step {step}: the networks"
        );
        reached.note(&op, &said_new);
        agree(&mut old, &new);
    }
    End::Ran
}

/// This core alone on a schedule, held to its byte bounds as it measures them, after every step.
///
/// Where a bound counts bytes, the two cores count different ones: raft-rs an entry's
/// protocol-buffers length, this core its length in its own format (`docs/raft.md` §3.1). A page,
/// a ready's committed entries and the uncommitted bound then cut at different entries by design,
/// and raft-rs is no oracle for where; each core is held to its own bound instead.
fn alone(seed: u64, steps: u64, settings: Settings, mix: Mix, reached: &mut Reached) {
    let voters = [1, 2, 3];
    let mut new: Cluster<New> = Cluster::new(5, &voters, settings, seed);
    let mut rng = Seeded(seed);
    let mut largest = 0usize;
    for step in 0..steps {
        let op = new.choose(&mut rng, &mix);
        let reports = new.act(&op);
        for report in &reports {
            for said in &report.output.persisted {
                largest = largest.max(said.3.len());
            }
            for message in &report.output.messages {
                if message.msg_type == MessageType::MsgAppend && message.entries.len() > 1 {
                    let bytes: u64 = message
                        .entries
                        .iter()
                        .map(hyper_raft::proto::encoded_bytes)
                        .sum();
                    assert!(
                        bytes <= settings.max_size_per_msg,
                        "seed {seed}, step {step}: an append of {bytes} bytes past its page of {}",
                        settings.max_size_per_msg
                    );
                }
            }
            // At least one proposal is admitted with nothing uncommitted, whatever its size.
            let bound = settings.max_uncommitted_size as usize + largest;
            assert!(
                report.view.uncommitted <= bound,
                "seed {seed}, step {step}: {} bytes uncommitted past {bound}",
                report.view.uncommitted
            );
        }
        reached.note(&op, &reports);
    }
}

/// [`alone`] over the seeds, with the coverage a campaign must reach.
fn bounded(name: &str, settings: Settings, mix: Mix) -> Reached {
    let mut reached = Reached::default();
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    for seed in first..first + seeds {
        alone(seed, steps, settings, mix, &mut reached);
    }
    println!("{name}: {seeds} schedules of {steps} steps, held to the bounds: {reached:?}");
    assert!(
        reached.terms > 0 && reached.committed > 0,
        "{name}: {reached:?}"
    );
    reached
}

/// The cores compared over the seeds. The schedules change no configuration: from a change's
/// entry on the cores differ by decision ([`parting`]).
fn campaign(name: &str, settings: Settings, mix: Mix) -> Reached {
    let mix = Mix {
        changes: false,
        ..mix
    };
    let mut reached = Reached::default();
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mut ran = 0u64;
    let mut changed = 0u64;
    let mut compared = 0u64;
    for seed in first..first + seeds {
        match run(seed, steps, settings, mix, &mut reached) {
            End::Ran => {
                ran += 1;
                compared += steps;
            }
            End::Changed(step) => {
                changed += 1;
                compared += step;
            }
        }
    }
    println!(
        "{name}: {seeds} schedules, {compared} steps compared; {ran} ran to their end, \
         {changed} ended where a change entered a log"
    );
    // Schedules that elect and commit nothing compare nothing: each must be reached; how often
    // is reported, not judged against a picked share.
    assert_eq!(changed, 0, "{name}: a schedule changed the configuration");
    println!("{name}: {reached:?}");
    assert!(
        reached.terms > 0 && reached.committed > 0,
        "{name}: {reached:?}"
    );
    assert!(
        reached.reads > 0 && reached.transfers > 0,
        "{name}: {reached:?}"
    );
    assert!(
        reached.votes_refused > 0 && reached.refusals > 0,
        "{name}: {reached:?}"
    );
    reached
}

/// The schedules with changes of the configuration: the cores agree on every step until a
/// change's entry enters a log, where they part by decision (the module's note).
fn parting(name: &str, settings: Settings) -> Reached {
    let mut reached = Reached::default();
    let seeds = count("HYPER_RAFT_SEEDS", 96);
    let steps = count("HYPER_RAFT_STEPS", 4_000);
    let first = count("HYPER_RAFT_SEED", 0);
    let mut compared = 0u64;
    for seed in first..first + seeds {
        compared += match run(seed, steps, settings, Mix::everything(), &mut reached) {
            End::Ran => steps,
            End::Changed(step) => step,
        };
    }
    println!("{name}: {seeds} schedules, {compared} steps compared before a change: {reached:?}");
    assert!(reached.changes_written > 0, "{name}: {reached:?}");
    reached
}

#[test]
fn the_cores_agree_until_a_change_enters_a_log() {
    let reached = parting("shell", Settings::shell());
    assert!(reached.terms > 0 && reached.committed > 0, "{reached:?}");
}

/// Every campaign again with this core's members given their `Ready`s in
/// place (`RawNode::ready_in_place`): what they persist and apply is read
/// where it is, and every step still says what `raft-rs` says.
#[test]
fn the_cores_agree_with_readies_given_in_place() {
    let place = |settings: Settings| Settings {
        in_place: true,
        ..settings
    };
    let shell = campaign(
        "shell in place",
        place(Settings::shell()),
        Mix::everything(),
    );
    assert!(shell.snapshots > 0 && shell.restarts > 0);
    for (name, settings, mix) in [
        (
            "plain in place",
            Settings {
                check_quorum: false,
                pre_vote: false,
                ..Settings::shell()
            },
            Mix::everything(),
        ),
        (
            "narrow in place",
            Settings {
                max_inflight_msgs: 2,
                max_size_per_msg: 1,
                max_committed_size_per_ready: 64,
                ..Settings::shell()
            },
            Mix::everything(),
        ),
        (
            "paged in place",
            Settings {
                max_size_per_msg: 100,
                max_committed_size_per_ready: 100,
                max_inflight_msgs: 4,
                ..Settings::shell()
            },
            Mix::everything(),
        ),
        (
            "whole in place",
            Settings::shell(),
            Mix {
                restarts: false,
                partitions: false,
                lose: 0,
                repeat: 0,
                ..Mix::everything()
            },
        ),
        (
            "uncommitted in place",
            Settings {
                max_uncommitted_size: 96,
                max_size_per_msg: 64,
                ..Settings::shell()
            },
            Mix {
                changes: false,
                ..Mix::everything()
            },
        ),
    ] {
        let reached = if settings.max_size_per_msg == 100 || settings.max_size_per_msg == 64 {
            bounded(name, place(settings), mix)
        } else {
            campaign(name, place(settings), mix)
        };
        assert!(reached.committed > 0);
    }
}

#[test]
fn the_cores_agree_as_the_shell_sets_them() {
    let reached = campaign("shell", Settings::shell(), Mix::everything());
    assert!(
        reached.snapshots > 0 && reached.rejections > 0,
        "{reached:?}"
    );
    assert!(reached.restarts > 0, "{reached:?}");
}

#[test]
fn the_cores_agree_without_pre_vote_and_check_quorum() {
    let settings = Settings {
        check_quorum: false,
        pre_vote: false,
        ..Settings::shell()
    };
    let reached = campaign("plain", settings, Mix::everything());
    assert!(reached.committed > 0);
    // The third decided difference is reached, or the comparison says nothing of it.
    assert!(reached.stale_leaders > 0, "{reached:?}");
}

#[test]
fn the_cores_agree_with_a_window_of_two_and_a_message_of_one_entry() {
    let settings = Settings {
        max_inflight_msgs: 2,
        max_size_per_msg: 1,
        max_committed_size_per_ready: 64,
        ..Settings::shell()
    };
    let reached = campaign("narrow", settings, Mix::everything());
    assert!(reached.committed > 0);
    // The fourth decided difference is reached, or the comparison says nothing of it.
    assert!(reached.older_rounds > 0, "{reached:?}");
}

/// Pages of a hundred bytes and a window of four: a lagging member is
/// caught up a page at a time, each sized before it is copied. The page
/// counts bytes, so this core is held to it alone ([`alone`]); every page
/// it sends is checked to hold no spare room (`support::New::drain`).
#[test]
fn pages_of_a_hundred_bytes_and_a_window_of_four_hold_their_bound() {
    let settings = Settings {
        max_size_per_msg: 100,
        max_committed_size_per_ready: 100,
        max_inflight_msgs: 4,
        ..Settings::shell()
    };
    let reached = bounded("paged", settings, Mix::everything());
    assert!(reached.committed > 0 && reached.rejections > 0);
}

#[test]
fn what_a_leader_may_hold_uncommitted_holds_its_bound() {
    // A change that is refused for its size leaves `raft-rs` believing one
    // is pending; this core does not. No change is proposed here. The page
    // counts bytes, so this core is held to its bounds alone ([`alone`]).
    let settings = Settings {
        max_uncommitted_size: 96,
        max_size_per_msg: 64,
        ..Settings::shell()
    };
    let mix = Mix {
        changes: false,
        ..Mix::everything()
    };
    let reached = bounded("uncommitted", settings, mix);
    assert!(reached.committed > 0);
}

#[test]
fn the_cores_agree_on_a_network_that_loses_nothing() {
    let mix = Mix {
        restarts: false,
        partitions: false,
        lose: 0,
        repeat: 0,
        ..Mix::everything()
    };
    let reached = campaign("whole", Settings::shell(), mix);
    assert!(reached.committed > 0);
}
