//! A node's record of what it fed its liveness stream and what the stream told it, kept by the
//! simulation's nodes (`sim.rs`) and the real processes' members (`processes.rs`), and the trace
//! that checks every suspicion in the records against the detector's rule, exactly
//! (`docs/timing.md` §2.8, "Tests").
//!
//! NFD-E suspects a peer at the freshness point `τ_{h+1}` of the latest heartbeat `h` taken from it
//! when no later heartbeat has been received by then (Chen, Toueg and Aguilera 2002, §5), received
//! meaning stamped by the kernel. Each suspicion is traced to that rule from the records alone:
//! - its heartbeat `h` is the latest the node took from the peer before the call that told it, or
//!   the one that call took where that heartbeat came at or past its own successor's freshness
//!   point, a configuration the take made having restated it;
//! - its freshness point is the one the stream held the peer trusted to before that call; where it
//!   held none (no margin judged the peer yet, or it held the peer suspected and the owner had been
//!   told since of the peer's restart), the point the margin given in that call set;
//! - the call that told it, a poll or a heartbeat's arrival, came at or past the point and noticed
//!   it then;
//! - no heartbeat of the peer's taken after `h` was stamped before the point.
//!
//! Every heartbeat that came at or past the point its peer was held trusted to, and every poll at or
//! past it, told the suspicion at that point: none was missed. What became of the heartbeat the
//! point awaited is reported, not judged: taken late, and how late, a slot its sender skipped, sent
//! and refused or lost, a new run's, or none taken. And every count a node's report states of a
//! peer is its record's: the suspicions told, the heartbeats taken and those refused for their
//! proof, the heartbeats sent and the slots skipped between them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use hyper_liveness::{Change, Heartbeat, Last, PairReport, PeerId, Refusal};
use hyper_timing::Trust;

/// A heartbeat fed to a node's stream: its run and number, the kernel's stamp on the node's clock,
/// and when it was due and sent on the peer's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Beat {
    pub(crate) run: u64,
    pub(crate) seq: u64,
    pub(crate) stamp: u64,
    pub(crate) due: u64,
    pub(crate) sent: u64,
}

impl Beat {
    /// The heartbeat `message` carries, stamped `stamp`: `None` for one that does not decode.
    pub(crate) fn of(message: &[u8], stamp: u64) -> Option<Self> {
        let beat = Heartbeat::decode(message).ok()?;
        Some(Self {
            run: beat.run,
            seq: beat.seq,
            stamp,
            due: beat.sent_ns.saturating_sub(beat.late_ns),
            sent: beat.sent_ns,
        })
    }

    /// The heartbeat as a suspicion states the latest taken.
    fn last(self) -> Last {
        Last {
            seq: self.seq,
            arrival_ns: self.stamp,
            due_ns: self.due,
            sent_ns: self.sent,
        }
    }
}

/// One entry of a node's record, in the order the node made it, times on its own clock.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Entry {
    /// The node's stream began: its first, or a new process's after a restart, which holds nothing
    /// of the one before.
    Began,
    /// A heartbeat fed to the stream from `peer`, the refusal if it was not taken, and the trust
    /// the stream holds of the peer after.
    Fed {
        peer: PeerId,
        beat: Beat,
        refused: Option<Refusal>,
        holds: Option<Trust>,
    },
    /// A poll at `now` that told a change, moved a trust, or came at or past a freshness point a
    /// peer was held trusted to; any other poll is not kept.
    Polled { now: u64 },
    /// A change the call before told, in the order told.
    Told(Change),
    /// The trust the stream holds of `peer` after the poll before, where the poll moved it (`None`
    /// once the pair is gone).
    Holds { peer: PeerId, trust: Option<Trust> },
    /// A heartbeat the node sent `peer`: its run and number.
    Sent { peer: PeerId, run: u64, seq: u64 },
}

/// What a record counts of a peer, as the node's report states it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counted {
    pub(crate) suspicions: u64,
    pub(crate) taken: u64,
    pub(crate) unproven: u64,
    pub(crate) sent: u64,
    pub(crate) skipped: u64,
}

/// A node's record: its entries, what it counts of each peer, and the trust it last recorded of
/// each, from which a poll's moves are told.
#[derive(Default)]
pub(crate) struct Record {
    pub(crate) entries: Vec<Entry>,
    counted: BTreeMap<PeerId, Counted>,
    /// The latest number sent to each peer: the slots after it the next skips.
    sent: BTreeMap<PeerId, u64>,
    stated: BTreeMap<PeerId, Option<Trust>>,
}

impl Record {
    /// An entry made, counted. A reader of a record kept elsewhere (the processes' supervisor)
    /// pushes each entry it reads.
    pub(crate) fn push(&mut self, entry: Entry) {
        match entry {
            Entry::Began => {
                // A new stream counts from nothing.
                self.counted.clear();
                self.sent.clear();
                self.stated.clear();
            }
            Entry::Fed { peer, refused, .. } => {
                let counted = self.counted.entry(peer).or_default();
                match refused {
                    None => counted.taken += 1,
                    Some(Refusal::Unproven) => counted.unproven += 1,
                    Some(_) => {}
                }
            }
            Entry::Told(Change::Suspected(suspicion)) => {
                self.counted.entry(suspicion.peer).or_default().suspicions += 1;
            }
            Entry::Sent { peer, seq, .. } => {
                let counted = self.counted.entry(peer).or_default();
                counted.sent += 1;
                if let Some(previous) = self.sent.insert(peer, seq) {
                    counted.skipped += seq.saturating_sub(previous).saturating_sub(1);
                }
            }
            Entry::Polled { .. } | Entry::Told(_) | Entry::Holds { .. } => {}
        }
        self.entries.push(entry);
    }

    /// The node's stream began.
    pub(crate) fn began(&mut self) {
        self.push(Entry::Began);
    }

    /// A heartbeat fed from `peer`: what the call made of it, what it told, and the trust the
    /// stream holds of the peer after.
    pub(crate) fn fed(
        &mut self,
        peer: PeerId,
        beat: Beat,
        outcome: Result<(), Refusal>,
        told: &[Change],
        holds: Option<Trust>,
    ) {
        self.push(Entry::Fed {
            peer,
            beat,
            refused: outcome.err(),
            holds,
        });
        for change in told {
            self.push(Entry::Told(*change));
        }
        self.stated.insert(peer, holds);
    }

    /// A poll at `now` that told `told`, the stream holding `trusts` of its peers after: kept if it
    /// told a change, moved a trust, or came at or past a point a peer was held trusted to.
    pub(crate) fn polled(
        &mut self,
        now: u64,
        told: &[Change],
        trusts: impl IntoIterator<Item = (PeerId, Option<Trust>)>,
    ) {
        let passed = self
            .stated
            .values()
            .any(|trust| matches!(trust, Some(Trust::Trusted { until_ns }) if *until_ns <= now));
        let moved: Vec<(PeerId, Option<Trust>)> = trusts
            .into_iter()
            .filter(|(peer, trust)| self.stated.get(peer) != Some(trust))
            .collect();
        if told.is_empty() && moved.is_empty() && !passed {
            return;
        }
        self.push(Entry::Polled { now });
        for change in told {
            self.push(Entry::Told(*change));
        }
        for (peer, trust) in moved {
            self.stated.insert(peer, trust);
            self.push(Entry::Holds { peer, trust });
        }
    }

    /// A heartbeat sent to `peer`.
    pub(crate) fn sent(&mut self, peer: PeerId, run: u64, seq: u64) {
        self.push(Entry::Sent { peer, run, seq });
    }

    /// What `report` states of `peer` that the record does not count: `None` where every count is
    /// the record's.
    pub(crate) fn differs(&self, peer: PeerId, report: &PairReport) -> Option<String> {
        let counted = self.counted.get(&peer).copied().unwrap_or_default();
        let reported = Counted {
            suspicions: report.suspicions,
            taken: report.taken,
            unproven: report.unproven,
            sent: report.sent,
            skipped: report.skipped,
        };
        (counted != reported).then(|| {
            format!("the report of {peer} states {reported:?}, and the record counts {counted:?}")
        })
    }
}

/// What tracing every suspicion found.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Traced {
    pub(crate) suspicions: u64,
    /// Told at a poll, at a heartbeat's arrival before it was taken, and at a heartbeat taken at or
    /// past its own successor's freshness point.
    pub(crate) at_poll: u64,
    pub(crate) at_arrival: u64,
    pub(crate) at_take: u64,
    /// Judged by a point the margin given in the call that told it set, the stream holding the
    /// peer trusted to none before.
    pub(crate) first_margin: u64,
    /// Of a peer no heartbeat had been taken from in its run.
    pub(crate) unheard: u64,
    /// The heartbeat the point awaited taken late; and the latest any heartbeat taken next came
    /// past its point.
    pub(crate) late: u64,
    pub(crate) latest: Duration,
    /// The heartbeat taken next a later slot's: the awaited slot never sent, its sender behind its
    /// schedule; sent and refused; sent and never fed.
    pub(crate) skipped: u64,
    pub(crate) refused: u64,
    pub(crate) lost: u64,
    /// The heartbeat taken next a new run's: the peer restarted.
    pub(crate) restarted: u64,
    /// No heartbeat taken after: none sent that the peer's record holds (killed, stalled, or
    /// stopped to the end), or sent and none taken.
    pub(crate) silent: u64,
    pub(crate) untaken: u64,
    /// Heartbeats and polls at or past the point their peer was held trusted to: each told the
    /// suspicion at that point.
    pub(crate) passed: u64,
}

impl fmt::Display for Traced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} suspicions traced, told at a poll {}, at a heartbeat's arrival {}, at a heartbeat \
             taken past its successor's point {}, by a point the call's own margin set {}, of a \
             peer unheard in its run {}; the heartbeat awaited taken late {}, its slot skipped by \
             its sender {}, refused {}, lost {}, a new run's {}, none taken with none sent {} or \
             with some sent {}; the latest a heartbeat taken next came past its point {:?}; {} \
             heartbeats and polls at or past a point a peer was held trusted to, each telling its \
             suspicion",
            self.suspicions,
            self.at_poll,
            self.at_arrival,
            self.at_take,
            self.first_margin,
            self.unheard,
            self.late,
            self.skipped,
            self.refused,
            self.lost,
            self.restarted,
            self.silent,
            self.untaken,
            self.latest,
            self.passed,
        )
    }
}

impl std::ops::AddAssign for Traced {
    fn add_assign(&mut self, other: Self) {
        self.suspicions += other.suspicions;
        self.at_poll += other.at_poll;
        self.at_arrival += other.at_arrival;
        self.at_take += other.at_take;
        self.first_margin += other.first_margin;
        self.unheard += other.unheard;
        self.late += other.late;
        self.latest = self.latest.max(other.latest);
        self.skipped += other.skipped;
        self.refused += other.refused;
        self.lost += other.lost;
        self.restarted += other.restarted;
        self.silent += other.silent;
        self.untaken += other.untaken;
        self.passed += other.passed;
    }
}

/// What a node held of a peer as its record went: the peer's latest run begun, the latest
/// heartbeat taken in it, and the trust the stream last stated.
#[derive(Clone, Copy, Debug, Default)]
struct Held {
    run: Option<u64>,
    last: Option<Last>,
    holds: Option<Trust>,
}

/// The call the told changes after it came from, and where it is in the record.
#[derive(Clone, Copy, Debug)]
enum Call {
    Fed {
        place: usize,
        peer: PeerId,
        beat: Beat,
        refused: Option<Refusal>,
        holds: Option<Trust>,
    },
    Polled {
        place: usize,
        now: u64,
    },
}

/// Whether the stream judged a heartbeat it refused at its arrival: one refused before its pair
/// took it up (from itself, an unknown peer, or undecodable) it did not.
fn judged(refused: Option<Refusal>) -> bool {
    refused.is_none_or(|refusal| {
        matches!(
            refusal,
            Refusal::Stale | Refusal::Unproven | Refusal::Unmeasured | Refusal::OutOfRange
        )
    })
}

/// One node's record traced.
struct Tracer<'a> {
    node: PeerId,
    entries: &'a [Entry],
    /// Every heartbeat any node sent: sender, receiver, run, number.
    sent: &'a BTreeSet<(PeerId, PeerId, u64, u64)>,
    held: BTreeMap<PeerId, Held>,
    call: Option<Call>,
    /// The peers a poll found past the point they were held trusted to, with the point.
    due: Vec<(PeerId, u64)>,
    /// The suspicions the call told: peer and point.
    told: Vec<(PeerId, u64)>,
    traced: &'a mut Traced,
    failures: &'a mut Vec<String>,
}

impl Tracer<'_> {
    fn fail(&mut self, place: usize, why: String) {
        self.failures
            .push(format!("node {}, record entry {place}: {why}", self.node));
    }

    /// The call under way ends: what the record held of its peer moves on.
    fn settle(&mut self) {
        match self.call.take() {
            Some(Call::Fed {
                peer,
                beat,
                refused,
                holds,
                ..
            }) => {
                let held = self.held.entry(peer).or_default();
                // A later run begins at any heartbeat the stream judged: taken, or refused once
                // past its run's check.
                if judged(refused) && held.run.is_none_or(|run| beat.run > run) {
                    held.run = Some(beat.run);
                    held.last = None;
                }
                if refused.is_none() {
                    held.last = Some(beat.last());
                }
                held.holds = holds;
            }
            Some(Call::Polled { place, .. }) => {
                for (peer, until) in std::mem::take(&mut self.due) {
                    let gone = self.held.get(&peer).is_none_or(|held| held.holds.is_none());
                    if !gone && !self.told.contains(&(peer, until)) {
                        self.fail(
                            place,
                            format!(
                                "a poll at or past the point {until} it held {peer} trusted to \
                                 told no suspicion of it"
                            ),
                        );
                    }
                }
            }
            None => {}
        }
        self.told.clear();
    }

    fn walk(&mut self) {
        for (place, entry) in self.entries.iter().enumerate() {
            match *entry {
                Entry::Began => {
                    self.settle();
                    self.held.clear();
                }
                Entry::Fed {
                    peer,
                    beat,
                    refused,
                    holds,
                } => {
                    self.settle();
                    let before = self.held.get(&peer).copied().unwrap_or_default();
                    if judged(refused)
                        && let Some(Trust::Trusted { until_ns }) = before.holds
                        && beat.stamp >= until_ns
                    {
                        // Judged at its arrival: the point passed, so the call tells it.
                        self.traced.passed += 1;
                        let told = self.entries[place + 1..]
                            .iter()
                            .take_while(|entry| matches!(entry, Entry::Told(_)))
                            .any(|entry| {
                                matches!(entry, Entry::Told(Change::Suspected(s))
                                    if s.peer == peer && s.at_ns == until_ns)
                            });
                        if !told {
                            self.fail(
                                place,
                                format!(
                                    "a heartbeat of {peer} stamped {} at or past the point \
                                     {until_ns} it was held trusted to told no suspicion",
                                    beat.stamp
                                ),
                            );
                        }
                    }
                    self.call = Some(Call::Fed {
                        place,
                        peer,
                        beat,
                        refused,
                        holds,
                    });
                }
                Entry::Polled { now } => {
                    self.settle();
                    self.due = self
                        .held
                        .iter()
                        .filter_map(|(peer, held)| match held.holds {
                            Some(Trust::Trusted { until_ns }) if until_ns <= now => {
                                Some((*peer, until_ns))
                            }
                            _ => None,
                        })
                        .collect();
                    self.traced.passed += self.due.len() as u64;
                    self.call = Some(Call::Polled { place, now });
                }
                Entry::Told(Change::Suspected(suspicion)) => self.suspicion(place, suspicion),
                Entry::Holds { peer, trust } => {
                    self.held.entry(peer).or_default().holds = trust;
                }
                Entry::Told(_) | Entry::Sent { .. } => {}
            }
        }
        self.settle();
    }

    /// A suspicion told at `place`, traced.
    fn suspicion(&mut self, place: usize, suspicion: hyper_liveness::Suspicion) {
        let peer = suspicion.peer;
        self.traced.suspicions += 1;
        let before = self.held.get(&peer).copied().unwrap_or_default();
        // The call that told it: when it judged, and the heartbeat it took, if one of the peer's.
        let (start, time, own) = match self.call {
            Some(Call::Polled { place, now }) => (place, now, None),
            Some(Call::Fed {
                place,
                peer: from,
                beat,
                refused,
                ..
            }) if from == peer => (place, beat.stamp, Some((beat, refused))),
            Some(Call::Fed { peer: from, .. }) => {
                self.fail(
                    place,
                    format!("{suspicion:?} told by a heartbeat of another peer, {from}"),
                );
                return;
            }
            None => {
                self.fail(place, format!("{suspicion:?} told outside any call"));
                return;
            }
        };
        self.told.push((peer, suspicion.at_ns));
        if suspicion.noticed_ns != time {
            self.fail(
                place,
                format!("{suspicion:?} noticed other than at its call, at {time}"),
            );
        }
        if suspicion.at_ns > suspicion.noticed_ns {
            self.fail(place, format!("{suspicion:?} noticed before its point"));
        }
        // Its heartbeat, and its point.
        let settled = own.and_then(|(beat, refused)| {
            (refused.is_none() && suspicion.last == Some(beat.last())).then_some(beat)
        });
        let run = if let Some(beat) = settled {
            // The take restated the point, at or before the heartbeat's own arrival, which the
            // call noticed it at.
            self.traced.at_take += 1;
            Some(beat.run)
        } else {
            if own.is_some() {
                self.traced.at_arrival += 1;
            } else {
                self.traced.at_poll += 1;
            }
            if suspicion.last != before.last {
                self.fail(
                    place,
                    format!(
                        "{suspicion:?} judged from other than the latest heartbeat taken before \
                         its call, {:?}",
                        before.last
                    ),
                );
            }
            match before.holds {
                Some(Trust::Trusted { until_ns }) if suspicion.at_ns != until_ns => self.fail(
                    place,
                    format!(
                        "{suspicion:?} at other than the point {until_ns} the peer was held \
                         trusted to"
                    ),
                ),
                Some(Trust::Trusted { .. }) => {}
                _ => self.traced.first_margin += 1,
            }
            before.run
        };
        if suspicion.last.is_none() {
            self.traced.unheard += 1;
        }
        // No heartbeat of the peer's taken after the one it judged from came before the point;
        // the first of them is what became of the heartbeat the point awaited.
        let from = start + usize::from(settled.is_some());
        let mut answer = None;
        for (at, entry) in self.entries.iter().enumerate().skip(from) {
            match *entry {
                Entry::Began => break,
                Entry::Fed {
                    peer: of,
                    beat,
                    refused: None,
                    ..
                } if of == peer => {
                    if beat.stamp < suspicion.at_ns {
                        self.fail(
                            place,
                            format!(
                                "{suspicion:?}: a heartbeat taken after the one it judged from, \
                                 at entry {at}, was stamped {} before its point",
                                beat.stamp
                            ),
                        );
                    }
                    answer.get_or_insert(beat);
                }
                _ => {}
            }
        }
        self.answered(suspicion, run, answer);
    }

    /// What became of the heartbeat a suspicion's point awaited, judged from in run `run`.
    fn answered(
        &mut self,
        suspicion: hyper_liveness::Suspicion,
        run: Option<u64>,
        answer: Option<Beat>,
    ) {
        let peer = suspicion.peer;
        let next = suspicion.last.map_or(0, |last| last.seq + 1);
        let Some(beat) = answer else {
            // Anything the peer sent this node after it, in the run or a later one.
            let sent_after = self.sent.iter().any(|&(from, to, of, seq)| {
                from == peer
                    && to == self.node
                    && run.is_none_or(|run| of > run || (of == run && seq >= next))
            });
            if sent_after {
                self.traced.untaken += 1;
            } else {
                self.traced.silent += 1;
            }
            return;
        };
        self.traced.latest = self.traced.latest.max(Duration::from_nanos(
            beat.stamp.saturating_sub(suspicion.at_ns),
        ));
        if suspicion.last.is_some() && run.is_some_and(|run| beat.run != run) {
            self.traced.restarted += 1;
        } else if suspicion.last.is_none() || beat.seq == next {
            self.traced.late += 1;
        } else if !self.sent.contains(&(peer, self.node, beat.run, next)) {
            self.traced.skipped += 1;
        } else if self.entries.iter().any(|entry| {
            matches!(*entry, Entry::Fed { peer: of, beat: fed, refused: Some(_), .. }
                if of == peer && fed.run == beat.run && fed.seq == next)
        }) {
            self.traced.refused += 1;
        } else {
            self.traced.lost += 1;
        }
    }
}

/// Every suspicion in `records`, each node's with its id, traced to the detector's rule; what the
/// trace found, and each way a suspicion or a passed point fails the rule.
pub(crate) fn trace(records: &[(PeerId, &[Entry])]) -> (Traced, Vec<String>) {
    let sent: BTreeSet<(PeerId, PeerId, u64, u64)> = records
        .iter()
        .flat_map(|(node, entries)| {
            entries.iter().filter_map(move |entry| match *entry {
                Entry::Sent { peer, run, seq } => Some((*node, peer, run, seq)),
                _ => None,
            })
        })
        .collect();
    let mut traced = Traced::default();
    let mut failures = Vec::new();
    for &(node, entries) in records {
        Tracer {
            node,
            entries,
            sent: &sent,
            held: BTreeMap::new(),
            call: None,
            due: Vec::new(),
            told: Vec::new(),
            traced: &mut traced,
            failures: &mut failures,
        }
        .walk();
    }
    (traced, failures)
}
