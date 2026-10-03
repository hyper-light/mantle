//! Members on hyper-log over simulated devices: each member's log on a `SimFile` of its own,
//! written through its group's handle ([`GroupStore`]), woken by the log's answers. A device
//! loses power at its `ops`-th write or flush (`Fault::PowerCut`), and a crash keeps nothing it
//! did not flush (`Crash::LoseAll`): the directed runs of mantle's range simulation (mantle
//! `crates/range/tests/sim.rs` at `1c179e8`) and focal's F17 cases, against this shell.
use std::sync::mpsc::{Receiver, sync_channel};
use std::task::Waker;
use std::time::Duration;

use hyper_block::buf::Alignment;
use hyper_block::sim::{Crash, Fault, SimFile};
use hyper_durable::{Cause, GroupStore, Output, Replica, ReplicaError, Unbounded};
use hyper_log::{Config, Log, Waits};
use hyper_raft::proto::{ConfChangeV2, ConfState, Message};

use super::Kv;
use super::cluster::settings;

/// The group every member's log holds.
pub const GROUP: u128 = 0x0064_7572_6162_6c65;
const LOG_ID: u128 = 0x006c_6f67;

pub type DeviceReplica = Replica<GroupStore<SimFile>, Kv, Unbounded>;

/// A log of the size mantle's range simulation runs, which never waits for more submitters
/// between frames: the run drives each member's writes one at a time.
pub fn log_config() -> Config {
    Config {
        segment_bytes: 64 * 4096,
        max_segments: 16,
        max_groups: 4,
        group_entries: 1 << 12,
        group_bytes: 1 << 20,
        group_cache: 1 << 16,
        queue_submissions: 64,
        waits: Waits::Never,
    }
}

pub fn sim_file(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(4096).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

/// One member: its log, its replica, and the waker the log's answers wake it by.
pub struct Device {
    pub id: u64,
    log: Option<Log<SimFile>>,
    pub replica: Option<DeviceReplica>,
    waker: Waker,
    woken: Receiver<usize>,
    seed: u64,
    /// The member's state machine across a crash.
    kv: Option<Kv>,
    /// Entries acted on at start before the last crash.
    pub acted_before: Vec<u64>,
}

impl Device {
    pub fn new(id: u64, seed: u64, configuration: &ConfState) -> Self {
        let log = Log::create(sim_file(seed ^ id), log_config(), LOG_ID).unwrap();
        let (tell, woken) = sync_channel(1024);
        let (waker, _) = hyper_measure::wake::waker(usize::try_from(id).unwrap(), tell);
        let mut device = Self {
            id,
            log: Some(log),
            replica: None,
            waker,
            woken,
            seed,
            kv: Some(Kv::new(configuration.clone(), true)),
            acted_before: Vec::new(),
        };
        device.open();
        device
    }

    fn open(&mut self) {
        let log = self.log.as_ref().unwrap();
        let store = GroupStore::claim(log, GROUP).expect("the group is claimed");
        let kv = self.kv.take().unwrap();
        self.replica = Some(
            Replica::open(&settings(self.id, self.seed), store, kv, Unbounded)
                .unwrap_or_else(|e| panic!("member {} does not open: {e}", self.id)),
        );
    }

    /// Arms a fault on the member's device.
    pub fn inject(&self, fault: Fault) {
        self.log
            .as_ref()
            .unwrap()
            .with_file(move |f| f.inject(fault))
            .unwrap()
            .unwrap();
    }

    /// The member's paths are measured: it is given its group's timing, and campaigns by
    /// suspicion from here on. Until then only the founder campaigns, on its owner's word, as no
    /// span is chosen before a quorum's paths are measured (`docs/timing.md` §2.3).
    pub fn measured(&mut self) {
        if let Some(r) = self.replica.as_mut() {
            r.set_timing(timing()).unwrap();
        }
    }

    /// Loses power: the process with everything its log and state machine had not made
    /// durable. The device keeps what `crash` says of its unflushed sectors. It reopens with its
    /// paths measured, as its node's are.
    pub fn crash(&mut self, crash: Crash) {
        let replica = self.replica.take().expect("up");
        self.acted_before
            .extend(replica.machine().acted.iter().copied());
        self.kv = Some(replica.into_machine().crashed());
        let log = self.log.take().unwrap();
        let file = log.close().expect("the log gives back its file");
        file.crash(crash).unwrap();
        file.clear_faults().unwrap();
        let (log, _) = Log::open(file, log_config(), LOG_ID).expect("the log reopens");
        self.log = Some(log);
        self.open();
        self.measured();
    }

    /// Drives the replica once; its messages go to `wire`. The configuration it held when a
    /// failed write fenced it, if one did.
    pub fn drive(&mut self, now: u64, wire: &mut Vec<Message>) -> Option<ConfState> {
        let replica = self.replica.as_mut()?;
        if replica.fenced().is_some() {
            return None;
        }
        let mut out = Output::default();
        match replica.drive(now, &self.waker, &mut out) {
            Ok(_) => {
                wire.extend(out.messages);
                None
            }
            Err(ReplicaError::Fenced(Cause::Write(_))) => Some(replica.configuration().clone()),
            Err(e) => panic!("member {}: drive: {e}", self.id),
        }
    }

    /// Waits until every write out is answered, driving as answers come: the log wakes the
    /// member once for every write it answers, so a wait never outlasts one.
    pub fn settle(&mut self, now: u64, wire: &mut Vec<Message>) -> Option<ConfState> {
        loop {
            let cut = self.drive(now, wire);
            if cut.is_some() {
                return cut;
            }
            let replica = self.replica.as_ref()?;
            if replica.fenced().is_some() || replica.in_flight() == 0 {
                return None;
            }
            self.woken
                .recv()
                .expect("the log answers every write it took");
        }
    }

    pub fn r(&mut self) -> &mut DeviceReplica {
        self.replica.as_mut().expect("up")
    }
}

/// How members take their readies in a directed run: each member's writes waited for as it
/// makes them, or every member's made first and waited for after (a node overlapping its
/// members' flushes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Takes {
    Waiting,
    Overlapping,
}

/// What the group is asked to do under member 1's lead while the target's power is cut.
#[derive(Clone, Debug)]
pub enum Action {
    /// A change of configuration, the configuration it makes, and the member it removes, which
    /// the operator stops for good once the leader says every voter knows the change committed.
    Change {
        change: ConfChangeV2,
        made: ConfState,
        removes: Option<u64>,
    },
    /// An entry every member acts on at its next start (focal's upgrade fence).
    Fence,
}

#[derive(Clone, Debug)]
pub struct Case {
    pub members: u64,
    pub target: u64,
    pub action: Action,
}

/// The timing a directed run's members elect by (timing step L-2): a round of ten of the run's
/// 10 ms rounds and a span as long, as the ticks before it waited ten ticks of a round each and
/// drew over ten more. A message is delivered at the end of the round it was sent in, so a vote
/// round takes two or three rounds and is covered.
pub fn timing() -> hyper_raft::Timing {
    hyper_raft::Timing {
        span: Duration::from_millis(100),
        round: Duration::from_millis(100),
    }
}

/// Rounds a directed run waits for what a group with a quorum and no faults does within a few
/// elections: twenty of the longest wait the core draws, a round and the span, as mantle's
/// directed runs wait twenty of their longest timeout. Reaching it is the failure the run looks
/// for.
pub const PATIENT_ROUNDS: usize = 20 * 20;

pub struct Directed {
    pub devices: Vec<Device>,
    now: u64,
}

impl Directed {
    pub fn new(case: &Case, seed: u64) -> Self {
        let configuration = ConfState {
            voters: (1..=case.members).collect(),
            ..ConfState::default()
        };
        Self {
            devices: (1..=case.members)
                .map(|id| Device::new(id, seed, &configuration))
                .collect(),
            now: 0,
        }
    }

    pub fn device(&mut self, id: u64) -> Option<&mut Device> {
        self.devices.iter_mut().find(|d| d.id == id)
    }

    pub fn leader(&mut self) -> Option<&mut DeviceReplica> {
        self.devices
            .iter_mut()
            .filter_map(|d| d.replica.as_mut())
            .filter(|r| r.fenced().is_none())
            .find(|r| r.is_leader())
    }

    /// The operator stops the member a change removed once the leader has applied the change
    /// and says every voter knows it committed.
    fn operate(&mut self, case: &Case) {
        let Action::Change {
            removes: Some(removed),
            ..
        } = case.action
        else {
            return;
        };
        let known = self.leader().is_some_and(|r| {
            !r.configuration().voters.contains(&removed) && r.configuration_known()
        });
        if known {
            self.devices.retain(|d| d.id != removed);
        }
    }

    /// Every member's paths are measured ([`Device::measured`]).
    pub fn measured(&mut self) {
        for d in &mut self.devices {
            d.measured();
        }
    }

    /// The member `id`'s power was cut and it reopened: every other member's detectors see its
    /// new incarnation.
    pub fn restarted(&mut self, id: u64) {
        for d in &mut self.devices {
            if d.id != id
                && let Some(r) = d.replica.as_mut()
            {
                match r.restarted(id) {
                    Ok(()) | Err(ReplicaError::Fenced(_)) => {}
                    Err(e) => panic!("member {}: restarted: {e}", d.id),
                }
            }
        }
    }

    /// One round: the clock moves on, every member takes its ready (woken at the clock), the
    /// operator looking on after each, and what was sent arrives. The target's configuration
    /// when its write failed, if one did.
    pub fn round(&mut self, case: &Case, takes: Takes) -> Option<ConfState> {
        self.now += 10_000_000;
        let now = self.now;
        let mut wire = Vec::new();
        let mut cut = None;
        let ids: Vec<u64> = self.devices.iter().map(|d| d.id).collect();
        for &id in &ids {
            let overlap = takes == Takes::Overlapping && id == case.target;
            let Some(d) = self.device(id) else { continue };
            let failed = if overlap {
                d.drive(now, &mut wire)
            } else {
                d.settle(now, &mut wire)
            };
            if id == case.target && failed.is_some() {
                cut = failed;
            }
            self.operate(case);
            if overlap
                && let Some(d) = self.device(id)
                && let Some(failed) = d.settle(now, &mut wire)
            {
                cut = Some(failed);
            }
        }
        for m in wire {
            let to = m.to;
            if let Some(r) = self.device(to).and_then(|d| d.replica.as_mut()) {
                match r.step(m) {
                    Ok(())
                    | Err(
                        ReplicaError::Refused(_) | ReplicaError::Stalled | ReplicaError::Fenced(_),
                    ) => {}
                    Err(e) => panic!("step on {to}: {e}"),
                }
            }
        }
        cut
    }

    pub fn now(&self) -> u64 {
        self.now
    }
}

/// Whether two configurations name the same members in the same roles.
pub use super::same_configuration;
