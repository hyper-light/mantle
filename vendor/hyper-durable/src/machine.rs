//! What the shell asks of the state machine a group applies to (`docs/durable.md` §9).
use hyper_raft::proto::{ConfChangeV2, ConfState};

use crate::store::{EntryRef, Point};

/// The state machine failed in a way that leaves its state unknown: the replica is fenced and
/// reopened from what is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the state machine failed: {0}")]
pub struct Fatal(pub &'static str);

/// What a call of [`StateMachine::image`] made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Imaged {
    /// The image is whole in `into`: of this point, under this configuration.
    Made(Point, ConfState),
    /// A slice was written: the next is made at the replica's next drive, which its owner takes
    /// at its next turn.
    Slice,
    /// The next slice waits on work of the machine's own (a page of its engine loading), which
    /// wakes its owner, as an entry the machine has no room for does ([`StateMachine::apply`]).
    Waits,
}

/// A group's state machine, owned by its replica.
///
/// Its durable state is what a restart opens at: [`StateMachine::durable`] says how far it
/// reaches, and [`StateMachine::configuration`] the configuration it held there, which it keeps
/// beside its rows. A machine that keeps nothing durable of its own (focal's control groups, a
/// replay of the log) reports the point before the log's first entry, and the shell replays
/// what the log states committed (`docs/durable.md` §4.3).
pub trait StateMachine {
    /// What applying an entry answers its callers.
    type Answer;

    /// Applies a committed entry that changes no configuration, appending what it answers: true.
    /// False when the machine has no room for the entry without waiting on work of its own (an
    /// engine's maintenance): nothing of the entry is applied or answered, the replica stops
    /// before it with the entry committed and unapplied, and gives it again at its next drive. A
    /// machine that answers false has arranged for its owner to be woken once the room may have
    /// come, by the work it waits on and not by a timer, and its owner drives the replica then
    /// ([`crate::Replica::waits_for_room`]).
    fn apply(
        &mut self,
        entry: &EntryRef<'_>,
        answers: &mut Vec<Self::Answer>,
    ) -> Result<bool, Fatal>;

    /// A committed change of configuration at `at`, as its entry stated it (`change`, its
    /// context among it), left the group with `configuration`, which the machine keeps with its
    /// state from this index on; the configuration before is the machine's own
    /// ([`StateMachine::configuration`]). A change the core refused, alike on every member,
    /// leaves the configuration it had. An owner that reports each change with what it carried,
    /// as focal's does, reads it here.
    fn apply_change(
        &mut self,
        at: Point,
        change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal>;

    /// The last entry a restart opens with applied, and its term.
    fn durable(&self) -> Point;

    /// The configuration as of the last entry applied.
    fn configuration(&self) -> &ConfState;

    /// Whether a member acts on this entry at its next start, before its group tells it anything
    /// (`docs/durable.md` §4.1, I5): such an entry is applied only once the member's durable
    /// commit covers it. A group whose members act at start on everything (focal's control
    /// groups) says so for every entry.
    fn acts_at_start(&self, entry: &EntryRef<'_>) -> bool;

    /// An image of the machine's state at a point it applied, written into `into`, with the
    /// point and the configuration the group held there, which a snapshot carries with it
    /// (Ongaro and Ousterhout 2014, §7): what a leader sends a member that lacks entries
    /// its log no longer holds. Asked only when the core asks for a snapshot for such a member,
    /// never for a compaction (mantle docs/research/91-snapshots-on-demand.md). A machine that images on demand gives
    /// everything applied, under [`StateMachine::configuration`]; one that keeps its owner's
    /// checkpoints, as focal's does, gives the latest, which may be behind what it applied and
    /// is never behind the log's start, for a compaction never passes [`StateMachine::durable`]
    /// (I8).
    ///
    /// Made a slice at a time, each about `bytes` of the image, so the thread that drives the
    /// group holds no image's whole cost in one drive (mantle docs/design/replica.md §4): asked
    /// with `into` empty, the machine begins an image of the point it applied; asked again with
    /// what it wrote, it goes on from where it stopped. The replica applies no entry while an
    /// image is under way, so every slice reads the state of that one point, and asks for the
    /// next slice at its next drive ([`Imaged`]). A machine that images in one step gives it
    /// whole at the first ask.
    fn image(&mut self, into: &mut Vec<u8>, bytes: u64) -> Result<Imaged, Fatal>;

    /// The bytes [`StateMachine::image`] would write now, or none for a machine that cannot say
    /// without reading its state. Asked when a member opens and at each compaction, on the
    /// thread that drives the group, so it reads no state: the compaction rule weighs the log
    /// against the image of the state it was last compacted to ([`crate::Compaction`]), and
    /// before the first, against the state the member opened with, which a group formed with
    /// state (a range split from another) holds already. A machine that states none is never due
    /// until it installs a snapshot.
    fn image_bytes(&self) -> Option<u64>;

    /// Replaces the machine's state with `image`, which is of `at` under `configuration`, and
    /// makes it durable before returning: the log's start moves to `at` only after (I8).
    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal>;

    /// Makes everything applied durable, so that the log may be compacted behind it: true once it
    /// is, [`StateMachine::durable`] reaching what was applied when the checkpoint began. False
    /// when the machine has begun a checkpoint it makes without waiting (an engine's writes and
    /// flushes on its own workers) and it is not durable yet: [`StateMachine::durable`] stays at
    /// the last one, the machine has arranged for its owner to be woken once it may have
    /// completed, by the work it waits on and not by a timer, and the replica asks again at its
    /// next drive ([`crate::Replica::persisting`]). Asked again while one is out, the machine
    /// goes on with that one and begins no other; entries applied meanwhile go on and are not
    /// covered by it. A crash before it completes reopens at the durable point before it, which
    /// the log still covers: its start never passes [`StateMachine::durable`] (I8).
    fn persist(&mut self) -> Result<bool, Fatal>;
}
