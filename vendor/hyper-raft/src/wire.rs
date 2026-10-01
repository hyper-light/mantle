//! hyper-raft's wire format (`docs/raft.md` §3.1): fixed-width little-endian fields at known
//! offsets, variable-length bytes after them, a version and a kind before, a CRC-32C after.
//!
//! A value is written whole into the caller's buffer ([`Record::encode`]) and read whole from a
//! slice ([`Record::decode`]). Reading checks every count and length against the bytes left before
//! taking or allocating anything, allocates each buffer once at its exact size, and refuses an
//! unknown version, kind or flag, a checksum that does not match, and bytes left over.

use thiserror::Error;

use crate::proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};

/// The format's version: the first byte of every record.
pub const VERSION: u8 = 1;
/// Bytes of a record before its body: the version and the kind.
pub const HEADER_BYTES: usize = 2;
/// Bytes of a record after its body: the CRC-32C.
pub const CHECKSUM_BYTES: usize = 4;
/// Bytes of an entry's body besides its data and context: the kind, the term, the index and the
/// two lengths.
pub const ENTRY_FIXED_BYTES: usize = 1 + 8 + 8 + 4 + 4;
/// Bytes of a message's body besides its context, entries and snapshot: the kind, the flags,
/// nine `u64`, the priority, the entry count and the context length.
pub const MESSAGE_FIXED_BYTES: usize = 1 + 1 + 9 * 8 + 8 + 4 + 4;

/// Flag bit of a message: the request is refused.
const REJECT: u8 = 1;
/// Flag bit of a message: a snapshot follows the entries.
const HAS_SNAPSHOT: u8 = 1 << 1;
/// Presence bit of a snapshot: its metadata follows.
const HAS_METADATA: u8 = 1;
/// Presence bit of a snapshot: the metadata's configuration follows its index and term.
const HAS_CONF: u8 = 1 << 1;

/// Why bytes are not a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeError {
    /// The bytes end before the value does.
    #[error("the record ends early")]
    Truncated,
    /// Bytes remain after the value.
    #[error("bytes remain after the record")]
    Trailing,
    /// The record's version is not this format's.
    #[error("format version {0}")]
    Version(u8),
    /// The record holds another kind of value.
    #[error("a record of kind {0}")]
    Kind(u8),
    /// The CRC-32C does not match the bytes.
    #[error("the checksum does not match")]
    Corrupt,
    /// A kind, transition or flag byte no value names.
    #[error("{what} {value} names nothing")]
    Unknown {
        /// The field.
        what: &'static str,
        /// The byte.
        value: u8,
    },
    /// A length or count larger than this machine can hold.
    #[error("a length past this machine's")]
    Length,
}

/// A value the format writes as a record.
pub trait Record: Sized {
    /// The record's kind byte.
    const KIND: u8;
    /// The bytes of the body.
    fn body_len(&self) -> usize;
    /// Appends the body.
    fn put_body(&self, out: &mut Vec<u8>);
    /// Reads the body.
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError>;

    /// The bytes of the record.
    fn encoded_len(&self) -> usize {
        HEADER_BYTES
            .saturating_add(self.body_len())
            .saturating_add(CHECKSUM_BYTES)
    }
    /// Appends the record to `out`.
    fn encode(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.reserve(self.encoded_len());
        out.push(VERSION);
        out.push(Self::KIND);
        self.put_body(out);
        let crc = crc32c::crc32c(out.get(start..).unwrap_or(&[]));
        out.extend_from_slice(&crc.to_le_bytes());
    }
    /// The record in a buffer of its own.
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode(&mut out);
        out
    }
    /// The value of the record `bytes` holds, exactly.
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (covered, crc) = bytes
            .split_last_chunk::<CHECKSUM_BYTES>()
            .ok_or(DecodeError::Truncated)?;
        let mut reader = Reader { bytes: covered };
        let version = reader.byte()?;
        if version != VERSION {
            return Err(DecodeError::Version(version));
        }
        let kind = reader.byte()?;
        if kind != Self::KIND {
            return Err(DecodeError::Kind(kind));
        }
        if crc32c::crc32c(covered) != u32::from_le_bytes(*crc) {
            return Err(DecodeError::Corrupt);
        }
        let value = Self::take_body(&mut reader)?;
        if reader.bytes.is_empty() {
            Ok(value)
        } else {
            Err(DecodeError::Trailing)
        }
    }
}

/// The bytes left of a record being read.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], DecodeError> {
        let (taken, rest) = self
            .bytes
            .split_at_checked(length)
            .ok_or(DecodeError::Truncated)?;
        self.bytes = rest;
        Ok(taken)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let (taken, rest) = self
            .bytes
            .split_first_chunk::<N>()
            .ok_or(DecodeError::Truncated)?;
        self.bytes = rest;
        Ok(*taken)
    }
    fn byte(&mut self) -> Result<u8, DecodeError> {
        self.array::<1>().map(|[byte]| byte)
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        self.array().map(u64::from_le_bytes)
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        self.array().map(i64::from_le_bytes)
    }
    fn length(&mut self) -> Result<usize, DecodeError> {
        let length = self.array().map(u32::from_le_bytes)?;
        usize::try_from(length).map_err(|_| DecodeError::Length)
    }
    /// A count of items at least `each` bytes long, refused if the bytes left cannot hold them.
    fn count(&mut self, each: usize) -> Result<usize, DecodeError> {
        let count = self.length()?;
        let least = count.checked_mul(each).ok_or(DecodeError::Truncated)?;
        if least > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        Ok(count)
    }
    /// A length and that many bytes, in a buffer of exactly their size.
    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let length = self.length()?;
        Ok(self.take(length)?.to_vec())
    }
    fn ids(&mut self, count: usize) -> Result<Vec<u64>, DecodeError> {
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(self.u64()?);
        }
        Ok(ids)
    }
}

/// A length as the format writes it: every length a value holds fits the format's `u32` by the
/// core's own bounds (an entry and a message are bounded far below 4 GiB by `max_size_per_msg`
/// and the owner's frame limits); one that does not is written saturated and refused on reading
/// as truncated, never wrapped.
fn put_length(out: &mut Vec<u8>, length: usize) {
    out.extend_from_slice(&u32::try_from(length).unwrap_or(u32::MAX).to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_length(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn take_entry_kind(value: u8) -> Result<EntryType, DecodeError> {
    EntryType::from_byte(value).ok_or(DecodeError::Unknown {
        what: "entry kind",
        value,
    })
}

/// The message kinds, in the order of their bytes.
const MESSAGE_KINDS: [MessageType; 21] = [
    MessageType::MsgHup,
    MessageType::MsgBeat,
    MessageType::MsgPropose,
    MessageType::MsgAppend,
    MessageType::MsgAppendResponse,
    MessageType::MsgRequestVote,
    MessageType::MsgRequestVoteResponse,
    MessageType::MsgSnapshot,
    MessageType::MsgHeartbeat,
    MessageType::MsgHeartbeatResponse,
    MessageType::MsgUnreachable,
    MessageType::MsgSnapStatus,
    MessageType::MsgCheckQuorum,
    MessageType::MsgTransferLeader,
    MessageType::MsgTimeoutNow,
    MessageType::MsgReadIndex,
    MessageType::MsgReadIndexResp,
    MessageType::MsgRequestPreVote,
    MessageType::MsgRequestPreVoteResponse,
    MessageType::MsgFastPropose,
    MessageType::MsgFastVote,
];

fn message_kind(kind: MessageType) -> u8 {
    MESSAGE_KINDS
        .iter()
        .position(|known| *known == kind)
        .and_then(|position| u8::try_from(position).ok())
        .unwrap_or(u8::MAX)
}

fn take_message_kind(value: u8) -> Result<MessageType, DecodeError> {
    MESSAGE_KINDS
        .get(usize::from(value))
        .copied()
        .ok_or(DecodeError::Unknown {
            what: "message kind",
            value,
        })
}

fn change_kind(kind: ConfChangeType) -> u8 {
    match kind {
        ConfChangeType::AddNode => 0,
        ConfChangeType::RemoveNode => 1,
        ConfChangeType::AddLearnerNode => 2,
    }
}

fn take_change_kind(value: u8) -> Result<ConfChangeType, DecodeError> {
    match value {
        0 => Ok(ConfChangeType::AddNode),
        1 => Ok(ConfChangeType::RemoveNode),
        2 => Ok(ConfChangeType::AddLearnerNode),
        value => Err(DecodeError::Unknown {
            what: "change kind",
            value,
        }),
    }
}

fn transition(kind: ConfChangeTransition) -> u8 {
    match kind {
        ConfChangeTransition::Auto => 0,
        ConfChangeTransition::Implicit => 1,
        ConfChangeTransition::Explicit => 2,
    }
}

fn take_transition(value: u8) -> Result<ConfChangeTransition, DecodeError> {
    match value {
        0 => Ok(ConfChangeTransition::Auto),
        1 => Ok(ConfChangeTransition::Implicit),
        2 => Ok(ConfChangeTransition::Explicit),
        value => Err(DecodeError::Unknown {
            what: "transition",
            value,
        }),
    }
}

impl Record for Entry {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 2;
    fn body_len(&self) -> usize {
        ENTRY_FIXED_BYTES
            .saturating_add(self.data.len())
            .saturating_add(self.context.len())
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.push(self.entry_type.byte());
        out.extend_from_slice(&self.term.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        put_length(out, self.data.len());
        put_length(out, self.context.len());
        out.extend_from_slice(&self.data);
        out.extend_from_slice(&self.context);
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let entry_type = take_entry_kind(reader.byte()?)?;
        let term = reader.u64()?;
        let index = reader.u64()?;
        let data = reader.length()?;
        let context = reader.length()?;
        Ok(Self {
            entry_type,
            term,
            index,
            data: reader.take(data)?.to_vec(),
            context: reader.take(context)?.to_vec(),
        })
    }
}

impl Record for HardState {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 3;
    fn body_len(&self) -> usize {
        3 * 8
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.term.to_le_bytes());
        out.extend_from_slice(&self.vote.to_le_bytes());
        out.extend_from_slice(&self.commit.to_le_bytes());
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            term: reader.u64()?,
            vote: reader.u64()?,
            commit: reader.u64()?,
        })
    }
}

impl Record for ConfState {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 4;
    fn body_len(&self) -> usize {
        let ids = self
            .voters
            .len()
            .saturating_add(self.learners.len())
            .saturating_add(self.voters_outgoing.len())
            .saturating_add(self.learners_next.len());
        (1 + 4 * 4usize).saturating_add(ids.saturating_mul(8))
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.push(u8::from(self.auto_leave));
        let sets = [
            &self.voters,
            &self.learners,
            &self.voters_outgoing,
            &self.learners_next,
        ];
        for set in sets {
            put_length(out, set.len());
        }
        for set in sets {
            for id in set {
                out.extend_from_slice(&id.to_le_bytes());
            }
        }
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let auto_leave = match reader.byte()? {
            0 => false,
            1 => true,
            value => {
                return Err(DecodeError::Unknown {
                    what: "auto-leave",
                    value,
                });
            }
        };
        let mut counts = [0usize; 4];
        for count in &mut counts {
            *count = reader.length()?;
        }
        let total = counts
            .iter()
            .try_fold(0usize, |total, count| total.checked_add(*count))
            .and_then(|total| total.checked_mul(8))
            .ok_or(DecodeError::Truncated)?;
        if total > reader.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let [voters, learners, outgoing, next] = counts;
        Ok(Self {
            voters: reader.ids(voters)?,
            learners: reader.ids(learners)?,
            voters_outgoing: reader.ids(outgoing)?,
            learners_next: reader.ids(next)?,
            auto_leave,
        })
    }
}

impl Record for Snapshot {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 5;
    fn body_len(&self) -> usize {
        let metadata = self.metadata.as_ref().map_or(0, |metadata| {
            (2 * 8usize).saturating_add(metadata.conf_state.as_ref().map_or(0, Record::body_len))
        });
        (1 + 4usize)
            .saturating_add(metadata)
            .saturating_add(self.data.len())
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        let mut presence = 0;
        if let Some(metadata) = &self.metadata {
            presence |= HAS_METADATA;
            if metadata.conf_state.is_some() {
                presence |= HAS_CONF;
            }
        }
        out.push(presence);
        if let Some(metadata) = &self.metadata {
            out.extend_from_slice(&metadata.index.to_le_bytes());
            out.extend_from_slice(&metadata.term.to_le_bytes());
            if let Some(conf) = &metadata.conf_state {
                conf.put_body(out);
            }
        }
        put_bytes(out, &self.data);
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let presence = reader.byte()?;
        if presence & !(HAS_METADATA | HAS_CONF) != 0
            || (presence & HAS_CONF != 0 && presence & HAS_METADATA == 0)
        {
            return Err(DecodeError::Unknown {
                what: "snapshot presence",
                value: presence,
            });
        }
        let metadata = if presence & HAS_METADATA != 0 {
            let index = reader.u64()?;
            let term = reader.u64()?;
            let conf_state = if presence & HAS_CONF != 0 {
                Some(ConfState::take_body(reader)?)
            } else {
                None
            };
            Some(SnapshotMetadata {
                conf_state,
                index,
                term,
            })
        } else {
            None
        };
        Ok(Self {
            data: reader.bytes()?,
            metadata,
        })
    }
}

impl Record for Message {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 1;
    fn body_len(&self) -> usize {
        let entries = self.entries.iter().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.body_len())
        });
        MESSAGE_FIXED_BYTES
            .saturating_add(self.context.len())
            .saturating_add(entries)
            .saturating_add(self.snapshot.as_deref().map_or(0, Record::body_len))
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.push(message_kind(self.msg_type));
        let mut flags = 0;
        if self.reject {
            flags |= REJECT;
        }
        if self.snapshot.is_some() {
            flags |= HAS_SNAPSHOT;
        }
        out.push(flags);
        for field in [
            self.to,
            self.from,
            self.term,
            self.log_term,
            self.index,
            self.commit,
            self.commit_term,
            self.request_snapshot,
            self.reject_hint,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
        out.extend_from_slice(&self.priority.to_le_bytes());
        put_length(out, self.entries.len());
        put_length(out, self.context.len());
        out.extend_from_slice(&self.context);
        for entry in &self.entries {
            entry.put_body(out);
        }
        if let Some(snapshot) = &self.snapshot {
            snapshot.put_body(out);
        }
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let msg_type = take_message_kind(reader.byte()?)?;
        let flags = reader.byte()?;
        if flags & !(REJECT | HAS_SNAPSHOT) != 0 {
            return Err(DecodeError::Unknown {
                what: "message flags",
                value: flags,
            });
        }
        let mut fields = [0u64; 9];
        for field in &mut fields {
            *field = reader.u64()?;
        }
        let [
            to,
            from,
            term,
            log_term,
            index,
            commit,
            commit_term,
            request_snapshot,
            reject_hint,
        ] = fields;
        let priority = reader.i64()?;
        let count = reader.length()?;
        let context = reader.length()?;
        let context = reader.take(context)?.to_vec();
        let least = count
            .checked_mul(ENTRY_FIXED_BYTES)
            .ok_or(DecodeError::Truncated)?;
        if least > reader.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(Entry::take_body(reader)?);
        }
        let snapshot = if flags & HAS_SNAPSHOT != 0 {
            Some(Box::new(Snapshot::take_body(reader)?))
        } else {
            None
        };
        Ok(Self {
            msg_type,
            to,
            from,
            term,
            log_term,
            index,
            entries,
            commit,
            commit_term,
            snapshot,
            request_snapshot,
            reject: flags & REJECT != 0,
            reject_hint,
            context,
            priority,
        })
    }
}

impl Record for ConfChange {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 6;
    fn body_len(&self) -> usize {
        (1 + 8 + 4usize).saturating_add(self.context.len())
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.push(change_kind(self.change_type));
        out.extend_from_slice(&self.node_id.to_le_bytes());
        put_bytes(out, &self.context);
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            change_type: take_change_kind(reader.byte()?)?,
            node_id: reader.u64()?,
            context: reader.bytes()?,
        })
    }
}

impl Record for ConfChangeV2 {
    /// The kind byte `docs/raft.md` §3.1 gives this record. Format.
    const KIND: u8 = 7;
    fn body_len(&self) -> usize {
        (1 + 4 + 4usize)
            .saturating_add(self.changes.len().saturating_mul(1 + 8))
            .saturating_add(self.context.len())
    }
    fn put_body(&self, out: &mut Vec<u8>) {
        out.push(transition(self.transition));
        put_length(out, self.changes.len());
        for change in &self.changes {
            out.push(change_kind(change.change_type));
            out.extend_from_slice(&change.node_id.to_le_bytes());
        }
        put_bytes(out, &self.context);
    }
    fn take_body(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let transition = take_transition(reader.byte()?)?;
        let count = reader.count(1 + 8)?;
        let mut changes = Vec::with_capacity(count);
        for _ in 0..count {
            changes.push(ConfChangeSingle {
                change_type: take_change_kind(reader.byte()?)?,
                node_id: reader.u64()?,
            });
        }
        Ok(Self {
            transition,
            changes,
            context: reader.bytes()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut record = vec![VERSION, kind];
        record.extend_from_slice(body);
        let crc = crc32c::crc32c(&record);
        record.extend_from_slice(&crc.to_le_bytes());
        record
    }

    fn entry() -> Entry {
        Entry {
            entry_type: EntryType::EntryConfChangeV2,
            term: 3,
            index: 0x0102_0304_0506_0708,
            data: b"data".to_vec(),
            context: b"ctx".to_vec(),
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            data: b"image".to_vec(),
            metadata: Some(SnapshotMetadata {
                conf_state: Some(ConfState {
                    voters: vec![1, 2, 3],
                    learners: vec![4],
                    voters_outgoing: vec![1, 5],
                    learners_next: vec![5],
                    auto_leave: true,
                }),
                index: 40,
                term: 6,
            }),
        }
    }

    fn message() -> Message {
        Message {
            msg_type: MessageType::MsgAppend,
            to: 2,
            from: 1,
            term: 7,
            log_term: 6,
            index: 41,
            entries: vec![entry(), Entry::default()],
            commit: 40,
            commit_term: 6,
            snapshot: Some(Box::new(snapshot())),
            request_snapshot: 9,
            reject: true,
            reject_hint: 39,
            context: b"read".to_vec(),
            priority: -5,
        }
    }

    fn change() -> ConfChangeV2 {
        ConfChangeV2 {
            transition: ConfChangeTransition::Explicit,
            changes: vec![
                ConfChangeSingle {
                    change_type: ConfChangeType::AddLearnerNode,
                    node_id: 4,
                },
                ConfChangeSingle {
                    change_type: ConfChangeType::RemoveNode,
                    node_id: 2,
                },
            ],
            context: b"why".to_vec(),
        }
    }

    /// The layouts of `docs/raft.md` §3.1, written out field by field.
    #[test]
    fn the_layouts_are_the_documented_ones() {
        let mut body = Vec::new();
        for field in [7u64, 9, 40] {
            body.extend_from_slice(&field.to_le_bytes());
        }
        let hard = HardState {
            term: 7,
            vote: 9,
            commit: 40,
        };
        assert_eq!(hard.encode_to_vec(), sealed(3, &body));

        let mut body = vec![2];
        body.extend_from_slice(&3u64.to_le_bytes());
        body.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        body.extend_from_slice(&4u32.to_le_bytes());
        body.extend_from_slice(&3u32.to_le_bytes());
        body.extend_from_slice(b"datactx");
        assert_eq!(entry().encode_to_vec(), sealed(2, &body));

        let mut body = vec![2];
        body.extend_from_slice(&2u32.to_le_bytes());
        body.extend_from_slice(&[2]);
        body.extend_from_slice(&4u64.to_le_bytes());
        body.extend_from_slice(&[1]);
        body.extend_from_slice(&2u64.to_le_bytes());
        body.extend_from_slice(&3u32.to_le_bytes());
        body.extend_from_slice(b"why");
        assert_eq!(change().encode_to_vec(), sealed(7, &body));

        // A message: kind, flags, nine words, priority, counts, context, entries, snapshot.
        let mut body = vec![3, REJECT | HAS_SNAPSHOT];
        for field in [2u64, 1, 7, 6, 41, 40, 6, 9, 39] {
            body.extend_from_slice(&field.to_le_bytes());
        }
        body.extend_from_slice(&(-5i64).to_le_bytes());
        body.extend_from_slice(&2u32.to_le_bytes());
        body.extend_from_slice(&4u32.to_le_bytes());
        body.extend_from_slice(b"read");
        entry().put_body(&mut body);
        Entry::default().put_body(&mut body);
        snapshot().put_body(&mut body);
        let encoded = message().encode_to_vec();
        assert_eq!(encoded, sealed(1, &body));
        assert_eq!(encoded.len(), message().encoded_len());
    }

    /// A record and whether bytes read back to its value.
    type Golden = (Vec<u8>, fn(&[u8]) -> bool);

    fn records() -> Vec<Golden> {
        fn round<T: Record + PartialEq + std::fmt::Debug>(bytes: &[u8], value: &T) -> bool {
            T::decode(bytes).as_ref() == Ok(value)
        }
        vec![
            (message().encode_to_vec(), |b| round(b, &message())),
            (entry().encode_to_vec(), |b| round(b, &entry())),
            (snapshot().encode_to_vec(), |b| round(b, &snapshot())),
            (change().encode_to_vec(), |b| round(b, &change())),
            (
                snapshot()
                    .metadata
                    .unwrap()
                    .conf_state
                    .unwrap()
                    .encode_to_vec(),
                |b| round(b, &snapshot().metadata.unwrap().conf_state.unwrap()),
            ),
            (
                ConfChange {
                    change_type: ConfChangeType::AddNode,
                    node_id: 3,
                    context: Vec::new(),
                }
                .encode_to_vec(),
                |b| {
                    round(
                        b,
                        &ConfChange {
                            change_type: ConfChangeType::AddNode,
                            node_id: 3,
                            context: Vec::new(),
                        },
                    )
                },
            ),
            (Message::default().encode_to_vec(), |b| {
                round(b, &Message::default())
            }),
            (Snapshot::default().encode_to_vec(), |b| {
                round(b, &Snapshot::default())
            }),
        ]
    }

    #[test]
    fn every_type_round_trips() {
        for (bytes, reads_back) in records() {
            assert!(reads_back(&bytes));
        }
    }

    /// Every prefix, every extension by a byte and every single bit flipped is refused: the
    /// checksum or the bounds catch each one, and none panics.
    #[test]
    fn every_truncation_extension_and_bit_flip_is_refused() {
        for (bytes, _) in records() {
            for cut in 0..bytes.len() {
                assert!(Message::decode(&bytes[..cut]).is_err() || cut == bytes.len());
                assert!(Entry::decode(&bytes[..cut]).is_err());
                assert!(Snapshot::decode(&bytes[..cut]).is_err());
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(Message::decode(&longer).is_err());
            for at in 0..bytes.len() {
                for bit in 0..8 {
                    let mut flipped = bytes.clone();
                    flipped[at] ^= 1 << bit;
                    assert!(Message::decode(&flipped).is_err(), "byte {at} bit {bit}");
                    assert!(Entry::decode(&flipped).is_err());
                    assert!(ConfChangeV2::decode(&flipped).is_err());
                }
            }
        }
    }

    /// A body that claims more than the bytes hold is refused before anything is allocated for
    /// it, whatever the checksum.
    #[test]
    fn a_count_past_the_bytes_is_refused_with_a_valid_checksum() {
        let mut body = vec![3, 0];
        body.extend_from_slice(&[0; 9 * 8 + 8]);
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(
            Message::decode(&sealed(1, &body)),
            Err(DecodeError::Truncated)
        );
        let mut body = vec![0];
        body.extend_from_slice(&[0xff; 16]);
        assert_eq!(
            ConfState::decode(&sealed(4, &body)),
            Err(DecodeError::Truncated)
        );
        let mut body = vec![0];
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            ConfChangeV2::decode(&sealed(7, &body)),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn unknown_kinds_flags_and_versions_are_refused() {
        let mut body = vec![99, 0];
        body.extend_from_slice(&[0; 9 * 8 + 8 + 8]);
        assert!(matches!(
            Message::decode(&sealed(1, &body)),
            Err(DecodeError::Unknown { .. })
        ));
        let mut body = vec![3, 1 << 7];
        body.extend_from_slice(&[0; 9 * 8 + 8 + 8]);
        assert!(matches!(
            Message::decode(&sealed(1, &body)),
            Err(DecodeError::Unknown { .. })
        ));
        let mut wrong = HardState::default().encode_to_vec();
        wrong[0] = 2;
        assert_eq!(HardState::decode(&wrong), Err(DecodeError::Version(2)));
        assert_eq!(
            Entry::decode(&HardState::default().encode_to_vec()),
            Err(DecodeError::Kind(3))
        );
    }

    /// Arbitrary bytes, sealed with a valid checksum so they reach the body, never panic.
    #[test]
    fn arbitrary_bodies_never_panic() {
        let mut state = 0x853c_49e6_748f_ea9bu64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let length = (state % 200) as usize;
            let body: Vec<u8> = (0..length)
                .map(|at| (state.rotate_left(at as u32 % 64) >> (at % 8)) as u8)
                .collect();
            for kind in 1..=7 {
                let record = sealed(kind, &body);
                let _ = Message::decode(&record);
                let _ = Entry::decode(&record);
                let _ = HardState::decode(&record);
                let _ = ConfState::decode(&record);
                let _ = Snapshot::decode(&record);
                let _ = ConfChange::decode(&record);
                let _ = ConfChangeV2::decode(&record);
            }
        }
    }
}
