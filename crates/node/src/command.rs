//! A command for a range as a client asks it and a node answers it, on the request class
//! (docs/design/node.md §4.2, §5.1). The request's head names the range's group and its body is
//! the entry (`mantle_meta::wire::Entry`); the reply's head says what became of it and, when the
//! entry was applied, its body holds the answers, one for each command, in the entry's order. The
//! transport checks each body's CRC-32C (CLAUDE.md §6).
//!
//! | Message | Head | Body |
//! |---|---|---|
//! | request | the group, u128 little-endian | the entry |
//! | reply | the status, u8; the leader the node knows, u64 little-endian (0: none) | answered: each answer's length, u32 little-endian, then the answer |

use mantle_meta::record::RecordError;
use mantle_meta::wire::Answer;

/// The bytes of a request's head: the group.
pub const HEAD_BYTES: usize = 16;
/// The bytes of a reply's head: the status and the leader.
pub const REPLY_HEAD_BYTES: usize = 1 + 8;
/// The bytes before each answer in a reply's body: its length.
const LENGTH_BYTES: usize = 4;

/// What became of a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The entry was applied; the body holds its answers.
    Answered,
    /// This node does not lead the range: ask the leader the head names, or, when it names none,
    /// ask again once the range has elected.
    Redirect,
    /// This node hosts no such range.
    NoRange,
    /// The leader refused the proposal: its log or its uncommitted bound is full, or it stepped
    /// down before the entry was taken. Nothing was applied.
    Refused,
    /// The request's head or body does not decode, or the entry is larger than the range takes.
    Malformed,
    /// The entry was applied, and its answers are larger than a reply carries: they are kept in
    /// the commands' sessions, and asking the same serials again returns them.
    TooLarge,
}

impl Status {
    fn code(self) -> u8 {
        match self {
            Self::Answered => 0,
            Self::Redirect => 1,
            Self::NoRange => 2,
            Self::Refused => 3,
            Self::Malformed => 4,
            Self::TooLarge => 5,
        }
    }

    fn of(code: u8) -> Option<Self> {
        [
            Self::Answered,
            Self::Redirect,
            Self::NoRange,
            Self::Refused,
            Self::Malformed,
            Self::TooLarge,
        ]
        .into_iter()
        .find(|s| s.code() == code)
    }
}

/// Why a command's message was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandError {
    #[error("a command's head of {0} bytes")]
    Head(usize),
    #[error("a reply's status {0} names none")]
    Status(u8),
    #[error("a reply's answers do not decode")]
    Answers,
    #[error("an answer: {0}")]
    Record(#[from] RecordError),
}

/// A request's head for `group`.
pub fn head(group: u128) -> [u8; HEAD_BYTES] {
    group.to_le_bytes()
}

/// The group a request's head names.
pub fn group_of(head: &[u8]) -> Result<u128, CommandError> {
    let bytes: [u8; HEAD_BYTES] = head
        .try_into()
        .map_err(|_| CommandError::Head(head.len()))?;
    Ok(u128::from_le_bytes(bytes))
}

/// A reply's head.
pub fn reply_head(status: Status, leader: u64) -> [u8; REPLY_HEAD_BYTES] {
    let mut out = [0; REPLY_HEAD_BYTES];
    let (code, rest) = out.split_at_mut(1);
    if let Some(first) = code.first_mut() {
        *first = status.code();
    }
    rest.copy_from_slice(&leader.to_le_bytes());
    out
}

/// The status and leader a reply's head states.
pub fn read_reply_head(head: &[u8]) -> Result<(Status, u64), CommandError> {
    let bytes: &[u8; REPLY_HEAD_BYTES] = head
        .try_into()
        .map_err(|_| CommandError::Head(head.len()))?;
    let (code, leader) = bytes.split_first().ok_or(CommandError::Head(0))?;
    let status = Status::of(*code).ok_or(CommandError::Status(*code))?;
    let leader: [u8; 8] = leader
        .try_into()
        .map_err(|_| CommandError::Head(head.len()))?;
    Ok((status, u64::from_le_bytes(leader)))
}

/// Appends `answers` to `out` as a reply's body.
pub fn encode_answers(out: &mut Vec<u8>, answers: &[Answer]) -> Result<(), CommandError> {
    for answer in answers {
        let encoded = answer.encode()?;
        let len = u32::try_from(encoded.len()).map_err(|_| RecordError::TooLarge(encoded.len()))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&encoded);
    }
    Ok(())
}

/// The answers a reply's body holds.
pub fn decode_answers(mut body: &[u8]) -> Result<Vec<Answer>, CommandError> {
    let mut answers = Vec::new();
    while !body.is_empty() {
        let (len, rest) = body
            .split_first_chunk::<LENGTH_BYTES>()
            .ok_or(CommandError::Answers)?;
        let len = usize::try_from(u32::from_le_bytes(*len)).map_err(|_| CommandError::Answers)?;
        let (answer, rest) = rest.split_at_checked(len).ok_or(CommandError::Answers)?;
        answers.push(Answer::decode(answer)?);
        body = rest;
    }
    Ok(answers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heads_group_reads_back_and_no_other_length_is_a_head() {
        let group = 0x0072_616e_6765_u128 << 64 | 7;
        assert_eq!(group_of(&head(group)), Ok(group));
        assert_eq!(group_of(&[0; 15]), Err(CommandError::Head(15)));
    }

    #[test]
    fn a_replys_head_reads_back_and_an_unknown_status_is_refused() {
        for status in [
            Status::Answered,
            Status::Redirect,
            Status::NoRange,
            Status::Refused,
            Status::Malformed,
            Status::TooLarge,
        ] {
            assert_eq!(read_reply_head(&reply_head(status, 3)), Ok((status, 3)));
        }
        let mut bad = reply_head(Status::Answered, 0);
        bad[0] = 6;
        assert_eq!(read_reply_head(&bad), Err(CommandError::Status(6)));
    }

    #[test]
    fn answers_read_back_and_a_cut_body_is_refused() {
        let answers = vec![
            Answer::Registered { session: 9 },
            Answer::Repeated,
            Answer::SessionsFull { until_ns: 5 },
        ];
        let mut body = Vec::new();
        encode_answers(&mut body, &answers).unwrap();
        assert_eq!(decode_answers(&body), Ok(answers));
        assert_eq!(
            decode_answers(&body[..body.len() - 1]),
            Err(CommandError::Answers)
        );
        assert_eq!(decode_answers(&[]), Ok(Vec::new()));
    }
}
