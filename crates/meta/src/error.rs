//! What a range's state machines fail with.

use crate::engine::EngineError;
use crate::record::RecordError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MetaError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Record(#[from] RecordError),
    /// The range's rows do not decode, or contradict one another.
    #[error("the range's rows are inconsistent")]
    Corrupt,
    /// The range's clock reached its last instant, which only a proposal stamped at the end
    /// of time can bring about.
    #[error("the range's clock has no later instant")]
    ClockExhausted,
}
