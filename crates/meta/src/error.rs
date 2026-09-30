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
    /// A merge came to be taken while this replica's copy of the range it takes is not frozen
    /// for it: the replicas would take different rows, so this one stops.
    #[error("the range a merge takes is not frozen for it on this replica")]
    Unfrozen,
    /// Memory for a list the step builds, sized by what it already holds, could not be
    /// reserved.
    #[error("memory for a range's step could not be reserved")]
    Memory,
}

/// An empty vector with room for `n` items, or [`MetaError::Memory`]: `Vec::with_capacity`
/// panics on a capacity past `isize::MAX` bytes and aborts when the allocation fails.
pub(crate) fn reserved<T>(n: usize) -> Result<Vec<T>, MetaError> {
    let mut out = Vec::new();
    out.try_reserve_exact(n).map_err(|_| MetaError::Memory)?;
    Ok(out)
}
