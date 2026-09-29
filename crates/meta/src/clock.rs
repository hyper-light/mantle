//! A range's clock: the time each write takes, which only moves forward. A key's versions
//! then never share an order, and a bucket re-created under the same name never repeats an
//! incarnation (docs/design/metadata.md §1–§2). Time is part of the command, as the leader
//! proposed it, so every replica assigns the same.

use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key;
use crate::record;

/// The last time the range assigned, nanoseconds since the Unix epoch.
const CLOCK: &[u8] = &[key::LOCAL, key::marker::CLOCK];

/// The time a write proposed at `at_ns` takes, the later of that and just after the range's
/// last, and the write that records it.
pub fn tick<E: Rows>(engine: &E, at_ns: u64) -> Result<(u64, Write), MetaError> {
    let next = last(engine)?
        .checked_add(1)
        .ok_or(MetaError::ClockExhausted)?;
    let time = at_ns.max(next);
    Ok((
        time,
        Write::Put(CLOCK.to_vec(), record::encode_number(time)),
    ))
}

/// The range's time at an entry proposed at `at_ns`, the later of that and the range's last,
/// read without taking an instant: what a lock's expiry is judged against.
pub fn now<E: Rows>(engine: &E, at_ns: u64) -> Result<u64, MetaError> {
    Ok(at_ns.max(last(engine)?))
}

fn last<E: Rows>(engine: &E) -> Result<u64, MetaError> {
    match engine.get(CLOCK)? {
        None => Ok(0),
        Some(bytes) => Ok(record::decode_number(&bytes, "clock")?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;

    #[test]
    fn the_clock_only_moves_forward_and_refuses_to_wrap() {
        let mut m = Model::default();
        let (first, write) = tick(&m, 100).unwrap();
        assert_eq!(first, 100);
        m.apply(1, &[write]).unwrap();
        let (second, write) = tick(&m, 50).unwrap();
        assert_eq!(
            second, 101,
            "a proposal behind the clock takes the next instant"
        );
        m.apply(2, &[write]).unwrap();
        let (last, write) = tick(&m, u64::MAX).unwrap();
        assert_eq!(last, u64::MAX);
        m.apply(3, &[write]).unwrap();
        assert_eq!(tick(&m, 0).unwrap_err(), MetaError::ClockExhausted);
    }
}
