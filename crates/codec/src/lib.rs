//! Bounds-checked little-endian encoding for on-disk structures. A reader past the end of
//! its input returns `None` instead of panicking, so a torn or corrupt structure decodes to
//! "invalid", never to a crash.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

/// Appends little-endian fields to a byte vector, or counts them.
///
/// A message is made at its length, known before its first byte is written: [`encode`] runs its
/// encoder once on a writer that only counts ([`Writer::counting`]), then once into a buffer of
/// exactly the count, so the buffer is allocated once and never grows. The count is the
/// message's own, computed from what it holds, not a guess or the last message's size.
#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
    /// The bytes written so far when the writer only counts.
    counted: Option<usize>,
}

impl Writer {
    /// A writer that reserves `capacity` bytes up front. The capacity is a hint: a reservation
    /// the allocator refuses leaves the buffer empty, and the writes that follow grow it as
    /// they would from `Default`, where `Vec::with_capacity` would panic on a capacity past
    /// `isize::MAX` bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut buf = Vec::new();
        if buf.try_reserve_exact(capacity).is_err() {
            buf = Vec::new();
        }
        Self { buf, counted: None }
    }

    /// A writer that keeps nothing and counts the bytes written to it: a message's length,
    /// computed by its own encoder.
    pub fn counting() -> Self {
        Self {
            buf: Vec::new(),
            counted: Some(0),
        }
    }

    fn put(&mut self, v: &[u8]) {
        match &mut self.counted {
            Some(n) => *n = n.saturating_add(v.len()),
            None => self.buf.extend_from_slice(v),
        }
    }

    pub fn u8(&mut self, v: u8) {
        self.put(&[v]);
    }

    pub fn u16(&mut self, v: u16) {
        self.put(&v.to_le_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.put(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.put(&v.to_le_bytes());
    }

    pub fn u128(&mut self, v: u128) {
        self.put(&v.to_le_bytes());
    }

    pub fn bytes(&mut self, v: &[u8]) {
        self.put(v);
    }

    pub fn zeros(&mut self, n: usize) {
        match &mut self.counted {
            Some(c) => *c = c.saturating_add(n),
            None => self.buf.resize(self.buf.len().saturating_add(n), 0),
        }
    }

    /// The bytes written, or counted.
    pub fn len(&self) -> usize {
        self.counted.unwrap_or(self.buf.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes written: none when the writer only counts.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// The message `put` writes, `extra` bytes more room after it for its caller's trailer (a
/// checksum), in one buffer allocated at their length: `put` counts the message first, then
/// writes it. Its errors are its own, the first pass's.
pub fn encode<E>(extra: usize, put: impl Fn(&mut Writer) -> Result<(), E>) -> Result<Writer, E> {
    let mut count = Writer::counting();
    put(&mut count)?;
    let mut w = Writer::with_capacity(count.len().saturating_add(extra));
    put(&mut w)?;
    Ok(w)
}

/// Reads little-endian fields from a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    pub fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[b]| b)
    }

    pub fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_le_bytes)
    }

    pub fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    pub fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    pub fn u128(&mut self) -> Option<u128> {
        self.array().map(u128::from_le_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fields_round_trip(a: u8, b: u16, c: u32, d: u64, e: u128) {
            let mut w = Writer::default();
            w.u8(a); w.u16(b); w.u32(c); w.u64(d); w.u128(e);
            let bytes = w.into_vec();
            let mut r = Reader::new(&bytes);
            prop_assert_eq!(r.u8(), Some(a));
            prop_assert_eq!(r.u16(), Some(b));
            prop_assert_eq!(r.u32(), Some(c));
            prop_assert_eq!(r.u64(), Some(d));
            prop_assert_eq!(r.u128(), Some(e));
            prop_assert_eq!(r.u8(), None);
        }

        /// A message encoded by `encode` is the bytes its encoder writes, in a buffer allocated
        /// at exactly their length.
        #[test]
        fn a_message_is_made_at_its_counted_length(a: u8, b: u16, s in proptest::collection::vec(any::<u8>(), 0..300), z in 0usize..40) {
            let put = |w: &mut Writer| -> Result<(), ()> {
                w.u8(a); w.u16(b); w.bytes(&s); w.zeros(z); w.u128(7);
                Ok(())
            };
            let mut grown = Writer::default();
            put(&mut grown).unwrap();
            let made = encode(4, put).unwrap();
            prop_assert_eq!(made.as_slice(), grown.as_slice());
            prop_assert_eq!(made.into_vec().capacity(), grown.len() + 4);
        }

        #[test]
        fn truncated_input_reads_none(bytes in proptest::collection::vec(any::<u8>(), 0..16)) {
            let mut r = Reader::new(&bytes);
            let _ = r.u128();
            prop_assert!(r.remaining() <= 16);
        }
    }
}
