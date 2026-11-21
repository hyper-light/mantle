//! What an image made a slice a drive keeps of its slices (docs/design/replica.md §4), and what
//! they tell when the image fails its check before it leaves (`RangeMachine::image`): which spans
//! of its bytes changed after their slice's CRC-32C was taken, and, against the rows the engine
//! still holds, the first byte that changed and what stood there. A gate run on 2026-11-20 fenced
//! a leader on an image of 598 MB whose checksum its bytes did not give, with no word of where.

use std::fmt;

use crate::image::{self, ImageError};

/// The bytes of the image and of the engine's rows shown from the first that differs, as the
/// owner asked of a failed check's report (2026-11-20).
pub const SHOWN: usize = 32;

/// A run of an image's bytes made by consecutive slices.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Span {
    /// Its first byte's offset in the image.
    pub at: u64,
    /// Its bytes.
    pub len: u64,
    /// The image's running CRC-32C over its rows' bytes through this span's last, as written: the
    /// span's bytes are as written while extending the span before's by them gives it, so each
    /// span is checked alone and a merge keeps the later's.
    pub crc: u32,
    /// The drives that stopped on a page loading ([`hyper_durable::Imaged::Waits`]) through its
    /// last slice, of the image's every one.
    pub waits: u64,
    /// The drives its first and last slices were written at.
    pub drives: (u64, u64),
    /// The clock when its last slice was written (`crate::clock_ns`).
    pub written_ns: u64,
}

impl Span {
    /// The offset past its last byte.
    pub fn end(&self) -> Option<u64> {
        self.at.checked_add(self.len)
    }

    /// Its bytes in `image`, where the image holds them.
    pub fn bytes<'a>(&self, image: &'a [u8]) -> Option<&'a [u8]> {
        let at = usize::try_from(self.at).ok()?;
        let end = usize::try_from(self.end()?).ok()?;
        image.get(at..end)
    }
}

/// A slice as a drive made it: where it began, its bytes, the image's running CRC-32C through
/// it, the drive, the clock then, and whether it stopped on a page loading.
#[derive(Clone, Copy, Debug)]
pub struct Slice {
    pub at: u64,
    pub len: u64,
    pub crc: u32,
    pub drive: u64,
    pub ns: u64,
    pub waited: bool,
}

/// The first drive that found the image's end other than where its last slice left it: bytes
/// written or taken between drives by something other than a slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Moved {
    pub drive: u64,
    pub left: u64,
    pub found: u64,
}

/// An image's spans, each holding `per` slices but the last, at most `most`: as many as one
/// slice's bytes hold spans, so the record holds no more memory than a slice of the image does,
/// the quantum a drive of the image takes (the shell's committed page, 1 MiB as a node runs it,
/// holds 18,724 spans of 56 bytes). Full, its spans merge pairwise and each holds twice the
/// slices it did, so a span holds at most 2/`most` of the image but its last; the 598 MB image
/// fenced on 2026-11-20 took about 570 slices of that page, a span each, and a failed check
/// reads one slice's rows again.
#[derive(Debug)]
pub struct Spans {
    spans: Vec<Span>,
    most: usize,
    /// The slices a span holds before the next begins: one, doubled at each merge.
    per: u64,
    /// The slices the last span holds.
    held: u64,
    /// The drives that stopped on a page loading.
    waits: u64,
    moved: Option<Moved>,
}

impl Spans {
    /// A record for an image made in slices of `slice` bytes, holding no more spans than the
    /// slice's bytes hold, two at least, so a merge halves it; grown as slices are recorded.
    pub fn new(slice: u64) -> Self {
        let most = usize::try_from(slice)
            .unwrap_or(usize::MAX)
            .checked_div(std::mem::size_of::<Span>())
            .unwrap_or(0)
            .max(2);
        Self {
            spans: Vec::new(),
            most,
            per: 1,
            held: 0,
            waits: 0,
            moved: None,
        }
    }

    /// Records a drive's slice: added to the last span while it holds fewer than `per` slices
    /// and the slice begins where it ends, a span of its own otherwise, the record merged
    /// pairwise first when full. A slice of no bytes counts its wait only. None past `u64`, or
    /// with no memory for a span.
    pub fn record(&mut self, slice: Slice) -> Option<()> {
        if slice.waited {
            self.waits = self.waits.saturating_add(1);
        }
        let end = match self.spans.last() {
            Some(last) => last.end()?,
            None => u64::try_from(image::HEAD).ok()?,
        };
        let moved = slice.at != end;
        if moved && self.moved.is_none() {
            self.moved = Some(Moved {
                drive: slice.drive,
                left: end,
                found: slice.at,
            });
        }
        if slice.len == 0 {
            if let Some(last) = self.spans.last_mut() {
                last.waits = self.waits;
            }
            return Some(());
        }
        if let Some(last) = self.spans.last_mut()
            && !moved
            && self.held < self.per
        {
            last.len = slice.at.checked_add(slice.len)?.checked_sub(last.at)?;
            last.crc = slice.crc;
            last.waits = self.waits;
            last.drives.1 = slice.drive;
            last.written_ns = slice.ns;
            self.held = self.held.checked_add(1)?;
            return Some(());
        }
        if self.spans.len() >= self.most {
            self.merge()?;
        }
        if self.spans.len() == self.spans.capacity() {
            // Doubled as it grows, never past its most, which a merge left room under.
            let room = self.most.saturating_sub(self.spans.len());
            let more = self.spans.len().max(1).min(room);
            self.spans.try_reserve_exact(more).ok()?;
        }
        self.spans.push(Span {
            at: slice.at,
            len: slice.len,
            crc: slice.crc,
            waits: self.waits,
            drives: (slice.drive, slice.drive),
            written_ns: slice.ns,
        });
        self.held = 1;
        Some(())
    }

    /// Merges the spans pairwise, an odd last one kept, each then holding twice the slices.
    fn merge(&mut self) -> Option<()> {
        let pairs = self.spans.len() / 2;
        for i in 0..pairs {
            let j = i.checked_mul(2)?;
            let (a, b) = (*self.spans.get(j)?, *self.spans.get(j.checked_add(1)?)?);
            *self.spans.get_mut(i)? = Span {
                at: a.at,
                len: b.end()?.checked_sub(a.at)?,
                crc: b.crc,
                waits: b.waits,
                drives: (a.drives.0, b.drives.1),
                written_ns: b.written_ns,
            };
        }
        let odd = self.spans.len() % 2;
        if odd == 1 {
            let last = *self.spans.last()?;
            *self.spans.get_mut(pairs)? = last;
        }
        self.spans.truncate(pairs.checked_add(odd)?);
        self.per = self.per.checked_mul(2)?;
        Some(())
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn get(&self, i: usize) -> Option<&Span> {
        self.spans.get(i)
    }

    pub fn waits(&self) -> u64 {
        self.waits
    }

    pub fn moved(&self) -> Option<Moved> {
        self.moved
    }

    /// The spans whose bytes in `image` no longer give their CRC, or that it no longer holds.
    pub fn changed(&self, image: &[u8]) -> Changed {
        let mut changed = Changed::default();
        let mut before = 0u32;
        for (i, span) in self.spans.iter().enumerate() {
            let same = span
                .bytes(image)
                .is_some_and(|bytes| mantle_crc::crc32c_extend(before, bytes) == span.crc);
            if !same {
                changed.count = changed.count.saturating_add(1);
                changed.first.get_or_insert(i);
            }
            before = span.crc;
        }
        changed
    }
}

/// The spans that changed: how many, and the first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Changed {
    pub count: usize,
    pub first: Option<usize>,
}

/// Where a span's bytes in the image first differ from the engine's rows rebuilt for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diff {
    /// The image's offset of the first byte that differs.
    pub at: u64,
    /// The bytes from it through the last that differs, and how many of them differ.
    pub run: u64,
    pub differing: u64,
    /// The engine's rows ended this many bytes short of the span.
    pub short: u64,
    /// Up to [`SHOWN`] bytes from it, the image's and the engine's.
    pub got: Vec<u8>,
    pub expected: Vec<u8>,
    /// The offset in the span of the row that holds it, the same in both before it.
    pub row: usize,
}

/// Where `got`, a span's bytes in the image beginning at `at`, differs from `expected`, the
/// engine's rows rebuilt for it, at most its length: none where they are the same.
pub fn diff(at: u64, got: &[u8], expected: &[u8]) -> Option<Diff> {
    let both = got.len().min(expected.len());
    let pairs = || got.iter().zip(expected).enumerate();
    let first = match pairs().find(|(_, (g, e))| g != e) {
        Some((i, _)) => i,
        None if got.len() != expected.len() => both,
        None => return None,
    };
    let last = pairs()
        .rev()
        .find(|(_, (g, e))| g != e)
        .map_or(first, |(i, _)| i);
    let differing = pairs().filter(|(_, (g, e))| g != e).count();
    let shown = |bytes: &[u8]| {
        let end = first.saturating_add(SHOWN).min(bytes.len());
        bytes.get(first..end).unwrap_or_default().to_vec()
    };
    let mut row = 0usize;
    while let Some((_, _, len)) = expected.get(row..).and_then(image::next_row) {
        match row.checked_add(len) {
            Some(next) if next <= first => row = next,
            _ => break,
        }
    }
    Some(Diff {
        at: at.checked_add(u64::try_from(first).ok()?)?,
        run: u64::try_from(last.checked_sub(first)?.checked_add(1)?).ok()?,
        differing: u64::try_from(differing).ok()?,
        short: u64::try_from(got.len().saturating_sub(expected.len())).ok()?,
        got: shown(got),
        expected: shown(expected),
        row,
    })
}

/// What reading a changed span's rows again from the engine found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reread {
    /// No span changed.
    None,
    /// Not read again, and why: a page not in memory now, or the rows before it unreadable.
    Not(&'static str),
    /// Its bytes are the engine's rows: the checksum it was written with changed, not its bytes.
    Same,
    Differs(Diff),
}

/// What the findings read as (the commit that added them says why each reads so).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pattern {
    /// Bytes changed in place: the row there is the engine's row, its bytes changed, the bytes
    /// after it as written.
    Corruption,
    /// The running checksum changed, not the bytes.
    Checksum,
    /// The image lacks a row of the engine's there, or holds one the engine does not, or its
    /// bytes moved between drives: the bytes after it moved.
    Shift,
    /// The image holds another of the engine's rows there, of a key before the engine's.
    Order,
    /// None of these.
    Unknown,
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Corruption => "memory corruption, bytes changed in place",
            Self::Checksum => "memory corruption of the running checksum, not of the bytes",
            Self::Shift => "a row missing or extra (logic), the bytes after it moved",
            Self::Order => "rows out of order or duplicated, another of the engine's rows there",
            Self::Unknown => "no known pattern",
        })
    }
}

/// What a leader's image that failed its check tells of itself: the check, its spans, the first
/// that changed with the drives and time since it was written, and the engine's rows against it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forensics {
    pub error: ImageError,
    pub spans: usize,
    pub changed: Changed,
    /// The first span that changed.
    pub span: Option<Span>,
    /// The drives that stopped on a page loading while it was made.
    pub waits_in: u64,
    /// From its last slice to the check: the drives, the nanoseconds, and the drives that
    /// stopped on a page loading.
    pub since: (u64, u64, u64),
    pub moved: Option<Moved>,
    pub reread: Reread,
    pub pattern: Pattern,
}

impl Forensics {
    /// The findings of `spans` against `image`, which failed `error`, at the check made at
    /// `drive` and `now_ns`, read again as `reread` found.
    pub fn of(error: ImageError, spans: &Spans, image: &[u8], drive: u64, now_ns: u64) -> Self {
        let changed = spans.changed(image);
        let first = changed.first.and_then(|i| spans.get(i).copied());
        let before = changed
            .first
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| spans.get(i))
            .map_or(0, |span| span.waits);
        Self {
            error,
            spans: spans.len(),
            changed,
            span: first,
            waits_in: first.map_or(0, |span| span.waits.saturating_sub(before)),
            since: first.map_or((0, 0, 0), |span| {
                (
                    drive.saturating_sub(span.drives.1),
                    now_ns.saturating_sub(span.written_ns),
                    spans.waits().saturating_sub(span.waits),
                )
            }),
            moved: spans.moved(),
            reread: Reread::None,
            pattern: Pattern::Unknown,
        }
    }

    /// Whether every span from the first changed on changed.
    fn runs_to_end(&self) -> bool {
        self.changed.first.is_some_and(|first| {
            self.changed.count > 1 && self.spans.checked_sub(first) == Some(self.changed.count)
        })
    }

    /// The pattern the findings read as, `keys` saying of the row that holds the first byte that
    /// differs whether the image's and the engine's keys there are the same (`Some(true)`), and
    /// if not, whether the image's is a row the engine holds before the engine's (`Order`).
    pub fn read(&mut self, keys: Keys) {
        self.pattern = match (&self.reread, keys) {
            (Reread::Same, _) => Pattern::Checksum,
            (Reread::Differs(_), Keys::Order) => Pattern::Order,
            (Reread::Differs(_), Keys::Other) => Pattern::Shift,
            (Reread::Differs(_), Keys::Same) => Pattern::Corruption,
            _ if self.moved.is_some() || self.runs_to_end() => Pattern::Shift,
            (Reread::Differs(_), Keys::Unread) if self.changed.count == 1 => Pattern::Corruption,
            _ => Pattern::Unknown,
        };
    }
}

/// The keys of the row that holds the first byte that differs, the image's against the engine's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keys {
    /// The same key: the row's bytes changed in place.
    Same,
    /// The image's row is another of the engine's, of a key before the engine's there.
    Order,
    /// The image's key is past the engine's there, or one the engine does not hold.
    Other,
    /// Either row does not read whole.
    Unread,
}

/// `bytes` in hex.
struct Hex<'a>(&'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

impl fmt::Display for Forensics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}; ", self.error)?;
        let (Some(first), Some(span)) = (self.changed.first, self.span) else {
            return write!(
                f,
                "no span of {} changed, each as its slice wrote it; {}",
                self.spans, self.pattern
            );
        };
        let (drives, ns, waits) = self.since;
        write!(
            f,
            "span {first} of {} changed first, {} in all{}: bytes {} to {}, made by drives {} to \
             {}, {} of them waiting on loads, {drives} drives and {}.{:03} ms before the check, \
             {waits} waits on loads since",
            self.spans,
            self.changed.count,
            if self.runs_to_end() {
                ", every one after it"
            } else {
                ""
            },
            span.at,
            span.end().unwrap_or(u64::MAX),
            span.drives.0,
            span.drives.1,
            self.waits_in,
            ns / 1_000_000,
            ns / 1_000 % 1_000,
        )?;
        if let Some(moved) = self.moved {
            write!(
                f,
                "; drive {} found the image's end at byte {} where its last slice left it at {}",
                moved.drive, moved.found, moved.left
            )?;
        }
        match &self.reread {
            Reread::None => {}
            Reread::Not(why) => write!(f, "; its rows not read again: {why}")?,
            Reread::Same => write!(f, "; its bytes are the engine's rows")?,
            Reread::Differs(diff) => {
                write!(
                    f,
                    "; it differs from the engine's rows from byte {} for a run of {} bytes, {} \
                     of them differing",
                    diff.at, diff.run, diff.differing
                )?;
                if diff.short > 0 {
                    write!(f, ", the engine's rows {} bytes short of it", diff.short)?;
                }
                write!(
                    f,
                    ": {} there against {} expected",
                    Hex(&diff.got),
                    Hex(&diff.expected)
                )?;
            }
        }
        write!(f, "; {}", self.pattern)
    }
}

impl std::error::Error for Forensics {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A slice of `len` bytes at `at`, its CRC extended from `crc` over `image`'s bytes there.
    fn slice(image: &[u8], at: usize, len: usize, crc: u32, drive: u64) -> Slice {
        Slice {
            at: at as u64,
            len: len as u64,
            crc: mantle_crc::crc32c_extend(crc, &image[at..at + len]),
            drive,
            ns: drive,
            waited: drive.is_multiple_of(3),
        }
    }

    /// Recorded a slice at a time past the spans a slice's bytes hold, the record keeps at most
    /// that many,
    /// each holding the same slices but the last, end to end from the image's head, and finds
    /// a byte changed in the span that holds it, and every span past bytes taken out.
    #[test]
    fn spans_stay_bounded_and_find_what_changed() {
        const SPANS: usize = 1_024;
        let slices = SPANS * 5 + 3;
        let image: Vec<u8> = (0..image::HEAD + slices * 10)
            .map(|i| u8::try_from(i * 7 % 251).unwrap())
            .collect();
        // A span's 56 bytes, so a node's committed page of 1 MiB holds 18,724.
        assert_eq!(std::mem::size_of::<Span>(), 56);
        assert_eq!(Spans::new(1 << 20).most, 18_724);
        let mut spans = Spans::new((SPANS * std::mem::size_of::<Span>()) as u64);
        assert_eq!(spans.most, SPANS);
        let mut crc = 0;
        for i in 0..slices {
            let s = slice(&image, image::HEAD + i * 10, 10, crc, i as u64 + 1);
            crc = s.crc;
            spans.record(s).unwrap();
            assert!(spans.len() <= SPANS);
            assert!(spans.spans.capacity() <= SPANS);
        }
        assert_eq!(spans.per, 8);
        let mut end = image::HEAD as u64;
        for i in 0..spans.len() {
            let span = spans.get(i).unwrap();
            assert_eq!(span.at, end);
            if i + 1 < spans.len() {
                assert_eq!(span.len, 80, "span {i}");
            }
            end = span.end().unwrap();
        }
        assert_eq!(end, image.len() as u64);
        assert_eq!(
            spans.waits(),
            (1..=slices as u64).filter(|d| d.is_multiple_of(3)).count() as u64
        );
        assert_eq!(spans.changed(&image), Changed::default());
        let mut changed = image.clone();
        changed[image::HEAD + 80 * 7 + 3] ^= 1;
        assert_eq!(
            spans.changed(&changed),
            Changed {
                count: 1,
                first: Some(7)
            }
        );
        let mut taken = image.clone();
        taken.drain(image::HEAD + 80 * 7 + 3..image::HEAD + 80 * 7 + 13);
        let found = spans.changed(&taken);
        assert_eq!(found.first, Some(7));
        assert_eq!(found.count, spans.len() - 7);
    }

    /// A diff names the first byte that differs, the run through the last, the row that holds
    /// it, and the bytes there in both.
    #[test]
    fn a_diff_names_the_first_byte_and_its_row() {
        let mut rows = Vec::new();
        for i in 0..4u8 {
            image::push_row(&mut rows, &[b'k', i], &[i; 5]).unwrap();
        }
        // Rows of 15 bytes: the third's value's second byte.
        let mut got = rows.clone();
        got[15 * 2 + 11] ^= 0xff;
        let diff = diff(100, &got, &rows).unwrap();
        assert_eq!(
            (diff.at, diff.run, diff.differing, diff.row),
            (141, 1, 1, 30)
        );
        assert_eq!(diff.got.len(), rows.len() - 41);
        assert_eq!(diff.expected[0], 2);
        assert_eq!(super::diff(0, &rows, &rows), None);
    }
}
