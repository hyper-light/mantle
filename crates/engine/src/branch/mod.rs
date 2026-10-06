//! A branch (docs/design/engine-structure.md §4, step E3): an immutable B-tree of store pages
//! packed in one pass from entries in ascending key order, as SplinterDB packs a memtable or a
//! compaction's output (research/34 §1). Its index entries carry their subtree's entry count, so
//! a range's size is known without reading its leaves.
//!
//! A page's payload, little-endian:
//!
//! | Bytes | Field |
//! |---|---|
//! | 0 | kind: 1 leaf, 2 index |
//! | 1..3 | entries, `n` |
//! | 3..5 | the prefix every key of the page shares, its length `p` |
//! | 5..5+p | the prefix |
//! | then `2n` | each entry's offset in the payload, in key order, for binary search |
//! | then | the entries |
//!
//! A leaf entry is the key's suffix length (2 bytes), the operation (1), the value's length (2),
//! the suffix and the value. An index entry is the suffix length (2), the child's page (8), the
//! entries under the child (8) and the suffix of the child's first key. The prefix is the longest
//! one the page's first and last keys share, which, the keys being sorted, every key between them
//! shares: object-store keys are paths, and their shared prefixes are most of each key.

use crate::error::{Error, Malformed};
use crate::store::Store;
use hyper_block::block::BlockFile;
use std::cmp::Ordering;

/// What an entry does to its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// The key holds the value.
    Put,
    /// The key is deleted: a tombstone shadowing older branches.
    Delete,
}

impl Op {
    fn byte(self) -> u8 {
        match self {
            Self::Put => 1,
            Self::Delete => 2,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

const LEAF: u8 = 1;
const INDEX: u8 = 2;
/// The page payload's fixed head: kind, entries, prefix length.
const HEAD: usize = 5;
/// A leaf entry's fixed bytes: suffix length, operation, value length.
const LEAF_FIXED: usize = 5;
/// An index entry's fixed bytes: suffix length, child page, entries under the child.
const INDEX_FIXED: usize = 18;
/// An entry's offset in the payload.
const OFFSET: usize = 2;

/// A built branch: its root page, its height (1 for a lone leaf), its entries, and the extents
/// holding its pages, each held once by the branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Branch {
    /// The root page.
    pub root: u64,
    /// Levels from the root to the leaves, 1 for a branch of one leaf.
    pub height: u8,
    /// Entries in the branch.
    pub count: u64,
    /// The extents holding its pages.
    pub extents: Vec<u64>,
}

fn corrupt(why: Malformed) -> Error {
    Error::Corruption {
        what: "a branch page",
        why,
    }
}

fn shared(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn u16_of(n: usize) -> Result<[u8; 2], Error> {
    u16::try_from(n)
        .map(u16::to_le_bytes)
        .map_err(|_| corrupt(Malformed::TooLarge))
}

/// One page being filled: its entries' keys and their encoded rest, until it is full.
#[derive(Debug, Default)]
struct Page {
    /// Keys, back to back, and where each ends.
    keys: Vec<u8>,
    key_ends: Vec<usize>,
    /// Each entry's bytes after its suffix length (operation and value, or child and count).
    rests: Vec<u8>,
    rest_ends: Vec<usize>,
    prefix: usize,
    /// Leaf entries' total under the page (1 each), or an index page's children's totals.
    total: u64,
}

impl Page {
    fn len(&self) -> usize {
        self.key_ends.len()
    }

    fn key(&self, i: usize) -> &[u8] {
        let start = i
            .checked_sub(1)
            .and_then(|j| self.key_ends.get(j))
            .copied()
            .unwrap_or(0);
        let end = self.key_ends.get(i).copied().unwrap_or(start);
        self.keys.get(start..end).unwrap_or(&[])
    }

    fn rest(&self, i: usize) -> &[u8] {
        let start = i
            .checked_sub(1)
            .and_then(|j| self.rest_ends.get(j))
            .copied()
            .unwrap_or(0);
        let end = self.rest_ends.get(i).copied().unwrap_or(start);
        self.rests.get(start..end).unwrap_or(&[])
    }

    /// The payload the page's entries take with `prefix` and one more entry of `key` and
    /// `rest` bytes after its suffix.
    fn size_with(&self, prefix: usize, key: usize, rest: usize) -> Option<usize> {
        let n = self.len().checked_add(1)?;
        let keys = self.keys.len().checked_add(key)?;
        let suffixes = keys.checked_sub(n.checked_mul(prefix)?)?;
        HEAD.checked_add(prefix)?
            .checked_add(n.checked_mul(OFFSET.checked_add(2)?)?)?
            .checked_add(suffixes)?
            .checked_add(self.rests.len())?
            .checked_add(rest)
    }

    fn push(&mut self, key: &[u8], rest: &[u8], under: u64) {
        self.prefix = match self.key_ends.first() {
            None => key.len(),
            Some(_) => self.prefix.min(shared(self.key(0), key)),
        };
        self.keys.extend_from_slice(key);
        self.key_ends.push(self.keys.len());
        self.rests.extend_from_slice(rest);
        self.rest_ends.push(self.rests.len());
        self.total = self.total.saturating_add(under);
    }

    fn clear(&mut self) {
        self.keys.clear();
        self.key_ends.clear();
        self.rests.clear();
        self.rest_ends.clear();
        self.prefix = 0;
        self.total = 0;
    }

    /// The page's payload into `out`.
    fn encode(&self, kind: u8, out: &mut Vec<u8>) -> Result<(), Error> {
        out.clear();
        let first = self.key(0);
        let prefix = first
            .get(..self.prefix)
            .ok_or(corrupt(Malformed::Truncated))?;
        out.push(kind);
        out.extend_from_slice(&u16_of(self.len())?);
        out.extend_from_slice(&u16_of(self.prefix)?);
        out.extend_from_slice(prefix);
        let table = out.len();
        out.resize(
            table
                .checked_add(
                    self.len()
                        .checked_mul(OFFSET)
                        .ok_or(corrupt(Malformed::TooLarge))?,
                )
                .ok_or(corrupt(Malformed::TooLarge))?,
            0,
        );
        for i in 0..self.len() {
            let at = out.len();
            let slot = table
                .checked_add(i.checked_mul(OFFSET).ok_or(corrupt(Malformed::TooLarge))?)
                .ok_or(corrupt(Malformed::TooLarge))?;
            out.get_mut(slot..slot.saturating_add(OFFSET))
                .ok_or(corrupt(Malformed::TooLarge))?
                .copy_from_slice(&u16_of(at)?);
            let suffix = self
                .key(i)
                .get(self.prefix..)
                .ok_or(corrupt(Malformed::Truncated))?;
            out.extend_from_slice(&u16_of(suffix.len())?);
            out.extend_from_slice(self.rest(i));
            out.extend_from_slice(suffix);
        }
        Ok(())
    }
}

/// Packs entries given in strictly ascending key order into a branch's pages in `store`.
#[derive(Debug)]
pub struct Builder {
    /// Level 0 the leaves, then each index level.
    levels: Vec<Page>,
    /// The extent pages are being written into, and the next page in it.
    extent: Option<(u64, u32)>,
    extents: Vec<u64>,
    last: Vec<u8>,
    count: u64,
    payload: Vec<u8>,
    capacity: usize,
}

impl Builder {
    /// A builder for pages of `capacity` payload bytes (`Store::page_capacity`), which offsets of
    /// 16 bits must reach.
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity > usize::from(u16::MAX) {
            return Err(Error::InvalidArgument {
                what: "a branch page past 64 KiB",
            });
        }
        Ok(Self {
            levels: vec![Page::default()],
            extent: None,
            extents: Vec::new(),
            last: Vec::new(),
            count: 0,
            payload: Vec::with_capacity(capacity),
            capacity,
        })
    }

    /// The largest key and value an entry may have: with a page's fixed bytes, one entry, and
    /// room for its key twice in an index page above, it fits an empty page.
    pub fn fits(&self, key: usize, value: usize) -> bool {
        let leaf = HEAD
            .saturating_add(OFFSET)
            .saturating_add(LEAF_FIXED)
            .saturating_add(key.saturating_mul(2))
            .saturating_add(value);
        let index = HEAD
            .saturating_add(OFFSET.saturating_add(INDEX_FIXED).saturating_mul(2))
            .saturating_add(key.saturating_mul(3));
        leaf <= self.capacity && index <= self.capacity
    }

    fn page<F: BlockFile>(&mut self, store: &mut Store<F>) -> Result<u64, Error> {
        let (extent, next) = match self.extent {
            Some((e, n)) if store.address(e, n).is_ok() => (e, n),
            _ => {
                let e = store.allocate_extent()?;
                self.extents.push(e);
                (e, 0)
            }
        };
        self.extent = Some((extent, next.saturating_add(1)));
        store.address(extent, next)
    }

    /// Adds an entry; keys must strictly ascend.
    pub fn add<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        key: &[u8],
        op: Op,
        value: &[u8],
    ) -> Result<(), Error> {
        if self.count > 0 && key.cmp(&self.last) != Ordering::Greater {
            return Err(corrupt(Malformed::OutOfOrder));
        }
        if !self.fits(key.len(), value.len()) {
            return Err(Error::LimitExceeded {
                what: "an entry's bytes in a branch page",
                limit: u64::try_from(self.capacity).unwrap_or(u64::MAX),
            });
        }
        let mut rest = Vec::with_capacity(LEAF_FIXED);
        rest.push(op.byte());
        rest.extend_from_slice(&u16_of(value.len())?);
        rest.extend_from_slice(value);
        self.insert(store, 0, key, &rest, 1)?;
        self.last.clear();
        self.last.extend_from_slice(key);
        self.count = self.count.saturating_add(1);
        Ok(())
    }

    /// Adds an entry to level `level`, writing the page out first if it would not fit.
    fn insert<F: BlockFile>(
        &mut self,
        store: &mut Store<F>,
        level: usize,
        key: &[u8],
        rest: &[u8],
        under: u64,
    ) -> Result<(), Error> {
        let page = self.levels.get(level).ok_or(corrupt(Malformed::TooLarge))?;
        let prefix = if page.len() == 0 {
            key.len()
        } else {
            page.prefix.min(shared(page.key(0), key))
        };
        let size = page
            .size_with(prefix, key.len(), rest.len())
            .ok_or(corrupt(Malformed::TooLarge))?;
        if size > self.capacity && page.len() > 0 {
            self.write(store, level)?;
        }
        self.levels
            .get_mut(level)
            .ok_or(corrupt(Malformed::TooLarge))?
            .push(key, rest, under);
        Ok(())
    }

    /// Writes level `level`'s page and adds its entry to the level above.
    fn write<F: BlockFile>(&mut self, store: &mut Store<F>, level: usize) -> Result<u64, Error> {
        let kind = if level == 0 { LEAF } else { INDEX };
        let mut payload = std::mem::take(&mut self.payload);
        let page = self.levels.get(level).ok_or(corrupt(Malformed::TooLarge))?;
        page.encode(kind, &mut payload)?;
        let first = page.key(0).to_vec();
        let total = page.total;
        let address = self.page(store)?;
        store.write_page(address, &payload)?;
        self.payload = payload;
        if let Some(page) = self.levels.get_mut(level) {
            page.clear();
        }
        let above = level.checked_add(1).ok_or(corrupt(Malformed::TooLarge))?;
        if self.levels.len() <= above {
            // A level holds at most a page of entries before it writes; the height is bounded
            // by the entries' count, logarithmically.
            self.levels.push(Page::default());
        }
        let mut rest = Vec::with_capacity(16);
        rest.extend_from_slice(&address.to_le_bytes());
        rest.extend_from_slice(&total.to_le_bytes());
        self.insert(store, above, &first, &rest, total)?;
        Ok(address)
    }

    /// Writes every page left and returns the branch; a builder given no entry is refused.
    pub fn finish<F: BlockFile>(mut self, store: &mut Store<F>) -> Result<Branch, Error> {
        if self.count == 0 {
            return Err(Error::InvalidArgument {
                what: "a branch with no entries",
            });
        }
        let mut level = 0usize;
        loop {
            let top = level.checked_add(1) == Some(self.levels.len());
            let lone = self.levels.get(level).is_some_and(|p| p.len() == 1) && level > 0;
            if top && lone {
                // The level's single entry names the root: the page below it.
                let page = self.levels.get(level).ok_or(corrupt(Malformed::TooLarge))?;
                let root = page
                    .rest(0)
                    .first_chunk::<8>()
                    .map(|b| u64::from_le_bytes(*b))
                    .ok_or(corrupt(Malformed::Truncated))?;
                return Ok(Branch {
                    root,
                    height: u8::try_from(level).map_err(|_| corrupt(Malformed::TooLarge))?,
                    count: self.count,
                    extents: self.extents,
                });
            }
            if self.levels.get(level).is_some_and(|p| p.len() > 0) {
                self.write(store, level)?;
            }
            level = level.checked_add(1).ok_or(corrupt(Malformed::TooLarge))?;
        }
    }
}

/// A page's decoded head: its kind, entries, prefix, and where its offsets start.
struct View<'a> {
    page: &'a [u8],
    kind: u8,
    n: usize,
    prefix: &'a [u8],
    table: usize,
}

impl<'a> View<'a> {
    fn new(page: &'a [u8]) -> Result<Self, Error> {
        let [kind, n0, n1, p0, p1] = *page
            .first_chunk::<HEAD>()
            .ok_or(corrupt(Malformed::Truncated))?;
        if kind != LEAF && kind != INDEX {
            return Err(corrupt(Malformed::UnknownTag(kind)));
        }
        let n = usize::from(u16::from_le_bytes([n0, n1]));
        let p = usize::from(u16::from_le_bytes([p0, p1]));
        let table = HEAD.checked_add(p).ok_or(corrupt(Malformed::TooLarge))?;
        let prefix = page.get(HEAD..table).ok_or(corrupt(Malformed::Truncated))?;
        if n == 0 {
            return Err(corrupt(Malformed::CountMismatch));
        }
        Ok(Self {
            page,
            kind,
            n,
            prefix,
            table,
        })
    }

    /// Entry `i`'s suffix and the bytes before it (operation and value, or child and count).
    fn entry(&self, i: usize) -> Result<(&'a [u8], &'a [u8]), Error> {
        let slot = self
            .table
            .checked_add(i.checked_mul(OFFSET).ok_or(corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))?;
        let at = self
            .page
            .get(slot..)
            .and_then(<[u8]>::first_chunk::<2>)
            .map(|b| usize::from(u16::from_le_bytes(*b)))
            .ok_or(corrupt(Malformed::Truncated))?;
        let len = self
            .page
            .get(at..)
            .and_then(<[u8]>::first_chunk::<2>)
            .map(|b| usize::from(u16::from_le_bytes(*b)))
            .ok_or(corrupt(Malformed::Truncated))?;
        let body = at.checked_add(2).ok_or(corrupt(Malformed::TooLarge))?;
        let fixed = if self.kind == LEAF {
            let vlen = self
                .page
                .get(body.saturating_add(1)..)
                .and_then(<[u8]>::first_chunk::<2>)
                .map(|b| usize::from(u16::from_le_bytes(*b)))
                .ok_or(corrupt(Malformed::Truncated))?;
            3usize
                .checked_add(vlen)
                .ok_or(corrupt(Malformed::TooLarge))?
        } else {
            16
        };
        let suffix_at = body
            .checked_add(fixed)
            .ok_or(corrupt(Malformed::TooLarge))?;
        let rest = self
            .page
            .get(body..suffix_at)
            .ok_or(corrupt(Malformed::Truncated))?;
        let suffix = self
            .page
            .get(
                suffix_at
                    ..suffix_at
                        .checked_add(len)
                        .ok_or(corrupt(Malformed::TooLarge))?,
            )
            .ok_or(corrupt(Malformed::Truncated))?;
        Ok((suffix, rest))
    }

    /// `key` against the page's key `i`: the prefix first, then the suffix.
    fn compare(&self, key: &[u8], i: usize) -> Result<Ordering, Error> {
        let (suffix, _) = self.entry(i)?;
        let head = key.get(..self.prefix.len().min(key.len())).unwrap_or(key);
        Ok(match head.cmp(self.prefix) {
            Ordering::Equal if key.len() >= self.prefix.len() => {
                key.get(self.prefix.len()..).unwrap_or(&[]).cmp(suffix)
            }
            Ordering::Equal => Ordering::Less,
            other => other,
        })
    }

    /// The last entry whose key is at most `key`, if any.
    fn floor(&self, key: &[u8]) -> Result<Option<usize>, Error> {
        let (mut lo, mut hi) = (0usize, self.n);
        while lo < hi {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            if self.compare(key, mid)? == Ordering::Less {
                hi = mid;
            } else {
                lo = mid.saturating_add(1);
            }
        }
        Ok(lo.checked_sub(1))
    }
}

impl Branch {
    /// The entry for `key`, if the branch has one: its operation, and its value into `value`.
    /// One page read a level.
    pub fn get<F: BlockFile>(
        &self,
        store: &mut Store<F>,
        key: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<Option<Op>, Error> {
        let mut address = self.root;
        let mut buf = Vec::new();
        for _ in 0..=self.height {
            buf.clear();
            store.read_page(address, &mut buf)?;
            let view = View::new(&buf)?;
            let Some(i) = view.floor(key)? else {
                return Ok(None);
            };
            let (_, rest) = view.entry(i)?;
            if view.kind == INDEX {
                address = rest
                    .first_chunk::<8>()
                    .map(|b| u64::from_le_bytes(*b))
                    .ok_or(corrupt(Malformed::Truncated))?;
                continue;
            }
            if view.compare(key, i)? != Ordering::Equal {
                return Ok(None);
            }
            let op = rest.first().and_then(|&b| Op::from_byte(b)).ok_or(corrupt(
                Malformed::UnknownTag(rest.first().copied().unwrap_or(0)),
            ))?;
            value.clear();
            value.extend_from_slice(rest.get(3..).ok_or(corrupt(Malformed::Truncated))?);
            return Ok(Some(op));
        }
        Err(corrupt(Malformed::CountMismatch))
    }
}
