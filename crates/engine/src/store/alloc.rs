//! The allocator (docs/design/engine-structure.md §3): a reference count for each extent of the
//! file, persisted with each checkpoint and read back at open, where SplinterDB invalidates its
//! map at every checkpoint and cannot rebuild it after a crash (research/34 §0).
//!
//! An extent freed while the durable checkpoint may still name it must not be written over: a
//! crash before the next checkpoint is durable recovers that checkpoint, and its pages must be
//! intact. So an extent whose count falls to zero waits in `pending` and is reused only once a
//! checkpoint that no longer names it is durable ([`Allocator::durable`]). Extent 0 holds the two
//! superblock copies and is never freed.

use crate::error::{Error, Malformed};

/// Extent reference counts, the extents free to reuse, and those freed since the durable
/// checkpoint. Every list is bounded by `limit`, the extents the file may hold.
#[derive(Debug)]
pub struct Allocator {
    refs: Vec<u32>,
    free: Vec<u64>,
    pending: Vec<u64>,
    limit: u64,
    /// A maintenance worker's: it holds its grant alone and never grows the file
    /// ([`Allocator::granted`]).
    granted: bool,
}

fn corrupt(why: Malformed) -> Error {
    Error::Corruption {
        what: "the allocator map",
        why,
    }
}

fn index(extent: u64) -> Result<usize, Error> {
    usize::try_from(extent).map_err(|_| Error::InvalidArgument {
        what: "an extent past the address space",
    })
}

impl Allocator {
    /// A new file's allocator: extent 0, the superblocks', held for good. `limit` is at least 1.
    pub fn new(limit: u64) -> Self {
        Self {
            refs: vec![1],
            free: Vec::new(),
            pending: Vec::new(),
            limit: limit.max(1),
            granted: false,
        }
    }

    /// The allocator a checkpoint's map gives back: `refs` as persisted, every unreferenced
    /// extent free. Extent 0 must be held.
    pub fn from_refs(refs: Vec<u32>, limit: u64) -> Result<Self, Error> {
        if refs.first().copied().unwrap_or(0) == 0 {
            return Err(corrupt(Malformed::Forbidden));
        }
        let held = u64::try_from(refs.len()).map_err(|_| corrupt(Malformed::TooLarge))?;
        if held > limit {
            return Err(corrupt(Malformed::TooLarge));
        }
        let mut free = Vec::new();
        for (extent, &count) in refs.iter().enumerate().skip(1) {
            if count == 0 {
                free.push(u64::try_from(extent).map_err(|_| corrupt(Malformed::TooLarge))?);
            }
        }
        // Lowest first off the stack, so a file's low extents are reused before it grows.
        free.reverse();
        Ok(Self {
            refs,
            free,
            pending: Vec::new(),
            limit,
            granted: false,
        })
    }

    /// A maintenance worker's allocator: the extents `grant`ed it by the shard's, and no other.
    /// Its counts span the granted extents alone, a few kilobytes, and it never grows the file
    /// past them: an extent past the grant is refused (`LimitExceeded`), so the shard sizes the
    /// grant to the job ([`crate::store::Store::grant`]). Nothing it holds is ever persisted.
    pub fn granted(grant: &[u64]) -> Result<Self, Error> {
        let most = grant.iter().copied().max().unwrap_or(0);
        let len = index(most)?.checked_add(1).ok_or(Error::InvalidArgument {
            what: "an extent past the address space",
        })?;
        let mut free = grant.to_vec();
        // Lowest first off the stack, as the shard's allocator reuses extents.
        free.sort_unstable_by(|a, b| b.cmp(a));
        Ok(Self {
            refs: vec![0; len],
            free,
            pending: Vec::new(),
            limit: u64::try_from(len).unwrap_or(u64::MAX),
            granted: true,
        })
    }

    /// A worker's granted extents it never wrote, or released again: given back to the shard.
    pub fn unused(&mut self) -> Vec<u64> {
        let mut out = std::mem::take(&mut self.free);
        out.append(&mut self.pending);
        out
    }

    /// An extent for new pages, its count 1: a free one, else one past the file's end. Refused
    /// once the file holds `limit` extents and none is free.
    pub fn allocate(&mut self) -> Result<u64, Error> {
        if let Some(extent) = self.free.pop() {
            if let Some(count) = self.refs.get_mut(index(extent)?) {
                *count = 1;
            }
            return Ok(extent);
        }
        // A worker's allocator holds its grant alone: past it, refused rather than grown.
        if self.granted {
            return Err(Error::LimitExceeded {
                what: "extents granted a maintenance worker",
                limit: self.limit,
            });
        }
        let extent = u64::try_from(self.refs.len()).map_err(|_| Error::LimitExceeded {
            what: "extents of a store",
            limit: self.limit,
        })?;
        if extent >= self.limit {
            return Err(Error::LimitExceeded {
                what: "extents of a store",
                limit: self.limit,
            });
        }
        self.refs.push(1);
        Ok(extent)
    }

    /// One more reference to a held extent.
    pub fn retain(&mut self, extent: u64) -> Result<(), Error> {
        let count = self
            .refs
            .get_mut(index(extent)?)
            .filter(|c| **c > 0)
            .ok_or(Error::InvalidArgument {
                what: "a reference to an extent that is not held",
            })?;
        *count = count.checked_add(1).ok_or(Error::LimitExceeded {
            what: "references to an extent",
            limit: u64::from(u32::MAX),
        })?;
        Ok(())
    }

    /// One reference fewer; at none, the extent waits for the next durable checkpoint.
    pub fn release(&mut self, extent: u64) -> Result<(), Error> {
        if extent == 0 {
            return Err(Error::InvalidArgument {
                what: "a release of the superblocks' extent",
            });
        }
        let count = self
            .refs
            .get_mut(index(extent)?)
            .filter(|c| **c > 0)
            .ok_or(Error::InvalidArgument {
                what: "a release of an extent that is not held",
            })?;
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.pending.push(extent);
        }
        Ok(())
    }

    /// Takes back an extent allocated and never named by any checkpoint, held once: free again
    /// at once, since no durable checkpoint can name it (a worker's grant it never wrote).
    pub fn give_back(&mut self, extent: u64) -> Result<(), Error> {
        let count = self
            .refs
            .get_mut(index(extent)?)
            .filter(|c| **c == 1)
            .ok_or(Error::InvalidArgument {
                what: "an extent given back that is not held once",
            })?;
        *count = 0;
        self.free.push(extent);
        Ok(())
    }

    /// A checkpoint that names none of the pending extents is durable: they are free to reuse.
    pub fn durable(&mut self) {
        self.free.append(&mut self.pending);
    }

    /// Whether `extent` is held.
    pub fn is_held(&self, extent: u64) -> bool {
        index(extent)
            .ok()
            .and_then(|i| self.refs.get(i))
            .is_some_and(|&c| c > 0)
    }

    /// The reference counts, extent by extent: what a checkpoint persists.
    pub fn refs(&self) -> &[u32] {
        &self.refs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_freed_extent_is_reused_only_after_a_durable_checkpoint() {
        let mut a = Allocator::new(8);
        let e = a.allocate().unwrap();
        assert_eq!(e, 1);
        a.release(e).unwrap();
        assert!(!a.is_held(e));
        // Not yet reusable: the durable checkpoint may name it.
        assert_eq!(a.allocate().unwrap(), 2);
        a.durable();
        assert_eq!(a.allocate().unwrap(), 1);
    }

    #[test]
    fn the_limit_refuses_and_the_superblocks_extent_is_never_freed() {
        let mut a = Allocator::new(2);
        assert_eq!(a.allocate().unwrap(), 1);
        assert!(matches!(a.allocate(), Err(Error::LimitExceeded { .. })));
        assert!(a.release(0).is_err());
        assert!(a.release(5).is_err());
        assert!(a.retain(3).is_err());
    }

    #[test]
    fn a_map_read_back_frees_what_it_does_not_hold() {
        let a = Allocator::from_refs(vec![1, 0, 2, 0], 8).unwrap();
        let mut a = a;
        assert_eq!(a.allocate().unwrap(), 1);
        assert_eq!(a.allocate().unwrap(), 3);
        assert_eq!(a.allocate().unwrap(), 4);
        assert!(Allocator::from_refs(vec![0, 1], 8).is_err());
        assert!(Allocator::from_refs(vec![1; 9], 8).is_err());
    }
}
