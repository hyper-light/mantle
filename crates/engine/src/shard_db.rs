//! One shard's engine (docs/design/engine-structure.md §2, §8): the memtable, and the trunk of
//! branches in the shard's store. A put or delete goes to the memtable; a full memtable is packed
//! into a branch and incorporated into the trunk; a get reads the memtable, then the trunk.
//!
//! Durability is the Raft log's (§2): the engine applies committed entries and makes its state
//! durable by checkpoints. A checkpoint packs the memtable into the trunk, writes the trunk's
//! image, and names it with the last applied Raft index in the store's superblock; recovery
//! opens the newest checkpoint and the caller replays the log from the index after it.

use crate::branch::{Builder, Op};
use crate::error::Error;
use crate::memtable::btree::BTreeMem;
use crate::store::{Config, Store};
use crate::trunk::{Trunk, TrunkConfig};
use hyper_block::block::BlockFile;

/// A shard's engine over a store.
#[derive(Debug)]
pub struct ShardDb<F: BlockFile> {
    store: Store<F>,
    mem: BTreeMem,
    trunk: Trunk,
}

impl<F: BlockFile> ShardDb<F> {
    /// A new engine in `file`, which must be empty: its store created, its trunk empty, its
    /// memtable bounded at `mem_limit` bytes.
    pub fn create(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<Self, Error> {
        Ok(Self {
            store: Store::create(file, store)?,
            mem: BTreeMem::new(mem_limit)?,
            trunk: Trunk::new(trunk)?,
        })
    }

    /// The engine in `file` at its newest checkpoint, and the last Raft index that checkpoint
    /// holds: the log is replayed from the next. A checkpoint with no trunk yet gives an empty
    /// trunk of `trunk`'s shape.
    pub fn open(
        file: F,
        store: Config,
        mem_limit: usize,
        trunk: TrunkConfig,
    ) -> Result<(Self, u64), Error> {
        let (mut store, recovered) = Store::open(file, store)?;
        let trunk = match recovered.root {
            Some(head) => Trunk::load(&mut store, head)?,
            None => Trunk::new(trunk)?,
        };
        Ok((
            Self {
                store,
                mem: BTreeMem::new(mem_limit)?,
                trunk,
            },
            recovered.applied,
        ))
    }

    /// Makes the engine's state through Raft index `applied` durable: the memtable packed into
    /// the trunk, the trunk's image written, and a checkpoint naming it, durable when this
    /// returns.
    pub fn checkpoint(&mut self, applied: u64) -> Result<(), Error> {
        self.flush()?;
        let head = self.trunk.save(&mut self.store)?;
        self.store.checkpoint(Some(head), applied)
    }

    /// Checks that the store holds exactly the extents the engine names, each once: the
    /// superblocks', the allocator map's, the trunk image's and every branch's. Run after
    /// recovery, it finds an extent leaked or freed while named; a difference is a typed
    /// corruption.
    pub fn check_references(&self) -> Result<(), Error> {
        let mut named = std::collections::BTreeSet::new();
        named.insert(0u64);
        let mut once = |e: u64| -> Result<(), Error> {
            if named.insert(e) {
                Ok(())
            } else {
                Err(Error::Corruption {
                    what: "the extents a shard names",
                    why: crate::error::Malformed::CountMismatch,
                })
            }
        };
        for &e in self.store.map_extents() {
            once(e)?;
        }
        for &e in self.trunk.image_extents() {
            once(e)?;
        }
        for b in self.trunk.branches() {
            for &e in &b.extents {
                once(e)?;
            }
        }
        let held = self
            .store
            .refs()
            .iter()
            .enumerate()
            .filter(|&(_, &c)| c > 0);
        let mut count = 0usize;
        for (e, &c) in held {
            let e = u64::try_from(e).map_err(|_| Error::InvalidArgument {
                what: "an extent past u64",
            })?;
            if c != 1 || !named.contains(&e) {
                return Err(Error::Corruption {
                    what: "the extents a shard holds",
                    why: crate::error::Malformed::CountMismatch,
                });
            }
            count = count.saturating_add(1);
        }
        if count != named.len() {
            return Err(Error::Corruption {
                what: "the extents a shard holds",
                why: crate::error::Malformed::CountMismatch,
            });
        }
        Ok(())
    }

    /// The store's file, the engine's work done.
    pub fn into_file(self) -> F {
        self.store.into_file()
    }

    fn apply(&mut self, key: &[u8], op: Op, value: &[u8]) -> Result<(), Error> {
        match self.mem.insert(key, op, value) {
            Err(Error::LimitExceeded { .. }) if !self.mem.is_empty() => {
                self.flush()?;
                self.mem.insert(key, op, value)
            }
            other => other,
        }
    }

    /// Records `key` holding `value`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.apply(key, Op::Put, value)
    }

    /// Records `key` deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.apply(key, Op::Delete, &[])
    }

    /// The value `key` holds, into `value`; false when it holds none.
    pub fn get(&mut self, key: &[u8], value: &mut Vec<u8>) -> Result<bool, Error> {
        let found = match self.mem.get(key, value)? {
            Some(op) => Some(op),
            None => self.trunk.get(&mut self.store, key, value)?,
        };
        Ok(found == Some(Op::Put))
    }

    /// Packs the memtable into a branch and incorporates it into the trunk.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let mut builder = Builder::new(self.store.page_capacity())?;
        let store = &mut self.store;
        self.mem
            .walk(|key, op, value| builder.add(store, key, op, value))?;
        let branch = builder.finish(&mut self.store)?;
        self.trunk.incorporate(&mut self.store, branch)?;
        self.mem.clear();
        Ok(())
    }

    /// The trunk's shape: height, nodes, leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        self.trunk.shape()
    }
}
