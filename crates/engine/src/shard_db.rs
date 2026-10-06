//! One shard's engine (docs/design/engine-structure.md §2, §8): the memtable, and the trunk of
//! branches in the shard's store. A put or delete goes to the memtable; a full memtable is packed
//! into a branch and incorporated into the trunk; a get reads the memtable, then the trunk.
//!
//! Durability is the Raft log's (§2): the engine applies committed entries and makes its state
//! durable by checkpoints, which name the trunk's root once the trunk is persisted (step E4c).

use crate::branch::{Builder, Op};
use crate::error::Error;
use crate::memtable::btree::BTreeMem;
use crate::store::Store;
use crate::trunk::{Trunk, TrunkConfig};
use hyper_block::block::BlockFile;

/// A shard's engine over a store.
#[derive(Debug)]
pub struct ShardDb<F: BlockFile> {
    store: Store<F>,
    mem: BTreeMem,
    mem_limit: usize,
    trunk: Trunk,
}

impl<F: BlockFile> ShardDb<F> {
    /// An engine over `store`, its memtable bounded at `mem_limit` bytes.
    pub fn new(store: Store<F>, mem_limit: usize, trunk: TrunkConfig) -> Result<Self, Error> {
        Ok(Self {
            store,
            mem: BTreeMem::new(mem_limit)?,
            mem_limit,
            trunk: Trunk::new(trunk)?,
        })
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
        self.mem = BTreeMem::new(self.mem_limit)?;
        Ok(())
    }

    /// The trunk's shape: height, nodes, leaves.
    pub fn shape(&self) -> Result<(usize, usize, usize), Error> {
        self.trunk.shape()
    }
}
