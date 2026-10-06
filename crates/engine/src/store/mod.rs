//! The engine's own file (docs/design/engine-structure.md §3, §7; step E1): checksummed pages in
//! extents, a reference-counted allocator persisted with each checkpoint, and two alternating
//! superblock copies naming checkpoints whole.
//!
//! A checkpoint is made in the order §7 gives:
//! 1. the structure's new pages are written (by the caller, [`Store::write_page`]);
//! 2. the allocator map is written, copy-on-write, to extents of its own;
//! 3. the platform's full flush;
//! 4. the superblock copy for the new generation, naming the root, the map and the applied index;
//! 5. the full flush;
//! 6. the extents no longer named are freed for reuse.
//!
//! So the newest superblock copy that verifies always names a checkpoint whose every page is
//! durable, and recovery ([`Store::open`]) takes it. A write or flush that fails leaves the
//! durability of what was written unknown (hyper-block's `BlockFile::sync_data`): the store then
//! takes no more writes, and the caller reopens it from the file.

pub mod alloc;
pub mod page;
pub mod superblock;

use crate::error::{Error, Malformed};
use alloc::Allocator;
use hyper_block::block::BlockFile;
use hyper_block::buf::AlignedBuf;
use page::{HEADER, Kind};
use superblock::Superblock;

/// The smallest page: 4 KiB, the largest logical block common devices use and the page every
/// file system the engine runs on maps (docs/design/raft-log.md §2's `B`).
pub const MIN_PAGE: usize = 4096;

/// A store's shape: fixed when the file is created, checked when it is opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Bytes a page, a multiple of the file's alignment and at least [`MIN_PAGE`].
    pub page_size: usize,
    /// Pages an extent, at least 2 (extent 0 holds the two superblock copies).
    pub extent_pages: u32,
    /// The extents the file may hold: its bound (CLAUDE.md §2).
    pub max_extents: u64,
}

/// What [`Store::open`] recovered: the newest durable checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// The checkpoint's generation.
    pub generation: u64,
    /// The last Raft index its state includes: the log is replayed from the next.
    pub applied: u64,
    /// The structure's root page.
    pub root: Option<u64>,
}

/// A store over one file, owned by one thread (as every `BlockFile` is).
#[derive(Debug)]
pub struct Store<F: BlockFile> {
    file: F,
    config: Config,
    alloc: Allocator,
    /// The durable checkpoint.
    durable: Superblock,
    /// One page's aligned buffer, reused by every read and write.
    buf: AlignedBuf,
    /// A write or flush failed: the store takes no more.
    fenced: bool,
}

fn io(op: &'static str, e: impl std::fmt::Display) -> Error {
    Error::Io {
        op,
        detail: e.to_string(),
    }
}

fn corrupt(why: Malformed) -> Error {
    Error::Corruption {
        what: "a store",
        why,
    }
}

impl<F: BlockFile> Store<F> {
    fn check(file: &F, config: Config) -> Result<(), Error> {
        let align = file.alignment();
        if config.page_size < MIN_PAGE || !align.is_aligned(config.page_size) {
            return Err(Error::InvalidArgument {
                what: "a page size below 4 KiB or off the file's alignment",
            });
        }
        if config.extent_pages < 2 {
            return Err(Error::InvalidArgument {
                what: "an extent of fewer than 2 pages",
            });
        }
        Ok(())
    }

    fn page_buf(file: &F, config: Config) -> Result<AlignedBuf, Error> {
        let mut buf = AlignedBuf::zeroed(config.page_size, file.alignment())
            .map_err(|e| io("allocate a page buffer", e))?;
        buf.extend_zeros(config.page_size)
            .map_err(|e| io("allocate a page buffer", e))?;
        Ok(buf)
    }

    /// Creates a store in `file`, which must be empty: generation 1, no root, applied 0, durable
    /// when this returns.
    pub fn create(file: F, config: Config) -> Result<Self, Error> {
        Self::check(&file, config)?;
        if !file.is_empty().map_err(|e| io("stat a store", e))? {
            return Err(Error::InvalidArgument {
                what: "a store created over a file that is not empty",
            });
        }
        let buf = Self::page_buf(&file, config)?;
        let page_size = u32::try_from(config.page_size).map_err(|_| Error::InvalidArgument {
            what: "a page size past 4 GiB",
        })?;
        let mut store = Self {
            file,
            config,
            alloc: Allocator::new(config.max_extents),
            durable: Superblock {
                page_size,
                extent_pages: config.extent_pages,
                generation: 0,
                applied: 0,
                root: None,
                extents: 1,
                map: Vec::new(),
            },
            buf,
            fenced: false,
        };
        store.checkpoint(None, 0)?;
        Ok(store)
    }

    /// Opens the store in `file` at its newest durable checkpoint: the newer of the two
    /// superblock copies that verify, its map read back. A file whose copies both fail is corrupt.
    pub fn open(file: F, config: Config) -> Result<(Self, Recovered), Error> {
        Self::check(&file, config)?;
        let mut buf = Self::page_buf(&file, config)?;
        let mut best: Option<Superblock> = None;
        for slot in 0..2u64 {
            let Ok(sb) = Self::read_superblock(&file, config, &mut buf, slot) else {
                continue;
            };
            if sb.slot() == slot && best.as_ref().is_none_or(|b| sb.generation > b.generation) {
                best = Some(sb);
            }
        }
        let sb = best.ok_or(corrupt(Malformed::ChecksumMismatch))?;
        if usize::try_from(sb.page_size).ok() != Some(config.page_size)
            || sb.extent_pages != config.extent_pages
        {
            return Err(Error::InvalidArgument {
                what: "a store opened with another page or extent size",
            });
        }
        let refs = Self::read_map(&file, config, &mut buf, &sb)?;
        let alloc = Allocator::from_refs(refs, config.max_extents)?;
        let recovered = Recovered {
            generation: sb.generation,
            applied: sb.applied,
            root: sb.root,
        };
        Ok((
            Self {
                file,
                config,
                alloc,
                durable: sb,
                buf,
                fenced: false,
            },
            recovered,
        ))
    }

    fn read_superblock(
        file: &F,
        config: Config,
        buf: &mut AlignedBuf,
        slot: u64,
    ) -> Result<Superblock, Error> {
        let offset = slot
            .checked_mul(u64::try_from(config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))?;
        file.read_exact_at(buf.as_mut_slice(), offset)
            .map_err(|e| io("read a superblock", e))?;
        let header = page::verify(buf.as_slice(), slot)?;
        if header.kind != Kind::Superblock {
            return Err(corrupt(Malformed::UnknownTag(0)));
        }
        let sb = Superblock::decode(page::payload(buf.as_slice(), header)?)?;
        if sb.generation != header.generation {
            return Err(corrupt(Malformed::CountMismatch));
        }
        Ok(sb)
    }

    fn read_map(
        file: &F,
        config: Config,
        buf: &mut AlignedBuf,
        sb: &Superblock,
    ) -> Result<Vec<u32>, Error> {
        let count = usize::try_from(sb.extents).map_err(|_| corrupt(Malformed::TooLarge))?;
        if sb.extents > config.max_extents {
            return Err(corrupt(Malformed::TooLarge));
        }
        let mut refs = Vec::with_capacity(count);
        'pages: for &extent in &sb.map {
            for i in 0..u64::from(config.extent_pages) {
                if refs.len() >= count {
                    break 'pages;
                }
                let address = Self::address_in(config, extent, i)?;
                file.read_exact_at(buf.as_mut_slice(), Self::offset_in(config, address)?)
                    .map_err(|e| io("read the allocator map", e))?;
                let header = page::verify(buf.as_slice(), address)?;
                if header.kind != Kind::Map || header.generation != sb.generation {
                    return Err(corrupt(Malformed::CountMismatch));
                }
                for c in page::payload(buf.as_slice(), header)?.as_chunks::<4>().0 {
                    if refs.len() < count {
                        refs.push(u32::from_le_bytes(*c));
                    }
                }
            }
        }
        if refs.len() != count {
            return Err(corrupt(Malformed::CountMismatch));
        }
        Ok(refs)
    }

    fn address_in(config: Config, extent: u64, page: u64) -> Result<u64, Error> {
        extent
            .checked_mul(u64::from(config.extent_pages))
            .and_then(|a| a.checked_add(page))
            .ok_or(corrupt(Malformed::TooLarge))
    }

    fn offset_in(config: Config, address: u64) -> Result<u64, Error> {
        address
            .checked_mul(u64::try_from(config.page_size).map_err(|_| corrupt(Malformed::TooLarge))?)
            .ok_or(corrupt(Malformed::TooLarge))
    }

    /// The page size.
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// The payload a page holds at most.
    pub fn page_capacity(&self) -> usize {
        page::capacity(self.config.page_size)
    }

    /// The address of page `page` (below the extent's pages) of `extent`.
    pub fn address(&self, extent: u64, page: u32) -> Result<u64, Error> {
        if page >= self.config.extent_pages {
            return Err(Error::InvalidArgument {
                what: "a page past its extent",
            });
        }
        Self::address_in(self.config, extent, u64::from(page))
    }

    /// The extent holding `address`.
    pub fn extent_of(&self, address: u64) -> u64 {
        address
            .checked_div(u64::from(self.config.extent_pages))
            .unwrap_or(0)
    }

    /// The durable checkpoint's generation.
    pub fn generation(&self) -> u64 {
        self.durable.generation
    }

    /// An extent for new pages, held once.
    pub fn allocate_extent(&mut self) -> Result<u64, Error> {
        self.alloc.allocate()
    }

    /// One more reference to a held extent (a new node naming an existing branch).
    pub fn retain(&mut self, extent: u64) -> Result<(), Error> {
        self.alloc.retain(extent)
    }

    /// One reference fewer; an extent left with none is reused once a checkpoint that no
    /// longer names it is durable.
    pub fn release(&mut self, extent: u64) -> Result<(), Error> {
        self.alloc.release(extent)
    }

    /// The extents holding the durable checkpoint's allocator map.
    pub fn map_extents(&self) -> &[u64] {
        &self.durable.map
    }

    /// The allocator's reference counts, extent by extent.
    pub fn refs(&self) -> &[u32] {
        self.alloc.refs()
    }

    fn fence<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        if result.is_err() {
            self.fenced = true;
        }
        result
    }

    fn put(
        &mut self,
        address: u64,
        kind: Kind,
        generation: u64,
        payload: &[u8],
    ) -> Result<(), Error> {
        if self.fenced {
            return Err(io(
                "write a store page",
                "the store was fenced by a failed write or flush",
            ));
        }
        let buf = self.buf.as_mut_slice();
        buf.get_mut(HEADER..HEADER.saturating_add(payload.len()))
            .ok_or(Error::InvalidArgument {
                what: "a page payload longer than the page",
            })?
            .copy_from_slice(payload);
        page::seal(buf, address, kind, generation, payload.len())?;
        let offset = Self::offset_in(self.config, address)?;
        let written = self
            .file
            .write_all_at(self.buf.as_slice(), offset)
            .map_err(|e| io("write a store page", e));
        self.fence(written)
    }

    fn sync(&mut self) -> Result<(), Error> {
        let synced = self.file.sync_data().map_err(|e| io("flush a store", e));
        self.fence(synced)
    }

    /// Writes a node page at `address`, in a held extent past the superblocks', for the next
    /// checkpoint. It is durable once that checkpoint is.
    pub fn write_page(&mut self, address: u64, payload: &[u8]) -> Result<(), Error> {
        let extent = self.extent_of(address);
        if extent == 0 || !self.alloc.is_held(extent) {
            return Err(Error::InvalidArgument {
                what: "a page written outside a held extent",
            });
        }
        let generation = self.durable.generation.saturating_add(1);
        self.put(address, Kind::Node, generation, payload)
    }

    /// Reads the node page at `address` and appends its payload to `out`.
    pub fn read_page(&mut self, address: u64, out: &mut Vec<u8>) -> Result<(), Error> {
        let offset = Self::offset_in(self.config, address)?;
        self.file
            .read_exact_at(self.buf.as_mut_slice(), offset)
            .map_err(|e| io("read a store page", e))?;
        let header = page::verify(self.buf.as_slice(), address)?;
        if header.kind != Kind::Node {
            return Err(corrupt(Malformed::UnknownTag(0)));
        }
        out.extend_from_slice(page::payload(self.buf.as_slice(), header)?);
        Ok(())
    }

    /// Makes a checkpoint naming `root` with state through Raft index `applied`, durable when
    /// this returns, in the order the module describes.
    pub fn checkpoint(&mut self, root: Option<u64>, applied: u64) -> Result<(), Error> {
        let generation = self
            .durable
            .generation
            .checked_add(1)
            .ok_or(corrupt(Malformed::TooLarge))?;
        // The new map's extents: enough pages for every extent's count, its own included, so
        // the extents are taken until the count they must hold stops growing.
        let per_page = self.page_capacity() / 4;
        let per_extent =
            per_page.saturating_mul(usize::try_from(self.config.extent_pages).unwrap_or(0));
        let max_map = superblock::max_map_extents(self.page_capacity());
        let mut map = Vec::new();
        while map.len().saturating_mul(per_extent) < self.alloc.refs().len() {
            if map.len() >= max_map {
                return Err(Error::LimitExceeded {
                    what: "allocator map extents a superblock names",
                    limit: u64::try_from(max_map).unwrap_or(u64::MAX),
                });
            }
            map.push(self.alloc.allocate()?);
        }
        // The old map is not named by this checkpoint: released now, so the counts persisted
        // show it free, and kept from reuse (pending) until this checkpoint is durable.
        for &extent in &self.durable.map {
            self.alloc.release(extent)?;
        }
        // The counts as this checkpoint names them, written page by page into the map extents.
        let refs: Vec<u32> = self.alloc.refs().to_vec();
        let mut chunks = refs.chunks(per_page);
        'extents: for &extent in &map {
            for i in 0..self.config.extent_pages {
                let Some(chunk) = chunks.next() else {
                    break 'extents;
                };
                let bytes: Vec<u8> = chunk.iter().flat_map(|c| c.to_le_bytes()).collect();
                let address = self.address(extent, i)?;
                self.put(address, Kind::Map, generation, &bytes)?;
            }
        }
        self.sync()?;
        let page_size = self.durable.page_size;
        let sb = Superblock {
            page_size,
            extent_pages: self.config.extent_pages,
            generation,
            applied,
            root,
            extents: u64::try_from(refs.len()).map_err(|_| corrupt(Malformed::TooLarge))?,
            map,
        };
        let mut payload = vec![0u8; sb.encoded_len()?];
        sb.encode(&mut payload)?;
        self.put(sb.slot(), Kind::Superblock, generation, &payload)?;
        self.sync()?;
        // Durable: every extent this checkpoint no longer names, the old map's included, is free.
        self.durable = sb;
        self.alloc.durable();
        Ok(())
    }

    /// Gives back the file, the store's work done.
    pub fn into_file(self) -> F {
        self.file
    }
}
