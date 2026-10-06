//! `Block`, `BlockIter`, `DataBlockIter`, `IndexBlockIter` and `MetaBlockIter` of
//! `table/block_based/block.{h,cc}` [R block.h:155-1052, block.cc:32-1619]: reading the blocks
//! [`super::block_builder`] writes.
//!
//! One iterator type serves the three kinds, its kind deciding how an entry's value is read and
//! how a block is searched, where RocksDB has a class each; the moves they share (restart-point
//! search, entry parsing, the linear scan after a binary search) are written once. Keys whole in
//! the block are borrowed from it, as RocksDB pins them, and only a key rebuilt from the bytes it
//! shares with the previous one is copied.
//!
//! The port is stricter than RocksDB on corrupt blocks: where RocksDB asserts that an entry's key
//! and value lie inside the block, or that an index value decodes, and so in a release build reads
//! past the block or returns a half-decoded handle, the port ends the iterator with corruption.
//! Not ported here: the read-amplification bitmap and statistics (with the port's statistics),
//! user-defined timestamps (P16) and the prefix index of hash-search index blocks (with the index
//! builders of this phase).

use std::cmp::Ordering;

use crate::db::dbformat::{
    DISABLE_GLOBAL_SEQUENCE_NUMBER, InternalKeyComparator, NUM_INTERNAL_BYTES, SequenceNumber,
    ValueType,
};
use crate::db::kv_checksum;
use crate::error::{Error, Malformed};
use crate::table::block_based::block_util::{decode_entry, read_be64_from_key};
use crate::table::block_based::data_block_footer::{DataBlockFooter, DataBlockIndexType};
use crate::table::block_based::data_block_hash_index::{COLLISION, DataBlockHashIndex, NO_ENTRY};
use crate::table::format::{BlockHandle, IndexValue};
use crate::util::coding::decode_fixed32;
use crate::util::comparator::Comparator;

/// How an index block is searched [R include/rocksdb/table.h, `BlockSearchType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockSearchType {
    /// Binary search over the restart points.
    #[default]
    Binary,
    /// Interpolation search over the restart keys read as integers: bytewise keys only.
    Interpolation,
    /// Interpolation where the block is marked uniform and the comparator is bytewise, binary
    /// otherwise.
    Auto,
}

/// `Block` [R block.h:155-312]: a block's bytes and where its parts are.
#[derive(Debug)]
pub struct Block {
    contents: Vec<u8>,
    /// Where the restart array begins.
    restart_offset: u32,
    num_restarts: u32,
    is_uniform: bool,
    hash_index: Option<DataBlockHashIndex>,
    /// Where the values section begins, for a block of separated keys and values.
    values_section: Option<u32>,
    kv_checksum: Vec<u8>,
    protection_bytes_per_key: u8,
    block_restart_interval: u32,
    /// Why the block cannot be read: RocksDB's size-zero marker and `GetCorruptionStatus`.
    corrupt: Option<Error>,
}

/// `kv_checksum` bytes for an entry count and width, bounded by the block's own size.
fn checksum_len(entries: u32, width: u8) -> Option<usize> {
    usize::try_from(entries)
        .ok()?
        .checked_mul(usize::from(width))
}

impl Block {
    /// `Block::Block` [R block.cc:1308-1372]: the block in `contents`, uncompressed. A block
    /// whose footer, hash index, restart array or values offset does not hold is kept as
    /// corrupt, and every iterator made from it ends at once with that corruption.
    /// `restart_interval`, when known from the table's properties, is given; 0 lets per-entry
    /// protection measure it.
    pub fn new(contents: Vec<u8>, restart_interval: u32) -> Self {
        let mut block = Self {
            contents,
            restart_offset: 0,
            num_restarts: 0,
            is_uniform: false,
            hash_index: None,
            values_section: None,
            kv_checksum: Vec::new(),
            protection_bytes_per_key: 0,
            block_restart_interval: restart_interval,
            corrupt: None,
        };
        if let Err(e) = block.parse() {
            block.corrupt = Some(e);
        }
        block
    }

    fn parse(&mut self) -> Result<(), Error> {
        let bad = || Error::corruption("bad block contents", Malformed::OutOfRange);
        let mut input = self.contents.as_slice();
        let footer = DataBlockFooter::decode_from(&mut input)?;
        self.num_restarts = footer.num_restarts;
        self.is_uniform = footer.is_uniform;
        if footer.index_type == DataBlockIndexType::BinaryAndHash {
            let index = DataBlockHashIndex::initialize(input).ok_or_else(bad)?;
            input = input
                .get(..usize::from(index.map_offset()))
                .ok_or_else(bad)?;
            self.hash_index = Some(index);
        }
        let restart_bytes = usize::try_from(self.num_restarts)
            .ok()
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(bad)?;
        let restart_offset = input.len().checked_sub(restart_bytes).ok_or_else(bad)?;
        self.restart_offset = u32::try_from(restart_offset).map_err(|_| bad())?;
        if footer.separated_kv {
            if footer.values_section_offset > self.restart_offset {
                return Err(bad());
            }
            self.values_section = Some(footer.values_section_offset);
        }
        Ok(())
    }

    /// `size`.
    pub fn size(&self) -> usize {
        if self.corrupt.is_some() {
            0
        } else {
            self.contents.len()
        }
    }

    /// The block's bytes.
    pub fn data(&self) -> &[u8] {
        &self.contents
    }

    /// `NumRestarts`.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// `IsUniform`.
    pub fn is_uniform(&self) -> bool {
        self.is_uniform
    }

    /// `HasSeparatedKV`.
    pub fn has_separated_kv(&self) -> bool {
        self.values_section.is_some()
    }

    /// `IndexType` [R block.cc:1273-1279].
    pub fn index_type(&self) -> DataBlockIndexType {
        if self.hash_index.is_some() {
            DataBlockIndexType::BinaryAndHash
        } else {
            DataBlockIndexType::BinarySearch
        }
    }

    /// The error an iterator of this block ends with before it starts, if any: RocksDB's
    /// `size() < 2 * sizeof(uint32_t)` check and `GetCorruptionStatus` [R block.cc:1288-1306].
    fn unreadable(&self) -> Option<Error> {
        if let Some(e) = &self.corrupt {
            return Some(e.clone());
        }
        (self.contents.len() < 8)
            .then(|| Error::corruption("bad block contents", Malformed::Truncated))
    }

    fn core(&self, icmp: InternalKeyComparator, global_seqno: SequenceNumber) -> Core<'_> {
        let keys_end = self.values_section.unwrap_or(self.restart_offset);
        Core {
            data: &self.contents,
            restarts: self.restart_offset,
            num_restarts: self.num_restarts,
            values_section: self.values_section,
            keys_end,
            icmp,
            global_seqno,
            current: keys_end,
            restart_index: self.num_restarts,
            entry: (0, 0),
            raw_key: RawKey::default(),
            key_buf: Vec::new(),
            key_from_buf: false,
            value: (0, 0),
            status: None,
            cur_entry_idx: -1,
            block_restart_interval: self.block_restart_interval,
            kv_checksum: &self.kv_checksum,
            protection_bytes_per_key: self.protection_bytes_per_key,
            live: true,
        }
    }

    /// An iterator that is already at its end, with `status`.
    fn ended(&self, kind: Kind, status: Option<Error>) -> BlockIter<'_> {
        let mut core = self.core(
            InternalKeyComparator::default(),
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
        );
        core.live = false;
        core.keys_end = 0;
        core.current = 0;
        core.status = status;
        BlockIter { core, kind }
    }

    /// `NewDataIterator` [R block.cc:1522-1559].
    pub fn new_data_iterator(
        &self,
        user_comparator: Comparator,
        global_seqno: SequenceNumber,
    ) -> BlockIter<'_> {
        let kind = Kind::Data(DataState {
            hash_index: self.hash_index,
            prev: PrevCache::default(),
        });
        if let Some(e) = self.unreadable() {
            return self.ended(kind, Some(e));
        }
        if self.num_restarts == 0 {
            return self.ended(kind, None);
        }
        let mut core = self.core(InternalKeyComparator::new(user_comparator), global_seqno);
        core.raw_key.is_user_key = false;
        BlockIter { core, kind }
    }

    /// `NewMetaIterator` [R block.cc:1506-1520]: user keys, bytewise.
    pub fn new_meta_iterator(&self) -> BlockIter<'_> {
        if let Some(e) = self.unreadable() {
            return self.ended(Kind::Meta, Some(e));
        }
        if self.num_restarts == 0 {
            return self.ended(Kind::Meta, None);
        }
        let mut core = self.core(
            InternalKeyComparator::new(Comparator::Bytewise),
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
        );
        core.raw_key.is_user_key = true;
        BlockIter {
            core,
            kind: Kind::Meta,
        }
    }

    /// `NewIndexIterator` [R block.cc:1561-1605], without a prefix index.
    pub fn new_index_iterator(
        &self,
        user_comparator: Comparator,
        global_seqno: SequenceNumber,
        options: IndexIterOptions,
    ) -> BlockIter<'_> {
        let search = match options.search {
            BlockSearchType::Auto if self.is_uniform && user_comparator == Comparator::Bytewise => {
                BlockSearchType::Interpolation
            }
            BlockSearchType::Auto => BlockSearchType::Binary,
            other => other,
        };
        let seqno_first_key = (options.have_first_key
            && global_seqno != DISABLE_GLOBAL_SEQUENCE_NUMBER)
            .then(Vec::new);
        let kind = Kind::Index(IndexState {
            value_delta_encoded: !options.value_is_full,
            have_first_key: options.have_first_key,
            search,
            decoded: DecodedIndexValue::default(),
            global_seqno,
            seqno_first_key,
        });
        if let Some(e) = self.unreadable() {
            return self.ended(kind, Some(e));
        }
        if self.num_restarts == 0 {
            return self.ended(kind, None);
        }
        // An index block's own keys never take the global sequence number; its values' first
        // keys do (`DecodeCurrentValue`).
        let mut core = self.core(
            InternalKeyComparator::new(user_comparator),
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
        );
        core.raw_key.is_user_key = !options.key_includes_seq;
        BlockIter { core, kind }
    }

    /// The checksum of every entry `iter` reads from the first, and the restart interval it
    /// measured, or the error that ended it.
    fn checksums(
        mut iter: BlockIter<'_>,
        width: u8,
        interval: u32,
        raw_value: bool,
    ) -> Result<(Vec<u8>, u32), Error> {
        let interval = if interval == 0 {
            iter.restart_interval()?
        } else {
            interval
        };
        let keys = iter.number_of_keys(interval)?;
        let mut sums = Vec::with_capacity(checksum_len(keys, width).unwrap_or(0));
        iter.seek_to_first();
        while iter.valid() {
            let value = if raw_value {
                iter.raw_value()
            } else {
                iter.value()
            };
            kv_checksum::encode_to(kv_checksum::protect_kv(iter.key(), value), width, &mut sums);
            iter.next();
        }
        iter.status()?;
        Ok((sums, interval))
    }

    /// Applies `InitializeDataBlockProtectionInfo` and its index and meta forms: an entry
    /// checksum of `width` bytes for every entry, which iterators then verify, or the block made
    /// corrupt by the error reading it.
    fn protect(&mut self, sums: Result<(Vec<u8>, u32), Error>, width: u8) {
        match sums {
            Ok((sums, interval)) => {
                self.kv_checksum = sums;
                self.block_restart_interval = interval;
                self.protection_bytes_per_key = width;
            }
            Err(e) => self.corrupt = Some(e),
        }
    }

    fn check_width(width: u8) -> Result<(), Error> {
        if kv_checksum::is_supported_len(width) {
            Ok(())
        } else {
            Err(Error::Unsupported {
                feature: "block protection bytes per key",
                value: u64::from(width),
            })
        }
    }

    /// `InitializeDataBlockProtectionInfo` [R block.cc:1374-1418].
    pub fn initialize_data_block_protection_info(
        &mut self,
        width: u8,
        user_comparator: Comparator,
    ) -> Result<(), Error> {
        self.protection_bytes_per_key = 0;
        if width == 0 || self.num_restarts == 0 {
            return Ok(());
        }
        Self::check_width(width)?;
        let iter = self.new_data_iterator(user_comparator, DISABLE_GLOBAL_SEQUENCE_NUMBER);
        let sums = Self::checksums(iter, width, self.block_restart_interval, false);
        self.protect(sums, width);
        Ok(())
    }

    /// `InitializeIndexBlockProtectionInfo` [R block.cc:1420-1470].
    pub fn initialize_index_block_protection_info(
        &mut self,
        width: u8,
        user_comparator: Comparator,
        value_is_full: bool,
        have_first_key: bool,
    ) -> Result<(), Error> {
        self.protection_bytes_per_key = 0;
        if width == 0 || self.num_restarts == 0 {
            return Ok(());
        }
        Self::check_width(width)?;
        let iter = self.new_index_iterator(
            user_comparator,
            DISABLE_GLOBAL_SEQUENCE_NUMBER,
            IndexIterOptions {
                have_first_key,
                key_includes_seq: false,
                value_is_full,
                search: BlockSearchType::Binary,
            },
        );
        let sums = Self::checksums(iter, width, self.block_restart_interval, true);
        self.protect(sums, width);
        Ok(())
    }

    /// `InitializeMetaIndexBlockProtectionInfo` [R block.cc:1472-1504]: a meta block's restart
    /// interval is one.
    pub fn initialize_meta_index_block_protection_info(&mut self, width: u8) -> Result<(), Error> {
        self.protection_bytes_per_key = 0;
        if width == 0 || self.num_restarts == 0 {
            return Ok(());
        }
        Self::check_width(width)?;
        let iter = self.new_meta_iterator();
        let sums = Self::checksums(iter, width, 1, false);
        self.protect(sums, width);
        Ok(())
    }

    /// The per-entry checksums, for tests: RocksDB's `TEST_GetKVChecksum`.
    pub fn kv_checksum(&self) -> &[u8] {
        &self.kv_checksum
    }
}

/// How an index block's iterator reads it: RocksDB's `NewIndexIterator` arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexIterOptions {
    /// The values carry their block's first internal key.
    pub have_first_key: bool,
    /// The keys are internal keys.
    pub key_includes_seq: bool,
    /// No value is delta encoded.
    pub value_is_full: bool,
    pub search: BlockSearchType,
}

/// The current key: a range of the block when stored whole there, or rebuilt from a shared
/// prefix and copied.
#[derive(Debug, Default)]
struct RawKey {
    /// `(start, len)` in the block, when the key is stored whole there.
    borrowed: Option<(u32, u32)>,
    owned: Vec<u8>,
    is_user_key: bool,
}

impl RawKey {
    fn clear(&mut self) {
        self.borrowed = None;
        self.owned.clear();
    }

    fn size(&self) -> usize {
        match self.borrowed {
            Some((_, len)) => len as usize,
            None => self.owned.len(),
        }
    }

    fn get<'d>(&'d self, data: &'d [u8]) -> &'d [u8] {
        match self.borrowed {
            Some((start, len)) => slice(data, start, len),
            None => &self.owned,
        }
    }

    fn set_borrowed(&mut self, start: u32, len: u32) {
        self.borrowed = Some((start, len));
    }

    /// `TrimAppend`: the first `shared` bytes of the current key and then `non_shared`.
    fn trim_append(&mut self, data: &[u8], shared: usize, non_shared: &[u8]) -> bool {
        if let Some((start, len)) = self.borrowed.take() {
            self.owned.clear();
            self.owned.extend_from_slice(slice(data, start, len));
        }
        if shared > self.owned.len() {
            return false;
        }
        self.owned.truncate(shared);
        self.owned.extend_from_slice(non_shared);
        true
    }
}

/// A restart index from the signed window the searches keep, which holds it in `0..num_restarts`
/// wherever it is used as one.
fn index_of(i: i64) -> u32 {
    u32::try_from(i).unwrap_or(0)
}

/// `(start, len)` less its first `shared` bytes.
fn suffix(key: (u32, u32), shared: usize) -> (u32, u32) {
    let shared = u32::try_from(shared).unwrap_or(u32::MAX).min(key.1);
    (key.0.saturating_add(shared), key.1.saturating_sub(shared))
}

/// `key` less its last `n` bytes.
fn tail_less(key: &[u8], n: usize) -> &[u8] {
    key.get(..key.len().saturating_sub(n)).unwrap_or_default()
}

/// `key` from byte `shared`.
fn tail(key: &[u8], shared: usize) -> &[u8] {
    key.get(shared.min(key.len())..).unwrap_or_default()
}

/// `data[start..start + len]`, or nothing where that does not lie in `data`.
fn slice(data: &[u8], start: u32, len: u32) -> &[u8] {
    let start = start as usize;
    start
        .checked_add(len as usize)
        .and_then(|end| data.get(start..end))
        .unwrap_or_default()
}

/// What every kind of block iterator holds: RocksDB's `BlockIter<TValue>` members.
#[derive(Debug)]
struct Core<'a> {
    data: &'a [u8],
    /// Where the restart array begins.
    restarts: u32,
    num_restarts: u32,
    values_section: Option<u32>,
    /// Where the keys end: the values section, or the restart array.
    keys_end: u32,
    icmp: InternalKeyComparator,
    global_seqno: SequenceNumber,
    /// The current entry's offset; at `keys_end` or past when not valid.
    current: u32,
    /// The restart interval the current entry is in.
    restart_index: u32,
    /// The current entry's `(start, len)` in the block, its value included unless separated.
    entry: (u32, u32),
    raw_key: RawKey,
    /// The key with the global sequence number in place.
    key_buf: Vec<u8>,
    key_from_buf: bool,
    /// The current value's `(start, len)` in the block.
    value: (u32, u32),
    status: Option<Error>,
    /// The index of the entry parsed last.
    cur_entry_idx: i64,
    block_restart_interval: u32,
    kv_checksum: &'a [u8],
    protection_bytes_per_key: u8,
    /// False for an iterator made ended (RocksDB's null `data_`).
    live: bool,
}

impl Core<'_> {
    fn valid(&self) -> bool {
        self.current < self.keys_end
    }

    fn next_entry_offset(&self) -> u32 {
        self.entry.0.saturating_add(self.entry.1)
    }

    /// `CorruptionError` [R block.h:600-606].
    fn corruption(&mut self, what: &'static str, why: Malformed) {
        self.refuse(Error::corruption(what, why));
    }

    /// `GetRestartPoint` [R block.h:715-721].
    fn restart_point(&self, index: u32) -> Option<u32> {
        let at = index.checked_mul(4)?.checked_add(self.restarts)?;
        decode_fixed32(self.data.get(at as usize..)?).ok()
    }

    /// `SeekToRestartPoint` [R block.h:723-733].
    fn seek_to_restart_point(&mut self, index: u32) -> bool {
        self.raw_key.clear();
        self.restart_index = index;
        self.cur_entry_idx = i64::from(index)
            .saturating_mul(i64::from(self.block_restart_interval))
            .saturating_sub(1);
        match self.restart_point(index) {
            Some(offset) => {
                self.entry = (offset, 0);
                true
            }
            None => {
                self.corruption("block restart point", Malformed::Truncated);
                false
            }
        }
    }

    /// `ParseNextKey` [R block.cc:524-619]: the entry after the current one; `with_length` for
    /// entries that store their value's length, `strict` for meta blocks, whose entries RocksDB
    /// checks against the block. Returns whether an entry was read, and whether its key shares
    /// bytes with the previous one.
    fn parse_next_key(&mut self, with_length: bool, strict: bool) -> Option<bool> {
        self.current = self.next_entry_offset();
        self.cur_entry_idx = self.cur_entry_idx.saturating_add(1);
        if self.current >= self.keys_end {
            self.current = self.keys_end;
            self.restart_index = self.num_restarts;
            return None;
        }
        let value_offset_encoded = match self.values_section {
            Some(_) if self.block_restart_interval == 0 => {
                self.corruption(
                    "separated block without its restart interval",
                    Malformed::Forbidden,
                );
                return None;
            }
            Some(_) => {
                self.cur_entry_idx
                    .checked_rem(i64::from(self.block_restart_interval))
                    == Some(0)
            }
            None => false,
        };
        let key_limit = slice(
            self.data,
            self.current,
            self.keys_end.saturating_sub(self.current),
        );
        let Ok((header, header_len)) = decode_entry(key_limit, with_length, value_offset_encoded)
        else {
            self.corruption("bad entry in block", Malformed::Truncated);
            return None;
        };
        if self.raw_key.size() < header.shared as usize {
            self.corruption("bad entry in block", Malformed::OutOfRange);
            return None;
        }
        let header_len = u32::try_from(header_len).unwrap_or(u32::MAX);
        let key_at = self.current.saturating_add(header_len);
        let room = self.keys_end.saturating_sub(key_at);
        let inline_value = if self.values_section.is_none() {
            header.value_length
        } else {
            0
        };
        // RocksDB checks this only for meta blocks; the port always does, as reading past the
        // block is what it would otherwise do.
        let wanted = u64::from(header.non_shared).saturating_add(if strict {
            u64::from(inline_value)
        } else {
            0
        });
        if u64::from(room) < wanted || room < header.non_shared {
            self.corruption("bad entry in block", Malformed::TooLarge);
            return None;
        }
        self.entry = (self.current, header_len.saturating_add(header.non_shared));
        let is_shared = header.shared != 0;
        if is_shared {
            let bytes = slice(self.data, key_at, header.non_shared);
            if !self
                .raw_key
                .trim_append(self.data, header.shared as usize, bytes)
            {
                self.corruption("bad entry in block", Malformed::OutOfRange);
                return None;
            }
        } else {
            self.raw_key.set_borrowed(key_at, header.non_shared);
            while self.restart_index.saturating_add(1) < self.num_restarts {
                match self.restart_point(self.restart_index.saturating_add(1)) {
                    Some(r) if r < self.current => {
                        self.restart_index = self.restart_index.saturating_add(1);
                    }
                    _ => break,
                }
            }
        }
        match self.values_section {
            Some(section) => {
                let start = if value_offset_encoded {
                    section.saturating_add(header.value_offset.unwrap_or(0))
                } else {
                    self.value.0.saturating_add(self.value.1)
                };
                self.value = (start, header.value_length);
                if u64::from(start).saturating_add(u64::from(header.value_length))
                    > u64::from(self.restarts)
                {
                    self.corruption("bad entry in block", Malformed::TooLarge);
                    return None;
                }
            }
            None => {
                let start = self.entry.0.saturating_add(self.entry.1);
                self.value = (start, header.value_length);
                if u64::from(start).saturating_add(u64::from(header.value_length))
                    > u64::from(self.restarts)
                {
                    self.corruption("bad entry in block", Malformed::TooLarge);
                    return None;
                }
                self.entry.1 = self.entry.1.saturating_add(header.value_length);
            }
        }
        Some(is_shared)
    }

    fn raw_key(&self) -> &[u8] {
        self.raw_key.get(self.data)
    }

    /// `CompareKey` [R block.h:672-681].
    fn compare_key(&self, a: &[u8], b: &[u8]) -> Ordering {
        if self.raw_key.is_user_key {
            self.icmp.user_comparator().compare(a, b)
        } else if self.global_seqno == DISABLE_GLOBAL_SEQUENCE_NUMBER {
            self.icmp.compare(a, b)
        } else {
            self.icmp.compare_with_global_seqno(
                a,
                self.global_seqno,
                b,
                DISABLE_GLOBAL_SEQUENCE_NUMBER,
            )
        }
    }

    fn compare_current_key(&self, target: &[u8]) -> Ordering {
        self.compare_key(self.raw_key(), target)
    }

    /// `GetRestartKey` [R block.cc:759-774]: the key stored whole at restart `index`.
    fn restart_key(&mut self, index: u32, with_length: bool) -> Option<(u32, u32)> {
        let Some(at) = self.restart_point(index) else {
            self.corruption("block restart point", Malformed::Truncated);
            return None;
        };
        let limit = slice(self.data, at, self.restarts.saturating_sub(at));
        match decode_entry(limit, with_length, self.values_section.is_some()) {
            Ok((h, n)) if h.shared == 0 => {
                let n = u32::try_from(n).unwrap_or(u32::MAX);
                if self.restarts.saturating_sub(at).saturating_sub(n) < h.non_shared {
                    self.corruption("bad entry in block", Malformed::TooLarge);
                    return None;
                }
                Some((at.saturating_add(n), h.non_shared))
            }
            _ => {
                self.corruption("bad entry in block", Malformed::Truncated);
                None
            }
        }
    }

    /// `BinarySeekRestartPointIndex` [R block.cc:776-846]: the restart interval to scan for
    /// `target`, and whether its first key is already the answer.
    fn binary_seek(&mut self, target: &[u8], with_length: bool) -> Option<(u32, bool)> {
        if self.restarts == 0 {
            return None;
        }
        let mut skip = false;
        let mut left = -1i64;
        let mut right = i64::from(self.num_restarts).saturating_sub(1);
        while left != right {
            let mid = left.saturating_add(right.saturating_sub(left).saturating_add(1) / 2);
            let (start, len) = self.restart_key(u32::try_from(mid).unwrap_or(0), with_length)?;
            self.raw_key.set_borrowed(start, len);
            match self.compare_current_key(target) {
                Ordering::Less => left = mid,
                Ordering::Greater => right = mid.saturating_sub(1),
                Ordering::Equal => {
                    skip = true;
                    left = mid;
                    right = mid;
                }
            }
        }
        if left == -1 {
            Some((0, true))
        } else {
            Some((u32::try_from(left).unwrap_or(0), skip))
        }
    }

    /// `InterpolationSeekRestartPointIndex` [R block.cc:848-1147]: as `binary_seek`, guessing each
    /// split from the keys' leading eight bytes past their shared prefix, read as integers; it
    /// falls back to binary search on a window of at most eight restarts or after eight guesses
    /// in a row that did not halve it. The guess is computed in 128-bit integers, as RocksDB does
    /// where its compiler has them. Bytewise keys only, which `new_index_iterator` ensures.
    fn interpolation_seek(&mut self, target: &[u8], with_length: bool) -> Option<(u32, bool)> {
        // [R block.cc:863-864]: the window binary search takes over at, and the run of poor
        // guesses that hands the search to it.
        const GUARD_LEN: i64 = 8;
        const MAX_POOR_SEARCHES: u64 = 8;
        if self.restarts == 0 {
            return None;
        }
        let is_user_key = self.raw_key.is_user_key;
        if !is_user_key && target.len() < NUM_INTERNAL_BYTES {
            self.refuse(Error::InvalidArgument {
                what: "a seek target shorter than an internal key's trailer",
            });
            return None;
        }
        let target_user_key = if is_user_key {
            target
        } else {
            tail_less(target, NUM_INTERNAL_BYTES)
        };
        let mut left = -1i64;
        let mut right = i64::from(self.num_restarts).saturating_sub(1);
        let mut shared = 0usize;
        // Restart keys as `(start, len)` in the block.
        let mut left_key = (0u32, 0u32);
        let (mut left_val, mut right_val, mut target_val) = (0u64, 0u64, 0u64);
        let mut first_iter = true;
        let mut poor = 0u64;
        let mut skip = false;
        while left != right {
            let mut mid = 0i64;
            let mut failed = right.saturating_sub(left) <= GUARD_LEN || poor >= MAX_POOR_SEARCHES;
            if !failed {
                let usable_left = left.max(0);
                if first_iter {
                    left_key = self.restart_key(index_of(usable_left), with_length)?;
                    let right_key = self.restart_key(index_of(right), with_length)?;
                    let lk = slice(self.data, left_key.0, left_key.1);
                    let rk = slice(self.data, right_key.0, right_key.1);
                    shared = lk.iter().zip(rk).take_while(|(a, b)| a == b).count();
                    if !is_user_key {
                        shared = shared.min(lk.len().saturating_sub(NUM_INTERNAL_BYTES));
                    }
                    left_val = self.be64(left_key, shared)?;
                    right_val = self.be64(right_key, shared)?;
                    target_val = read_be64_from_key(target, is_user_key, shared).ok()?;
                    if shared > 0 {
                        let cmp_len = target_user_key.len().min(shared);
                        let t = target_user_key.get(..cmp_len).unwrap_or_default();
                        let l = slice(self.data, left_key.0, left_key.1)
                            .get(..cmp_len)
                            .unwrap_or_default();
                        match t.cmp(l) {
                            Ordering::Less => return Some((index_of(usable_left), true)),
                            Ordering::Equal if cmp_len < shared => {
                                return Some((index_of(usable_left), true));
                            }
                            Ordering::Greater => return Some((index_of(right), false)),
                            Ordering::Equal => {}
                        }
                    }
                }
                if left_val > right_val {
                    self.corruption("left key is greater than right key", Malformed::OutOfOrder);
                    return None;
                }
                let left_suffix = suffix(left_key, shared);
                let lte_left = target_val < left_val
                    || (target_val == left_val
                        && self.compare_key(
                            slice(self.data, left_suffix.0, left_suffix.1),
                            tail(target, shared),
                        ) != Ordering::Less);
                if lte_left {
                    return Some((index_of(usable_left), true));
                }
                if target_val > right_val {
                    return Some((index_of(right), false));
                }
                if right_val == left_val {
                    failed = true;
                } else {
                    // target_val - left_val <= right_val - left_val, so the offset is at most
                    // right - usable_left and the product fits in 128 bits.
                    let range = u128::from(index_of(right.saturating_sub(usable_left)));
                    let target_delta = u128::from(target_val.saturating_sub(left_val));
                    let range_delta = u128::from(right_val.saturating_sub(left_val));
                    let offset = range
                        .saturating_mul(target_delta)
                        .checked_div(range_delta)
                        .and_then(|o| i64::try_from(o).ok())
                        .unwrap_or(0);
                    left = usable_left;
                    mid = usable_left.saturating_add(offset);
                    if mid == usable_left {
                        mid = mid.saturating_add(1);
                    }
                }
            }
            if failed {
                mid = left.saturating_add(right.saturating_sub(left).saturating_add(1) / 2);
            }
            if !(left < mid && mid <= right) {
                self.corruption(
                    "interpolation search outside its window",
                    Malformed::OutOfRange,
                );
                return None;
            }
            let mid_key = self.restart_key(index_of(mid), with_length)?;
            let mid_suffix = suffix(mid_key, shared);
            self.raw_key.set_borrowed(mid_suffix.0, mid_suffix.1);
            let previous = right.saturating_sub(left);
            match self.compare_current_key(tail(target, shared)) {
                Ordering::Less => {
                    left = mid;
                    left_key = mid_key;
                    left_val = self.be64(left_key, shared)?;
                }
                Ordering::Greater => {
                    right = mid.saturating_sub(1);
                    if !failed && left != right {
                        let key = self.restart_key(index_of(right), with_length)?;
                        right_val = self.be64(key, shared)?;
                    }
                }
                Ordering::Equal => {
                    skip = true;
                    left = mid;
                    right = mid;
                }
            }
            if right.saturating_sub(left) > previous / 2 {
                poor = poor.saturating_add(1);
            } else {
                poor = 0;
            }
            first_iter = false;
        }
        if left == -1 {
            Some((0, true))
        } else {
            Some((index_of(left), skip))
        }
    }

    /// `ReadBe64FromKey` of a restart key in the block; a key too short to be an internal key is
    /// corruption.
    fn be64(&mut self, key: (u32, u32), shared: usize) -> Option<u64> {
        match read_be64_from_key(
            slice(self.data, key.0, key.1),
            self.raw_key.is_user_key,
            shared,
        ) {
            Ok(v) => Some(v),
            Err(_) => {
                self.corruption("internal key in block", Malformed::Truncated);
                None
            }
        }
    }

    /// Ends the iterator with `error`.
    fn refuse(&mut self, error: Error) {
        self.current = self.keys_end;
        self.restart_index = self.num_restarts;
        self.status = Some(error);
        self.raw_key.clear();
        self.value = (0, 0);
    }

    /// `UpdateKey` [R block.h:635-666]: the key callers see, with the global sequence number in
    /// place, and the entry's checksum verified.
    fn update_key(&mut self) {
        self.key_buf.clear();
        self.key_from_buf = false;
        if !self.valid() {
            return;
        }
        if !self.raw_key.is_user_key && self.global_seqno != DISABLE_GLOBAL_SEQUENCE_NUMBER {
            let raw = self.raw_key.get(self.data);
            let user = raw.len().saturating_sub(NUM_INTERNAL_BYTES);
            let kind = raw.get(user).copied().unwrap_or_default();
            self.key_buf
                .extend_from_slice(raw.get(..user).unwrap_or_default());
            let packed = (self.global_seqno << 8) | u64::from(kind);
            self.key_buf.extend_from_slice(&packed.to_le_bytes());
            self.key_from_buf = true;
        }
        if self.protection_bytes_per_key > 0 {
            let width = usize::from(self.protection_bytes_per_key);
            let at = usize::try_from(self.cur_entry_idx)
                .ok()
                .and_then(|i| i.checked_mul(width));
            let stored = at
                .and_then(|at| self.kv_checksum.get(at..at.saturating_add(width)))
                .unwrap_or_default();
            let sum = kv_checksum::protect_kv(
                self.raw_key(),
                slice(self.data, self.value.0, self.value.1),
            );
            if !kv_checksum::verify(sum, self.protection_bytes_per_key, stored) {
                self.corruption(
                    "per key-value checksum verification failed",
                    Malformed::ChecksumMismatch,
                );
            }
        }
    }

    fn key(&self) -> &[u8] {
        if self.key_from_buf {
            &self.key_buf
        } else {
            self.raw_key()
        }
    }
}

/// The previous entries a data iterator's `Prev` caches, one restart interval at a time
/// [R block.h:833-859].
#[derive(Debug, Default)]
struct PrevCache {
    entries: Vec<PrevEntry>,
    keys: Vec<u8>,
    idx: i64,
}

#[derive(Debug, Clone, Copy)]
struct PrevEntry {
    offset: u32,
    entry_size: u32,
    /// The key in the block, or in `PrevCache::keys` when rebuilt.
    key: (u32, u32),
    in_block: bool,
    value: (u32, u32),
}

#[derive(Debug)]
struct DataState {
    hash_index: Option<DataBlockHashIndex>,
    prev: PrevCache,
}

/// An index entry's decoded value: its handle and where its first key is.
#[derive(Debug, Default, Clone, Copy)]
struct DecodedIndexValue {
    handle: BlockHandle,
    /// `(start, len)` in the block, or in the global-sequence buffer.
    first_key: (u32, u32),
}

#[derive(Debug)]
struct IndexState {
    value_delta_encoded: bool,
    have_first_key: bool,
    search: BlockSearchType,
    decoded: DecodedIndexValue,
    global_seqno: SequenceNumber,
    /// The first key with the global sequence number in place: RocksDB's `GlobalSeqnoState`.
    seqno_first_key: Option<Vec<u8>>,
}

#[derive(Debug)]
enum Kind {
    Data(DataState),
    Index(IndexState),
    Meta,
}

/// A block's iterator: RocksDB's `DataBlockIter`, `IndexBlockIter` or `MetaBlockIter`, by the
/// [`Block`] method that made it.
#[derive(Debug)]
pub struct BlockIter<'a> {
    core: Core<'a>,
    kind: Kind,
}

impl<'a> BlockIter<'a> {
    /// `Valid`.
    pub fn valid(&self) -> bool {
        self.core.valid()
    }

    /// `status`: the error the iterator ended with, if any.
    pub fn status(&self) -> Result<(), Error> {
        match &self.core.status {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// `key`: the current key, with the global sequence number in place.
    pub fn key(&self) -> &[u8] {
        self.core.key()
    }

    /// `user_key`: the current key less its trailer.
    pub fn user_key(&self) -> &[u8] {
        let raw = self.core.raw_key();
        if self.core.raw_key.is_user_key {
            raw
        } else {
            raw.get(..raw.len().saturating_sub(NUM_INTERNAL_BYTES))
                .unwrap_or_default()
        }
    }

    /// `value`: the current value; for an index block, the value as stored.
    pub fn value(&self) -> &[u8] {
        slice(self.core.data, self.core.value.0, self.core.value.1)
    }

    /// `raw_value` [R block.h:960-963].
    pub fn raw_value(&self) -> &[u8] {
        self.value()
    }

    /// `IndexBlockIter::value` [R block.h:945-958]: the current index entry's handle and first
    /// key. RocksDB asserts the stored value decodes; here a value that does not is corruption.
    pub fn index_value(&self) -> Result<IndexValue<'_>, Error> {
        let Kind::Index(state) = &self.kind else {
            return Err(Error::InvalidArgument {
                what: "an index value read from a block that is not an index block",
            });
        };
        if state.value_delta_encoded || state.seqno_first_key.is_some() {
            let first = match &state.seqno_first_key {
                Some(buf) => buf.as_slice(),
                None => slice(
                    self.core.data,
                    state.decoded.first_key.0,
                    state.decoded.first_key.1,
                ),
            };
            Ok(IndexValue {
                handle: state.decoded.handle,
                first_internal_key: first,
            })
        } else {
            let mut v = self.value();
            IndexValue::decode_from(&mut v, state.have_first_key, None)
        }
    }

    /// `SeekToFirst`.
    pub fn seek_to_first(&mut self) {
        self.seek_to_first_impl();
        self.core.update_key();
    }

    /// `SeekToLast`.
    pub fn seek_to_last(&mut self) {
        self.seek_to_last_impl();
        self.core.update_key();
    }

    /// `Seek`: the first entry at or after `target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.seek_impl(target);
        self.core.update_key();
    }

    /// `SeekForPrev`: the last entry at or before `target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek_for_prev_impl(target);
        self.core.update_key();
    }

    /// `Next`.
    pub fn next(&mut self) {
        let _ = self.next_impl();
        self.core.update_key();
    }

    /// `Prev`.
    pub fn prev(&mut self) {
        self.prev_impl();
        self.core.update_key();
    }

    /// `DataBlockIter::SeekForGet` [R block.h:795-808, block.cc:215-341]: positions for a point
    /// lookup of the internal key `target`, through the hash index where the block has one;
    /// false when the key cannot be in this block or the next.
    pub fn seek_for_get(&mut self, target: &[u8]) -> bool {
        let hash_index = match &self.kind {
            Kind::Data(state) => state.hash_index,
            _ => None,
        };
        let found = match hash_index {
            Some(index) => self.seek_for_get_impl(index, target),
            None => {
                self.seek_impl(target);
                true
            }
        };
        self.core.update_key();
        found
    }

    fn seek_for_get_impl(&mut self, index: DataBlockHashIndex, target: &[u8]) -> bool {
        let target_user_key = target
            .get(..target.len().saturating_sub(NUM_INTERNAL_BYTES))
            .unwrap_or_default();
        let bucket = match index.lookup(self.core.data, target_user_key) {
            Ok(b) => b,
            Err(e) => {
                self.core.status = Some(e);
                self.core.current = self.core.keys_end;
                return false;
            }
        };
        if bucket == COLLISION {
            self.seek_impl(target);
            return true;
        }
        let restart_index = if bucket == NO_ENTRY {
            self.core.num_restarts.saturating_sub(1)
        } else {
            u32::from(bucket)
        };
        if restart_index >= self.core.num_restarts {
            self.core
                .corruption("data block hash index", Malformed::OutOfRange);
            return false;
        }
        if !self.core.seek_to_restart_point(restart_index) {
            return false;
        }
        let Some(start) = self.core.restart_point(restart_index) else {
            return false;
        };
        self.core.current = start;
        let limit = if restart_index.saturating_add(1) < self.core.num_restarts {
            self.core
                .restart_point(restart_index.saturating_add(1))
                .unwrap_or(self.core.keys_end)
        } else {
            self.core.keys_end
        };
        while self.core.current < limit {
            if self.parse_next_data_key().is_none()
                || self.core.compare_current_key(target) != Ordering::Less
            {
                break;
            }
        }
        if self.core.current == self.core.restarts {
            return true;
        }
        if !self.core.valid() {
            return true;
        }
        let raw = self.core.raw_key();
        let user = raw
            .get(..raw.len().saturating_sub(NUM_INTERNAL_BYTES))
            .unwrap_or_default();
        if self
            .core
            .icmp
            .user_comparator()
            .compare(user, target_user_key)
            != Ordering::Equal
        {
            return false;
        }
        let kind = raw
            .get(raw.len().saturating_sub(NUM_INTERNAL_BYTES))
            .copied();
        let point = [
            ValueType::Value,
            ValueType::Deletion,
            ValueType::Merge,
            ValueType::SingleDeletion,
            ValueType::BlobIndex,
            ValueType::WideColumnEntity,
            ValueType::ValuePreferredSeqno,
        ];
        if !kind.is_some_and(|k| point.iter().any(|t| t.as_u8() == k)) {
            self.seek_impl(target);
        }
        true
    }

    fn parse_next_data_key(&mut self) -> Option<bool> {
        self.core.parse_next_key(true, false)
    }

    /// `IndexBlockIter::ParseNextIndexKey` [R block.cc:650-661].
    fn parse_next_index_key(&mut self) -> Option<bool> {
        let Kind::Index(state) = &self.kind else {
            return None;
        };
        let decode = state.value_delta_encoded || state.seqno_first_key.is_some();
        let with_length = !state.value_delta_encoded;
        let is_shared = self.core.parse_next_key(with_length, false)?;
        if decode && !self.decode_current_value(is_shared) {
            return None;
        }
        Some(is_shared)
    }

    /// `IndexBlockIter::DecodeCurrentValue` [R block.cc:663-716]: the value at the current entry,
    /// its handle given as a size delta where the key shares bytes.
    fn decode_current_value(&mut self, is_shared: bool) -> bool {
        let Kind::Index(state) = &mut self.kind else {
            return false;
        };
        let start = self.core.value.0;
        let mut v = slice(
            self.core.data,
            start,
            self.core.restarts.saturating_sub(start),
        );
        let before = v.len();
        let previous = if state.value_delta_encoded && is_shared {
            Some(state.decoded.handle)
        } else {
            None
        };
        let decoded = match IndexValue::decode_from(&mut v, state.have_first_key, previous.as_ref())
        {
            Ok(d) => d,
            Err(_) => {
                self.core
                    .corruption("bad index value in block", Malformed::Truncated);
                return false;
            }
        };
        let consumed = u32::try_from(before.saturating_sub(v.len())).unwrap_or(u32::MAX);
        self.core.value = (start, consumed);
        if self.core.values_section.is_none() && state.value_delta_encoded {
            self.core.entry.1 = self.core.entry.1.saturating_add(consumed);
        }
        // Where the first key lies in the block: after the handle, its length-prefix aside.
        let first_len = u32::try_from(decoded.first_internal_key.len()).unwrap_or(u32::MAX);
        let first_start = start.saturating_add(consumed).saturating_sub(first_len);
        state.decoded = DecodedIndexValue {
            handle: decoded.handle,
            first_key: (first_start, first_len),
        };
        if let Some(buf) = &mut state.seqno_first_key {
            let first = decoded.first_internal_key;
            let user = first.len().saturating_sub(NUM_INTERNAL_BYTES);
            let kind = first.get(user).copied().unwrap_or_default();
            buf.clear();
            buf.extend_from_slice(first.get(..user).unwrap_or_default());
            buf.extend_from_slice(&((state.global_seqno << 8) | u64::from(kind)).to_le_bytes());
        }
        true
    }

    /// `NextImpl`: whether an entry was read.
    fn next_impl(&mut self) -> bool {
        match self.kind {
            Kind::Data(_) => self.parse_next_data_key().is_some(),
            Kind::Meta => self.core.parse_next_key(true, true).is_some(),
            Kind::Index(_) => self.parse_next_index_key().is_some(),
        }
    }

    fn with_length(&self) -> bool {
        !matches!(&self.kind, Kind::Index(s) if s.value_delta_encoded)
    }

    fn seek_to_first_impl(&mut self) {
        if !self.core.live {
            return;
        }
        if matches!(self.kind, Kind::Index(_)) {
            self.core.status = None;
        }
        if self.core.seek_to_restart_point(0) {
            let _ = self.next_impl();
        }
    }

    fn seek_to_last_impl(&mut self) {
        if !self.core.live {
            return;
        }
        if matches!(self.kind, Kind::Index(_)) {
            self.core.status = None;
        }
        if !self
            .core
            .seek_to_restart_point(self.core.num_restarts.saturating_sub(1))
        {
            return;
        }
        while self.next_impl() && self.core.next_entry_offset() < self.core.keys_end {}
    }

    /// `FindKeyAfterBinarySeek` [R block.cc:718-757].
    fn find_key_after_binary_seek(&mut self, target: &[u8], index: u32, skip: bool) {
        if !self.core.seek_to_restart_point(index) {
            return;
        }
        let _ = self.next_impl();
        if skip {
            return;
        }
        let max_offset = if index.saturating_add(1) < self.core.num_restarts {
            self.core
                .restart_point(index.saturating_add(1))
                .unwrap_or(u32::MAX)
        } else {
            u32::MAX
        };
        loop {
            let _ = self.next_impl();
            if !self.core.valid()
                || self.core.current == max_offset
                || self.core.compare_current_key(target) != Ordering::Less
            {
                break;
            }
        }
    }

    fn seek_impl(&mut self, target: &[u8]) {
        if !self.core.live {
            return;
        }
        let with_length = self.with_length();
        let (seek_key, search): (&[u8], BlockSearchType) = match &self.kind {
            Kind::Index(state) => {
                self.core.status = None;
                let key = if self.core.raw_key.is_user_key {
                    target
                        .get(..target.len().saturating_sub(NUM_INTERNAL_BYTES))
                        .unwrap_or_default()
                } else {
                    target
                };
                (key, state.search)
            }
            _ => (target, BlockSearchType::Binary),
        };
        let found = if search == BlockSearchType::Interpolation {
            self.core.interpolation_seek(seek_key, with_length)
        } else {
            self.core.binary_seek(seek_key, with_length)
        };
        if let Some((index, skip)) = found {
            self.find_key_after_binary_seek(seek_key, index, skip);
        }
    }

    fn seek_for_prev_impl(&mut self, target: &[u8]) {
        if matches!(self.kind, Kind::Index(_)) {
            self.core.current = self.core.keys_end;
            self.core.restart_index = self.core.num_restarts;
            self.core.status = Some(Error::InvalidArgument {
                what: "SeekForPrev on an index block",
            });
            self.core.raw_key.clear();
            self.core.value = (0, 0);
            return;
        }
        if !self.core.live {
            return;
        }
        let Some((index, skip)) = self.core.binary_seek(target, true) else {
            return;
        };
        self.find_key_after_binary_seek(target, index, skip);
        if !self.core.valid() {
            if self.core.status.is_none() {
                self.seek_to_last_impl();
            }
        } else {
            while self.core.valid() && self.core.compare_current_key(target) == Ordering::Greater {
                self.prev_impl();
            }
        }
    }

    fn prev_impl(&mut self) {
        if !self.core.valid() {
            return;
        }
        if matches!(self.kind, Kind::Data(_)) {
            self.data_prev();
            return;
        }
        let original = self.core.current;
        let prev_entry_idx = self.core.cur_entry_idx.saturating_sub(1);
        if !self.back_to_restart_before(original) {
            return;
        }
        while self.next_impl() && self.core.next_entry_offset() < original {}
        self.core.cur_entry_idx = prev_entry_idx;
    }

    /// Moves to the last restart point before `original`; false when there is none, the
    /// iterator then ended.
    fn back_to_restart_before(&mut self, original: u32) -> bool {
        loop {
            match self.core.restart_point(self.core.restart_index) {
                Some(r) if r >= original => {
                    if self.core.restart_index == 0 {
                        self.core.current = self.core.keys_end;
                        self.core.restart_index = self.core.num_restarts;
                        return false;
                    }
                    self.core.restart_index = self.core.restart_index.saturating_sub(1);
                }
                Some(_) => break,
                None => {
                    self.core
                        .corruption("block restart point", Malformed::Truncated);
                    return false;
                }
            }
        }
        self.core.seek_to_restart_point(self.core.restart_index)
    }

    /// `DataBlockIter::PrevImpl` [R block.cc:93-179]: the previous entry, from the entries of its
    /// restart interval cached on the first step back.
    fn data_prev(&mut self) {
        let prev_entry_idx = self.core.cur_entry_idx.saturating_sub(1);
        let Kind::Data(state) = &mut self.kind else {
            return;
        };
        let cached = usize::try_from(state.prev.idx)
            .ok()
            .filter(|&i| i > 0)
            .and_then(|i| state.prev.entries.get(i).map(|e| (i, e.offset)));
        if let Some((i, offset)) = cached
            && offset == self.core.current
        {
            let i = i.saturating_sub(1);
            state.prev.idx = i64::try_from(i).unwrap_or(-1);
            if let Some(e) = state.prev.entries.get(i).copied() {
                self.core.current = e.offset;
                if e.in_block {
                    self.core.raw_key.set_borrowed(e.key.0, e.key.1);
                } else {
                    let key = slice(&state.prev.keys, e.key.0, e.key.1);
                    self.core.raw_key.borrowed = None;
                    self.core.raw_key.owned.clear();
                    self.core.raw_key.owned.extend_from_slice(key);
                }
                self.core.value = e.value;
                self.core.entry = (e.offset, e.entry_size);
                self.core.cur_entry_idx = prev_entry_idx;
            }
            return;
        }
        state.prev.idx = -1;
        state.prev.entries.clear();
        state.prev.keys.clear();
        let original = self.core.current;
        if !self.back_to_restart_before(original) {
            self.core.cur_entry_idx = prev_entry_idx;
            return;
        }
        loop {
            if self.parse_next_data_key().is_none() {
                break;
            }
            let Kind::Data(state) = &mut self.kind else {
                return;
            };
            let (key, in_block) = match self.core.raw_key.borrowed {
                Some(range) => (range, true),
                None => {
                    let at = u32::try_from(state.prev.keys.len()).unwrap_or(u32::MAX);
                    state.prev.keys.extend_from_slice(&self.core.raw_key.owned);
                    let len = u32::try_from(self.core.raw_key.owned.len()).unwrap_or(u32::MAX);
                    ((at, len), false)
                }
            };
            state.prev.entries.push(PrevEntry {
                offset: self.core.current,
                entry_size: self.core.entry.1,
                key,
                in_block,
                value: self.core.value,
            });
            if self.core.next_entry_offset() >= original {
                break;
            }
        }
        if let Kind::Data(state) = &mut self.kind {
            state.prev.idx = i64::try_from(state.prev.entries.len())
                .unwrap_or(0)
                .saturating_sub(1);
        }
        self.core.cur_entry_idx = prev_entry_idx;
    }

    /// `GetRestartInterval` [R block.h:517-530]: the entries of the first restart interval; one
    /// for a meta block; 0 with fewer than two restarts.
    fn restart_interval(&mut self) -> Result<u32, Error> {
        if matches!(self.kind, Kind::Meta) {
            return Ok(1);
        }
        if self.core.num_restarts <= 1 || !self.core.live {
            return Ok(0);
        }
        self.seek_to_first_impl();
        let end = self
            .core
            .restart_point(1)
            .ok_or(Error::truncated("block restart point"))?;
        let mut count = 1u32;
        while self.core.next_entry_offset() < end && self.core.status.is_none() {
            let _ = self.next_impl();
            count = count.saturating_add(1);
        }
        self.status()?;
        Ok(count)
    }

    /// `NumberOfKeys` [R block.h:532-549]; a meta block holds one key a restart.
    fn number_of_keys(&mut self, interval: u32) -> Result<u32, Error> {
        if matches!(self.kind, Kind::Meta) {
            return Ok(self.core.num_restarts);
        }
        if self.core.num_restarts == 0 || !self.core.live {
            return Ok(0);
        }
        let mut count = self
            .core
            .num_restarts
            .saturating_sub(1)
            .saturating_mul(interval);
        self.core
            .seek_to_restart_point(self.core.num_restarts.saturating_sub(1));
        let keys_end = self.core.keys_end;
        while self.core.next_entry_offset() < keys_end && self.core.status.is_none() {
            let _ = self.next_impl();
            count = count.saturating_add(1);
        }
        self.status()?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    //! `DataBlockKVChecksumCorruptionTest`, `IndexBlockKVChecksumCorruptionTest` and
    //! `MetaIndexBlockKVChecksumCorruptionTest` [R block_test.cc:1734-2075]: a value changed in
    //! memory after its block's checksums were made ends every move that reads it with
    //! corruption. RocksDB changes it through a sync point as the iterator reads it; here the
    //! test changes the block's bytes, which only this module can reach.

    use super::*;
    use crate::db::dbformat::{ValueType, append_internal_key_footer};
    use crate::table::block_based::block_builder::{BlockBuilder, BlockBuilderOptions};

    fn key(i: u32) -> Vec<u8> {
        let mut k = format!("{i:6}{:4}{}", 0, "p".repeat(24)).into_bytes();
        append_internal_key_footer(&mut k, 0, ValueType::Value).unwrap();
        k
    }

    fn value(i: u32) -> Vec<u8> {
        (0..100u32)
            .map(|j| b' ' + u8::try_from((i * 31 + j) % 95).unwrap())
            .collect()
    }

    /// Where each entry's value lies in the block, read by `iter`.
    fn value_starts(mut iter: BlockIter<'_>) -> Vec<(u32, u32)> {
        let mut starts = Vec::new();
        iter.seek_to_first();
        while iter.valid() {
            starts.push(iter.core.value);
            iter.next();
        }
        starts
    }

    /// Changes byte 10 of every value but the `spared` one, as RocksDB's sync point does, or
    /// the last byte of a value shorter than that: an index value of a handle alone is.
    fn corrupt(block: &mut Block, values: &[(u32, u32)], spared: Option<usize>) {
        for (i, &(start, len)) in values.iter().enumerate() {
            if Some(i) != spared {
                let at = start as usize + 10.min(len as usize - 1);
                block.contents[at] = block.contents[at].wrapping_add(1);
            }
        }
    }

    /// A move under test, given the seek key.
    type Move = fn(&mut BlockIter<'_>, &[u8]);

    fn is_corruption(iter: &BlockIter<'_>) -> bool {
        !iter.valid() && matches!(iter.status(), Err(Error::Corruption { .. }))
    }

    /// Every move on a block whose values were all changed, then a step off an entry left
    /// whole onto changed ones.
    fn check(
        build: impl Fn() -> Block,
        new_iter: impl for<'b> Fn(&'b Block) -> BlockIter<'b>,
        records: usize,
        seek_for_prev: bool,
        seek_for_get: bool,
    ) {
        let seek_key = key(u32::try_from(records / 2).unwrap());
        let moves: [(&str, Move); 5] = [
            ("first", |it, _| it.seek_to_first()),
            ("last", |it, _| it.seek_to_last()),
            ("seek", |it, k| it.seek(k)),
            ("seek for prev", |it, k| it.seek_for_prev(k)),
            ("seek for get", |it, k| {
                it.seek_for_get(k);
            }),
        ];
        for (name, mv) in moves {
            if (name == "seek for prev" && !seek_for_prev)
                || (name == "seek for get" && !seek_for_get)
            {
                continue;
            }
            let mut block = build();
            let starts = value_starts(new_iter(&block));
            assert_eq!(starts.len(), records);
            corrupt(&mut block, &starts, None);
            let mut it = new_iter(&block);
            it.status().unwrap();
            mv(&mut it, &seek_key);
            assert!(is_corruption(&it), "{name}");
        }
        if records > 1 {
            for forward in [false, true] {
                let mut block = build();
                let starts = value_starts(new_iter(&block));
                corrupt(&mut block, &starts, Some(records / 2));
                let mut it = new_iter(&block);
                it.seek(&seek_key);
                assert!(it.valid());
                it.status().unwrap();
                if forward {
                    it.next();
                } else {
                    it.prev();
                }
                assert!(is_corruption(&it), "step forward {forward}");
            }
        }
    }

    fn data_block(records: usize, hash: bool, width: u8, interval: u32, delta: bool) -> Block {
        let mut b = BlockBuilder::new(BlockBuilderOptions {
            restart_interval: interval,
            use_delta_encoding: delta,
            index_type: if hash {
                DataBlockIndexType::BinaryAndHash
            } else {
                DataBlockIndexType::BinarySearch
            },
            ..BlockBuilderOptions::default()
        })
        .unwrap();
        for i in 0..u32::try_from(records).unwrap() {
            b.add(&key(i), &value(i), None, false).unwrap();
        }
        let mut block = Block::new(b.finish().unwrap().to_vec(), interval);
        block
            .initialize_data_block_protection_info(width, Comparator::Bytewise)
            .unwrap();
        block
    }

    #[test]
    fn data_block_corrupt_entry() {
        for hash in [false, true] {
            for width in [4u8, 8] {
                for interval in [1u32, 3, 8, 16] {
                    for delta in [false, true] {
                        for intervals in [1usize, 3] {
                            let records = intervals * interval as usize;
                            check(
                                || data_block(records, hash, width, interval, delta),
                                |b| {
                                    b.new_data_iterator(
                                        Comparator::Bytewise,
                                        DISABLE_GLOBAL_SEQUENCE_NUMBER,
                                    )
                                },
                                records,
                                true,
                                true,
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn meta_index_block_corrupt_entry() {
        for width in [4u8, 8] {
            for records in [1usize, 3] {
                let build = || {
                    let mut b = BlockBuilder::new(BlockBuilderOptions {
                        restart_interval: 1,
                        ..BlockBuilderOptions::default()
                    })
                    .unwrap();
                    for i in 0..u32::try_from(records).unwrap() {
                        b.add(&key(i), &value(i), None, false).unwrap();
                    }
                    let mut block = Block::new(b.finish().unwrap().to_vec(), 1);
                    block
                        .initialize_meta_index_block_protection_info(width)
                        .unwrap();
                    block
                };
                check(build, |b| b.new_meta_iterator(), records, true, false);
            }
        }
    }

    #[test]
    fn index_block_corrupt_entry() {
        for width in [4u8, 8] {
            for interval in [1u32, 3, 8, 16] {
                for value_delta in [false, true] {
                    for first_key in [false, true] {
                        for intervals in [1usize, 3] {
                            let records = intervals * interval as usize;
                            let build = || {
                                let mut b = BlockBuilder::new(BlockBuilderOptions {
                                    restart_interval: interval,
                                    use_value_delta_encoding: value_delta,
                                    ..BlockBuilderOptions::default()
                                })
                                .unwrap();
                                let mut last: Option<BlockHandle> = None;
                                for i in 0..u32::try_from(records).unwrap() {
                                    let handle = BlockHandle {
                                        offset: u64::from(i) * 4101,
                                        size: 4096,
                                    };
                                    // A first key long enough that byte 10 of the value is in it,
                                    // or in the handle and the padding after it.
                                    let first = key(i);
                                    let v = IndexValue {
                                        handle,
                                        first_internal_key: &first,
                                    };
                                    let mut full = Vec::new();
                                    v.encode_to(&mut full, first_key, None).unwrap();
                                    let mut delta = Vec::new();
                                    if let Some(prev) = last.filter(|_| value_delta) {
                                        v.encode_to(&mut delta, first_key, Some(&prev)).unwrap();
                                    }
                                    b.add(&key(i), &full, Some(&delta), false).unwrap();
                                    last = Some(handle);
                                }
                                let mut block = Block::new(b.finish().unwrap().to_vec(), interval);
                                block
                                    .initialize_index_block_protection_info(
                                        width,
                                        Comparator::Bytewise,
                                        !value_delta,
                                        first_key,
                                    )
                                    .unwrap();
                                block
                            };
                            let options = IndexIterOptions {
                                have_first_key: first_key,
                                key_includes_seq: true,
                                value_is_full: !value_delta,
                                search: BlockSearchType::Binary,
                            };
                            check(
                                build,
                                move |b| {
                                    b.new_index_iterator(
                                        Comparator::Bytewise,
                                        DISABLE_GLOBAL_SEQUENCE_NUMBER,
                                        options,
                                    )
                                },
                                records,
                                false,
                                false,
                            );
                        }
                    }
                }
            }
        }
    }
}
