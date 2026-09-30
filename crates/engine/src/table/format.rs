//! Block checksums: `ChecksumType` of `include/rocksdb/table.h` [R :117-123], and
//! `ComputeBuiltinChecksum`, `ComputeBuiltinChecksumWithLastByte` and
//! `ChecksumModifierForContext` of `table/format.{h,cc}` (docs/research/24 §1.2, §1.7).
//!
//! A block's trailer stores the checksum of the block and its one compression-type byte. From
//! format_version 6 each stored checksum also carries a modifier from the file's random base and
//! the block's offset, so a block read from the wrong place, or from another file, fails its
//! check [R table/format.h:143-170].

use crate::error::Error;
use crate::util::crc32c;
use crate::util::hash::{lower32of64, upper32of64};
use crate::util::xxhash::{xxh3_64bits, xxh32, xxh32_with_last_byte, xxh64, xxh64_with_last_byte};

/// The checksum a table's blocks carry [R include/rocksdb/table.h:117-123]; the byte is stored
/// in the table's properties and footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    NoChecksum,
    Crc32c,
    XxHash,
    XxHash64,
    /// The default [R include/rocksdb/table.h:374]; RocksDB 6.27 and later.
    Xxh3,
}

impl ChecksumType {
    /// The stored byte.
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::NoChecksum => 0x0,
            Self::Crc32c => 0x1,
            Self::XxHash => 0x2,
            Self::XxHash64 => 0x3,
            Self::Xxh3 => 0x4,
        }
    }

    /// The type a stored byte names. RocksDB computes 0 for an unknown one and so reports a
    /// checksum mismatch; here it is refused as unsupported.
    pub const fn from_byte(b: u8) -> Result<Self, Error> {
        match b {
            0x0 => Ok(Self::NoChecksum),
            0x1 => Ok(Self::Crc32c),
            0x2 => Ok(Self::XxHash),
            0x3 => Ok(Self::XxHash64),
            0x4 => Ok(Self::Xxh3),
            other => Err(Error::Unsupported {
                feature: "block checksum type",
                value: other as u64,
            }),
        }
    }
}

/// The multiplier of `ModifyChecksumForLastByte` [R table/format.cc:606-612].
pub const LAST_BYTE_PRIME: u32 = 0x6b90_83d9;

/// `ModifyChecksumForLastByte` [R table/format.cc:606-612]: folds one more byte into an XXH3
/// checksum; its own inverse.
const fn modify_checksum_for_last_byte(checksum: u32, last_byte: u8) -> u32 {
    checksum ^ (last_byte as u32).wrapping_mul(LAST_BYTE_PRIME)
}

/// `ComputeBuiltinChecksum` [R table/format.cc:615-639]: the checksum of `data` by `kind`.
pub fn compute_builtin_checksum(kind: ChecksumType, data: &[u8]) -> u32 {
    match kind {
        ChecksumType::NoChecksum => 0,
        ChecksumType::Crc32c => crc32c::mask(crc32c::value(data)),
        ChecksumType::XxHash => xxh32(data, 0),
        ChecksumType::XxHash64 => lower32of64(xxh64(data, 0)),
        // All but the last byte through XXH3, then the last byte folded in; 0 for no bytes.
        ChecksumType::Xxh3 => match data.split_last() {
            Some((&last, head)) => {
                modify_checksum_for_last_byte(lower32of64(xxh3_64bits(head)), last)
            }
            None => 0,
        },
    }
}

/// `ComputeBuiltinChecksumWithLastByte` [R table/format.cc:641-682]: the checksum of
/// `data ‖ last_byte` (a block and its compression type) without joining them.
pub fn compute_builtin_checksum_with_last_byte(
    kind: ChecksumType,
    data: &[u8],
    last_byte: u8,
) -> u32 {
    match kind {
        ChecksumType::NoChecksum => 0,
        ChecksumType::Crc32c => crc32c::mask(crc32c::extend(crc32c::value(data), &[last_byte])),
        ChecksumType::XxHash => xxh32_with_last_byte(data, last_byte, 0),
        ChecksumType::XxHash64 => lower32of64(xxh64_with_last_byte(data, last_byte, 0)),
        ChecksumType::Xxh3 => {
            modify_checksum_for_last_byte(lower32of64(xxh3_64bits(data)), last_byte)
        }
    }
}

/// `ChecksumModifierForContext` [R table/format.h:143-170]: added (wrapping) to a stored
/// checksum from format_version 6; 0 when the file's `base_context_checksum` is 0.
pub const fn checksum_modifier_for_context(base_context_checksum: u32, offset: u64) -> u32 {
    if base_context_checksum == 0 {
        return 0;
    }
    base_context_checksum ^ lower32of64(offset).wrapping_add(upper32of64(offset))
}

/// The first format_version whose checksums carry the context modifier
/// [R table/format.h:218-220].
pub const CONTEXT_CHECKSUM_FORMAT_VERSION: u32 = 6;

/// `FormatVersionUsesContextChecksum` [R table/format.h:218-220].
pub const fn format_version_uses_context_checksum(version: u32) -> bool {
    version >= CONTEXT_CHECKSUM_FORMAT_VERSION
}
