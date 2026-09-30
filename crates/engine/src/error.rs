//! The engine's typed errors, which replace RocksDB's `Status` codes (docs/research/24 §2.3).
//!
//! RocksDB reports a bad decode as `Status::Corruption`, a `false` return or a null pointer,
//! and a refused format as `Status::NotSupported`; its decoders also trust some inputs and
//! read past them (docs/research/24 §5 R6). Here each of those is a variant a caller can match.

use std::fmt;

/// An engine failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Stored bytes that do not decode: RocksDB's `Status::Corruption`. It feeds repair
    /// (CLAUDE.md §6).
    #[error("corruption in {what}: {why}")]
    Corruption { what: &'static str, why: Malformed },
    /// A value the format defines, or does not, that this engine does not read or write:
    /// RocksDB's `Status::NotSupported` (docs/research/24 §1.18).
    #[error("unsupported {feature}: {value}")]
    Unsupported { feature: &'static str, value: u64 },
    /// A caller's argument the format cannot represent: RocksDB's `Status::InvalidArgument`, or
    /// a C++ precondition RocksDB leaves unchecked.
    #[error("invalid argument: {what}")]
    InvalidArgument { what: &'static str },
}

/// How stored bytes fail to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Malformed {
    /// The input ends before the value does.
    Truncated,
    /// A varint whose last permitted byte still has its continuation bit set.
    VarintTooLong,
    /// A varint whose last byte carries bits above its type's width, which RocksDB drops and
    /// never writes (docs/research/24 §1.1).
    VarintOverflow,
    /// A prefix varint whose first byte names a length its type does not have.
    PrefixVarintFirstByte,
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "input ends inside a value",
            Self::VarintTooLong => "varint longer than its type allows",
            Self::VarintOverflow => "varint carries bits above its type's width",
            Self::PrefixVarintFirstByte => "prefix varint's first byte names no valid length",
        })
    }
}

impl Error {
    pub(crate) const fn truncated(what: &'static str) -> Self {
        Self::Corruption {
            what,
            why: Malformed::Truncated,
        }
    }
}
