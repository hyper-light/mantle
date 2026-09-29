//! Commands and their answers as a range's Raft entries carry them (docs/design/replica.md
//! §1). Numbers are little-endian, and every length and count is bounded by the bytes that
//! follow it, so a malformed entry decodes to an error and never to a panic or an allocation
//! it cannot fill. Time is the entry's: every command in it takes the leader's time from
//! when the entry was proposed.

use mantle_codec::{Reader, Writer};

use crate::record::{
    self, BlockHeader, Checksum, ChunkPlace, DefaultRetention, Extent, GateState, Part,
    RecordError, Referrer, Upload, Verdict, Version, Versioning,
};
use crate::{block, bucket, file, name};

const FORMAT: u8 = 1;

/// Commands one entry carries at most: a session is named by the index of the entry that
/// registered it and the registration's place in that entry.
pub const MAX_COMMANDS: usize = 1 << 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The leader's time when it proposed the entry, nanoseconds since the Unix epoch.
    pub at_ns: u64,
    pub commands: Vec<Sessioned>,
}

/// A command from a gateway's session (06 §A1.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sessioned {
    pub session: u64,
    pub serial: u64,
    /// The lowest serial number whose answer the gateway has not yet received: the range
    /// forgets the answers before it.
    pub unanswered: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Opens a session; the command's own session fields are not read.
    Register,
    Bucket(bucket::Command),
    Name(Box<name::Command>),
    File(file::Command),
    Block(block::Command),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Registered {
        session: u64,
    },
    /// The session is unknown or expired, and the command was not applied.
    SessionExpired,
    /// A serial the session answered and has since forgotten, as the gateway acknowledged
    /// it: a repeat, not applied again.
    Repeated,
    /// The command is for another layer than the range's.
    WrongLayer,
    Bucket(bucket::Outcome),
    Name(name::Outcome),
    File(file::Outcome),
    Block(block::Outcome),
}

fn too_large(len: usize) -> RecordError {
    RecordError::TooLarge(len)
}

fn corrupt() -> RecordError {
    RecordError::Corrupt("entry")
}

impl Entry {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        if self.commands.len() > MAX_COMMANDS {
            return Err(too_large(self.commands.len()));
        }
        let mut w = Writer::default();
        w.u8(FORMAT);
        w.u64(self.at_ns);
        record::put_len(&mut w, self.commands.len())?;
        for c in &self.commands {
            w.u64(c.session);
            w.u64(c.serial);
            w.u64(c.unanswered);
            put_command(&mut w, &c.command)?;
        }
        Ok(w.into_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = Reader::new(bytes);
        let entry = (|| {
            if r.u8()? != FORMAT {
                return None;
            }
            let at_ns = r.u64()?;
            let count = usize::try_from(r.u32()?).ok()?;
            // A command takes 25 bytes at least.
            if count > MAX_COMMANDS || count > r.remaining() / 25 {
                return None;
            }
            let mut commands = Vec::with_capacity(count);
            for _ in 0..count {
                commands.push(Sessioned {
                    session: r.u64()?,
                    serial: r.u64()?,
                    unanswered: r.u64()?,
                    command: take_command(&mut r, at_ns)?,
                });
            }
            Some(Self { at_ns, commands })
        })();
        entry.filter(|_| r.remaining() == 0).ok_or_else(corrupt)
    }
}

impl Answer {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = Writer::default();
        match self {
            Answer::Registered { session } => {
                w.u8(0);
                w.u64(*session);
            }
            Answer::SessionExpired => w.u8(1),
            Answer::WrongLayer => w.u8(2),
            Answer::Repeated => w.u8(7),
            Answer::Bucket(o) => {
                w.u8(3);
                put_bucket_outcome(&mut w, o);
            }
            Answer::Name(o) => {
                w.u8(4);
                put_name_outcome(&mut w, o)?;
            }
            Answer::File(o) => {
                w.u8(5);
                match o {
                    file::Outcome::Written { deadline_ns } => {
                        w.u8(0);
                        w.u64(*deadline_ns);
                    }
                    file::Outcome::Deleted => w.u8(1),
                    file::Outcome::Conflict => w.u8(2),
                    file::Outcome::Invalid => w.u8(3),
                    file::Outcome::Settled => w.u8(4),
                    file::Outcome::Expired => w.u8(5),
                    file::Outcome::BlocksChecked(verdicts) => {
                        w.u8(6);
                        put_verdicts(&mut w, verdicts)?;
                    }
                }
            }
            Answer::Block(o) => {
                w.u8(6);
                match o {
                    block::Outcome::Written { deadline_ns } => {
                        w.u8(0);
                        w.u64(*deadline_ns);
                    }
                    block::Outcome::Moved => w.u8(1),
                    block::Outcome::Deleted => w.u8(2),
                    block::Outcome::Conflict => w.u8(3),
                    block::Outcome::Invalid => w.u8(4),
                    block::Outcome::NoSuchBlock => w.u8(5),
                    block::Outcome::Settled => w.u8(6),
                }
            }
        }
        Ok(w.into_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = Reader::new(bytes);
        let answer = (|| {
            Some(match r.u8()? {
                0 => Answer::Registered { session: r.u64()? },
                1 => Answer::SessionExpired,
                2 => Answer::WrongLayer,
                7 => Answer::Repeated,
                3 => Answer::Bucket(take_bucket_outcome(&mut r)?),
                4 => Answer::Name(take_name_outcome(&mut r)?),
                5 => Answer::File(match r.u8()? {
                    0 => file::Outcome::Written {
                        deadline_ns: r.u64()?,
                    },
                    1 => file::Outcome::Deleted,
                    2 => file::Outcome::Conflict,
                    3 => file::Outcome::Invalid,
                    4 => file::Outcome::Settled,
                    5 => file::Outcome::Expired,
                    6 => file::Outcome::BlocksChecked(take_verdicts(&mut r)?),
                    _ => return None,
                }),
                6 => Answer::Block(match r.u8()? {
                    0 => block::Outcome::Written {
                        deadline_ns: r.u64()?,
                    },
                    1 => block::Outcome::Moved,
                    2 => block::Outcome::Deleted,
                    3 => block::Outcome::Conflict,
                    4 => block::Outcome::Invalid,
                    5 => block::Outcome::NoSuchBlock,
                    6 => block::Outcome::Settled,
                    _ => return None,
                }),
                _ => return None,
            })
        })();
        answer
            .filter(|_| r.remaining() == 0)
            .ok_or(RecordError::Corrupt("answer"))
    }
}

fn put_command(w: &mut Writer, command: &Command) -> Result<(), RecordError> {
    match command {
        Command::Register => w.u8(0),
        Command::Bucket(c) => {
            w.u8(1);
            put_bucket(w, c)?;
        }
        Command::Name(c) => {
            w.u8(2);
            put_name(w, c)?;
        }
        Command::File(c) => {
            w.u8(3);
            match c {
                file::Command::Write {
                    file,
                    extents,
                    referrer,
                    handover_ns,
                    blocks_deadline_ns,
                    ..
                } => {
                    w.u8(0);
                    w.u128(*file);
                    record::put_len(w, extents.len())?;
                    for e in extents {
                        record::put_bytes(w, &e.encode())?;
                    }
                    record::put_bytes(w, &referrer.encode()?)?;
                    w.u64(*handover_ns);
                    w.u64(*blocks_deadline_ns);
                }
                file::Command::Delete { file } => {
                    w.u8(1);
                    w.u128(*file);
                }
                file::Command::Settle { files } => {
                    w.u8(2);
                    record::put_len(w, files.len())?;
                    for &f in files {
                        w.u128(f);
                    }
                }
                file::Command::CheckBlocks { file, blocks, .. } => {
                    w.u8(3);
                    w.u128(*file);
                    record::put_len(w, blocks.len())?;
                    for &(block, deadline_ns) in blocks {
                        w.u128(block);
                        w.u64(deadline_ns);
                    }
                }
            }
        }
        Command::Block(c) => {
            w.u8(4);
            match c {
                block::Command::Write {
                    block,
                    header,
                    chunks,
                    file,
                    handover_ns,
                    ..
                } => {
                    w.u8(0);
                    w.u128(*block);
                    record::put_bytes(w, &header.encode())?;
                    record::put_len(w, chunks.len())?;
                    for c in chunks {
                        record::put_bytes(w, &c.encode())?;
                    }
                    w.u128(*file);
                    w.u64(*handover_ns);
                }
                block::Command::Move { chunk, from, to } => {
                    w.u8(1);
                    chunk.encode(w);
                    w.u128(*from);
                    w.u128(*to);
                }
                block::Command::Delete { block } => {
                    w.u8(2);
                    w.u128(*block);
                }
                block::Command::Settle { blocks } => {
                    w.u8(3);
                    record::put_len(w, blocks.len())?;
                    for &b in blocks {
                        w.u128(b);
                    }
                }
            }
        }
    }
    Ok(())
}

fn take_command(r: &mut Reader<'_>, at_ns: u64) -> Option<Command> {
    Some(match r.u8()? {
        0 => Command::Register,
        1 => Command::Bucket(take_bucket(r, at_ns)?),
        2 => Command::Name(Box::new(take_name(r, at_ns)?)),
        3 => Command::File(match r.u8()? {
            0 => {
                let file = r.u128()?;
                let count = bounded(r, 4)?;
                let mut extents = Vec::with_capacity(count);
                for _ in 0..count {
                    extents.push(Extent::decode(&record::take_bytes(r)?).ok()?);
                }
                file::Command::Write {
                    file,
                    extents,
                    referrer: Referrer::decode(&record::take_bytes(r)?).ok()?,
                    handover_ns: r.u64()?,
                    blocks_deadline_ns: r.u64()?,
                    at_ns,
                }
            }
            1 => file::Command::Delete { file: r.u128()? },
            2 => {
                let count = bounded(r, 16)?;
                let mut files = Vec::with_capacity(count);
                for _ in 0..count {
                    files.push(r.u128()?);
                }
                file::Command::Settle { files }
            }
            3 => {
                let file = r.u128()?;
                let count = bounded(r, 24)?;
                let mut blocks = Vec::with_capacity(count);
                for _ in 0..count {
                    blocks.push((r.u128()?, r.u64()?));
                }
                file::Command::CheckBlocks {
                    file,
                    blocks,
                    at_ns,
                }
            }
            _ => return None,
        }),
        4 => Command::Block(match r.u8()? {
            0 => {
                let block = r.u128()?;
                let header = BlockHeader::decode(&record::take_bytes(r)?).ok()?;
                let count = bounded(r, 4)?;
                let mut chunks = Vec::with_capacity(count);
                for _ in 0..count {
                    chunks.push(ChunkPlace::decode(&record::take_bytes(r)?).ok()?);
                }
                block::Command::Write {
                    block,
                    header,
                    chunks,
                    file: r.u128()?,
                    handover_ns: r.u64()?,
                    at_ns,
                }
            }
            1 => block::Command::Move {
                chunk: mantle_chunk::ChunkKey::decode(r)?,
                from: r.u128()?,
                to: r.u128()?,
            },
            2 => block::Command::Delete { block: r.u128()? },
            3 => {
                let count = bounded(r, 16)?;
                let mut blocks = Vec::with_capacity(count);
                for _ in 0..count {
                    blocks.push(r.u128()?);
                }
                block::Command::Settle { blocks }
            }
            _ => return None,
        }),
        _ => return None,
    })
}

/// A count read before items of at least `least` bytes each, refused when the rest cannot
/// hold that many.
fn bounded(r: &mut Reader<'_>, least: usize) -> Option<usize> {
    let count = usize::try_from(r.u32()?).ok()?;
    if r.remaining().checked_div(least)? >= count {
        Some(count)
    } else {
        None
    }
}

fn put_bucket(w: &mut Writer, c: &bucket::Command) -> Result<(), RecordError> {
    match c {
        bucket::Command::Create(c) => {
            w.u8(0);
            record::put_str(w, &c.bucket)?;
            record::put_str(w, &c.owner)?;
            record::put_str(w, &c.location)?;
            w.u32(c.quota);
            w.u8(u8::from(c.lock));
        }
        bucket::Command::Activate { bucket, attempt } => {
            w.u8(1);
            record::put_str(w, bucket)?;
            w.u64(*attempt);
        }
        bucket::Command::Version {
            bucket,
            incarnation,
            versioning,
        } => {
            w.u8(2);
            record::put_str(w, bucket)?;
            w.u64(*incarnation);
            w.u8(versioning.code());
        }
        bucket::Command::BeginDelete { bucket, .. } => {
            w.u8(3);
            record::put_str(w, bucket)?;
        }
        bucket::Command::Abandon { bucket, .. } => {
            w.u8(4);
            record::put_str(w, bucket)?;
        }
        bucket::Command::Restore { bucket, attempt } => {
            w.u8(5);
            record::put_str(w, bucket)?;
            w.u64(*attempt);
        }
        bucket::Command::Delete { bucket, attempt } => {
            w.u8(6);
            record::put_str(w, bucket)?;
            w.u64(*attempt);
        }
        bucket::Command::Forget { bucket, attempt } => {
            w.u8(7);
            record::put_str(w, bucket)?;
            w.u64(*attempt);
        }
        bucket::Command::Lock {
            bucket,
            incarnation,
            default,
        } => {
            w.u8(8);
            record::put_str(w, bucket)?;
            w.u64(*incarnation);
            put_default(w, *default);
        }
        bucket::Command::Progress {
            bucket, attempt, ..
        } => {
            w.u8(9);
            record::put_str(w, bucket)?;
            w.u64(*attempt);
        }
    }
    Ok(())
}

fn take_bucket(r: &mut Reader<'_>, at_ns: u64) -> Option<bucket::Command> {
    Some(match r.u8()? {
        0 => bucket::Command::Create(bucket::Create {
            bucket: record::take_str(r)?,
            owner: record::take_str(r)?,
            location: record::take_str(r)?,
            at_ns,
            quota: r.u32()?,
            lock: take_bool(r)?,
        }),
        1 => bucket::Command::Activate {
            bucket: record::take_str(r)?,
            attempt: r.u64()?,
        },
        2 => bucket::Command::Version {
            bucket: record::take_str(r)?,
            incarnation: r.u64()?,
            versioning: Versioning::from_code(r.u8()?)?,
        },
        3 => bucket::Command::BeginDelete {
            bucket: record::take_str(r)?,
            at_ns,
        },
        4 => bucket::Command::Abandon {
            bucket: record::take_str(r)?,
            at_ns,
        },
        5 => bucket::Command::Restore {
            bucket: record::take_str(r)?,
            attempt: r.u64()?,
        },
        6 => bucket::Command::Delete {
            bucket: record::take_str(r)?,
            attempt: r.u64()?,
        },
        7 => bucket::Command::Forget {
            bucket: record::take_str(r)?,
            attempt: r.u64()?,
        },
        8 => bucket::Command::Lock {
            bucket: record::take_str(r)?,
            incarnation: r.u64()?,
            default: take_default(r)?,
        },
        9 => bucket::Command::Progress {
            bucket: record::take_str(r)?,
            attempt: r.u64()?,
            at_ns,
        },
        _ => return None,
    })
}

fn put_name(w: &mut Writer, c: &name::Command) -> Result<(), RecordError> {
    match c {
        name::Command::Put(p) => {
            w.u8(0);
            put_target(w, &p.bucket, p.incarnation, &p.key)?;
            w.u8(p.versioning.code());
            put_preconditions(w, &p.preconditions)?;
            put_option(w, p.ordered_ns);
            record::put_bytes(w, &p.version.encode()?)?;
            put_default(w, p.default);
            w.u64(p.deadline_ns);
        }
        name::Command::Delete(d) => {
            w.u8(1);
            put_target(w, &d.bucket, d.incarnation, &d.key)?;
            w.u8(d.versioning.code());
            put_named(w, d.named);
            put_match(w, d.if_match.as_ref())?;
            w.u8(u8::from(d.bypass));
        }
        name::Command::CreateUpload(c) => {
            w.u8(2);
            put_target(w, &c.bucket, c.incarnation, &c.key)?;
            record::put_bytes(w, &c.upload.encode()?)?;
        }
        name::Command::PutPart(p) => {
            w.u8(3);
            put_target(w, &p.bucket, p.incarnation, &p.key)?;
            record::put_str(w, &p.upload)?;
            w.u16(p.number);
            record::put_bytes(w, &p.part.encode()?)?;
            w.u64(p.deadline_ns);
        }
        name::Command::Complete(c) => {
            w.u8(4);
            put_target(w, &c.bucket, c.incarnation, &c.key)?;
            record::put_str(w, &c.upload)?;
            w.u8(c.versioning.code());
            put_preconditions(w, &c.preconditions)?;
            record::put_len(w, c.parts.len())?;
            for part in &c.parts {
                w.u16(part.number);
                record::put_str(w, &part.etag)?;
                w.u128(part.file);
            }
            record::put_str(w, &c.etag)?;
            w.u64(c.size);
            match &c.checksum {
                None => w.u8(0),
                Some(sum) => {
                    w.u8(1);
                    w.u8(sum.algorithm);
                    w.u16(sum.parts);
                    record::put_bytes(w, &sum.value)?;
                }
            }
            record::put_file(w, c.file);
            put_default(w, c.default);
            w.u64(c.deadline_ns);
        }
        name::Command::Abort(a) => {
            w.u8(5);
            put_target(w, &a.bucket, a.incarnation, &a.key)?;
            record::put_str(w, &a.upload)?;
        }
        name::Command::Retain(c) => {
            w.u8(8);
            put_target(w, &c.bucket, c.incarnation, &c.key)?;
            put_named(w, c.named);
            match c.retention {
                None => w.u8(0),
                Some(retention) => {
                    w.u8(1);
                    record::put_retention(w, Some(retention));
                }
            }
            w.u8(u8::from(c.bypass));
        }
        name::Command::Hold(c) => {
            w.u8(9);
            put_target(w, &c.bucket, c.incarnation, &c.key)?;
            put_named(w, c.named);
            w.u8(u8::from(c.on));
        }
        name::Command::Gate(g) => {
            w.u8(6);
            record::put_str(w, &g.bucket)?;
            w.u64(g.incarnation);
            w.u64(g.attempt);
            w.u8(gate_code(g.from));
            w.u8(gate_code(g.to));
        }
        name::Command::Collect(c) => {
            w.u8(7);
            record::put_str(w, &c.bucket)?;
            w.u64(c.incarnation);
            w.u32(c.budget);
        }
        name::Command::Reclaim(c) => {
            w.u8(10);
            w.u64(c.released_ns);
            w.u128(c.file);
        }
        name::Command::Check(c) => {
            w.u8(11);
            record::put_len(w, c.files.len())?;
            for &(file, deadline_ns) in &c.files {
                w.u128(file);
                w.u64(deadline_ns);
            }
        }
        name::Command::Unmark(u) => {
            w.u8(12);
            record::put_len(w, u.files.len())?;
            for &file in &u.files {
                w.u128(file);
            }
        }
    }
    Ok(())
}

fn take_name(r: &mut Reader<'_>, at_ns: u64) -> Option<name::Command> {
    Some(match r.u8()? {
        0 => {
            let (bucket, incarnation, key) = take_target(r)?;
            name::Command::Put(name::Put {
                bucket,
                incarnation,
                key,
                versioning: Versioning::from_code(r.u8()?)?,
                preconditions: take_preconditions(r)?,
                at_ns,
                ordered_ns: take_option(r)?,
                version: Version::decode(&record::take_bytes(r)?).ok()?,
                default: take_default(r)?,
                deadline_ns: r.u64()?,
            })
        }
        1 => {
            let (bucket, incarnation, key) = take_target(r)?;
            let versioning = Versioning::from_code(r.u8()?)?;
            let named = take_named(r)?;
            name::Command::Delete(name::Delete {
                bucket,
                incarnation,
                key,
                versioning,
                named,
                if_match: take_match(r)?,
                at_ns,
                bypass: take_bool(r)?,
            })
        }
        2 => {
            let (bucket, incarnation, key) = take_target(r)?;
            name::Command::CreateUpload(name::CreateUpload {
                bucket,
                incarnation,
                key,
                at_ns,
                upload: Upload::decode(&record::take_bytes(r)?).ok()?,
            })
        }
        3 => {
            let (bucket, incarnation, key) = take_target(r)?;
            name::Command::PutPart(name::PutPart {
                bucket,
                incarnation,
                key,
                upload: record::take_str(r)?,
                number: r.u16()?,
                part: Part::decode(&record::take_bytes(r)?).ok()?,
                at_ns,
                deadline_ns: r.u64()?,
            })
        }
        4 => {
            let (bucket, incarnation, key) = take_target(r)?;
            let upload = record::take_str(r)?;
            let versioning = Versioning::from_code(r.u8()?)?;
            let preconditions = take_preconditions(r)?;
            // A listed part takes 22 bytes at least.
            let count = bounded(r, 22)?;
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                parts.push(name::Listed {
                    number: r.u16()?,
                    etag: record::take_str(r)?,
                    file: r.u128()?,
                });
            }
            let etag = record::take_str(r)?;
            let size = r.u64()?;
            let checksum = match r.u8()? {
                0 => None,
                1 => Some(Checksum {
                    algorithm: r.u8()?,
                    parts: r.u16()?,
                    value: record::take_bytes(r)?,
                }),
                _ => return None,
            };
            name::Command::Complete(name::Complete {
                bucket,
                incarnation,
                key,
                upload,
                versioning,
                preconditions,
                at_ns,
                parts,
                etag,
                size,
                checksum,
                file: record::take_file(r)?,
                default: take_default(r)?,
                deadline_ns: r.u64()?,
            })
        }
        5 => {
            let (bucket, incarnation, key) = take_target(r)?;
            name::Command::Abort(name::Abort {
                bucket,
                incarnation,
                key,
                upload: record::take_str(r)?,
                at_ns,
            })
        }
        6 => name::Command::Gate(name::GateChange {
            bucket: record::take_str(r)?,
            incarnation: r.u64()?,
            attempt: r.u64()?,
            from: gate_state(r.u8()?)?,
            to: gate_state(r.u8()?)?,
        }),
        7 => name::Command::Collect(name::Collect {
            bucket: record::take_str(r)?,
            incarnation: r.u64()?,
            budget: r.u32()?,
            at_ns,
        }),
        10 => name::Command::Reclaim(name::Reclaim {
            released_ns: r.u64()?,
            file: r.u128()?,
        }),
        11 => {
            let count = bounded(r, 24)?;
            let mut files = Vec::with_capacity(count);
            for _ in 0..count {
                files.push((r.u128()?, r.u64()?));
            }
            name::Command::Check(name::Check { files, at_ns })
        }
        12 => {
            let count = bounded(r, 16)?;
            let mut files = Vec::with_capacity(count);
            for _ in 0..count {
                files.push(r.u128()?);
            }
            name::Command::Unmark(name::Unmark { files })
        }
        8 => {
            let (bucket, incarnation, key) = take_target(r)?;
            let named = take_named(r)?;
            let retention = match r.u8()? {
                0 => None,
                1 => record::take_retention(r, true)?,
                _ => return None,
            };
            name::Command::Retain(name::Retain {
                bucket,
                incarnation,
                key,
                named,
                retention,
                bypass: take_bool(r)?,
                at_ns,
            })
        }
        9 => {
            let (bucket, incarnation, key) = take_target(r)?;
            name::Command::Hold(name::Hold {
                bucket,
                incarnation,
                key,
                named: take_named(r)?,
                on: take_bool(r)?,
            })
        }
        _ => return None,
    })
}

fn put_named(w: &mut Writer, named: Option<name::Named>) {
    match named {
        None => w.u8(0),
        Some(name::Named::Null) => w.u8(1),
        Some(name::Named::Order(order)) => {
            w.u8(2);
            w.u64(order);
        }
    }
}

fn take_named(r: &mut Reader<'_>) -> Option<Option<name::Named>> {
    Some(match r.u8()? {
        0 => None,
        1 => Some(name::Named::Null),
        2 => Some(name::Named::Order(r.u64()?)),
        _ => return None,
    })
}

fn take_bool(r: &mut Reader<'_>) -> Option<bool> {
    match r.u8()? {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

fn put_default(w: &mut Writer, default: Option<DefaultRetention>) {
    match default {
        None => w.u8(0),
        Some(default) => {
            w.u8(1);
            record::put_default(w, default);
        }
    }
}

fn take_default(r: &mut Reader<'_>) -> Option<Option<DefaultRetention>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(record::take_default(r)?)),
        _ => None,
    }
}

fn put_target(
    w: &mut Writer,
    bucket: &str,
    incarnation: u64,
    key: &str,
) -> Result<(), RecordError> {
    record::put_str(w, bucket)?;
    w.u64(incarnation);
    record::put_str(w, key)
}

fn take_target(r: &mut Reader<'_>) -> Option<(String, u64, String)> {
    Some((record::take_str(r)?, r.u64()?, record::take_str(r)?))
}

fn put_option(w: &mut Writer, value: Option<u64>) {
    match value {
        None => w.u8(0),
        Some(v) => {
            w.u8(1);
            w.u64(v);
        }
    }
}

fn take_option(r: &mut Reader<'_>) -> Option<Option<u64>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.u64()?)),
        _ => None,
    }
}

fn put_match(w: &mut Writer, m: Option<&name::Match>) -> Result<(), RecordError> {
    match m {
        None => w.u8(0),
        Some(name::Match::Any) => w.u8(1),
        Some(name::Match::Tags(tags)) => {
            w.u8(2);
            record::put_len(w, tags.len())?;
            for tag in tags {
                record::put_str(w, tag)?;
            }
        }
    }
    Ok(())
}

fn take_match(r: &mut Reader<'_>) -> Option<Option<name::Match>> {
    Some(match r.u8()? {
        0 => None,
        1 => Some(name::Match::Any),
        2 => {
            let count = bounded(r, 4)?;
            let mut tags = Vec::with_capacity(count);
            for _ in 0..count {
                tags.push(record::take_str(r)?);
            }
            Some(name::Match::Tags(tags))
        }
        _ => return None,
    })
}

fn put_preconditions(w: &mut Writer, p: &name::Preconditions) -> Result<(), RecordError> {
    put_match(w, p.if_match.as_ref())?;
    put_match(w, p.if_none_match.as_ref())
}

fn take_preconditions(r: &mut Reader<'_>) -> Option<name::Preconditions> {
    Some(name::Preconditions {
        if_match: take_match(r)?,
        if_none_match: take_match(r)?,
    })
}

fn gate_code(state: Option<GateState>) -> u8 {
    match state {
        None => 0,
        Some(GateState::Open) => 1,
        Some(GateState::Closed) => 2,
        Some(GateState::Condemned) => 3,
    }
}

fn gate_state(code: u8) -> Option<Option<GateState>> {
    match code {
        0 => Some(None),
        1 => Some(Some(GateState::Open)),
        2 => Some(Some(GateState::Closed)),
        3 => Some(Some(GateState::Condemned)),
        _ => None,
    }
}

fn put_bucket_outcome(w: &mut Writer, o: &bucket::Outcome) {
    use bucket::Outcome as O;
    let (code, attempt) = match *o {
        O::Creating {
            incarnation,
            attempt,
        } => (0, Some((incarnation, attempt))),
        O::Deleting {
            incarnation,
            attempt,
        } => (1, Some((incarnation, attempt))),
        O::Activated => (2, None),
        O::Versioned => (3, None),
        O::Restored => (4, None),
        O::Deleted => (5, None),
        O::Forgotten => (6, None),
        O::AlreadyOwnedByYou => (7, None),
        O::AlreadyExists => (8, None),
        O::TooManyBuckets => (9, None),
        O::OperationAborted => (10, None),
        O::NoSuchBucket => (11, None),
        O::Conflict => (12, None),
        O::Invalid => (13, None),
        O::VersioningLocked => (14, None),
        O::VersioningNotEnabled => (15, None),
        O::LockConfigured => (16, None),
        O::Progressed => (17, None),
    };
    w.u8(code);
    if let Some((incarnation, attempt)) = attempt {
        w.u64(incarnation);
        w.u64(attempt);
    }
}

fn take_bucket_outcome(r: &mut Reader<'_>) -> Option<bucket::Outcome> {
    use bucket::Outcome as O;
    Some(match r.u8()? {
        0 => O::Creating {
            incarnation: r.u64()?,
            attempt: r.u64()?,
        },
        1 => O::Deleting {
            incarnation: r.u64()?,
            attempt: r.u64()?,
        },
        2 => O::Activated,
        3 => O::Versioned,
        4 => O::Restored,
        5 => O::Deleted,
        6 => O::Forgotten,
        7 => O::AlreadyOwnedByYou,
        8 => O::AlreadyExists,
        9 => O::TooManyBuckets,
        10 => O::OperationAborted,
        11 => O::NoSuchBucket,
        12 => O::Conflict,
        13 => O::Invalid,
        14 => O::VersioningLocked,
        15 => O::VersioningNotEnabled,
        16 => O::LockConfigured,
        17 => O::Progressed,
        _ => return None,
    })
}

fn put_name_outcome(w: &mut Writer, o: &name::Outcome) -> Result<(), RecordError> {
    use name::Outcome as O;
    match o {
        O::Put { version } => {
            w.u8(0);
            record::put_str(w, version)?;
        }
        O::Deleted { marker, version } => {
            w.u8(1);
            w.u8(u8::from(*marker));
            match version {
                None => w.u8(0),
                Some(v) => {
                    w.u8(1);
                    record::put_str(w, v)?;
                }
            }
        }
        O::Created { upload } => {
            w.u8(2);
            record::put_str(w, upload)?;
        }
        O::Collected { done } => {
            w.u8(3);
            w.u8(u8::from(*done));
        }
        O::PreconditionFailed => w.u8(4),
        O::NoSuchKey => w.u8(5),
        O::PartWritten => w.u8(6),
        O::Aborted => w.u8(7),
        O::NoSuchUpload => w.u8(8),
        O::InvalidPart => w.u8(9),
        O::InvalidPartOrder => w.u8(10),
        O::EntityTooSmall => w.u8(11),
        O::Stale => w.u8(12),
        O::NoSuchBucket => w.u8(13),
        O::GateMoved => w.u8(14),
        O::Conflict => w.u8(15),
        O::Invalid => w.u8(16),
        O::NotEmpty => w.u8(17),
        O::Retained => w.u8(18),
        O::Held => w.u8(19),
        O::Locked => w.u8(20),
        O::NoSuchVersion => w.u8(21),
        O::DeleteMarker => w.u8(22),
        O::Reclaimed => w.u8(23),
        O::Expired => w.u8(24),
        O::Checked(verdicts) => {
            w.u8(25);
            put_verdicts(w, verdicts)?;
        }
        O::Unmarked => w.u8(26),
    }
    Ok(())
}

fn put_verdicts(w: &mut Writer, verdicts: &[Verdict]) -> Result<(), RecordError> {
    record::put_len(w, verdicts.len())?;
    for v in verdicts {
        w.u8(match v {
            Verdict::Held => 0,
            Verdict::Released => 1,
            Verdict::Young => 2,
        });
    }
    Ok(())
}

fn take_verdicts(r: &mut Reader<'_>) -> Option<Vec<Verdict>> {
    let count = bounded(r, 1)?;
    let mut verdicts = Vec::with_capacity(count);
    for _ in 0..count {
        verdicts.push(match r.u8()? {
            0 => Verdict::Held,
            1 => Verdict::Released,
            2 => Verdict::Young,
            _ => return None,
        });
    }
    Some(verdicts)
}

fn take_name_outcome(r: &mut Reader<'_>) -> Option<name::Outcome> {
    use name::Outcome as O;
    Some(match r.u8()? {
        0 => O::Put {
            version: record::take_str(r)?,
        },
        1 => {
            let marker = match r.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            };
            let version = match r.u8()? {
                0 => None,
                1 => Some(record::take_str(r)?),
                _ => return None,
            };
            O::Deleted { marker, version }
        }
        2 => O::Created {
            upload: record::take_str(r)?,
        },
        3 => O::Collected {
            done: match r.u8()? {
                0 => false,
                1 => true,
                _ => return None,
            },
        },
        4 => O::PreconditionFailed,
        5 => O::NoSuchKey,
        6 => O::PartWritten,
        7 => O::Aborted,
        8 => O::NoSuchUpload,
        9 => O::InvalidPart,
        10 => O::InvalidPartOrder,
        11 => O::EntityTooSmall,
        12 => O::Stale,
        13 => O::NoSuchBucket,
        14 => O::GateMoved,
        15 => O::Conflict,
        16 => O::Invalid,
        17 => O::NotEmpty,
        18 => O::Retained,
        19 => O::Held,
        20 => O::Locked,
        21 => O::NoSuchVersion,
        22 => O::DeleteMarker,
        23 => O::Reclaimed,
        24 => O::Expired,
        25 => O::Checked(take_verdicts(r)?),
        26 => O::Unmarked,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Period, Retention, RetentionMode, Target, Upload};
    use mantle_chunk::ChunkKey;
    use proptest::prelude::*;

    fn version() -> Version {
        Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: "e".into(),
            size: 3,
            checksum: Some(Checksum {
                algorithm: 1,
                parts: 0,
                value: vec![1, 2, 3, 4],
            }),
            file: Some(9),
            owner: "o".into(),
            headers: vec![("content-type".into(), "a/b".into())],
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 1_893_456_000_000,
            }),
            legal_hold: Some(false),
        }
    }

    fn named(c: name::Command) -> Command {
        Command::Name(Box::new(c))
    }

    /// One command of every kind, each with the time the entry gives it.
    fn commands(at_ns: u64) -> Vec<Command> {
        let preconditions = name::Preconditions {
            if_match: Some(name::Match::Tags(vec!["a".into(), "b".into()])),
            if_none_match: Some(name::Match::Any),
        };
        let chunk = ChunkKey {
            block: 5,
            epoch: 1,
            index: 0,
        };
        vec![
            Command::Register,
            Command::Bucket(bucket::Command::Create(bucket::Create {
                bucket: "b".into(),
                owner: "o".into(),
                location: "eu".into(),
                at_ns,
                quota: 10,
                lock: true,
            })),
            Command::Bucket(bucket::Command::Activate {
                bucket: "b".into(),
                attempt: 3,
            }),
            Command::Bucket(bucket::Command::Version {
                bucket: "b".into(),
                incarnation: 2,
                versioning: Versioning::Suspended,
            }),
            Command::Bucket(bucket::Command::BeginDelete {
                bucket: "b".into(),
                at_ns,
            }),
            Command::Bucket(bucket::Command::Abandon {
                bucket: "b".into(),
                at_ns,
            }),
            Command::Bucket(bucket::Command::Restore {
                bucket: "b".into(),
                attempt: 4,
            }),
            Command::Bucket(bucket::Command::Delete {
                bucket: "b".into(),
                attempt: 5,
            }),
            Command::Bucket(bucket::Command::Forget {
                bucket: "b".into(),
                attempt: 6,
            }),
            Command::Bucket(bucket::Command::Lock {
                bucket: "b".into(),
                incarnation: 2,
                default: Some(DefaultRetention {
                    mode: RetentionMode::Compliance,
                    period: Period::Years(3),
                }),
            }),
            Command::Bucket(bucket::Command::Lock {
                bucket: "b".into(),
                incarnation: 2,
                default: None,
            }),
            Command::Bucket(bucket::Command::Progress {
                bucket: "b".into(),
                attempt: 7,
                at_ns,
            }),
            named(name::Command::Retain(name::Retain {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                named: Some(name::Named::Null),
                retention: Some(Retention {
                    mode: RetentionMode::Governance,
                    until_ms: 1_893_456_000_000,
                }),
                bypass: true,
                at_ns,
            })),
            named(name::Command::Retain(name::Retain {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                named: None,
                retention: None,
                bypass: false,
                at_ns,
            })),
            named(name::Command::Hold(name::Hold {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                named: Some(name::Named::Order(3)),
                on: true,
            })),
            named(name::Command::Put(name::Put {
                bucket: "b".into(),
                incarnation: 1,
                key: "k\0é".into(),
                versioning: Versioning::Enabled,
                preconditions: preconditions.clone(),
                at_ns,
                ordered_ns: Some(7),
                version: version(),
                default: Some(DefaultRetention {
                    mode: RetentionMode::Governance,
                    period: Period::Days(30),
                }),
                deadline_ns: u64::MAX,
            })),
            named(name::Command::Delete(name::Delete {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                versioning: Versioning::Unversioned,
                named: Some(name::Named::Order(9)),
                if_match: None,
                at_ns,
                bypass: true,
            })),
            named(name::Command::CreateUpload(name::CreateUpload {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                at_ns,
                upload: Upload {
                    initiated_ns: 0,
                    owner: "o".into(),
                    headers: Vec::new(),
                    checksum: Some((2, true)),
                    retention: Some(Retention {
                        mode: RetentionMode::Governance,
                        until_ms: 1,
                    }),
                    legal_hold: Some(true),
                },
            })),
            named(name::Command::PutPart(name::PutPart {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: "u".into(),
                number: 3,
                part: Part {
                    etag: "p".into(),
                    size: 5 << 20,
                    checksum: None,
                    file: 4,
                    modified_ns: 0,
                },
                at_ns,
                deadline_ns: u64::MAX,
            })),
            named(name::Command::Complete(name::Complete {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: "u".into(),
                versioning: Versioning::Enabled,
                preconditions,
                at_ns,
                parts: vec![name::Listed {
                    number: 1,
                    etag: "p".into(),
                    file: 4,
                }],
                etag: "x-1".into(),
                size: 10,
                checksum: None,
                file: Some(8),
                default: Some(DefaultRetention {
                    mode: RetentionMode::Compliance,
                    period: Period::Days(1),
                }),
                deadline_ns: u64::MAX,
            })),
            named(name::Command::Abort(name::Abort {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
                upload: "u".into(),
                at_ns,
            })),
            named(name::Command::Gate(name::GateChange {
                bucket: "b".into(),
                incarnation: 1,
                attempt: 2,
                from: Some(GateState::Closed),
                to: None,
            })),
            named(name::Command::Collect(name::Collect {
                bucket: "b".into(),
                incarnation: 1,
                budget: 64,
                at_ns,
            })),
            named(name::Command::Reclaim(name::Reclaim {
                released_ns: 5,
                file: u128::MAX,
            })),
            named(name::Command::Check(name::Check {
                files: vec![(1, 2), (u128::MAX, u64::MAX)],
                at_ns,
            })),
            named(name::Command::Unmark(name::Unmark {
                files: vec![1, u128::MAX],
            })),
            Command::File(file::Command::Write {
                file: 3,
                extents: vec![Extent {
                    length: 7,
                    target: Target::File(1),
                }],
                referrer: Referrer {
                    bucket: "b".into(),
                    incarnation: 2,
                    key: "k".into(),
                },
                handover_ns: 60,
                at_ns,
                blocks_deadline_ns: u64::MAX,
            }),
            Command::File(file::Command::Delete { file: 3 }),
            Command::File(file::Command::Settle {
                files: vec![3, u128::MAX],
            }),
            Command::File(file::Command::CheckBlocks {
                file: 3,
                blocks: vec![(5, 6), (u128::MAX, u64::MAX)],
                at_ns,
            }),
            Command::Block(block::Command::Settle {
                blocks: vec![1, u128::MAX],
            }),
            Command::Block(block::Command::Write {
                block: 5,
                header: BlockHeader {
                    length: 1,
                    data: 1,
                    parity: 0,
                    chunk_len: 1,
                    crc32c: 2,
                },
                chunks: vec![ChunkPlace {
                    volume: 1,
                    key: chunk,
                }],
                at_ns,
                file: 1,
                handover_ns: u64::MAX / 2,
            }),
            Command::Block(block::Command::Move {
                chunk,
                from: 1,
                to: 2,
            }),
            Command::Block(block::Command::Delete { block: 5 }),
        ]
    }

    #[test]
    fn every_command_round_trips_and_takes_the_entry_time() {
        let at_ns = 1_700_000_000_000_000_000;
        let entry = Entry {
            at_ns,
            commands: commands(at_ns)
                .into_iter()
                .enumerate()
                .map(|(i, command)| Sessioned {
                    session: i as u64,
                    serial: 2 * i as u64,
                    unanswered: 1,
                    command,
                })
                .collect(),
        };
        let bytes = entry.encode().unwrap();
        assert_eq!(Entry::decode(&bytes).unwrap(), entry);
        // Every truncation is refused.
        for len in 0..bytes.len() {
            assert!(Entry::decode(&bytes[..len]).is_err(), "{len}");
        }
    }

    #[test]
    fn every_answer_round_trips() {
        use bucket::Outcome as B;
        use name::Outcome as N;
        let mut answers = vec![
            Answer::Registered { session: 7 },
            Answer::SessionExpired,
            Answer::WrongLayer,
            Answer::Repeated,
        ];
        answers.extend(
            [
                B::Creating {
                    incarnation: 1,
                    attempt: 2,
                },
                B::Deleting {
                    incarnation: 1,
                    attempt: 3,
                },
                B::Activated,
                B::Versioned,
                B::Restored,
                B::Deleted,
                B::Forgotten,
                B::AlreadyOwnedByYou,
                B::AlreadyExists,
                B::TooManyBuckets,
                B::OperationAborted,
                B::NoSuchBucket,
                B::Conflict,
                B::Invalid,
                B::VersioningLocked,
                B::VersioningNotEnabled,
                B::LockConfigured,
                B::Progressed,
            ]
            .map(Answer::Bucket),
        );
        answers.extend(
            [
                N::Put {
                    version: "v".into(),
                },
                N::Deleted {
                    marker: true,
                    version: Some("v".into()),
                },
                N::Deleted {
                    marker: false,
                    version: None,
                },
                N::Created { upload: "u".into() },
                N::Collected { done: true },
                N::PreconditionFailed,
                N::NoSuchKey,
                N::PartWritten,
                N::Aborted,
                N::NoSuchUpload,
                N::InvalidPart,
                N::InvalidPartOrder,
                N::EntityTooSmall,
                N::Stale,
                N::NoSuchBucket,
                N::GateMoved,
                N::Conflict,
                N::Invalid,
                N::NotEmpty,
                N::Retained,
                N::Held,
                N::Locked,
                N::NoSuchVersion,
                N::DeleteMarker,
                N::Reclaimed,
                N::Expired,
                N::Checked(vec![
                    name::Verdict::Held,
                    name::Verdict::Released,
                    name::Verdict::Young,
                ]),
                N::Checked(Vec::new()),
                N::Unmarked,
            ]
            .map(Answer::Name),
        );
        answers.extend(
            [
                file::Outcome::Written { deadline_ns: 9 },
                file::Outcome::Deleted,
                file::Outcome::Conflict,
                file::Outcome::Invalid,
                file::Outcome::Settled,
                file::Outcome::Expired,
                file::Outcome::BlocksChecked(vec![Verdict::Held, Verdict::Young]),
            ]
            .map(Answer::File),
        );
        answers.extend(
            [
                block::Outcome::Written { deadline_ns: 3 },
                block::Outcome::Settled,
                block::Outcome::Moved,
                block::Outcome::Deleted,
                block::Outcome::Conflict,
                block::Outcome::Invalid,
                block::Outcome::NoSuchBlock,
            ]
            .map(Answer::Block),
        );
        for answer in answers {
            let bytes = answer.encode().unwrap();
            assert_eq!(Answer::decode(&bytes).unwrap(), answer);
        }
    }

    proptest! {
        /// Whatever arrives decodes to an entry or to an error, never to a panic, and what
        /// decodes encodes back to the same bytes.
        #[test]
        fn any_bytes_decode_safely(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
            if let Ok(entry) = Entry::decode(&bytes) {
                prop_assert_eq!(entry.encode().unwrap(), bytes.clone());
            }
            if let Ok(answer) = Answer::decode(&bytes) {
                prop_assert_eq!(answer.encode().unwrap(), bytes);
            }
        }
    }
}
