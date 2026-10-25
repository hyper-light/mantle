//! A read a range answers by ReadIndex (docs/design/node.md §5.1; replica.md §3): its leader has a
//! quorum confirm that it still leads, waits until it has applied through the index confirmed,
//! then reads its engine. The read sees every write committed before it was asked, so a history of
//! reads and writes through the range's leaders is linearizable.
//!
//! On the request class, as a command is: the request's head names the range's group
//! (`crate::command::head`) and its body is the query; the reply's head is a command's
//! (`crate::command::reply_head`) and, answered, its body is the answer.
//!
//! | Query | Body | Answer |
//! |---|---|---|
//! | the current version of a key, of a Name range | kind 0, the bucket's and the key's lengths (u32 little-endian) each before its bytes | 0 for none; 1, the version's order (u64 little-endian), then the version (`mantle_meta::record::Version`); 2 when the bucket has no gate here, or a condemned one |
//! | a page of the volume table, of the root range | kind 1; 0 from the first volume, or 1 and the volume after which the page starts (u128) | 1 if more volumes follow the page, else 0; the count (u32); each volume's id (u128), node (u64) and bytes (u64), then its node's domain, its length (u32) before its bytes |
//! | a file's header, of a File range | kind 2; the file (u128) | 0 for none; 1, then the header (`mantle_meta::record::FileHeader`) |
//! | a file's extents from the one holding an offset, of a File range | kind 3; the file (u128), the offset (u64), the most extents (u32) | the count (u32); each extent's first byte's offset (u64), then the extent (`Extent`), its length (u32) before it |
//! | a block's header and its chunks' places, of a Block range | kind 4; the block (u128) | 0 for none; 1, the header (`BlockHeader`) and the count of places (u32), each record its length (u32) before it |
//! | a bucket's row, of the Bucket range | kind 5; the bucket's name, its length (u32) before it | 0 for none; 1, then the row (`Bucket`) |
//! | a bucket's gate, of a Name range routed by a descriptor's generation | kind 6; the bucket's name, its length (u32) before it; the generation (u64) | 0 for no gate; 1, then the gate (`Gate`); 2, then the range's lineage (`Lineage`), where its span went |
//! | a page of ListObjectsV2, of a Name range | kind 7; the listing's request as an object's request carries it (`crate::object`) | 0 when the bucket has no gate here, or a condemned one; 1, then the page as an object's reply carries it |
//! | a page of ListBuckets, of the Bucket range | kind 8; the owner, the prefix, the bucket after which the page starts (optional: a byte, 0 or 1, before it) and the most buckets (u32) | the page as an object's reply carries it |
//! | whether a bucket's rows remain, of a Name range routed by a descriptor's generation | kind 9; the bucket; the generation (u64); where the read starts (optional, a key behind its length); the most rows it reads (u32) | 0, a version or delete marker is there; 1, none; 2, then the key the next read starts at, behind its length; 3, then the range's lineage |
//!
//! A read is answered only by a range of the layer whose rows it reads (`Query::layer`). A page
//! holds as many volumes as a reply carries, each counted at the longest domain, so it fits
//! whatever their domains. Extents are read as many as asked, the asker's window's worth (gateway.md
//! §3): a read of more than a reply carries is refused whole, since an answer cut short reads as
//! the file's end. A page of keys or buckets ends early, truncated, at the first item a reply has
//! no room for, which S3 allows: "The response might contain fewer keys" (05 §6.2); a reply too
//! small for the longest item is refused, so a listing always moves.
//!
//! Which reads a range answers grows with the request path (node.md §5.1: headers, extents, blocks,
//! uploads and parts); a read never changes what the range holds.

use mantle_codec::{Reader, Writer};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Rows;
use mantle_meta::error::MetaError;
use mantle_meta::name::{self, Probe, Routed, Scan};
use mantle_meta::record::{
    BlockHeader, Bucket, ChunkPlace, Extent, FileHeader, Gate, GateState, Lineage, Version,
};
use mantle_meta::root::{self, MAX_DOMAIN, Placed};
use mantle_meta::wire::{MAX_BUCKET, MAX_KEY, MAX_OWNER};
use mantle_meta::{block, bucket, file};
use mantle_s3::checksum::Algorithm;
use mantle_s3::list::{self, Paging, Resume, Step};

use crate::object::{self, Buckets, List, Listed, Page};

/// The query's kind: the current version of a key, or a page of the volume table.
const CURRENT: u8 = 0;
const VOLUMES: u8 = 1;
const HEADER: u8 = 2;
const EXTENTS: u8 = 3;
const BLOCK: u8 = 4;
const BUCKET: u8 = 5;
const GATE: u8 = 6;
const LIST: u8 = 7;
const OWNED: u8 = 8;
const PROBE: u8 = 9;
/// An extent's bytes in an answer, at most: its first byte's offset, its record's length and the
/// record (`Extent::encode`: the format, the length, the target's kind and id, the CRC-32C).
const EXTENT_BYTES: usize = 8 + 4 + (1 + 8 + 1 + 16 + 4);
/// A page's bytes before its volumes: whether more follow, and the count.
const PAGE_HEAD_BYTES: usize = 1 + 4;
/// A volume's bytes in a page at the longest domain: its id, node and bytes, and its domain behind
/// its length.
const PLACED_BYTES: usize = 16 + 8 + 8 + 4 + MAX_DOMAIN;
/// A page of buckets' bytes beside its buckets, at most: the count, and the bucket after which
/// the next page starts behind its flag and length.
const BUCKETS_HEAD_BYTES: usize = 4 + 1 + 4 + MAX_BUCKET;
/// A bucket's bytes in a page at the longest name: its name behind its length, and its creation.
const OWNED_BYTES: usize = 4 + MAX_BUCKET + 8;
/// Where a probe resumes at its longest, past an object's rows (`mantle_meta::name::probe`): the
/// data space's byte, the bucket and the key each escaped, every byte at most doubled, and ended
/// (`mantle_meta::key`), and the byte past every row of the object.
const MAX_POSITION: usize = 1 + 2 * MAX_BUCKET + 1 + 2 * MAX_KEY + 1 + 1;

/// What a read asks of a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// The current version of `key` in `bucket`: what a GET or HEAD of it reads first.
    Current { bucket: String, key: String },
    /// The volume table's volumes after `after`, or from the first (node.md §6.6).
    Volumes { after: Option<u128> },
    /// A file's header: what a GET reads of its object's file first (gateway.md §3).
    Header { file: u128 },
    /// A file's extents from the one holding byte `offset`, at most `max`, in order.
    Extents { file: u128, offset: u64, max: u32 },
    /// A block's header and where each of its chunks lives.
    Block { block: u128 },
    /// A bucket's row: its owner, incarnation, versioning and state (metadata.md §1).
    Bucket { bucket: String },
    /// A bucket's gate in a Name range, routed by the generation of the range's descriptor the
    /// asker holds (the bucket's coordinator's read, `mantle_meta::coordinator`).
    Gate { bucket: String, generation: u64 },
    /// A page of ListObjectsV2 over a bucket's keys in a Name range (s3-protocol.md §3).
    List(List),
    /// A page of ListBuckets: `owner`'s buckets whose names begin with `prefix`, after `after`,
    /// at most `max` (05 §10.4).
    Owned {
        owner: String,
        prefix: String,
        after: Option<String>,
        max: u32,
    },
    /// Whether a Name range routed by `generation` holds a version or delete marker of
    /// `bucket`, reading at most `budget` rows from `from` (DeleteBucket's coordinator's read,
    /// `mantle_meta::coordinator`).
    Probe {
        bucket: String,
        generation: u64,
        from: Option<Vec<u8>>,
        budget: u32,
    },
}

/// What a range answered a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// The key's current version and its order, or none.
    Current(Option<(u64, Version)>),
    /// A page of the volume table, and whether more volumes follow it.
    Volumes { volumes: Vec<Placed>, more: bool },
    /// The file's header, or none.
    Header(Option<FileHeader>),
    /// The extents asked, each with its first byte's offset; fewer past the file's end.
    Extents(Vec<(u64, Extent)>),
    /// The block's header and its chunks' places, or none.
    Block(Option<(BlockHeader, Vec<ChunkPlace>)>),
    /// The bucket's row, or none.
    Bucket(Option<Bucket>),
    /// The bucket's gate, or none; or where the range's span went.
    Gate(Routed<Option<Gate>>),
    /// The bucket has no gate in the range, or a condemned one: S3's `NoSuchBucket`.
    NoBucket,
    /// A page of the bucket's keys.
    Keys(Page),
    /// A page of the owner's buckets.
    Owned(Buckets),
    /// Whether the bucket's rows remain; or where the range's span went.
    Probe(Routed<Probe>),
}

/// Why a read's message was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    #[error("a read's query does not decode")]
    Query,
    #[error("a read's answer does not decode")]
    Answer,
    #[error("the version: {0}")]
    Record(#[from] mantle_meta::record::RecordError),
    #[error("the range's rows: {0}")]
    Meta(String),
    #[error("a reply of {0} bytes holds no page of volumes, or not the extents asked")]
    Page(usize),
}

fn put_text(w: &mut Writer, text: &str) {
    // Each name is within S3's bounds, which a u32 holds; `decode` refuses one past them.
    w.u32(u32::try_from(text.len()).unwrap_or(u32::MAX));
    w.bytes(text.as_bytes());
}

/// A record behind its length, a u32.
fn put_record(w: &mut Writer, record: &[u8]) -> Result<(), ReadError> {
    w.u32(u32::try_from(record.len()).map_err(|_| ReadError::Answer)?);
    w.bytes(record);
    Ok(())
}

/// A record behind its length, no longer than the rest holds.
fn take_record<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], ReadError> {
    let len = usize::try_from(r.u32().ok_or(ReadError::Answer)?).map_err(|_| ReadError::Answer)?;
    r.take(len).ok_or(ReadError::Answer)
}

fn take_text(r: &mut Reader<'_>, most: usize) -> Option<String> {
    let len = usize::try_from(r.u32()?).ok()?;
    if len > most {
        return None;
    }
    String::from_utf8(r.take(len)?.to_vec()).ok()
}

impl Query {
    pub fn encode(&self) -> Result<Vec<u8>, ReadError> {
        let mut w = Writer::default();
        match self {
            Self::Current { bucket, key } => {
                w.u8(CURRENT);
                put_text(&mut w, bucket);
                put_text(&mut w, key);
            }
            Self::Volumes { after } => {
                w.u8(VOLUMES);
                match after {
                    None => w.u8(0),
                    Some(after) => {
                        w.u8(1);
                        w.u128(*after);
                    }
                }
            }
            Self::Header { file } => {
                w.u8(HEADER);
                w.u128(*file);
            }
            Self::Extents { file, offset, max } => {
                w.u8(EXTENTS);
                w.u128(*file);
                w.u64(*offset);
                w.u32(*max);
            }
            Self::Block { block } => {
                w.u8(BLOCK);
                w.u128(*block);
            }
            Self::Bucket { bucket } => {
                w.u8(BUCKET);
                put_text(&mut w, bucket);
            }
            Self::Gate { bucket, generation } => {
                w.u8(GATE);
                put_text(&mut w, bucket);
                w.u64(*generation);
            }
            Self::List(list) => {
                w.u8(LIST);
                object::put_list(&mut w, list).map_err(|_| ReadError::Query)?;
            }
            Self::Owned {
                owner,
                prefix,
                after,
                max,
            } => {
                w.u8(OWNED);
                put_text(&mut w, owner);
                put_text(&mut w, prefix);
                match after {
                    None => w.u8(0),
                    Some(after) => {
                        w.u8(1);
                        put_text(&mut w, after);
                    }
                }
                w.u32(*max);
            }
            Self::Probe {
                bucket,
                generation,
                from,
                budget,
            } => {
                w.u8(PROBE);
                put_text(&mut w, bucket);
                w.u64(*generation);
                match from {
                    None => w.u8(0),
                    Some(from) => {
                        w.u8(1);
                        put_record(&mut w, from)?;
                    }
                }
                w.u32(*budget);
            }
        }
        Ok(w.into_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReadError> {
        let mut r = Reader::new(bytes);
        let query = (|| match r.u8()? {
            CURRENT => Some(Self::Current {
                bucket: take_text(&mut r, MAX_BUCKET)?,
                key: take_text(&mut r, MAX_KEY)?,
            }),
            VOLUMES => Some(Self::Volumes {
                after: match r.u8()? {
                    0 => None,
                    1 => Some(r.u128()?),
                    _ => return None,
                },
            }),
            HEADER => Some(Self::Header { file: r.u128()? }),
            EXTENTS => Some(Self::Extents {
                file: r.u128()?,
                offset: r.u64()?,
                max: r.u32()?,
            }),
            BLOCK => Some(Self::Block { block: r.u128()? }),
            BUCKET => Some(Self::Bucket {
                bucket: take_text(&mut r, MAX_BUCKET)?,
            }),
            GATE => Some(Self::Gate {
                bucket: take_text(&mut r, MAX_BUCKET)?,
                generation: r.u64()?,
            }),
            LIST => Some(Self::List(object::take_list(&mut r)?)),
            OWNED => Some(Self::Owned {
                owner: take_text(&mut r, MAX_OWNER)?,
                prefix: take_text(&mut r, MAX_BUCKET)?,
                after: match r.u8()? {
                    0 => None,
                    1 => Some(take_text(&mut r, MAX_BUCKET)?),
                    _ => return None,
                },
                max: r.u32().filter(|m| {
                    usize::try_from(*m).is_ok_and(|m| (1..=object::MAX_BUCKETS).contains(&m))
                })?,
            }),
            PROBE => Some(Self::Probe {
                bucket: take_text(&mut r, MAX_BUCKET)?,
                generation: r.u64()?,
                from: match r.u8()? {
                    0 => None,
                    1 => {
                        let len = usize::try_from(r.u32()?).ok()?;
                        if len > MAX_POSITION {
                            return None;
                        }
                        Some(r.take(len)?.to_vec())
                    }
                    _ => return None,
                },
                budget: r.u32()?,
            }),
            _ => None,
        })();
        query.filter(|_| r.remaining() == 0).ok_or(ReadError::Query)
    }

    /// The layer whose rows the query reads: only a range of it answers.
    pub fn layer(&self) -> Layer {
        match self {
            Self::Current { .. } => Layer::Name,
            Self::Volumes { .. } => Layer::Root,
            Self::Header { .. } | Self::Extents { .. } => Layer::File,
            Self::Block { .. } => Layer::Block,
            Self::Bucket { .. } | Self::Owned { .. } => Layer::Bucket,
            Self::Gate { .. } | Self::List(_) | Self::Probe { .. } => Layer::Name,
        }
    }

    /// The answer `engine` gives the query now, in at most `max_bytes` once encoded.
    pub fn answer<E: Rows>(&self, engine: &mut E, max_bytes: usize) -> Result<Read, ReadError> {
        let meta = |e: mantle_meta::error::MetaError| ReadError::Meta(e.to_string());
        match self {
            Self::Current { bucket, key } => {
                if !gated(engine, bucket).map_err(meta)? {
                    return Ok(Read::NoBucket);
                }
                name::current(engine, bucket, key)
                    .map(Read::Current)
                    .map_err(meta)
            }
            Self::Volumes { after } => {
                let fits = max_bytes.saturating_sub(PAGE_HEAD_BYTES) / PLACED_BYTES;
                if fits == 0 {
                    return Err(ReadError::Page(max_bytes));
                }
                // One more than the page holds, read only to learn whether more follow.
                let mut volumes =
                    root::volumes(engine, *after, fits.saturating_add(1)).map_err(meta)?;
                let more = volumes.len() > fits;
                volumes.truncate(fits);
                Ok(Read::Volumes { volumes, more })
            }
            Self::Header { file } => file::header(engine, *file).map(Read::Header).map_err(meta),
            Self::Extents { file, offset, max } => {
                let max = usize::try_from(*max).map_err(|_| ReadError::Page(max_bytes))?;
                let needed = max
                    .checked_mul(EXTENT_BYTES)
                    .and_then(|b| b.checked_add(4))
                    .ok_or(ReadError::Page(max_bytes))?;
                if needed > max_bytes {
                    return Err(ReadError::Page(max_bytes));
                }
                file::extents(engine, *file, *offset, max)
                    .map(Read::Extents)
                    .map_err(meta)
            }
            Self::Block { block } => block::read(engine, *block).map(Read::Block).map_err(meta),
            Self::Bucket { bucket } => bucket::read(engine, bucket).map(Read::Bucket).map_err(meta),
            Self::Gate { bucket, generation } => name::read_gate(engine, bucket, *generation)
                .map(Read::Gate)
                .map_err(meta),
            Self::List(list) => {
                if !gated(engine, &list.bucket).map_err(meta)? {
                    return Ok(Read::NoBucket);
                }
                // The reply's room past the page's own bytes; the longest key must fit, so a
                // page lists at least one item.
                let room = max_bytes
                    .checked_sub(1 + object::PAGE_BYTES)
                    .filter(|room| *room >= object::LISTED_BYTES)
                    .ok_or(ReadError::Page(max_bytes))?;
                let mut index = Index {
                    engine,
                    bucket: &list.bucket,
                    owners: list.fetch_owner,
                    room,
                };
                let request = list::Request {
                    prefix: &list.prefix,
                    delimiter: Some(list.delimiter.as_str()),
                    start: list.start.as_ref().map(Resume::start),
                    max_keys: usize::try_from(list.max_keys).unwrap_or(usize::MAX),
                    paging: Paging::Token,
                };
                let page = list::page(&request, &mut index).map_err(meta)?;
                Ok(Read::Keys(Page {
                    contents: page
                        .contents
                        .into_iter()
                        .map(|(key, facts)| facts.listed(key))
                        .collect(),
                    common_prefixes: page.common_prefixes,
                    truncated: page.truncated,
                    next: page.next,
                }))
            }
            Self::Owned {
                owner,
                prefix,
                after,
                max,
            } => {
                let fits = max_bytes.saturating_sub(BUCKETS_HEAD_BYTES) / OWNED_BYTES;
                let most = usize::try_from(*max).unwrap_or(usize::MAX).min(fits);
                if most == 0 {
                    return Err(ReadError::Page(max_bytes));
                }
                // One more than the page holds, read only to learn whether more follow.
                let mut owned = bucket::owned(
                    engine,
                    owner,
                    prefix,
                    after.as_deref(),
                    most.saturating_add(1),
                )
                .map_err(meta)?;
                let more = owned.len() > most;
                owned.truncate(most);
                let next = if more {
                    owned.last().map(|(name, _)| name.clone())
                } else {
                    None
                };
                Ok(Read::Owned(Buckets {
                    buckets: owned
                        .into_iter()
                        .map(|(name, row)| (name, object::seconds(row.created_ns)))
                        .collect(),
                    next,
                }))
            }
            Self::Probe {
                bucket,
                generation,
                from,
                budget,
            } => {
                let budget = usize::try_from(*budget).unwrap_or(usize::MAX);
                name::probe(engine, bucket, *generation, from.as_deref(), budget)
                    .map(Read::Probe)
                    .map_err(meta)
            }
        }
    }
}

/// Whether `bucket` has a gate in the range that is not condemned: a bucket deleted, or never
/// made, has none, and S3 answers its reads `NoSuchBucket`. A gate closed for a delete's probe
/// still names a bucket that may stay (`mantle_meta::coordinator`).
fn gated<E: Rows>(engine: &mut E, bucket: &str) -> Result<bool, MetaError> {
    Ok(name::gate(engine, bucket)?.is_some_and(|g| g.state != GateState::Condemned))
}

/// What a page lists of a key's current version, beside its key.
struct Facts {
    etag: String,
    size: u64,
    modified: u64,
    checksum: Option<(Algorithm, u16)>,
    owner: Option<String>,
}

impl Facts {
    fn listed(self, key: String) -> Listed {
        Listed {
            key,
            etag: self.etag,
            size: self.size,
            modified: self.modified,
            checksum: self.checksum,
            owner: self.owner,
        }
    }
}

/// A bucket's keys in a Name range, as ListObjectsV2 pages them, within a reply's room.
struct Index<'a, E> {
    engine: &'a mut E,
    bucket: &'a str,
    owners: bool,
    /// The bytes of the reply left for the page's items.
    room: usize,
}

impl<E: Rows> list::Keys for Index<'_, E> {
    type Value = Facts;
    type Error = MetaError;

    fn seek(
        &mut self,
        from: &[u8],
        to: &[u8],
        budget: &mut usize,
    ) -> Result<Step<(String, Facts)>, MetaError> {
        Ok(
            match name::next_current(self.engine, self.bucket, from, to, budget)? {
                Scan::End => Step::End,
                Scan::Paused(key) => Step::Paused(key),
                Scan::Found(current) => {
                    let version = current.version;
                    let checksum = match version.checksum {
                        None => None,
                        Some(c) => Some((
                            Algorithm::from_code(c.algorithm).ok_or(MetaError::Corrupt)?,
                            c.parts,
                        )),
                    };
                    let facts = Facts {
                        etag: version.etag,
                        size: version.size,
                        modified: object::seconds(version.modified_ns),
                        checksum,
                        owner: if self.owners {
                            Some(version.owner)
                        } else {
                            None
                        },
                    };
                    Step::Found((current.key, facts))
                }
            },
        )
    }

    fn room(&mut self, key: &str, value: Option<&Facts>) -> bool {
        let bytes = match value {
            None => object::prefix_bytes(key),
            Some(f) => object::listed_bytes(key, &f.etag, f.checksum.is_some(), f.owner.as_deref()),
        };
        match self.room.checked_sub(bytes) {
            Some(left) => {
                self.room = left;
                true
            }
            None => false,
        }
    }
}

impl Read {
    pub fn encode(&self) -> Result<Vec<u8>, ReadError> {
        let mut w = Writer::default();
        match self {
            Self::Current(None) => w.u8(0),
            Self::Current(Some((order, version))) => {
                w.u8(1);
                w.u64(*order);
                w.bytes(&version.encode()?);
            }
            Self::Volumes { volumes, more } => {
                w.u8(u8::from(*more));
                w.u32(u32::try_from(volumes.len()).map_err(|_| ReadError::Answer)?);
                for p in volumes {
                    w.u128(p.volume);
                    w.u64(p.node);
                    w.u64(p.bytes);
                    put_text(&mut w, &p.domain);
                }
            }
            Self::Header(None)
            | Self::Block(None)
            | Self::Bucket(None)
            | Self::Gate(Routed::Here(None)) => w.u8(0),
            Self::NoBucket => w.u8(2),
            Self::Keys(page) => {
                w.u8(1);
                object::put_page(&mut w, page).map_err(|_| ReadError::Answer)?;
            }
            Self::Owned(page) => {
                object::put_buckets(&mut w, page).map_err(|_| ReadError::Answer)?;
            }
            Self::Probe(Routed::Here(Probe::Found)) => w.u8(0),
            Self::Probe(Routed::Here(Probe::Clear)) => w.u8(1),
            Self::Probe(Routed::Here(Probe::Paused(from))) => {
                w.u8(2);
                put_record(&mut w, from)?;
            }
            Self::Probe(Routed::Moved(lineage)) => {
                w.u8(3);
                w.bytes(&lineage.encode()?);
            }
            Self::Bucket(Some(row)) => {
                w.u8(1);
                w.bytes(&row.encode()?);
            }
            Self::Gate(Routed::Here(Some(gate))) => {
                w.u8(1);
                w.bytes(&gate.encode());
            }
            Self::Gate(Routed::Moved(lineage)) => {
                w.u8(2);
                w.bytes(&lineage.encode()?);
            }
            Self::Header(Some(header)) => {
                w.u8(1);
                w.bytes(&header.encode()?);
            }
            Self::Extents(extents) => {
                w.u32(u32::try_from(extents.len()).map_err(|_| ReadError::Answer)?);
                for (offset, extent) in extents {
                    w.u64(*offset);
                    put_record(&mut w, &extent.encode())?;
                }
            }
            Self::Block(Some((header, places))) => {
                w.u8(1);
                put_record(&mut w, &header.encode())?;
                w.u32(u32::try_from(places.len()).map_err(|_| ReadError::Answer)?);
                for place in places {
                    put_record(&mut w, &place.encode())?;
                }
            }
        }
        Ok(w.into_vec())
    }

    /// The answer to `query` that `bytes` holds.
    pub fn decode(query: &Query, bytes: &[u8]) -> Result<Self, ReadError> {
        let mut r = Reader::new(bytes);
        match query {
            Query::Current { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Current(None)),
                Some(1) => {
                    let order = r.u64().ok_or(ReadError::Answer)?;
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Current(Some((order, Version::decode(rest)?))))
                }
                Some(2) if r.remaining() == 0 => Ok(Self::NoBucket),
                _ => Err(ReadError::Answer),
            },
            Query::List(_) => match r.u8() {
                Some(2) if r.remaining() == 0 => Ok(Self::NoBucket),
                Some(1) => object::take_page(&mut r)
                    .filter(|_| r.remaining() == 0)
                    .map(Self::Keys)
                    .ok_or(ReadError::Answer),
                _ => Err(ReadError::Answer),
            },
            Query::Owned { .. } => object::take_buckets(&mut r)
                .filter(|_| r.remaining() == 0)
                .map(Self::Owned)
                .ok_or(ReadError::Answer),
            Query::Probe { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Probe(Routed::Here(Probe::Found))),
                Some(1) if r.remaining() == 0 => Ok(Self::Probe(Routed::Here(Probe::Clear))),
                Some(2) => {
                    let from = take_record(&mut r)?;
                    if from.len() > MAX_POSITION || r.remaining() != 0 {
                        return Err(ReadError::Answer);
                    }
                    Ok(Self::Probe(Routed::Here(Probe::Paused(from.to_vec()))))
                }
                Some(3) => {
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Probe(Routed::Moved(Box::new(Lineage::decode(rest)?))))
                }
                _ => Err(ReadError::Answer),
            },
            Query::Volumes { .. } => {
                let page = (|| {
                    let more = match r.u8()? {
                        0 => false,
                        1 => true,
                        _ => return None,
                    };
                    let count = usize::try_from(r.u32()?).ok()?;
                    // A volume takes 36 bytes at least: the count is no larger than the rest holds.
                    if count > r.remaining() / (PLACED_BYTES - MAX_DOMAIN) {
                        return None;
                    }
                    let mut volumes = Vec::with_capacity(count);
                    for _ in 0..count {
                        volumes.push(Placed {
                            volume: r.u128()?,
                            node: r.u64()?,
                            bytes: r.u64()?,
                            domain: take_text(&mut r, MAX_DOMAIN)?,
                        });
                    }
                    Some(Self::Volumes { volumes, more })
                })();
                page.filter(|_| r.remaining() == 0).ok_or(ReadError::Answer)
            }
            Query::Header { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Header(None)),
                Some(1) => {
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Header(Some(FileHeader::decode(rest)?)))
                }
                _ => Err(ReadError::Answer),
            },
            Query::Extents { .. } => {
                let count = usize::try_from(r.u32().ok_or(ReadError::Answer)?)
                    .map_err(|_| ReadError::Answer)?;
                // An extent takes 12 bytes at least: the count is no larger than the rest holds.
                if count > r.remaining() / 12 {
                    return Err(ReadError::Answer);
                }
                let mut extents = Vec::with_capacity(count);
                for _ in 0..count {
                    let offset = r.u64().ok_or(ReadError::Answer)?;
                    extents.push((offset, Extent::decode(take_record(&mut r)?)?));
                }
                if r.remaining() != 0 {
                    return Err(ReadError::Answer);
                }
                Ok(Self::Extents(extents))
            }
            Query::Bucket { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Bucket(None)),
                Some(1) => {
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Bucket(Some(Bucket::decode(rest)?)))
                }
                _ => Err(ReadError::Answer),
            },
            Query::Gate { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Gate(Routed::Here(None))),
                Some(1) => {
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Gate(Routed::Here(Some(Gate::decode(rest)?))))
                }
                Some(2) => {
                    let rest = r.take(r.remaining()).ok_or(ReadError::Answer)?;
                    Ok(Self::Gate(Routed::Moved(Box::new(Lineage::decode(rest)?))))
                }
                _ => Err(ReadError::Answer),
            },
            Query::Block { .. } => match r.u8() {
                Some(0) if r.remaining() == 0 => Ok(Self::Block(None)),
                Some(1) => {
                    let header = BlockHeader::decode(take_record(&mut r)?)?;
                    let count = usize::try_from(r.u32().ok_or(ReadError::Answer)?)
                        .map_err(|_| ReadError::Answer)?;
                    if count > r.remaining() / 4 {
                        return Err(ReadError::Answer);
                    }
                    let mut places = Vec::with_capacity(count);
                    for _ in 0..count {
                        places.push(ChunkPlace::decode(take_record(&mut r)?)?);
                    }
                    if r.remaining() != 0 {
                        return Err(ReadError::Answer);
                    }
                    Ok(Self::Block(Some((header, places))))
                }
                _ => Err(ReadError::Answer),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_meta::engine::Model;

    #[test]
    fn a_query_reads_back_and_a_name_past_s3s_bound_is_refused() {
        let query = Query::Current {
            bucket: "b".into(),
            key: "k/ü".into(),
        };
        assert_eq!(Query::decode(&query.encode().unwrap()), Ok(query));
        let long = Query::Current {
            bucket: "b".repeat(MAX_BUCKET + 1),
            key: "k".into(),
        };
        assert_eq!(
            Query::decode(&long.encode().unwrap()),
            Err(ReadError::Query)
        );
        let mut cut = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        }
        .encode()
        .unwrap();
        cut.pop();
        assert_eq!(Query::decode(&cut), Err(ReadError::Query));
    }

    /// A Name range holding bucket "b", incarnation 1, its gate open, and the index of the last
    /// entry it applied.
    fn name_range() -> (Model, u64) {
        use mantle_meta::engine::Engine as _;
        let mut m = Model::default();
        m.install(0, name::first(1).unwrap()).unwrap();
        let open = name::Command::Gate(name::GateChange {
            bucket: "b".into(),
            incarnation: 1,
            attempt: 1,
            from: None,
            to: Some(GateState::Open),
            generation: 1,
        });
        assert_eq!(
            name::apply(&mut m, 1, &open).unwrap(),
            name::Outcome::GateMoved
        );
        (m, 1)
    }

    /// Puts an empty object at `key`, as entry `index`.
    fn put(m: &mut Model, index: u64, key: &str) {
        let put = name::Command::Put(name::Put {
            bucket: "b".into(),
            incarnation: 1,
            key: key.into(),
            versioning: mantle_meta::record::Versioning::Unversioned,
            preconditions: name::Preconditions::default(),
            at_ns: index * 1_000_000_000,
            ordered_ns: None,
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
                size: 0,
                checksum: None,
                file: None,
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: u128::from(index),
            deadline_ns: u64::MAX,
        });
        assert!(matches!(
            name::apply(m, index, &put).unwrap(),
            name::Outcome::Put { .. }
        ));
    }

    fn round_trip(query: &Query, engine: &mut Model, room: usize) -> Read {
        assert_eq!(Query::decode(&query.encode().unwrap()), Ok(query.clone()));
        let read = query.answer(engine, room).unwrap();
        let bytes = read.encode().unwrap();
        assert!(bytes.len() <= room, "{} > {room}", bytes.len());
        assert_eq!(Read::decode(query, &bytes), Ok(read.clone()));
        read
    }

    #[test]
    fn a_key_reads_as_none_in_its_bucket_and_no_bucket_without_one() {
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        assert_eq!(
            round_trip(&query, &mut Model::default(), 1 << 10),
            Read::NoBucket
        );
        let (mut m, index) = name_range();
        assert_eq!(round_trip(&query, &mut m, 1 << 10), Read::Current(None));
        put(&mut m, index + 1, "k");
        let Read::Current(Some((_, version))) = round_trip(&query, &mut m, 1 << 10) else {
            panic!("no version");
        };
        assert_eq!(version.size, 0);
    }

    /// A listing's pages, each within its reply's room, list every key and common prefix once
    /// however small the room, and a reply too small for the longest key is refused.
    #[test]
    fn a_listing_pages_within_its_room_and_resumes_exactly() {
        let (mut m, mut index) = name_range();
        let mut keys: Vec<String> = (0..20).map(|n| format!("k{n:02}")).collect();
        keys.extend(["d/1".into(), "d/2".into(), "e/f/g".into()]);
        for key in &keys {
            index += 1;
            put(&mut m, index, key);
        }
        let list = |start: Option<Resume>| {
            Query::List(List {
                bucket: "b".into(),
                prefix: String::new(),
                delimiter: "/".into(),
                start,
                max_keys: 1000,
                fetch_owner: true,
            })
        };
        let mut expected: Vec<String> = keys
            .iter()
            .filter(|k| !k.contains('/'))
            .cloned()
            .chain(["d/".into(), "e/".into()])
            .collect();
        expected.sort();
        let least = 1 + object::PAGE_BYTES + object::LISTED_BYTES;
        for room in [least, least + 200, 1 << 20] {
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let Read::Keys(page) = round_trip(&list(start.take()), &mut m, room) else {
                    panic!("no page");
                };
                assert!(
                    page.contents
                        .iter()
                        .all(|l| l.owner.as_deref() == Some("o"))
                );
                seen.extend(page.contents.into_iter().map(|l| l.key));
                seen.extend(page.common_prefixes);
                if !page.truncated {
                    break;
                }
                start = page.next;
            }
            seen.sort();
            assert_eq!(seen, expected, "room {room}");
        }
        assert_eq!(
            list(None).answer(&mut m, least - 1),
            Err(ReadError::Page(least - 1))
        );
        assert_eq!(
            round_trip(&list(None), &mut Model::default(), 1 << 10),
            Read::NoBucket
        );
        assert_eq!(list(None).layer(), Layer::Name);
    }

    /// The owner's buckets read a page at a time, by prefix, and the delete's probe reads
    /// whether a bucket's rows remain.
    #[test]
    fn an_owners_buckets_page_and_a_probe_finds_what_remains() {
        let mut buckets = Model::default();
        for (index, name) in (1u64..).zip(["a", "b", "ba", "c"]) {
            let create = bucket::Command::Create(bucket::Create {
                bucket: name.into(),
                owner: "o".into(),
                location: String::new(),
                at_ns: index,
                quota: 10,
                lock: false,
            });
            let bucket::Outcome::Creating { attempt, .. } =
                bucket::apply(&mut buckets, index * 2, &create).unwrap()
            else {
                panic!("not created");
            };
            let activate = bucket::Command::Activate {
                bucket: name.into(),
                attempt,
            };
            bucket::apply(&mut buckets, index * 2 + 1, &activate).unwrap();
        }
        let owned = |prefix: &str, after: Option<&str>, max| Query::Owned {
            owner: "o".into(),
            prefix: prefix.into(),
            after: after.map(Into::into),
            max,
        };
        let names = |read: Read| match read {
            Read::Owned(page) => (
                page.buckets.into_iter().map(|(n, _)| n).collect::<Vec<_>>(),
                page.next,
            ),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            names(round_trip(&owned("", None, 2), &mut buckets, 1 << 10)),
            (vec!["a".into(), "b".into()], Some("b".into()))
        );
        assert_eq!(
            names(round_trip(&owned("", Some("b"), 2), &mut buckets, 1 << 10)),
            (vec!["ba".into(), "c".into()], None)
        );
        assert_eq!(
            names(round_trip(&owned("b", None, 10), &mut buckets, 1 << 10)),
            (vec!["b".into(), "ba".into()], None)
        );
        // A reply with room for one bucket lists one, and says where the next page starts.
        let one = 4 + 1 + 4 + MAX_BUCKET + 4 + MAX_BUCKET + 8;
        assert_eq!(
            names(round_trip(&owned("", None, 10), &mut buckets, one)),
            (vec!["a".into()], Some("a".into()))
        );
        assert_eq!(owned("", None, 1).layer(), Layer::Bucket);

        let (mut m, index) = name_range();
        let probe = |from: Option<Vec<u8>>| Query::Probe {
            bucket: "b".into(),
            generation: 1,
            from,
            budget: 8,
        };
        assert_eq!(
            round_trip(&probe(None), &mut m, 1 << 10),
            Read::Probe(Routed::Here(Probe::Clear))
        );
        put(&mut m, index + 1, "k");
        assert_eq!(
            round_trip(&probe(None), &mut m, 1 << 10),
            Read::Probe(Routed::Here(Probe::Found))
        );
        // A read that starts past the bucket's rows finds none of them.
        assert_eq!(
            round_trip(&probe(Some(vec![0xFF; 4])), &mut m, 1 << 10),
            Read::Probe(Routed::Here(Probe::Clear))
        );
        let moved = Query::Probe {
            bucket: "b".into(),
            generation: 2,
            from: None,
            budget: 8,
        };
        assert!(matches!(
            round_trip(&moved, &mut m, 1 << 12),
            Read::Probe(Routed::Moved(_))
        ));
        assert_eq!(probe(None).layer(), Layer::Name);
    }

    #[test]
    fn the_volume_table_reads_a_page_at_a_time_each_within_its_bytes() {
        let mut m = Model::default();
        for node in 1..=3u64 {
            let enlist = root::Command::Enlist(root::Enlist {
                node,
                domain: "d".repeat(usize::try_from(node).unwrap() * 80),
                volumes: vec![(u128::from(node), node << 30), (u128::from(node) + 10, 1)],
                max_nodes: 3,
                max_volumes: 6,
            });
            assert_eq!(
                root::apply(&mut m, node, &enlist).unwrap(),
                root::Outcome::Enlisted
            );
        }
        // Two volumes a page, counted at the longest domain.
        let page = PAGE_HEAD_BYTES + 2 * PLACED_BYTES;
        let mut after = None;
        let mut read = Vec::new();
        loop {
            let query = Query::Volumes { after };
            assert_eq!(Query::decode(&query.encode().unwrap()), Ok(query.clone()));
            let answer = query.answer(&mut m, page).unwrap();
            let bytes = answer.encode().unwrap();
            assert!(bytes.len() <= page);
            assert_eq!(Read::decode(&query, &bytes), Ok(answer.clone()));
            let Read::Volumes { volumes, more } = answer else {
                panic!("{answer:?}");
            };
            assert!(volumes.len() <= 2);
            after = volumes.last().map(|p| p.volume);
            read.extend(volumes);
            if !more {
                break;
            }
        }
        let ids: Vec<u128> = read.iter().map(|p| p.volume).collect();
        assert_eq!(ids, vec![1, 2, 3, 11, 12, 13]);
        assert!(
            read.iter()
                .all(|p| p.domain.len() == 80 * usize::try_from(p.node).unwrap())
        );
        // A reply too small for one volume is refused, not cut.
        assert_eq!(
            Query::Volumes { after: None }.answer(&mut m, PLACED_BYTES),
            Err(ReadError::Page(PLACED_BYTES))
        );
        assert_eq!(Query::Volumes { after: None }.layer(), Layer::Root);
    }

    #[test]
    fn a_files_header_and_extents_and_a_blocks_places_read_back_through_their_answers() {
        use mantle_chunk::ChunkKey;
        use mantle_meta::record::{Referrer, Target};
        let mut files = Model::default();
        let extents: Vec<Extent> = (1..=3u128)
            .map(|block| Extent {
                length: 1 << 20,
                target: Target::Block(block),
            })
            .collect();
        let write = file::Command::Write {
            file: 7,
            extents: extents.clone(),
            referrer: Referrer {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
            },
            key: None,
            handover_ns: 100,
            at_ns: 1_000,
            blocks_deadline_ns: u64::MAX,
        };
        file::apply(&mut files, 1, &write).unwrap();
        let mut blocks = Model::default();
        let places: Vec<ChunkPlace> = (0u16..3)
            .map(|index| ChunkPlace {
                volume: u128::from(index) + 10,
                key: ChunkKey {
                    block: 2,
                    epoch: 1,
                    index,
                },
            })
            .collect();
        let header = BlockHeader {
            length: 1 << 20,
            data: 2,
            parity: 1,
            chunk_len: 1 << 19,
            crc32c: 9,
        };
        let write = block::Command::Write {
            block: 2,
            header,
            chunks: places.clone(),
            file: 7,
            handover_ns: u64::MAX / 2,
            at_ns: 0,
        };
        block::apply(&mut blocks, 1, &write).unwrap();
        let round_trip = |query: Query, engine: &mut Model| {
            assert_eq!(Query::decode(&query.encode().unwrap()), Ok(query.clone()));
            let read = query.answer(engine, 1 << 12).unwrap();
            assert_eq!(
                Read::decode(&query, &read.encode().unwrap()),
                Ok(read.clone())
            );
            read
        };
        let Read::Header(Some(h)) = round_trip(Query::Header { file: 7 }, &mut files) else {
            panic!("no header");
        };
        assert_eq!(h.length, 3 << 20);
        assert_eq!(
            round_trip(Query::Header { file: 8 }, &mut files),
            Read::Header(None)
        );
        let from_second = Query::Extents {
            file: 7,
            offset: 1 << 20,
            max: 5,
        };
        assert_eq!(
            round_trip(from_second, &mut files),
            Read::Extents(vec![(1 << 20, extents[1]), (2 << 20, extents[2])])
        );
        assert_eq!(
            round_trip(Query::Block { block: 2 }, &mut blocks),
            Read::Block(Some((header, places)))
        );
        assert_eq!(
            round_trip(Query::Block { block: 3 }, &mut blocks),
            Read::Block(None)
        );
        // More extents than a reply carries are refused whole, not cut short.
        let many = Query::Extents {
            file: 7,
            offset: 0,
            max: 1_000,
        };
        assert_eq!(
            many.answer(&mut files, 1 << 12),
            Err(ReadError::Page(1 << 12))
        );
        assert_eq!(many.layer(), Layer::File);
        assert_eq!(Query::Block { block: 2 }.layer(), Layer::Block);
    }
}
