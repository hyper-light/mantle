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
//! | the current version of a key, of a Name range | kind 0, the bucket's and the key's lengths (u32 little-endian) each before its bytes | 0 for none; 1, the version's order (u64 little-endian), then the version (`mantle_meta::record::Version`) |
//! | a page of the volume table, of the root range | kind 1; 0 from the first volume, or 1 and the volume after which the page starts (u128) | 1 if more volumes follow the page, else 0; the count (u32); each volume's id (u128), node (u64) and bytes (u64), then its node's domain, its length (u32) before its bytes |
//! | a file's header, of a File range | kind 2; the file (u128) | 0 for none; 1, then the header (`mantle_meta::record::FileHeader`) |
//! | a file's extents from the one holding an offset, of a File range | kind 3; the file (u128), the offset (u64), the most extents (u32) | the count (u32); each extent's first byte's offset (u64), then the extent (`Extent`), its length (u32) before it |
//! | a block's header and its chunks' places, of a Block range | kind 4; the block (u128) | 0 for none; 1, the header (`BlockHeader`) and the count of places (u32), each record its length (u32) before it |
//! | a bucket's row, of the Bucket range | kind 5; the bucket's name, its length (u32) before it | 0 for none; 1, then the row (`Bucket`) |
//! | a bucket's gate, of a Name range routed by a descriptor's generation | kind 6; the bucket's name, its length (u32) before it; the generation (u64) | 0 for no gate; 1, then the gate (`Gate`); 2, then the range's lineage (`Lineage`), where its span went |
//!
//! A read is answered only by a range of the layer whose rows it reads (`Query::layer`). A page
//! holds as many volumes as a reply carries, each counted at the longest domain, so it fits
//! whatever their domains. Extents are read as many as asked, the asker's window's worth (gateway.md
//! §3): a read of more than a reply carries is refused whole, since an answer cut short reads as
//! the file's end.
//!
//! Which reads a range answers grows with the request path (node.md §5.1: headers, extents, blocks,
//! uploads and parts); a read never changes what the range holds.

use mantle_codec::{Reader, Writer};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Rows;
use mantle_meta::name::{self, Routed};
use mantle_meta::record::{
    BlockHeader, Bucket, ChunkPlace, Extent, FileHeader, Gate, Lineage, Version,
};
use mantle_meta::root::{self, MAX_DOMAIN, Placed};
use mantle_meta::wire::{MAX_BUCKET, MAX_KEY};
use mantle_meta::{block, bucket, file};

/// The query's kind: the current version of a key, or a page of the volume table.
const CURRENT: u8 = 0;
const VOLUMES: u8 = 1;
const HEADER: u8 = 2;
const EXTENTS: u8 = 3;
const BLOCK: u8 = 4;
const BUCKET: u8 = 5;
const GATE: u8 = 6;
/// An extent's bytes in an answer, at most: its first byte's offset, its record's length and the
/// record (`Extent::encode`: the format, the length, the target's kind and id, the CRC-32C).
const EXTENT_BYTES: usize = 8 + 4 + (1 + 8 + 1 + 16 + 4);
/// A page's bytes before its volumes: whether more follow, and the count.
const PAGE_HEAD_BYTES: usize = 1 + 4;
/// A volume's bytes in a page at the longest domain: its id, node and bytes, and its domain behind
/// its length.
const PLACED_BYTES: usize = 16 + 8 + 8 + 4 + MAX_DOMAIN;

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
    pub fn encode(&self) -> Vec<u8> {
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
        }
        w.into_vec()
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
            Self::Bucket { .. } => Layer::Bucket,
            Self::Gate { .. } => Layer::Name,
        }
    }

    /// The answer `engine` gives the query now, in at most `max_bytes` once encoded.
    pub fn answer<E: Rows>(&self, engine: &mut E, max_bytes: usize) -> Result<Read, ReadError> {
        let meta = |e: mantle_meta::error::MetaError| ReadError::Meta(e.to_string());
        match self {
            Self::Current { bucket, key } => name::current(engine, bucket, key)
                .map(Read::Current)
                .map_err(meta),
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
        assert_eq!(Query::decode(&query.encode()), Ok(query));
        let long = Query::Current {
            bucket: "b".repeat(MAX_BUCKET + 1),
            key: "k".into(),
        };
        assert_eq!(Query::decode(&long.encode()), Err(ReadError::Query));
        let mut cut = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        }
        .encode();
        cut.pop();
        assert_eq!(Query::decode(&cut), Err(ReadError::Query));
    }

    #[test]
    fn a_key_no_range_row_names_reads_as_none() {
        let query = Query::Current {
            bucket: "b".into(),
            key: "k".into(),
        };
        let read = query.answer(&mut Model::default(), 1 << 10).unwrap();
        assert_eq!(read, Read::Current(None));
        assert_eq!(Read::decode(&query, &read.encode().unwrap()), Ok(read));
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
            assert_eq!(Query::decode(&query.encode()), Ok(query.clone()));
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
            assert_eq!(Query::decode(&query.encode()), Ok(query.clone()));
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
