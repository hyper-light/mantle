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
//!
//! A read is answered only by a range of the layer whose rows it reads (`Query::layer`). A page
//! holds as many volumes as a reply carries, each counted at the longest domain, so it fits
//! whatever their domains.
//! Which reads a range answers grows with the request path (node.md §5.1: headers, extents, blocks,
//! uploads and parts); a read never changes what the range holds.

use mantle_codec::{Reader, Writer};
use mantle_meta::apply::Layer;
use mantle_meta::engine::Rows;
use mantle_meta::name;
use mantle_meta::record::Version;
use mantle_meta::root::{self, MAX_DOMAIN, Placed};
use mantle_meta::wire::{MAX_BUCKET, MAX_KEY};

/// The query's kind: the current version of a key, or a page of the volume table.
const CURRENT: u8 = 0;
const VOLUMES: u8 = 1;
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
    /// The volume table's volumes after `after`, or from the first (node.md §5.6).
    Volumes { after: Option<u128> },
}

/// What a range answered a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// The key's current version and its order, or none.
    Current(Option<(u64, Version)>),
    /// A page of the volume table, and whether more volumes follow it.
    Volumes { volumes: Vec<Placed>, more: bool },
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
    #[error("a reply of {0} bytes holds no page of volumes")]
    Page(usize),
}

fn put_text(w: &mut Writer, text: &str) {
    // Each name is within S3's bounds, which a u32 holds; `decode` refuses one past them.
    w.u32(u32::try_from(text.len()).unwrap_or(u32::MAX));
    w.bytes(text.as_bytes());
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
            _ => None,
        })();
        query.filter(|_| r.remaining() == 0).ok_or(ReadError::Query)
    }

    /// The layer whose rows the query reads: only a range of it answers.
    pub fn layer(&self) -> Layer {
        match self {
            Self::Current { .. } => Layer::Name,
            Self::Volumes { .. } => Layer::Root,
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
}
