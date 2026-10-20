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
//! | the current version of a key | kind 0, the bucket's and the key's lengths (u32 little-endian) each before its bytes | 0 for none; 1, the version's order (u64 little-endian), then the version (`mantle_meta::record::Version`) |
//!
//! Which reads a range answers grows with the request path (node.md §5.1: headers, extents, blocks,
//! uploads and parts); a read never changes what the range holds.

use mantle_codec::{Reader, Writer};
use mantle_meta::engine::Rows;
use mantle_meta::name;
use mantle_meta::record::Version;
use mantle_meta::wire::{MAX_BUCKET, MAX_KEY};

/// The query's kind: the current version of a key.
const CURRENT: u8 = 0;

/// What a read asks of a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// The current version of `key` in `bucket`: what a GET or HEAD of it reads first.
    Current { bucket: String, key: String },
}

/// What a range answered a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// The key's current version and its order, or none.
    Current(Option<(u64, Version)>),
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
            _ => None,
        })();
        query.filter(|_| r.remaining() == 0).ok_or(ReadError::Query)
    }

    /// The answer `engine` gives the query now.
    pub fn answer<E: Rows>(&self, engine: &mut E) -> Result<Read, ReadError> {
        match self {
            Self::Current { bucket, key } => name::current(engine, bucket, key)
                .map(Read::Current)
                .map_err(|e| ReadError::Meta(e.to_string())),
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
        let read = query.answer(&mut Model::default()).unwrap();
        assert_eq!(read, Read::Current(None));
        assert_eq!(Read::decode(&query, &read.encode().unwrap()), Ok(read));
    }
}
