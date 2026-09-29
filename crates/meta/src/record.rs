//! Row values: a format byte, the fields, and a CRC-32C of everything before it, verified
//! before a field is read (CLAUDE.md rule 6). A value whose checksum fails is `Corrupt`.

use mantle_chunk::codec::{Reader, Writer};

/// Values written by this code.
const FORMAT: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("a {0} row failed its checksum or does not decode")]
    Corrupt(&'static str),
    /// A field longer than a length field holds; the protocol's own limits keep every field
    /// far below it.
    #[error("a field of {0} bytes is too long for a row")]
    TooLarge(usize),
}

/// A version of an object: its bytes and what S3 returns about them, or a delete marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// A delete marker rather than an object.
    pub marker: bool,
    /// The key's null version (05 §7.1).
    pub null: bool,
    /// When it was written, nanoseconds since the Unix epoch: its `Last-Modified`.
    pub modified_ns: u64,
    /// The ETag without its quotes.
    pub etag: String,
    pub size: u64,
    pub checksum: Option<Checksum>,
    /// The file holding the bytes; none for a delete marker or an empty object.
    pub file: Option<u128>,
    pub owner: String,
    /// Content headers and user metadata, as the object was written with them.
    pub headers: Vec<(String, String)>,
}

/// An object's checksum as S3 returns it (05 §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    /// The algorithm's code, as the protocol layer numbers them.
    pub algorithm: u8,
    /// Parts combined for a composite value; zero for a full-object value.
    pub parts: u16,
    pub value: Vec<u8>,
}

/// A multipart upload in progress (05 §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// When it was created: it also orders the version its completion makes (05 §4.4).
    pub initiated_ns: u64,
    pub owner: String,
    pub headers: Vec<(String, String)>,
    /// The checksum algorithm and whether values combine as full-object or composite.
    pub checksum: Option<(u8, bool)>,
}

/// A part of an upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub etag: String,
    pub size: u64,
    pub checksum: Option<Vec<u8>>,
    pub file: u128,
    pub modified_ns: u64,
}

impl Version {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u8(u8::from(self.marker) | (u8::from(self.null) << 1));
        w.u64(self.modified_ns);
        put_str(&mut w, &self.etag)?;
        w.u64(self.size);
        match &self.checksum {
            None => w.u8(0),
            Some(c) => {
                w.u8(1);
                w.u8(c.algorithm);
                w.u16(c.parts);
                put_bytes(&mut w, &c.value)?;
            }
        }
        put_file(&mut w, self.file);
        put_str(&mut w, &self.owner)?;
        put_pairs(&mut w, &self.headers)?;
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "version")?;
        decoded(
            (|| {
                let flags = r.u8()?;
                let version = Self {
                    marker: flags & 1 != 0,
                    null: flags & 2 != 0,
                    modified_ns: r.u64()?,
                    etag: take_str(&mut r)?,
                    size: r.u64()?,
                    checksum: match r.u8()? {
                        0 => None,
                        1 => Some(Checksum {
                            algorithm: r.u8()?,
                            parts: r.u16()?,
                            value: take_bytes(&mut r)?,
                        }),
                        _ => return None,
                    },
                    file: take_file(&mut r)?,
                    owner: take_str(&mut r)?,
                    headers: take_pairs(&mut r)?,
                };
                if flags < 4 { Some(version) } else { None }
            })(),
            &r,
            "version",
        )
    }
}

impl Upload {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        w.u64(self.initiated_ns);
        put_str(&mut w, &self.owner)?;
        put_pairs(&mut w, &self.headers)?;
        match self.checksum {
            None => w.u8(0),
            Some((algorithm, full)) => {
                w.u8(1 | (u8::from(full) << 1));
                w.u8(algorithm);
            }
        }
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "upload")?;
        decoded(
            (|| {
                Some(Self {
                    initiated_ns: r.u64()?,
                    owner: take_str(&mut r)?,
                    headers: take_pairs(&mut r)?,
                    checksum: match r.u8()? {
                        0 => None,
                        flag @ (1 | 3) => Some((r.u8()?, flag == 3)),
                        _ => return None,
                    },
                })
            })(),
            &r,
            "upload",
        )
    }
}

impl Part {
    pub fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let mut w = start();
        put_str(&mut w, &self.etag)?;
        w.u64(self.size);
        match &self.checksum {
            None => w.u8(0),
            Some(value) => {
                w.u8(1);
                put_bytes(&mut w, value)?;
            }
        }
        w.u128(self.file);
        w.u64(self.modified_ns);
        Ok(finish(w))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut r = open(bytes, "part")?;
        decoded(
            (|| {
                Some(Self {
                    etag: take_str(&mut r)?,
                    size: r.u64()?,
                    checksum: match r.u8()? {
                        0 => None,
                        1 => Some(take_bytes(&mut r)?),
                        _ => return None,
                    },
                    file: r.u128()?,
                    modified_ns: r.u64()?,
                })
            })(),
            &r,
            "part",
        )
    }
}

fn start() -> Writer {
    let mut w = Writer::default();
    w.u8(FORMAT);
    w
}

/// Appends the CRC-32C of everything written.
fn finish(mut w: Writer) -> Vec<u8> {
    let crc = mantle_crc::crc32c(w.as_slice());
    w.u32(crc);
    w.into_vec()
}

/// A reader over a value's fields once its checksum and format are verified.
fn open<'a>(bytes: &'a [u8], what: &'static str) -> Result<Reader<'a>, RecordError> {
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or(RecordError::Corrupt(what))?;
    let (body, crc) = bytes.split_at(body_len);
    let crc = u32::from_le_bytes(crc.try_into().map_err(|_| RecordError::Corrupt(what))?);
    if mantle_crc::crc32c(body) != crc {
        return Err(RecordError::Corrupt(what));
    }
    let mut r = Reader::new(body);
    if r.u8() != Some(FORMAT) {
        return Err(RecordError::Corrupt(what));
    }
    Ok(r)
}

/// A decoded value, if its fields used every byte.
fn decoded<T>(value: Option<T>, r: &Reader<'_>, what: &'static str) -> Result<T, RecordError> {
    value
        .filter(|_| r.remaining() == 0)
        .ok_or(RecordError::Corrupt(what))
}

fn put_len(w: &mut Writer, len: usize) -> Result<(), RecordError> {
    w.u32(u32::try_from(len).map_err(|_| RecordError::TooLarge(len))?);
    Ok(())
}

fn put_bytes(w: &mut Writer, b: &[u8]) -> Result<(), RecordError> {
    put_len(w, b.len())?;
    w.bytes(b);
    Ok(())
}

fn put_str(w: &mut Writer, s: &str) -> Result<(), RecordError> {
    put_bytes(w, s.as_bytes())
}

fn put_file(w: &mut Writer, file: Option<u128>) {
    match file {
        None => w.u8(0),
        Some(id) => {
            w.u8(1);
            w.u128(id);
        }
    }
}

fn put_pairs(w: &mut Writer, pairs: &[(String, String)]) -> Result<(), RecordError> {
    put_len(w, pairs.len())?;
    for (name, value) in pairs {
        put_str(w, name)?;
        put_str(w, value)?;
    }
    Ok(())
}

fn take_bytes(r: &mut Reader<'_>) -> Option<Vec<u8>> {
    let len = usize::try_from(r.u32()?).ok()?;
    Some(r.take(len)?.to_vec())
}

fn take_str(r: &mut Reader<'_>) -> Option<String> {
    String::from_utf8(take_bytes(r)?).ok()
}

fn take_file(r: &mut Reader<'_>) -> Option<Option<u128>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.u128()?)),
        _ => None,
    }
}

fn take_pairs(r: &mut Reader<'_>) -> Option<Vec<(String, String)>> {
    let count = usize::try_from(r.u32()?).ok()?;
    // Each pair takes at least eight bytes, so a count the value cannot hold is corrupt
    // before anything is allocated for it.
    if count > r.remaining() / 8 {
        return None;
    }
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        pairs.push((take_str(r)?, take_str(r)?));
    }
    Some(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version() -> Version {
        Version {
            marker: false,
            null: true,
            modified_ns: 1_727_179_200_000_000_000,
            etag: "6805f2cfc46c0f04559748bb039d69ae".into(),
            size: 11,
            checksum: Some(Checksum {
                algorithm: 1,
                parts: 3,
                value: vec![1, 2, 3, 4],
            }),
            file: Some(0x1234_5678_9abc_def0_1234_5678_9abc_def0),
            owner: "owner".into(),
            headers: vec![
                ("content-type".into(), "text/plain".into()),
                ("x-amz-meta-a".into(), "é".into()),
            ],
        }
    }

    #[test]
    fn rows_round_trip() {
        let v = version();
        assert_eq!(Version::decode(&v.encode().unwrap()), Ok(v));
        let marker = Version {
            marker: true,
            checksum: None,
            file: None,
            headers: vec![],
            ..version()
        };
        assert_eq!(Version::decode(&marker.encode().unwrap()), Ok(marker));
        let u = Upload {
            initiated_ns: 5,
            owner: "o".into(),
            headers: vec![("content-type".into(), "a/b".into())],
            checksum: Some((2, true)),
        };
        assert_eq!(Upload::decode(&u.encode().unwrap()), Ok(u));
        let p = Part {
            etag: "e".into(),
            size: 5 << 20,
            checksum: None,
            file: 7,
            modified_ns: 9,
        };
        assert_eq!(Part::decode(&p.encode().unwrap()), Ok(p));
    }

    /// Every flipped bit and every truncation is refused, never misread.
    #[test]
    fn damage_is_corrupt() {
        let bytes = version().encode().unwrap();
        for i in 0..bytes.len() * 8 {
            let mut damaged = bytes.clone();
            damaged[i / 8] ^= 1 << (i % 8);
            assert_eq!(
                Version::decode(&damaged),
                Err(RecordError::Corrupt("version"))
            );
        }
        for len in 0..bytes.len() {
            assert!(Version::decode(&bytes[..len]).is_err());
        }
        assert!(Part::decode(&bytes).is_err());
    }
}
