//! The native door of `mantle bench transfer`: each object put through mantle-client as `mantle cp`
//! puts one (`Client::upload`: whole up to S3's least part, in runs the node cuts past it), its
//! bytes made as they are read from the generator, and read back through a client that asks
//! another node first, its bytes checked against the generator as they arrive. Neither side holds
//! an object: a piece of it at a time.

use mantle_client::{Client, Native, Rewind, Sink, Source, TRAILER, Watch};
use mantle_protocol::object::{Headers, List, Listing, Op, Progress, Status};

use super::generate::{GenError, Object};

/// An object's bytes as a source: made from the generator at the offset reached.
pub struct Made<'o, 'g> {
    object: &'o Object<'g>,
    at: u64,
    length: u64,
}

impl<'o, 'g> Made<'o, 'g> {
    pub fn new(object: &'o Object<'g>, length: u64) -> Self {
        Self {
            object,
            at: 0,
            length,
        }
    }

    /// The object's bytes from `from` up to `to`: a part of it.
    pub fn range(object: &'o Object<'g>, from: u64, to: u64) -> Self {
        Self {
            object,
            at: from,
            length: to,
        }
    }
}

fn io(e: GenError) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl Source for Made<'_, '_> {
    async fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let left = usize::try_from(self.length.saturating_sub(self.at)).unwrap_or(usize::MAX);
        let n = into.len().min(left);
        let piece = into.get_mut(..n).ok_or_else(|| io(GenError::Offset))?;
        self.object.fill(self.at, piece).map_err(io)?;
        self.at = self
            .at
            .saturating_add(u64::try_from(n).map_err(|_| io(GenError::Offset))?);
        Ok(n)
    }
}

impl Rewind for Made<'_, '_> {
    async fn rewind(&mut self, offset: u64) -> std::io::Result<()> {
        self.at = offset;
        Ok(())
    }
}

/// A get's bytes as a sink: each piece checked against the generator at its offset, the first
/// wrong offset kept, and the bytes counted.
pub struct Checked<'o, 'g> {
    object: &'o Object<'g>,
    at: u64,
    scratch: Vec<u8>,
    pub wrong: Option<u64>,
}

impl<'o, 'g> Checked<'o, 'g> {
    /// A sink checking `object`'s bytes a piece of `piece` bytes at a time.
    pub fn new(object: &'o Object<'g>, piece: usize) -> Self {
        Self {
            object,
            at: 0,
            scratch: vec![0u8; piece.max(1)],
            wrong: None,
        }
    }

    /// The bytes it took.
    pub fn len(&self) -> u64 {
        self.at
    }
}

impl Sink for Checked<'_, '_> {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.wrong.is_none() {
            self.wrong = self
                .object
                .verify(self.at, bytes, &mut self.scratch)
                .map_err(io)?;
        }
        self.at = self
            .at
            .saturating_add(u64::try_from(bytes.len()).map_err(|_| io(GenError::Offset))?);
        Ok(())
    }
}

/// An upload's progress, which the harness keeps no record of: a run restarts no copy.
pub struct Unwatched;

impl Watch for Unwatched {
    async fn opened(&mut self, _: &str) -> std::io::Result<()> {
        Ok(())
    }

    async fn progressed(&mut self, _: &Progress) -> std::io::Result<()> {
        Ok(())
    }
}

/// What an object's put or get came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Came {
    /// Done: put whole, or read whole and every byte the generator's.
    Done,
    /// Read back with a byte not the generator's, at this offset, or of another length.
    Wrong(u64),
    /// Answered with a status other than done.
    Refused(Status),
}

/// Puts `object`, `length` bytes, at `key` in `bucket`.
pub async fn put(
    client: &mut Client,
    (bucket, key): (&str, &str),
    object: &Object<'_>,
    length: u64,
) -> Result<Came, mantle_client::ClientError> {
    let native = Native {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        id: mantle_client::identity()?,
        upload: None,
        checksum: TRAILER,
        headers: Headers::default(),
    };
    let mut made = Made::new(object, length);
    let reply = client
        .upload(&native, (&mut made, length), &mut Unwatched, u64::MAX)
        .await?;
    Ok(if reply.status == Status::Done {
        Came::Done
    } else {
        Came::Refused(reply.status)
    })
}

/// The part an object of `length` bytes is put in: the AWS CLI's 8 MiB (`crate::client::PART`),
/// or what S3's 10,000 parts need of it where that is more (research/05 §4.1).
pub fn part_size(length: u64) -> u64 {
    let parts = u64::from(mantle_s3::body::MAX_PARTS);
    let whole = length.checked_div(parts).unwrap_or(0);
    let least = whole.saturating_add(u64::from(length.checked_rem(parts).unwrap_or(0) != 0));
    least.max(crate::client::PART)
}

/// Puts `object`, `length` bytes, at `key` in `bucket` as an explicit multipart upload of parts
/// `part_size` makes, each part's bytes made from the generator at its offset as they are sent. A
/// part refused ends the upload, aborted, so nothing of the object stays listed.
pub async fn put_parts(
    client: &mut Client,
    (bucket, key): (&str, &str),
    object: &Object<'_>,
    length: u64,
) -> Result<Came, mantle_client::ClientError> {
    let created = client
        .call(
            &Op::CreateUpload {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                id: mantle_client::identity()?,
                checksum: Some((TRAILER, true)),
            },
            u64::MAX,
        )
        .await?;
    if created.status != Status::Done {
        return Ok(Came::Refused(created.status));
    }
    let upload = created.upload;
    let part = part_size(length);
    let mut parts = Vec::new();
    let mut at = 0u64;
    let mut number = 0u16;
    while at < length {
        number = number
            .checked_add(1)
            .ok_or(mantle_client::ClientError::Reply("parts past S3's"))?;
        let to = at.saturating_add(part).min(length);
        let op = Op::UploadPart {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload: upload.clone(),
            number,
            id: mantle_client::identity()?,
            length: to.saturating_sub(at),
            checksum: None,
            md5: None,
            trailing: Some(TRAILER),
        };
        let mut made = Made::range(object, at, to);
        let reply = client.put(&op, &mut made, u64::MAX).await?;
        if reply.status != Status::Done {
            let abort = Op::Abort {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload,
                id: mantle_client::identity()?,
            };
            client.call(&abort, u64::MAX).await?;
            return Ok(Came::Refused(reply.status));
        }
        parts.push((number, reply.etag.trim_matches('"').to_owned()));
        at = to;
    }
    let done = client
        .call(
            &Op::Complete {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload,
                id: mantle_client::identity()?,
                headers: Headers::default(),
                parts,
            },
            u64::MAX,
        )
        .await?;
    Ok(if done.status == Status::Done {
        Came::Done
    } else {
        Came::Refused(done.status)
    })
}

/// Reads `key` in `bucket` back whole and checks it is `object`'s `length` bytes.
pub async fn get(
    client: &mut Client,
    (bucket, key): (&str, &str),
    object: &Object<'_>,
    length: u64,
) -> Result<Came, mantle_client::ClientError> {
    let op = Op::Get {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        headers: Headers::default(),
    };
    let mut checked = Checked::new(object, mantle_client::PIECE);
    let reply = client.get(&op, &mut checked, u64::MAX).await?;
    if reply.status != Status::Done {
        return Ok(Came::Refused(reply.status));
    }
    Ok(match checked.wrong {
        Some(at) => Came::Wrong(at),
        None if checked.len() != length => Came::Wrong(checked.len().min(length)),
        None => Came::Done,
    })
}

/// The keys in `bucket`, listed a page at a time, at most `pages` pages: as many as the keys the
/// case put fill.
pub async fn list(
    client: &mut Client,
    bucket: &str,
    pages: u64,
) -> Result<Vec<String>, mantle_client::ClientError> {
    let mut keys = Vec::new();
    let mut start = None;
    for _ in 0..pages {
        let op = Op::List(List {
            bucket: bucket.to_owned(),
            prefix: String::new(),
            delimiter: String::new(),
            start: start.take(),
            max_keys: u32::try_from(mantle_s3::list::MAX_KEYS).unwrap_or(u32::MAX),
            fetch_owner: false,
        });
        let reply = client.call(&op, u64::MAX).await?;
        let Some(Listing::Keys(page)) = reply.listing else {
            return Err(mantle_client::ClientError::Reply(
                "a listing answered no keys",
            ));
        };
        keys.extend(page.contents.into_iter().map(|listed| listed.key));
        match (page.truncated, page.next) {
            (true, Some(next)) => start = Some(next),
            _ => return Ok(keys),
        }
    }
    Err(mantle_client::ClientError::Reply(
        "a listing past the pages its keys fill",
    ))
}
