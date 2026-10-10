//! Completing a multipart upload (research/05 §4.4; docs/design/gateway.md §2): the parts the
//! request lists read from the Name range and checked; the object's size, ETag and checksum
//! derived from those rows, never taken from the request (audit §16.5); the object's file of
//! parts written in the File range, addressed by the parts' plaintext; and the Name range's
//! Complete, which checks the parts again against its own rows and commits the version.
//!
//! A [`Completion`] names each request and moves on with its answer, doing no I/O, as a PUT
//! does.

use std::collections::{BTreeMap, VecDeque};

use mantle_meta::file;
use mantle_meta::key;
use mantle_meta::name::{self, Listed, MAX_PART_NUMBER, MIN_PART};
use mantle_meta::record::{self, Extent, LISTING, Part, Referrer, Target, Upload};
use mantle_s3::checksum::{self, Algorithm, Checksum, Hasher};
use mantle_s3::crypto::{self, CryptoError};

/// A request's name among those in flight.
pub type Id = u64;

/// A completion's request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// The upload's row, from the Name range that holds the key.
    Upload,
    /// The upload's parts numbered after `after`, in order, at most `max` of them.
    Parts { after: u16, max: usize },
    /// A command to the File range that holds the object's file.
    File(file::Command),
    /// A command to the Name range that holds the key.
    Name(Box<name::Command>),
}

/// What a request answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Upload(Option<Upload>),
    Parts(Vec<(u16, Part)>),
    File(file::Outcome),
    Name(name::Outcome),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompleteError {
    /// `404 NoSuchUpload`.
    #[error("no such upload")]
    NoSuchUpload,
    /// `400 InvalidPart`: a listed part was never uploaded, its ETag differs, or it lacks the
    /// checksum its upload names.
    #[error("a listed part is not the upload's")]
    InvalidPart,
    /// `400 InvalidPartOrder`: parts not listed in ascending order of number, or none.
    #[error("parts not listed in ascending order")]
    InvalidPartOrder,
    /// `400 EntityTooSmall`: a part other than the last is under 5 MiB.
    #[error("a part other than the last is under 5 MiB")]
    EntityTooSmall,
    /// The upload names a checksum combination its algorithm does not have: full-object for a
    /// hash, composite for CRC-64/NVME (05 §3.3). Creating the upload refuses these, so this
    /// is a row no request could make (`500 InternalError`).
    #[error("the upload names a checksum combination its algorithm does not have")]
    ChecksumType,
    /// Parts of an upload with composite checksums not numbered 1, 2, 3, …: S3 answers
    /// `500 InternalError` for them, and mantle `400 InvalidPart` (05 §3.3).
    #[error("composite checksums need parts numbered from 1 without a gap")]
    NotConsecutive,
    /// A listed part was uploaded again while the completion ran: the request is retried.
    #[error("a listed part changed while the upload completed")]
    Stale,
    #[error("the File range answered {0:?}")]
    File(file::Outcome),
    /// The Name range refused the write: a precondition, the bucket, a lock, a deadline.
    #[error("the Name range answered {0:?}")]
    Name(name::Outcome),
    #[error("an answer of another kind than its request")]
    Mismatch,
    #[error("an answer to no request in flight: {0}")]
    Unknown(Id),
    #[error("the completion is over")]
    Over,
    #[error("a length past what the completion can address")]
    Overflow,
    #[error(transparent)]
    Crypto(#[from] CryptoError),
}

/// What a completion committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    pub version: String,
    /// Bare of quotes.
    pub etag: String,
    pub size: u64,
    pub checksum: Option<record::Checksum>,
}

/// What a request in flight asked.
#[derive(Debug, Clone, Copy)]
enum Asked {
    Upload,
    Parts,
    File,
    Name,
}

pub struct Completion {
    /// The Complete to send, which the completion fills with what it derives.
    complete: name::Complete,
    /// The parts the request lists, each with the ETag it sent, in its order.
    listed: Vec<(u16, String)>,
    /// The object's file of parts, a file ID never used before, and how long the File range
    /// holds it for the Name range.
    file: u128,
    handover_ns: u64,
    upload: Option<Upload>,
    /// The upload's parts read so far, by number, and the number the next page starts after.
    rows: BTreeMap<u16, Part>,
    after: u16,
    /// How many of the parts listed are numbered at most `after`: those a page has passed.
    /// Both lists ascend, so one walk along each matches them, rather than a search of the
    /// list for every row, which for 10,000 parts was some 10^8 comparisons.
    passed: usize,
    ready: VecDeque<(Id, Request)>,
    asked: BTreeMap<Id, Asked>,
    next_id: Id,
    outcome: Option<Result<Completed, CompleteError>>,
}

impl Completion {
    /// Completes `complete`'s upload with the parts `listed`, as the request lists them, into
    /// file `file`, held `handover_ns` for the Name range, at the caller's time `now_ns`. What
    /// `complete` says of the object, its parts' files, ETag, size, checksum, file, ID and
    /// deadline, is replaced by what the completion derives from the upload's rows.
    pub fn new(
        complete: name::Complete,
        listed: Vec<(u16, String)>,
        file: u128,
        handover_ns: u64,
        now_ns: u64,
    ) -> Result<Self, CompleteError> {
        // Refused before anything is asked, as the Name range would refuse it (05 §4.4). An
        // upload ID the Name range never made names no upload, and what a request sends goes
        // no further than this, so the Complete sent is within the largest entry a range
        // takes (`mantle_meta::wire::largest_entry_bytes`; audit §16.5).
        if key::parse_version_id(&complete.upload).is_none() {
            return Err(CompleteError::NoSuchUpload);
        }
        if listed
            .iter()
            .any(|(n, _)| !(1..=MAX_PART_NUMBER).contains(n))
        {
            return Err(CompleteError::InvalidPart);
        }
        let ascending = listed.array_windows::<2>().all(|[a, b]| a.0 < b.0);
        if listed.is_empty() || !ascending {
            return Err(CompleteError::InvalidPartOrder);
        }
        // A completion with no file, of parts all empty, or a retry once the upload is gone, is
        // named by the file's ID, which no file then takes, and held to the same handover from
        // the caller's time now, so a copy of it is recognised (docs/design/metadata.md §2).
        let complete = name::Complete {
            listing: listing(&listed)?,
            id: file,
            deadline_ns: now_ns
                .checked_add(handover_ns)
                .ok_or(CompleteError::Overflow)?,
            ..complete
        };
        let mut completion = Self {
            complete,
            listed,
            file,
            handover_ns,
            upload: None,
            rows: BTreeMap::new(),
            after: 0,
            passed: 0,
            ready: VecDeque::new(),
            asked: BTreeMap::new(),
            next_id: 0,
            outcome: None,
        };
        completion.ask(Asked::Upload, Request::Upload)?;
        Ok(completion)
    }

    /// The next request to send, each once; `None` while every request is in flight or the
    /// completion is over.
    pub fn poll(&mut self) -> Option<(Id, Request)> {
        if self.outcome.is_some() {
            return None;
        }
        self.ready.pop_front()
    }

    /// Takes the answer to request `id`, and moves on.
    pub fn answer(&mut self, id: Id, answer: Answer) -> Result<(), CompleteError> {
        if self.outcome.is_some() {
            return Err(CompleteError::Over);
        }
        let result = self
            .asked
            .remove(&id)
            .ok_or(CompleteError::Unknown(id))
            .and_then(|asked| self.answered(asked, answer));
        if let Err(e) = &result {
            self.outcome = Some(Err(e.clone()));
            self.ready.clear();
            self.asked.clear();
        }
        result
    }

    /// How the completion ended, once it has.
    pub fn outcome(&self) -> Option<&Result<Completed, CompleteError>> {
        self.outcome.as_ref()
    }

    fn ask(&mut self, asked: Asked, request: Request) -> Result<(), CompleteError> {
        let id = self.next_id;
        self.next_id = id.checked_add(1).ok_or(CompleteError::Overflow)?;
        self.asked.insert(id, asked);
        self.ready.push_back((id, request));
        Ok(())
    }

    fn answered(&mut self, asked: Asked, answer: Answer) -> Result<(), CompleteError> {
        match (asked, answer) {
            (Asked::Upload, Answer::Upload(Some(upload))) => {
                self.upload = Some(upload);
                self.next_page()
            }
            // No upload: a completion that committed before, whose retry finds the version it
            // made by its ETag, or none (05 §4.4).
            (Asked::Upload, Answer::Upload(None)) => self.retry(),
            (Asked::Parts, Answer::Parts(page)) => self.page(page),
            (Asked::File, Answer::File(file::Outcome::Written { deadline_ns })) => {
                self.commit(Some(deadline_ns))
            }
            (Asked::File, Answer::File(outcome)) => Err(CompleteError::File(outcome)),
            (Asked::Name, Answer::Name(outcome)) => self.done(outcome),
            _ => Err(CompleteError::Mismatch),
        }
    }

    /// Asks for the next page of parts: as many as the parts listed after the last one read,
    /// which the parts not listed between them may push to later pages.
    fn next_page(&mut self) -> Result<(), CompleteError> {
        let left = self.listed.get(self.passed..).map_or(0, <[_]>::len);
        self.ask(
            Asked::Parts,
            Request::Parts {
                after: self.after,
                max: left,
            },
        )
    }

    /// A page of parts: kept where listed, and another page asked for until every listed part
    /// is read or the upload has no more.
    fn page(&mut self, page: Vec<(u16, Part)>) -> Result<(), CompleteError> {
        let last_listed = self.listed.last().map_or(0, |(n, _)| *n);
        let mut ended = page.is_empty();
        for (number, part) in page {
            if number <= self.after {
                return Err(CompleteError::Mismatch);
            }
            self.after = number;
            while self
                .listed
                .get(self.passed)
                .is_some_and(|(n, _)| *n < number)
            {
                self.passed = self.passed.checked_add(1).ok_or(CompleteError::Overflow)?;
            }
            if self
                .listed
                .get(self.passed)
                .is_some_and(|(n, _)| *n == number)
            {
                self.rows.insert(number, part);
                self.passed = self.passed.checked_add(1).ok_or(CompleteError::Overflow)?;
            }
            if number >= last_listed {
                ended = true;
            }
        }
        if ended {
            self.combine()
        } else {
            self.next_page()
        }
    }

    /// Checks the listed parts against their rows, derives the object from them, and writes
    /// its file of parts.
    fn combine(&mut self) -> Result<(), CompleteError> {
        let upload = self.upload.as_ref().ok_or(CompleteError::Mismatch)?;
        if let Some((_, false)) = upload.checksum {
            let consecutive = (1u16..)
                .zip(&self.listed)
                .all(|(n, (number, _))| n == *number);
            if !consecutive {
                return Err(CompleteError::NotConsecutive);
            }
        }
        let last = self.listed.len().saturating_sub(1);
        // The list is the client's, already parsed and held; a reservation the allocator
        // refuses is a completion too large to address, not an abort.
        let mut parts = Vec::new();
        parts
            .try_reserve_exact(self.listed.len())
            .map_err(|_| CompleteError::Overflow)?;
        for (i, (number, etag)) in self.listed.iter().enumerate() {
            let part = self.rows.get(number).ok_or(CompleteError::InvalidPart)?;
            if part.etag != *etag {
                return Err(CompleteError::InvalidPart);
            }
            if i < last && part.size < MIN_PART {
                return Err(CompleteError::EntityTooSmall);
            }
            parts.push((*number, part));
        }
        let size = parts
            .iter()
            .try_fold(0u64, |sum, (_, p)| sum.checked_add(p.size));
        let size = size.ok_or(CompleteError::Overflow)?;
        let etag = multipart_etag(parts.iter().map(|(_, p)| p.etag.as_str()))?;
        let checksum = match upload.checksum {
            None => None,
            Some((code, full)) => Some(combined(code, full, &parts)?),
        };
        self.complete.parts = parts
            .iter()
            .map(|(number, p)| Listed {
                number: *number,
                etag: p.etag.clone(),
                file: p.file,
            })
            .collect();
        self.complete.etag = etag;
        self.complete.size = size;
        self.complete.checksum = checksum;
        // The object's file is addressed by its parts' plaintext; an empty part holds no byte
        // of it, and the Name range gives it back rather than let the file adopt it.
        let extents: Vec<Extent> = parts
            .iter()
            .filter(|(_, p)| p.size > 0)
            .map(|(_, p)| Extent {
                length: p.size,
                target: Target::File(p.file),
            })
            .collect();
        if extents.is_empty() {
            return self.commit(None);
        }
        let write = file::Command::Write {
            file: self.file,
            extents,
            referrer: Referrer {
                bucket: self.complete.bucket.clone(),
                incarnation: self.complete.incarnation,
                key: self.complete.key.clone(),
            },
            key: None,
            handover_ns: self.handover_ns,
            // It names files, not blocks: no block's deadline bounds it.
            blocks_deadline_ns: u64::MAX,
            at_ns: 0,
        };
        self.ask(Asked::File, Request::File(write))
    }

    /// A retry of a completion whose upload is gone: the Name range answers with the version
    /// it made if the parts listed, their numbers and ETags as sent, are those it was made from
    /// (`name::Complete::listing`), or `NoSuchUpload`.
    fn retry(&mut self) -> Result<(), CompleteError> {
        self.complete.etag = multipart_etag(self.listed.iter().map(|(_, e)| e.as_str()))?;
        self.complete.parts = Vec::new();
        self.complete.file = None;
        self.ask(
            Asked::Name,
            Request::Name(Box::new(name::Command::Complete(self.complete.clone()))),
        )
    }

    fn commit(&mut self, deadline_ns: Option<u64>) -> Result<(), CompleteError> {
        self.complete.file = deadline_ns.map(|_| self.file);
        if let Some(deadline_ns) = deadline_ns {
            self.complete.deadline_ns = deadline_ns;
        }
        self.ask(
            Asked::Name,
            Request::Name(Box::new(name::Command::Complete(self.complete.clone()))),
        )
    }

    fn done(&mut self, outcome: name::Outcome) -> Result<(), CompleteError> {
        let version = match outcome {
            name::Outcome::Put { version } => version,
            // A retry: the object is the version the first completion made.
            name::Outcome::Completed {
                version,
                size,
                checksum,
            } => {
                self.complete.size = size;
                self.complete.checksum = checksum;
                version
            }
            name::Outcome::NoSuchUpload => return Err(CompleteError::NoSuchUpload),
            name::Outcome::InvalidPart => return Err(CompleteError::InvalidPart),
            name::Outcome::InvalidPartOrder => return Err(CompleteError::InvalidPartOrder),
            name::Outcome::EntityTooSmall => return Err(CompleteError::EntityTooSmall),
            name::Outcome::Stale => return Err(CompleteError::Stale),
            outcome => return Err(CompleteError::Name(outcome)),
        };
        self.outcome = Some(Ok(Completed {
            version,
            etag: self.complete.etag.clone(),
            size: self.complete.size,
            checksum: self.complete.checksum.clone(),
        }));
        Ok(())
    }
}

/// The SHA-256 of the parts a request lists, in its order: each part's number, its ETag's
/// length and its ETag as sent, so that two lists share a digest only if they are the same
/// list (FIPS 180-4's collision resistance).
fn listing(listed: &[(u16, String)]) -> Result<[u8; LISTING], CompleteError> {
    let mut h = crypto::Digest::new(&crypto::SHA256)?;
    for (number, etag) in listed {
        let len = u32::try_from(etag.len()).map_err(|_| CompleteError::Overflow)?;
        h.update(&number.to_be_bytes())?;
        h.update(&len.to_be_bytes())?;
        h.update(etag.as_bytes())?;
    }
    Ok(h.finish_array()?)
}

/// The multipart ETag of parts with these ETags, each the hex MD5 a part's PUT made
/// (05 §4.5); a part ETag that is not one is `InvalidPart`.
fn multipart_etag<'a>(etags: impl Iterator<Item = &'a str>) -> Result<String, CompleteError> {
    let mut digests = Vec::new();
    for etag in etags {
        digests.push(md5_of_hex(etag).ok_or(CompleteError::InvalidPart)?);
    }
    Ok(checksum::multipart_etag(&digests)?)
}

fn md5_of_hex(etag: &str) -> Option<[u8; 16]> {
    let hex = etag.as_bytes();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    let (pairs, _) = hex.as_chunks::<2>();
    for (byte, [high, low]) in out.iter_mut().zip(pairs) {
        *byte = hex_digit(*high)?
            .checked_mul(16)?
            .checked_add(hex_digit(*low)?)?;
    }
    Some(out)
}

/// One hexadecimal digit's value, either case; decoded by hand because `from_str_radix`
/// panics on a radix outside 2..=36 and the lint cannot see the radix is a constant.
fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => c.checked_sub(b'0'),
        b'a'..=b'f' => c.checked_sub(b'a')?.checked_add(10),
        b'A'..=b'F' => c.checked_sub(b'A')?.checked_add(10),
        _ => None,
    }
}

/// The object's checksum from its parts', in the algorithm and form the upload named: the
/// full-object CRC combined from each part's CRC and length, or the composite hash of the
/// parts' values (05 §3.4). A part without a value in that algorithm is `InvalidPart`.
fn combined(
    code: u8,
    full: bool,
    parts: &[(u16, &Part)],
) -> Result<record::Checksum, CompleteError> {
    let algorithm = Algorithm::from_code(code).ok_or(CompleteError::ChecksumType)?;
    let combines = if full {
        algorithm.full_object()
    } else {
        algorithm.composite()
    };
    if !combines {
        return Err(CompleteError::ChecksumType);
    }
    let values: Vec<(Checksum, u64)> = parts
        .iter()
        .map(|(_, p)| {
            p.checksum
                .as_ref()
                .map(|bytes| {
                    (
                        Checksum {
                            algorithm,
                            bytes: bytes.clone(),
                        },
                        p.size,
                    )
                })
                .ok_or(CompleteError::InvalidPart)
        })
        .collect::<Result<_, _>>()?;
    if full {
        let whole = checksum::full_object(&values).ok_or(CompleteError::InvalidPart)?;
        return Ok(record::Checksum {
            algorithm: code,
            parts: 0,
            value: whole.bytes,
        });
    }
    let mut h = Hasher::new(algorithm)?;
    for (value, _) in &values {
        h.update(&value.bytes)?;
    }
    Ok(record::Checksum {
        algorithm: code,
        parts: u16::try_from(values.len()).map_err(|_| CompleteError::Overflow)?,
        value: h.finish()?.bytes,
    })
}
