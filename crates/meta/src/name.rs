//! The Name layer's state machine (docs/design/metadata.md §2): S3's writes to one object key
//! as commands applied at a log index, and its reads.
//!
//! Applying a command reads the key's rows, decides, and writes one batch with the entry's
//! index, so every replica that applies the same log holds the same rows. Time is part of the
//! command, as the leader proposed it, and the range's clock only moves forward: a write's
//! time is the later of the command's and just after the range's last, so versions of a key
//! never share an order.

use crate::engine::{Engine, EngineError, Write};
use crate::key::{self, NULL_VERSION, NameRow};
use crate::record::{RecordError, Version};

/// The range's clock: the last time it assigned, nanoseconds since the Unix epoch.
const CLOCK: &[u8] = &[key::LOCAL, b'c'];

/// A bucket's versioning state, as the write was made under (05 §7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    Unversioned,
    Enabled,
    Suspended,
}

/// Entity tags a precondition names, bare of quotes, or `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Match {
    Any,
    Tags(Vec<String>),
}

impl Match {
    fn names(&self, etag: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Tags(tags) => tags.iter().any(|t| t == etag),
        }
    }
}

/// A write's preconditions on the key's current version, judged as it commits (05 §2.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    pub if_match: Option<Match>,
    pub if_none_match: Option<Match>,
}

/// A version a request names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Named {
    Null,
    Order(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put(Put),
    Delete(Delete),
}

/// A new version of `key`: PutObject, CopyObject's destination, or a completed upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Put {
    pub bucket: String,
    pub key: String,
    pub versioning: Versioning,
    pub preconditions: Preconditions,
    pub at_ns: u64,
    /// Orders the version at this time instead of the commit's: a completed upload's
    /// initiation, since the upload that "started most recently" is current (05 §4.4). S3
    /// does not document a completed upload's `Last-Modified`; the version's is the time
    /// that orders it.
    pub ordered_ns: Option<u64>,
    pub version: Version,
}

/// DeleteObject (05 §7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub bucket: String,
    pub key: String,
    pub versioning: Versioning,
    pub named: Option<Named>,
    pub if_match: Option<Match>,
    pub at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A version was written; its ID.
    Put { version: String },
    /// A delete was done: whether the version it made or removed is a delete marker, and
    /// that version's ID. An unversioned delete names none.
    Deleted {
        marker: bool,
        version: Option<String>,
    },
    /// `412 PreconditionFailed`.
    PreconditionFailed,
    /// `404 NoSuchKey`: `If-Match` with no current version (05 §2.2).
    NoSuchKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Record(#[from] RecordError),
    /// A row whose key does not decode where a Name row belongs.
    #[error("a row in the Name layer does not decode")]
    Corrupt,
}

/// Applies `command` as log entry `index`. A refused command still advances the index.
pub fn apply<E: Engine>(
    engine: &mut E,
    index: u64,
    command: &Command,
) -> Result<Outcome, NameError> {
    let (outcome, writes) = match command {
        Command::Put(p) => put(engine, p)?,
        Command::Delete(d) => delete(engine, d)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn put<E: Engine>(engine: &E, p: &Put) -> Result<(Outcome, Vec<Write>), NameError> {
    let (bucket, key, versioning) = (p.bucket.as_str(), p.key.as_str(), p.versioning);
    let (preconditions, at_ns, ordered_ns, version) =
        (&p.preconditions, p.at_ns, p.ordered_ns, &p.version);
    // A delete marker is no current version to a write (05 §2.2).
    let current = current(engine, bucket, key)?.filter(|(_, v)| !v.marker);
    if let Some(none_match) = &preconditions.if_none_match
        && current
            .as_ref()
            .is_some_and(|(_, v)| none_match.names(&v.etag))
    {
        return Ok((Outcome::PreconditionFailed, Vec::new()));
    }
    if let Some(if_match) = &preconditions.if_match {
        match &current {
            None => return Ok((Outcome::NoSuchKey, Vec::new())),
            Some((_, v)) if !if_match.names(&v.etag) => {
                return Ok((Outcome::PreconditionFailed, Vec::new()));
            }
            Some(_) => {}
        }
    }
    let (time, mut writes) = tick(engine, at_ns)?;
    let when = ordered_ns.unwrap_or(time);
    let order = !when;
    let null = versioning != Versioning::Enabled;
    if null {
        writes.extend(remove_null(engine, bucket, key)?.1);
        writes.push(Write::Put(
            key::name(bucket, key, &NameRow::Null),
            order.to_be_bytes().to_vec(),
        ));
    }
    let written = Version {
        marker: false,
        null,
        modified_ns: when,
        ..version.clone()
    };
    writes.push(Write::Put(
        key::name(bucket, key, &NameRow::Version(order)),
        written.encode()?,
    ));
    Ok((
        Outcome::Put {
            version: id(null, order),
        },
        writes,
    ))
}

fn delete<E: Engine>(engine: &E, d: &Delete) -> Result<(Outcome, Vec<Write>), NameError> {
    let (bucket, key, versioning) = (d.bucket.as_str(), d.key.as_str(), d.versioning);
    let (named, if_match, at_ns) = (d.named, d.if_match.as_ref(), d.at_ns);
    if let Some(if_match) = if_match {
        // A conditional delete judges the version it removes, or the current one (05 §2.3).
        let target = match named {
            None => current(engine, bucket, key)?,
            Some(named) => version(engine, bucket, key, named)?,
        };
        match target {
            None => return Ok((Outcome::NoSuchKey, Vec::new())),
            Some((_, v)) if v.marker || !if_match.names(&v.etag) => {
                return Ok((Outcome::PreconditionFailed, Vec::new()));
            }
            Some(_) => {}
        }
    }
    match named {
        Some(Named::Null) => {
            let (removed, writes) = remove_null(engine, bucket, key)?;
            Ok((
                Outcome::Deleted {
                    marker: removed.is_some_and(|v| v.marker),
                    version: Some(NULL_VERSION.to_owned()),
                },
                writes,
            ))
        }
        Some(Named::Order(order)) => {
            let mut writes = Vec::new();
            let removed = match version(engine, bucket, key, Named::Order(order))? {
                None => None,
                Some((_, v)) => {
                    writes.push(Write::Delete(key::name(
                        bucket,
                        key,
                        &NameRow::Version(order),
                    )));
                    if v.null {
                        writes.push(Write::Delete(key::name(bucket, key, &NameRow::Null)));
                    }
                    Some(v)
                }
            };
            Ok((
                Outcome::Deleted {
                    marker: removed.as_ref().is_some_and(|v| v.marker),
                    version: Some(id(removed.is_some_and(|v| v.null), order)),
                },
                writes,
            ))
        }
        None => match versioning {
            Versioning::Unversioned => {
                let (_, writes) = remove_null(engine, bucket, key)?;
                Ok((
                    Outcome::Deleted {
                        marker: false,
                        version: None,
                    },
                    writes,
                ))
            }
            Versioning::Enabled | Versioning::Suspended => {
                // Enabled stacks a new marker; suspended replaces the null version with a null
                // marker (05 §7.3).
                let null = versioning == Versioning::Suspended;
                let (time, mut writes) = tick(engine, at_ns)?;
                let order = !time;
                if null {
                    writes.extend(remove_null(engine, bucket, key)?.1);
                    writes.push(Write::Put(
                        key::name(bucket, key, &NameRow::Null),
                        order.to_be_bytes().to_vec(),
                    ));
                }
                let marker = Version {
                    marker: true,
                    null,
                    modified_ns: time,
                    etag: String::new(),
                    size: 0,
                    checksum: None,
                    file: None,
                    owner: String::new(),
                    headers: Vec::new(),
                };
                writes.push(Write::Put(
                    key::name(bucket, key, &NameRow::Version(order)),
                    marker.encode()?,
                ));
                Ok((
                    Outcome::Deleted {
                        marker: true,
                        version: Some(id(null, order)),
                    },
                    writes,
                ))
            }
        },
    }
}

/// The key's current version: its newest, a delete marker or not.
pub fn current<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
) -> Result<Option<(u64, Version)>, NameError> {
    let from = key::name(bucket, key, &NameRow::Version(0));
    let to = key::name(bucket, key, &NameRow::Upload(Vec::new()));
    match engine.next(&from, &to)? {
        None => Ok(None),
        Some((k, v)) => match key::decode_name(&k) {
            Some((_, _, NameRow::Version(order))) => Ok(Some((order, Version::decode(&v)?))),
            _ => Err(NameError::Corrupt),
        },
    }
}

/// The version a request names.
pub fn version<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
    named: Named,
) -> Result<Option<(u64, Version)>, NameError> {
    let order = match named {
        Named::Order(order) => order,
        Named::Null => match null_order(engine, bucket, key)? {
            Some(order) => order,
            None => return Ok(None),
        },
    };
    match engine.get(&key::name(bucket, key, &NameRow::Version(order)))? {
        None => Ok(None),
        Some(v) => Ok(Some((order, Version::decode(&v)?))),
    }
}

/// The order of the key's null version.
fn null_order<E: Engine>(engine: &E, bucket: &str, key: &str) -> Result<Option<u64>, NameError> {
    match engine.get(&key::name(bucket, key, &NameRow::Null))? {
        None => Ok(None),
        Some(bytes) => Ok(Some(u64::from_be_bytes(
            bytes.try_into().map_err(|_| NameError::Corrupt)?,
        ))),
    }
}

/// Writes that remove the key's null version and its pointer, and the version removed.
fn remove_null<E: Engine>(
    engine: &E,
    bucket: &str,
    key: &str,
) -> Result<(Option<Version>, Vec<Write>), NameError> {
    match version(engine, bucket, key, Named::Null)? {
        None => Ok((None, Vec::new())),
        Some((order, v)) => Ok((
            Some(v),
            vec![
                Write::Delete(key::name(bucket, key, &NameRow::Version(order))),
                Write::Delete(key::name(bucket, key, &NameRow::Null)),
            ],
        )),
    }
}

/// The time a write takes, after the range's last, and the write that records it.
fn tick<E: Engine>(engine: &E, at_ns: u64) -> Result<(u64, Vec<Write>), NameError> {
    let last = match engine.get(CLOCK)? {
        None => 0,
        Some(bytes) => u64::from_be_bytes(bytes.try_into().map_err(|_| NameError::Corrupt)?),
    };
    let time = at_ns.max(last.saturating_add(1));
    Ok((
        time,
        vec![Write::Put(CLOCK.to_vec(), time.to_be_bytes().to_vec())],
    ))
}

/// A version's ID: "null" for the null version, else its order's.
fn id(null: bool, order: u64) -> String {
    if null {
        NULL_VERSION.to_owned()
    } else {
        key::version_id(order)
    }
}

/// What a listing step found (docs/design/s3-protocol.md §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scan {
    /// The first key at or after the position whose current version is an object.
    Found {
        key: String,
        order: u64,
        version: Version,
    },
    /// No such key is left in the bucket.
    End,
    /// The budget of rows ran out first: the next step starts at this object-key position.
    Paused(Vec<u8>),
}

/// The first key at or after `from`, in object-key byte order, whose current version is an
/// object rather than a delete marker, reading at most `budget` rows: a bucket whose keys are
/// mostly delete markers pauses the scan rather than running it unbounded.
pub fn next_current<E: Engine>(
    engine: &E,
    bucket: &str,
    from: &[u8],
    budget: usize,
) -> Result<Scan, NameError> {
    let mut position = key::position(bucket, from);
    // Where the next step starts, in object-key space.
    let mut resume = from.to_vec();
    let mut end = key::objects(bucket);
    if let Some(last) = end.last_mut() {
        // Past the bucket name's end marker: after every row of the bucket.
        *last = 1;
    }
    for _ in 0..budget {
        let Some((k, v)) = engine.next(&position, &end)? else {
            return Ok(Scan::End);
        };
        let Some((_, object, row)) = key::decode_name(&k) else {
            return Err(NameError::Corrupt);
        };
        match row {
            // The null pointer sorts before the key's versions.
            NameRow::Null => {
                position = after(&k);
                resume = object.into_bytes();
            }
            NameRow::Version(order) => {
                let version = Version::decode(&v)?;
                if !version.marker {
                    return Ok(Scan::Found {
                        key: object,
                        order,
                        version,
                    });
                }
                position = beyond_rows(bucket, &object);
                resume = after(object.as_bytes());
            }
            // Uploads with no version before them: the key has no current version.
            NameRow::Upload(_) | NameRow::Part(..) => {
                position = beyond_rows(bucket, &object);
                resume = after(object.as_bytes());
            }
        }
    }
    Ok(Scan::Paused(resume))
}

/// The smallest key after `k`.
fn after(k: &[u8]) -> Vec<u8> {
    let mut next = k.to_vec();
    next.push(0);
    next
}

/// A position after every row of `object`: its prefix and a byte past every row kind.
fn beyond_rows(bucket: &str, object: &str) -> Vec<u8> {
    let mut next = key::object(bucket, object);
    next.push(u8::MAX);
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;

    struct Range {
        engine: Model,
        index: u64,
        clock: u64,
    }

    impl Range {
        fn new() -> Self {
            Self {
                engine: Model::default(),
                index: 0,
                clock: 1_000,
            }
        }

        fn run(&mut self, command: Command) -> Outcome {
            self.index += 1;
            apply(&mut self.engine, self.index, &command).unwrap()
        }

        fn put(&mut self, key: &str, etag: &str, versioning: Versioning) -> Outcome {
            self.put_if(key, etag, versioning, Preconditions::default())
        }

        fn put_if(
            &mut self,
            key: &str,
            etag: &str,
            versioning: Versioning,
            p: Preconditions,
        ) -> Outcome {
            self.clock += 10;
            self.run(Command::Put(Put {
                bucket: "b".into(),
                key: key.into(),
                versioning,
                preconditions: p,
                at_ns: self.clock,
                ordered_ns: None,
                version: object(etag),
            }))
        }

        fn delete(&mut self, key: &str, versioning: Versioning, named: Option<Named>) -> Outcome {
            self.clock += 10;
            self.run(Command::Delete(Delete {
                bucket: "b".into(),
                key: key.into(),
                versioning,
                named,
                if_match: None,
                at_ns: self.clock,
            }))
        }

        /// Every version of `key`, newest first: (ID, delete marker, ETag).
        fn versions(&self, key: &str) -> Vec<(String, bool, String)> {
            let mut out = Vec::new();
            let mut from = key::name("b", key, &NameRow::Version(0));
            let to = key::name("b", key, &NameRow::Upload(Vec::new()));
            while let Some((k, v)) = self.engine.next(&from, &to).unwrap() {
                let Some((_, _, NameRow::Version(order))) = key::decode_name(&k) else {
                    panic!("not a version row")
                };
                let v = Version::decode(&v).unwrap();
                out.push((id(v.null, order), v.marker, v.etag));
                from = after(&k);
            }
            out
        }
    }

    fn object(etag: &str) -> Version {
        Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: etag.into(),
            size: 1,
            checksum: None,
            file: Some(1),
            owner: "o".into(),
            headers: Vec::new(),
        }
    }

    fn put_id(o: &Outcome) -> String {
        match o {
            Outcome::Put { version } => version.clone(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_unversioned_key_has_one_null_version() {
        let mut r = Range::new();
        assert_eq!(put_id(&r.put("k", "a", Versioning::Unversioned)), "null");
        r.put("k", "b", Versioning::Unversioned);
        assert_eq!(r.versions("k"), [("null".into(), false, "b".into())]);
        assert_eq!(
            r.delete("k", Versioning::Unversioned, None),
            Outcome::Deleted {
                marker: false,
                version: None
            }
        );
        assert!(r.versions("k").is_empty());
        assert_eq!(current(&r.engine, "b", "k").unwrap(), None);
    }

    /// Enabled: every put is a version, deletes stack markers, and deleting a marker by its
    /// ID undeletes (05 §7.2–§7.3; s3-tests test_versioning_stack_delete_merkers).
    #[test]
    fn enabled_versioning_keeps_every_version_and_stacks_markers() {
        let mut r = Range::new();
        let v1 = put_id(&r.put("k", "a", Versioning::Enabled));
        let v2 = put_id(&r.put("k", "b", Versioning::Enabled));
        assert!(v2 < v1, "newer versions sort first");
        for _ in 0..3 {
            let Outcome::Deleted {
                marker: true,
                version: Some(_),
            } = r.delete("k", Versioning::Enabled, None)
            else {
                panic!("no marker")
            };
        }
        let versions = r.versions("k");
        assert_eq!(versions.len(), 5);
        assert!(versions[..3].iter().all(|(_, marker, _)| *marker));
        assert!(current(&r.engine, "b", "k").unwrap().unwrap().1.marker);
        // Removing the markers by ID brings "b" back as current.
        for (marker_id, _, _) in versions[..3].iter().cloned() {
            let order = key::parse_version_id(&marker_id).unwrap();
            assert_eq!(
                r.delete("k", Versioning::Enabled, Some(Named::Order(order))),
                Outcome::Deleted {
                    marker: true,
                    version: Some(marker_id)
                }
            );
        }
        assert_eq!(current(&r.engine, "b", "k").unwrap().unwrap().1.etag, "b");
        // A version removed by ID is gone; removing it again is still a success.
        let order = key::parse_version_id(&v1).unwrap();
        r.delete("k", Versioning::Enabled, Some(Named::Order(order)));
        assert_eq!(r.versions("k"), [(v2.clone(), false, "b".into())]);
        assert_eq!(
            r.delete("k", Versioning::Enabled, Some(Named::Order(order))),
            Outcome::Deleted {
                marker: false,
                version: Some(v1)
            }
        );
    }

    /// Suspended: puts and deletes replace the null version and keep the others (05 §7.2;
    /// s3-tests test_versioning_obj_plain_null_version_overwrite_suspended).
    #[test]
    fn suspended_versioning_replaces_only_the_null_version() {
        let mut r = Range::new();
        r.put("k", "pre", Versioning::Unversioned);
        let v = put_id(&r.put("k", "on", Versioning::Enabled));
        assert_eq!(r.versions("k").len(), 2);
        assert_eq!(put_id(&r.put("k", "off", Versioning::Suspended)), "null");
        assert_eq!(
            r.versions("k"),
            [
                ("null".into(), false, "off".into()),
                (v.clone(), false, "on".into())
            ]
        );
        assert_eq!(
            r.delete("k", Versioning::Suspended, None),
            Outcome::Deleted {
                marker: true,
                version: Some("null".into())
            }
        );
        assert_eq!(
            r.versions("k"),
            [
                ("null".into(), true, String::new()),
                (v, false, "on".into())
            ]
        );
        // The plain s3-tests case: a pre-versioning object overwritten while suspended leaves
        // exactly the null version.
        let mut r = Range::new();
        r.put("p", "pre", Versioning::Unversioned);
        r.put("p", "over", Versioning::Suspended);
        assert_eq!(r.versions("p"), [("null".into(), false, "over".into())]);
        assert_eq!(
            r.delete("p", Versioning::Suspended, Some(Named::Null)),
            Outcome::Deleted {
                marker: false,
                version: Some("null".into())
            }
        );
        assert!(r.versions("p").is_empty());
    }

    /// The conditional-write matrix of s3-tests (05 §2.2), judged at commit.
    #[test]
    fn preconditions_are_judged_against_the_current_version() {
        let mut r = Range::new();
        let none = |m: Match| Preconditions {
            if_none_match: Some(m),
            ..Preconditions::default()
        };
        let some = |m: Match| Preconditions {
            if_match: Some(m),
            ..Preconditions::default()
        };
        let v = Versioning::Enabled;
        assert_eq!(r.put_if("k", "a", v, some(Match::Any)), Outcome::NoSuchKey);
        assert!(matches!(
            r.put_if("k", "a", v, none(Match::Any)),
            Outcome::Put { .. }
        ));
        assert_eq!(
            r.put_if("k", "b", v, none(Match::Any)),
            Outcome::PreconditionFailed
        );
        assert_eq!(
            r.put_if("k", "b", v, none(Match::Tags(vec!["a".into()]))),
            Outcome::PreconditionFailed
        );
        assert_eq!(
            r.put_if("k", "b", v, some(Match::Tags(vec!["x".into()]))),
            Outcome::PreconditionFailed
        );
        assert!(matches!(
            r.put_if("k", "b", v, some(Match::Tags(vec!["x".into(), "a".into()]))),
            Outcome::Put { .. }
        ));
        // A delete marker is no current version to a write.
        r.delete("k", v, None);
        assert!(matches!(
            r.put_if("k", "c", v, none(Match::Any)),
            Outcome::Put { .. }
        ));
        // A refused write still advances the index.
        let index = r.engine.applied();
        r.put_if("k", "d", v, none(Match::Any));
        assert_eq!(r.engine.applied(), index + 1);
    }

    #[test]
    fn a_completed_upload_is_ordered_by_its_initiation() {
        let mut r = Range::new();
        let early = r.clock + 1;
        r.put("k", "later", Versioning::Enabled);
        r.index += 1;
        let outcome = apply(
            &mut r.engine,
            r.index,
            &Command::Put(Put {
                bucket: "b".into(),
                key: "k".into(),
                versioning: Versioning::Enabled,
                preconditions: Preconditions::default(),
                at_ns: r.clock + 100,
                ordered_ns: Some(early),
                version: object("upload"),
            }),
        )
        .unwrap();
        let versions = r.versions("k");
        assert_eq!(versions[0].2, "later");
        assert_eq!(versions[1], (put_id(&outcome), false, "upload".into()));
    }

    #[test]
    fn listing_skips_markers_and_pauses_within_its_budget() {
        let mut r = Range::new();
        for k in ["a", "b", "c", "d"] {
            r.put(k, k, Versioning::Enabled);
        }
        r.delete("b", Versioning::Enabled, None);
        r.delete("c", Versioning::Enabled, None);
        let found = |s: Scan| match s {
            Scan::Found { key, .. } => key,
            other => panic!("{other:?}"),
        };
        assert_eq!(found(next_current(&r.engine, "b", b"", 10).unwrap()), "a");
        assert_eq!(
            found(next_current(&r.engine, "b", b"a\0", 10).unwrap()),
            "d"
        );
        // Two markers read, the budget spent: the next step starts just after "c".
        assert_eq!(
            next_current(&r.engine, "b", b"a\0", 2).unwrap(),
            Scan::Paused(b"c\0".to_vec())
        );
        assert_eq!(found(next_current(&r.engine, "b", b"c\0", 1).unwrap()), "d");
        assert_eq!(next_current(&r.engine, "b", b"e", 10).unwrap(), Scan::End);
        assert_eq!(
            next_current(&r.engine, "other", b"", 10).unwrap(),
            Scan::End
        );
    }

    /// Replaying the log from the durable point after a crash rebuilds the same rows.
    #[test]
    fn replay_after_a_crash_reaches_the_same_rows() {
        let commands: Vec<Command> = (0..20u64)
            .map(|i| {
                let versioning = [Versioning::Enabled, Versioning::Suspended][(i % 2) as usize];
                if i % 3 == 0 {
                    Command::Delete(Delete {
                        bucket: "b".into(),
                        key: format!("k{}", i % 4),
                        versioning,
                        named: None,
                        if_match: None,
                        at_ns: 100 + i,
                    })
                } else {
                    Command::Put(Put {
                        bucket: "b".into(),
                        key: format!("k{}", i % 4),
                        versioning,
                        preconditions: Preconditions::default(),
                        at_ns: 100 + i,
                        ordered_ns: None,
                        version: object(&format!("e{i}")),
                    })
                }
            })
            .collect();
        let mut whole = Model::default();
        for (i, c) in commands.iter().enumerate() {
            apply(&mut whole, i as u64 + 1, c).unwrap();
        }
        let mut crashed = Model::default();
        for (i, c) in commands.iter().enumerate() {
            apply(&mut crashed, i as u64 + 1, c).unwrap();
            if i == 7 {
                crashed.persist().unwrap();
            }
        }
        crashed.crash();
        for (i, c) in commands.iter().enumerate().skip(crashed.applied() as usize) {
            apply(&mut crashed, i as u64 + 1, c).unwrap();
        }
        let all = |m: &Model| {
            let mut rows = Vec::new();
            let mut from = Vec::new();
            while let Some((k, v)) = m.next(&from, &[0xFF]).unwrap() {
                from = after(&k);
                rows.push((k, v));
            }
            rows
        };
        assert_eq!(all(&crashed), all(&whole));
    }
}
