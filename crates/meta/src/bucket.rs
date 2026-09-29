//! The Bucket layer's state machine (docs/design/metadata.md §1–§2): each bucket's row, its
//! owner's count and listing, and the steps of creating and deleting a bucket that the Bucket
//! range takes. Between those steps, the Name ranges move the bucket's gates.
//!
//! Every create and delete is an attempt, numbered by the range's clock when it began. A
//! request that finds another attempt in progress takes it over with a new number, and the
//! steps of the attempt left behind are refused from then on, here and at the gates.

use crate::clock;
use crate::engine::{Rows, Write};
use crate::error::MetaError;
use crate::key;
use crate::record::{Bucket, BucketState, DefaultRetention, Lock, Owned, Owner, Versioning};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Create(Create),
    /// Marks the bucket active once every Name range its keys fall in has opened its gate:
    /// CreateBucket's last step.
    Activate {
        bucket: String,
        attempt: u64,
    },
    /// PutBucketVersioning (05 §7.1).
    Version {
        bucket: String,
        incarnation: u64,
        versioning: Versioning,
    },
    /// PutObjectLockConfiguration: turns Object Lock on, with a default retention or none
    /// (18 §1.1).
    Lock {
        bucket: String,
        incarnation: u64,
        default: Option<DefaultRetention>,
    },
    /// Marks the bucket as being deleted, or takes over a delete in progress: DeleteBucket's
    /// first step.
    BeginDelete {
        bucket: String,
        at_ns: u64,
    },
    /// Takes over a create left unfinished to delete what it made, as the collector does.
    Abandon {
        bucket: String,
        at_ns: u64,
    },
    /// A Name range held a version and the gates have reopened: the bucket stays.
    Restore {
        bucket: String,
        attempt: u64,
    },
    /// No Name range held a version: the bucket is deleted.
    Delete {
        bucket: String,
        attempt: u64,
    },
    /// Every gate of the deleted bucket is gone: the name is free.
    Forget {
        bucket: String,
        attempt: u64,
    },
    /// The attempt is alive: its driver says so while it works through the Name ranges, so
    /// the collector does not take it over (docs/design/metadata.md §2).
    Progress {
        bucket: String,
        attempt: u64,
        at_ns: u64,
    },
}

/// CreateBucket's first step: records the bucket as being created, or takes over a create
/// in progress by the same owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub bucket: String,
    pub owner: String,
    /// The location constraint; empty for the default.
    pub location: String,
    pub at_ns: u64,
    /// Buckets the owner may have.
    pub quota: u32,
    /// `x-amz-bucket-object-lock-enabled: true`: the bucket is made with Object Lock on and
    /// versioning enabled, as Object Lock needs (18 §2.1).
    pub lock: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The bucket is being created: the Name ranges open its gates next.
    Creating {
        incarnation: u64,
        attempt: u64,
    },
    Activated,
    Versioned,
    LockConfigured,
    /// The bucket is being deleted: the Name ranges close its gates next and are read.
    Deleting {
        incarnation: u64,
        attempt: u64,
    },
    Restored,
    Deleted,
    Forgotten,
    /// The attempt's progress was recorded.
    Progressed,
    /// `409 BucketAlreadyOwnedByYou` (05 §10.3).
    AlreadyOwnedByYou,
    /// `409 BucketAlreadyExists` (05 §10.3).
    AlreadyExists,
    /// `400 TooManyBuckets` (05 §10.4).
    TooManyBuckets,
    /// `409 OperationAborted`: the bucket is being created, or is deleted and not yet
    /// forgotten (05 §11).
    OperationAborted,
    /// `404 NoSuchBucket`.
    NoSuchBucket,
    /// The bucket is not where the step expects, or a later attempt has taken over: the
    /// coordinator reads it again.
    Conflict,
    /// Versioning set back to unversioned, which S3 never allows once enabled (05 §7.1).
    Invalid,
    /// `409 InvalidBucketState`, "An Object Lock configuration is present on this bucket, so
    /// the versioning state cannot be changed." (18 §5).
    VersioningLocked,
    /// `409 InvalidBucketState`, "Versioning must be 'Enabled' on the bucket to apply a Object
    /// Lock configuration" (18 §5).
    VersioningNotEnabled,
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Rows>(engine: &mut E, index: u64, command: &Command) -> Result<Outcome, MetaError> {
    use BucketState::{Active, Creating, Deleted, Deleting};
    let (outcome, writes) = match command {
        Command::Create(c) => create(engine, c)?,
        Command::Activate { bucket, attempt } => step(
            engine,
            bucket,
            *attempt,
            Creating,
            Active,
            Outcome::Activated,
        )?,
        Command::Version {
            bucket,
            incarnation,
            versioning,
        } => version(engine, bucket, *incarnation, *versioning)?,
        Command::Lock {
            bucket,
            incarnation,
            default,
        } => lock(engine, bucket, *incarnation, *default)?,
        Command::BeginDelete { bucket, at_ns } => begin(engine, bucket, *at_ns, Active)?,
        Command::Abandon { bucket, at_ns } => begin(engine, bucket, *at_ns, Creating)?,
        Command::Restore { bucket, attempt } => step(
            engine,
            bucket,
            *attempt,
            Deleting,
            Active,
            Outcome::Restored,
        )?,
        Command::Delete { bucket, attempt } => step(
            engine,
            bucket,
            *attempt,
            Deleting,
            Deleted,
            Outcome::Deleted,
        )?,
        Command::Forget { bucket, attempt } => forget(engine, bucket, *attempt)?,
        Command::Progress {
            bucket,
            attempt,
            at_ns,
        } => progress(engine, bucket, *attempt, *at_ns)?,
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn create<E: Rows>(engine: &E, c: &Create) -> Result<(Outcome, Vec<Write>), MetaError> {
    if let Some(row) = read(engine, &c.bucket)? {
        let outcome = match row.state {
            BucketState::Creating
                if row.owner == c.owner
                    && row.location == c.location
                    && row.lock.is_some() == c.lock =>
            {
                // The same request again, after its coordinator stopped answering.
                let (attempt, clock) = clock::tick(engine, c.at_ns)?;
                let row = Bucket {
                    attempt,
                    progress_ns: attempt,
                    ..row
                };
                let writes = vec![clock, Write::Put(key::bucket(&c.bucket), row.encode()?)];
                return Ok((
                    Outcome::Creating {
                        incarnation: row.created_ns,
                        attempt,
                    },
                    writes,
                ));
            }
            BucketState::Creating | BucketState::Deleted => Outcome::OperationAborted,
            BucketState::Active | BucketState::Deleting if row.owner == c.owner => {
                Outcome::AlreadyOwnedByYou
            }
            BucketState::Active | BucketState::Deleting => Outcome::AlreadyExists,
        };
        return Ok((outcome, Vec::new()));
    }
    let buckets = owned_count(engine, &c.owner)?;
    if buckets >= c.quota {
        return Ok((Outcome::TooManyBuckets, Vec::new()));
    }
    let (time, clock) = clock::tick(engine, c.at_ns)?;
    let row = Bucket {
        owner: c.owner.clone(),
        created_ns: time,
        location: c.location.clone(),
        versioning: if c.lock {
            Versioning::Enabled
        } else {
            Versioning::Unversioned
        },
        state: BucketState::Creating,
        attempt: time,
        progress_ns: time,
        lock: c.lock.then(Lock::default),
    };
    let count = Owner {
        buckets: buckets.checked_add(1).ok_or(MetaError::Corrupt)?,
    };
    let writes = vec![
        clock,
        Write::Put(key::bucket(&c.bucket), row.encode()?),
        Write::Put(key::owner(&c.owner), count.encode()),
        Write::Put(key::attempt(&c.bucket), Vec::new()),
    ];
    Ok((
        Outcome::Creating {
            incarnation: time,
            attempt: time,
        },
        writes,
    ))
}

fn version<E: Rows>(
    engine: &E,
    bucket: &str,
    incarnation: u64,
    versioning: Versioning,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if versioning == Versioning::Unversioned {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    let row = match active(engine, bucket, incarnation)? {
        Ok(row) => row,
        Err(outcome) => return Ok((outcome, Vec::new())),
    };
    // "After you enable Object Lock on a bucket, you can't disable Object Lock or suspend
    // versioning for that bucket" (18 §2.1).
    if row.lock.is_some() && versioning != Versioning::Enabled {
        return Ok((Outcome::VersioningLocked, Vec::new()));
    }
    let row = Bucket { versioning, ..row };
    Ok((
        Outcome::Versioned,
        vec![Write::Put(key::bucket(bucket), row.encode()?)],
    ))
}

/// Sets the bucket's Object Lock configuration, which "works only in buckets that have S3
/// Versioning enabled" (18 §2.1).
fn lock<E: Rows>(
    engine: &E,
    bucket: &str,
    incarnation: u64,
    default: Option<DefaultRetention>,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let row = match active(engine, bucket, incarnation)? {
        Ok(row) => row,
        Err(outcome) => return Ok((outcome, Vec::new())),
    };
    if row.versioning != Versioning::Enabled {
        return Ok((Outcome::VersioningNotEnabled, Vec::new()));
    }
    let row = Bucket {
        lock: Some(Lock { default }),
        ..row
    };
    Ok((
        Outcome::LockConfigured,
        vec![Write::Put(key::bucket(bucket), row.encode()?)],
    ))
}

/// The row of `bucket`'s `incarnation` while it is active, or the answer to a setting's change
/// otherwise.
fn active<E: Rows>(
    engine: &E,
    bucket: &str,
    incarnation: u64,
) -> Result<Result<Bucket, Outcome>, MetaError> {
    let row = match read(engine, bucket)? {
        Some(row) if row.created_ns == incarnation => row,
        _ => return Ok(Err(Outcome::NoSuchBucket)),
    };
    Ok(match row.state {
        BucketState::Active => Ok(row),
        BucketState::Deleting => Err(Outcome::OperationAborted),
        BucketState::Creating | BucketState::Deleted => Err(Outcome::NoSuchBucket),
    })
}

/// Starts a delete of a bucket in state `from`, or takes over a delete in progress.
fn begin<E: Rows>(
    engine: &E,
    bucket: &str,
    at_ns: u64,
    from: BucketState,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(row) = read(engine, bucket)? else {
        return Ok((Outcome::NoSuchBucket, Vec::new()));
    };
    if row.state != from && row.state != BucketState::Deleting {
        let outcome = match row.state {
            BucketState::Creating => Outcome::OperationAborted,
            BucketState::Active => Outcome::Conflict,
            BucketState::Deleting | BucketState::Deleted => Outcome::NoSuchBucket,
        };
        return Ok((outcome, Vec::new()));
    }
    let (attempt, clock) = clock::tick(engine, at_ns)?;
    let incarnation = row.created_ns;
    let row = Bucket {
        state: BucketState::Deleting,
        attempt,
        progress_ns: attempt,
        ..row
    };
    Ok((
        Outcome::Deleting {
            incarnation,
            attempt,
        },
        vec![
            clock,
            Write::Put(key::bucket(bucket), row.encode()?),
            Write::Put(key::attempt(bucket), Vec::new()),
        ],
    ))
}

/// Moves the bucket from `from` to `to` for `attempt`, with the owner's rows that change with
/// it: its listing holds the buckets that are active or being deleted, and its count every
/// bucket not yet deleted.
fn step<E: Rows>(
    engine: &E,
    bucket: &str,
    attempt: u64,
    from: BucketState,
    to: BucketState,
    done: Outcome,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(row) = read(engine, bucket)? else {
        return Ok((Outcome::Conflict, Vec::new()));
    };
    if row.attempt != attempt {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    if row.state == to {
        // A retry finds the bucket where it moved it.
        return Ok((done, Vec::new()));
    }
    if row.state != from {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let owned = key::owned(&row.owner, bucket);
    let mut writes = Vec::with_capacity(4);
    match to {
        BucketState::Active => {
            let listed = Owned {
                created_ns: row.created_ns,
                location: row.location.clone(),
            };
            writes.push(Write::Put(owned, listed.encode()?));
            // The attempt is over: nothing is left for the collector.
            writes.push(Write::Delete(key::attempt(bucket)));
        }
        BucketState::Deleted => {
            writes.push(Write::Delete(owned));
            writes.push(uncount(engine, &row.owner)?);
        }
        BucketState::Creating | BucketState::Deleting => {}
    }
    let row = Bucket { state: to, ..row };
    writes.push(Write::Put(key::bucket(bucket), row.encode()?));
    Ok((done, writes))
}

fn forget<E: Rows>(
    engine: &E,
    bucket: &str,
    attempt: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    match read(engine, bucket)? {
        None => Ok((Outcome::Forgotten, Vec::new())),
        Some(row) if row.state == BucketState::Deleted && row.attempt == attempt => Ok((
            Outcome::Forgotten,
            vec![
                Write::Delete(key::bucket(bucket)),
                Write::Delete(key::attempt(bucket)),
            ],
        )),
        Some(_) => Ok((Outcome::Conflict, Vec::new())),
    }
}

/// Records that `attempt` is alive, if it still owns the bucket's row and has not finished.
fn progress<E: Rows>(
    engine: &E,
    bucket: &str,
    attempt: u64,
    at_ns: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(row) = read(engine, bucket)? else {
        return Ok((Outcome::Conflict, Vec::new()));
    };
    if row.attempt != attempt || row.state == BucketState::Active {
        return Ok((Outcome::Conflict, Vec::new()));
    }
    let (progress_ns, clock) = clock::tick(engine, at_ns)?;
    let row = Bucket { progress_ns, ..row };
    Ok((
        Outcome::Progressed,
        vec![clock, Write::Put(key::bucket(bucket), row.encode()?)],
    ))
}

/// The buckets whose create or delete is in progress, after `after` in name order, at most
/// `max`, with their rows: what the collector looks through for attempts to take over.
pub fn attempts<E: Rows>(
    engine: &E,
    after: Option<&str>,
    max: usize,
) -> Result<Vec<(String, Bucket)>, MetaError> {
    let (mut from, to) = key::attempts_after(after);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, _)) = engine.next(&from, &to)? else {
            break;
        };
        let name = key::decode_attempt(&k).ok_or(MetaError::Corrupt)?;
        let row = read(engine, &name)?.ok_or(MetaError::Corrupt)?;
        out.push((name, row));
        from = k;
        from.push(0);
    }
    Ok(out)
}

/// The write that counts one bucket fewer for `owner`, removing its row at none.
fn uncount<E: Rows>(engine: &E, owner: &str) -> Result<Write, MetaError> {
    let buckets = owned_count(engine, owner)?
        .checked_sub(1)
        .ok_or(MetaError::Corrupt)?;
    let row = key::owner(owner);
    Ok(if buckets == 0 {
        Write::Delete(row)
    } else {
        Write::Put(row, Owner { buckets }.encode())
    })
}

/// A bucket's row.
pub fn read<E: Rows>(engine: &E, bucket: &str) -> Result<Option<Bucket>, MetaError> {
    Ok(engine
        .get(&key::bucket(bucket))?
        .map(|b| Bucket::decode(&b))
        .transpose()?)
}

/// How many buckets `owner` has, counting those being created or deleted.
pub fn owned_count<E: Rows>(engine: &E, owner: &str) -> Result<u32, MetaError> {
    match engine.get(&key::owner(owner))? {
        None => Ok(0),
        Some(bytes) => Ok(Owner::decode(&bytes)?.buckets),
    }
}

/// `owner`'s active buckets and those being deleted, by name after `after`, at most `max`:
/// a page of ListBuckets (05 §10.4).
pub fn owned<E: Rows>(
    engine: &E,
    owner: &str,
    after: Option<&str>,
    max: usize,
) -> Result<Vec<(String, Owned)>, MetaError> {
    let (mut from, to) = key::owned_rows(owner, after);
    let mut out = Vec::new();
    while out.len() < max {
        let Some((k, v)) = engine.next(&from, &to)? else {
            break;
        };
        let (_, bucket) = key::decode_owned(&k).ok_or(MetaError::Corrupt)?;
        out.push((bucket, Owned::decode(&v)?));
        from = k;
        from.push(0);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;

    #[derive(Default)]
    struct Range {
        engine: Model,
        index: u64,
    }

    impl Range {
        fn run(&mut self, command: Command) -> Outcome {
            self.index += 1;
            apply(&mut self.engine, self.index, &command).unwrap()
        }

        fn create(&mut self, bucket: &str, owner: &str, at_ns: u64, quota: u32) -> Outcome {
            self.run(Command::Create(Create {
                bucket: bucket.into(),
                owner: owner.into(),
                location: String::new(),
                at_ns,
                quota,
                lock: false,
            }))
        }

        fn state(&self, bucket: &str) -> Option<BucketState> {
            read(&self.engine, bucket).unwrap().map(|b| b.state)
        }
    }

    /// The incarnation and attempt of a create or delete that began.
    fn begun(outcome: Outcome) -> (u64, u64) {
        match outcome {
            Outcome::Creating {
                incarnation,
                attempt,
            }
            | Outcome::Deleting {
                incarnation,
                attempt,
            } => (incarnation, attempt),
            other => panic!("{other:?}"),
        }
    }

    fn activate(bucket: &str, attempt: u64) -> Command {
        Command::Activate {
            bucket: bucket.into(),
            attempt,
        }
    }

    #[test]
    fn a_bucket_is_listed_once_active_and_counted_from_its_creation() {
        let mut r = Range::default();
        let (incarnation, attempt) = begun(r.create("b", "o", 100, 10));
        assert_eq!(incarnation, 100);
        assert!(owned(&r.engine, "o", None, 10).unwrap().is_empty());
        assert_eq!(owned_count(&r.engine, "o").unwrap(), 1);
        // The same create again takes over under a later attempt; the one left behind is
        // refused.
        let (again, taken) = begun(r.create("b", "o", 50, 10));
        assert_eq!(again, incarnation);
        assert!(taken > attempt);
        assert_eq!(r.run(activate("b", attempt)), Outcome::Conflict);
        assert_eq!(r.run(activate("b", taken)), Outcome::Activated);
        assert_eq!(r.run(activate("b", taken)), Outcome::Activated);
        let listed = Owned {
            created_ns: 100,
            location: String::new(),
        };
        assert_eq!(
            owned(&r.engine, "o", None, 10).unwrap(),
            [("b".to_owned(), listed)]
        );
        assert_eq!(owned_count(&r.engine, "o").unwrap(), 1);
        assert_eq!(r.create("b", "o", 200, 10), Outcome::AlreadyOwnedByYou);
        assert_eq!(r.create("b", "p", 200, 10), Outcome::AlreadyExists);
        // A create in progress is another owner's to wait for.
        begun(r.create("c", "o", 300, 10));
        assert_eq!(r.create("c", "p", 300, 10), Outcome::OperationAborted);
    }

    #[test]
    fn an_owner_creates_at_most_its_quota_and_lists_in_pages() {
        let mut r = Range::default();
        for (i, name) in ["a", "b", "c"].into_iter().enumerate() {
            let (_, attempt) = begun(r.create(name, "o", i as u64, 3));
            assert_eq!(r.run(activate(name, attempt)), Outcome::Activated);
        }
        assert_eq!(r.create("d", "o", 9, 3), Outcome::TooManyBuckets);
        begun(r.create("d", "p", 9, 3));
        let names =
            |page: Vec<(String, Owned)>| page.into_iter().map(|(n, _)| n).collect::<Vec<_>>();
        assert_eq!(names(owned(&r.engine, "o", None, 2).unwrap()), ["a", "b"]);
        assert_eq!(names(owned(&r.engine, "o", Some("b"), 2).unwrap()), ["c"]);
        assert!(owned(&r.engine, "p", None, 2).unwrap().is_empty());
    }

    #[test]
    fn a_delete_is_restored_or_finished_and_then_forgotten() {
        let mut r = Range::default();
        let (incarnation, a) = begun(r.create("b", "o", 100, 10));
        r.run(activate("b", a));
        let version = |incarnation, versioning| Command::Version {
            bucket: "b".into(),
            incarnation,
            versioning,
        };
        assert_eq!(
            r.run(version(incarnation, Versioning::Enabled)),
            Outcome::Versioned
        );
        assert_eq!(
            r.run(version(incarnation, Versioning::Unversioned)),
            Outcome::Invalid
        );
        assert_eq!(
            r.run(version(incarnation + 1, Versioning::Suspended)),
            Outcome::NoSuchBucket
        );
        assert_eq!(
            read(&r.engine, "b").unwrap().unwrap().versioning,
            Versioning::Enabled
        );

        let begin = |at_ns| Command::BeginDelete {
            bucket: "b".into(),
            at_ns,
        };
        // A delete that finds a version restores the bucket.
        let (_, d1) = begun(r.run(begin(200)));
        assert_eq!(
            r.run(version(incarnation, Versioning::Suspended)),
            Outcome::OperationAborted
        );
        assert_eq!(r.create("b", "o", 210, 10), Outcome::AlreadyOwnedByYou);
        let restore = |attempt| Command::Restore {
            bucket: "b".into(),
            attempt,
        };
        assert_eq!(r.run(restore(d1)), Outcome::Restored);
        assert_eq!(r.run(restore(d1)), Outcome::Restored);
        assert_eq!(r.state("b"), Some(BucketState::Active));

        // A delete taken over: the first attempt's steps are refused.
        let (_, d2) = begun(r.run(begin(300)));
        let (_, d3) = begun(r.run(begin(250)));
        assert!(d3 > d2);
        let delete = |attempt| Command::Delete {
            bucket: "b".into(),
            attempt,
        };
        assert_eq!(r.run(delete(d2)), Outcome::Conflict);
        assert_eq!(r.run(restore(d2)), Outcome::Conflict);
        assert_eq!(r.run(delete(d3)), Outcome::Deleted);
        assert_eq!(r.run(delete(d3)), Outcome::Deleted);
        assert!(owned(&r.engine, "o", None, 10).unwrap().is_empty());
        assert_eq!(owned_count(&r.engine, "o").unwrap(), 0);
        assert_eq!(r.engine.get(&key::owner("o")).unwrap(), None);

        // Deleted but not forgotten: gone to requests, and the name not yet free.
        assert_eq!(r.run(begin(400)), Outcome::NoSuchBucket);
        assert_eq!(r.create("b", "p", 400, 10), Outcome::OperationAborted);
        let forget = |attempt| Command::Forget {
            bucket: "b".into(),
            attempt,
        };
        assert_eq!(r.run(forget(d2)), Outcome::Conflict);
        assert_eq!(r.run(forget(d3)), Outcome::Forgotten);
        assert_eq!(r.run(forget(d3)), Outcome::Forgotten);
        assert_eq!(read(&r.engine, "b").unwrap(), None);
        // A new incarnation comes after every attempt before it, whatever its proposal says.
        let (again, _) = begun(r.create("b", "p", 1, 10));
        assert!(again > d3);
    }

    #[test]
    fn an_unfinished_create_is_abandoned_and_its_steps_refused() {
        let mut r = Range::default();
        let (_, a) = begun(r.create("b", "o", 100, 10));
        let begin = Command::BeginDelete {
            bucket: "b".into(),
            at_ns: 150,
        };
        assert_eq!(r.run(begin), Outcome::OperationAborted);
        let abandon = |at_ns| Command::Abandon {
            bucket: "b".into(),
            at_ns,
        };
        let (_, d) = begun(r.run(abandon(200)));
        assert_eq!(r.run(activate("b", a)), Outcome::Conflict);
        let delete = Command::Delete {
            bucket: "b".into(),
            attempt: d,
        };
        assert_eq!(r.run(delete), Outcome::Deleted);
        assert_eq!(owned_count(&r.engine, "o").unwrap(), 0);
        assert_eq!(r.run(abandon(300)), Outcome::NoSuchBucket);
        // An active bucket is never abandoned.
        let (_, c) = begun(r.create("c", "o", 400, 10));
        r.run(activate("c", c));
        let active = Command::Abandon {
            bucket: "c".into(),
            at_ns: 500,
        };
        assert_eq!(r.run(active), Outcome::Conflict);
        assert_eq!(r.state("c"), Some(BucketState::Active));
    }

    /// Object Lock needs versioning enabled and keeps it so (18 §2.1, §5).
    #[test]
    fn object_lock_holds_versioning_enabled() {
        use crate::record::{DefaultRetention, Period, RetentionMode};
        let mut r = Range::default();
        let version = |bucket: &str, incarnation, versioning| Command::Version {
            bucket: bucket.into(),
            incarnation,
            versioning,
        };
        let lock = |bucket: &str, incarnation, default| Command::Lock {
            bucket: bucket.into(),
            incarnation,
            default,
        };
        let default = Some(DefaultRetention {
            mode: RetentionMode::Governance,
            period: Period::Years(1),
        });
        // Configured on a bucket whose versioning is not enabled: refused, then accepted once
        // it is.
        let (b, a) = begun(r.create("b", "o", 100, 10));
        r.run(activate("b", a));
        assert_eq!(r.run(lock("b", b, None)), Outcome::VersioningNotEnabled);
        assert_eq!(
            r.run(version("b", b, Versioning::Suspended)),
            Outcome::Versioned
        );
        assert_eq!(r.run(lock("b", b, None)), Outcome::VersioningNotEnabled);
        assert_eq!(
            r.run(version("b", b, Versioning::Enabled)),
            Outcome::Versioned
        );
        assert_eq!(r.run(lock("b", b, default)), Outcome::LockConfigured);
        let row = read(&r.engine, "b").unwrap().unwrap();
        assert_eq!(row.lock, Some(Lock { default }));
        // Removing the default keeps Object Lock on; versioning cannot be suspended.
        assert_eq!(r.run(lock("b", b, None)), Outcome::LockConfigured);
        assert_eq!(
            read(&r.engine, "b").unwrap().unwrap().lock,
            Some(Lock::default())
        );
        assert_eq!(
            r.run(version("b", b, Versioning::Suspended)),
            Outcome::VersioningLocked
        );
        assert_eq!(
            r.run(version("b", b, Versioning::Enabled)),
            Outcome::Versioned
        );
        assert_eq!(r.run(lock("b", b + 1, None)), Outcome::NoSuchBucket);

        // Made with Object Lock: versioning enabled from the start.
        let created = r.run(Command::Create(Create {
            bucket: "c".into(),
            owner: "o".into(),
            location: String::new(),
            at_ns: 200,
            quota: 10,
            lock: true,
        }));
        let (c, a) = begun(created);
        let row = read(&r.engine, "c").unwrap().unwrap();
        assert_eq!(
            (row.versioning, row.lock),
            (Versioning::Enabled, Some(Lock::default()))
        );
        // A retry of that create is the same request; one without Object Lock is not.
        assert!(matches!(
            r.run(Command::Create(Create {
                bucket: "c".into(),
                owner: "o".into(),
                location: String::new(),
                at_ns: 210,
                quota: 10,
                lock: false,
            })),
            Outcome::OperationAborted
        ));
        r.run(activate("c", a));
        assert_eq!(
            r.run(version("c", c, Versioning::Suspended)),
            Outcome::VersioningLocked
        );
    }

    fn names(r: &Range) -> Vec<String> {
        attempts(&r.engine, None, 100)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    fn progress(bucket: &str, attempt: u64, at_ns: u64) -> Command {
        Command::Progress {
            bucket: bucket.into(),
            attempt,
            at_ns,
        }
    }

    /// Every create and delete in progress is in the index, stamped with its latest progress,
    /// until it ends; the collector reads nothing else.
    #[test]
    fn attempts_in_progress_are_indexed_and_stamped_until_they_end() {
        let mut r = Range::default();
        let (_, a) = begun(r.create("b", "o", 100, 10));
        let (_, c) = begun(r.create("c", "o", 110, 10));
        assert_eq!(names(&r), ["b", "c"]);
        assert_eq!(read(&r.engine, "b").unwrap().unwrap().progress_ns, 100);
        // Progress moves the stamp forward, in the range's clock; a stale attempt's does not.
        assert_eq!(r.run(progress("b", a, 150)), Outcome::Progressed);
        assert_eq!(read(&r.engine, "b").unwrap().unwrap().progress_ns, 150);
        assert_eq!(r.run(progress("b", a, 120)), Outcome::Progressed);
        assert_eq!(read(&r.engine, "b").unwrap().unwrap().progress_ns, 151);
        assert_eq!(r.run(progress("b", a + 1, 200)), Outcome::Conflict);
        assert_eq!(r.run(progress("nope", 1, 200)), Outcome::Conflict);
        // Paging resumes after the last bucket read.
        let first = attempts(&r.engine, None, 1).unwrap();
        assert_eq!(first.len(), 1);
        let rest = attempts(&r.engine, Some(&first[0].0), 10).unwrap();
        assert_eq!(
            rest.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["c"]
        );
        // An active bucket has no attempt left: it leaves the index, and takes no progress.
        assert_eq!(r.run(activate("b", a)), Outcome::Activated);
        assert_eq!(names(&r), ["c"]);
        assert_eq!(r.run(progress("b", a, 300)), Outcome::Conflict);
        // A delete joins the index, stamped at its start, and stays through the cleanup that
        // follows a deletion, until the name is forgotten.
        let (_, d) = begun(r.run(Command::BeginDelete {
            bucket: "b".into(),
            at_ns: 400,
        }));
        assert_eq!(read(&r.engine, "b").unwrap().unwrap().progress_ns, d);
        assert_eq!(names(&r), ["b", "c"]);
        let delete = Command::Delete {
            bucket: "b".into(),
            attempt: d,
        };
        assert_eq!(r.run(delete), Outcome::Deleted);
        assert_eq!(names(&r), ["b", "c"]);
        assert_eq!(r.run(progress("b", d, 500)), Outcome::Progressed);
        let forget = Command::Forget {
            bucket: "b".into(),
            attempt: d,
        };
        assert_eq!(r.run(forget), Outcome::Forgotten);
        assert_eq!(names(&r), ["c"]);
        // A create abandoned by the collector stays in the index as a delete.
        let (_, e) = begun(r.run(Command::Abandon {
            bucket: "c".into(),
            at_ns: 600,
        }));
        assert!(e > c);
        assert_eq!(names(&r), ["c"]);
        let restore = Command::Restore {
            bucket: "c".into(),
            attempt: e,
        };
        assert_eq!(r.run(restore), Outcome::Restored);
        assert!(names(&r).is_empty());
    }
}
