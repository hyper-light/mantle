//! The Bucket layer's state machine (docs/design/metadata.md §1–§2): each bucket's row, its
//! owner's count and listing, and the steps of creating and deleting a bucket that the Bucket
//! range takes. Between those steps, the Name ranges move the bucket's gates.
//!
//! Every create and delete is an attempt, numbered by the range's clock when it began. A
//! request that finds another attempt in progress takes it over with a new number, and the
//! steps of the attempt left behind are refused from then on, here and at the gates.

use crate::clock;
use crate::engine::{Engine, Write};
use crate::error::MetaError;
use crate::key;
use crate::record::{Bucket, BucketState, Owned, Owner, Versioning};

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
    /// The bucket is being deleted: the Name ranges close its gates next and are read.
    Deleting {
        incarnation: u64,
        attempt: u64,
    },
    Restored,
    Deleted,
    Forgotten,
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
}

/// Applies `command` as log entry `index`.
pub fn apply<E: Engine>(
    engine: &mut E,
    index: u64,
    command: &Command,
) -> Result<Outcome, MetaError> {
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
    };
    engine.apply(index, &writes)?;
    Ok(outcome)
}

fn create<E: Engine>(engine: &E, c: &Create) -> Result<(Outcome, Vec<Write>), MetaError> {
    if let Some(row) = read(engine, &c.bucket)? {
        let outcome = match row.state {
            BucketState::Creating if row.owner == c.owner && row.location == c.location => {
                // The same request again, after its coordinator stopped answering.
                let (attempt, clock) = clock::tick(engine, c.at_ns)?;
                let row = Bucket { attempt, ..row };
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
        versioning: Versioning::Unversioned,
        state: BucketState::Creating,
        attempt: time,
    };
    let count = Owner {
        buckets: buckets.checked_add(1).ok_or(MetaError::Corrupt)?,
    };
    let writes = vec![
        clock,
        Write::Put(key::bucket(&c.bucket), row.encode()?),
        Write::Put(key::owner(&c.owner), count.encode()),
    ];
    Ok((
        Outcome::Creating {
            incarnation: time,
            attempt: time,
        },
        writes,
    ))
}

fn version<E: Engine>(
    engine: &E,
    bucket: &str,
    incarnation: u64,
    versioning: Versioning,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if versioning == Versioning::Unversioned {
        return Ok((Outcome::Invalid, Vec::new()));
    }
    let row = match read(engine, bucket)? {
        Some(row) if row.created_ns == incarnation => row,
        _ => return Ok((Outcome::NoSuchBucket, Vec::new())),
    };
    let outcome = match row.state {
        BucketState::Active => Outcome::Versioned,
        BucketState::Deleting => Outcome::OperationAborted,
        BucketState::Creating | BucketState::Deleted => Outcome::NoSuchBucket,
    };
    if outcome != Outcome::Versioned {
        return Ok((outcome, Vec::new()));
    }
    let row = Bucket { versioning, ..row };
    Ok((
        Outcome::Versioned,
        vec![Write::Put(key::bucket(bucket), row.encode()?)],
    ))
}

/// Starts a delete of a bucket in state `from`, or takes over a delete in progress.
fn begin<E: Engine>(
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
        ..row
    };
    Ok((
        Outcome::Deleting {
            incarnation,
            attempt,
        },
        vec![clock, Write::Put(key::bucket(bucket), row.encode()?)],
    ))
}

/// Moves the bucket from `from` to `to` for `attempt`, with the owner's rows that change with
/// it: its listing holds the buckets that are active or being deleted, and its count every
/// bucket not yet deleted.
fn step<E: Engine>(
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
    let mut writes = Vec::with_capacity(3);
    match to {
        BucketState::Active => {
            let listed = Owned {
                created_ns: row.created_ns,
                location: row.location.clone(),
            };
            writes.push(Write::Put(owned, listed.encode()?));
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

fn forget<E: Engine>(
    engine: &E,
    bucket: &str,
    attempt: u64,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    match read(engine, bucket)? {
        None => Ok((Outcome::Forgotten, Vec::new())),
        Some(row) if row.state == BucketState::Deleted && row.attempt == attempt => {
            Ok((Outcome::Forgotten, vec![Write::Delete(key::bucket(bucket))]))
        }
        Some(_) => Ok((Outcome::Conflict, Vec::new())),
    }
}

/// The write that counts one bucket fewer for `owner`, removing its row at none.
fn uncount<E: Engine>(engine: &E, owner: &str) -> Result<Write, MetaError> {
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
pub fn read<E: Engine>(engine: &E, bucket: &str) -> Result<Option<Bucket>, MetaError> {
    Ok(engine
        .get(&key::bucket(bucket))?
        .map(|b| Bucket::decode(&b))
        .transpose()?)
}

/// How many buckets `owner` has, counting those being created or deleted.
pub fn owned_count<E: Engine>(engine: &E, owner: &str) -> Result<u32, MetaError> {
    match engine.get(&key::owner(owner))? {
        None => Ok(0),
        Some(bytes) => Ok(Owner::decode(&bytes)?.buckets),
    }
}

/// `owner`'s active buckets and those being deleted, by name after `after`, at most `max`:
/// a page of ListBuckets (05 §10.4).
pub fn owned<E: Engine>(
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
}
