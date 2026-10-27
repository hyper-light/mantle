//! The large objects of `mantle bench transfer` (plan step 9; research/76): objects of a stated
//! size put through the four ways the two doors take one, in turn: the native door's upload in
//! runs, as `mantle cp` puts a file; the AWS CLI from its standard input; the native door's
//! explicit multipart upload; and boto3's. A case puts a stated count of them or, with none stated,
//! puts until the cell refuses one as out of space through each door. Every object acknowledged is
//! read back through both doors and through another node than it was put through; each refused as
//! out of space is read through both doors, which must find no such key, and listed by neither,
//! which must list every object acknowledged.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use mantle_protocol::object::Status;

use super::cell::Cell;
use super::generate::{GenError, Generator};
use super::native::{self, Came};
use super::stock::{Boto, Cli};
use super::{BUCKET, Natives, Oracle, Tally, TransferError, elapsed};

/// The four ways a large object goes in, two through each door, taken in turn.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Way {
    /// The native door's upload in runs the node cuts (`mantle cp`'s).
    Runs,
    /// The AWS CLI from its standard input, in the parts it cuts.
    Cli,
    /// The native door's explicit multipart upload.
    Parts,
    /// boto3's explicit multipart upload.
    Boto,
}

pub const WAYS: [Way; 4] = [Way::Runs, Way::Cli, Way::Parts, Way::Boto];

impl Way {
    pub fn name(self) -> &'static str {
        match self {
            Self::Runs => "native",
            Self::Cli => "cli",
            Self::Parts => "native-parts",
            Self::Boto => "boto-parts",
        }
    }

    /// The door it goes through: 0 the native one, 1 the S3 listener.
    fn door(self) -> usize {
        match self {
            Self::Runs | Self::Parts => 0,
            Self::Cli | Self::Boto => 1,
        }
    }

    /// Object `i`'s way, and its tally's place.
    fn of(i: u64) -> (Self, usize) {
        let at = usize::try_from(i.checked_rem(4).unwrap_or(0)).unwrap_or(0);
        (WAYS.get(at).copied().unwrap_or(Self::Runs), at)
    }
}

/// What a large case came to.
pub struct Large {
    /// Each way's tally, in `WAYS`' order.
    pub tallies: [Tally; 4],
    /// The object each door refused as out of space, the native door's and the S3 listener's.
    pub full: [Option<u64>; 2],
    /// Every object refused as out of space read as no such key through both doors and was listed
    /// by neither, and every object acknowledged was listed by both.
    pub unlisted: bool,
}

/// Object `i`'s key.
pub(super) fn key(i: u64) -> String {
    format!("l{i}")
}

/// The case of `count` objects of `length` bytes on `cell`, or with no count, of objects until
/// the cell refuses one as out of space through each door: at most as many as its founders'
/// volumes of `volume_bytes` hold of one, each node holding a copy of every block (f34), and one
/// past that through each door.
pub(super) fn large(
    cell: &Cell,
    natives: &mut Natives,
    dir: &Path,
    (seed, length, count): (u64, u64, Option<u64>),
    volume_bytes: u64,
) -> Result<(Large, Oracle, u64), TransferError> {
    let budget = match count {
        Some(count) => count,
        None => volume_bytes
            .checked_div(length.max(1))
            .and_then(|held| held.checked_add(2))
            .ok_or(GenError::Offset)?,
    };
    let listeners = cell.s3();
    let third = listeners
        .get(2)
        .cloned()
        .ok_or_else(|| TransferError::Io(std::io::Error::other("no third listener")))?;
    let mut boto =
        Boto::start(dir, cell.out(), (third.0, &third.1), BUCKET).map_err(TransferError::Stock)?;
    let mut cli = Cli::new(dir, cell.out(), (third.0, &third.1)).map_err(TransferError::Stock)?;
    natives.create(BUCKET)?;

    let generator = Generator::new(seed)?;
    let mut tallies = [Tally::new(), Tally::new(), Tally::new(), Tally::new()];
    let mut oracle = Oracle::new(budget)?;
    let mut full: [Option<u64>; 2] = [None, None];
    let part = native::part_size(length);
    for i in 0..budget {
        let (way, at) = Way::of(i);
        // Filling, a door that refused an object as out of space is asked no more.
        if count.is_none() && full.get(way.door()).is_some_and(Option::is_some) {
            if full.iter().all(Option::is_some) {
                break;
            }
            continue;
        }
        let name = key(i);
        let object = generator.object(&name)?;
        let began = Instant::now();
        // Asked once: a full cell's volumes hold a put for their cleaners' verdicts, so its first
        // answer is the typed one (research/75), and a SlowDown is counted as refused.
        let came = {
            match way {
                Way::Runs | Way::Parts => {
                    let settings = natives.settings(0)?;
                    natives.block(async move {
                        let generator = Generator::new(seed)?;
                        let object = generator.object(&key(i))?;
                        let mut client = mantle_client::Client::open(settings).await?;
                        let at = (BUCKET, key(i));
                        Ok(if way == Way::Runs {
                            native::put(&mut client, (at.0, &at.1), &object, length).await?
                        } else {
                            native::put_parts(&mut client, (at.0, &at.1), &object, length).await?
                        })
                    })?
                }
                Way::Cli => cli
                    .put((BUCKET, &name), &object, length)
                    .map_err(TransferError::Stock)?,
                Way::Boto => boto
                    .mput(&name, &object, (length, part))
                    .map_err(TransferError::Stock)?,
            }
        };
        let took = elapsed(began);
        if let Some(tally) = tallies.get_mut(at) {
            if came == Came::Refused(Status::InsufficientStorage) {
                tally.full = tally.full.saturating_add(1);
                if let Some(door) = full.get_mut(way.door()) {
                    door.get_or_insert(i);
                }
            } else if tally.put_came(length, came, took) {
                oracle.set(i);
            }
        }
    }

    // Every acknowledged object read back natively through the second node, then through the
    // third node's listener by the CLI.
    for i in (0..budget).filter(|i| oracle.has(*i)) {
        let (_, at) = Way::of(i);
        let settings = natives.settings(1)?;
        let began = Instant::now();
        let came = natives.block(async move {
            let generator = Generator::new(seed)?;
            let object = generator.object(&key(i))?;
            let mut client = mantle_client::Client::open(settings).await?;
            Ok(native::get(&mut client, (BUCKET, &key(i)), &object, length).await?)
        })?;
        if let Some(tally) = tallies.get_mut(at) {
            tally.native_came(came, elapsed(began));
        }
        let object = generator.object(&key(i))?;
        let began = Instant::now();
        let came = cli
            .get((BUCKET, &key(i)), &object, length)
            .map_err(TransferError::Stock)?;
        if let Some(tally) = tallies.get_mut(at) {
            tally.http_came(came, elapsed(began));
        }
    }

    // Nothing of a refused object is readable or listed, and every acknowledged one is listed.
    let refused: Vec<u64> = full.iter().flatten().copied().collect();
    let mut unlisted = true;
    for &i in &refused {
        let settings = natives.settings(1)?;
        let came = natives.block(async move {
            let generator = Generator::new(seed)?;
            let object = generator.object(&key(i))?;
            let mut client = mantle_client::Client::open(settings).await?;
            Ok(native::get(&mut client, (BUCKET, &key(i)), &object, length).await?)
        })?;
        let object = generator.object(&key(i))?;
        let over_http = cli
            .get((BUCKET, &key(i)), &object, length)
            .map_err(TransferError::Stock)?;
        let absent = Came::Refused(Status::NoSuchKey);
        unlisted &= came == absent && over_http == absent;
    }
    let settings = natives.settings(1)?;
    let pages = budget
        .checked_div(u64::try_from(mantle_s3::list::MAX_KEYS).unwrap_or(u64::MAX))
        .and_then(|p| p.checked_add(1))
        .ok_or(GenError::Offset)?;
    let natively: BTreeSet<String> = natives
        .block(async move {
            let mut client = mantle_client::Client::open(settings).await?;
            Ok(native::list(&mut client, BUCKET, pages).await?)
        })?
        .into_iter()
        .collect();
    let over_http: BTreeSet<String> = cli
        .list(BUCKET)
        .map_err(TransferError::Stock)?
        .into_iter()
        .collect();
    let acknowledged: BTreeSet<String> = (0..budget).filter(|i| oracle.has(*i)).map(key).collect();
    unlisted &= natively == acknowledged && over_http == acknowledged;
    Ok((
        Large {
            tallies,
            full,
            unlisted,
        },
        oracle,
        budget,
    ))
}

impl Large {
    /// Every object read back exact through both doors, and, filling, each door refused one as
    /// out of space with nothing of it readable or listed; with a count, none refused.
    pub fn exact(&self, filling: bool) -> bool {
        let tallies = self.tallies.iter().all(Tally::exact);
        let full = if filling {
            self.full.iter().all(Option::is_some)
        } else {
            self.full.iter().all(Option::is_none)
        };
        tallies && full && self.unlisted
    }
}
