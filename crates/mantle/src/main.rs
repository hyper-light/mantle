//! The mantle command line.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bench_ec;
mod bench_gateway;
mod bench_hash;
mod bench_http;
mod bench_log;
mod bench_meta;
mod bench_s3;
mod client;
mod disk;
mod display;
mod drive;
mod durability;
mod files;
mod init;
mod journal;
mod transfer;

#[derive(Parser)]
#[command(
    name = "mantle",
    version,
    about = "Durable object storage, from one laptop to a fleet"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// The client directory `mantle init client` made, whose client the object commands ask the
    /// cell as; `MANTLE_CLIENT` where none is given.
    #[arg(long, global = true)]
    client: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect the storage mantle would keep data on.
    Disk {
        #[command(subcommand)]
        command: DiskCommand,
    },
    /// Measure mantle's storage on a device.
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
    /// Run a node: its range replicas and their transport, as the file at CONFIG states them
    /// (mantle_node::process). It says where it listens and how its range moves on standard
    /// output, and takes where its peers listen on standard input; it stops when that closes.
    Node {
        /// The node's configuration: one `key = value` a line.
        config: PathBuf,
    },
    /// Make a cell from the machines it starts with, its founders: the cell's authority and root
    /// key, its founding ranges, and each founder's identity and directory with its configuration
    /// of the operator's statements (docs/design/node.md §1.5). One founder makes a cell of one in
    /// OUT, measured now; several make OUT/authority, to keep, and OUT/node-ID for each founder, to
    /// move to its host.
    #[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
    Init {
        #[command(subcommand)]
        what: Option<InitCommand>,
        /// An empty or missing directory to make the cell in.
        #[arg(required = true)]
        out: Option<PathBuf>,
        /// A founder: quic=ADDR,plane=ADDR,s3=ADDR,name=HOST,domain=NAME,volume=DIR:SIZE,
        /// memory=SIZE (s3 its S3 endpoint's address, its QUIC address unless stated; a name, a
        /// DNS name its S3 clients dial, and a volume repeatable; sizes in bytes or with a K, M, G
        /// or T suffix). Repeatable, one a founder.
        #[arg(long = "node")]
        nodes: Vec<String>,
        /// A machines file: the cell's statements (max_nodes, max_volumes, durability.target,
        /// region), then a [node] block of each founder's (quic, plane, s3, name, domain, volume =
        /// DIR SIZE, memory). Its founders come before the --node ones.
        #[arg(long)]
        founders: Option<PathBuf>,
        /// The cell's bounds on its volume table: the most nodes and volumes it names.
        #[arg(long)]
        max_nodes: Option<u32>,
        #[arg(long)]
        max_volumes: Option<u32>,
        /// A block's annual chance of loss at most: S3's eleven nines, 1e-11, unless stated.
        #[arg(long)]
        target: Option<f64>,
        /// The region the cell's S3 endpoint serves and its clients sign for: S3's default,
        /// us-east-1, unless stated.
        #[arg(long)]
        region: Option<String>,
    },
    /// Run a node from what its operator states alone (mantle_node::process::serve_stated): its
    /// devices measured, or their kept profiles taken while they match, and every other value
    /// derived from those and the host's memory, cores and page, each said with its basis, a
    /// `basis` line each, before it runs as `mantle node` does.
    Serve {
        /// The node's configuration: the operator's keys only, one `key = value` a line.
        config: PathBuf,
    },
    /// Make a bucket, s3://BUCKET, asked of the cell over its native protocol as the client of
    /// --client (each object command does).
    Mb { bucket: String },
    /// Remove a bucket, s3://BUCKET, once it holds nothing.
    Rb { bucket: String },
    /// List the buckets, or a bucket's keys under a prefix a level of `/` at a time:
    /// [s3://BUCKET[/PREFIX]].
    Ls { at: Option<String> },
    /// Copy an object's bytes from a file or standard input (-) to s3://BUCKET/KEY, or back to a
    /// file or standard output (-). A file is the whole object or untouched. A copy into the cell
    /// is resumed by running it again, from where the cell holds it.
    Cp {
        from: String,
        to: String,
        /// The bytes standard input holds, where known: each run is then sent as it is read,
        /// rather than read whole first.
        #[arg(long)]
        size: Option<u64>,
    },
    /// Remove an object, s3://BUCKET/KEY.
    Rm { at: String },
    /// An object's facts, s3://BUCKET/KEY: its size, ETag, date, version and checksum.
    Head { at: String },
    /// A multipart upload's steps, each one of S3's operations.
    Multipart {
        #[command(subcommand)]
        step: MultipartCommand,
    },
    /// Estimate how likely a block is to be lost in a year under each scheme mantle can store
    /// it in, and the scheme that meets the durability target at least cost.
    Durability {
        /// Failure domains a block's chunks spread over, one to each: racks, or whatever level
        /// the deployment names.
        #[arg(long)]
        domains: usize,
        /// Each chunk's device's annual failure rate, in percent: 2 to 4 is common for disks in
        /// the field, and flash runs from 0.07 to 1.2 by model (docs/design/durability.md).
        #[arg(long)]
        afr: f64,
        /// Hours from a chunk's loss to its rebuild, detection included.
        #[arg(long)]
        repair_hours: f64,
        /// Events that destroy a share of the nodes at once, as PER_YEAR:PERCENT; 1:1 is a
        /// yearly power loss after which 1% of the nodes do not come back. Repeatable.
        #[arg(long = "burst", value_parser = parse_burst)]
        bursts: Vec<(f64, f64)>,
        /// Zones a block's chunks spread over, evenly, and each zone's losses per year, as
        /// ZONES:PER_YEAR.
        #[arg(long, value_parser = parse_zones)]
        zones: Option<(usize, f64)>,
        /// The highest annual probability of losing a block: S3's design of 99.999999999%
        /// durability over a year by default.
        #[arg(long, default_value_t = 1e-11)]
        target: f64,
    },
}

#[derive(Subcommand)]
enum InitCommand {
    /// A client of the cell's native protocol: its certificate under the cell's authority, a
    /// client's alone (`client-ID`, acting for the cell's account), its owner-only key, and the
    /// directory NAME of its identity and config, which the object commands read (--client NAME).
    Client {
        /// An empty or missing directory to make the client in.
        name: PathBuf,
        /// The directory `mantle init` made the cell in, which holds its authority.
        #[arg(long)]
        cell: PathBuf,
    },
}

#[derive(Subcommand)]
enum MultipartCommand {
    /// CreateMultipartUpload of s3://BUCKET/KEY; says the upload's ID.
    Create { at: String },
    /// UploadPart: part NUMBER of UPLOAD from a file, or from standard input (-) with its
    /// --length; says the number and the part's ETag.
    Put {
        at: String,
        upload: String,
        number: u16,
        from: String,
        #[arg(long)]
        length: Option<u64>,
    },
    /// CompleteMultipartUpload of the parts listed, each NUMBER=ETAG, in ascending order.
    Complete {
        at: String,
        upload: String,
        parts: Vec<String>,
    },
    /// AbortMultipartUpload.
    Abort { at: String, upload: String },
    /// ListParts of UPLOAD.
    Parts { at: String, upload: String },
    /// ListMultipartUploads of s3://BUCKET.
    Uploads { bucket: String },
}

#[derive(Subcommand)]
enum BenchCommand {
    /// Measure the device under PATH, then the chunk store's puts and reads on it across
    /// chunk sizes and concurrency, in a scratch volume of at most 4 GiB and a tenth of the
    /// free space (removed afterwards). Each point runs ten to thirty rounds of about three
    /// steps each: from eight minutes to about half an hour for every point at the defaults.
    Chunk {
        /// A directory on the device to measure.
        path: PathBuf,
        /// Seconds each put and each read measurement runs.
        #[arg(long, default_value_t = 1.0)]
        seconds: f64,
        /// Chunk sizes to measure, comma-separated, in bytes or with a K or M suffix
        /// (powers of 1024): 4K,64K,1M,8M by default.
        #[arg(long, value_delimiter = ',', value_parser = parse_size)]
        sizes: Vec<usize>,
        /// Clients to measure, comma-separated: 1,4,16,64 by default. Closed loop, each has one
        /// request in flight; open loop, at most one.
        #[arg(long, value_delimiter = ',')]
        workers: Vec<usize>,
        /// Requests a second in all, arriving open loop from a Poisson process, with latency
        /// from each request's intended start; without it, each client issues its next request
        /// when its last is answered.
        #[arg(long)]
        rate: Option<f64>,
        /// Rounds each point runs at most, from ten to 120: thirty by default, enough for two
        /// states to each carry an interval when the smaller holds a fifth of the rounds.
        #[arg(long, default_value_t = mantle_disk::rounds::Policy::STANDARD.max)]
        rounds: usize,
        /// Leave out the measurement of the device itself.
        #[arg(long)]
        skip_device: bool,
        /// Bytes of each of the scratch volume's segments, with a K or M suffix: a volume's
        /// 256 MiB by default. Smaller segments give the store as many to keep as a larger
        /// device would have.
        #[arg(long, value_parser = parse_size)]
        segment_size: Option<usize>,
    },
    /// Measure the device under PATH, then the Raft log's appends across entry sizes and
    /// replicas appending at once, in scratch files (removed afterwards).
    Log {
        /// A directory on the device to measure.
        path: PathBuf,
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 1.0)]
        seconds: f64,
        /// Entry sizes, comma-separated, in bytes or with a K or M suffix: 128,1K,16K by
        /// default.
        #[arg(long, value_delimiter = ',', value_parser = parse_size)]
        sizes: Vec<usize>,
        /// Replicas appending at once, comma-separated: 1,4,16,64,256 by default.
        #[arg(long, value_delimiter = ',')]
        replicas: Vec<usize>,
        /// Leave out the measurement of the device itself.
        #[arg(long)]
        skip_device: bool,
    },
    /// Measure erasure coding on one core: encoding, and rebuilding after losing one data
    /// chunk and as many as each code tolerates.
    Ec {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
        /// Chunk sizes to measure, comma-separated, in bytes or with a K or M suffix: 64K,1M,8M
        /// by default.
        #[arg(long, value_delimiter = ',', value_parser = parse_size)]
        sizes: Vec<usize>,
    },
    /// Measure the object path's PUT and GET on one core against a cell in memory, with the
    /// round trips each waits through: three copies and RS(6,3), across object sizes.
    Gateway {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
        /// Object sizes, comma-separated, in bytes or with a K or M suffix: 64K,8M,64M by
        /// default.
        #[arg(long, value_delimiter = ',', value_parser = parse_size)]
        sizes: Vec<usize>,
        /// Clients PUTting at once on the one core, comma-separated, each a record rather than
        /// a thread: a row for each count.
        #[arg(long, value_delimiter = ',')]
        clients: Vec<usize>,
        /// PUTs a second in all, arriving open loop from a Poisson process, with latency from
        /// each PUT's intended start; without it, each client starts its next PUT when its last
        /// is stored.
        #[arg(long)]
        rate: Option<f64>,
    },
    /// Measure the S3 gateway's cryptography and framing on one core: each checksum algorithm
    /// across buffer sizes, verifying a request's signature, decoding signed chunks and form
    /// bodies, checking a form's policy, and sealing data at rest.
    Hash {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
        /// Buffer sizes to measure, comma-separated, in bytes or with a K or M suffix:
        /// 8K,64K,1M,8M by default.
        #[arg(long, value_delimiter = ',', value_parser = parse_size)]
        sizes: Vec<usize>,
    },
    /// Measure the S3 listener's HTTP/1.1 codec on one core: the request heads stock clients
    /// send parsed, a mebibyte's body decoded framed by length and chunked, and a response head
    /// written.
    Http {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
    },
    /// Measure the S3 listener's own cost over loopback, plain and TLS 1.3, its host a stub on
    /// the door: round trips of a request, and the AWS CLI's 8 MiB part's gets and puts, plain and
    /// aws-chunked.
    S3 {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 2.0)]
        seconds: f64,
    },
    /// Measure what applying the metadata layers' heaviest commands costs on one core: a
    /// multipart completion of up to 10,000 parts, an entry of many commands from one session,
    /// a create or delete attempt learning up to 10,000 ranges, and the sweep's check of a
    /// page of blocks against a large file.
    Meta {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
    },
    /// Put objects through a cell of three this program makes and runs under DIR, each read back
    /// through both doors and another node and checked against the bytes its seed and name make
    /// (plan step 9): a case each small set and each large size, each on a cell of its own. Exits
    /// non-zero when any check is not exact. Never on a shared runner: it fills the volumes it
    /// states.
    Transfer {
        /// The run's directory: each case's cell in a directory of its own under it, which must
        /// not exist yet.
        dir: PathBuf,
        /// The seed every object's bytes and the small sets' sizes come from.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// The small sets' sizes, comma-separated, a case each.
        #[arg(long, value_delimiter = ',')]
        small: Vec<u64>,
        /// The large objects' sizes, comma-separated, in bytes or with a K, M, G or T suffix
        /// (powers of 1024), a case each: objects put natively in runs and in parts, by the AWS
        /// CLI from its standard input and by boto3 in parts, in turn.
        #[arg(long, value_delimiter = ',', value_parser = init::parse_bytes)]
        large: Vec<u64>,
        /// The objects of each large case; without it, a large case puts objects until the cell
        /// refuses one as out of space through each door.
        #[arg(long)]
        count: Option<u64>,
        /// Each founder's volume, in bytes or with a K, M, G or T suffix (powers of 1024).
        #[arg(long, value_parser = init::parse_bytes, default_value = "256M")]
        volume: u64,
    },
}

#[derive(Subcommand)]
enum DiskCommand {
    /// Show what the operating system reports about the device under PATH, and with
    /// --measure, what the device actually does.
    Probe {
        /// A directory on the device to inspect.
        path: PathBuf,
        /// Measure the device with a scratch file (at most 256 MiB and a tenth of the free
        /// space, removed afterwards; about 20 seconds on an SSD).
        #[arg(long)]
        measure: bool,
        /// Also list every query the OS could not answer, and why.
        #[arg(long)]
        verbose: bool,
    },
}

/// The cell `mantle init` makes: the machines file's founders, then the `--node` ones, and the
/// cell's statements, each from the file or a flag and refused where stated in both.
fn stated_cell(
    file: Option<&std::path::Path>,
    nodes: &[String],
    (max_nodes, max_volumes, target): (Option<u32>, Option<u32>, Option<f64>),
    region: Option<&str>,
) -> Result<init::Cell, init::Error> {
    let mut cell = match file {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(init::Error::Output)?;
            init::parse_file(path, &text)?
        }
        None => init::Cell::default(),
    };
    for spec in nodes {
        cell.founders.push(init::parse_node(spec)?);
    }
    let twice = |what: &str| init::Error::Cell(format!("{what} stated in the file and as a flag"));
    if max_nodes.is_some() {
        if cell.max_nodes.is_some() {
            return Err(twice("max_nodes"));
        }
        cell.max_nodes = max_nodes;
    }
    if max_volumes.is_some() {
        if cell.max_volumes.is_some() {
            return Err(twice("max_volumes"));
        }
        cell.max_volumes = max_volumes;
    }
    if target.is_some() {
        if cell.target.is_some() {
            return Err(twice("durability.target"));
        }
        cell.target = target;
    }
    if let Some(region) = region {
        if cell.region.is_some() {
            return Err(twice("region"));
        }
        cell.region = Some(init::check_region(region).map_err(init::Error::Cell)?);
    }
    Ok(cell)
}

/// A byte count, optionally with a K or M suffix (powers of 1024).
fn parse_size(text: &str) -> Result<usize, String> {
    let text = text.trim();
    let (digits, shift) = match text.strip_suffix(['k', 'K']) {
        Some(d) => (d, 10),
        None => match text.strip_suffix(['m', 'M']) {
            Some(d) => (d, 20),
            None => (text, 0),
        },
    };
    let value: usize = digits
        .parse()
        .map_err(|_| format!("{text} is not a size in bytes"))?;
    value
        .checked_shl(shift)
        .filter(|v| v.checked_shr(shift) == Some(value) && *v > 0)
        .ok_or_else(|| format!("{text} is not a usable size"))
}

/// `PER_YEAR:PERCENT`: events a year, and the percent of the nodes each destroys.
fn parse_burst(text: &str) -> Result<(f64, f64), String> {
    let bad = || format!("{text} is not PER_YEAR:PERCENT");
    let (rate, percent) = text.split_once(':').ok_or_else(bad)?;
    let rate: f64 = rate.trim().parse().map_err(|_| bad())?;
    let percent: f64 = percent.trim().parse().map_err(|_| bad())?;
    if rate.is_finite() && rate >= 0.0 && (0.0..=100.0).contains(&percent) {
        Ok((rate, percent / 100.0))
    } else {
        Err(bad())
    }
}

/// `ZONES:PER_YEAR`: zones, and each zone's losses a year.
fn parse_zones(text: &str) -> Result<(usize, f64), String> {
    let bad = || format!("{text} is not ZONES:PER_YEAR");
    let (zones, rate) = text.split_once(':').ok_or_else(bad)?;
    let zones: usize = zones.trim().parse().map_err(|_| bad())?;
    let rate: f64 = rate.trim().parse().map_err(|_| bad())?;
    if zones > 0 && rate.is_finite() && rate >= 0.0 {
        Ok((zones, rate))
    } else {
        Err(bad())
    }
}

/// The object command `command` names, or none for any other command; refused where its places do
/// not read.
fn object_command(command: &Command) -> Option<Result<client::Command, String>> {
    let at = |text: &str| {
        client::location(text).ok_or_else(|| format!("{text}: not s3://BUCKET or s3://BUCKET/KEY"))
    };
    let object = |text: &str| {
        at(text).and_then(|l| {
            if l.key.is_empty() {
                Err(format!("{text}: names no key"))
            } else {
                Ok(l)
            }
        })
    };
    let bucket = |text: &str| {
        at(text).and_then(|l| {
            if l.key.is_empty() {
                Ok(l.bucket)
            } else {
                Err(format!("{text}: a bucket names no key"))
            }
        })
    };
    Some(match command {
        Command::Mb { bucket: b } => bucket(b).map(client::Command::MakeBucket),
        Command::Rb { bucket: b } => bucket(b).map(client::Command::RemoveBucket),
        Command::Ls { at: None } => Ok(client::Command::List(None)),
        Command::Ls { at: Some(text) } => at(text).map(|l| client::Command::List(Some(l))),
        Command::Cp { from, to, size } => Ok(client::Command::Copy(
            client::place(from),
            client::place(to),
            *size,
        )),
        Command::Rm { at: text } => object(text).map(client::Command::Remove),
        Command::Head { at: text } => object(text).map(client::Command::Head),
        Command::Multipart { step } => match step {
            MultipartCommand::Create { at: text } => {
                object(text).map(client::Command::CreateUpload)
            }
            MultipartCommand::Put {
                at: text,
                upload,
                number,
                from,
                length,
            } => object(text).map(|at| client::Command::UploadPart {
                at,
                upload: upload.clone(),
                number: *number,
                from: client::place(from),
                length: *length,
            }),
            MultipartCommand::Complete {
                at: text,
                upload,
                parts,
            } => object(text).and_then(|at| {
                let mut listed = Vec::with_capacity(parts.len());
                for part in parts {
                    let (number, etag) = part
                        .split_once('=')
                        .ok_or_else(|| format!("{part}: not NUMBER=ETAG"))?;
                    let number = number
                        .parse()
                        .map_err(|_| format!("{part}: a part's number"))?;
                    listed.push((number, etag.trim_matches('"').to_owned()));
                }
                Ok(client::Command::Complete {
                    at,
                    upload: upload.clone(),
                    parts: listed,
                })
            }),
            MultipartCommand::Abort { at: text, upload } => {
                object(text).map(|at| client::Command::Abort {
                    at,
                    upload: upload.clone(),
                })
            }
            MultipartCommand::Parts { at: text, upload } => {
                object(text).map(|at| client::Command::Parts {
                    at,
                    upload: upload.clone(),
                })
            }
            MultipartCommand::Uploads { bucket: b } => bucket(b).map(client::Command::Uploads),
        },
        _ => return None,
    })
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    // A node writes its lines through the runtime's own writer thread: this thread holds no
    // lock on standard output for it.
    match &cli.command {
        Command::Node { config } => {
            return exit(mantle_node::process::run(config).map_err(|e| e.to_string()));
        }
        Command::Serve { config } => {
            return exit(mantle_node::process::serve_stated(config).map_err(|e| e.to_string()));
        }
        _ => {}
    }
    // An object command writes its lines a lock at a time: a copy to standard output writes its
    // bytes through the runtime's own writer thread.
    if let Some(command) = object_command(&cli.command) {
        let ran = command.and_then(|command| {
            let dir = cli
                .client
                .clone()
                .or_else(|| std::env::var_os("MANTLE_CLIENT").map(PathBuf::from))
                .ok_or_else(|| "no client: --client DIR or MANTLE_CLIENT".to_owned())?;
            client::run(&dir, command, std::io::stdout()).map_err(|e| e.to_string())
        });
        return exit(ran);
    }
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let result = match cli.command {
        Command::Node { .. }
        | Command::Serve { .. }
        | Command::Mb { .. }
        | Command::Rb { .. }
        | Command::Ls { .. }
        | Command::Cp { .. }
        | Command::Rm { .. }
        | Command::Head { .. }
        | Command::Multipart { .. } => Ok(()),
        Command::Init {
            what: Some(InitCommand::Client { name, cell }),
            ..
        } => init::client(&mut out, &name, &cell).map_err(|e| e.to_string()),
        Command::Init {
            what: None,
            out: dir,
            nodes,
            founders,
            max_nodes,
            max_volumes,
            target,
            region,
        } => match dir {
            Some(dir) => stated_cell(
                founders.as_deref(),
                &nodes,
                (max_nodes, max_volumes, target),
                region.as_deref(),
            )
            .and_then(|cell| init::init(&mut out, &dir, &cell))
            .map(|_| ())
            .map_err(|e| e.to_string()),
            None => Err("init OUT, or init client NAME --cell OUT".to_owned()),
        },
        Command::Disk {
            command:
                DiskCommand::Probe {
                    path,
                    measure,
                    verbose,
                },
        } => disk::probe(&mut out, &path, measure, verbose).map_err(|e| e.to_string()),
        Command::Bench {
            command:
                BenchCommand::Chunk {
                    path,
                    seconds,
                    sizes,
                    workers,
                    rate,
                    rounds,
                    skip_device,
                    segment_size,
                },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench::chunk(
                &mut out,
                &path,
                &bench::Options {
                    step,
                    sizes,
                    workers,
                    rate,
                    rounds,
                    skip_device,
                    segment_size,
                },
            )
            .map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command:
                BenchCommand::Log {
                    path,
                    seconds,
                    sizes,
                    replicas,
                    skip_device,
                },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench_log::log(
                &mut out,
                &path,
                &bench_log::Options {
                    step,
                    sizes,
                    replicas,
                    skip_device,
                },
            )
            .map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command: BenchCommand::Ec { seconds, sizes },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => {
                let sizes = if sizes.is_empty() {
                    vec![64 << 10, 1 << 20, 8 << 20]
                } else {
                    sizes
                };
                bench_ec::ec(&mut out, &bench_ec::CODES, &sizes, step).map_err(|e| e.to_string())
            }
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command:
                BenchCommand::Gateway {
                    seconds,
                    sizes,
                    clients,
                    rate,
                },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => {
                let sizes = if sizes.is_empty() {
                    vec![64 << 10, 8 << 20, 64 << 20]
                } else {
                    sizes
                };
                let loads: Vec<_> = clients
                    .into_iter()
                    .map(|clients| bench_gateway::Load { clients, rate })
                    .collect();
                bench_gateway::schemes()
                    .and_then(|schemes| {
                        bench_gateway::gateway(&mut out, &schemes, &sizes, &loads, step)
                    })
                    .map_err(|e| e.to_string())
            }
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command: BenchCommand::Hash { seconds, sizes },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => {
                let sizes = if sizes.is_empty() {
                    vec![8 << 10, 64 << 10, 1 << 20, 8 << 20]
                } else {
                    sizes
                };
                bench_hash::hash(&mut out, &sizes, step).map_err(|e| e.to_string())
            }
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command: BenchCommand::Http { seconds },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench_http::http(&mut out, step).map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command: BenchCommand::S3 { seconds },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench_s3::s3(&mut out, step).map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command: BenchCommand::Meta { seconds },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench_meta::meta(&mut out, step).map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
        Command::Bench {
            command:
                BenchCommand::Transfer {
                    dir,
                    seed,
                    small,
                    large,
                    count,
                    volume,
                },
        } => {
            if small.is_empty() && large.is_empty() {
                Err("no case: --small N or --large SIZE".into())
            } else {
                let options = transfer::Options {
                    dir,
                    seed,
                    small,
                    large,
                    count,
                    volume_bytes: volume,
                };
                match transfer::transfer(&mut out, &options) {
                    Ok(true) => Ok(()),
                    Ok(false) => Err("a check was not exact: the rows above say which".into()),
                    Err(e) => Err(e.to_string()),
                }
            }
        }
        Command::Durability {
            domains,
            afr,
            repair_hours,
            bursts,
            zones,
            target,
        } => {
            if (0.0..=100.0).contains(&afr) {
                durability::durability(
                    &mut out,
                    &durability::Options {
                        domains,
                        annual_failure: afr / 100.0,
                        repair_hours,
                        bursts,
                        zones,
                        target,
                    },
                )
            } else {
                Err(format!("--afr {afr} is not a percentage"))
            }
        }
    };
    exit(result)
}

fn exit(result: Result<(), String>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Reporting the failure is best effort: stderr may be closed too.
            let _ = writeln!(std::io::stderr().lock(), "mantle: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A machines file's founders come before the flags', and a cell's statement made in both is
    /// refused.
    #[test]
    fn founders_come_from_the_file_then_the_flags() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("machines.conf");
        std::fs::write(
            &file,
            "max_nodes = 2\n[node]\nquic = 127.0.0.1:1\nplane = 127.0.0.1:2\n",
        )
        .unwrap();
        let flag = vec!["quic=127.0.0.1:3,plane=127.0.0.1:4".to_owned()];
        let cell = stated_cell(Some(&file), &flag, (None, Some(4), None), None).unwrap();
        let quics: Vec<String> = cell.founders.iter().map(|f| f.quic.to_string()).collect();
        assert_eq!(quics, vec!["127.0.0.1:1", "127.0.0.1:3"]);
        assert_eq!((cell.max_nodes, cell.max_volumes), (Some(2), Some(4)));
        assert!(matches!(
            stated_cell(Some(&file), &flag, (Some(3), None, None), None),
            Err(init::Error::Cell(_))
        ));
        let flags_alone = stated_cell(
            None,
            &flag,
            (Some(1), Some(1), Some(1e-9)),
            Some("eu-west-1"),
        )
        .unwrap();
        assert_eq!(flags_alone.founders.len(), 1);
        assert_eq!(flags_alone.target, Some(1e-9));
        assert_eq!(flags_alone.region(), "eu-west-1");
        assert_eq!(cell.region(), "us-east-1");
        // A region is S3's alphabet, and stated once.
        assert!(matches!(
            stated_cell(None, &flag, (None, None, None), Some("eu/west")),
            Err(init::Error::Cell(_))
        ));
        std::fs::write(&file, "region = eu-west-1\n").unwrap();
        assert!(matches!(
            stated_cell(Some(&file), &flag, (None, None, None), Some("us-east-2")),
            Err(init::Error::Cell(_))
        ));
    }
}
