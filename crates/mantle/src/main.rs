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
mod bench_hash;
mod bench_log;
mod bench_meta;
mod disk;
mod display;
mod durability;

#[derive(Parser)]
#[command(
    name = "mantle",
    version,
    about = "Durable object storage, from one laptop to a fleet"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
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
        /// Requests in flight to measure, comma-separated: 1,4,16,64 by default.
        #[arg(long, value_delimiter = ',')]
        workers: Vec<usize>,
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
    /// Measure what applying the metadata layers' heaviest commands costs on one core: a
    /// multipart completion of up to 10,000 parts, an entry of many commands from one session,
    /// a create or delete attempt learning up to 10,000 ranges, and the sweep's check of a
    /// page of blocks against a large file.
    Meta {
        /// Seconds each measurement runs.
        #[arg(long, default_value_t = 0.5)]
        seconds: f64,
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let result = match cli.command {
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
            command: BenchCommand::Meta { seconds },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench_meta::meta(&mut out, step).map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
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
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Reporting the failure is best effort: stderr may be closed too.
            let _ = writeln!(std::io::stderr().lock(), "mantle: {e}");
            ExitCode::FAILURE
        }
    }
}
