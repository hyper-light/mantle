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
mod disk;
mod display;

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
}

#[derive(Subcommand)]
enum BenchCommand {
    /// Measure the device under PATH, then the chunk store's puts and reads on it across
    /// chunk sizes and concurrency, in a scratch volume of at most 4 GiB and a tenth of the
    /// free space (removed afterwards; about a minute on an SSD).
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
        /// Leave out the measurement of the device itself.
        #[arg(long)]
        skip_device: bool,
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
                    skip_device,
                },
        } => match std::time::Duration::try_from_secs_f64(seconds) {
            Ok(step) => bench::chunk(
                &mut out,
                &path,
                &bench::Options {
                    step,
                    sizes,
                    workers,
                    skip_device,
                },
            )
            .map_err(|e| e.to_string()),
            Err(_) => Err(format!("--seconds {seconds} is not a duration")),
        },
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
