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
        } => disk::probe(&mut out, &path, measure, verbose),
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
