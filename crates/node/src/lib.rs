//! A node (docs/design/node.md): its ranges' replicas on its shards, replicating over hyper-raft's
//! transport, QUIC for everything stateful and a sealed UDP datagram plane for consensus control
//! (node.md §3).
//!
//! The native protocol's messages and classes, and the transport's bounds, are
//! `mantle_protocol`'s, which the node's clients share.
//!
//! - [`client`]: a client of the cell's ranges, asking their leaders to apply entries.
//! - [`command`]: a command for a range as a client asks it and a node answers it.
//! - [`directory`]: the peers a connection may name, by certificate.
//! - [`frame`]: a range's Raft message as it travels.
//! - [`layout`]: what the node keeps beside its log: its run.
//! - [`node`]: the owner: the ranges, their log, the transport, one turn at a time.
//! - [`process`]: a node as a process, `mantle node`: its configuration and its run.
//! - [`read`]: a read a range's leader answers by ReadIndex.

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

pub mod ask;
pub mod chunk;
pub mod client;
pub mod command;
pub mod derive;
pub mod directory;
pub mod door;
pub mod frame;
pub mod history;
pub mod layout;
pub mod listener;
pub mod node;
pub mod place;
pub mod process;
pub mod read;
pub mod secret;
pub mod storage;

/// DIAGNOSTIC (never merged): a line written whole to this process's own file under
/// `MANTLE_DIAG_DIR`, else to standard error, so processes sharing an output never interleave.
pub fn diag_write(line: &str) {
    use std::io::Write;
    static FILE: std::sync::Mutex<Option<std::fs::File>> = std::sync::Mutex::new(None);
    let text = format!("{line}\n");
    let Ok(mut file) = FILE.lock() else {
        return;
    };
    if file.is_none() {
        *file = std::env::var_os("MANTLE_DIAG_DIR").and_then(|dir| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(std::path::Path::new(&dir).join(format!("{}.log", std::process::id())))
                .ok()
        });
    }
    match file.as_mut() {
        Some(f) => {
            let _ = f.write_all(text.as_bytes());
        }
        None => {
            let _ = std::io::stderr().write_all(text.as_bytes());
        }
    }
}

/// DIAGNOSTIC (never merged): [`diag_write`] of a formatted line.
#[macro_export]
macro_rules! diag_line {
    ($($arg:tt)*) => { $crate::diag_write(&format!($($arg)*)) };
}

/// DIAGNOSTIC (never merged): wall-clock milliseconds and this process's id, for a stderr line.
pub fn diag_at() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!("diag {ms} {}", std::process::id())
}
