//! The simulated cell (`support/cell.rs`) under settings for requests larger than a connection's
//! first window, in a process of its own: a process states its message bounds once, from its
//! ranges' settings (`mantle_node::classes::state_bounds`), and the other simulation tests run the
//! cluster test's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use hyper_log::Config as LogConfig;
use hyper_transport::{Classes, MIN_DATAGRAM, class_reserve, initial_window};
use mantle_meta::wire::{Command as Op, Entry, Sessioned};
use mantle_node::classes::NodeClasses;
use mantle_node::client::{Asks, ClientError};
use mantle_node::command;
use mantle_range::Settings;

#[allow(dead_code)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "support/history.rs"]
mod history;
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[path = "../../range/tests/support/store.rs"]
mod store;
use cell::{CLIENT, GROUP, SETTINGS, Sim, log_config};

/// The range's settings for requests larger than a connection's first window: entries and messages
/// to 32 KiB, twice the first window (`first_window`), the rest as above.
const LARGE: Settings = Settings {
    max_size_per_msg: 32 << 10,
    max_uncommitted_size: 64 << 10,
    max_entry_bytes: 32 << 10,
    ..SETTINGS
};

/// A node's log for a range under `LARGE`: frames that hold its largest entry, and groups that
/// hold a ready of its settings, 96 KiB, in bytes and in entries of the fewest bytes (`mantle_range`
/// checks a member's log against its range's settings).
fn large_log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 64 * 4096,
        group_entries: 1 << 15,
        group_bytes: 256 << 10,
        ..log_config()
    }
}

/// What a node's connection lets a peer send before the node has read any of it: its first
/// connection window (RFC 9000 §4.1's `initial_max_data`), the transport's initial window and a
/// packet's stream bytes for each class more urgent than the least (`hyper_transport::credit`).
fn first_window() -> u64 {
    initial_window(MIN_DATAGRAM) + class_reserve(NodeClasses::RANKS - 1)
}

/// A command whose body is past a node's first connection window: registrations, as many as that
/// takes.
fn past_first_window() -> Entry {
    let mut entry = Entry {
        at_ns: 0,
        commands: Vec::new(),
    };
    while u64::try_from(entry.encode().unwrap().len()).unwrap() <= first_window() {
        let serial = u64::try_from(entry.commands.len()).unwrap() + 1;
        entry.commands.push(Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Op::Register {
                owner: CLIENT,
                run: 1,
                nth: serial,
            },
        });
    }
    entry
}

/// A request a node answers without its body, its body past the node's first connection window so
/// that its head arrives before the rest of it can, is answered: the node reads the body to its
/// end before it answers. Answered before that, the request's unread half would be reset with the
/// exchange, and its client would hear the exchange refused rather than the answer it carried
/// (RFC 9114 §4.1's early response, which a client must keep whole), and ask again for ever.
#[test]
fn a_request_for_a_range_no_node_hosts_is_answered_so_when_its_body_comes_after_its_head() {
    let mut cell = Sim::with(1, LARGE, large_log_config());
    cell.leader();
    let order: Vec<u64> = (1..=cell.nodes).collect();
    let asking = cell.asking(CLIENT, &order);
    let (answered, asks) = cell.ask(&asking, GROUP + 1, past_first_window());
    assert_eq!(
        answered,
        Err(ClientError::Status(command::Status::NoRange).to_string())
    );
    assert_eq!(asks, Asks::default());
}
