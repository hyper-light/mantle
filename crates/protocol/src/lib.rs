//! mantle's native protocol (docs/design/node.md §3–§4.1, gateway.md §6): what a node's QUIC
//! connections carry and how an object's operations travel on them, shared by the node and its
//! clients, so a client depends on none of the engine, the ranges or the chunk store.
//!
//! - [`bounds`]: every bound of a transport, derived from what its owner states.
//! - [`classes`]: the kinds of message QUIC carries, their classes and who may send each.
//! - [`object`]: an object's request and reply, S3's operations on the native protocol.

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

pub mod bounds;
pub mod classes;
pub mod object;

/// The name a node's certificate carries and a dial checks it against (RFC 6125).
pub fn node_name(id: u64) -> String {
    format!("node-{id}")
}

/// The name a client's certificate gives it, its subject's common name: `client-` and its id.
pub fn client_name(id: u64) -> String {
    format!("client-{id}")
}

/// The id a client's name gives, [`client_name`]'s form exactly and nonzero; none for any other.
pub fn client_id(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("client-")?;
    let id: u64 = digits.parse().ok()?;
    // One spelling a client: no sign or leading zero, which parse takes.
    if id != 0 && id.to_string() == digits {
        Some(id)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_is_named_by_its_id_in_one_spelling() {
        assert_eq!(client_id(&client_name(7)), Some(7));
        assert_eq!(client_id(&client_name(u64::MAX)), Some(u64::MAX));
        for other in [
            "client-0",
            "client-07",
            "client-+7",
            "client-",
            "node-7",
            "client-7 ",
        ] {
            assert_eq!(client_id(other), None, "{other}");
        }
    }
}
