//! The endpoint's TLS sessions, kept so a client resumes rather than handshakes in full, with no
//! early data (node.md §4.1; research/94). Every session stays on the node: a TLS 1.3 ticket is the
//! key of a session kept here, used once (RFC 8446 §8.1), and a TLS 1.2 session is found by the
//! session ID the server gave (RFC 5246 §7.4.1.3). No ticket carries a session encrypted under a
//! key of the node's, so there is no such key to rotate or to lose, and a TLS 1.2 master secret
//! never crosses the wire in a ticket, which RFC 5077 sends before `ChangeCipherSpec`. No ticket
//! admits early data: the listener's configuration allows none, so a ticket says none.
//!
//! The store holds the last sessions made, as many as its slots, each slot reserved when the
//! listener binds; a newer session takes the slot of the one made longest ago. A session is
//! resumed for [`LIFETIME_NS`] from the full handshake that authenticated the endpoint, however
//! often it was resumed since: a TLS 1.3 ticket a resumed handshake gives inherits the time of the
//! session that handshake resumed. A session's bytes are zeroed when it is taken, found past its
//! lifetime, or its slot goes to a newer one.

use std::collections::HashMap;
use std::sync::Mutex;

use hyper_tls::server::StoresServerSessions;

/// The tickets a TLS 1.3 handshake gives its client, full or resumed. A ticket is used once (RFC
/// 8446 §8.1), and RFC 8446 asks a server to "issue new tickets with every connection" and to
/// "offer at least as many tickets as the number of connections that a client might use"
/// (Appendix C.4): a resumed connection spends one, so two is the least count with which a client's
/// tickets grow with its connections, one more for each it resumes, so a client that has had `n`
/// connections holds a ticket for each of its next `n` at least, however many it opens at once.
pub(crate) const TICKETS: usize = 2;

/// How long a session is resumed for, counted from the full handshake that authenticated the
/// endpoint: 24 hours, the upper limit RFC 5246 suggests "for session ID lifetimes, since an
/// attacker who obtains a master_secret may be able to impersonate the compromised party until the
/// corresponding session ID is retired" (Appendix F.1.4). A resumed TLS 1.3 handshake's tickets
/// keep the time of the session it resumed, the limit RFC 8446 recommends "on the total lifetime"
/// of keying material that new tickets would otherwise extend indefinitely (§4.6.1). It is the
/// `ticket_lifetime` hyper-tls states on a ticket a store keeps (`server/tls13.rs`, `emit_ticket`),
/// so no ticket is refused within the lifetime it states unless its session began before it.
pub(crate) const LIFETIME_NS: u64 = 24 * 60 * 60 * 1_000_000_000;

/// The length of a session's id: the 32 random octets hyper-tls's server draws for a TLS 1.3
/// ticket (`server/tls13.rs`, `emit_ticket`) and for a TLS 1.2 session ID (`SessionId::random`),
/// the most a session ID holds (RFC 5246 §7.4.1.2, `SessionID<0..32>`).
const ID: usize = 32;

/// The most octets a session this listener makes encodes to (hyper-tls `msgs/persist.rs`,
/// `ServerSessionValue::encode`, field by field): the server name, a flag and an 8-bit length
/// before at most 255 octets; the version and the cipher suite, two each; the secret, an 8-bit
/// length before 48 octets at most (a TLS 1.2 master secret, RFC 5246 §8.1, and a TLS 1.3 PSK of
/// SHA-384, the longest hash of the suites); the extended master secret flag; the client's chain,
/// one flag, as the listener asks no client for a certificate; ALPN's protocol, a flag and an 8-bit
/// length before the listener's one ([`super::ALPN`]); the application's data, a 16-bit length
/// before none, as the listener sets none; the time it was made, eight; and the ticket age's
/// offset, four.
const SESSION: usize =
    (1 + 1 + 255) + 2 + 2 + (1 + 48) + 1 + 1 + (1 + 1 + super::ALPN.len()) + 2 + 8 + 4;

/// One session's place in the store, empty while `len` is zero.
#[derive(Clone, Copy)]
struct Slot {
    id: [u8; ID],
    /// When the full handshake this session descends from was made, on the host's clock ([`now`]).
    origin: u64,
    len: usize,
    value: [u8; SESSION],
}

impl Slot {
    /// A slot holding nothing, every octet zero.
    const EMPTY: Self = Self {
        id: [0; ID],
        origin: 0,
        len: 0,
        value: [0; SESSION],
    };
}

/// The endpoint's sessions: the last ones made, as many as its slots.
pub(crate) struct Sessions {
    /// Every slot, reserved whole.
    slots: Box<[Slot]>,
    /// The slot the next session takes: slots are taken in turn, so it is the one taken longest
    /// ago.
    next: usize,
    /// Each kept session's slot, by its id. It is made able to hold twice the slots' ids, so the
    /// table it was made with always holds them: hashbrown, std's map, rehashes in place, never
    /// into a new table, while what it would hold is at most half of what it can
    /// (`RawTableInner::reserve_rehash`).
    index: HashMap<[u8; ID], usize>,
}

impl Sessions {
    /// A store of `slots` sessions, its every slot and its index reserved now.
    pub(crate) fn new(slots: usize) -> Self {
        Self {
            slots: vec![Slot::EMPTY; slots].into_boxed_slice(),
            next: 0,
            index: HashMap::with_capacity(slots.saturating_mul(2)),
        }
    }

    /// Keeps `value` under `id`, its session descending from a full handshake at `origin`, in the
    /// slot of the session made longest ago. False, keeping nothing, for an id that is not
    /// [`ID`] octets, a value longer than [`SESSION`], or a store of no slots.
    fn put(&mut self, id: &[u8], value: &[u8], origin: u64) -> bool {
        let Ok(id) = <[u8; ID]>::try_from(id) else {
            return false;
        };
        if value.len() > SESSION {
            return false;
        }
        let at = self.next;
        let Some(slot) = self.slots.get_mut(at) else {
            return false;
        };
        if slot.len > 0 {
            self.index.remove(&slot.id);
        }
        *slot = Slot::EMPTY;
        let Some(kept) = slot.value.get_mut(..value.len()) else {
            return false;
        };
        kept.copy_from_slice(value);
        slot.id = id;
        slot.origin = origin;
        slot.len = value.len();
        // An id is drawn at random; one already kept loses its older slot.
        if let Some(older) = self.index.insert(id, at)
            && older != at
            && let Some(older) = self.slots.get_mut(older)
        {
            *older = Slot::EMPTY;
        }
        self.next = at
            .checked_add(1)
            .filter(|&next| next < self.slots.len())
            .unwrap_or(0);
        true
    }

    /// The session kept under `id` and the time its full handshake was made, if it is within its
    /// lifetime at `now`; one past it is zeroed and forgotten. `take` forgets the one found too.
    fn find(&mut self, id: &[u8], now: u64, take: bool) -> Option<(Vec<u8>, u64)> {
        let id = <[u8; ID]>::try_from(id).ok()?;
        let at = *self.index.get(&id)?;
        let slot = self.slots.get_mut(at)?;
        let found = if now.saturating_sub(slot.origin) > LIFETIME_NS {
            None
        } else {
            Some((slot.value.get(..slot.len)?.to_vec(), slot.origin))
        };
        if take || found.is_none() {
            *slot = Slot::EMPTY;
            self.index.remove(&id);
        }
        found
    }
}

impl std::fmt::Debug for Sessions {
    /// The store's size, never a session's octets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sessions")
            .field("slots", &self.slots.len())
            .field("kept", &self.index.len())
            .finish()
    }
}

/// One connection's way into the endpoint's store, as its handshake keeps and finds sessions
/// (hyper-tls `StoresServerSessions`). The store is behind a lock as hyper-tls asks a store to be
/// `Send` and `Sync`; each call holds it for one copy and never waits while it does. A lock that
/// cannot be taken keeps and finds nothing: the client handshakes in full.
pub(crate) struct Kept {
    store: &'static Mutex<Sessions>,
    /// The time of the full handshake behind the session this connection resumed: hyper-tls takes
    /// a TLS 1.3 ticket while it reads the client's hello and keeps the connection's own tickets
    /// once it has the client's `Finished`, so they inherit it. A ticket taken but not resumed,
    /// for a suite or a name it does not suit, passes its time on too, which shortens their life
    /// and never lengthens it.
    origin: Option<u64>,
}

impl Kept {
    pub(crate) fn new(store: &'static Mutex<Sessions>) -> Self {
        Self {
            store,
            origin: None,
        }
    }

    /// Keeps a session made at `now` by this connection's handshake.
    fn put_at(&mut self, id: &[u8], value: &[u8], now: u64) -> bool {
        let origin = self.origin.unwrap_or(now);
        self.store
            .lock()
            .is_ok_and(|mut store| store.put(id, value, origin))
    }

    /// The session under `id` at `now`, forgotten once found when `take`: a TLS 1.3 ticket is
    /// used once, a TLS 1.2 session ID as often as its lifetime allows.
    fn find_at(&mut self, id: &[u8], now: u64, take: bool) -> Option<Vec<u8>> {
        let (value, origin) = self.store.lock().ok()?.find(id, now, take)?;
        if take {
            self.origin = Some(self.origin.map_or(origin, |held| held.min(origin)));
        }
        Some(value)
    }
}

/// The time on the host's clock, which every shard reads alike: connections on any of the node's
/// shards keep and resume sessions in the one store, so each session's time must mean the same on
/// all of them. hyper-rt's `shard_clock_ns` is the host's continuous clock, which counts time
/// asleep: `mach_continuous_time` on macOS, `CLOCK_BOOTTIME` on Linux, the interrupt time on
/// Windows (`hyper_rt::machine::clock`), where a shard's own clock (`futures::now_ns`) counts from
/// that shard's start.
fn now() -> u64 {
    hyper_rt::machine::clock::shard_clock_ns()
}

impl StoresServerSessions for Kept {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        self.put_at(&key, &value, now())
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (value, _) = self.store.lock().ok()?.find(key, now(), false)?;
        Some(value)
    }

    fn take(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.find_at(key, now(), true)
    }

    fn can_cache(&self) -> bool {
        self.store.lock().is_ok_and(|store| !store.slots.is_empty())
    }
}

impl std::fmt::Debug for Kept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kept")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session's id: `n` in its first octets, as hyper-tls draws ids at random.
    fn id(n: u64) -> [u8; ID] {
        let mut id = [0xa5; ID];
        id[..8].copy_from_slice(&n.to_le_bytes());
        id
    }

    /// A session's octets: `len` of them, each a function of `n`.
    fn value(n: u64, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| n.wrapping_add(u64::try_from(i).unwrap()).to_le_bytes()[0])
            .collect()
    }

    fn store(slots: usize) -> &'static Mutex<Sessions> {
        Box::leak(Box::new(Mutex::new(Sessions::new(slots))))
    }

    /// The store's clock is the host's on every shard: read on each shard of two, between two reads
    /// of the host's clock on this thread, it falls between them, where a shard's own clock counts
    /// from that shard's start, milliseconds against the host's time since boot.
    #[test]
    fn the_stores_clock_is_the_hosts_on_every_shard() {
        let rt = crate::listener::shards::tests::started(2, 2);
        for shard in rt.shard_ids().to_vec() {
            let (told, heard) = std::sync::mpsc::sync_channel(1);
            let before = hyper_rt::machine::clock::shard_clock_ns();
            rt.spawn_on(shard, async move {
                told.try_send((hyper_rt::futures::shard_id(), now()))
                    .unwrap();
            })
            .unwrap();
            let (on, read) = heard.recv().unwrap();
            let after = hyper_rt::machine::clock::shard_clock_ns();
            assert_eq!(on, Some(shard));
            assert!(before <= read && read <= after, "{before} {read} {after}");
        }
        rt.shutdown().unwrap();
    }

    /// What one handshake came to: the kind each side says it was, the version, whether the client
    /// had early data to send or the server any to read, and the sessions the store then keeps.
    #[derive(Debug, PartialEq)]
    struct Shook {
        client: Option<hyper_tls::HandshakeKind>,
        server: Option<hyper_tls::HandshakeKind>,
        version: Option<hyper_tls::ProtocolVersion>,
        early: (bool, bool, bool),
        kept: usize,
    }

    /// Moves what each side writes to the other until neither writes more.
    fn exchange(
        (client, client_config): (
            &mut hyper_tls::ClientConnection,
            &mut hyper_tls::ClientConfig,
        ),
        (server, server_config): (
            &mut hyper_tls::ServerConnection,
            &mut hyper_tls::ServerConfig,
        ),
    ) {
        loop {
            let mut wire = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut wire).unwrap();
            }
            let mut read = wire.as_slice();
            while !read.is_empty() {
                server.read_tls(&mut read).unwrap();
                server.process_new_packets(server_config).unwrap();
            }
            let mut back = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut back).unwrap();
            }
            let mut read = back.as_slice();
            while !read.is_empty() {
                client.read_tls(&mut read).unwrap();
                client.process_new_packets(client_config).unwrap();
            }
            if wire.is_empty() && back.is_empty() {
                return;
            }
        }
    }

    /// A stock client's TLS (hyper-tls's, rustls's) connecting three times with one configuration,
    /// as a client reconnects, to the listener's own configuration around one store, each
    /// connection's configuration built afresh as the listener builds it: over TLS 1.3 the second
    /// and third resume by a ticket the first and second left, and over TLS 1.2 by the first's
    /// session ID. The client asks for early data and has none to send, as no ticket allows it, and
    /// the server reads none. Each side says what the handshake was; the store says what it keeps:
    /// TLS 1.3 two tickets a handshake, a ticket gone once it is resumed, and TLS 1.2 one session.
    #[test]
    fn a_reconnecting_client_resumes_with_no_early_data() {
        use hyper_tls::HandshakeKind::{Full, Resumed};
        use hyper_tls::ProtocolVersion::{TLSv1_2, TLSv1_3};
        use hyper_tls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let validity = mantle_cert::Validity {
            not_before: now - 3_600,
            not_after: now + 86_400,
        };
        let name = "mantle s3 endpoint authority";
        let authority_key = mantle_cert::EndpointKey::generate().unwrap();
        let authority = mantle_cert::endpoint_authority(
            &authority_key,
            name,
            validity,
            mantle_cert::Serial::draw().unwrap(),
        )
        .unwrap();
        let key = mantle_cert::EndpointKey::generate().unwrap();
        let leaf = mantle_cert::endpoint(
            &authority_key,
            name,
            key.public_key(),
            &[mantle_cert::EndpointName::Dns("localhost")],
            (validity, mantle_cert::Serial::draw().unwrap()),
        )
        .unwrap();
        let signing = super::super::provider()
            .key_provider
            .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                key.pkcs8().to_vec(),
            )))
            .unwrap();
        let certified: &'static hyper_tls::sign::CertifiedKey = Box::leak(Box::new(
            hyper_tls::sign::CertifiedKey::new(vec![CertificateDer::from(leaf)], signing),
        ));

        let mut said = Vec::new();
        for version in [&hyper_tls::version::TLS13, &hyper_tls::version::TLS12] {
            let sessions = store(TICKETS * 4);
            let mut roots = hyper_tls::RootCertStore::empty();
            roots.add(CertificateDer::from(authority.clone())).unwrap();
            let mut client_config = hyper_tls::ClientConfig::builder_with_provider(
                &hyper_tls::crypto::aws_lc_rs::DEFAULT_PROVIDER,
            )
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            client_config.alpn_protocols = vec![super::super::ALPN.to_vec()];
            client_config.enable_early_data = true;
            for _ in 0..3 {
                let mut server_config = super::super::tls_config(certified, sessions).unwrap();
                let mut server = hyper_tls::ServerConnection::new(&server_config).unwrap();
                let mut client = hyper_tls::ClientConnection::new(
                    &mut client_config,
                    ServerName::try_from("localhost").unwrap(),
                )
                .unwrap();
                let could_send_early = client.early_data().is_some();
                exchange(
                    (&mut client, &mut client_config),
                    (&mut server, &mut server_config),
                );
                assert!(!client.is_handshaking() && !server.is_handshaking());
                said.push(Shook {
                    client: client.handshake_kind(),
                    server: server.handshake_kind(),
                    version: client.protocol_version(),
                    early: (
                        could_send_early,
                        client.is_early_data_accepted(),
                        server.early_data().is_some(),
                    ),
                    kept: sessions.lock().unwrap().index.len(),
                });
            }
        }
        let shook = |kind, version, kept| Shook {
            client: Some(kind),
            server: Some(kind),
            version: Some(version),
            early: (false, false, false),
            kept,
        };
        assert_eq!(
            said,
            [
                shook(Full, TLSv1_3, 2),
                shook(Resumed, TLSv1_3, 3),
                shook(Resumed, TLSv1_3, 4),
                shook(Full, TLSv1_2, 1),
                shook(Resumed, TLSv1_2, 1),
                shook(Resumed, TLSv1_2, 1),
            ]
        );
    }

    #[test]
    fn a_session_id_resumes_within_its_lifetime_and_a_ticket_once() {
        let mut sessions = Sessions::new(4);
        assert!(sessions.put(&id(1), &value(1, 100), 0));
        assert!(sessions.put(&id(2), &value(2, SESSION), 0));
        // A TLS 1.2 session ID, as often as its lifetime allows.
        for _ in 0..3 {
            assert_eq!(
                sessions.find(&id(1), LIFETIME_NS, false),
                Some((value(1, 100), 0))
            );
        }
        // A TLS 1.3 ticket, once.
        assert_eq!(sessions.find(&id(2), 1, true), Some((value(2, SESSION), 0)));
        assert_eq!(sessions.find(&id(2), 1, true), None);
        assert_eq!(sessions.find(&id(2), 1, false), None);
        assert_eq!(sessions.find(&id(3), 1, false), None);
    }

    #[test]
    fn a_session_past_its_lifetime_is_refused_and_zeroed() {
        let mut sessions = Sessions::new(2);
        assert!(sessions.put(&id(1), &value(1, 64), 5));
        assert_eq!(sessions.find(&id(1), 5 + LIFETIME_NS + 1, false), None);
        assert!(sessions.slots.iter().all(|slot| slot.len == 0
            && slot.origin == 0
            && slot.id == [0; ID]
            && slot.value.iter().all(|&octet| octet == 0)));
        assert!(sessions.index.is_empty());
        // Taken at the last moment of its life, it is still found.
        assert!(sessions.put(&id(2), &value(2, 64), 5));
        assert_eq!(
            sessions.find(&id(2), 5 + LIFETIME_NS, true),
            Some((value(2, 64), 5))
        );
    }

    #[test]
    fn the_store_keeps_the_last_sessions_made() {
        let mut sessions = Sessions::new(3);
        for n in 1..=5 {
            assert!(sessions.put(&id(n), &value(n, 32), n));
        }
        for gone in [1, 2] {
            assert_eq!(sessions.find(&id(gone), 5, false), None, "{gone}");
        }
        for kept in [3, 4, 5] {
            assert_eq!(
                sessions.find(&id(kept), 5, false),
                Some((value(kept, 32), kept))
            );
        }
        assert_eq!(sessions.index.len(), 3);
        // A slot taken empty is taken again in turn: the next goes where the oldest, 3, is.
        assert!(sessions.find(&id(4), 5, true).is_some());
        assert!(sessions.put(&id(6), &value(6, 32), 6));
        assert_eq!(sessions.find(&id(3), 6, false), None);
        assert!(sessions.find(&id(5), 6, false).is_some());
        assert!(sessions.find(&id(6), 6, false).is_some());
    }

    #[test]
    fn what_the_store_cannot_hold_it_refuses_and_keeps_what_it_holds() {
        let mut sessions = Sessions::new(1);
        assert!(sessions.put(&id(1), &value(1, 32), 0));
        assert!(!sessions.put(&id(2)[..ID - 1], &value(2, 32), 0));
        assert!(!sessions.put(&id(3), &value(3, SESSION + 1), 0));
        assert_eq!(sessions.find(&id(1), 0, false), Some((value(1, 32), 0)));
        assert_eq!(sessions.find(&id(1)[..ID - 1], 0, false), None);
        assert!(!Sessions::new(0).put(&id(4), &value(4, 32), 0));
        // An id kept twice is kept once, in its newer slot.
        let mut sessions = Sessions::new(3);
        assert!(sessions.put(&id(5), &value(5, 32), 0));
        assert!(sessions.put(&id(5), &value(6, 32), 1));
        assert_eq!(sessions.index.len(), 1);
        assert_eq!(sessions.slots.iter().filter(|slot| slot.len > 0).count(), 1);
        assert_eq!(sessions.find(&id(5), 1, false), Some((value(6, 32), 1)));
    }

    #[test]
    fn a_resumed_handshakes_tickets_keep_the_time_of_the_session_it_resumed() {
        let store = store(8);
        let hour = LIFETIME_NS / 24;
        // A full handshake at 0 leaves its tickets.
        let mut first = Kept::new(store);
        assert!(first.put_at(&id(1), &value(1, 64), 0));
        assert!(first.put_at(&id(2), &value(2, 64), 0));
        // One resumed ten hours on takes a ticket, once, and leaves two of the same time.
        let mut second = Kept::new(store);
        assert_eq!(second.find_at(&id(1), 10 * hour, true), Some(value(1, 64)));
        assert!(second.put_at(&id(3), &value(3, 64), 10 * hour));
        assert_eq!(Kept::new(store).find_at(&id(1), 10 * hour, true), None);
        // Another resumed from that one at 23 hours does the same.
        let mut third = Kept::new(store);
        assert_eq!(third.find_at(&id(3), 23 * hour, true), Some(value(3, 64)));
        assert!(third.put_at(&id(4), &value(4, 64), 23 * hour));
        // Made at 23 hours, it ends with the full handshake at 24.
        assert_eq!(
            Kept::new(store).find_at(&id(4), LIFETIME_NS + 1, true),
            None
        );
        // A full handshake's own begin at its own time.
        let mut fourth = Kept::new(store);
        assert!(fourth.put_at(&id(5), &value(5, 64), LIFETIME_NS + 1));
        assert_eq!(
            Kept::new(store).find_at(&id(5), 2 * LIFETIME_NS, true),
            Some(value(5, 64))
        );
        // A TLS 1.2 session ID found is not taken and passes no time on.
        let mut fifth = Kept::new(store);
        assert!(fifth.find_at(&id(2), hour, false).is_some());
        assert_eq!(fifth.origin, None);
    }

    #[test]
    fn a_store_shows_no_session() {
        let mut sessions = Sessions::new(2);
        assert!(sessions.put(&id(1), &[0x5a; 64], 0));
        let shown = format!("{sessions:?} {:?}", Kept::new(store(1)));
        assert_eq!(
            shown,
            "Sessions { slots: 2, kept: 1 } Kept { origin: None, .. }"
        );
    }

    /// The connections a count of the store's reservation is made at: every count to 64, and
    /// either side of each count past it at which the index's table doubles, its ids four a
    /// connection and its buckets 8/7 of them to a power of two, to 2^17 buckets.
    fn counts() -> Vec<usize> {
        let mut counts: Vec<usize> = (1..=64).collect();
        for power in 8..=17 {
            let edge = (1usize << power) * 7 / 32;
            counts.extend([edge, edge + 1, edge + 2]);
        }
        counts
    }

    #[test]
    fn the_store_is_reserved_whole_within_its_stated_bytes_and_never_grows() {
        use hyper_measure::alloc;
        let stated = crate::listener::SESSION_BYTES;
        let mut most = 0u64;
        for connections in counts() {
            alloc::begin();
            let sessions = Sessions::new(TICKETS * connections);
            let made = alloc::end();
            drop(sessions);
            let each = u64::try_from(made.live).unwrap() / u64::try_from(connections).unwrap();
            assert!(
                each <= stated,
                "{connections} connections: {each} octets each"
            );
            most = most.max(each);
        }
        eprintln!("at most {most} octets a connection, {stated} stated");
        // Kept full and churned: a session put in every slot many times over, half of them
        // taken, half found past their lifetime. Nothing but the octets each find returns, which
        // its caller owns, is allocated, and nothing grows.
        let connections = 64;
        let mut sessions = Sessions::new(TICKETS * connections);
        let octets = value(7, SESSION);
        let puts = u64::try_from(100 * TICKETS * connections).unwrap();
        alloc::begin();
        let mut found = 0u64;
        for n in 0..puts {
            assert!(sessions.put(&id(n), &octets, n));
            if n % 2 == 0 {
                found += u64::from(sessions.find(&id(n), n, true).is_some());
            } else if n > 8 {
                assert_eq!(
                    sessions.find(&id(n - 8), n - 8 + LIFETIME_NS + 1, false),
                    None
                );
            }
        }
        let churned = alloc::end();
        assert_eq!(churned.reallocations, 0, "{churned:?}");
        assert_eq!(churned.allocations, found, "{churned:?}");
        assert_eq!(churned.live, 0, "{churned:?}");
    }
}
