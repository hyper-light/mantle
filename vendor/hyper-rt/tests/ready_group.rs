//! A readiness group tells its task what fired alone (`hyper_rt::readiness::ReadyGroup`): a task that
//! waits on many sockets learns which fired without asking each, and every slot its members and the
//! group hold comes back, whether a member fired, was dropped armed, or was dropped on the group's list.

// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::collections::BTreeSet;
use std::future::poll_fn;
use std::task::Poll;

use hyper_rt::RtError;
use hyper_rt::combine::{Either, join2, race2};
use hyper_rt::futures::{sleep, yield_now};
use hyper_rt::readiness::{Member, ReadyGroup};
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Ipv4Addr, SocketAddr, UdpSocket};

/// Shape: the waits the shard holds at once, a group's own slot among them.
const WAITS: usize = 16;
/// Shape: how long a wait that should have fired is given before the test fails rather than hangs.
const PATIENCE_NS: u64 = 10_000_000_000;

fn config(waits: usize) -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: waits,
        ring_entries: 64,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn waits_held() -> usize {
    registry::with_current(|ctx| ctx.waits_held()).unwrap()
}

/// Arms each socket's read readiness in `group`, keyed by its index.
async fn arm_all(group: &mut ReadyGroup, sockets: &[UdpSocket]) -> Vec<Member> {
    poll_fn(|cx| {
        Poll::Ready(
            sockets
                .iter()
                .enumerate()
                .map(|(key, socket)| socket.readable_in(group, cx, key as u64).unwrap())
                .collect(),
        )
    })
    .await
}

/// Takes what the group tells until `want` members fired: their keys, in the order told.
async fn fired(group: &mut ReadyGroup, want: usize) -> Vec<u64> {
    let mut keys = Vec::new();
    let told = race2(
        poll_fn(|_| {
            while let Some((key, outcome)) = group.next_fired().unwrap() {
                outcome.unwrap();
                keys.push(key);
            }
            if keys.len() >= want {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
        sleep(PATIENCE_NS),
    )
    .await;
    assert!(matches!(told, Either::First(())), "only {keys:?} fired");
    keys
}

/// Do: a task arms eight sockets' read readiness in one group; a peer sends to sockets 5 and 2. Expect:
/// the group tells keys 5 and 2, once each, and nothing else; the two fired waits' slots are back, the
/// six armed ones and the group's held; once every member and the group are dropped, no slot is held.
#[test]
fn a_group_tells_the_members_that_fired_and_no_other() {
    let mut rt = LocalRuntime::new(&config(WAITS)).unwrap();
    rt.block_on(async {
        let sockets: Vec<UdpSocket> = (0..8)
            .map(|_| UdpSocket::bind(loopback()).unwrap())
            .collect();
        let peer = UdpSocket::bind(loopback()).unwrap();
        let mut group = ReadyGroup::new();
        let mut members = arm_all(&mut group, &sockets).await;
        assert_eq!(waits_held(), 9, "eight members and the group's own slot");
        for key in [5usize, 2] {
            peer.send_to(b"x", sockets[key].local_addr().unwrap())
                .unwrap();
        }
        let keys = fired(&mut group, 2).await;
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert_eq!(
            keys.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([2, 5])
        );
        // Nothing else fired: the other six wait on.
        yield_now().await;
        assert_eq!(group.next_fired().unwrap(), None);
        assert_eq!(waits_held(), 7, "six armed members and the group");
        members.clear();
        drop(group);
        // The armed members' withdrawals are the loop's to take, after this poll.
        yield_now().await;
        assert_eq!(waits_held(), 0);
    })
    .unwrap();
}

/// Waits until each of `sockets` is readable, by a lone wait of its own: the loop fires every wait on a
/// handle in the direction that fired at once, so a group's members on them have fired too.
async fn both_readable(sockets: &[UdpSocket]) {
    let both = race2(
        join2(sockets[0].readable(), sockets[1].readable()),
        sleep(PATIENCE_NS),
    )
    .await;
    assert!(matches!(both, Either::First((Ok(()), Ok(())))), "{both:?}");
}

/// Do: two members fire; one is dropped before the group tells it. Expect: the group tells only the
/// other, and the dropped one's slot comes back as the group passes it.
#[test]
fn a_member_dropped_on_its_groups_list_is_passed_over_and_let_go() {
    let mut rt = LocalRuntime::new(&config(WAITS)).unwrap();
    rt.block_on(async {
        let sockets: Vec<UdpSocket> = (0..2)
            .map(|_| UdpSocket::bind(loopback()).unwrap())
            .collect();
        let peer = UdpSocket::bind(loopback()).unwrap();
        let mut group = ReadyGroup::new();
        let mut members = arm_all(&mut group, &sockets).await;
        for socket in &sockets {
            peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
        }
        both_readable(&sockets).await;
        let dropped = members.remove(0);
        drop(dropped);
        let keys = fired(&mut group, 1).await;
        assert_eq!(keys, vec![1]);
        assert_eq!(group.next_fired().unwrap(), None);
        assert_eq!(waits_held(), 1, "the group's own slot alone");
    })
    .unwrap();
}

/// Do: three members are armed and two fire; the group is dropped before telling them, its members
/// kept. Expect: the two listed members' slots and the group's come back at once; the third, armed,
/// holds its slot until it is dropped.
#[test]
fn a_group_dropped_lets_go_of_the_members_on_its_list() {
    let mut rt = LocalRuntime::new(&config(WAITS)).unwrap();
    rt.block_on(async {
        let sockets: Vec<UdpSocket> = (0..3)
            .map(|_| UdpSocket::bind(loopback()).unwrap())
            .collect();
        let peer = UdpSocket::bind(loopback()).unwrap();
        let mut group = ReadyGroup::new();
        let members = arm_all(&mut group, &sockets).await;
        for socket in &sockets[..2] {
            peer.send_to(b"x", socket.local_addr().unwrap()).unwrap();
        }
        both_readable(&sockets[..2]).await;
        drop(group);
        assert_eq!(waits_held(), 1, "the armed member alone");
        drop(members);
        yield_now().await;
        assert_eq!(waits_held(), 0);
    })
    .unwrap();
}

/// Do: on a shard of four wait slots, a group arms members until one is refused. Expect: three
/// members (the group holding the fourth slot), the next refused `Capacity`; one dropped, another is
/// armed in its place.
#[test]
fn a_groups_members_are_bounded_by_the_shards_wait_slots() {
    let mut rt = LocalRuntime::new(&config(4)).unwrap();
    rt.block_on(async {
        let sockets: Vec<UdpSocket> = (0..4)
            .map(|_| UdpSocket::bind(loopback()).unwrap())
            .collect();
        let mut group = ReadyGroup::new();
        let mut members: Vec<Member> = Vec::new();
        let refused = poll_fn(|cx| {
            for (key, socket) in sockets.iter().enumerate() {
                match socket.readable_in(&mut group, cx, key as u64) {
                    Ok(member) => members.push(member),
                    Err(refusal) => return Poll::Ready(Some((key, refusal))),
                }
            }
            Poll::Ready(None)
        })
        .await;
        assert!(
            matches!(refused, Some((3, RtError::Capacity { .. }))),
            "{refused:?}"
        );
        assert_eq!(members.len(), 3);
        members.pop();
        yield_now().await;
        let again = poll_fn(|cx| Poll::Ready(sockets[3].readable_in(&mut group, cx, 3))).await;
        assert!(again.is_ok(), "{again:?}");
    })
    .unwrap();
}
