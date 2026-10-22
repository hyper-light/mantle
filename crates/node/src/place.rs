//! The order in which placement offers a block's volumes (docs/design/node.md §6.6;
//! docs/research/63): every volume of the cell's table, one of each failure domain before a second
//! of any, the gateway writing chunk `i` to the `i`-th and taking the next past the block's width
//! for a volume that refuses (gateway.md §2).
//!
//! Each volume is keyed for the block by highest random weight (Thaler and Ravishankar, IEEE/ACM
//! ToN 6(1), 1998, §IV-A) weighted by its bytes as Efraimidis and Spirakis key a weighted sample
//! (IPL 97(5), 2006; Ceph's straw2): `ln(u) / bytes`, for `u` uniform in `(0, 1]` drawn from the
//! block's and the volume's ids, largest first. Each place is then a draw, in proportion to bytes,
//! among the volumes not yet offered, and a volume added to or removed from the table moves no
//! other volume's rank. The first volume of each domain in that order is the domain's own draw,
//! weighted by its total bytes, so domains too come in proportion to what they hold.

use std::collections::BTreeSet;

use mantle_disk::measure::SplitMix64;
use mantle_meta::root::Placed;

/// The volumes of `table` in the order placement offers them for `block`; a volume of no bytes,
/// which can hold no chunk, after every other, whatever its domain.
pub fn order(block: u128, table: &[Placed]) -> Vec<u128> {
    let ranked = ranked(block, table);
    let mut first = Vec::with_capacity(ranked.len());
    let mut rest = Vec::new();
    let mut empty = Vec::new();
    let mut domains: BTreeSet<&str> = BTreeSet::new();
    for placed in ranked {
        if placed.bytes == 0 {
            empty.push(placed.volume);
        } else if domains.insert(placed.domain.as_str()) {
            first.push(placed.volume);
        } else {
            rest.push(placed.volume);
        }
    }
    first.extend(rest);
    first.extend(empty);
    first
}

/// The volumes of `table` by their keys for `block`, largest first; equal keys, which only volumes
/// of no bytes share past chance, by id.
fn ranked(block: u128, table: &[Placed]) -> Vec<&Placed> {
    let mut keyed: Vec<(f64, &Placed)> = table
        .iter()
        .map(|placed| (key(block, placed.volume, placed.bytes), placed))
        .collect();
    keyed.sort_by(|(a, x), (b, y)| b.total_cmp(a).then(x.volume.cmp(&y.volume)));
    keyed.into_iter().map(|(_, placed)| placed).collect()
}

/// `volume`'s key for `block`: `ln(u) / bytes`, `u` uniform in `(0, 1]` from 53 bits of the ids'
/// draw; a volume of no bytes keyed below every other.
fn key(block: u128, volume: u128, bytes: u64) -> f64 {
    if bytes == 0 {
        return f64::NEG_INFINITY;
    }
    // (0, 1]: the draw's top 53 bits, one more, over 2^53, each exact in an f64.
    let top = draw(block, volume) >> 11;
    let u = top.saturating_add(1) as f64 / (1u64 << 53) as f64;
    u.ln() / bytes as f64
}

/// A uniform 64-bit draw for the pair: SplitMix64's output function (Steele, Lea and Flood,
/// OOPSLA 2014) chained over the ids' 64-bit halves.
fn draw(block: u128, volume: u128) -> u64 {
    let half =
        |id: u128, shift: u32| u64::try_from((id >> shift) & u128::from(u64::MAX)).unwrap_or(0);
    [
        half(block, 0),
        half(block, 64),
        half(volume, 0),
        half(volume, 64),
    ]
    .into_iter()
    .fold(0, |x, word| SplitMix64::new(x ^ word).next_u64())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placed(volume: u128, domain: &str, bytes: u64) -> Placed {
        Placed {
            volume,
            node: 0,
            domain: domain.into(),
            bytes,
        }
    }

    /// Three domains: `a` with three volumes, `b` with two, `c` with one, sizes apart.
    fn table() -> Vec<Placed> {
        vec![
            placed(11, "a", 1 << 30),
            placed(12, "a", 2 << 30),
            placed(13, "a", 1 << 29),
            placed(21, "b", 4 << 30),
            placed(22, "b", 1 << 30),
            placed(31, "c", 3 << 30),
        ]
    }

    #[test]
    fn every_volume_once_and_one_of_each_domain_before_a_second() {
        let table = table();
        for block in 0..2_000u128 {
            let block = block.wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
            let order = order(block, &table);
            let mut sorted = order.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, vec![11, 12, 13, 21, 22, 31]);
            let domains: BTreeSet<u128> = order[..3].iter().map(|v| v / 10).collect();
            assert_eq!(domains.len(), 3, "block {block:x}: {order:?}");
        }
    }

    #[test]
    fn the_same_block_and_table_give_the_same_order_whatever_the_tables_order() {
        let table = table();
        let mut reversed = table.clone();
        reversed.reverse();
        for block in [0u128, 1, u128::MAX, 0x1234_5678_9abc_def0] {
            assert_eq!(order(block, &table), order(block, &reversed));
        }
    }

    #[test]
    fn a_volume_of_no_bytes_comes_after_every_other() {
        let mut table = table();
        table.push(placed(41, "d", 0));
        for block in 0..500u128 {
            assert_eq!(order(block, &table).last(), Some(&41));
        }
    }

    #[test]
    fn a_volume_leaving_the_table_moves_no_other_volumes_rank() {
        let table = table();
        for gone in 0..table.len() {
            let mut fewer = table.clone();
            let left = fewer.remove(gone).volume;
            for block in 0..500u128 {
                let before: Vec<u128> = ranked(block, &table)
                    .into_iter()
                    .map(|p| p.volume)
                    .filter(|v| *v != left)
                    .collect();
                let after: Vec<u128> = ranked(block, &fewer)
                    .into_iter()
                    .map(|p| p.volume)
                    .collect();
                assert_eq!(before, after, "block {block}, {left} gone");
            }
        }
    }
}
