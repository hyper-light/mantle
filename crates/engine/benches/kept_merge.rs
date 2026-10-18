//! A kept index's merge alone (`RecordTable::merge`, research/63): memtable-sized lists of the
//! shard benchmark's records merged, oldest first, into one table, as a maintenance worker extends
//! an index copy. The fill draws `DRAWS` keys over `KEYS` (db_bench's fillrandom), cut into lists
//! of `LIST` draws, each list's keys once and in key order, as a packing walks its memtable. Each
//! list is merged as built (`key`), or first ordered by its records' places (`place`), the order
//! a table's slots run in. Nanoseconds a record merged, and the table's bytes.
//! `cargo bench -p mantle-engine --bench kept_merge -- KEYS DRAWS LIST key|place`
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_precision_loss
)]

use std::time::Instant;

use mantle_engine::branch::Op;
use mantle_engine::branch::filter::{hash, hash32};
use mantle_engine::kept_table::{Record, RecordList, RecordTable};

#[path = "support/rocks_workload.rs"]
mod rocks_workload;

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .collect();
    let keys: u64 = args.first().map_or(10_000_000, |a| a.parse().unwrap());
    let draws: u64 = args.get(1).map_or(keys, |a| a.parse().unwrap());
    let list: u64 = args.get(2).map_or(550_000, |a| a.parse().unwrap());
    let place = args.get(3).is_some_and(|a| a == "place");
    let value = [b'v'; 100];
    let mut rng = rocks_workload::Rng::new(301, 1);
    let mut lists = Vec::new();
    let mut made = 0u64;
    while made < draws {
        let n = list.min(draws - made);
        let mut batch: Vec<u64> = (0..n).map(|_| rng.next() % keys).collect();
        made += n;
        batch.sort_unstable_by_key(|&k| rocks_workload::key(k));
        batch.dedup();
        let mut records: Vec<([u8; 16], u32)> = batch
            .iter()
            .map(|&k| {
                let key = rocks_workload::key(k);
                (key, hash32(hash(&key)))
            })
            .collect();
        if place {
            records.sort_unstable_by_key(|&(_, h)| h);
        }
        let mut l = RecordList::default();
        for (key, h) in &records {
            l.push(&Record {
                hash32: *h,
                op: Op::Put,
                key,
                value: &value,
            })
            .unwrap();
        }
        lists.push(l);
    }
    let records: usize = lists.iter().map(RecordList::len).sum();
    let mut table = RecordTable::new();
    let start = Instant::now();
    for (number, l) in lists.iter().enumerate() {
        table.merge(u32::try_from(number).unwrap(), l, 0).unwrap();
    }
    let ns = start.elapsed().as_nanos() as f64;
    println!(
        "kept_merge order {} keys {keys} draws {draws} lists {} records {records} held {} \
         table_mib {:.0} ns_per_record {:.1} total_ms {:.0}",
        if place { "place" } else { "key" },
        lists.len(),
        table.len(),
        table.bytes() as f64 / (1 << 20) as f64,
        ns / records as f64,
        ns / 1e6
    );
}
