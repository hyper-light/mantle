//! Complete cold recovery verification, after measured query and retirement intervals.
use hyper_block::block::BlockFile;
use mantle_engine::Error;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;

pub fn verify<F: BlockFile>(
    db: &mut ShardDb<F>,
    present: &super::present::Present,
    value: &[u8],
    limit: usize,
) -> Result<(), Error> {
    let bad = || Error::InvalidArgument {
        what: "benchmark recovered key/value/continuation differs from fill oracle",
    };
    if limit == 0 {
        return Err(bad());
    }
    db.check_references()?;
    let mut from = Vec::new();
    let mut rows = Rows::new();
    let mut next = Vec::new();
    let mut keys = present.iter();
    let mut at = 0;
    // Even one-row pages finish in at most the expected cardinality plus one terminal page.
    for _ in 0..=present.len() {
        rows.clear();
        let more = db.scan(&from, None, limit, &mut rows, &mut next)?;
        for (actual_key, actual_value) in rows.iter() {
            let number = keys.next().ok_or_else(bad)?;
            if actual_key != super::rocks_workload::key(number) || actual_value != value {
                return Err(bad());
            }
            at += 1;
        }
        if !more {
            if at != present.len() {
                return Err(bad());
            }
            return db.check_references();
        }
        if next.as_slice() <= from.as_slice() {
            return Err(bad());
        }
        from.clear();
        from.extend_from_slice(&next);
    }
    Err(bad())
}
