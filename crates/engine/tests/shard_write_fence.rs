//! A synchronous page-write failure prevents later in-memory writes being acknowledged.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::Error;
use mantle_engine::rows::Rows;
use mantle_engine::shard_db::ShardDb;
use mantle_engine::store::Config;
use mantle_engine::trunk::TrunkConfig;

const STORE: Config = Config {
    page_size: 4096,
    extent_pages: 4,
    max_extents: 64,
};
const TRUNK: TrunkConfig = TrunkConfig {
    fanout: 3,
    leaf_entries: 96,
};

#[derive(Default)]
struct Fault {
    writes: AtomicUsize,
    reject: AtomicBool,
}

struct File {
    file: DeviceFile,
    fault: Arc<Fault>,
}

impl BlockFile for File {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.fault.writes.fetch_add(1, Ordering::SeqCst);
        if self.fault.reject.load(Ordering::SeqCst) {
            return Err(DiskError::Io {
                op: "synchronous write refused by test",
                path: std::path::PathBuf::new(),
                source: std::io::Error::other("synchronous write refused by test"),
            });
        }
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.file.sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(Self {
            file: self.file.try_clone()?,
            fault: Arc::clone(&self.fault),
        })
    }
}

#[test]
fn a_failed_write_refuses_fitting_puts_and_deletes_before_further_io() {
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(Fault::default());
    let file = File {
        file: DeviceFile::open(
            &dir.path().join("store"),
            true,
            CachingRequest::Buffered,
            Alignment::new(STORE.page_size).unwrap(),
        )
        .unwrap(),
        fault: Arc::clone(&fault),
    };
    let mut db = ShardDb::create(file, STORE, STORE.page_size, TRUNK).unwrap();
    db.put(b"known", b"durable value").unwrap();
    db.checkpoint(1).unwrap();
    fault.reject.store(true, Ordering::SeqCst);
    let value = vec![7; STORE.page_size / 2];
    let mut failed = false;
    // Separate large values fill memtables and packed runs within this input-shaped bound.
    for i in 0..TRUNK.fanout * STORE.extent_pages as usize {
        let key = format!("uncheckpointed-{i}");
        match db.put(key.as_bytes(), &value) {
            Ok(()) => {}
            Err(Error::Io { .. }) => {
                failed = true;
                break;
            }
            Err(error) => panic!("unexpected put failure: {error:?}"),
        }
    }
    assert!(failed, "the injected page write failed on a foreground put");
    let writes = fault.writes.load(Ordering::SeqCst);
    let put = db.put(b"tiny", b"v");
    assert!(matches!(put, Err(Error::Io { .. })), "fitting put: {put:?}");
    assert!(matches!(db.delete(b"known"), Err(Error::Io { .. })));
    assert_eq!(fault.writes.load(Ordering::SeqCst), writes);
    assert!(matches!(db.checkpoint(2), Err(Error::Io { .. })));
    let (file, landed) = db.into_file();
    assert!(landed.is_err());
    fault.reject.store(false, Ordering::SeqCst);
    let (mut recovered, applied) = ShardDb::open(file, STORE, STORE.page_size, TRUNK).unwrap();
    assert_eq!(applied, 1);
    recovered.check_references().unwrap();
    let mut out = Vec::new();
    assert!(recovered.get(b"known", &mut out).unwrap());
    assert_eq!(out, b"durable value");
    assert!(!recovered.get(b"tiny", &mut out).unwrap());
    let mut rows = Rows::new();
    let mut next = Vec::new();
    assert!(
        !recovered
            .scan(
                b"",
                None,
                usize::try_from(TRUNK.leaf_entries).unwrap(),
                &mut rows,
                &mut next
            )
            .unwrap()
    );
    assert_eq!(
        rows.iter().collect::<Vec<_>>(),
        vec![(b"known".as_slice(), b"durable value".as_slice())]
    );
}
