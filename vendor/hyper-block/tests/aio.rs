//! Native AIO reads (`hyper_block::aio`) on a real file opened for direct I/O. On Linux they read
//! through the kernel's AIO; elsewhere, and over a buffered file, they are refused, typed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use hyper_block::DiskError;
use hyper_block::aio::AioFile;
use hyper_block::buf::{AlignedBuf, Alignment};
#[cfg(target_os = "linux")]
use hyper_block::file::Caching;
use hyper_block::file::{CachingRequest, DeviceFile};

/// The page every read in these tests moves: the block of the file systems they run on.
const PAGE: usize = 4096;

fn align() -> Alignment {
    Alignment::new(PAGE).unwrap()
}

/// The byte page `i` of the test file is filled with.
fn fill(i: usize) -> u8 {
    u8::try_from(i % 251).unwrap() + 1
}

/// A file of `pages` pages, page `i` filled with `fill(i)`, flushed, opened as `request` asks.
fn file(dir: &std::path::Path, pages: usize, request: CachingRequest) -> DeviceFile {
    let path = dir.join("aio");
    let file = DeviceFile::open(&path, true, request, align()).unwrap();
    let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
    for i in 0..pages {
        buf.clear();
        buf.extend_from_slice(&[fill(i); PAGE]).unwrap();
        file.write_all_at(buf.as_slice(), (i * PAGE) as u64)
            .unwrap();
    }
    file.sync_data().unwrap();
    file
}

#[cfg(target_os = "linux")]
fn reads(pages: &[usize]) -> Vec<(AlignedBuf, u64)> {
    pages
        .iter()
        .map(|&i| {
            let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
            buf.set_len(PAGE).unwrap();
            (buf, (i * PAGE) as u64)
        })
        .collect()
}

/// Do: open AIO reads over a buffered file. Expect: refused, typed, since a buffered AIO read is
/// done inside io_submit; on every OS but Linux, refused over any file.
#[test]
fn a_buffered_file_or_another_os_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let buffered = file(dir.path(), 1, CachingRequest::Buffered);
    assert!(matches!(
        AioFile::new(buffered, 4, 1),
        Err(DiskError::Unsupported { .. })
    ));
    if cfg!(not(target_os = "linux")) {
        let direct = DeviceFile::open(
            &dir.path().join("direct"),
            true,
            CachingRequest::PreferDirect,
            align(),
        )
        .unwrap();
        assert!(matches!(
            AioFile::new(direct, 4, 1),
            Err(DiskError::Unsupported { .. })
        ));
    }
}

/// The direct file, on Linux, where the file system takes direct I/O (the test's directory may
/// be on one that does not, tmpfs among them; then there is nothing to read through AIO).
#[cfg(target_os = "linux")]
fn direct(dir: &std::path::Path, pages: usize) -> Option<DeviceFile> {
    let file = file(dir, pages, CachingRequest::PreferDirect);
    (file.caching() == Caching::Direct).then_some(file)
}

/// Do: read 8 pages in one batch at a depth of 4, so half wait for the first half. Expect: every
/// buffer comes back in the order given, holding its page's bytes.
#[cfg(target_os = "linux")]
#[test]
fn a_batch_deeper_than_the_context_reads_every_page_back_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 64) else {
        return;
    };
    let mut aio = AioFile::new(file, 4, 2).unwrap();
    let pages = [3, 17, 0, 63, 9, 40, 22, 5];
    let number = aio.submit_reads(reads(&pages)).unwrap();
    let (answered, done) = aio.answer().unwrap();
    assert_eq!(answered, number);
    let done = done.unwrap();
    assert_eq!(done.len(), pages.len());
    for ((buf, at), &page) in done.iter().zip(&pages) {
        assert_eq!(*at, (page * PAGE) as u64);
        assert!(
            buf.as_slice().iter().all(|&b| b == fill(page)),
            "page {page}"
        );
    }
    assert_eq!(aio.out(), 0);
}

/// Do: submit two batches, then poll with try_answer until both are answered. Expect: both
/// answered, each with its own pages, and try_answer with nothing out answers none.
#[cfg(target_os = "linux")]
#[test]
fn two_batches_out_at_once_are_both_answered_by_polling() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 16) else {
        return;
    };
    let mut aio = AioFile::new(file, 8, 2).unwrap();
    let first = aio.submit_reads(reads(&[1, 2, 3])).unwrap();
    let second = aio.submit_reads(reads(&[10, 11])).unwrap();
    let mut answers = Vec::new();
    // Polling ends at the fact it needs, both answers, each poll a non-blocking reap.
    while answers.len() < 2 {
        if let Some(answer) = aio.try_answer().unwrap() {
            answers.push(answer);
        }
    }
    answers.sort_by_key(|(n, _)| *n);
    let pages: Vec<Vec<u8>> = answers
        .into_iter()
        .map(|(_, a)| a.unwrap().iter().map(|(b, _)| b.as_slice()[0]).collect())
        .collect();
    assert_eq!(
        pages,
        [vec![fill(1), fill(2), fill(3)], vec![fill(10), fill(11)]]
    );
    let _ = (first, second);
    assert!(aio.try_answer().unwrap().is_none());
}

/// Do: read a page past the end of the file beside one inside it. Expect: the batch fails as a
/// whole, typed (a short read), and nothing is left out.
#[cfg(target_os = "linux")]
#[test]
fn a_read_past_the_end_fails_its_batch() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 2) else {
        return;
    };
    let mut aio = AioFile::new(file, 4, 1).unwrap();
    aio.submit_reads(reads(&[0, 8])).unwrap();
    let (_, answer) = aio.answer().unwrap();
    assert!(matches!(answer, Err(DiskError::ShortRead { .. })));
    assert_eq!(aio.out(), 0);
}

/// Do: submit past the batches allowed out, then a misaligned read. Expect: each refused, typed,
/// before anything is submitted, its reads handed back.
#[cfg(target_os = "linux")]
#[test]
fn a_batch_past_the_bound_or_misaligned_is_refused_and_given_back() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 4) else {
        return;
    };
    let mut aio = AioFile::new(file, 4, 1).unwrap();
    aio.submit_reads(reads(&[0])).unwrap();
    let (refused, given_back) = aio.submit_reads(reads(&[1, 2])).unwrap_err();
    assert!(matches!(refused, DiskError::Unsupported { .. }));
    assert_eq!(given_back.len(), 2);
    aio.answer().unwrap().1.unwrap();
    let mut odd = reads(&[0]);
    odd[0].1 = 512;
    let (refused, _) = aio.submit_reads(odd).unwrap_err();
    assert!(matches!(refused, DiskError::Misaligned { .. }));
    assert_eq!(aio.out(), 0);
}

/// Do: submit a batch and drop the reads with it still out. Expect: the drop waits for the kernel
/// (io_destroy blocks on completion), so no buffer is freed under a read; nothing panics.
#[cfg(target_os = "linux")]
#[test]
fn dropping_with_reads_out_waits_for_them() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 32) else {
        return;
    };
    let mut aio = AioFile::new(file, 16, 1).unwrap();
    aio.submit_reads(reads(&(0..32).collect::<Vec<_>>()))
        .unwrap();
    drop(aio);
}

/// Pages `pages` to write, page `i` filled with `fill(i + 100)`, so they differ from the file's.
#[cfg(target_os = "linux")]
fn writes(pages: &[usize]) -> Vec<(AlignedBuf, u64)> {
    pages
        .iter()
        .map(|&i| {
            let mut buf = AlignedBuf::zeroed(PAGE, align()).unwrap();
            buf.extend_from_slice(&[fill(i + 100); PAGE]).unwrap();
            (buf, (i * PAGE) as u64)
        })
        .collect()
}

/// Whether the running kernel takes `IOCB_CMD_FDSYNC`: Linux 4.18 and after (commit a3c0d439).
#[cfg(target_os = "linux")]
fn kernel_takes_aio_flush() -> bool {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap();
    let mut parts = release
        .trim()
        .split(|c: char| !c.is_ascii_digit())
        .map(|n| n.parse::<u32>().unwrap());
    (parts.next().unwrap(), parts.next().unwrap()) >= (4, 18)
}

/// Do: write 8 pages in one batch at a depth of 4 and ask a flush, then read them back. Expect:
/// the writes' buffers come back in order, the flush goes through the context where the kernel
/// takes one (else in place, once), and every page reads back as written.
#[cfg(target_os = "linux")]
#[test]
fn a_batch_of_writes_with_a_flush_reads_back_as_written() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 16) else {
        return;
    };
    let mut aio = AioFile::new(file, 4, 1).unwrap();
    let pages: Vec<usize> = (4..12).collect();
    let number = aio.submit_writes(writes(&pages), true).unwrap();
    let (answered, given_back) = aio.answer().unwrap();
    assert_eq!(answered, number);
    let given_back = given_back.unwrap();
    let offsets: Vec<u64> = given_back.iter().map(|(_, at)| *at).collect();
    assert_eq!(
        offsets,
        pages.iter().map(|&i| (i * PAGE) as u64).collect::<Vec<_>>()
    );
    let in_place = u64::from(!kernel_takes_aio_flush());
    assert_eq!(aio.flushes_in_place(), in_place);
    aio.submit_reads(reads(&(0..16).collect::<Vec<_>>()))
        .unwrap();
    let (_, read) = aio.answer().unwrap();
    for (i, (buf, _)) in read.unwrap().iter().enumerate() {
        let expect = if pages.contains(&i) {
            fill(i + 100)
        } else {
            fill(i)
        };
        assert!(buf.as_slice().iter().all(|&b| b == expect), "page {i}");
    }
}

/// Whether `dir` is on ext4: its file system type as statfs(2) gives it, `EXT4_SUPER_MAGIC`
/// (0xef53), read through coreutils' `stat --file-system`.
#[cfg(target_os = "linux")]
fn on_ext4(dir: &std::path::Path) -> bool {
    let out = std::process::Command::new("stat")
        .args(["--file-system", "--format=%t"])
        .arg(dir)
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim() == "ef53"
}

/// Do: write a batch with a page past the file system's largest file, asking a flush; then write
/// and flush a good batch. Expect: the first batch fails, typed, and no flush is made for it (none
/// in place); the second is answered, flushed.
#[cfg(target_os = "linux")]
#[test]
fn a_failed_write_fails_its_batch_and_is_not_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 4) else {
        return;
    };
    if !on_ext4(dir.path()) {
        return;
    }
    let mut aio = AioFile::new(file, 4, 1).unwrap();
    let mut bad = writes(&[0]);
    // Page 2^38 starts at 2^50 bytes, past ext4's largest file with 4 KiB blocks (16 TiB): EFBIG.
    bad.extend(writes(&[1 << 38]));
    aio.submit_writes(bad, true).unwrap();
    let (_, answer) = aio.answer().unwrap();
    assert!(matches!(
        answer,
        Err(DiskError::Io {
            op: "aio write",
            ..
        })
    ));
    assert_eq!(aio.flushes_in_place(), 0);
    aio.submit_writes(writes(&[2]), true).unwrap();
    let (_, answer) = aio.answer().unwrap();
    answer.unwrap();
}

/// Do: write a batch of no transfers asking a flush, and one with a page ending past `i64::MAX`.
/// Expect: the first is answered, flushed, with its empty vector; the second is refused before
/// anything is submitted, its writes given back.
#[cfg(target_os = "linux")]
#[test]
fn an_empty_flush_is_answered_and_an_offset_past_the_kernels_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let Some(file) = direct(dir.path(), 1) else {
        return;
    };
    let mut aio = AioFile::new(file, 4, 1).unwrap();
    aio.submit_writes(Vec::new(), true).unwrap();
    let (_, answer) = aio.answer().unwrap();
    assert!(answer.unwrap().is_empty());
    let mut far = writes(&[0]);
    far[0].1 = (i64::MAX as u64) & !(PAGE as u64 - 1);
    let (refused, given_back) = aio.submit_writes(far, true).unwrap_err();
    assert!(matches!(refused, DiskError::Unsupported { .. }));
    assert_eq!(given_back.len(), 1);
    assert_eq!(aio.out(), 0);
}
