//! db/log_test.cc, ported test for test: `LogTest` (36), `RetriableLogTest` (3),
//! `CompressionLogTest` (7) and `StreamingCompressionTest` (1). Each C++ `TEST_P` runs here over
//! the same parameter sets as its `INSTANTIATE_TEST_CASE_P`.
//!
//! Differences from the C++ harness: `Read` does not check the record checksum RocksDB can
//! return (the port does not compute it; see db/log_reader.rs), and `RetriableLogTest`'s two
//! `TailLog` tests, which order a writer and a reader thread with sync points, run the same
//! order on one thread: the writer's first part, a read that must stop at EOF, the second
//! part, then the read that must return the record.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::collections::HashMap;

use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use mantle_engine::Error;
use mantle_engine::db::log_format::{
    BLOCK_SIZE, FIRST_TYPE, HEADER_SIZE, LAST_TYPE, MIDDLE_TYPE, RECORD_TYPE_SAFE_IGNORE_MASK,
    RECYCLABLE_FIRST_TYPE, RECYCLABLE_HEADER_SIZE, RECYCLABLE_LAST_TYPE, RECYCLABLE_MIDDLE_TYPE,
    WalRecoveryMode,
};
use mantle_engine::db::log_reader::{DropReason, FragmentBufferedReader, Reader, Reporter};
use mantle_engine::db::log_writer::{Writer, WriterOptions};
use mantle_engine::file::{
    BlockSequentialFile, BlockWritableFile, ReadFailure, SequentialFile, WritableFile,
    WritableFileWriter,
};
use mantle_engine::util::coding::encode_fixed32;
use mantle_engine::util::compression::{CompressionType, StreamingCompress, StreamingUncompress};
use mantle_engine::util::crc32c;

const TOLERATE: WalRecoveryMode = WalRecoveryMode::TolerateCorruptedTailRecords;
const ABSOLUTE: WalRecoveryMode = WalRecoveryMode::AbsoluteConsistency;

/// util/random.h's `Random`: Park and Miller's minimal standard generator.
struct Random(u32);

impl Random {
    const M: u32 = 2147483647;

    fn new(s: u32) -> Self {
        Self(if s & Self::M != 0 { s & Self::M } else { 1 })
    }

    fn next(&mut self) -> u32 {
        let product = u64::from(self.0) * 16807;
        self.0 = ((product >> 31) + (product & u64::from(Self::M))) as u32;
        if self.0 > Self::M {
            self.0 -= Self::M;
        }
        self.0
    }

    fn uniform(&mut self, n: u32) -> u32 {
        self.next() % n
    }

    fn skewed(&mut self, max_log: u32) -> u32 {
        let bits = self.uniform(max_log + 1);
        self.uniform(1 << bits)
    }

    /// `RandomBinaryString`: bytes below CHAR_MAX, 127 where `char` is signed.
    fn random_binary_string(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.uniform(127) as u8).collect()
    }
}

fn big_string(partial: &str, n: usize) -> Vec<u8> {
    partial.bytes().cycle().take(n).collect()
}

fn number_string(n: usize) -> Vec<u8> {
    format!("{n}.").into_bytes()
}

fn random_skewed_string(i: usize, rnd: &mut Random) -> Vec<u8> {
    let n = rnd.skewed(17) as usize;
    big_string(&String::from_utf8(number_string(i)).unwrap(), n)
}

/// The file the readers read. RocksDB's `StringSource` reads a slice of its sink's string; here
/// the source owns the bytes, and the fixture hands it what each sink flushed before every read
/// or edit ([`LogTest::deliver`]), so each byte moves once and nothing is shared. RocksDB's
/// `reader_contents_` is the unread, flushed part, `contents[read_pos..read_end]`.
#[derive(Default)]
struct StringSource {
    contents: Vec<u8>,
    read_pos: usize,
    read_end: usize,
    force_error: bool,
    force_error_position: usize,
    force_eof: bool,
    force_eof_position: usize,
    returned_partial: bool,
    fail_after_read_partial: bool,
}

/// test_util/testutil.h's `StringSink`: what was appended and not yet handed to the source,
/// the first `flushed` bytes of it flushed.
#[derive(Default)]
struct StringSink {
    pending: Vec<u8>,
    flushed: usize,
}

impl WritableFile for StringSink {
    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        self.pending.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.flushed = self.pending.len();
        Ok(())
    }

    fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// test_util/testutil.h's `OverwritingStringSink`: its flushed bytes go over the reader's unread
/// ones, from where the reader stands.
#[derive(Default)]
struct OverwritingStringSink {
    contents: Vec<u8>,
    last_flush: usize,
    delivered: usize,
}

impl WritableFile for OverwritingStringSink {
    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        self.contents.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.last_flush = self.contents.len();
        Ok(())
    }

    fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// log_test.cc's `StringSource::Read`.
impl SequentialFile for StringSource {
    fn read(&mut self, scratch: &mut [u8]) -> Result<usize, ReadFailure> {
        let s = self;
        if s.fail_after_read_partial {
            assert!(!s.returned_partial, "must not Read() after eof/error");
        }
        let mut n = scratch.len();
        if s.force_error {
            if s.force_error_position >= n {
                s.force_error_position -= n;
            } else {
                let k = s.force_error_position;
                let from = s.read_pos;
                scratch[..k].copy_from_slice(&s.contents[from..from + k]);
                s.read_pos += k;
                s.force_error = false;
                s.returned_partial = true;
                return Err(ReadFailure {
                    filled: k,
                    error: Error::Io {
                        op: "read",
                        detail: "read error".to_owned(),
                    },
                });
            }
        }
        let unread = s.read_end - s.read_pos;
        if unread < n {
            n = unread;
            s.returned_partial = true;
        }
        if s.force_eof {
            if s.force_eof_position >= n {
                s.force_eof_position -= n;
            } else {
                s.force_eof = false;
                n = s.force_eof_position;
                s.returned_partial = true;
            }
        }
        let from = s.read_pos;
        scratch[..n].copy_from_slice(&s.contents[from..from + n]);
        s.read_pos += n;
        Ok(n)
    }

    fn file_name(&self) -> &str {
        ""
    }
}

/// log_test.cc's `ReportCollector`.
#[derive(Default)]
struct ReportCollector {
    dropped_bytes: usize,
    message: String,
}

impl Reporter for ReportCollector {
    fn corruption(&mut self, bytes: usize, reason: &DropReason, _log_number: Option<u64>) {
        self.dropped_bytes += bytes;
        self.message.push_str(&reason.to_string());
    }
}

enum AnyReader<F> {
    Plain(Reader<F, ReportCollector>),
    Retry(FragmentBufferedReader<F, ReportCollector>),
}

impl<F: SequentialFile> AnyReader<F> {
    fn read_record(&mut self, record: &mut Vec<u8>, mode: WalRecoveryMode) -> bool {
        match self {
            Self::Plain(r) => r.read_record(record, mode),
            Self::Retry(r) => r.read_record(record, mode),
        }
    }

    fn reporter(&self) -> &ReportCollector {
        match self {
            Self::Plain(r) => r.reporter(),
            Self::Retry(r) => r.reporter(),
        }
    }

    fn unmark_eof(&mut self) {
        match self {
            Self::Plain(r) => r.unmark_eof(),
            Self::Retry(r) => r.unmark_eof(),
        }
    }

    fn file_mut(&mut self) -> &mut F {
        match self {
            Self::Plain(r) => r.file_mut(),
            Self::Retry(r) => r.file_mut(),
        }
    }

    fn is_eof(&self) -> bool {
        match self {
            Self::Plain(r) => r.is_eof(),
            Self::Retry(r) => r.is_eof(),
        }
    }

    fn recorded_timestamp_size(&self) -> HashMap<u32, usize> {
        match self {
            Self::Plain(r) => r.recorded_timestamp_size().clone(),
            Self::Retry(r) => r.recorded_timestamp_size().clone(),
        }
    }
}

const EOF: &[u8] = b"EOF";

/// log_test.cc's `LogTest` fixture. It owns the writer, the reader (which owns the file) and the
/// second writer a recycled log gets, and hands the file each writer's flushed bytes before the
/// file is read, measured or edited.
struct LogTest {
    writer: Writer<StringSink>,
    reader: AnyReader<StringSource>,
    overwriter: Option<Writer<OverwritingStringSink>>,
    recyclable: bool,
    allow_retry_read: bool,
    compression: CompressionType,
}

impl LogTest {
    fn new(recyclable: bool, allow_retry_read: bool, compression: CompressionType) -> Self {
        let mut options = WriterOptions::new(123, recyclable);
        options.compression_type = compression;
        let writer = Writer::new(WritableFileWriter::new(StringSink::default()), options).unwrap();
        let source = StringSource {
            fail_after_read_partial: !allow_retry_read,
            ..StringSource::default()
        };
        let reader = if allow_retry_read {
            AnyReader::Retry(FragmentBufferedReader::new(
                source,
                ReportCollector::default(),
                true,
                123,
            ))
        } else {
            AnyReader::Plain(Reader::new(source, ReportCollector::default(), true, 123))
        };
        Self {
            writer,
            reader,
            overwriter: None,
            recyclable,
            allow_retry_read,
            compression,
        }
    }

    /// `INSTANTIATE_TEST_CASE_P(Log, LogTest, ...)`: recycling 0/1 by retry false/true, no
    /// compression.
    fn each(f: impl Fn(LogTest)) {
        for recyclable in [false, true] {
            for retry in [false, true] {
                f(LogTest::new(
                    recyclable,
                    retry,
                    CompressionType::NoCompression,
                ));
            }
        }
    }

    /// `INSTANTIATE_TEST_CASE_P(Compression, CompressionLogTest, ...)`, each with
    /// `SetupTestEnv` (the compression type record) done.
    fn each_compression(f: impl Fn(LogTest)) {
        for recyclable in [false, true] {
            for retry in [false, true] {
                for c in [CompressionType::NoCompression, CompressionType::Zstd] {
                    let mut t = LogTest::new(recyclable, retry, c);
                    t.writer.add_compression_type_record().unwrap();
                    f(t);
                }
            }
        }
    }

    /// The file, with every flushed byte handed to it: the writer's appended at its end and the
    /// end made readable, as `StringSink::Flush` does; the second writer's laid over the unread
    /// bytes from where the reader stands, as `OverwritingStringSink::Flush` does.
    fn deliver(&mut self) -> &mut StringSource {
        let sink = self.writer.file_mut().unwrap().file_mut();
        let flushed = sink.flushed;
        let moved: Vec<u8> = sink.pending.drain(..flushed).collect();
        sink.flushed = 0;
        let over = self
            .overwriter
            .as_mut()
            .map(|w| w.file_mut().unwrap().file_mut());
        let file = self.reader.file_mut();
        if !moved.is_empty() {
            file.contents.extend_from_slice(&moved);
            file.read_end = file.contents.len();
        }
        if let Some(sink) = over
            && sink.delivered < sink.last_flush
        {
            assert!(file.read_end - file.read_pos >= sink.last_flush);
            let base = file.read_pos;
            for i in sink.delivered..sink.last_flush {
                file.contents[base + i] = sink.contents[i];
            }
            sink.delivered = sink.last_flush;
        }
        file
    }

    /// A second writer of log 123 over the same bytes, as `ReuseWritableFile` reuses a file.
    fn recycle_writer(&mut self) -> &mut Writer<OverwritingStringSink> {
        let w = Writer::new(
            WritableFileWriter::new(OverwritingStringSink::default()),
            WriterOptions::new(123, true),
        )
        .unwrap();
        self.overwriter.insert(w)
    }

    fn header_size(&self) -> usize {
        if self.recyclable {
            RECYCLABLE_HEADER_SIZE
        } else {
            HEADER_SIZE
        }
    }

    fn write(&mut self, msg: &[u8]) {
        self.writer.add_record(msg, 0).unwrap();
    }

    fn write_ts(&mut self, msg: &[u8], cf_to_ts_sz: &[(u32, usize)]) {
        self.writer
            .maybe_add_user_defined_timestamp_size_record(cf_to_ts_sz)
            .unwrap();
        self.write(msg);
    }

    /// `dest_contents().size()`: everything the writer appended, flushed or not.
    fn written_bytes(&mut self) -> usize {
        let delivered = self.deliver().contents.len();
        delivered + self.writer.file().unwrap().file().pending.len()
    }

    /// The flushed bytes the reader has not read.
    fn unread_bytes(&mut self) -> usize {
        let file = self.deliver();
        file.read_end - file.read_pos
    }

    fn read_mode(&mut self, mode: WalRecoveryMode) -> Vec<u8> {
        self.deliver();
        let mut record = Vec::new();
        if self.reader.read_record(&mut record, mode) {
            record
        } else {
            EOF.to_vec()
        }
    }

    fn read(&mut self) -> Vec<u8> {
        self.read_mode(TOLERATE)
    }

    fn increment_byte(&mut self, offset: usize, delta: u8) {
        let s = self.deliver();
        s.contents[offset] = s.contents[offset].wrapping_add(delta);
    }

    fn set_byte(&mut self, offset: usize, new_byte: u8) {
        self.deliver().contents[offset] = new_byte;
    }

    /// `StringSink::Drop`, which RocksDB calls with everything flushed.
    fn shrink_size(&mut self, bytes: usize) {
        self.deliver();
        assert!(self.writer.file().unwrap().file().pending.is_empty());
        let s = self.reader.file_mut();
        let len = s.contents.len() - bytes;
        s.contents.truncate(len);
        s.read_end -= bytes;
    }

    fn fix_checksum(&mut self, header_offset: usize, len: usize, recyclable: bool) {
        let header_size = if recyclable {
            RECYCLABLE_HEADER_SIZE
        } else {
            HEADER_SIZE
        };
        let s = self.deliver();
        let crc = crc32c::mask(crc32c::value(
            &s.contents[header_offset + 6..header_offset + header_size + len],
        ));
        s.contents[header_offset..header_offset + 4].copy_from_slice(&encode_fixed32(crc));
    }

    fn force_error(&mut self, position: usize) {
        let s = self.deliver();
        s.force_error = true;
        s.force_error_position = position;
    }

    fn force_eof(&mut self, position: usize) {
        let s = self.deliver();
        s.force_eof = true;
        s.force_eof_position = position;
    }

    fn unmark_eof(&mut self) {
        self.deliver().returned_partial = false;
        self.reader.unmark_eof();
    }

    fn dropped_bytes(&self) -> usize {
        self.reader.reporter().dropped_bytes
    }

    fn report_message(&self) -> String {
        self.reader.reporter().message.clone()
    }

    /// "OK" iff the recorded message contains `msg`.
    fn match_error(&self, msg: &str) -> String {
        let m = self.report_message();
        if m.contains(msg) { "OK".to_owned() } else { m }
    }

    fn check_record_and_timestamp_size(&mut self, record: &[u8], expected: &HashMap<u32, usize>) {
        assert_eq!(record, &self.read()[..]);
        assert_eq!(expected, &self.reader.recorded_timestamp_size());
    }
}

fn map(entries: &[(u32, usize)]) -> HashMap<u32, usize> {
    entries.iter().copied().collect()
}

#[test]
fn log_test_empty() {
    LogTest::each(|mut t| assert_eq!(EOF, t.read()));
}

#[test]
fn log_test_read_write() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.write(b"bar");
        t.write(b"");
        t.write(b"xxxx");
        assert_eq!(b"foo", &t.read()[..]);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(b"", &t.read()[..]);
        assert_eq!(b"xxxx", &t.read()[..]);
        assert_eq!(EOF, t.read());
        assert_eq!(EOF, t.read()); // Make sure reads at eof work
    });
}

fn read_write_with_timestamp_size(mut t: LogTest) {
    let ts_sz_one = [(1, 8)];
    t.write_ts(b"foo", &ts_sz_one);
    t.write(b"bar");
    let ts_sz_two = [(2, 1)];
    t.write_ts(b"", &ts_sz_two);
    t.write(b"xxxx");

    t.check_record_and_timestamp_size(b"foo", &map(&ts_sz_one));
    t.check_record_and_timestamp_size(b"bar", &map(&ts_sz_one));
    // Timestamp size records accumulate and apply to the records after them.
    let expected_two = map(&[(1, 8), (2, 1)]);
    t.check_record_and_timestamp_size(b"", &expected_two);
    t.check_record_and_timestamp_size(b"xxxx", &expected_two);
    assert_eq!(EOF, t.read());
    assert_eq!(EOF, t.read());
}

#[test]
fn log_test_read_write_with_timestamp_size() {
    LogTest::each(read_write_with_timestamp_size);
}

#[test]
fn log_test_read_write_with_timestamp_size_zero_timestamp_ignored() {
    LogTest::each(|mut t| {
        let ts_sz_one = [(1, 8)];
        t.write_ts(b"foo", &ts_sz_one);
        let ts_sz_two = [(1, 8), (2, 0)];
        t.write_ts(b"bar", &ts_sz_two);

        t.check_record_and_timestamp_size(b"foo", &map(&ts_sz_one));
        t.check_record_and_timestamp_size(b"bar", &map(&ts_sz_one));
        assert_eq!(EOF, t.read());
        assert_eq!(EOF, t.read());
    });
}

fn many_blocks(mut t: LogTest) {
    for i in 0..100_000 {
        t.write(&number_string(i));
    }
    for i in 0..100_000 {
        assert_eq!(number_string(i), t.read());
    }
    assert_eq!(EOF, t.read());
}

#[test]
fn log_test_many_blocks() {
    LogTest::each(many_blocks);
}

#[test]
fn log_test_fragmentation() {
    LogTest::each(|mut t| {
        t.write(b"small");
        t.write(&big_string("medium", 50000));
        t.write(&big_string("large", 100000));
        assert_eq!(b"small", &t.read()[..]);
        assert_eq!(big_string("medium", 50000), t.read());
        assert_eq!(big_string("large", 100000), t.read());
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_marginal_trailer() {
    LogTest::each(|mut t| {
        // A trailer exactly the length of an empty record.
        let header_size = t.header_size();
        let n = BLOCK_SIZE - 2 * header_size;
        t.write(&big_string("foo", n));
        assert_eq!(BLOCK_SIZE - header_size, t.written_bytes());
        t.write(b"");
        t.write(b"bar");
        assert_eq!(big_string("foo", n), t.read());
        assert_eq!(b"", &t.read()[..]);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_marginal_trailer2() {
    LogTest::each(|mut t| {
        let header_size = t.header_size();
        let n = BLOCK_SIZE - 2 * header_size;
        t.write(&big_string("foo", n));
        assert_eq!(BLOCK_SIZE - header_size, t.written_bytes());
        t.write(b"bar");
        assert_eq!(big_string("foo", n), t.read());
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
        assert_eq!(0, t.dropped_bytes());
        assert_eq!("", t.report_message());
    });
}

#[test]
fn log_test_short_trailer() {
    LogTest::each(|mut t| {
        let header_size = t.header_size();
        let n = BLOCK_SIZE - 2 * header_size + 4;
        t.write(&big_string("foo", n));
        assert_eq!(BLOCK_SIZE - header_size + 4, t.written_bytes());
        t.write(b"");
        t.write(b"bar");
        assert_eq!(big_string("foo", n), t.read());
        assert_eq!(b"", &t.read()[..]);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_aligned_eof() {
    LogTest::each(|mut t| {
        let header_size = t.header_size();
        let n = BLOCK_SIZE - 2 * header_size + 4;
        t.write(&big_string("foo", n));
        assert_eq!(BLOCK_SIZE - header_size + 4, t.written_bytes());
        assert_eq!(big_string("foo", n), t.read());
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_random_read() {
    LogTest::each(|mut t| {
        const N: usize = 500;
        let mut write_rnd = Random::new(301);
        for i in 0..N {
            t.write(&random_skewed_string(i, &mut write_rnd));
        }
        let mut read_rnd = Random::new(301);
        for i in 0..N {
            assert_eq!(random_skewed_string(i, &mut read_rnd), t.read());
        }
        assert_eq!(EOF, t.read());
    });
}

// Tests of all the error paths in log_reader.cc follow:

#[test]
fn log_test_read_error() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.force_error(0);
        assert_eq!(EOF, t.read());
        assert_eq!(BLOCK_SIZE, t.dropped_bytes());
        assert_eq!("OK", t.match_error("read error"));
    });
}

#[test]
fn log_test_bad_record_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        // Type is stored in header[6]
        t.increment_byte(6, 100);
        t.fix_checksum(0, 3, false);
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("unknown record type"));
    });
}

#[test]
fn log_test_ignorable_record_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.set_byte(6, RECORD_TYPE_SAFE_IGNORE_MASK + 100);
        t.fix_checksum(0, 3, false);
        assert_eq!(EOF, t.read());
        // The new type has value 228 and is ignorable when unknown.
        assert_eq!(0, t.dropped_bytes());
        assert_eq!("", t.report_message());
    });
}

#[test]
fn log_test_truncated_trailing_record_is_ignored() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.shrink_size(4); // Drop all payload as well as a header byte
        assert_eq!(EOF, t.read());
        // Truncated last record is ignored, not treated as an error
        assert_eq!(0, t.dropped_bytes());
        assert_eq!("", t.report_message());
    });
}

#[test]
fn log_test_truncated_trailing_record_is_not_ignored() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            // With retries a truncated trailing record is not an error.
            return;
        }
        t.write(b"foo");
        t.shrink_size(4);
        assert_eq!(EOF, t.read_mode(ABSOLUTE));
        assert!(t.dropped_bytes() > 0);
        assert_eq!("OK", t.match_error("Corruption: truncated header"));
    });
}

#[test]
fn log_test_bad_length() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            // With retries a length past the data read so far is a record not yet written.
            return;
        }
        let payload_size = BLOCK_SIZE - t.header_size();
        t.write(&big_string("bar", payload_size));
        t.write(b"foo");
        // Least significant size byte is stored in header[4].
        t.increment_byte(4, 1);
        if !t.recyclable {
            assert_eq!(b"foo", &t.read()[..]);
            assert_eq!(BLOCK_SIZE, t.dropped_bytes());
            assert_eq!("OK", t.match_error("bad record length"));
        } else {
            assert_eq!(EOF, t.read());
        }
    });
}

#[test]
fn log_test_bad_length_at_end_is_ignored() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            return;
        }
        t.write(b"foo");
        t.shrink_size(1);
        assert_eq!(EOF, t.read());
        assert_eq!(0, t.dropped_bytes());
        assert_eq!("", t.report_message());
    });
}

#[test]
fn log_test_bad_length_at_end_is_not_ignored() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            return;
        }
        t.write(b"foo");
        t.shrink_size(1);
        assert_eq!(EOF, t.read_mode(ABSOLUTE));
        assert!(t.dropped_bytes() > 0);
        assert_eq!("OK", t.match_error("Corruption: truncated record body"));
    });
}

#[test]
fn log_test_checksum_mismatch() {
    LogTest::each(|mut t| {
        t.write(b"foooooo");
        t.increment_byte(0, 14);
        assert_eq!(EOF, t.read());
        if !t.recyclable {
            assert_eq!(14, t.dropped_bytes());
            assert_eq!("OK", t.match_error("checksum mismatch"));
        } else {
            assert_eq!(0, t.dropped_bytes());
            assert_eq!("", t.report_message());
        }
    });
}

#[test]
fn log_test_unexpected_middle_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        let r = t.recyclable;
        t.set_byte(
            6,
            if r {
                RECYCLABLE_MIDDLE_TYPE
            } else {
                MIDDLE_TYPE
            },
        );
        t.fix_checksum(0, 3, r);
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("missing start"));
    });
}

#[test]
fn log_test_unexpected_last_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        let r = t.recyclable;
        t.set_byte(6, if r { RECYCLABLE_LAST_TYPE } else { LAST_TYPE });
        t.fix_checksum(0, 3, r);
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("missing start"));
    });
}

#[test]
fn log_test_unexpected_full_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.write(b"bar");
        let r = t.recyclable;
        t.set_byte(6, if r { RECYCLABLE_FIRST_TYPE } else { FIRST_TYPE });
        t.fix_checksum(0, 3, r);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("partial record without end"));
    });
}

#[test]
fn log_test_unexpected_first_type() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.write(&big_string("bar", 100000));
        let r = t.recyclable;
        t.set_byte(6, if r { RECYCLABLE_FIRST_TYPE } else { FIRST_TYPE });
        t.fix_checksum(0, 3, r);
        assert_eq!(big_string("bar", 100000), t.read());
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("partial record without end"));
    });
}

#[test]
fn log_test_missing_last_is_ignored() {
    LogTest::each(|mut t| {
        t.write(&big_string("bar", BLOCK_SIZE));
        // Remove the LAST block, including header.
        t.shrink_size(14);
        assert_eq!(EOF, t.read());
        assert_eq!("", t.report_message());
        assert_eq!(0, t.dropped_bytes());
    });
}

#[test]
fn log_test_missing_last_is_not_ignored() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            return;
        }
        t.write(&big_string("bar", BLOCK_SIZE));
        t.shrink_size(14);
        assert_eq!(EOF, t.read_mode(ABSOLUTE));
        assert!(t.dropped_bytes() > 0);
        assert_eq!(
            "OK",
            t.match_error("Corruption: error reading trailing data")
        );
    });
}

#[test]
fn log_test_partial_last_is_ignored() {
    LogTest::each(|mut t| {
        t.write(&big_string("bar", BLOCK_SIZE));
        // Cause a bad record length in the LAST block.
        t.shrink_size(1);
        assert_eq!(EOF, t.read());
        assert_eq!("", t.report_message());
        assert_eq!(0, t.dropped_bytes());
    });
}

#[test]
fn log_test_partial_last_is_not_ignored() {
    LogTest::each(|mut t| {
        if t.allow_retry_read {
            return;
        }
        t.write(&big_string("bar", BLOCK_SIZE));
        t.shrink_size(1);
        assert_eq!(EOF, t.read_mode(ABSOLUTE));
        assert!(t.dropped_bytes() > 0);
        assert_eq!("OK", t.match_error("Corruption: truncated record body"));
    });
}

#[test]
fn log_test_error_joins_records() {
    LogTest::each(|mut t| {
        // Two fragmented records, first(R1) last(R1) first(R2) last(R2), lose their middle
        // fragments; first(R1) and last(R2) must not be joined into a record.
        t.write(&big_string("foo", BLOCK_SIZE));
        t.write(&big_string("bar", BLOCK_SIZE));
        t.write(b"correct");
        // Wipe the middle block
        for offset in BLOCK_SIZE..2 * BLOCK_SIZE {
            t.set_byte(offset, b'x');
        }
        if !t.recyclable {
            assert_eq!(b"correct", &t.read()[..]);
            assert_eq!(EOF, t.read());
            let dropped = t.dropped_bytes();
            assert!(dropped <= 2 * BLOCK_SIZE + 100);
            assert!(dropped >= 2 * BLOCK_SIZE);
        } else {
            assert_eq!(EOF, t.read());
        }
    });
}

#[test]
fn log_test_clear_eof_single_block() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.write(b"bar");
        let header_size = t.header_size();
        t.force_eof(3 + header_size + 2);
        assert_eq!(b"foo", &t.read()[..]);
        t.unmark_eof();
        assert_eq!(b"bar", &t.read()[..]);
        assert!(t.reader.is_eof());
        assert_eq!(EOF, t.read());
        t.write(b"xxx");
        t.unmark_eof();
        assert_eq!(b"xxx", &t.read()[..]);
        assert!(t.reader.is_eof());
    });
}

#[test]
fn log_test_clear_eof_multi_block() {
    LogTest::each(|mut t| {
        let num_full_blocks = 5;
        let header_size = t.header_size();
        let n = (BLOCK_SIZE - header_size) * num_full_blocks + 25;
        t.write(&big_string("foo", n));
        t.write(&big_string("bar", n));
        t.force_eof(n + num_full_blocks * header_size + header_size + 3);
        assert_eq!(big_string("foo", n), t.read());
        assert!(t.reader.is_eof());
        t.unmark_eof();
        assert_eq!(big_string("bar", n), t.read());
        assert!(t.reader.is_eof());
        t.write(&big_string("xxx", n));
        t.unmark_eof();
        assert_eq!(big_string("xxx", n), t.read());
        assert!(t.reader.is_eof());
    });
}

#[test]
fn log_test_clear_eof_error() {
    LogTest::each(|mut t| {
        // A read error in UnmarkEOF leaves the buffered records readable, then the end.
        t.write(b"foo");
        t.write(b"bar");
        t.unmark_eof();
        assert_eq!(b"foo", &t.read()[..]);
        assert!(t.reader.is_eof());
        t.write(b"xxx");
        t.force_error(0);
        t.unmark_eof();
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_clear_eof_error2() {
    LogTest::each(|mut t| {
        t.write(b"foo");
        t.write(b"bar");
        t.unmark_eof();
        assert_eq!(b"foo", &t.read()[..]);
        t.write(b"xxx");
        t.force_error(3);
        t.unmark_eof();
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
        assert_eq!(3, t.dropped_bytes());
        assert_eq!("OK", t.match_error("read error"));
    });
}

fn fill_two_blocks(t: &mut LogTest) {
    while t.unread_bytes() < BLOCK_SIZE * 2 {
        t.write(b"xxxxxxxxxxxxxxxx");
    }
}

#[test]
fn log_test_recycle() {
    LogTest::each(|mut t| {
        if !t.recyclable {
            return; // test is only valid for recycled logs
        }
        for r in [&b"foo"[..], b"bar", b"baz", b"bif", b"blitz"] {
            t.write(r);
        }
        fill_two_blocks(&mut t);
        let w = t.recycle_writer();
        w.add_record(b"foooo", 0).unwrap();
        w.add_record(b"bar", 0).unwrap();
        assert!(t.written_bytes() >= BLOCK_SIZE * 2);
        assert_eq!(b"foooo", &t.read()[..]);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_recycle_with_timestamp_size() {
    LogTest::each(|mut t| {
        if !t.recyclable {
            return;
        }
        t.write_ts(b"foo", &[(1, 4)]);
        for r in [&b"bar"[..], b"baz", b"bif", b"blitz"] {
            t.write(r);
        }
        fill_two_blocks(&mut t);
        let w = t.recycle_writer();
        let ts_sz_two = [(2, 8)];
        w.maybe_add_user_defined_timestamp_size_record(&ts_sz_two)
            .unwrap();
        w.add_record(b"foooo", 0).unwrap();
        w.add_record(b"bar", 0).unwrap();
        assert!(t.written_bytes() >= BLOCK_SIZE * 2);
        t.check_record_and_timestamp_size(b"foooo", &map(&ts_sz_two));
        t.check_record_and_timestamp_size(b"bar", &map(&ts_sz_two));
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn log_test_timestamp_size_record_padding() {
    // A timestamp size record that does not fit pads the block's tail and starts a new one.
    LogTest::each(|mut t| {
        let data_len = BLOCK_SIZE - 2 * t.header_size();
        let first_str = big_string("foo", data_len);
        t.write(&first_str);
        let ts_sz = [(2, 8)];
        t.writer
            .maybe_add_user_defined_timestamp_size_record(&ts_sz)
            .unwrap();
        assert!(t.writer.block_offset() < BLOCK_SIZE);
        let second_str = big_string("bar", 1000);
        t.write(&second_str);
        assert_eq!(first_str, t.read());
        t.check_record_and_timestamp_size(&second_str, &map(&ts_sz));
    });
}

/// log_test.cc's `RetriableLogTest`, on a real file (`INSTANTIATE_TEST_CASE_P(bool, ..,
/// Values(0, 2))`: a recycle count of 0 or 2, so legacy or recyclable records).
/// The encoder's in-memory file, owned by its writer.
#[derive(Default)]
struct VecSink(Vec<u8>);

impl WritableFile for VecSink {
    fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

struct RetriableLogTest {
    _dir: tempfile::TempDir,
    log_writer: Writer<VecSink>,
    writer: WritableFileWriter<BlockWritableFile<DeviceFile>>,
    log_reader: FragmentBufferedReader<BlockSequentialFile<DeviceFile>, ReportCollector>,
    header_size: usize,
}

impl RetriableLogTest {
    fn new(recycle: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        let open =
            || DeviceFile::open(&path, true, CachingRequest::Buffered, Alignment::BYTE).unwrap();
        let log_writer = Writer::new(
            WritableFileWriter::new(VecSink::default()),
            WriterOptions::new(123, recycle),
        )
        .unwrap();
        let writer = WritableFileWriter::new(BlockWritableFile::new(open(), 0).unwrap());
        let file = BlockSequentialFile::new(open(), path.display().to_string()).unwrap();
        let log_reader = FragmentBufferedReader::new(file, ReportCollector::default(), true, 123);
        Self {
            _dir: dir,
            log_writer,
            writer,
            log_reader,
            header_size: if recycle {
                RECYCLABLE_HEADER_SIZE
            } else {
                HEADER_SIZE
            },
        }
    }

    fn contents(&self) -> Vec<u8> {
        self.log_writer.file().unwrap().file().0.clone()
    }

    fn encode(&mut self, msg: &[u8]) {
        self.log_writer.add_record(msg, 0).unwrap();
    }

    fn write(&mut self, data: &[u8]) {
        self.writer.append(data).unwrap();
        self.writer.sync().unwrap();
    }

    fn try_read(&mut self, result: &mut Vec<u8>) -> bool {
        self.log_reader.read_record(result, TOLERATE)
    }

    /// The record `msg` cut after `delta` bytes.
    fn parts(&mut self, msg: &[u8], delta: usize) -> (Vec<u8>, Vec<u8>) {
        let old_sz = self.contents().len();
        self.encode(msg);
        let c = self.contents();
        (
            c[old_sz..old_sz + delta].to_vec(),
            c[old_sz + delta..].to_vec(),
        )
    }
}

fn tail_log(delta_from_header: fn(usize) -> usize) {
    for recycle in [false, true] {
        let mut t = RetriableLogTest::new(recycle);
        let delta = delta_from_header(t.header_size);
        let (part1, part2) = t.parts(b"foo", delta);
        t.write(&part1);
        let mut record = Vec::new();
        assert!(!t.try_read(&mut record));
        assert!(t.log_reader.is_eof());
        t.write(&part2);
        assert!(t.try_read(&mut record));
        assert_eq!(b"foo", &record[..]);
    }
}

#[test]
fn retriable_log_test_tail_log_partial_header() {
    tail_log(|h| h - 1);
}

#[test]
fn retriable_log_test_tail_log_full_header() {
    tail_log(|h| h + 1);
}

#[test]
fn retriable_log_test_non_blocking_read_full_record() {
    for recycle in [false, true] {
        let mut t = RetriableLogTest::new(recycle);
        let delta = t.header_size - 1;
        let (part1, part2) = t.parts(b"foo-bar", delta);
        t.write(&part1);
        let mut record = Vec::new();
        assert!(!t.try_read(&mut record));
        assert!(record.is_empty());
        t.write(&part2);
        assert!(t.try_read(&mut record));
        assert_eq!(b"foo-bar", &record[..]);
    }
}

#[test]
fn compression_log_test_empty() {
    LogTest::each_compression(|mut t| {
        // With WAL compression a record names the compression type.
        let expected = if t.compression == CompressionType::NoCompression {
            0
        } else {
            HEADER_SIZE + 4
        };
        assert_eq!(expected, t.written_bytes());
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn compression_log_test_read_write() {
    LogTest::each_compression(|mut t| {
        t.write(b"foo");
        t.write(b"bar");
        t.write(b"");
        t.write(b"xxxx");
        assert_eq!(b"foo", &t.read()[..]);
        assert_eq!(b"bar", &t.read()[..]);
        assert_eq!(b"", &t.read()[..]);
        assert_eq!(b"xxxx", &t.read()[..]);
        assert_eq!(EOF, t.read());
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn compression_log_test_read_write_with_timestamp_size() {
    LogTest::each_compression(read_write_with_timestamp_size);
}

#[test]
fn compression_log_test_many_blocks() {
    LogTest::each_compression(many_blocks);
}

#[test]
fn compression_log_test_fragmentation() {
    LogTest::each_compression(|mut t| {
        let mut rnd = Random::new(301);
        let entries = [
            b"small".to_vec(),
            rnd.random_binary_string(3 * BLOCK_SIZE / 2), // Spans into block 2
            rnd.random_binary_string(3 * BLOCK_SIZE),     // Spans into block 5
        ];
        for e in &entries {
            t.write(e);
        }
        for e in &entries {
            assert_eq!(e, &t.read());
        }
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn compression_log_test_aligned_fragmentation() {
    LogTest::each_compression(|mut t| {
        let mut rnd = Random::new(301);
        let mut num_filler_records = 0;
        // Small records until the next one starts a block.
        while t.written_bytes() & (BLOCK_SIZE - 1) >= HEADER_SIZE {
            t.writer.add_record(b"a", 0).unwrap();
            num_filler_records += 1;
        }
        let entries = [rnd.random_binary_string(3 * BLOCK_SIZE)];
        for e in &entries {
            t.write(e);
        }
        for _ in 0..num_filler_records {
            assert_eq!(b"a", &t.read()[..]);
        }
        for e in &entries {
            assert_eq!(e, &t.read());
        }
        assert_eq!(EOF, t.read());
    });
}

#[test]
fn compression_log_test_checksum_mismatch() {
    LogTest::each_compression(|mut t| {
        t.write(b"foooooo");
        let header_len = t.header_size();
        let compression_record_len = if t.compression == CompressionType::NoCompression {
            0
        } else {
            header_len + 4
        };
        t.increment_byte(compression_record_len + header_len, 14);
        assert_eq!(EOF, t.read());
        if !t.recyclable {
            assert!(t.dropped_bytes() > 0);
            assert_eq!("OK", t.match_error("checksum mismatch"));
        } else {
            assert_eq!(0, t.dropped_bytes());
            assert_eq!("", t.report_message());
        }
    });
}

#[test]
fn streaming_compression_test_basic() {
    for input_size in [10, 100, 1000, BLOCK_SIZE, BLOCK_SIZE * 2] {
        let mut compress = StreamingCompress::zstd(BLOCK_SIZE).unwrap();
        let mut uncompress = StreamingUncompress::zstd(BLOCK_SIZE).unwrap();
        let input = big_string("abc", input_size);
        let mut compressed_buffers = Vec::new();
        // Compress until the entire input is consumed.
        loop {
            let mut output = vec![0u8; BLOCK_SIZE];
            let out = compress.compress(&input, &mut output).unwrap();
            if out.output_len > 0 {
                compressed_buffers.push(output[..out.output_len].to_vec());
            }
            if out.remaining == 0 {
                break;
            }
        }
        let mut uncompressed = Vec::new();
        let mut output = vec![0u8; BLOCK_SIZE];
        let mut remaining = 0;
        // Uncompress the fragments and concatenate them.
        for buffer in &compressed_buffers {
            let mut pos = 0;
            loop {
                let out = uncompress
                    .uncompress(buffer, &mut pos, &mut output)
                    .unwrap();
                remaining = out.remaining;
                uncompressed.extend_from_slice(&output[..out.output_len]);
                if out.remaining == 0 && out.output_len != BLOCK_SIZE {
                    break;
                }
            }
        }
        // The final call leaves no input.
        assert_eq!(0, remaining);
        assert_eq!(input, uncompressed);
    }
}

/// Not in log_test.cc: a column family's timestamp size does not change while the DB runs,
/// which RocksDB's writer only asserts (db/log_writer.cc:279-282, compiled out of release
/// builds, where the new size is silently not recorded). The port refuses the change, writes
/// nothing for it, and goes on taking records.
#[test]
fn log_test_a_changed_timestamp_size_is_refused() {
    LogTest::each(|mut t| {
        t.write_ts(b"foo", &[(1, 8)]);
        let before = t.written_bytes();
        let err = t
            .writer
            .maybe_add_user_defined_timestamp_size_record(&[(1, 16)])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }), "{err}");
        assert_eq!(before, t.written_bytes());
        t.write(b"bar");
        t.check_record_and_timestamp_size(b"foo", &map(&[(1, 8)]));
        t.check_record_and_timestamp_size(b"bar", &map(&[(1, 8)]));
        assert_eq!(EOF, t.read());
    });
}
