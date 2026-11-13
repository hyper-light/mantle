//! What a peer's compressed certificate can make the decoder hold before the peer has
//! authenticated, counted by a counting allocator: a server reads a client's certificate there, and
//! RFC 8879 lets a stream name its own window (RFC 7932 §9.1) and each metablock its own length
//! (§9.2). Each stream here is a header only, refused for a certificate of 1 KiB; the test prints
//! the peak each takes and holds it to what the decoder takes for a whole certificate.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    missing_docs
)]

use hyper_measure::alloc::{self, Counting};
use hyper_tls::compress::{CompressionLevel, BROTLI_COMPRESSOR, BROTLI_DECOMPRESSOR};

#[global_allocator]
static GLOBAL: Counting = Counting;

/// A certificate's length: the 1 KiB a node's chain of one takes, within a few hundred bytes.
const CERTIFICATE: usize = 1024;

/// Bits written low bit first, as Brotli reads them (RFC 7932 §2).
#[derive(Default)]
struct Bits {
    bytes: Vec<u8>,
    at: usize,
}

impl Bits {
    fn put(&mut self, value: u64, count: usize) -> &mut Self {
        for bit in 0..count {
            if self.at.is_multiple_of(8) {
                self.bytes.push(0);
            }
            if value >> bit & 1 == 1 {
                *self.bytes.last_mut().unwrap() |= 1 << (self.at % 8);
            }
            self.at += 1;
        }
        self
    }

    /// The largest window RFC 7932 takes, 2^24 − 16 bytes: a 1, then 7 in three bits (§9.1).
    fn window_24(&mut self) -> &mut Self {
        self.put(1, 1).put(7, 3)
    }

    /// The large-window extension's header (not RFC 7932): 0b0010001, a zero bit, then WBITS.
    fn large_window(&mut self, bits: u64) -> &mut Self {
        self.put(0x11, 7).put(0, 1).put(bits, 6)
    }

    /// A metablock that is not the last, of 2^24 bytes, the most its header says (§9.2: MNIBBLES
    /// 6, MLEN − 1 all ones), stored uncompressed or not.
    fn metablock_of_16_mib(&mut self, uncompressed: bool) -> &mut Self {
        self.put(0, 1)
            .put(2, 2)
            .put((1 << 24) - 1, 24)
            .put(u64::from(uncompressed), 1)
    }
}

fn peak(stream: &[u8]) -> i64 {
    let mut output = vec![0u8; CERTIFICATE];
    alloc::begin();
    let decoded = BROTLI_DECOMPRESSOR.decompress(stream, &mut output);
    let counted = alloc::end();
    assert!(decoded.is_err(), "{stream:x?} was taken");
    counted.peak
}

#[test]
fn a_peers_brotli_header_holds_the_decoder_to_a_certificates_cost() {
    let mut input = vec![0u8; CERTIFICATE];
    for (index, byte) in input.iter_mut().enumerate() {
        *byte = (index * 7 % 251) as u8;
    }
    let compressed = BROTLI_COMPRESSOR
        .compress(input.clone(), CompressionLevel::Amortized)
        .unwrap();
    let mut output = vec![0u8; CERTIFICATE];
    alloc::begin();
    BROTLI_DECOMPRESSOR
        .decompress(&compressed, &mut output)
        .unwrap();
    let whole = alloc::end().peak;
    assert_eq!(output, input);
    println!("a whole certificate of {CERTIFICATE} bytes: peak {whole}");

    let streams = [
        (
            "RFC 7932's largest window, a 16 MiB compressed metablock",
            {
                let mut bits = Bits::default();
                bits.window_24().metablock_of_16_mib(false);
                bits.bytes
            },
        ),
        ("RFC 7932's largest window, a 16 MiB stored metablock", {
            let mut bits = Bits::default();
            bits.window_24().metablock_of_16_mib(true);
            bits.bytes
        }),
        ("the large window, 2^30, a 16 MiB stored metablock", {
            let mut bits = Bits::default();
            bits.large_window(30).metablock_of_16_mib(true);
            bits.bytes
        }),
    ];
    for (what, stream) in &streams {
        let peak = peak(stream);
        println!("{what} ({stream:x?}): peak {peak}");
        assert!(
            peak <= whole,
            "{what}: {peak} past a whole certificate's {whole}"
        );
    }
}

/// Every certificate a stock encoder sends still decodes under the bound: Brotli's default window,
/// 2^22 (upstream rustls's), and the window hyper-tls sizes to the input, at the qualities a
/// handshake compresses with (4 and 11), for lengths about each ring buffer size the decoder takes
/// up to RFC 8879's 2^16 bound, compressible and not (a stored metablock).
#[test]
fn every_certificate_a_stock_encoder_sends_decodes() {
    use std::io::Write as _;
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut noise = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state as u8
    };
    let lengths = [
        1usize, 31, 32, 33, 1023, 1024, 1025, 2047, 2048, 2049, 4096, 16_384, 32_767, 32_768,
        32_769, 65_535,
    ];
    for len in lengths {
        let shaped: Vec<u8> = (0..len).map(|index| (index * 7 % 251) as u8).collect();
        let random: Vec<u8> = (0..len).map(|_| noise()).collect();
        for input in [shaped, random] {
            let mut streams = Vec::new();
            for quality in [4, 11] {
                let mut stock = brotli::CompressorWriter::new(Vec::new(), 4096, quality, 22);
                stock.write_all(&input).unwrap();
                streams.push(stock.into_inner());
            }
            for level in [CompressionLevel::Interactive, CompressionLevel::Amortized] {
                streams.push(BROTLI_COMPRESSOR.compress(input.clone(), level).unwrap());
            }
            for stream in streams {
                let mut output = vec![0u8; len];
                BROTLI_DECOMPRESSOR
                    .decompress(&stream, &mut output)
                    .unwrap_or_else(|_| panic!("{len} bytes, a stream of {}", stream.len()));
                assert_eq!(output, input);
            }
        }
    }
}
