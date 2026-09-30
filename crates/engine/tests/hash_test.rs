//! RocksDB's util/hash_test.cc, test for test: its 16 cases with the same literals — the
//! `HashTest` suite (the 32-bit `Hash`, `Hash64` = XXPH3 and `Hash128`/`Hash2x64`, whose
//! values are part of the file format), `FastRange32Test`, `FastRange64Test`,
//! `FastRangeGenericTest` and the `MathTest` suite of util/math.h and util/math128.h.
//!
//! Two pieces of `MathTest.BitOps` have nothing to convert: `BitwiseAnd` picks the narrower C++
//! operand type (Rust never widens implicitly), and `ConstexprFloorLog2` is `FloorLog2` in a
//! `constexpr` context, checked here through `floor_log2`. The C++ runs `BitOps` over 21 integer
//! types that are aliases of the ten Rust has; it runs over those ten.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_lossless
)]

use mantle_engine::util::coding::encode_fixed32;
use mantle_engine::util::fastrange::{FastRangeHash, fast_range32, fast_range64};
use mantle_engine::util::hash::{
    bijective_hash2x64, bijective_hash2x64_with_seed, bijective_unhash2x64,
    bijective_unhash2x64_with_seed, get_slice_hash64, get_slice_hash128, hash, hash2x64,
    hash2x64_with_seed, hash64, hash64_with_seed, hash128, hash128_with_seed, lower32of64,
    upper32of64,
};
use mantle_engine::util::math::BitMath;
use mantle_engine::util::math128::{
    FixedGeneric, decode_fixed128, encode_fixed128, lower64of128, multiply64to128, upper64of128,
};

/// The hash is part of the file format (the Bloom filters): its values are stable for these
/// strings of varying lengths.
#[test]
fn values() {
    const SEED: u32 = 0xbc9f_1d34; // Same as BloomHash.

    assert_eq!(hash(b"", SEED), 3_164_544_308);
    assert_eq!(hash(b"\x08", SEED), 422_599_524);
    assert_eq!(hash(b"\x17", SEED), 3_168_152_998);
    assert_eq!(hash(b"\x9a", SEED), 3_195_034_349);
    assert_eq!(hash(b"\x1c", SEED), 2_651_681_383);
    assert_eq!(hash(b"\x4d\x76", SEED), 2_447_836_956);
    assert_eq!(hash(b"\x52\xd5", SEED), 3_854_228_105);
    assert_eq!(hash(b"\x91\xf7", SEED), 31_066_776);
    assert_eq!(hash(b"\xd6\x27", SEED), 1_806_091_603);
    assert_eq!(hash(b"\x30\x46\x0b", SEED), 3_808_221_797);
    assert_eq!(hash(b"\x56\xdc\xd6", SEED), 2_157_698_265);
    assert_eq!(hash(b"\xd4\x52\x33", SEED), 1_721_992_661);
    assert_eq!(hash(b"\x6a\xb5\xf4", SEED), 2_469_105_222);
    assert_eq!(hash(b"\x67\x53\x81\x1c", SEED), 118_283_265);
    assert_eq!(hash(b"\x69\xb8\xc0\x88", SEED), 3_416_318_611);
    assert_eq!(hash(b"\x1e\x84\xaf\x2d", SEED), 3_315_003_572);
    assert_eq!(hash(b"\x46\xdc\x54\xbe", SEED), 447_346_355);
    assert_eq!(hash(b"\xd0\x7a\x6e\xea\x56", SEED), 4_255_445_370);
    assert_eq!(hash(b"\x86\x83\xd5\xa4\xd8", SEED), 2_390_603_402);
    assert_eq!(hash(b"\xb7\x46\xbb\x77\xce", SEED), 2_048_907_743);
    assert_eq!(hash(b"\x6c\xa8\xbc\xe5\x99", SEED), 2_177_978_500);
    assert_eq!(hash(b"\x5c\x5e\xe1\xa0\x73\x81", SEED), 1_036_846_008);
    assert_eq!(hash(b"\x08\x5d\x73\x1c\xe5\x2e", SEED), 229_980_482);
    assert_eq!(hash(b"\x42\xfb\xf2\x52\xb4\x10", SEED), 3_655_585_422);
    assert_eq!(hash(b"\x73\xe1\xff\x56\x9c\xce", SEED), 3_502_708_029);
    assert_eq!(hash(b"\x5c\xbe\x97\x75\x54\x9a\x52", SEED), 815_120_748);
    assert_eq!(hash(b"\x16\x82\x39\x49\x88\x2b\x36", SEED), 3_056_033_698);
    assert_eq!(hash(b"\x59\x77\xf0\xa7\x24\xf4\x78", SEED), 587_205_227);
    assert_eq!(hash(b"\xd3\xa5\x7c\x0e\xc0\x02\x07", SEED), 2_030_937_252);
    assert_eq!(hash(b"\x31\x1b\x98\x75\x96\x22\xd3\x9a", SEED), 469_635_402);
    assert_eq!(
        hash(b"\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", SEED),
        3_530_274_698
    );
    assert_eq!(
        hash(b"\xbb\x18\x5d\xf4\x12\x03\xf7\x99", SEED),
        1_974_545_809
    );
    assert_eq!(
        hash(b"\x80\xd4\x3b\x3b\xae\x22\xa2\x78", SEED),
        3_563_570_120
    );
    assert_eq!(
        hash(b"\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", SEED),
        2_706_087_434
    );
    assert_eq!(
        hash(b"\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", SEED),
        1_534_654_151
    );
    assert_eq!(
        hash(b"\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", SEED),
        2_355_554_696
    );
    assert_eq!(
        hash(b"\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", SEED),
        1_400_800_912
    );
    assert_eq!(
        hash(b"\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", SEED),
        3_420_325_137
    );
    assert_eq!(
        hash(b"\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", SEED),
        3_427_803_584
    );
    assert_eq!(
        hash(b"\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", SEED),
        1_152_407_945
    );
    assert_eq!(
        hash(b"\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", SEED),
        3_382_479_516
    );
}

/// The hash is part of the file format (the Bloom filters).
#[test]
fn hash64_misc() {
    const SEED: u64 = 0; // Same as GetSliceHash64.

    for fill in [0u8, b'a', b'1', 0xff] {
        const MAX_SIZE: usize = 1000;
        let s = vec![fill; MAX_SIZE];

        for size in 0..=MAX_SIZE {
            let here = hash64_with_seed(&s[..size], SEED);

            // Same as unseeded Hash64 and GetSliceHash64.
            assert_eq!(here, hash64(&s[..size]));
            assert_eq!(here, get_slice_hash64(&s[..size]));

            // Upper and Lower reconstruct the hash.
            let upper = u64::from(upper32of64(here)) << 32;
            let lower = u64::from(lower32of64(here));
            assert_eq!(here, upper | lower);
            assert_eq!(here, upper + lower);
            assert_eq!(here, upper ^ lower);

            // The seed changes the value (with high probability).
            let mut var_seed = 1u64;
            while var_seed != 0 {
                assert_ne!(here, hash64_with_seed(&s[..size], var_seed));
                var_seed <<= 1;
            }

            // The size changes the value (with high probability).
            let max_smaller_by = size.min(30);
            for smaller_by in 1..=max_smaller_by {
                assert_ne!(here, hash64_with_seed(&s[..size - smaller_by], SEED));
            }
        }
    }
}

/// Hash values are "non-trivial" for "trivial" inputs.
#[test]
fn hash64_trivial() {
    // The thorough form is too slow for regression testing.
    const THOROUGH: bool = false;

    // For various seeds, the hash of the empty string is not zero.
    let max_seed: u64 = if THOROUGH { 0x100_0000 } else { 0x1_0000 };
    for seed in 0..max_seed {
        let here = hash64_with_seed(b"", seed);
        assert_ne!(lower32of64(here), 0);
        assert_ne!(upper32of64(here), 0);
    }

    // For the standard seed, the hashes of small strings are not zero.
    const SEED: u64 = 0; // Same as GetSliceHash64.
    let max_len = if THOROUGH { 3 } else { 2 };
    for len in 1..=max_len {
        let mut i = 0u32;
        while i >> (len * 8) == 0 {
            let input = encode_fixed32(i);
            let here = hash64_with_seed(&input[..len], SEED);
            assert_ne!(lower32of64(here), 0);
            assert_ne!(upper32of64(here), 0);
            i += 1;
        }
    }
}

/// Hash values are stable for these strings of varying small lengths.
#[test]
fn hash64_small_value_schema() {
    const SEED: u64 = 0; // Same as GetSliceHash64.

    assert_eq!(hash64_with_seed(b"", SEED), 5_999_572_062_939_766_020);
    assert_eq!(hash64_with_seed(b"\x08", SEED), 583_283_813_901_344_696);
    assert_eq!(hash64_with_seed(b"\x17", SEED), 16_175_549_975_585_474_943);
    assert_eq!(hash64_with_seed(b"\x9a", SEED), 16_322_991_629_225_003_903);
    assert_eq!(hash64_with_seed(b"\x1c", SEED), 13_269_285_487_706_833_447);
    assert_eq!(
        hash64_with_seed(b"\x4d\x76", SEED),
        6_859_542_833_406_258_115
    );
    assert_eq!(
        hash64_with_seed(b"\x52\xd5", SEED),
        4_919_611_532_550_636_959
    );
    assert_eq!(
        hash64_with_seed(b"\x91\xf7", SEED),
        14_199_427_467_559_720_719
    );
    assert_eq!(
        hash64_with_seed(b"\xd6\x27", SEED),
        12_292_689_282_614_532_691
    );
    assert_eq!(
        hash64_with_seed(b"\x30\x46\x0b", SEED),
        11_404_699_285_340_020_889
    );
    assert_eq!(
        hash64_with_seed(b"\x56\xdc\xd6", SEED),
        12_404_347_133_785_524_237
    );
    assert_eq!(
        hash64_with_seed(b"\xd4\x52\x33", SEED),
        15_853_805_298_481_534_034
    );
    assert_eq!(
        hash64_with_seed(b"\x6a\xb5\xf4", SEED),
        16_863_488_758_399_383_382
    );
    assert_eq!(
        hash64_with_seed(b"\x67\x53\x81\x1c", SEED),
        9_010_661_983_527_562_386
    );
    assert_eq!(
        hash64_with_seed(b"\x69\xb8\xc0\x88", SEED),
        6_611_781_377_647_041_447
    );
    assert_eq!(
        hash64_with_seed(b"\x1e\x84\xaf\x2d", SEED),
        15_290_969_111_616_346_501
    );
    assert_eq!(
        hash64_with_seed(b"\x46\xdc\x54\xbe", SEED),
        7_063_754_590_279_313_623
    );
    assert_eq!(
        hash64_with_seed(b"\xd0\x7a\x6e\xea\x56", SEED),
        6_384_167_718_754_869_899
    );
    assert_eq!(
        hash64_with_seed(b"\x86\x83\xd5\xa4\xd8", SEED),
        16_874_407_254_108_011_067
    );
    assert_eq!(
        hash64_with_seed(b"\xb7\x46\xbb\x77\xce", SEED),
        16_809_880_630_149_135_206
    );
    assert_eq!(
        hash64_with_seed(b"\x6c\xa8\xbc\xe5\x99", SEED),
        1_249_038_833_153_141_148
    );
    assert_eq!(
        hash64_with_seed(b"\x5c\x5e\xe1\xa0\x73\x81", SEED),
        17_358_142_495_308_219_330
    );
    assert_eq!(
        hash64_with_seed(b"\x08\x5d\x73\x1c\xe5\x2e", SEED),
        4_237_646_583_134_806_322
    );
    assert_eq!(
        hash64_with_seed(b"\x42\xfb\xf2\x52\xb4\x10", SEED),
        4_373_664_924_115_234_051
    );
    assert_eq!(
        hash64_with_seed(b"\x73\xe1\xff\x56\x9c\xce", SEED),
        12_012_981_210_634_596_029
    );
    assert_eq!(
        hash64_with_seed(b"\x5c\xbe\x97\x75\x54\x9a\x52", SEED),
        5_716_522_398_211_028_826
    );
    assert_eq!(
        hash64_with_seed(b"\x16\x82\x39\x49\x88\x2b\x36", SEED),
        15_604_531_309_862_565_013
    );
    assert_eq!(
        hash64_with_seed(b"\x59\x77\xf0\xa7\x24\xf4\x78", SEED),
        8_601_330_687_345_614_172
    );
    assert_eq!(
        hash64_with_seed(b"\xd3\xa5\x7c\x0e\xc0\x02\x07", SEED),
        8_088_079_329_364_056_942
    );
    assert_eq!(
        hash64_with_seed(b"\x31\x1b\x98\x75\x96\x22\xd3\x9a", SEED),
        9_844_314_944_338_447_628
    );
    assert_eq!(
        hash64_with_seed(b"\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", SEED),
        10_973_293_517_982_163_143
    );
    assert_eq!(
        hash64_with_seed(b"\xbb\x18\x5d\xf4\x12\x03\xf7\x99", SEED),
        9_986_007_080_564_743_219
    );
    assert_eq!(
        hash64_with_seed(b"\x80\xd4\x3b\x3b\xae\x22\xa2\x78", SEED),
        1_729_303_145_008_254_458
    );
    assert_eq!(
        hash64_with_seed(b"\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", SEED),
        13_253_403_748_084_181_481
    );
    assert_eq!(
        hash64_with_seed(b"\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", SEED),
        7_768_754_303_876_232_188
    );
    assert_eq!(
        hash64_with_seed(b"\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", SEED),
        12_439_346_786_701_492
    );
    assert_eq!(
        hash64_with_seed(b"\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", SEED),
        10_841_838_338_450_144_690
    );
    assert_eq!(
        hash64_with_seed(b"\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", SEED),
        12_883_919_702_069_153_152
    );
    assert_eq!(
        hash64_with_seed(b"\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", SEED),
        12_692_903_507_676_842_188
    );
    assert_eq!(
        hash64_with_seed(b"\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", SEED),
        6_540_985_900_674_032_620
    );
    assert_eq!(
        hash64_with_seed(b"\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", SEED),
        10_551_812_464_348_219_044
    );
}

const MOD61_ENCODE: &[u8; 61] = b"abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// `repeat` repeated to at least `limit` bytes.
fn repeated(repeat: &str, limit: usize) -> Vec<u8> {
    let mut input = Vec::new();
    while input.len() < limit {
        input.extend_from_slice(repeat.as_bytes());
    }
    input
}

/// RocksDB's `Hash64TestDescriptor`: one character per length, from its hash mod 61.
fn hash64_test_descriptor(repeat: &str, limit: usize) -> String {
    let input = repeated(repeat, limit);
    (0..limit)
        .map(|i| char::from(MOD61_ENCODE[(get_slice_hash64(&input[..i]) % 61) as usize]))
        .collect()
}

/// XXPH3 changes its algorithm for sizes up through 250 bytes, so larger sizes are checked
/// too.
#[test]
fn hash64_large_value_schema() {
    // Each derives a "descriptor" from the hashes of all lengths up to 430. "c" is common for
    // the zero-length string.
    assert_eq!(
        hash64_test_descriptor("foo", 430),
        concat!(
            "cRhyWsY67B6klRA1udmOuiYuX7IthyGBKqbeosz2hzVglWCmQx8nEdnpkvPfYX56Up2OWOTV",
            "lTzfAoYwvtqKzjD8E9xttR2unelbXbIV67NUe6bOO23BxaSFRcA3njGu5cUWfgwOqNoTsszp",
            "uPvKRP6qaUR5VdoBkJUCFIefd7edlNK5mv6JYWaGdwxehg65hTkTmjZoPKxTZo4PLyzbL9U4",
            "xt12ITSfeP2MfBHuLI2z2pDlBb44UQKVMx27LEoAHsdLp3WfWfgH3sdRBRCHm33UxCM4QmE2",
            "xJ7gqSvNwTeH7v9GlC8zWbGroyD3UVNeShMLx29O7tH1biemLULwAHyIw8zdtLMDpEJ8m2ic",
            "l6Lb4fDuuFNAs1GCVUthjK8CV8SWI8Rsz5THSwn5CGhpqUwSZcFknjwWIl5rNCvDxXJqYr"
        )
    );
    // "1EeRk" is common for "Rocks".
    assert_eq!(
        hash64_test_descriptor("Rocks", 430),
        concat!(
            "c1EeRkrzgOYWLA8PuhJrwTePJewoB44WdXYDfhbk3ZxTqqg25WlPExDl7IKIQLJvnA6gJxxn",
            "9TCSLkFGfJeXehaSS1GBqWSzfhEH4VXiXIUCuxJXxtKXcSC6FrNIQGTZbYDiUOLD6Y5inzrF",
            "9etwQhXUBanw55xAUdNMFQAm2GjJ6UDWp2mISLiMMkLjANWMKLaZMqaFLX37qB4MRO1ooVRv",
            "zSvaNRSCLxlggQCasQq8icWjzf3HjBlZtU6pd4rkaUxSzHqmo9oM5MghbU5Rtxg8wEfO7lVN",
            "5wdMONYecslQTwjZUpO1K3LDf3K3XK6sUXM6ShQQ3RHmMn2acB4YtTZ3QQcHYJSOHn2DuWpa",
            "Q8RqzX5lab92YmOLaCdOHq1BPsM7SIBzMdLgePNsJ1vvMALxAaoDUHPxoFLO2wx18IXnyX"
        )
    );
    assert_eq!(
        hash64_test_descriptor("RocksDB", 430),
        concat!(
            "c1EeRkukbkb28wLTahwD2sfUhZzaBEnF8SVrxnPVB6A7b8CaAl3UKsDZISF92GSq2wDCukOq",
            "Jgrsp7A3KZhDiLW8dFXp8UPqPxMCRlMdZeVeJ2dJxrmA6cyt99zkQFj7ELbut6jAeVqARFnw",
            "fnWVXOsaLrq7bDCbMcns2DKvTaaqTCLMYxI7nhtLpFN1jR755FRQFcOzrrDbh7QhypjdvlYw",
            "cdAMSZgp9JMHxbM23wPSuH6BOFgxejz35PScZfhDPvTOxIy1jc3MZsWrMC3P324zNolO7JdW",
            "CX2I5UDKjjaEJfxbgVgJIXxtQGlmj2xkO5sPpjULQV4X2HlY7FQleJ4QRaJIB4buhCA4vUTF",
            "eMFlxCIYUpTCsal2qsmnGOWa8WCcefrohMjDj1fjzSvSaQwlpyR1GZHF2uPOoQagiCpHpm"
        )
    );
}

#[test]
fn hash128_misc() {
    const SEED: u64 = 0; // Same as GetSliceHash128.

    for fill in [0u8, b'a', b'1', 0xff, b'e'] {
        const MAX_SIZE: usize = 1000;
        let mut s = vec![fill; MAX_SIZE];
        if fill == b'e' {
            // Different characters check endianness handling.
            for (i, c) in s.iter_mut().enumerate() {
                *c = c.wrapping_add(i as u8);
            }
        }

        for size in 0..=MAX_SIZE {
            let here = hash128_with_seed(&s[..size], SEED);

            // Same as unseeded Hash128 and GetSliceHash128.
            assert_eq!(here, hash128(&s[..size]));
            assert_eq!(here, get_slice_hash128(&s[..size]));
            {
                let (hi, lo) = hash2x64(&s[..size]);
                assert_eq!(lower64of128(here), lo);
                assert_eq!(upper64of128(here), hi);
            }
            if size == 16 {
                let in_hi = u64::from_le_bytes(s[8..16].try_into().unwrap());
                let in_lo = u64::from_le_bytes(s[..8].try_into().unwrap());
                let (hi, lo) = bijective_hash2x64(in_hi, in_lo);
                assert_eq!(lower64of128(here), lo);
                assert_eq!(upper64of128(here), hi);
                let (un_hi, un_lo) = bijective_unhash2x64(hi, lo);
                assert_eq!(in_lo, un_lo);
                assert_eq!(in_hi, un_hi);
            }

            // Upper and Lower reconstruct the hash.
            let upper = u128::from(upper64of128(here)) << 64;
            assert_eq!(here, upper | u128::from(lower64of128(here)));
            assert_eq!(here, upper ^ u128::from(lower64of128(here)));

            // The seed changes the value (with high probability).
            let mut var_seed = 1u64;
            while var_seed != 0 {
                let seeded = hash128_with_seed(&s[..size], var_seed);
                assert_ne!(here, seeded);
                // Matches the seeded Hash2x64.
                {
                    let (hi, lo) = hash2x64_with_seed(&s[..size], var_seed);
                    assert_eq!(lower64of128(seeded), lo);
                    assert_eq!(upper64of128(seeded), hi);
                }
                if size == 16 {
                    let in_hi = u64::from_le_bytes(s[8..16].try_into().unwrap());
                    let in_lo = u64::from_le_bytes(s[..8].try_into().unwrap());
                    let (hi, lo) = bijective_hash2x64_with_seed(in_hi, in_lo, var_seed);
                    assert_eq!(lower64of128(seeded), lo);
                    assert_eq!(upper64of128(seeded), hi);
                    let (un_hi, un_lo) = bijective_unhash2x64_with_seed(hi, lo, var_seed);
                    assert_eq!(in_lo, un_lo);
                    assert_eq!(in_hi, un_hi);
                }
                var_seed <<= 1;
            }

            // The size changes the value (with high probability).
            let max_smaller_by = size.min(30);
            for smaller_by in 1..=max_smaller_by {
                assert_ne!(here, hash128_with_seed(&s[..size - smaller_by], SEED));
            }
        }
    }
}

/// Hash values are "non-trivial" for "trivial" inputs.
#[test]
fn hash128_trivial() {
    // The thorough form is too slow for regression testing.
    const THOROUGH: bool = false;

    // For various seeds, the hash of the empty string is not zero.
    let max_seed: u64 = if THOROUGH { 0x100_0000 } else { 0x1_0000 };
    for seed in 0..max_seed {
        let here = hash128_with_seed(b"", seed);
        assert_ne!(lower64of128(here), 0);
        assert_ne!(upper64of128(here), 0);
    }

    // For the standard seed, the hashes of small strings are not zero.
    const SEED: u64 = 0; // Same as GetSliceHash128.
    let max_len = if THOROUGH { 3 } else { 2 };
    for len in 1..=max_len {
        let mut i = 0u32;
        while i >> (len * 8) == 0 {
            let input = encode_fixed32(i);
            let here = hash128_with_seed(&input[..len], SEED);
            assert_ne!(lower64of128(here), 0);
            assert_ne!(upper64of128(here), 0);
            i += 1;
        }
    }
}

/// RocksDB's `Hash128TestDescriptor`: one character per length, from the sum of the hash's
/// halves mod 61.
fn hash128_test_descriptor(repeat: &str, limit: usize) -> String {
    let input = repeated(repeat, limit);
    (0..limit)
        .map(|i| {
            let h = get_slice_hash128(&input[..i]);
            let h2 = upper64of128(h).wrapping_add(lower64of128(h));
            char::from(MOD61_ENCODE[(h2 % 61) as usize])
        })
        .collect()
}

/// XXH3 changes its algorithm for sizes up through 250 bytes, so larger sizes are checked too.
#[test]
fn hash128_value_schema() {
    // Each derives a "descriptor" from the hashes of all lengths up to 430. "b" is common for
    // the zero-length string.
    assert_eq!(
        hash128_test_descriptor("foo", 430),
        concat!(
            "bUMA3As8n9I4vNGhThXlEevxZlyMcbb6TYAlIKJ2f5ponsv99q962rYclQ7u3gfnRdCDQ5JI",
            "2LrGUaCycbXrvLFe4SjgRb9RQwCfrnmNQ7VSEwSKMnkGCK3bDbXSrnIh5qLXdtvIZklbJpGH",
            "Dqr93BlqF9ubTnOSYkSdx89XvQqflMIW8bjfQp9BPjQejWOeEQspnN1D3sfgVdFhpaQdHYA5",
            "pI2XcPlCMFPxvrFuRr7joaDvjNe9IUZaunLPMewuXmC3EL95h52Ju3D7y9RNKhgYxMTrA84B",
            "yJrMvyjdm3vlBxet4EN7v2GEyjbGuaZW9UL6lrX6PghJDg7ACfLGdxNbH3qXM4zaiG2RKnL5",
            "S3WXKR78RBB5fRFQ8KDIEQjHFvSNsc3GrAEi6W8P2lv8JMTzjBODO2uN4wadVQFT9wpGfV"
        )
    );
    // "35D2v" is common for "Rocks".
    assert_eq!(
        hash128_test_descriptor("Rocks", 430),
        concat!(
            "b35D2vzvklFVDqJmyLRXyApwGGO3EAT3swhe8XJAN3mY2UVPglzdmydxcba6JI2tSvwO6zSu",
            "ANpjSM7tc9G5iMhsa7R8GfyCXRO1TnLg7HvdWNdgGGBirxZR68BgT7TQsYJt6zyEyISeXI1n",
            "MXA48Xo7dWfJeYN6Z4KWlqZY7TgFXGbks9AX4ehZNSGtIhdO5i58qlgVX1bEejeOVaCcjC79",
            "67DrMfOKds7rUQzjBa77sMPcoPW1vu6ljGJPZH3XkRyDMZ1twxXKkNxN3tE8nR7JHwyqBAxE",
            "fTcjbOWrLZ1irWxRSombD8sGDEmclgF11IxqEhe3Rt7gyofO3nExGckKkS9KfRqsCHbiUyva",
            "JGkJwUHRXaZnh58b4i1Ei9aQKZjXlvIVDixoZrjcNaH5XJIJlRZce9Z9t82wYapTpckYSg"
        )
    );
    assert_eq!(
        hash128_test_descriptor("RocksDB", 430),
        concat!(
            "b35D2vFUst3XDZCRlSrhmYYakmqImV97LbBsV6EZlOEQpUPH1d1sD3xMKAPlA5UErHehg5O7",
            "n966fZqhAf3hRc24kGCLfNAWjyUa7vSNOx3IcPoTyVRFZeFlcCtfl7t1QJumHOCpS33EBmBF",
            "hvK13QjBbDWYWeHQhJhgV9Mqbx17TIcvUkEnYZxb8IzWNmjVsJG44Z7v52DjGj1ZzS62S2Vv",
            "qWcDO7apvH5VHg68E9Wl6nXP21vlmUqEH9GeWRehfWVvY7mUpsAg5drHHQyDSdiMceiUuUxJ",
            "XJqHFcDdzbbPk7xDvbLgWCKvH8k3MpQNWOmbSSRDdAP6nGlDjoTToYkcqVREHJzztSWAAq5h",
            "GHSUNJ6OxsMHhf8EhXfHtKyUzRmPtjYyeckQcGmrQfFFLidc6cjMDKCdBG6c6HVBrS7H2R"
        )
    );
}

#[test]
fn fast_range32_values() {
    // Zero range
    assert_eq!(fast_range32(0, 0), 0);
    assert_eq!(fast_range32(123, 0), 0);
    assert_eq!(fast_range32(0xffff_ffff, 0), 0);

    // One range
    assert_eq!(fast_range32(0, 1), 0);
    assert_eq!(fast_range32(123, 1), 0);
    assert_eq!(fast_range32(0xffff_ffff, 1), 0);

    // Two range
    assert_eq!(fast_range32(0, 2), 0);
    assert_eq!(fast_range32(123, 2), 0);
    assert_eq!(fast_range32(0x7fff_ffff, 2), 0);
    assert_eq!(fast_range32(0x8000_0000, 2), 1);
    assert_eq!(fast_range32(0xffff_ffff, 2), 1);

    // Seven range
    assert_eq!(fast_range32(0, 7), 0);
    assert_eq!(fast_range32(123, 7), 0);
    assert_eq!(fast_range32(613_566_756, 7), 0);
    assert_eq!(fast_range32(613_566_757, 7), 1);
    assert_eq!(fast_range32(1_227_133_513, 7), 1);
    assert_eq!(fast_range32(1_227_133_514, 7), 2);
    // etc.
    assert_eq!(fast_range32(0xffff_ffff, 7), 6);

    // Big
    assert_eq!(fast_range32(1, 0x8000_0000), 0);
    assert_eq!(fast_range32(2, 0x8000_0000), 1);
    assert_eq!(fast_range32(4, 0x7fff_ffff), 1);
    assert_eq!(fast_range32(4, 0x8000_0000), 2);
    assert_eq!(fast_range32(0xffff_ffff, 0x7fff_ffff), 0x7fff_fffe);
    assert_eq!(fast_range32(0xffff_ffff, 0x8000_0000), 0x7fff_ffff);
}

#[test]
fn fast_range64_values() {
    // Zero range
    assert_eq!(fast_range64(0, 0), 0);
    assert_eq!(fast_range64(123, 0), 0);
    assert_eq!(fast_range64(0xffff_ffff, 0), 0);
    assert_eq!(fast_range64(0xffff_ffff_ffff_ffff, 0), 0);

    // One range
    assert_eq!(fast_range64(0, 1), 0);
    assert_eq!(fast_range64(123, 1), 0);
    assert_eq!(fast_range64(0xffff_ffff, 1), 0);
    assert_eq!(fast_range64(0xffff_ffff_ffff_ffff, 1), 0);

    // Two range
    assert_eq!(fast_range64(0, 2), 0);
    assert_eq!(fast_range64(123, 2), 0);
    assert_eq!(fast_range64(0xffff_ffff, 2), 0);
    assert_eq!(fast_range64(0x7fff_ffff_ffff_ffff, 2), 0);
    assert_eq!(fast_range64(0x8000_0000_0000_0000, 2), 1);
    assert_eq!(fast_range64(0xffff_ffff_ffff_ffff, 2), 1);

    // Seven range
    assert_eq!(fast_range64(0, 7), 0);
    assert_eq!(fast_range64(123, 7), 0);
    assert_eq!(fast_range64(0xffff_ffff, 7), 0);
    assert_eq!(fast_range64(2_635_249_153_387_078_802, 7), 0);
    assert_eq!(fast_range64(2_635_249_153_387_078_803, 7), 1);
    assert_eq!(fast_range64(5_270_498_306_774_157_604, 7), 1);
    assert_eq!(fast_range64(5_270_498_306_774_157_605, 7), 2);
    assert_eq!(fast_range64(0x7fff_ffff_ffff_ffff, 7), 3);
    assert_eq!(fast_range64(0x8000_0000_0000_0000, 7), 3);
    assert_eq!(fast_range64(0xffff_ffff_ffff_ffff, 7), 6);

    // Big but 32-bit range
    assert_eq!(fast_range64(0x1_0000_0000, 0x8000_0000), 0);
    assert_eq!(fast_range64(0x2_0000_0000, 0x8000_0000), 1);
    assert_eq!(fast_range64(0x4_0000_0000, 0x7fff_ffff), 1);
    assert_eq!(fast_range64(0x4_0000_0000, 0x8000_0000), 2);
    assert_eq!(
        fast_range64(0xffff_ffff_ffff_ffff, 0x7fff_ffff),
        0x7fff_fffe
    );
    assert_eq!(
        fast_range64(0xffff_ffff_ffff_ffff, 0x8000_0000),
        0x7fff_ffff
    );

    // Big, > 32-bit range (size_t is 64 bits on every target mantle builds for).
    assert_eq!(
        fast_range64(0x7fff_ffff_ffff_ffff, 0x42_0000_0002),
        0x21_0000_0000
    );
    assert_eq!(
        fast_range64(0x8000_0000_0000_0000, 0x42_0000_0002),
        0x21_0000_0001
    );

    assert_eq!(fast_range64(0x0000_0000_0000_0000, 420_000_000_002), 0);
    assert_eq!(
        fast_range64(0x7fff_ffff_ffff_ffff, 420_000_000_002),
        210_000_000_000
    );
    assert_eq!(
        fast_range64(0x8000_0000_0000_0000, 420_000_000_002),
        210_000_000_001
    );
    assert_eq!(
        fast_range64(0xffff_ffff_ffff_ffff, 420_000_000_002),
        420_000_000_001
    );

    assert_eq!(
        fast_range64(0xffff_ffff_ffff_ffff, 0xffff_ffff_ffff_ffff),
        0xffff_ffff_ffff_fffe
    );
}

#[test]
fn fast_range_generic_values() {
    // Generic (big and small); FastRangeGeneric is also tested through FastRange32/64 above.
    assert_eq!(
        0x8000_0000_0000_0000u64.fast_range_generic(420_000_000_002u64),
        210_000_000_001u64
    );
    assert_eq!(
        0x8000_0000_0000_0000u64.fast_range_generic(12468u16),
        6234u16
    );
    assert_eq!(0x8000_0000u32.fast_range_generic(12468u16), 6234u16);
}

/// RocksDB's `test_BitOps<T>`, less `BitwiseAnd` (see the file comment). `$unsigned` enables
/// the `ReverseBits(vm1)` check RocksDB runs on unsigned types.
macro_rules! test_bit_ops {
    ($t:ty, $unsigned:expr) => {{
        type T = $t;
        let bits = T::BITS as i32;
        let size = std::mem::size_of::<T>() as i32;
        // This builds the pattern for every width, 128 bits included.
        let mut every_other_bit: T = 0;
        for _ in 0..size {
            every_other_bit = every_other_bit.checked_shl(8).unwrap_or(0) | 0x55;
        }

        // "v minus one", built with bit operations.
        let mut vm1: T = 0;

        for i in 0..bits {
            let v: T = (1 as T) << i;

            // BottomNBits
            {
                // The mask, the extremely slow way.
                let mut bottom_n_mask: T = 0;
                for _ in 0..i {
                    bottom_n_mask = (bottom_n_mask << 1) | 1;
                }

                // An essentially full-length value, made slightly irregular.
                let mut x = every_other_bit;
                if i > 2 {
                    x ^= (1 as T) << (i / 2);
                }
                let a = x.bottom_n_bits(i as u32);
                let b = (!x).bottom_n_bits(i as u32);

                assert_eq!(a, x & bottom_n_mask);
                assert_eq!(b, (!x) & bottom_n_mask);

                assert_eq!(x | a, x);
                assert_eq!(a | b, vm1);
                assert_eq!(a & b, 0);
                assert_eq!((x ^ a).bottom_n_bits(i as u32), 0);
            }

            // FloorLog2 (and ConstexprFloorLog2)
            if v > 0 {
                assert_eq!(v.floor_log2(), Some(i as u32));
            }
            if vm1 > 0 {
                assert_eq!(vm1.floor_log2(), Some((i - 1) as u32));
                assert_eq!(
                    (every_other_bit & vm1).floor_log2(),
                    Some(((i - 1) & !1) as u32)
                );
            }

            // CountTrailingZeroBits
            if v != 0 {
                assert_eq!(v.count_trailing_zero_bits(), Some(i as u32));
            }
            if vm1 != 0 {
                assert_eq!(vm1.count_trailing_zero_bits(), Some(0));
            }
            if i < bits - 1 {
                assert_eq!(
                    (!vm1 & every_other_bit).count_trailing_zero_bits(),
                    Some(((i + 1) & !1) as u32)
                );
            }

            // BitsSetToOne
            assert_eq!(v.bits_set_to_one(), 1);
            assert_eq!(vm1.bits_set_to_one(), i as u32);
            assert_eq!(
                (vm1 & every_other_bit).bits_set_to_one(),
                ((i + 1) / 2) as u32
            );

            // BitParity
            assert_eq!(v.bit_parity(), 1);
            assert_eq!(vm1.bit_parity(), (i & 1) as u32);
            assert_eq!(
                (vm1 & every_other_bit).bit_parity(),
                (((i + 1) / 2) & 1) as u32
            );

            // EndianSwapValue
            let ev: T = (1 as T) << (((size - 1 - (i / 8)) * 8) + i % 8);
            assert_eq!(v.endian_swap_value(), ev);

            // ReverseBits
            assert_eq!(v.reverse_bits_value(), (1 as T) << (bits - 1 - i));
            if $unsigned {
                let rv: T = (1 as T) << (bits - 1 - i);
                assert_eq!(vm1.reverse_bits_value(), rv.wrapping_mul(!(1 as T)));
            }

            // DownwardInvolution
            {
                let mut misc = 0xc682_cd15_3d0e_3279u64
                    .wrapping_add((i as u64).wrapping_mul(0x9b39_72f3_bea0_baa3))
                    as T;
                if size > 8 {
                    misc = (misc.wrapping_shl(64))
                        | (0x52af_031a_38ce_d62du64
                            .wrapping_add((i as u64).wrapping_mul(0x936f_803d_9752_ddc3))
                            as T);
                }
                let misc_masked = misc & vm1;
                assert!(misc_masked <= vm1);
                let di_misc_masked = misc_masked.downward_involution();
                assert!(di_misc_masked <= vm1);
                if misc_masked > 0 {
                    // The highest 1 stays where it was.
                    assert_eq!(misc_masked.floor_log2(), di_misc_masked.floor_log2());
                }
                // An involution on a short value...
                assert_eq!(di_misc_masked.downward_involution(), misc_masked);

                // ...and on a long one.
                let di_misc = misc.downward_involution();
                assert_eq!(di_misc.downward_involution(), misc);
                if misc > 0 {
                    assert_eq!(misc.floor_log2(), di_misc.floor_log2());
                }

                // It distributes over xor.
                assert_eq!(
                    (misc_masked ^ vm1).downward_involution(),
                    di_misc_masked ^ vm1.downward_involution()
                );
                let misc2 = misc >> 1;
                assert_eq!(
                    (misc ^ misc2).downward_involution(),
                    di_misc ^ misc2.downward_involution()
                );

                // A few bits pulled off test the combined uniqueness guarantee.
                let in_bits = (i % 7) as u32;
                let in_mask = (1u32 << in_bits) - 1;
                let mut seen = [false; 256];
                for j in 0..255 {
                    let t_in = misc ^ (j as T);
                    let inp = t_in as u32;
                    let out = t_in.downward_involution() as u32;
                    let val = (((out << in_bits) | (inp & in_mask)) & 255) as usize;
                    assert!(!seen[val]);
                    seen[val] = true;
                }

                if i + 8 < bits {
                    // Changing bits in the middle of the input is bijective in the bottom of
                    // the output.
                    let mut seen = [false; 256];
                    for j in 0..255 {
                        let inp = misc ^ ((j as T) << i);
                        let val = (inp.downward_involution() as u32 & 255) as usize;
                        assert!(!seen[val]);
                        seen[val] = true;
                    }
                }
            }

            vm1 = (vm1 << 1) | 1;
        }

        // ConstexprFloorLog2
        assert_eq!((1 as T).floor_log2(), Some(0));
        assert_eq!((2 as T).floor_log2(), Some(1));
        assert_eq!((3 as T).floor_log2(), Some(1));
        assert_eq!((42 as T).floor_log2(), Some(5));
    }};
}

#[test]
fn math_bit_ops() {
    test_bit_ops!(u32, true);
    test_bit_ops!(u64, true);
    test_bit_ops!(u16, true);
    test_bit_ops!(u8, true);
    test_bit_ops!(usize, true);
    test_bit_ops!(i32, false);
    test_bit_ops!(i64, false);
    test_bit_ops!(i16, false);
    test_bit_ops!(i8, false);
    test_bit_ops!(isize, false);
}

#[test]
fn math_bit_ops128() {
    test_bit_ops!(u128, true);
}

#[test]
fn math_math128() {
    let sixteen_hex_ones: u128 = 0x1111_1111_1111_1111;
    let thirty_hex_ones = (sixteen_hex_ones << 56) | sixteen_hex_ones;
    let sixteen_hex_twos: u128 = 0x2222_2222_2222_2222;
    let thirty_hex_twos = (sixteen_hex_twos << 56) | sixteen_hex_twos;

    // v slides from all hex ones to all hex twos.
    let mut v = thirty_hex_ones;
    for i in 0..=30u32 {
        // Bitwise operations
        assert_eq!(v.bits_set_to_one(), 30);
        assert_eq!((!v).bits_set_to_one(), 128 - 30);
        assert_eq!((v & thirty_hex_ones).bits_set_to_one(), 30 - i);
        assert_eq!((v | thirty_hex_ones).bits_set_to_one(), 30 + i);
        assert_eq!((v ^ thirty_hex_ones).bits_set_to_one(), 2 * i);
        assert_eq!((v & thirty_hex_twos).bits_set_to_one(), i);
        assert_eq!((v | thirty_hex_twos).bits_set_to_one(), 60 - i);
        assert_eq!((v ^ thirty_hex_twos).bits_set_to_one(), 60 - 2 * i);

        // Comparisons
        assert_eq!(v == thirty_hex_ones, i == 0);
        assert_eq!(v == thirty_hex_twos, i == 30);
        assert_eq!(v > thirty_hex_ones, i > 0);
        assert!(v <= thirty_hex_twos);
        assert!(v >= thirty_hex_ones);
        assert_eq!(v >= thirty_hex_twos, i == 30);
        assert!(v >= thirty_hex_ones);
        assert_eq!(v < thirty_hex_twos, i < 30);
        assert_eq!(v <= thirty_hex_ones, i == 0);
        assert!(v <= thirty_hex_twos);

        // Update v, clearing the uppermost byte.
        v = ((v << 12) >> 8) | 0x2;
    }

    for i in 0..128i32 {
        // Shifts
        let sl = thirty_hex_ones << i;
        let sr = thirty_hex_ones >> i;
        assert_eq!(sl.bits_set_to_one() as i32, 30.min(32 - i / 4));
        assert_eq!(sr.bits_set_to_one() as i32, 0.max(30 - (i + 3) / 4));
        assert_eq!(
            (sl & sr).bits_set_to_one() as i32,
            if i % 2 != 0 { 0 } else { 0.max(30 - i / 2) }
        );
    }

    // 64x64->128 multiply
    let product = multiply64to128(0x1111_1111_1111_1111, 0x2222_2222_2222_2222);
    assert_eq!(lower64of128(product), 2_295_594_818_061_633_090);
    assert_eq!(upper64of128(product), 163_971_058_432_973_792);
}

#[test]
fn math_coding128() {
    let input = b"_1234567890123456";
    // input[1..] is likely unaligned.
    let decoded = decode_fixed128(&input[1..]).unwrap();
    assert_eq!(lower64of128(decoded), 0x3837_3635_3433_3231);
    assert_eq!(upper64of128(decoded), 0x3635_3433_3231_3039);
    let mut out = b"_".to_vec();
    out.extend_from_slice(&encode_fixed128(decoded));
    assert_eq!(&input[..], &out[..]);
}

#[test]
fn math_coding_generic() {
    let input = b"_1234567890123456";
    // Decode; input[1..] is likely unaligned.
    let decoded128 = u128::decode_fixed_generic(&input[1..]).unwrap();
    assert_eq!(lower64of128(decoded128), 0x3837_3635_3433_3231);
    assert_eq!(upper64of128(decoded128), 0x3635_3433_3231_3039);

    let decoded64 = u64::decode_fixed_generic(&input[1..]).unwrap();
    assert_eq!(decoded64, 0x3837_3635_3433_3231);

    let decoded32 = u32::decode_fixed_generic(&input[1..]).unwrap();
    assert_eq!(decoded32, 0x3433_3231);

    let decoded16 = u16::decode_fixed_generic(&input[1..]).unwrap();
    assert_eq!(decoded16, 0x3231);

    // Encode
    let encode = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut out = b"_".to_vec();
        f(&mut out);
        out
    };
    assert_eq!(
        &input[..],
        &encode(&|o| decoded128.put_fixed_generic(o))[..]
    );
    assert_eq!(
        b"_12345678",
        &encode(&|o| decoded64.put_fixed_generic(o))[..]
    );
    assert_eq!(b"_1234", &encode(&|o| decoded32.put_fixed_generic(o))[..]);
    assert_eq!(b"_12", &encode(&|o| decoded16.put_fixed_generic(o))[..]);
}
