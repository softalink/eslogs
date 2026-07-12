//! Port of Softalink LLC `lib/encoding/zstd`.
//!
//! The Go package has two implementations selected by build tags: a pure-Go
//! one based on `github.com/klauspost/compress/zstd` (used by release
//! EsLogs binaries, which are built with CGO_ENABLED=0) and a cgo one
//! based on libzstd (`github.com/valyala/gozstd`). This port wraps the `zstd`
//! crate (libzstd bindings) and mirrors the pure-Go implementation's level
//! and limit semantics.
//!
//! PORT NOTE: klauspost is a Go-native zstd implementation, so the compressed
//! bytes produced here differ from the pure-Go build's output. Both outputs
//! are standard zstd frames without CRC (klauspost passes
//! `WithEncoderCRC(false)`; libzstd's checksum flag defaults to off), so each
//! side can decompress data produced by the other.

use std::cell::RefCell;
use std::io::Read;

use ::zstd::stream::read::Decoder;
use ::zstd::zstd_safe;

thread_local! {
    // PORT NOTE: Go caches one encoder per compression level in a global map.
    // A thread-local compression context gives the same context-reuse
    // behavior without cross-thread locking.
    static CCTX: RefCell<zstd_safe::CCtx<'static>> = RefCell::new(zstd_safe::CCtx::create());
}

/// Appends compressed src to dst.
///
/// The given compression_level is used for the compression.
pub fn compress_level(dst: &mut Vec<u8>, src: &[u8], compression_level: i32) {
    let real_compression_level = encoder_level_from_zstd(compression_level);
    let dst_len = dst.len();
    let bound = zstd_safe::compress_bound(src.len());
    dst.resize(dst_len + bound, 0);
    let n = CCTX.with(|cctx| {
        cctx.borrow_mut()
            .compress(&mut dst[dst_len..], src, real_compression_level)
    });
    match n {
        Ok(n) => dst.truncate(dst_len + n),
        Err(code) => {
            crate::panicf!(
                "BUG: unexpected error when compressing the data: {}",
                zstd_safe::get_error_name(code)
            );
            unreachable!();
        }
    }
}

// PORT NOTE: the pure-Go implementation converts the zstd compression level
// to one of the four klauspost encoder levels via `zstd.EncoderLevelFromZstd`
// (see github.com/klauspost/compress/zstd/encoder_options.go). Mirror the
// same bucketing, mapped back onto the libzstd levels the klauspost docs
// declare as roughly equivalent: SpeedFastest~1, SpeedDefault~3,
// SpeedBetterCompression~7, SpeedBestCompression~11.
fn encoder_level_from_zstd(level: i32) -> i32 {
    if level < 3 {
        1
    } else if level < 6 {
        // klauspost's SpeedDefault is documented as "roughly zstd level 3",
        // but it is tuned faster than libzstd-3 at a slightly worse ratio.
        // libzstd-2 is the closer match on the speed/ratio curve (measured on
        // the logs benchmark: ~equal output size to the Go binary, ~25%
        // faster block compression than libzstd-3).
        2
    } else if level < 10 {
        7
    } else {
        11
    }
}

/// Appends decompressed src to dst and returns the result.
///
/// This function must be called only for the trusted src.
///
/// Otherwise use decompress_limited function.
pub fn decompress(dst: &mut Vec<u8>, src: &[u8]) -> Result<(), String> {
    decompress_impl(dst, src, 0)
}

/// Appends decompressed src to dst and returns the result.
///
/// If the decompressed result exceeds max_data_size_bytes, then error is returned.
pub fn decompress_limited(
    dst: &mut Vec<u8>,
    src: &[u8],
    max_data_size_bytes: usize,
) -> Result<(), String> {
    decompress_impl(dst, src, max_data_size_bytes)
}

// PORT NOTE: the pure-Go implementation enforces the limit via klauspost's
// `WithDecoderMaxMemory`, which rejects both frames whose declared content
// size exceeds the limit and frames whose window size exceeds the limit. The
// same checks are mirrored here with an explicit frame content size check
// plus libzstd's windowLogMax parameter and an output byte cap. Error message
// wording differs from klauspost. max_data_size_bytes=0 means no limit
// (klauspost then defaults to a 64GiB cap, which is far beyond any block
// handled here).
fn decompress_impl(
    dst: &mut Vec<u8>,
    src: &[u8],
    max_data_size_bytes: usize,
) -> Result<(), String> {
    if let Ok(Some(content_size)) = zstd_safe::get_frame_content_size(src) {
        if max_data_size_bytes > 0 && content_size > max_data_size_bytes as u64 {
            return Err(format!(
                "decompressed data size {content_size} bytes exceeds the limit {max_data_size_bytes} bytes"
            ));
        }
        dst.reserve(content_size as usize);
    }
    let mut d =
        Decoder::with_buffer(src).map_err(|err| format!("cannot create zstd decoder: {err}"))?;
    d.window_log_max(window_log_max(max_data_size_bytes))
        .map_err(|err| format!("cannot set zstd decoder window size limit: {err}"))?;
    if max_data_size_bytes > 0 {
        let dst_len = dst.len();
        let mut limited = d.take(max_data_size_bytes as u64 + 1);
        limited
            .read_to_end(dst)
            .map_err(|err| format!("cannot decompress data: {err}"))?;
        if dst.len() - dst_len > max_data_size_bytes {
            return Err(format!(
                "decompressed data size exceeds the limit {max_data_size_bytes} bytes"
            ));
        }
    } else {
        d.read_to_end(dst)
            .map_err(|err| format!("cannot decompress data: {err}"))?;
    }
    Ok(())
}

fn window_log_max(max_data_size_bytes: usize) -> u32 {
    if max_data_size_bytes == 0 {
        // Mirror klauspost's 64GiB default; libzstd caps the window log at 31
        // on 64-bit targets.
        return 31;
    }
    // The largest window log whose window fits into the limit.
    // PORT NOTE: zstd window sizes aren't always powers of two, so frames
    // with a fractional window between 2^wl and the limit are rejected here
    // while klauspost would accept them. This only affects corrupted or
    // foreign frames close to the limit.
    let wl = 63 - (max_data_size_bytes as u64).leading_zeros();
    wl.clamp(10, 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_big_data() -> Vec<u8> {
        let mut bb = Vec::new();
        while bb.len() < 12 * 128 * 1024 {
            bb.extend_from_slice(format!("compress/decompress big data {}, ", bb.len()).as_bytes());
        }
        bb
    }

    #[test]
    fn test_decomrpess_limited_ok() {
        let f = |compressed_data: &[u8], limit: usize| {
            let mut dst = Vec::new();
            if let Err(err) = decompress_limited(&mut dst, compressed_data, limit) {
                panic!("cannot decompress data with limit={limit}: {err}");
            }
        };

        let origin_data = build_big_data();
        // block decompression
        let mut cd = Vec::new();
        compress_level(&mut cd, &origin_data, 0);

        // decompressed size matches block limit
        f(&cd, origin_data.len());

        // unlimited
        f(&cd, 0);
    }

    #[test]
    fn test_decompress_limited_fail() {
        let f = |input: &[u8], limit: usize| {
            let mut dst = Vec::new();
            if decompress_limited(&mut dst, input, limit).is_ok() {
                panic!("unexpected nil-error for decompress with limit: {limit}");
            }
        };

        let bb = build_big_data();

        // valid input bigger than limit
        f(&bb, 1024);

        // input with framecontent bigger than actual payload
        let input: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x84, 0x00, 0x00, 0x5e, 0xd0, 0xb2, 0x09, 0x00, 0x00, 0x30,
            0xec, 0xaf, 0x44, 0x12,
        ];
        f(input, 512);

        // input with stream windowSize bigger than limit
        let input: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x98, 0x19, 0x00, 0x00, 0x30, 0x30, 0x30, 0x4e, 0x8d,
            0xa2, 0x2b,
        ];
        f(input, 80_000_000);
    }

    // PORT NOTE: the Go cgo test cross-checks the klauspost and gozstd codecs
    // against each other; only one codec exists here, so the test degenerates
    // to a round-trip plus prefix-preservation check.
    #[test]
    fn test_compress_decompress() {
        test_compress_decompress_data(b"a");
        test_compress_decompress_data(b"foobarbaz");

        let mut rng = 1u64;
        let mut b = Vec::new();
        for _ in 0..64 * 1024 {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            b.push((rng.wrapping_mul(0x2545F4914F6CDD1D) >> 56) as u8);
        }
        test_compress_decompress_data(&b);
    }

    fn test_compress_decompress_data(b: &[u8]) {
        let mut bc = Vec::new();
        compress_level(&mut bc, b, 5);
        let mut b_new = Vec::new();
        if let Err(err) = decompress(&mut b_new, &bc) {
            panic!("unexpected error when decompressing b={b:x?} from bc={bc:x?}: {err}");
        }
        if b_new != b {
            panic!("invalid bNew; got\n{b_new:x?}; expecting\n{b:x?}");
        }

        let prefix: &[u8] = &[1, 2, 33];
        let mut bc_new = prefix.to_vec();
        compress_level(&mut bc_new, b, 5);
        if &bc_new[..prefix.len()] != prefix {
            panic!(
                "invalid prefix for b={b:x?}; got\n{:x?}; expecting\n{prefix:x?}",
                &bc_new[..prefix.len()]
            );
        }
        if bc_new[prefix.len()..] != bc[..] {
            panic!(
                "invalid prefixed bcNew for b={b:x?}; got\n{:x?}; expecting\n{bc:x?}",
                &bc_new[prefix.len()..]
            );
        }

        let mut b_new = prefix.to_vec();
        if let Err(err) = decompress(&mut b_new, &bc) {
            panic!(
                "unexpected error when decompressing b={b:x?} from bc={bc:x?} with prefix: {err}"
            );
        }
        if &b_new[..prefix.len()] != prefix {
            panic!(
                "invalid bNew prefix when decompressing bc={bc:x?}; got\n{:x?}; expecting\n{prefix:x?}",
                &b_new[..prefix.len()]
            );
        }
        if b_new[prefix.len()..] != b[..] {
            panic!(
                "invalid prefixed bNew; got\n{:x?}; expecting\n{b:x?}",
                &b_new[prefix.len()..]
            );
        }
    }
}
