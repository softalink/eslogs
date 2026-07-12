//! Port of Softalink LLC `lib/encoding`.
//!
//! Sources: `encoding.go`, `int.go`, `float.go`, `nearest_delta.go`,
//! `nearest_delta2.go`, `compress.go`, `util.go`. The on-disk byte format
//! produced by the marshal functions is byte-identical to the Go package.

pub mod zstd;

use std::sync::Mutex;

// minCompressibleBlockSize is the minimum block size in bytes for trying compression.
//
// There is no sense in compressing smaller blocks.
const MIN_COMPRESSIBLE_BLOCK_SIZE: usize = 128;

/// MarshalType is the type used for the marshaling.
// PORT NOTE: Go declares `type MarshalType byte` with free-standing constants;
// a newtype over u8 keeps arbitrary on-disk values representable while
// allowing the `needs_validation` method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarshalType(pub u8);

impl MarshalType {
    /// ZSTD_NEAREST_DELTA2 is used for marshaling counter timeseries.
    pub const ZSTD_NEAREST_DELTA2: MarshalType = MarshalType(1);

    /// DELTA_CONST is used for marshaling constantly changed time series with
    /// constant delta.
    pub const DELTA_CONST: MarshalType = MarshalType(2);

    /// CONST is used for marshaling time series containing only a single
    /// constant.
    pub const CONST: MarshalType = MarshalType(3);

    /// ZSTD_NEAREST_DELTA is used for marshaling gauge timeseries.
    pub const ZSTD_NEAREST_DELTA: MarshalType = MarshalType(4);

    /// NEAREST_DELTA2 is used instead of ZSTD_NEAREST_DELTA2 if compression
    /// doesn't help.
    pub const NEAREST_DELTA2: MarshalType = MarshalType(5);

    /// NEAREST_DELTA is used instead of ZSTD_NEAREST_DELTA if compression
    /// doesn't help.
    pub const NEAREST_DELTA: MarshalType = MarshalType(6);

    /// Returns true if mt may need additional validation for silent data corruption.
    pub fn needs_validation(self) -> bool {
        match self {
            MarshalType::NEAREST_DELTA2 | MarshalType::NEAREST_DELTA => true,
            // Other types do not need additional validation,
            // since they either already contain checksums (e.g. compressed data)
            // or they are trivial and cannot be validated (e.g. const or delta const)
            _ => false,
        }
    }
}

/// Verifies whether the mt is valid.
pub fn check_marshal_type(mt: MarshalType) -> Result<(), String> {
    // PORT NOTE: Go also checks `mt < 0`, which is unrepresentable for u8.
    if mt.0 > 6 {
        return Err(format!(
            "MarshalType should be in range [0..6]; got {}",
            mt.0
        ));
    }
    Ok(())
}

/// Makes sure precision_bits is in the range [1..64].
pub fn check_precision_bits(precision_bits: u8) -> Result<(), String> {
    if !(1..=64).contains(&precision_bits) {
        return Err(format!(
            "precisionBits must be in the range [1...64]; got {precision_bits}"
        ));
    }
    Ok(())
}

/// Marshals timestamps, appends the marshaled result to dst and returns
/// `(marshal_type, first_timestamp)`.
///
/// timestamps must contain non-decreasing values.
///
/// precision_bits must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
pub fn marshal_timestamps(
    dst: &mut Vec<u8>,
    timestamps: &[i64],
    precision_bits: u8,
) -> (MarshalType, i64) {
    marshal_int64_array(dst, timestamps, precision_bits)
}

/// Unmarshals timestamps from src and appends them to dst.
///
/// first_timestamp must be the timestamp returned from marshal_timestamps.
pub fn unmarshal_timestamps(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_timestamp: i64,
    items_count: usize,
) -> Result<(), String> {
    unmarshal_int64_array(dst, src, mt, first_timestamp, items_count).map_err(|err| {
        format!(
            "cannot unmarshal {} timestamps from len(src)={} bytes: {}",
            items_count,
            src.len(),
            err
        )
    })
}

/// Marshals values, appends the marshaled result to dst and returns
/// `(marshal_type, first_value)`.
///
/// precision_bits must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
pub fn marshal_values(dst: &mut Vec<u8>, values: &[i64], precision_bits: u8) -> (MarshalType, i64) {
    marshal_int64_array(dst, values, precision_bits)
}

/// Unmarshals values from src and appends them to dst.
///
/// first_value must be the value returned from marshal_values.
pub fn unmarshal_values(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    unmarshal_int64_array(dst, src, mt, first_value, items_count).map_err(|err| {
        format!(
            "cannot unmarshal {} values from len(src)={} bytes: {}",
            items_count,
            src.len(),
            err
        )
    })
}

fn marshal_int64_array(dst: &mut Vec<u8>, a: &[i64], precision_bits: u8) -> (MarshalType, i64) {
    if a.is_empty() {
        crate::panicf!("BUG: a must contain at least one item");
    }
    if is_const(a) {
        return (MarshalType::CONST, a[0]);
    }
    if is_delta_const(a) {
        let first_value = a[0];
        marshal_var_int64(dst, a[1].wrapping_sub(a[0]));
        return (MarshalType::DELTA_CONST, first_value);
    }

    let mut bb = get_byte_buffer();
    bb.clear();
    let mut mt;
    let first_value;
    if is_gauge(a) {
        // Gauge values are better compressed with delta encoding.
        mt = MarshalType::ZSTD_NEAREST_DELTA;
        let mut pb = precision_bits;
        if pb < 6 {
            // Increase precision bits for gauges, since they suffer more
            // from low precision bits comparing to counters.
            pb += 2;
        }
        first_value = marshal_int64_nearest_delta(&mut bb, a, pb);
    } else {
        // Non-gauge values, i.e. counters are better compressed with delta2 encoding.
        mt = MarshalType::ZSTD_NEAREST_DELTA2;
        first_value = marshal_int64_nearest_delta2(&mut bb, a, precision_bits);
    }

    // Try compressing the result.
    let dst_orig_len = dst.len();
    if bb.len() >= MIN_COMPRESSIBLE_BLOCK_SIZE {
        let compress_level = get_compress_level(a.len());
        compress_zstd_level(dst, &bb, compress_level);
    }
    if bb.len() < MIN_COMPRESSIBLE_BLOCK_SIZE
        || ((dst.len() - dst_orig_len) as f64) > 0.9 * (bb.len() as f64)
    {
        // Ineffective compression. Store plain data.
        mt = match mt {
            MarshalType::ZSTD_NEAREST_DELTA2 => MarshalType::NEAREST_DELTA2,
            MarshalType::ZSTD_NEAREST_DELTA => MarshalType::NEAREST_DELTA,
            _ => {
                crate::panicf!("BUG: unexpected mt={}", mt.0);
                unreachable!();
            }
        };
        dst.truncate(dst_orig_len);
        dst.extend_from_slice(&bb);
    }
    put_byte_buffer(bb);

    (mt, first_value)
}

// PORT NOTE: on error Go returns a nil slice while this port may leave
// partially appended data in dst; Go callers discard dst on error anyway.
fn unmarshal_int64_array(
    dst: &mut Vec<i64>,
    src: &[u8],
    mt: MarshalType,
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    // Extend dst capacity in order to eliminate memory allocations below.
    // PORT NOTE: Go calls decimal.ExtendInt64sCapacity; lib/decimal is still a
    // stub, so Vec::reserve provides the same behavior.
    dst.reserve(items_count);

    match mt {
        MarshalType::ZSTD_NEAREST_DELTA => {
            let mut bb = get_byte_buffer();
            bb.clear();
            if let Err(err) = decompress_zstd(&mut bb, src) {
                put_byte_buffer(bb);
                return Err(format!("cannot decompress zstd data: {err}"));
            }
            let res = unmarshal_int64_nearest_delta(dst, &bb, first_value, items_count);
            put_byte_buffer(bb);
            res.map_err(|err| {
                format!(
                    "cannot unmarshal nearest delta data after zstd decompression: {}; src_zstd={}",
                    err,
                    hex_upper(src)
                )
            })
        }
        MarshalType::ZSTD_NEAREST_DELTA2 => {
            let mut bb = get_byte_buffer();
            bb.clear();
            if let Err(err) = decompress_zstd(&mut bb, src) {
                put_byte_buffer(bb);
                return Err(format!("cannot decompress zstd data: {err}"));
            }
            let res = unmarshal_int64_nearest_delta2(dst, &bb, first_value, items_count);
            put_byte_buffer(bb);
            res.map_err(|err| {
                format!(
                    "cannot unmarshal nearest delta2 data after zstd decompression: {}; src_zstd={}",
                    err,
                    hex_upper(src)
                )
            })
        }
        MarshalType::NEAREST_DELTA => {
            unmarshal_int64_nearest_delta(dst, src, first_value, items_count)
                .map_err(|err| format!("cannot unmarshal nearest delta data: {err}"))
        }
        MarshalType::NEAREST_DELTA2 => {
            unmarshal_int64_nearest_delta2(dst, src, first_value, items_count)
                .map_err(|err| format!("cannot unmarshal nearest delta2 data: {err}"))
        }
        MarshalType::CONST => {
            if !src.is_empty() {
                return Err(format!(
                    "unexpected data left in const encoding: {} bytes",
                    src.len()
                ));
            }
            if first_value == 0 {
                append_int64_zeros(dst, items_count);
                return Ok(());
            }
            if first_value == 1 {
                append_int64_ones(dst, items_count);
                return Ok(());
            }
            for _ in 0..items_count {
                dst.push(first_value);
            }
            Ok(())
        }
        MarshalType::DELTA_CONST => {
            let mut v = first_value;
            let (d, n_len) = unmarshal_var_int64(src);
            if n_len <= 0 {
                return Err("cannot unmarshal delta value for delta const".to_string());
            }
            let n_len = n_len as usize;
            if n_len < src.len() {
                return Err(format!(
                    "unexpected trailing data after delta const (d={}): {} bytes",
                    d,
                    src.len() - n_len
                ));
            }
            for _ in 0..items_count {
                dst.push(v);
                v = v.wrapping_add(d);
            }
            Ok(())
        }
        _ => Err(format!("unknown MarshalType={}", mt.0)),
    }
}

/// Makes sure the first item in a is v_min, the last item in a is v_max and
/// all the items in a are non-decreasing.
///
/// If this isn't the case then a is fixed accordingly.
pub fn ensure_non_decreasing_sequence(a: &mut [i64], v_min: i64, v_max: i64) {
    if v_max < v_min {
        crate::panicf!(
            "BUG: vMax cannot be smaller than vMin; got {} vs {}",
            v_max,
            v_min
        );
    }
    if a.is_empty() {
        return;
    }
    if a[0] != v_min {
        a[0] = v_min;
    }
    let mut v_prev = a[0];
    for v in a[1..].iter_mut() {
        if *v < v_prev {
            *v = v_prev;
        }
        v_prev = *v;
    }
    let mut i = a.len() - 1;
    if a[i] != v_max {
        a[i] = v_max;
        while i > 0 {
            i -= 1;
            if a[i] <= v_max {
                break;
            }
            a[i] = v_max;
        }
    }
}

/// Returns true if a contains only equal values.
fn is_const(a: &[i64]) -> bool {
    if a.is_empty() {
        return false;
    }
    if is_int64_zeros(a) {
        // Fast path for array containing only zeros.
        return true;
    }
    if is_int64_ones(a) {
        // Fast path for array containing only ones.
        return true;
    }
    let v1 = a[0];
    a.iter().all(|&v| v == v1)
}

/// Returns true if a contains counter with constant delta.
fn is_delta_const(a: &[i64]) -> bool {
    if a.len() < 2 {
        return false;
    }
    let d1 = a[1].wrapping_sub(a[0]);
    let mut prev = a[1];
    for &next in &a[2..] {
        if next.wrapping_sub(prev) != d1 {
            return false;
        }
        prev = next;
    }
    true
}

/// Returns true if a contains gauge values, i.e. arbitrary changing values.
///
/// It is OK if a few gauges aren't detected (i.e. detected as counters),
/// since misdetected counters as gauges leads to worse compression ratio.
fn is_gauge(a: &[i64]) -> bool {
    // Check all the items in a, since a part of items may lead
    // to incorrect gauge detection.

    if a.len() < 2 {
        return false;
    }

    let mut resets = 0;
    let mut v_prev = a[0];
    if v_prev < 0 {
        // Counter values cannot be negative.
        return true;
    }
    for &v in &a[1..] {
        if v < v_prev {
            if v < 0 {
                // Counter values cannot be negative.
                return true;
            }
            if v > (v_prev >> 3) {
                // Decreasing sequence detected.
                // This is a gauge.
                return true;
            }
            // Possible counter reset.
            resets += 1;
        }
        v_prev = v;
    }
    if resets <= 2 {
        // Counter with a few resets.
        return false;
    }

    // Let it be a gauge if resets exceeds len(a)/8,
    // otherwise assume counter.
    resets > (a.len() >> 3)
}

fn get_compress_level(items_count: usize) -> i32 {
    if items_count <= 1 << 6 {
        return 1;
    }
    if items_count <= 1 << 8 {
        return 2;
    }
    if items_count <= 1 << 10 {
        return 3;
    }
    if items_count <= 1 << 12 {
        return 4;
    }
    5
}

//
// compress.go
//

// PORT NOTE: the Go package increments vm_zstd_block_* metrics counters in
// these wrappers; lib/metrics has no port yet, so the counters are omitted.

/// Appends compressed src to dst.
///
/// The given compress_level is used for the compression.
pub fn compress_zstd_level(dst: &mut Vec<u8>, src: &[u8], compress_level: i32) {
    zstd::compress_level(dst, src, compress_level);
}

/// Decompresses src and appends the result to dst.
///
/// This function must be called only for the trusted src.
/// Use decompress_zstd_limited for untrusted src.
pub fn decompress_zstd(dst: &mut Vec<u8>, src: &[u8]) -> Result<(), String> {
    zstd::decompress(dst, src).map_err(|err| {
        format!(
            "cannot decompress zstd block with len={}: {}; block data (hex): {}",
            src.len(),
            err,
            hex_upper(src)
        )
    })
}

/// Decompresses src and appends the result to dst.
///
/// If the decompressed result exceeds max_data_size_bytes, then error is returned.
pub fn decompress_zstd_limited(
    dst: &mut Vec<u8>,
    src: &[u8],
    max_data_size_bytes: usize,
) -> Result<(), String> {
    zstd::decompress_limited(dst, src, max_data_size_bytes).map_err(|err| {
        format!(
            "cannot decompress zstd block with len={} and maxDataSizeBytes={}: {}",
            src.len(),
            max_data_size_bytes,
            err
        )
    })
}

//
// util.go
//

/// Checks if the given data is compressed using the zstd format.
/// It does this by verifying the presence of the zstd magic number (0xFD2FB528)
/// at the beginning of the byte slice.
///
/// See: <https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#zstandard-frames>
pub fn is_zstd(data: &[u8]) -> bool {
    data.len() >= 4 && u32::from_le_bytes(data[..4].try_into().unwrap()) == 0xFD2F_B528
}

//
// int.go
//

/// Appends marshaled u to dst.
pub fn marshal_uint16(dst: &mut Vec<u8>, u: u16) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u16 from src.
///
/// The caller must ensure that len(src) >= 2
pub fn unmarshal_uint16(src: &[u8]) -> u16 {
    u16::from_be_bytes(src[..2].try_into().unwrap())
}

/// Appends marshaled u to dst.
pub fn marshal_uint32(dst: &mut Vec<u8>, u: u32) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u32 from src.
///
/// The caller must ensure that len(src) >= 4
pub fn unmarshal_uint32(src: &[u8]) -> u32 {
    u32::from_be_bytes(src[..4].try_into().unwrap())
}

/// Appends marshaled u to dst.
pub fn marshal_uint64(dst: &mut Vec<u8>, u: u64) {
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled u64 from src.
///
/// The caller must ensure that len(src) >= 8
pub fn unmarshal_uint64(src: &[u8]) -> u64 {
    u64::from_be_bytes(src[..8].try_into().unwrap())
}

/// Appends marshaled v to dst.
pub fn marshal_int16(dst: &mut Vec<u8>, v: i16) {
    // Such encoding for negative v must improve compression.
    let v = v.wrapping_shl(1) ^ (v >> 15); // zig-zag encoding without branching.
    let u = v as u16;
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled i16 from src.
///
/// The caller must ensure that len(src) >= 2
pub fn unmarshal_int16(src: &[u8]) -> i16 {
    let u = u16::from_be_bytes(src[..2].try_into().unwrap());
    ((u >> 1) as i16) ^ (((u << 15) as i16) >> 15) // zig-zag decoding without branching.
}

/// Appends marshaled v to dst.
pub fn marshal_int64(dst: &mut Vec<u8>, v: i64) {
    // Such encoding for negative v must improve compression.
    let v = v.wrapping_shl(1) ^ (v >> 63); // zig-zag encoding without branching.
    let u = v as u64;
    dst.extend_from_slice(&u.to_be_bytes());
}

/// Returns unmarshaled i64 from src.
///
/// The caller must ensure that len(src) >= 8
pub fn unmarshal_int64(src: &[u8]) -> i64 {
    let u = u64::from_be_bytes(src[..8].try_into().unwrap());
    ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63) // zig-zag decoding without branching.
}

/// Appends marshaled v to dst.
pub fn marshal_var_int64(dst: &mut Vec<u8>, v: i64) {
    let u = (v.wrapping_shl(1) ^ (v >> 63)) as u64;

    if v < (1 << 6) && v > -(1 << 6) {
        dst.push(u as u8);
        return;
    }
    if u < (1 << (2 * 7)) {
        dst.extend_from_slice(&[(u | 0x80) as u8, (u >> 7) as u8]);
        return;
    }
    if u < (1 << (3 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            (u >> (2 * 7)) as u8,
        ]);
        return;
    }

    // Slow path for big integers
    write_var_uint64_slow(dst, u);
}

/// Appends marshaled vs to dst.
pub fn marshal_var_int64s(dst: &mut Vec<u8>, vs: &[i64]) {
    let dst_len = dst.len();
    for &v in vs {
        if v >= (1 << 6) || v <= -(1 << 6) {
            dst.truncate(dst_len);
            marshal_var_int64s_slow(dst, vs);
            return;
        }
        let u = (v.wrapping_shl(1) ^ (v >> 63)) as u64;
        dst.push(u as u8);
    }
}

fn marshal_var_int64s_slow(dst: &mut Vec<u8>, vs: &[i64]) {
    for &v in vs {
        let u = (v.wrapping_shl(1) ^ (v >> 63)) as u64;
        write_var_uint64_slow(dst, u);
    }
}

// PORT NOTE: Go duplicates this branch chain in marshalVarInt64sSlow and
// marshalVarUint64sSlow; both produce identical bytes, so a shared helper is
// used here.
fn write_var_uint64_slow(dst: &mut Vec<u8>, u: u64) {
    // Cases below are sorted in the descending order of frequency on real data
    if u < (1 << 7) {
        dst.push(u as u8);
        return;
    }
    if u < (1 << (2 * 7)) {
        dst.extend_from_slice(&[(u | 0x80) as u8, (u >> 7) as u8]);
        return;
    }
    if u < (1 << (3 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            (u >> (2 * 7)) as u8,
        ]);
        return;
    }

    if u >= (1 << (8 * 7)) {
        if u < (1 << (9 * 7)) {
            dst.extend_from_slice(&[
                (u | 0x80) as u8,
                ((u >> 7) | 0x80) as u8,
                ((u >> (2 * 7)) | 0x80) as u8,
                ((u >> (3 * 7)) | 0x80) as u8,
                ((u >> (4 * 7)) | 0x80) as u8,
                ((u >> (5 * 7)) | 0x80) as u8,
                ((u >> (6 * 7)) | 0x80) as u8,
                ((u >> (7 * 7)) | 0x80) as u8,
                (u >> (8 * 7)) as u8,
            ]);
            return;
        }
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            ((u >> (5 * 7)) | 0x80) as u8,
            ((u >> (6 * 7)) | 0x80) as u8,
            ((u >> (7 * 7)) | 0x80) as u8,
            ((u >> (8 * 7)) | 0x80) as u8,
            1,
        ]);
        return;
    }

    if u < (1 << (4 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            (u >> (3 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (5 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            (u >> (4 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (6 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            (u >> (5 * 7)) as u8,
        ]);
        return;
    }
    if u < (1 << (7 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            ((u >> (2 * 7)) | 0x80) as u8,
            ((u >> (3 * 7)) | 0x80) as u8,
            ((u >> (4 * 7)) | 0x80) as u8,
            ((u >> (5 * 7)) | 0x80) as u8,
            (u >> (6 * 7)) as u8,
        ]);
        return;
    }
    dst.extend_from_slice(&[
        (u | 0x80) as u8,
        ((u >> 7) | 0x80) as u8,
        ((u >> (2 * 7)) | 0x80) as u8,
        ((u >> (3 * 7)) | 0x80) as u8,
        ((u >> (4 * 7)) | 0x80) as u8,
        ((u >> (5 * 7)) | 0x80) as u8,
        ((u >> (6 * 7)) | 0x80) as u8,
        (u >> (7 * 7)) as u8,
    ]);
}

/// Returns unmarshaled i64 from src and its size in bytes.
///
/// It returns 0 or negative size if it cannot unmarshal i64 from src.
pub fn unmarshal_var_int64(src: &[u8]) -> (i64, isize) {
    let (u64v, n_size) = uvarint(src);
    let i64v = ((u64v >> 1) as i64) ^ (((u64v << 63) as i64) >> 63);
    (i64v, n_size)
}

/// Unmarshals dst.len() i64 values from src to dst and returns the remaining
/// tail from src.
pub fn unmarshal_var_int64s<'a>(dst: &mut [i64], src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.len() < dst.len() {
        return Err(format!(
            "too small len(src)={}; it must be bigger or equal to len(dst)={}",
            src.len(),
            dst.len()
        ));
    }
    let mut need_slow = false;
    for (out, &c) in dst.iter_mut().zip(src.iter()) {
        if c >= 0x80 {
            need_slow = true;
            break;
        }
        *out = (((c >> 1) as i8) ^ (((c << 7) as i8) >> 7)) as i64;
    }
    if need_slow {
        return unmarshal_var_int64s_slow(dst, src);
    }
    Ok(&src[dst.len()..])
}

fn unmarshal_var_int64s_slow<'a>(dst: &mut [i64], src: &'a [u8]) -> Result<&'a [u8], String> {
    let mut idx = 0usize;
    for out in dst.iter_mut() {
        if idx >= src.len() {
            return Err("cannot unmarshal varint from empty data".to_string());
        }
        let c = src[idx];
        idx += 1;
        if c < 0x80 {
            // Fast path for 1 byte
            *out = (((c >> 1) as i8) ^ (((c << 7) as i8) >> 7)) as i64;
            continue;
        }

        if idx >= src.len() {
            return Err(format!(
                "unexpected end of encoded varint at byte 1; src={}",
                hex_lower(&src[idx - 1..])
            ));
        }
        let d = src[idx];
        idx += 1;
        if d < 0x80 {
            // Fast path for 2 bytes
            let u = ((c & 0x7f) as u64) | ((d as u64) << 7);
            *out = ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63);
            continue;
        }

        if idx >= src.len() {
            return Err(format!(
                "unexpected end of encoded varint at byte 2; src={}",
                hex_lower(&src[idx - 2..])
            ));
        }
        let e = src[idx];
        idx += 1;
        if e < 0x80 {
            // Fast path for 3 bytes
            let u = ((c & 0x7f) as u64) | (((d & 0x7f) as u64) << 7) | ((e as u64) << (2 * 7));
            *out = ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63);
            continue;
        }

        let u = ((c & 0x7f) as u64) | (((d & 0x7f) as u64) << 7) | (((e & 0x7f) as u64) << (2 * 7));
        let u = match unmarshal_var_uint64_tail(src, &mut idx, u, true)? {
            Some(u) => u,
            None => unreachable!(),
        };
        *out = ((u >> 1) as i64) ^ (((u << 63) as i64) >> 63);
    }
    Ok(&src[idx..])
}

/// Appends marshaled u to dst.
pub fn marshal_var_uint64(dst: &mut Vec<u8>, u: u64) {
    if u < (1 << 7) {
        dst.push(u as u8);
        return;
    }
    if u < (1 << (2 * 7)) {
        dst.extend_from_slice(&[(u | 0x80) as u8, (u >> 7) as u8]);
        return;
    }
    if u < (1 << (3 * 7)) {
        dst.extend_from_slice(&[
            (u | 0x80) as u8,
            ((u >> 7) | 0x80) as u8,
            (u >> (2 * 7)) as u8,
        ]);
        return;
    }

    // Slow path for big integers.
    write_var_uint64_slow(dst, u);
}

/// Appends marshaled us to dst.
pub fn marshal_var_uint64s(dst: &mut Vec<u8>, us: &[u64]) {
    let dst_len = dst.len();
    for &u in us {
        if u >= (1 << 7) {
            dst.truncate(dst_len);
            marshal_var_uint64s_slow(dst, us);
            return;
        }
        dst.push(u as u8);
    }
}

fn marshal_var_uint64s_slow(dst: &mut Vec<u8>, us: &[u64]) {
    for &u in us {
        write_var_uint64_slow(dst, u);
    }
}

/// Returns unmarshaled u64 from src and its size in bytes.
///
/// It returns 0 or negative size if it cannot unmarshal u64 from src.
pub fn unmarshal_var_uint64(src: &[u8]) -> (u64, isize) {
    if src.is_empty() {
        return (0, 0);
    }
    if src[0] < 0x80 {
        // Fast path for a single byte
        return (src[0] as u64, 1);
    }
    if src.len() == 1 {
        return (0, 0);
    }
    if src[1] < 0x80 {
        // Fast path for two bytes
        return (((src[0] & 0x7f) as u64) | ((src[1] as u64) << 7), 2);
    }

    // Slow path for other number of bytes
    uvarint(src)
}

// Port of Go's binary.Uvarint.
fn uvarint(buf: &[u8]) -> (u64, isize) {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            // Catch byte reads past MaxVarintLen64.
            return (0, -(i as isize + 1)); // overflow
        }
        if b < 0x80 {
            if i == 10 - 1 && b > 1 {
                return (0, -(i as isize + 1)); // overflow
            }
            return (x | ((b as u64) << s), i as isize + 1);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

/// Unmarshals dst.len() u64 values from src to dst and returns the remaining
/// tail from src.
pub fn unmarshal_var_uint64s<'a>(dst: &mut [u64], src: &'a [u8]) -> Result<&'a [u8], String> {
    if src.len() < dst.len() {
        return Err(format!(
            "too small len(src)={}; it must be bigger or equal to len(dst)={}",
            src.len(),
            dst.len()
        ));
    }
    let mut need_slow = false;
    for (out, &c) in dst.iter_mut().zip(src.iter()) {
        if c >= 0x80 {
            need_slow = true;
            break;
        }
        *out = c as u64;
    }
    if need_slow {
        return unmarshal_var_uint64s_slow(dst, src);
    }
    Ok(&src[dst.len()..])
}

fn unmarshal_var_uint64s_slow<'a>(dst: &mut [u64], src: &'a [u8]) -> Result<&'a [u8], String> {
    let mut idx = 0usize;
    for out in dst.iter_mut() {
        if idx >= src.len() {
            return Err("cannot unmarshal varuint from empty data".to_string());
        }
        let c = src[idx];
        idx += 1;
        if c < 0x80 {
            // Fast path for 1 byte
            *out = c as u64;
            continue;
        }

        if idx >= src.len() {
            return Err(format!(
                "unexpected end of encoded varuint at byte 1; src={}",
                hex_lower(&src[idx - 1..])
            ));
        }
        let d = src[idx];
        idx += 1;
        if d < 0x80 {
            // Fast path for 2 bytes
            *out = ((c & 0x7f) as u64) | ((d as u64) << 7);
            continue;
        }

        if idx >= src.len() {
            return Err(format!(
                "unexpected end of encoded varuint at byte 2; src={}",
                hex_lower(&src[idx - 2..])
            ));
        }
        let e = src[idx];
        idx += 1;
        if e < 0x80 {
            // Fast path for 3 bytes
            *out = ((c & 0x7f) as u64) | (((d & 0x7f) as u64) << 7) | ((e as u64) << (2 * 7));
            continue;
        }

        let u = ((c & 0x7f) as u64) | (((d & 0x7f) as u64) << 7) | (((e & 0x7f) as u64) << (2 * 7));
        let u = match unmarshal_var_uint64_tail(src, &mut idx, u, false)? {
            Some(u) => u,
            None => unreachable!(),
        };
        *out = u;
    }
    Ok(&src[idx..])
}

// Shared slow path for varint bytes 4..10. `signed` only selects the error
// message wording ("varint" vs "varuint") to match the Go sources.
fn unmarshal_var_uint64_tail(
    src: &[u8],
    idx: &mut usize,
    mut u: u64,
    signed: bool,
) -> Result<Option<u64>, String> {
    let kind = if signed { "varint" } else { "varuint" };

    // Slow path
    let j = *idx;
    loop {
        if *idx >= src.len() {
            return Err(format!(
                "unexpected end of encoded varint; src={}",
                hex_lower(&src[j - 3..])
            ));
        }
        let c = src[*idx];
        *idx += 1;
        if c < 0x80 {
            break;
        }
    }

    // These are the most common cases
    match *idx - j {
        1 => {
            u |= (src[j] as u64) << (3 * 7);
        }
        2 => {
            let b = &src[j..j + 2];
            u |= (((b[0] & 0x7f) as u64) << (3 * 7)) | ((b[1] as u64) << (4 * 7));
        }
        3 => {
            let b = &src[j..j + 3];
            u |= (((b[0] & 0x7f) as u64) << (3 * 7))
                | (((b[1] & 0x7f) as u64) << (4 * 7))
                | ((b[2] as u64) << (5 * 7));
        }
        4 => {
            let b = &src[j..j + 4];
            u |= (((b[0] & 0x7f) as u64) << (3 * 7))
                | (((b[1] & 0x7f) as u64) << (4 * 7))
                | (((b[2] & 0x7f) as u64) << (5 * 7))
                | ((b[3] as u64) << (6 * 7));
        }
        5 => {
            let b = &src[j..j + 5];
            u |= (((b[0] & 0x7f) as u64) << (3 * 7))
                | (((b[1] & 0x7f) as u64) << (4 * 7))
                | (((b[2] & 0x7f) as u64) << (5 * 7))
                | (((b[3] & 0x7f) as u64) << (6 * 7))
                | ((b[4] as u64) << (7 * 7));
        }
        6 => {
            let b = &src[j..j + 6];
            u |= (((b[0] & 0x7f) as u64) << (3 * 7))
                | (((b[1] & 0x7f) as u64) << (4 * 7))
                | (((b[2] & 0x7f) as u64) << (5 * 7))
                | (((b[3] & 0x7f) as u64) << (6 * 7))
                | (((b[4] & 0x7f) as u64) << (7 * 7))
                | ((b[5] as u64) << (8 * 7));
        }
        7 => {
            let b = &src[j..j + 7];
            if b[6] > 1 {
                return Err(format!(
                    "too big encoded {kind}; src={}",
                    hex_lower(&src[j - 3..])
                ));
            }
            u |= (((b[0] & 0x7f) as u64) << (3 * 7))
                | (((b[1] & 0x7f) as u64) << (4 * 7))
                | (((b[2] & 0x7f) as u64) << (5 * 7))
                | (((b[3] & 0x7f) as u64) << (6 * 7))
                | (((b[4] & 0x7f) as u64) << (7 * 7))
                | (((b[5] & 0x7f) as u64) << (8 * 7))
                | (1 << (9 * 7));
        }
        n => {
            return Err(format!(
                "too long encoded {kind}; the maximum allowed length is 10 bytes; got {} bytes; src={}",
                n + 3,
                hex_lower(&src[j - 3..])
            ));
        }
    }

    Ok(Some(u))
}

/// Appends marshaled v to dst.
pub fn marshal_bool(dst: &mut Vec<u8>, v: bool) {
    dst.push(v as u8);
}

/// Unmarshals bool from src.
pub fn unmarshal_bool(src: &[u8]) -> bool {
    src[0] != 0
}

/// Appends marshaled b to dst.
pub fn marshal_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    marshal_var_uint64(dst, b.len() as u64);
    dst.extend_from_slice(b);
}

/// Returns unmarshaled bytes from src and the size of the unmarshaled bytes.
///
/// It returns `(None, 0)` if it is impossible to unmarshal bytes from src.
pub fn unmarshal_bytes(src: &[u8]) -> (Option<&[u8]>, isize) {
    let (n, n_size) = unmarshal_var_uint64(src);
    if n_size <= 0 {
        return (None, 0);
    }
    let start = n_size as usize;
    if n > (src.len() - start) as u64 {
        return (None, 0);
    }
    let end = start + n as usize;
    (Some(&src[start..end]), end as isize)
}

//
// Slice pools.
//
// PORT NOTE: Go uses sync.Pool; a Mutex<Vec<...>> free list gives the same
// buffer-reuse behavior. Go's slicesutil.SetLength leaves grown slice
// contents uninitialized; safe Rust zero-fills the grown part instead —
// callers overwrite the contents anyway.
//

/// Int64s holds an i64 slice.
pub struct Int64s {
    pub a: Vec<i64>,
}

static INT64S_POOL: Mutex<Vec<Vec<i64>>> = Mutex::new(Vec::new());

/// Returns an i64 slice with the given size.
pub fn get_int64s(size: usize) -> Int64s {
    let mut a = INT64S_POOL.lock().unwrap().pop().unwrap_or_default();
    a.resize(size, 0);
    Int64s { a }
}

/// Returns is to the pool.
pub fn put_int64s(is: Int64s) {
    INT64S_POOL.lock().unwrap().push(is.a);
}

/// Uint64s holds an u64 slice.
pub struct Uint64s {
    pub a: Vec<u64>,
}

static UINT64S_POOL: Mutex<Vec<Vec<u64>>> = Mutex::new(Vec::new());

/// Returns an u64 slice with the given size.
pub fn get_uint64s(size: usize) -> Uint64s {
    let mut a = UINT64S_POOL.lock().unwrap().pop().unwrap_or_default();
    a.resize(size, 0);
    Uint64s { a }
}

/// Returns is to the pool.
pub fn put_uint64s(is: Uint64s) {
    UINT64S_POOL.lock().unwrap().push(is.a);
}

/// Uint32s holds an u32 slice.
pub struct Uint32s {
    pub a: Vec<u32>,
}

static UINT32S_POOL: Mutex<Vec<Vec<u32>>> = Mutex::new(Vec::new());

/// Returns an u32 slice with the given size.
pub fn get_uint32s(size: usize) -> Uint32s {
    let mut a = UINT32S_POOL.lock().unwrap().pop().unwrap_or_default();
    a.resize(size, 0);
    Uint32s { a }
}

/// Returns is to the pool.
pub fn put_uint32s(is: Uint32s) {
    UINT32S_POOL.lock().unwrap().push(is.a);
}

//
// float.go
//

/// Float64s holds an array of f64 values.
pub struct Float64s {
    pub a: Vec<f64>,
}

static FLOAT64S_POOL: Mutex<Vec<Vec<f64>>> = Mutex::new(Vec::new());

/// Returns a slice of f64 values with the given size.
///
/// When the returned slice is no longer needed, it is advised calling
/// put_float64s() on it, so it could be reused.
pub fn get_float64s(size: usize) -> Float64s {
    let mut a = FLOAT64S_POOL.lock().unwrap().pop().unwrap_or_default();
    a.resize(size, 0.0);
    Float64s { a }
}

/// Returns a to the pool, so it can be reused via get_float64s.
pub fn put_float64s(mut a: Float64s) {
    a.a.clear();
    FLOAT64S_POOL.lock().unwrap().push(a.a);
}

// PORT NOTE: Go uses a bytesutil.ByteBufferPool here; lib/bytesutil is still
// a stub, so a minimal private Vec<u8> free list provides the same reuse
// behavior.
static BB_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

fn get_byte_buffer() -> Vec<u8> {
    BB_POOL.lock().unwrap().pop().unwrap_or_default()
}

fn put_byte_buffer(b: Vec<u8>) {
    BB_POOL.lock().unwrap().push(b);
}

//
// nearest_delta.go
//

/// Encodes src using `nearest delta` encoding with the given precision_bits,
/// appends the encoded value to dst and returns the first value.
///
/// precision_bits must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
fn marshal_int64_nearest_delta(dst: &mut Vec<u8>, src: &[i64], precision_bits: u8) -> i64 {
    if src.is_empty() {
        crate::panicf!(
            "BUG: src must contain at least 1 item; got {} items",
            src.len()
        );
    }
    if let Err(err) = check_precision_bits(precision_bits) {
        crate::panicf!("BUG: {}", err);
    }

    let first_value = src[0];
    let mut v = src[0];
    let src = &src[1..];
    let mut is = get_int64s(src.len());
    if precision_bits == 64 {
        // Fast path.
        for (i, &next) in src.iter().enumerate() {
            let d = next.wrapping_sub(v);
            v = v.wrapping_add(d);
            is.a[i] = d;
        }
    } else {
        // Slower path.
        let mut trailing_zeros = get_trailing_zeros(v, precision_bits);
        for (i, &next) in src.iter().enumerate() {
            let (d, tzs) = nearest_delta(next, v, precision_bits, trailing_zeros);
            trailing_zeros = tzs;
            v = v.wrapping_add(d);
            is.a[i] = d;
        }
    }
    marshal_var_int64s(dst, &is.a);
    put_int64s(is);
    first_value
}

/// Decodes src using `nearest delta` encoding and appends the result to dst.
///
/// The first_value must be the value returned from marshal_int64_nearest_delta.
fn unmarshal_int64_nearest_delta(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    if items_count < 1 {
        crate::panicf!(
            "BUG: itemsCount must be greater than 0; got {}",
            items_count
        );
    }

    let mut is = get_int64s(items_count - 1);

    let res = unmarshal_var_int64s(&mut is.a, src);
    let err = match res {
        Err(err) => Some(format!(
            "cannot unmarshal nearest delta from {} bytes; src={}: {}",
            src.len(),
            hex_upper(src),
            err
        )),
        Ok(tail) if !tail.is_empty() => Some(format!(
            "unexpected tail left after unmarshaling {} items from {} bytes; tail size={}; src={}; tail={}",
            items_count,
            src.len(),
            tail.len(),
            hex_upper(src),
            hex_upper(tail)
        )),
        Ok(_) => None,
    };
    if let Some(err) = err {
        put_int64s(is);
        return Err(err);
    }

    let mut v = first_value;
    dst.push(v);
    for &d in &is.a {
        v = v.wrapping_add(d);
        dst.push(v);
    }
    put_int64s(is);
    Ok(())
}

/// Returns the nearest value for (next-prev) with the given precision_bits.
///
/// The second returned value is the number of zeroed trailing bits in the
/// returned delta.
fn nearest_delta(next: i64, prev: i64, precision_bits: u8, prev_trailing_zeros: u8) -> (i64, u8) {
    let d = next.wrapping_sub(prev);
    if d == 0 {
        // Fast path.
        return (0, dec_if_non_zero(prev_trailing_zeros));
    }

    let mut origin = next;
    if origin < 0 {
        origin = origin.wrapping_neg();
        // There is no need in handling special case origin = -1<<63.
    }

    let origin_bits = (64 - (origin as u64).leading_zeros()) as u8;
    if origin_bits <= precision_bits {
        // Cannot zero trailing bits for the given precision_bits.
        return (d, dec_if_non_zero(prev_trailing_zeros));
    }

    // origin_bits > precision_bits. May zero trailing bits in d.
    let trailing_zeros = origin_bits - precision_bits;
    if trailing_zeros > prev_trailing_zeros + 4 {
        // Probably counter reset. Return d with full precision.
        return (d, prev_trailing_zeros + 2);
    }
    if trailing_zeros + 4 < prev_trailing_zeros {
        // Probably counter reset. Return d with full precision.
        return (d, prev_trailing_zeros - 2);
    }

    // zero trailing bits in d.
    let mut minus = false;
    let mut d = d;
    if d < 0 {
        minus = true;
        d = d.wrapping_neg();
        // There is no need in handling special case d = -1<<63.
    }
    let mut nd = ((d as u64) & (u64::MAX << trailing_zeros)) as i64;
    if minus {
        nd = nd.wrapping_neg();
    }
    (nd, trailing_zeros)
}

fn dec_if_non_zero(n: u8) -> u8 {
    if n == 0 {
        return 0;
    }
    n - 1
}

fn get_trailing_zeros(v: i64, precision_bits: u8) -> u8 {
    let mut v = v;
    if v < 0 {
        v = v.wrapping_neg();
        // There is no need in special case handling for v = -1<<63
    }
    let v_bits = (64 - (v as u64).leading_zeros()) as u8;
    if v_bits <= precision_bits {
        return 0;
    }
    v_bits - precision_bits
}

//
// nearest_delta2.go
//

/// Encodes src using `nearest delta2` encoding with the given precision_bits,
/// appends the encoded value to dst and returns the first value.
///
/// precision_bits must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
fn marshal_int64_nearest_delta2(dst: &mut Vec<u8>, src: &[i64], precision_bits: u8) -> i64 {
    if src.len() < 2 {
        crate::panicf!(
            "BUG: src must contain at least 2 items; got {} items",
            src.len()
        );
    }
    if let Err(err) = check_precision_bits(precision_bits) {
        crate::panicf!("BUG: {}", err);
    }

    let first_value = src[0];
    let mut d1 = src[1].wrapping_sub(src[0]);
    marshal_var_int64(dst, d1);
    let mut v = src[1];
    let src = &src[2..];
    let mut is = get_int64s(src.len());
    if precision_bits == 64 {
        // Fast path.
        for (i, &next) in src.iter().enumerate() {
            let d2 = next.wrapping_sub(v).wrapping_sub(d1);
            d1 = d1.wrapping_add(d2);
            v = v.wrapping_add(d1);
            is.a[i] = d2;
        }
    } else {
        // Slower path.
        let mut trailing_zeros = get_trailing_zeros(v, precision_bits);
        for (i, &next) in src.iter().enumerate() {
            let (d2, tzs) = nearest_delta(next.wrapping_sub(v), d1, precision_bits, trailing_zeros);
            trailing_zeros = tzs;
            d1 = d1.wrapping_add(d2);
            v = v.wrapping_add(d1);
            is.a[i] = d2;
        }
    }
    marshal_var_int64s(dst, &is.a);
    put_int64s(is);
    first_value
}

/// Decodes src using `nearest delta2` encoding and appends the result to dst.
///
/// first_value must be the value returned from marshal_int64_nearest_delta2.
fn unmarshal_int64_nearest_delta2(
    dst: &mut Vec<i64>,
    src: &[u8],
    first_value: i64,
    items_count: usize,
) -> Result<(), String> {
    if items_count < 2 {
        crate::panicf!(
            "BUG: itemsCount must be greater than 1; got {}",
            items_count
        );
    }

    let mut is = get_int64s(items_count - 1);

    let res = unmarshal_var_int64s(&mut is.a, src);
    let err = match res {
        Err(err) => Some(format!(
            "cannot unmarshal nearest delta from {} bytes; src={}: {}",
            src.len(),
            hex_upper(src),
            err
        )),
        Ok(tail) if !tail.is_empty() => Some(format!(
            "unexpected tail left after unmarshaling {} items from {} bytes; tail size={}; src={}; tail={}",
            items_count,
            src.len(),
            tail.len(),
            hex_upper(src),
            hex_upper(tail)
        )),
        Ok(_) => None,
    };
    if let Some(err) = err {
        put_int64s(is);
        return Err(err);
    }

    let dst_len = dst.len();
    dst.resize(dst_len + items_count, 0);
    let as_ = &mut dst[dst_len..];

    let mut v = first_value;
    let mut d1 = is.a[0];
    as_[0] = v;
    v = v.wrapping_add(d1);
    as_[1] = v;
    let as_ = &mut as_[2..];
    for (i, &d2) in is.a[1..].iter().enumerate() {
        d1 = d1.wrapping_add(d2);
        v = v.wrapping_add(d1);
        as_[i] = v;
    }

    put_int64s(is);
    Ok(())
}

//
// Private helpers standing in for still-unported esl-common modules.
//

// PORT NOTE: lib/fastnum is still a stub; these minimal private helpers match
// the behavior of fastnum.IsInt64Zeros/IsInt64Ones/AppendInt64Zeros/
// AppendInt64Ones for the cases needed here.
fn is_int64_zeros(a: &[i64]) -> bool {
    a.iter().all(|&v| v == 0)
}

fn is_int64_ones(a: &[i64]) -> bool {
    a.iter().all(|&v| v == 1)
}

fn append_int64_zeros(dst: &mut Vec<i64>, items_count: usize) {
    dst.resize(dst.len() + items_count, 0);
}

fn append_int64_ones(dst: &mut Vec<i64>, items_count: usize) {
    dst.resize(dst.len() + items_count, 1);
}

// Helpers matching Go's fmt `%x` / `%X` formatting of byte slices in error
// messages.
fn hex_lower(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn hex_upper(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: the Go tests seed math/rand with rand.NewSource(1). The Go
    // PRNG stream cannot be reproduced without porting math/rand's internal
    // state tables, so a deterministic xorshift64* generator with a
    // Box-Muller NormFloat64 substitute is used. Distribution-sensitive
    // expectations (MarshalType boundaries, marshaled sizes) were re-verified
    // against this generator; see the PORT NOTEs on the affected tests.
    struct GoRand {
        state: u64,
    }

    impl GoRand {
        fn new(seed: u64) -> GoRand {
            GoRand {
                state: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            }
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.state = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn float64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }

        fn norm_float64(&mut self) -> f64 {
            loop {
                let u1 = self.float64();
                if u1 > 0.0 {
                    let u2 = self.float64();
                    return (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                }
            }
        }

        fn int63n(&mut self, n: i64) -> i64 {
            ((self.next_u64() >> 1) % (n as u64)) as i64
        }
    }

    //
    // int_test.go
    //

    #[test]
    fn test_marshal_unmarshal_uint16() {
        let f = |u: u16| {
            let mut b = Vec::new();
            marshal_uint16(&mut b, u);
            assert_eq!(
                b.len(),
                2,
                "unexpected b length: {}; expecting {}",
                b.len(),
                2
            );
            let u_new = unmarshal_uint16(&b);
            assert_eq!(
                u_new, u,
                "unexpected uNew from b={b:x?}; got {u_new}; expecting {u}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_uint16(&mut b1, u);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for u={u}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for u={u}");
        };
        f(0);
        f(1);
        f(u16::MAX);
        f((1 << 15) + 1);
        f((1 << 15) - 1);
        f(1 << 15);

        for i in 0..10_000u16 {
            f(i);
        }
    }

    #[test]
    fn test_marshal_unmarshal_uint32() {
        let f = |u: u32| {
            let mut b = Vec::new();
            marshal_uint32(&mut b, u);
            assert_eq!(
                b.len(),
                4,
                "unexpected b length: {}; expecting {}",
                b.len(),
                4
            );
            let u_new = unmarshal_uint32(&b);
            assert_eq!(
                u_new, u,
                "unexpected uNew from b={b:x?}; got {u_new}; expecting {u}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_uint32(&mut b1, u);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for u={u}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for u={u}");
        };
        f(0);
        f(1);
        f(u32::MAX);
        f((1 << 31) + 1);
        f((1 << 31) - 1);
        f(1 << 31);

        for i in 0..10_000u32 {
            f(i);
        }
    }

    #[test]
    fn test_marshal_unmarshal_uint64() {
        let f = |u: u64| {
            let mut b = Vec::new();
            marshal_uint64(&mut b, u);
            assert_eq!(
                b.len(),
                8,
                "unexpected b length: {}; expecting {}",
                b.len(),
                8
            );
            let u_new = unmarshal_uint64(&b);
            assert_eq!(
                u_new, u,
                "unexpected uNew from b={b:x?}; got {u_new}; expecting {u}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_uint64(&mut b1, u);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for u={u}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for u={u}");
        };
        f(0);
        f(1);
        f(u64::MAX);
        f((1 << 63) + 1);
        f((1 << 63) - 1);
        f(1 << 63);

        for i in 0..10_000u64 {
            f(i);
        }
    }

    #[test]
    fn test_marshal_unmarshal_int16() {
        let f = |v: i16| {
            let mut b = Vec::new();
            marshal_int16(&mut b, v);
            assert_eq!(
                b.len(),
                2,
                "unexpected b length: {}; expecting {}",
                b.len(),
                2
            );
            let v_new = unmarshal_int16(&b);
            assert_eq!(
                v_new, v,
                "unexpected vNew from b={b:x?}; got {v_new}; expecting {v}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_int16(&mut b1, v);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for v={v}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for v={v}");
        };
        f(0);
        f(1);
        f(-1);
        f(i16::MIN);
        f(i16::MIN + 1);
        f(i16::MAX);

        for i in 0..10_000i16 {
            f(i);
            f(-i);
        }
    }

    #[test]
    fn test_marshal_unmarshal_int64() {
        let f = |v: i64| {
            let mut b = Vec::new();
            marshal_int64(&mut b, v);
            assert_eq!(
                b.len(),
                8,
                "unexpected b length: {}; expecting {}",
                b.len(),
                8
            );
            let v_new = unmarshal_int64(&b);
            assert_eq!(
                v_new, v,
                "unexpected vNew from b={b:x?}; got {v_new}; expecting {v}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_int64(&mut b1, v);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for v={v}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for v={v}");
        };
        f(0);
        f(1);
        f(-1);
        f(i64::MIN);
        f(i64::MIN + 1);
        f(i64::MAX);

        for i in 0..10_000i64 {
            f(i);
            f(-i);
        }
    }

    fn test_marshal_unmarshal_var_int64_value(v: i64) {
        let mut b = Vec::new();
        marshal_var_int64(&mut b, v);
        let (v_new, n_size) = unmarshal_var_int64(&b);
        assert!(
            n_size > 0,
            "unexpected error when unmarshaling v={v} from b={b:x?}"
        );
        let tail = &b[n_size as usize..];
        assert_eq!(
            v_new, v,
            "unexpected vNew from b={b:x?}; got {v_new}; expecting {v}"
        );
        assert!(
            tail.is_empty(),
            "unexpected data left after unmarshaling v={v} from b={b:x?}: {tail:x?}"
        );

        let prefix: &[u8] = &[1, 2, 3];
        let mut b1 = prefix.to_vec();
        marshal_var_int64(&mut b1, v);
        assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for v={v}");
        assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for v={v}");
    }

    #[test]
    fn test_marshal_unmarshal_var_int64() {
        let f = test_marshal_unmarshal_var_int64_value;
        f(0);
        f(1);
        f(-1);
        f((1 << 6) - 1);
        f((-1 << 6) + 1);
        f(1 << 6);
        f(-1 << 6);
        f((1 << 13) - 1);
        f((-1 << 13) + 1);
        f(1 << 13);
        f((1 << 13) + 1);
        f(-1 << 13);
        f(i64::MIN);
        f(i64::MIN + 1);
        f(i64::MAX);

        for i in 0..10_000i64 {
            f(i);
            f(-i);
            f(i.wrapping_shl(8));
            f((-i).wrapping_shl(8));
            f(i.wrapping_shl(16));
            f((-i).wrapping_shl(16));
            f(i.wrapping_shl(23));
            f((-i).wrapping_shl(23));
            f(i.wrapping_shl(33));
            f((-i).wrapping_shl(33));
            f(i.wrapping_shl(35));
            f((-i).wrapping_shl(35));
            f(i.wrapping_shl(43));
            f((-i).wrapping_shl(43));
            f(i.wrapping_shl(53));
            f((-i).wrapping_shl(53));
        }
    }

    #[test]
    fn test_marshal_unmarshal_var_uint64() {
        let f = |u: u64| {
            let mut b = Vec::new();
            marshal_var_uint64(&mut b, u);
            let (u_new, n_size) = unmarshal_var_uint64(&b);
            assert!(
                n_size > 0,
                "unexpected error when unmarshaling u={u} from b={b:x?}"
            );
            let tail = &b[n_size as usize..];
            assert_eq!(
                u_new, u,
                "unexpected uNew from b={b:x?}; got {u_new}; expecting {u}"
            );
            assert!(
                tail.is_empty(),
                "unexpected data left after unmarshaling u={u} from b={b:x?}: {tail:x?}"
            );

            let prefix: &[u8] = &[1, 2, 3];
            let mut b1 = prefix.to_vec();
            marshal_var_uint64(&mut b1, u);
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for u={u}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for u={u}");
        };
        f(0);
        f(1);
        test_marshal_unmarshal_var_int64_value((1 << 6) - 1);
        test_marshal_unmarshal_var_int64_value(1 << 6);
        test_marshal_unmarshal_var_int64_value((1 << 13) - 1);
        test_marshal_unmarshal_var_int64_value(1 << 13);
        test_marshal_unmarshal_var_int64_value((1 << 13) + 1);
        f((1 << 63) - 1);

        for i in 0..1024u64 {
            f(i);
            f(i << 8);
            f(i << 16);
            f(i << 23);
            f(i << 33);
            f(i << 35);
            f(i << 41);
            f(i << 49);
            f(i << 54);
        }
    }

    #[test]
    fn test_unmarshal_bytes_overflow() {
        let poison_varint: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        let (result, n_size) = unmarshal_bytes(poison_varint);
        assert!(
            n_size <= 0 && result.is_none(),
            "expected error from overflow input, got nSize={n_size} result={result:x?}"
        );
    }

    #[test]
    fn test_marshal_unmarshal_bytes() {
        let f = |s: &str| {
            let mut b = Vec::new();
            marshal_bytes(&mut b, s.as_bytes());
            let (b_new, n_size) = unmarshal_bytes(&b);
            assert!(
                n_size > 0,
                "unexpected error when unmarshaling s={s:?} from b={b:x?}"
            );
            let tail = &b[n_size as usize..];
            assert_eq!(
                b_new.unwrap(),
                s.as_bytes(),
                "unexpected sNew from b={b:x?}; expecting {s:?}"
            );
            assert!(
                tail.is_empty(),
                "unexpected data left after unmarshaling s={s:?} from b={b:x?}: {tail:x?}"
            );

            let prefix = b"abcde";
            let mut b1 = prefix.to_vec();
            marshal_bytes(&mut b1, s.as_bytes());
            assert_eq!(&b1[..prefix.len()], prefix, "unexpected prefix for s={s:?}");
            assert_eq!(&b1[prefix.len()..], &b[..], "unexpected b for s={s:?}");
        };
        f("");
        f("x");
        f("xy");

        let mut bb = String::new();
        for i in 0..100 {
            bb.push_str(&format!(" {i} "));
            f(&bb.clone());
        }
    }

    //
    // encoding_test.go
    //

    #[test]
    fn test_is_const() {
        let f = |a: &[i64], ok_expected: bool| {
            let ok = is_const(a);
            assert_eq!(
                ok, ok_expected,
                "unexpected isConst for a={a:?}; got {ok}; want {ok_expected}"
            );
        };
        f(&[], false);
        f(&[1], true);
        f(&[1, 2], false);
        f(&[1, 1], true);
        f(&[1, 1, 1], true);
        f(&[1, 1, 2], false);
    }

    #[test]
    fn test_is_delta_const() {
        let f = |a: &[i64], ok_expected: bool| {
            let ok = is_delta_const(a);
            assert_eq!(
                ok, ok_expected,
                "unexpected isDeltaConst for a={a:?}; got {ok}; want {ok_expected}"
            );
        };
        f(&[], false);
        f(&[1], false);
        f(&[1, 2], true);
        f(&[1, 2, 3], true);
        f(&[3, 2, 1], true);
        f(&[3, 2, 1, 0, -1, -2], true);
        f(&[3, 2, 1, 0, -1, -2, 2], false);
        f(&[1, 1], true);
        f(&[1, 2, 1], false);
        f(&[1, 2, 4], false);
    }

    #[test]
    fn test_is_gauge() {
        let f = |a: &[i64], ok_expected: bool| {
            let ok = is_gauge(a);
            assert_eq!(
                ok, ok_expected,
                "unexpected result for isGauge({a:?}); got {ok}; expecting {ok_expected}"
            );
        };
        f(&[], false);
        f(&[0], false);
        f(&[1, 2], false);
        f(&[0, 1, 2, 3, 4, 5], false);
        f(&[0, -1, -2, -3, -4], true);
        f(&[0, 0, 0, 0, 0, 0, 0], false);
        f(&[1, 1, 1, 1, 1], false);
        f(&[1, 1, 2, 2, 2, 2], false);
        f(&[1, 17, 2, 3], false); // a single counter reset
        f(&[1, 5, 2, 3], true);
        f(&[1, 5, 2, 3, 2], true);
        f(&[-1, -5, -2, -3], true);
        f(&[-1, -5, -2, -3, -2], true);
        f(&[5, 6, 4, 3, 2], true);
        f(&[4, 5, 6, 5, 4, 3, 2], true);
        f(&[1064, 1132, 1083, 1062, 856, 747], true);
    }

    #[test]
    fn test_ensure_non_decreasing_sequence() {
        let f = |a: &[i64], v_min: i64, v_max: i64, a_expected: &[i64]| {
            let mut a = a.to_vec();
            ensure_non_decreasing_sequence(&mut a, v_min, v_max);
            assert_eq!(
                a, a_expected,
                "unexpected a; got\n{a:?}; expecting\n{a_expected:?}"
            );
        };
        f(&[], -1234, -34, &[]);
        f(&[123], -1234, -1234, &[-1234]);
        f(&[123], -1234, 345, &[345]);
        f(&[-23, -14], -23, -14, &[-23, -14]);
        f(&[-23, -14], -25, 0, &[-25, 0]);
        f(&[0, -1, 10, 5, 6, 7], 2, 8, &[2, 2, 8, 8, 8, 8]);
        f(&[0, -1, 10, 5, 6, 7], -2, 8, &[-2, -1, 8, 8, 8, 8]);
        f(&[0, -1, 10, 5, 6, 7], -2, 12, &[-2, -1, 10, 10, 10, 12]);
        f(&[1, 2, 1, 3, 4, 5], 1, 5, &[1, 2, 2, 3, 4, 5]);
    }

    fn test_marshal_unmarshal_int64_array_impl(
        va: &[i64],
        precision_bits: u8,
        mt_expected: MarshalType,
    ) {
        let mut b = Vec::new();
        let (mt, first_value) = marshal_int64_array(&mut b, va, precision_bits);
        assert_eq!(
            mt, mt_expected,
            "unexpected MarshalType for va={va:?}, precisionBits={precision_bits}: got {}; expecting {}",
            mt.0, mt_expected.0
        );
        let mut va_new = Vec::new();
        if let Err(err) = unmarshal_int64_array(&mut va_new, &b, mt, first_value, va.len()) {
            panic!(
                "unexpected error when unmarshaling va={va:?}, precisionBits={precision_bits}: {err}"
            );
        }
        match mt {
            MarshalType::ZSTD_NEAREST_DELTA
            | MarshalType::ZSTD_NEAREST_DELTA2
            | MarshalType::NEAREST_DELTA
            | MarshalType::NEAREST_DELTA2 => {
                if let Err(err) = check_precision_bits_test(va, &va_new, precision_bits) {
                    panic!("too low precision for vaNew: {err}");
                }
            }
            _ => {
                assert_eq!(
                    va,
                    &va_new[..],
                    "unexpected vaNew for va={va:?}, precisionBits={precision_bits}"
                );
            }
        }

        let b_prefix: &[u8] = &[1, 2, 3];
        let mut b_new = b_prefix.to_vec();
        let (mt_new, first_value_new) = marshal_int64_array(&mut b_new, va, precision_bits);
        assert_eq!(
            first_value_new, first_value,
            "unexpected firstValue for prefixed va={va:?}, precisionBits={precision_bits}; got {first_value_new}; want {first_value}"
        );
        assert_eq!(
            &b_new[..b_prefix.len()],
            b_prefix,
            "unexpected prefix for va={va:?}, precisionBits={precision_bits}"
        );
        assert_eq!(
            &b_new[b_prefix.len()..],
            &b[..],
            "unexpected b for prefixed va={va:?}, precisionBits={precision_bits}"
        );
        assert_eq!(
            mt_new, mt,
            "unexpected mt for prefixed va={va:?}, precisionBits={precision_bits}; got {}; expecting {}",
            mt_new.0, mt.0
        );

        let va_prefix: &[i64] = &[4, 5, 6, 8];
        let mut va_new = va_prefix.to_vec();
        if let Err(err) = unmarshal_int64_array(&mut va_new, &b, mt, first_value, va.len()) {
            panic!(
                "unexpected error when unmarshaling prefixed va={va:?}, precisionBits={precision_bits}: {err}"
            );
        }
        assert_eq!(
            &va_new[..va_prefix.len()],
            va_prefix,
            "unexpected prefix for va={va:?}, precisionBits={precision_bits}"
        );
        match mt {
            MarshalType::ZSTD_NEAREST_DELTA
            | MarshalType::ZSTD_NEAREST_DELTA2
            | MarshalType::NEAREST_DELTA
            | MarshalType::NEAREST_DELTA2 => {
                if let Err(err) =
                    check_precision_bits_test(&va_new[va_prefix.len()..], va, precision_bits)
                {
                    panic!("too low precision for prefixed vaNew: {err}");
                }
            }
            _ => {
                assert_eq!(
                    &va_new[va_prefix.len()..],
                    va,
                    "unexpected prefixed vaNew for va={va:?}, precisionBits={precision_bits}"
                );
            }
        }
    }

    #[test]
    fn test_marshal_unmarshal_timestamps() {
        let mut r = GoRand::new(1);
        const PRECISION_BITS: u8 = 3;

        let mut timestamps = Vec::new();
        let mut v = 0i64;
        for _ in 0..8 * 1024 {
            v += 30_000 * (r.norm_float64() * 5e2) as i64;
            timestamps.push(v);
        }
        let mut result = Vec::new();
        let (mt, first_timestamp) = marshal_timestamps(&mut result, &timestamps, PRECISION_BITS);
        let mut timestamps2 = Vec::new();
        if let Err(err) = unmarshal_timestamps(
            &mut timestamps2,
            &result,
            mt,
            first_timestamp,
            timestamps.len(),
        ) {
            panic!("cannot unmarshal timestamps: {err}");
        }
        if let Err(err) = check_precision_bits_test(&timestamps, &timestamps2, PRECISION_BITS) {
            panic!("too low precision for timestamps: {err}");
        }
    }

    #[test]
    fn test_marshal_unmarshal_values() {
        let mut r = GoRand::new(1);
        const PRECISION_BITS: u8 = 3;

        let mut values = Vec::new();
        let mut v = 0i64;
        for _ in 0..8 * 1024 {
            v += (r.norm_float64() * 1e2) as i64;
            values.push(v);
        }
        let mut result = Vec::new();
        let (mt, first_value) = marshal_values(&mut result, &values, PRECISION_BITS);
        let mut values2 = Vec::new();
        if let Err(err) = unmarshal_values(&mut values2, &result, mt, first_value, values.len()) {
            panic!("cannot unmarshal values: {err}");
        }
        if let Err(err) = check_precision_bits_test(&values, &values2, PRECISION_BITS) {
            panic!("too low precision for values: {err}");
        }
    }

    #[test]
    fn test_marshal_unmarshal_int64_array_generic() {
        let f = test_marshal_unmarshal_int64_array_impl;
        f(&[1, 20, 234], 4, MarshalType::NEAREST_DELTA2);
        f(&[1, 20, -2345, 678934, 342], 4, MarshalType::NEAREST_DELTA);
        f(&[1, 20, 2345, 6789, 12342], 4, MarshalType::NEAREST_DELTA2);

        // Constant encoding
        f(&[1], 4, MarshalType::CONST);
        f(&[1, 2], 4, MarshalType::DELTA_CONST);
        f(&[-1, 0, 1, 2, 3, 4, 5], 4, MarshalType::DELTA_CONST);
        f(&[-10, -1, 8, 17, 26], 4, MarshalType::DELTA_CONST);
        f(&[0, 0, 0, 0, 0, 0], 4, MarshalType::CONST);
        f(&[100, 100, 100, 100], 4, MarshalType::CONST);
    }

    //
    // encoding_cgo_test.go (this port wraps libzstd, matching the cgo build)
    //

    // PORT NOTE: the Go test drives this with math/rand data; the substitute
    // generator produces a different (same-distribution) random walk, so the
    // precisionBits boundary between the ZSTD* and plain types can shift by a
    // couple of bits. The ranges below leave a small gap around the Go cgo
    // boundary (22/23 for delta, 23/24 for delta2), the same way the Go pure
    // test leaves a gap for codec variance.
    #[test]
    fn test_marshal_unmarshal_int64_array() {
        let mut r = GoRand::new(1);

        // Verify nearest delta encoding.
        let mut va = Vec::new();
        let mut v = 0i64;
        for _ in 0..8 * 1024 {
            v += (r.norm_float64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1u8..21 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::ZSTD_NEAREST_DELTA,
            );
        }
        for precision_bits in 24u8..65 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::NEAREST_DELTA,
            );
        }

        // Verify nearest delta2 encoding.
        va.clear();
        v = 0;
        for _ in 0..8 * 1024 {
            v += 30_000_000 + (r.norm_float64() * 1e6) as i64;
            va.push(v);
        }
        for precision_bits in 1u8..22 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::ZSTD_NEAREST_DELTA2,
            );
        }
        for precision_bits in 25u8..65 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::NEAREST_DELTA2,
            );
        }

        // Verify nearest delta encoding.
        va.clear();
        v = 1000;
        for _ in 0..6 {
            v += (r.norm_float64() * 100.0) as i64;
            va.push(v);
        }
        assert!(
            is_gauge(&va),
            "the 6-item test array must be a gauge; got {va:?}"
        );
        for precision_bits in 1u8..65 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::NEAREST_DELTA,
            );
        }

        // Verify nearest delta2 encoding.
        va.clear();
        v = 0;
        for _ in 0..6 {
            v += 3000 + (r.norm_float64() * 100.0) as i64;
            va.push(v);
        }
        for precision_bits in 5u8..65 {
            test_marshal_unmarshal_int64_array_impl(
                &va,
                precision_bits,
                MarshalType::NEAREST_DELTA2,
            );
        }
    }

    fn test_marshal_int64_array_size_impl(
        va: &[i64],
        precision_bits: u8,
        min_size_expected: usize,
        max_size_expected: usize,
    ) {
        let mut b = Vec::new();
        marshal_int64_array(&mut b, va, precision_bits);
        assert!(
            b.len() <= max_size_expected,
            "too big size for marshaled {} items with precisionBits {}: got {}; expecting {}",
            va.len(),
            precision_bits,
            b.len(),
            max_size_expected
        );
        assert!(
            b.len() >= min_size_expected,
            "too small size for marshaled {} items with precisionBits {}: got {}; expecting {}",
            va.len(),
            precision_bits,
            b.len(),
            min_size_expected
        );
    }

    // PORT NOTE: the minimum bounds are the Go cgo (libzstd) expectations.
    // The maximum bounds are widened by ~15% over the sizes measured with the
    // substitute random generator (a different same-distribution walk than
    // Go's math/rand), leaving headroom for libzstd version changes.
    #[test]
    fn test_marshal_int64_array_size() {
        let mut r = GoRand::new(1);

        let mut va = Vec::new();
        let mut v = (r.float64() * 1e9) as i64;
        for _ in 0..8 * 1024 {
            va.push(v);
            v += 30_000 + (r.norm_float64() * 1e3) as i64;
        }

        test_marshal_int64_array_size_impl(&va, 1, 180, 2000);
        test_marshal_int64_array_size_impl(&va, 2, 250, 1600);
        test_marshal_int64_array_size_impl(&va, 3, 600, 2000);
        test_marshal_int64_array_size_impl(&va, 4, 1300, 2300);
        test_marshal_int64_array_size_impl(&va, 5, 2000, 3800);
        test_marshal_int64_array_size_impl(&va, 6, 3000, 5800);
        test_marshal_int64_array_size_impl(&va, 7, 4000, 7600);
        test_marshal_int64_array_size_impl(&va, 8, 6000, 9200);
        test_marshal_int64_array_size_impl(&va, 9, 7000, 10000);
        test_marshal_int64_array_size_impl(&va, 10, 8000, 11500);
    }

    //
    // nearest_delta_test.go
    //

    #[test]
    fn test_marshal_int64_nearest_delta() {
        let f = |va: &[i64], precision_bits: u8, first_value_expected: i64, b_expected: &str| {
            let mut b = Vec::new();
            let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
            assert_eq!(
                first_value, first_value_expected,
                "unexpected firstValue for va={va:?}, precisionBits={precision_bits}; got {first_value}; want {first_value_expected}"
            );
            assert_eq!(
                hex_lower(&b),
                b_expected,
                "invalid marshaled data for va={va:?}, precisionBits={precision_bits}"
            );

            let prefix = b"foobar";
            let mut b = prefix.to_vec();
            let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
            assert_eq!(
                first_value, first_value_expected,
                "unexpected firstValue for va={va:?}, precisionBits={precision_bits}; got {first_value}; want {first_value_expected}"
            );
            assert_eq!(
                &b[..prefix.len()],
                prefix,
                "invalid prefix for va={va:?}, precisionBits={precision_bits}"
            );
            assert_eq!(
                hex_lower(&b[prefix.len()..]),
                b_expected,
                "invalid marshaled prefixed data for va={va:?}, precisionBits={precision_bits}"
            );
        };
        f(&[0], 4, 0, "");
        f(&[0, 0], 4, 0, "00");
        f(&[1, -3], 4, 1, "07");
        f(&[255, 255], 4, 255, "00");
        f(&[0, 1, 2, 3, 4, 5], 4, 0, "0202020202");
        f(&[5, 4, 3, 2, 1, 0], 1, 5, "0003000301");
        f(&[5, 4, 3, 2, 1, 0], 2, 5, "0003010101");
        f(&[5, 4, 3, 2, 1, 0], 3, 5, "0101010101");
        f(&[5, 4, 3, 2, 1, 0], 4, 5, "0101010101");

        f(&[-500, -600, -700, -800, -890], 1, -500, "00000000");
        f(&[-500, -600, -700, -800, -890], 2, -500, "0000ff0300");
        f(&[-500, -600, -700, -800, -890], 3, -500, "00ff01ff01ff01");
        f(&[-500, -600, -700, -800, -890], 4, -500, "7fff017fff01");
        f(&[-500, -600, -700, -800, -890], 5, -500, "bf01bf01bf01bf01");
        f(&[-500, -600, -700, -800, -890], 6, -500, "bf01bf01bf01bf01");
        f(&[-500, -600, -700, -800, -890], 7, -500, "bf01cf01bf01af01");
        f(&[-500, -600, -700, -800, -890], 8, -500, "c701c701c701af01");
    }

    fn test_marshal_unmarshal_int64_nearest_delta_impl(va: &[i64], precision_bits: u8) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta(&mut b, va, precision_bits);
        let mut va_new = Vec::new();
        if let Err(err) = unmarshal_int64_nearest_delta(&mut va_new, &b, first_value, va.len()) {
            panic!(
                "cannot unmarshal data for va={va:?}, precisionBits={precision_bits} from b={b:x?}: {err}"
            );
        }
        if let Err(err) = check_precision_bits_test(&va_new, va, precision_bits) {
            panic!(
                "too small precisionBits for va={va:?}, precisionBits={precision_bits}: {err}, vaNew=\n{va_new:?}"
            );
        }

        let va_prefix: &[i64] = &[1, 2, 3, 4];
        let mut va_new = va_prefix.to_vec();
        if let Err(err) = unmarshal_int64_nearest_delta(&mut va_new, &b, first_value, va.len()) {
            panic!(
                "cannot unmarshal prefixed data for va={va:?}, precisionBits={precision_bits} from b={b:x?}: {err}"
            );
        }
        assert_eq!(
            &va_new[..va_prefix.len()],
            va_prefix,
            "unexpected prefix for va={va:?}, precisionBits={precision_bits}"
        );
        if let Err(err) = check_precision_bits_test(&va_new[va_prefix.len()..], va, precision_bits)
        {
            panic!(
                "too small precisionBits for prefixed va={va:?}, precisionBits={precision_bits}: {err}, vaNew=\n{:?}",
                &va_new[va_prefix.len()..]
            );
        }
    }

    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta() {
        let f = test_marshal_unmarshal_int64_nearest_delta_impl;
        let mut r = GoRand::new(1);

        f(&[0], 4);
        f(&[0, 0], 4);
        f(&[1, -3], 4);
        f(&[255, 255], 4);
        f(&[0, 1, 2, 3, 4, 5], 4);
        f(&[5, 4, 3, 2, 1, 0], 4);
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            1,
        );
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            2,
        );
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            3,
        );
        f(
            &[
                -5_000_000_000_000,
                -5_600_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            4,
        );

        // Verify constant encoding.
        let mut va = vec![9_876_543_210_123i64; 1024];
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for monotonically incremented va.
        let mut v = -35i64;
        va.clear();
        for _ in 0..1024 {
            v += 8;
            va.push(v);
        }
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for monotonically decremented va.
        v = 793;
        va.clear();
        for _ in 0..1024 {
            v -= 16;
            va.push(v);
        }
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for quadratically incremented va.
        v = -1_234_567;
        va.clear();
        for i in 0..1024i64 {
            v += 32 + i;
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for decremented va with norm-float noise.
        v = 787_933;
        va.clear();
        for _ in 0..1024 {
            v -= 25 + (r.norm_float64() * 2.0) as i64;
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for incremented va with random noise.
        v = 943_854;
        va.clear();
        for _ in 0..1024 {
            v += 30 + r.int63n(5);
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for constant va with norm-float noise.
        v = -12_345;
        va.clear();
        for _ in 0..1024 {
            v += (r.norm_float64() * 10.0) as i64;
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for constant va with random noise.
        v = -12_345;
        va.clear();
        for _ in 0..1024 {
            v += r.int63n(15) - 1;
            va.push(v);
        }
        f(&va, 4);
    }

    //
    // nearest_delta2_test.go
    //

    #[test]
    fn test_nearest_delta() {
        let f = |next: i64,
                 prev: i64,
                 precision_bits: u8,
                 d_expected: i64,
                 trailing_zero_bits_expected: u8| {
            let tz = get_trailing_zeros(prev, precision_bits);
            let (d, trailing_zero_bits) = nearest_delta(next, prev, precision_bits, tz);
            assert_eq!(
                d, d_expected,
                "unexpected d for next={next}, prev={prev}, precisionBits={precision_bits}; got {d}; expecting {d_expected}"
            );
            assert_eq!(
                trailing_zero_bits, trailing_zero_bits_expected,
                "unexpected trailingZeroBits for next={next}, prev={prev}, precisionBits={precision_bits}; got {trailing_zero_bits}; expecting {trailing_zero_bits_expected}"
            );
        };

        f(0, 0, 1, 0, 0);
        f(0, 0, 2, 0, 0);
        f(0, 0, 3, 0, 0);
        f(0, 0, 4, 0, 0);

        f(100, 100, 4, 0, 2);
        f(123456, 123456, 4, 0, 12);
        f(-123456, -123456, 4, 0, 12);
        f(9876543210, 9876543210, 4, 0, 29);

        f(1, 2, 3, -1, 0);
        f(2, 1, 3, 1, 0);
        f(-1, -2, 3, 1, 0);
        f(-2, -1, 3, -1, 0);

        f(0, 1, 1, -1, 0);
        f(1, 2, 1, -1, 0);
        f(2, 3, 1, 0, 1);
        f(1, 0, 1, 1, 0);
        f(2, 1, 1, 0, 1);
        f(2, 1, 2, 1, 0);
        f(2, 1, 3, 1, 0);

        f(0, -1, 1, 1, 0);
        f(-1, -2, 1, 1, 0);
        f(-2, -3, 1, 0, 1);
        f(-1, 0, 1, -1, 0);
        f(-2, -1, 1, 0, 1);
        f(-2, -1, 2, -1, 0);
        f(-2, -1, 3, -1, 0);

        f(0, 2, 3, -2, 0);
        f(3, 0, 3, 3, 0);
        f(4, 0, 3, 4, 0);
        f(5, 0, 3, 5, 0);
        f(6, 0, 3, 6, 0);
        f(0, 7, 3, -7, 0);
        f(8, 0, 3, 8, 1);
        f(9, 0, 3, 8, 1);
        f(15, 0, 3, 14, 1);
        f(16, 0, 3, 16, 2);
        f(17, 0, 3, 16, 2);
        f(18, 0, 3, 16, 2);
        f(0, 59, 6, -59, 0);

        f(128, 121, 1, 0, 7);
        f(128, 121, 2, 0, 6);
        f(128, 121, 3, 0, 5);
        f(128, 121, 4, 0, 4);
        f(128, 121, 5, 0, 3);
        f(128, 121, 6, 4, 2);
        f(128, 121, 7, 6, 1);
        f(128, 121, 8, 7, 0);

        f(32, 37, 1, 0, 5);
        f(32, 37, 2, 0, 4);
        f(32, 37, 3, 0, 3);
        f(32, 37, 4, -4, 2);
        f(32, 37, 5, -4, 1);
        f(32, 37, 6, -5, 0);

        f(-10, 20, 1, -24, 3);
        f(-10, 20, 2, -28, 2);
        f(-10, 20, 3, -30, 1);
        f(-10, 20, 4, -30, 0);
        f(-10, 21, 4, -31, 0);
        f(-10, 21, 5, -31, 0);

        f(10, -20, 1, 24, 3);
        f(10, -20, 2, 28, 2);
        f(10, -20, 3, 30, 1);
        f(10, -20, 4, 30, 0);
        f(10, -21, 4, 31, 0);
        f(10, -21, 5, 31, 0);

        f(1_234_000_000_000_000, 1_235_000_000_000_000, 1, 0, 50);
        f(1_234_000_000_000_000, 1_235_000_000_000_000, 10, 0, 41);
        f(
            1_234_000_000_000_000,
            1_235_000_000_000_000,
            35,
            -999_999_995_904,
            16,
        );

        f(i64::MAX, 0, 1, i64::MAX, 2);
    }

    #[test]
    fn test_marshal_int64_nearest_delta2() {
        let f = |va: &[i64], precision_bits: u8, first_value_expected: i64, b_expected: &str| {
            let mut b = Vec::new();
            let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
            assert_eq!(
                first_value, first_value_expected,
                "unexpected firstValue for va={va:?}, precisionBits={precision_bits}; got {first_value}; want {first_value_expected}"
            );
            assert_eq!(
                hex_lower(&b),
                b_expected,
                "invalid marshaled data for va={va:?}, precisionBits={precision_bits}"
            );

            let prefix = b"foobar";
            let mut b = prefix.to_vec();
            let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
            assert_eq!(
                first_value, first_value_expected,
                "unexpected firstValue for va={va:?}, precisionBits={precision_bits}; got {first_value}; want {first_value_expected}"
            );
            assert_eq!(
                &b[..prefix.len()],
                prefix,
                "invalid prefix for va={va:?}, precisionBits={precision_bits}"
            );
            assert_eq!(
                hex_lower(&b[prefix.len()..]),
                b_expected,
                "invalid marshaled prefixed data for va={va:?}, precisionBits={precision_bits}"
            );
        };
        f(&[0, 0], 4, 0, "00");
        f(&[1, -3], 4, 1, "07");
        f(&[255, 255], 4, 255, "00");
        f(&[0, 1, 2, 3, 4, 5], 4, 0, "0200000000");
        f(&[5, 4, 3, 2, 1, 0], 4, 5, "0100000000");

        f(&[-5000, -6000, -7000, -8000, -8900], 1, -5000, "cf0f000000");
        f(&[-5000, -6000, -7000, -8000, -8900], 2, -5000, "cf0f000000");
        f(&[-5000, -6000, -7000, -8000, -8900], 3, -5000, "cf0f000000");
        f(
            &[-5000, -6000, -7000, -8000, -8900],
            4,
            -5000,
            "cf0f00008001",
        );
        f(
            &[-5000, -6000, -7000, -8000, -8900],
            5,
            -5000,
            "cf0f0000c001",
        );
        f(
            &[-5000, -6000, -7000, -8000, -8900],
            6,
            -5000,
            "cf0f0000c001",
        );
        f(
            &[-5000, -6000, -7000, -8000, -8900],
            7,
            -5000,
            "cf0f0000c001",
        );
        f(
            &[-5000, -6000, -7000, -8000, -8900],
            8,
            -5000,
            "cf0f0000c801",
        );
    }

    fn test_marshal_unmarshal_int64_nearest_delta2_impl(va: &[i64], precision_bits: u8) {
        let mut b = Vec::new();
        let first_value = marshal_int64_nearest_delta2(&mut b, va, precision_bits);
        let mut va_new = Vec::new();
        if let Err(err) = unmarshal_int64_nearest_delta2(&mut va_new, &b, first_value, va.len()) {
            panic!(
                "cannot unmarshal data for va={va:?}, precisionBits={precision_bits} from b={b:x?}: {err}"
            );
        }
        if let Err(err) = check_precision_bits_test(&va_new, va, precision_bits) {
            panic!(
                "too small precisionBits for va={va:?}, precisionBits={precision_bits}: {err}, vaNew=\n{va_new:?}"
            );
        }

        let va_prefix: &[i64] = &[1, 2, 3, 4];
        let mut va_new = va_prefix.to_vec();
        if let Err(err) = unmarshal_int64_nearest_delta2(&mut va_new, &b, first_value, va.len()) {
            panic!(
                "cannot unmarshal prefixed data for va={va:?}, precisionBits={precision_bits} from b={b:x?}: {err}"
            );
        }
        assert_eq!(
            &va_new[..va_prefix.len()],
            va_prefix,
            "unexpected prefix for va={va:?}, precisionBits={precision_bits}"
        );
        if let Err(err) = check_precision_bits_test(&va_new[va_prefix.len()..], va, precision_bits)
        {
            panic!(
                "too small precisionBits for prefixed va={va:?}, precisionBits={precision_bits}: {err}, vaNew=\n{:?}",
                &va_new[va_prefix.len()..]
            );
        }
    }

    #[test]
    fn test_marshal_unmarshal_int64_nearest_delta2() {
        let f = test_marshal_unmarshal_int64_nearest_delta2_impl;
        let mut r = GoRand::new(1);

        f(&[0, 0], 4);
        f(&[1, -3], 4);
        f(&[255, 255], 4);
        f(&[0, 1, 2, 3, 4, 5], 4);
        f(&[5, 4, 3, 2, 1, 0], 4);
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            1,
        );
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            2,
        );
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            3,
        );
        f(
            &[
                -5_000_000_000_000,
                -6_000_000_000_000,
                -7_000_000_000_000,
                -8_000_000_000_000,
                -8_900_000_000_000,
            ],
            4,
        );

        // Verify constant encoding.
        let mut va = vec![9_876_543_210_123i64; 1024];
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for monotonically incremented va.
        let mut v = -35i64;
        va.clear();
        for _ in 0..1024 {
            v += 8;
            va.push(v);
        }
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for monotonically decremented va.
        v = 793;
        va.clear();
        for _ in 0..1024 {
            v -= 16;
            va.push(v);
        }
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for quadratically incremented va.
        v = -1_234_567;
        va.clear();
        for i in 0..1024i64 {
            v += 32 + i;
            va.push(v);
        }
        f(&va, 4);
        f(&va, 63);

        // Verify encoding for decremented va with norm-float noise.
        v = 787_933;
        va.clear();
        for _ in 0..1024 {
            v -= 25 + (r.norm_float64() * 2.0) as i64;
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for incremented va with random noise.
        v = 943_854;
        va.clear();
        for _ in 0..1024 {
            v += 30 + r.int63n(5);
            va.push(v);
        }
        f(&va, 4);

        // Verify encoding for constant va with norm-float noise.
        v = -12_345;
        va.clear();
        for _ in 0..1024 {
            v += (r.norm_float64() * 10.0) as i64;
            va.push(v);
        }
        f(&va, 2);

        // Verify encoding for constant va with random noise.
        v = -12_345;
        va.clear();
        for _ in 0..1024 {
            v += r.int63n(15) - 1;
            va.push(v);
        }
        f(&va, 3);
    }

    fn check_precision_bits_test(a: &[i64], b: &[i64], precision_bits: u8) -> Result<(), String> {
        if a.len() != b.len() {
            return Err(format!(
                "different-sized arrays: {} vs {}",
                a.len(),
                b.len()
            ));
        }
        for (i, (&av0, &bv0)) in a.iter().zip(b.iter()).enumerate() {
            let (mut av, bv) = if av0 < bv0 { (bv0, av0) } else { (av0, bv0) };
            let eps = av.wrapping_sub(bv);
            if eps == 0 {
                continue;
            }
            if av < 0 {
                av = av.wrapping_neg();
            }
            let mut pbe = 1u8;
            while eps < av {
                av >>= 1;
                pbe += 1;
            }
            if pbe < precision_bits {
                return Err(format!(
                    "too low precisionBits for\na={a:?}\nb={b:?}\ngot {pbe}; expecting {precision_bits}; compared values: {} vs {}, eps={eps}",
                    a[i], b[i]
                ));
            }
        }
        Ok(())
    }

    //
    // compress_test.go
    //

    #[test]
    fn test_compress_decompress_zstd() {
        test_compress_decompress_zstd_data(b"a");
        test_compress_decompress_zstd_data(b"foobarbaz");

        let mut r = GoRand::new(1);
        let mut b = Vec::new();
        for _ in 0..64 * 1024 {
            b.push(r.int63n(256) as u8);
        }
        test_compress_decompress_zstd_data(&b);
    }

    fn test_compress_decompress_zstd_data(b: &[u8]) {
        let mut bc = Vec::new();
        compress_zstd_level(&mut bc, b, 5);
        let mut b_new = Vec::new();
        if let Err(err) = decompress_zstd(&mut b_new, &bc) {
            panic!("unexpected error when decompressing b={b:x?} from bc={bc:x?}: {err}");
        }
        assert_eq!(b_new, b, "invalid bNew; got\n{b_new:x?}; expecting\n{b:x?}");

        let prefix: &[u8] = &[1, 2, 33];
        let mut bc_new = prefix.to_vec();
        compress_zstd_level(&mut bc_new, b, 5);
        assert_eq!(
            &bc_new[..prefix.len()],
            prefix,
            "invalid prefix for b={b:x?}"
        );
        assert_eq!(
            &bc_new[prefix.len()..],
            &bc[..],
            "invalid prefixed bcNew for b={b:x?}"
        );

        let mut b_new = prefix.to_vec();
        if let Err(err) = decompress_zstd(&mut b_new, &bc) {
            panic!(
                "unexpected error when decompressing b={b:x?} from bc={bc:x?} with prefix: {err}"
            );
        }
        assert_eq!(
            &b_new[..prefix.len()],
            prefix,
            "invalid bNew prefix when decompressing bc={bc:x?}"
        );
        assert_eq!(
            &b_new[prefix.len()..],
            b,
            "invalid prefixed bNew; expecting\n{b:x?}"
        );
    }

    //
    // util_test.go
    //

    #[test]
    fn test_is_zstd() {
        // nil / empty
        assert!(
            !is_zstd(&[]),
            "unexpected IsZstd result; got true; expecting false"
        );

        // less than 4 bytes
        assert!(
            !is_zstd(b"foo"),
            "unexpected IsZstd result; got true; expecting false"
        );

        // plain text
        assert!(
            !is_zstd(b"foobar"),
            "unexpected IsZstd result; got true; expecting false"
        );

        // snappy compressed
        // PORT NOTE: the Go test calls snappy.Encode(nil, []byte("foobar"));
        // there is no snappy dependency here, so the equivalent deterministic
        // snappy block-format bytes are hardcoded.
        let snappy_foobar: &[u8] = &[0x06, 0x14, b'f', b'o', b'o', b'b', b'a', b'r'];
        assert!(
            !is_zstd(snappy_foobar),
            "unexpected IsZstd result; got true; expecting false"
        );

        // zstd minimum compressed level
        let mut b = Vec::new();
        compress_zstd_level(&mut b, b"foobar", -22);
        assert!(
            is_zstd(&b),
            "unexpected IsZstd result; got false; expecting true"
        );

        // zstd maximum compressed level
        let mut b = Vec::new();
        compress_zstd_level(&mut b, b"foobar", 22);
        assert!(
            is_zstd(&b),
            "unexpected IsZstd result; got false; expecting true"
        );
    }
}
