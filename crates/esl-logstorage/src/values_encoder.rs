//! Port of EsLogs `lib/logstorage/values_encoder.go`.
//!
//! This is THE on-disk column value format: every marshaled byte produced
//! here must be identical to the bytes produced by the Go implementation.
//!
//! PORT NOTE: Go represents both raw log values and encoded (binary) values
//! as `string`. The port uses `&str`/`String` for human-readable log values
//! and `&[u8]`/`Vec<u8>` for encoded values, since encoded values are not
//! valid UTF-8 in general.

use std::fmt;
use std::io::Write;
use std::sync::Mutex;

use esl_common::{encoding, panicf, timeutil};

use crate::consts::{MAX_DICT_LEN, MAX_DICT_SIZE_BYTES};

/// ValueType is the type of values stored in every column block.
///
/// PORT NOTE: a newtype over `u8` instead of an enum, since the type byte is
/// read from disk and may contain arbitrary values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ValueType(pub u8);

impl ValueType {
    /// Used for determining whether the value type is unknown.
    pub const UNKNOWN: ValueType = ValueType(0);

    /// Default encoding for column blocks. Strings are stored as is.
    pub const STRING: ValueType = ValueType(1);

    /// Column blocks with small number of unique values are encoded as dict.
    pub const DICT: ValueType = ValueType(2);

    /// Uint values up to 2^8-1 are encoded into `ValueType::UINT8`.
    /// Every value occupies a single byte.
    pub const UINT8: ValueType = ValueType(3);

    /// Uint values up to 2^16-1 are encoded into `ValueType::UINT16`.
    /// Every value occupies 2 bytes.
    pub const UINT16: ValueType = ValueType(4);

    /// Uint values up to 2^31-1 are encoded into `ValueType::UINT32`.
    /// Every value occupies 4 bytes.
    pub const UINT32: ValueType = ValueType(5);

    /// Uint values up to 2^64-1 are encoded into `ValueType::UINT64`.
    /// Every value occupies 8 bytes.
    pub const UINT64: ValueType = ValueType(6);

    /// Int values in the range [-(2^63) ... 2^63-1] are encoded into `ValueType::INT64`.
    pub const INT64: ValueType = ValueType(10);

    /// Floating-point values are encoded into `ValueType::FLOAT64`.
    pub const FLOAT64: ValueType = ValueType(7);

    /// Column blocks with ipv4 addresses are encoded as 4-byte strings.
    pub const IPV4: ValueType = ValueType(8);

    /// Column blocks with ISO8601 timestamps are encoded into `ValueType::TIMESTAMP_ISO8601`.
    /// These timestamps are commonly used by Logstash.
    pub const TIMESTAMP_ISO8601: ValueType = ValueType(9);
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ValueType::UNKNOWN => write!(f, "unknown"),
            ValueType::STRING => write!(f, "string"),
            ValueType::DICT => write!(f, "dict"),
            ValueType::UINT8 => write!(f, "uint8"),
            ValueType::UINT16 => write!(f, "uint16"),
            ValueType::UINT32 => write!(f, "uint32"),
            ValueType::UINT64 => write!(f, "uint64"),
            ValueType::INT64 => write!(f, "int64"),
            ValueType::FLOAT64 => write!(f, "float64"),
            ValueType::IPV4 => write!(f, "ipv4"),
            ValueType::TIMESTAMP_ISO8601 => write!(f, "iso8601"),
            _ => write!(f, "unknown valueType={}", self.0),
        }
    }
}

/// ValuesEncoder encodes values into the on-disk representation.
///
/// PORT NOTE: Go keeps `values []string` with unsafe string views into
/// `ve.buf`. The port stores `(start, end)` ranges into `buf` instead and
/// exposes them through [`ValuesEncoder::values`]; this preserves the
/// single-buffer reuse pattern without unsafe self-references. For
/// `ValueType::STRING` the Go code references the caller strings without
/// copying; the port copies them into `buf` (same observable values).
#[derive(Default)]
pub struct ValuesEncoder {
    /// buf contains data for values.
    buf: Vec<u8>,

    /// values contains ranges of the encoded values within buf.
    values: Vec<(usize, usize)>,
}

impl ValuesEncoder {
    /// Resets the encoder.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.values.clear();
    }

    /// Returns the encoded values.
    ///
    /// The returned values are valid until the next `encode()`/`reset()` call.
    pub fn values(&self) -> impl ExactSizeIterator<Item = &[u8]> + '_ {
        self.values
            .iter()
            .map(|&(start, end)| &self.buf[start..end])
    }

    /// Encodes values and returns the encoded value type with min/max encoded values.
    ///
    /// The encoded values are available via `values()` and dict is valid until
    /// values are changed.
    pub fn encode(&mut self, values: &[Vec<u8>], dict: &mut ValuesDict) -> (ValueType, u64, u64) {
        self.reset();

        if values.is_empty() {
            return (ValueType::STRING, 0, 0);
        }

        // Try dict encoding at first, since it gives the highest speedup during querying.
        // It also usually gives the best compression, since every value is encoded as a single byte.
        let vt = try_dict_encoding(&mut self.buf, &mut self.values, values, dict);
        if vt != ValueType::UNKNOWN {
            return (vt, 0, 0);
        }

        for try_encoding in [
            try_uint_encoding,
            try_int_encoding,
            try_float64_encoding,
            try_ipv4_encoding,
            try_timestamp_iso8601_encoding,
        ] {
            self.buf.clear();
            self.values.clear();
            let (vt, min_value, max_value) = try_encoding(&mut self.buf, &mut self.values, values);
            if vt != ValueType::UNKNOWN {
                return (vt, min_value, max_value);
            }
        }

        // Fall back to default encoding, e.g. leave values as is.
        self.buf.clear();
        self.values.clear();
        for v in values {
            let start = self.buf.len();
            self.buf.extend_from_slice(v);
            self.values.push((start, self.buf.len()));
        }
        (ValueType::STRING, 0, 0)
    }
}

/// Obtains a values encoder from the pool.
pub fn get_values_encoder() -> ValuesEncoder {
    VALUES_ENCODER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns ve to the pool.
pub fn put_values_encoder(mut ve: ValuesEncoder) {
    ve.reset();
    VALUES_ENCODER_POOL.lock().unwrap().push(ve);
}

// PORT NOTE: Go uses `sync.Pool` with `*valuesEncoder`; the port uses a
// `Mutex<Vec<ValuesEncoder>>` pool handing encoders out by value, preserving
// the buffer reuse pattern.
static VALUES_ENCODER_POOL: Mutex<Vec<ValuesEncoder>> = Mutex::new(Vec::new());

/// ValuesDecoder decodes values encoded by [`ValuesEncoder`].
///
/// PORT NOTE: Go's `valuesDecoder` owns a `buf` backing the unsafe string
/// views it writes into `values`, which stay valid until `reset()`. The port
/// writes each decoded value into the per-value `Vec<u8>` in place (reusing
/// its capacity), so no backing buffer is needed and decoded values stay
/// valid independently of the decoder.
#[derive(Default)]
pub struct ValuesDecoder {}

impl ValuesDecoder {
    /// Resets the decoder.
    pub fn reset(&mut self) {}

    /// Decodes values encoded with the given vt and the given dict_values inplace.
    pub fn decode_inplace(
        &mut self,
        values: &mut [Vec<u8>],
        vt: ValueType,
        dict_values: &[Vec<u8>],
    ) -> Result<(), String> {
        match vt {
            ValueType::STRING => {
                // nothing to do - values are already decoded.
            }
            ValueType::DICT => {
                // PORT NOTE: Go copies dict_values into vd.buf via a stringBucket so
                // the decoded values outlive dict_values; the owned per-value buffers
                // make that copy (and the stringbucket module) unnecessary here.
                for v in values.iter_mut() {
                    let id = v[0] as usize;
                    if id >= dict_values.len() {
                        return Err(format!(
                            "unexpected dictionary id: {id}; it must be smaller than {}",
                            dict_values.len()
                        ));
                    }
                    let dict_value = dict_values[id].as_slice();
                    v.clear();
                    v.extend_from_slice(dict_value);
                }
            }
            ValueType::UINT8 => {
                for v in values.iter_mut() {
                    if v.len() != 1 {
                        return Err(format!(
                            "unexpected value length for uint8; got {}; want 1",
                            v.len()
                        ));
                    }
                    let n = unmarshal_uint8(v);
                    v.clear();
                    marshal_uint8_string(v, n);
                }
            }
            ValueType::UINT16 => {
                for v in values.iter_mut() {
                    if v.len() != 2 {
                        return Err(format!(
                            "unexpected value length for uint16; got {}; want 2",
                            v.len()
                        ));
                    }
                    let n = unmarshal_uint16(v);
                    v.clear();
                    marshal_uint16_string(v, n);
                }
            }
            ValueType::UINT32 => {
                for v in values.iter_mut() {
                    if v.len() != 4 {
                        return Err(format!(
                            "unexpected value length for uint32; got {}; want 4",
                            v.len()
                        ));
                    }
                    let n = unmarshal_uint32(v);
                    v.clear();
                    marshal_uint32_string(v, n);
                }
            }
            ValueType::UINT64 => {
                for v in values.iter_mut() {
                    if v.len() != 8 {
                        return Err(format!(
                            "unexpected value length for uint64; got {}; want 8",
                            v.len()
                        ));
                    }
                    let n = unmarshal_uint64(v);
                    v.clear();
                    marshal_uint64_string(v, n);
                }
            }
            ValueType::INT64 => {
                for v in values.iter_mut() {
                    if v.len() != 8 {
                        return Err(format!(
                            "unexpected value length for int64; got {}; want 8",
                            v.len()
                        ));
                    }
                    let n = unmarshal_int64(v);
                    v.clear();
                    marshal_int64_string(v, n);
                }
            }
            ValueType::FLOAT64 => {
                for v in values.iter_mut() {
                    if v.len() != 8 {
                        return Err(format!(
                            "unexpected value length for uint64; got {}; want 8",
                            v.len()
                        ));
                    }
                    let f = unmarshal_float64(v);
                    v.clear();
                    marshal_float64_string(v, f);
                }
            }
            ValueType::IPV4 => {
                for v in values.iter_mut() {
                    if v.len() != 4 {
                        return Err(format!(
                            "unexpected value length for ipv4; got {}; want 4",
                            v.len()
                        ));
                    }
                    let ip = unmarshal_ipv4(v);
                    v.clear();
                    marshal_ipv4_string(v, ip);
                }
            }
            ValueType::TIMESTAMP_ISO8601 => {
                for v in values.iter_mut() {
                    if v.len() != 8 {
                        return Err(format!(
                            "unexpected value length for uint64; got {}; want 8",
                            v.len()
                        ));
                    }
                    let timestamp = unmarshal_timestamp_iso8601(v);
                    v.clear();
                    marshal_timestamp_iso8601_string(v, timestamp);
                }
            }
            _ => return Err(format!("unknown valueType={}", vt.0)),
        }
        Ok(())
    }
}

/// Obtains a values decoder from the pool.
pub fn get_values_decoder() -> ValuesDecoder {
    VALUES_DECODER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns vd to the pool.
pub fn put_values_decoder(mut vd: ValuesDecoder) {
    vd.reset();
    VALUES_DECODER_POOL.lock().unwrap().push(vd);
}

static VALUES_DECODER_POOL: Mutex<Vec<ValuesDecoder>> = Mutex::new(Vec::new());

fn try_timestamp_iso8601_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
) -> (ValueType, u64, u64) {
    let mut i64s = encoding::get_int64s(src_values.len());
    let mut min_value: i64 = 0;
    let mut max_value: i64 = 0;
    for (i, v) in src_values.iter().enumerate() {
        let Some(n) = try_parse_timestamp_iso8601_bytes(v) else {
            encoding::put_int64s(i64s);
            return (ValueType::UNKNOWN, 0, 0);
        };
        i64s.a[i] = n;
        if i == 0 || n < min_value {
            min_value = n;
        }
        if i == 0 || n > max_value {
            max_value = n;
        }
    }
    for &n in &i64s.a {
        let start = dst_buf.len();
        encoding::marshal_uint64(dst_buf, n as u64);
        dst_values.push((start, dst_buf.len()));
    }
    encoding::put_int64s(i64s);
    (
        ValueType::TIMESTAMP_ISO8601,
        min_value as u64,
        max_value as u64,
    )
}

/// Parses s as RFC3339 with optional nanoseconds part and timezone offset and
/// returns unix timestamp in nanoseconds.
///
/// If s doesn't contain timezone offset, then the local timezone is used.
///
/// The returned timestamp can be negative if s is smaller than 1970 year.
pub fn try_parse_timestamp_rfc3339_nano(s: &str) -> Option<i64> {
    if s.len() < "2006-01-02T15:04:05".len() {
        return None;
    }

    let (secs, tail) = try_parse_timestamp_secs(s.as_bytes())?;
    let mut nsecs = secs * 1_000_000_000;

    // Parse timezone offset
    let (offset_nsecs, prefix) = parse_timezone_offset(tail)?;
    nsecs = sub_int64_no_overflow(nsecs, offset_nsecs);
    let mut s = prefix;

    // Parse optional fractional part of seconds.
    if s.is_empty() {
        return Some(nsecs);
    }
    if s[0] == b'.' {
        s = &s[1..];
    }
    let digits = s.len();
    if digits > 9 {
        return None;
    }
    let mut n64 = try_parse_date_uint64(s)?;

    if digits < 9 {
        // PORT NOTE: Go multiplies by `uint64(math.Pow10(9 - digits))`;
        // Pow10(0..=8) converts exactly to these integers.
        n64 *= POW10_U64[9 - digits];
    }
    // PORT NOTE: wrapping add mirrors Go's silently-wrapping `nsecs += int64(n64)`.
    nsecs = nsecs.wrapping_add(n64 as i64);
    Some(nsecs)
}

const POW10_U64: [u64; 9] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
];

fn parse_timezone_offset(s: &[u8]) -> Option<(i64, &[u8])> {
    if let Some(prefix) = s.strip_suffix(b"Z") {
        return Some((0, prefix));
    }

    let Some(n) = s.iter().rposition(|&c| c == b'+' || c == b'-') else {
        let offset_nsecs = timeutil::get_local_timezone_offset_nsecs();
        return Some((offset_nsecs, s));
    };
    let offset_str = &s[n + 1..];
    let is_minus = s[n] == b'-';
    if offset_str.is_empty() {
        return None;
    }
    let mut offset_nsecs = try_parse_hhmm(offset_str)?;
    if is_minus {
        offset_nsecs = -offset_nsecs;
    }
    Some((offset_nsecs, &s[..n]))
}

fn try_parse_hhmm(s: &[u8]) -> Option<i64> {
    let (hour_str, minute_str) = match s.len() {
        5 if s[2] == b':' => (&s[..2], &s[3..]),
        4 => (&s[..2], &s[2..]),
        _ => return None,
    };
    let hours = try_parse_date_uint64(hour_str)?;
    if hours > 24 {
        return None;
    }
    let minutes = try_parse_date_uint64(minute_str)?;
    if minutes > 60 {
        return None;
    }
    Some(hours as i64 * NSECS_PER_HOUR + minutes as i64 * NSECS_PER_MINUTE)
}

/// Parses 'YYYY-MM-DDThh:mm:ss.mssZ' and returns unix timestamp in nanoseconds.
///
/// The returned timestamp can be negative if s is smaller than 1970 year.
pub fn try_parse_timestamp_iso8601(s: &str) -> Option<i64> {
    try_parse_timestamp_iso8601_bytes(s.as_bytes())
}

/// Byte-native core of [`try_parse_timestamp_iso8601`].
pub fn try_parse_timestamp_iso8601_bytes(s: &[u8]) -> Option<i64> {
    // Do not parse timestamps with timezone, since they cannot be converted back
    // to the same string representation in general case.
    // This may break search.
    if s.len() != "2006-01-02T15:04:05.000Z".len() {
        return None;
    }

    let (secs, tail) = try_parse_timestamp_secs(s)?;
    let mut s = tail;
    let mut nsecs = secs * 1_000_000_000;

    if s[0] != b'.' {
        return None;
    }
    s = &s[1..];

    // Parse milliseconds
    let tz_delimiter = s[3];
    if tz_delimiter != b'Z' {
        return None;
    }
    let millisecond_str = &s[..3];
    let msecs = try_parse_date_uint64(millisecond_str)?;
    s = &s[4..];

    if !s.is_empty() {
        panicf!(
            "BUG: unexpected tail in timestamp: {:?}",
            String::from_utf8_lossy(s)
        );
    }

    nsecs += msecs as i64 * 1_000_000;
    Some(nsecs)
}

/// Parses YYYY-MM-DDTHH:mm:ss into unix timestamp in seconds and returns the tail.
fn try_parse_timestamp_secs(s: &[u8]) -> Option<(i64, &[u8])> {
    // Parse year
    if s[4] != b'-' {
        return None;
    }
    let year_str = &s[..4];
    let n = try_parse_date_uint64(year_str)?;
    if !(1677..=2262).contains(&n) {
        return None;
    }
    let year = n as i64;
    let s = &s[5..];

    // Parse month
    if s[2] != b'-' {
        return None;
    }
    let month_str = &s[..2];
    let month = try_parse_date_uint64(month_str)? as i64;
    let s = &s[3..];

    // Parse day.
    //
    // Allow whitespace additionally to T as the delimiter after DD,
    // so SQL datetime format can be parsed additionally to RFC3339.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6721
    let delim = s[2];
    if delim != b'T' && delim != b' ' {
        return None;
    }
    let day_str = &s[..2];
    let day = try_parse_date_uint64(day_str)? as i64;
    let s = &s[3..];

    // Parse hour
    if s[2] != b':' {
        return None;
    }
    let hour_str = &s[..2];
    let hour = try_parse_date_uint64(hour_str)? as i64;
    let s = &s[3..];

    // Parse minute
    if s[2] != b':' {
        return None;
    }
    let minute_str = &s[..2];
    let minute = try_parse_date_uint64(minute_str)? as i64;
    let s = &s[3..];

    // Parse second
    let second_str = &s[..2];
    let second = try_parse_date_uint64(second_str)? as i64;
    let s = &s[2..];

    let secs = unix_secs_from_date(year, month, day, hour, minute, second);
    if !(i64::MIN / 1_000_000_000..i64::MAX / 1_000_000_000).contains(&secs) {
        // Too big or too small timestamp
        return None;
    }
    Some((secs, s))
}

/// Computes the unix timestamp in seconds the way Go's
/// `time.Date(year, month, day, hour, minute, second, 0, time.UTC).Unix()` does.
///
/// PORT NOTE: like Go's time.Date, out-of-range month values are normalized
/// into the year, while out-of-range day/hour/minute/second values simply
/// overflow linearly into the resulting timestamp.
fn unix_secs_from_date(
    mut year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
) -> i64 {
    // Normalize month, overflowing into year.
    let m = month - 1;
    year += m.div_euclid(12);
    let month = m.rem_euclid(12) + 1;

    let days = days_from_civil(year, month) + (day - 1);
    days * 86_400 + hour * 3_600 + minute * 60 + second
}

/// Returns the number of days since the unix epoch for the first day of the
/// given month in the proleptic Gregorian calendar (Howard Hinnant's
/// `days_from_civil` algorithm), which matches Go's time package for all
/// normalized inputs.
fn days_from_civil(y: i64, m: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Parses s as u64 value.
pub fn try_parse_uint64(s: &str) -> Option<u64> {
    try_parse_uint64_bytes(s.as_bytes())
}

/// Byte-native core of [`try_parse_uint64`] (Go strings are arbitrary bytes;
/// invalid UTF-8 simply fails the parse).
pub fn try_parse_uint64_bytes(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > "18_446_744_073_709_551_615".len() {
        return None;
    }
    if b.len() > 1 && b[0] == b'0' {
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8361
        return None;
    }

    let mut n: u64 = 0;
    for &ch in b {
        if ch == b'_' {
            continue;
        }
        if !ch.is_ascii_digit() {
            return None;
        }
        if n > u64::MAX / 10 {
            // overflow
            return None;
        }
        n *= 10;
        let d = (ch - b'0') as u64;
        let n1 = n.wrapping_add(d);
        if n1 < n {
            // overflow
            return None;
        }
        n = n1;
    }
    Some(n)
}

/// Parses s (which is a part of some timestamp) as u64 value.
fn try_parse_date_uint64(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 9 {
        return None;
    }

    if s.len() == 2 {
        // fast path for two-digit number, which is used in hours, minutes and seconds
        if !s[0].is_ascii_digit() {
            return None;
        }
        // PORT NOTE: like Go, the second byte isn't validated here; the
        // wrapping subtraction mirrors Go's byte arithmetic.
        let n = 10 * (s[0] - b'0') as u64 + s[1].wrapping_sub(b'0') as u64;
        return Some(n);
    }

    let mut n: u64 = 0;
    for &ch in s {
        if !ch.is_ascii_digit() {
            return None;
        }
        if n > u64::MAX / 10 {
            return None;
        }
        n *= 10;
        let d = (ch - b'0') as u64;
        if n > u64::MAX - d {
            return None;
        }
        n += d;
    }
    Some(n)
}

/// Parses s as i64 value.
pub fn try_parse_int64(s: &str) -> Option<i64> {
    try_parse_int64_bytes(s.as_bytes())
}

/// Byte-native core of [`try_parse_int64`].
pub fn try_parse_int64_bytes(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let is_minus = s[0] == b'-';
    let s = if is_minus { &s[1..] } else { s };
    let n = try_parse_uint64_bytes(s)?;
    if n >= 1 << 63 {
        if is_minus && n == 1 << 63 {
            return Some(i64::MIN);
        }
        return None;
    }
    let ni = n as i64;
    Some(if is_minus { -ni } else { ni })
}

fn try_ipv4_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
) -> (ValueType, u64, u64) {
    let mut u32s = encoding::get_uint32s(src_values.len());
    let mut min_value: u32 = 0;
    let mut max_value: u32 = 0;
    for (i, v) in src_values.iter().enumerate() {
        let Some(n) = try_parse_ipv4_bytes(v) else {
            encoding::put_uint32s(u32s);
            return (ValueType::UNKNOWN, 0, 0);
        };
        u32s.a[i] = n;
        if i == 0 || n < min_value {
            min_value = n;
        }
        if i == 0 || n > max_value {
            max_value = n;
        }
    }
    for &n in &u32s.a {
        let start = dst_buf.len();
        encoding::marshal_uint32(dst_buf, n);
        dst_values.push((start, dst_buf.len()));
    }
    encoding::put_uint32s(u32s);
    (ValueType::IPV4, min_value as u64, max_value as u64)
}

/// Tries parsing ipv4 from s.
pub fn try_parse_ipv4(s: &str) -> Option<u32> {
    try_parse_ipv4_bytes(s.as_bytes())
}

/// Byte-native core of [`try_parse_ipv4`].
pub fn try_parse_ipv4_bytes(b: &[u8]) -> Option<u32> {
    if b.len() < "1.1.1.1".len()
        || b.len() > "255.255.255.255".len()
        || b.iter().filter(|&&c| c == b'.').count() != 3
    {
        // Fast path - the entry isn't IPv4
        return None;
    }

    let mut octets = [0u8; 4];
    let mut s = b;

    for octet in &mut octets[..3] {
        let n = s.iter().position(|&c| c == b'.')?;
        if n == 0 || n > 3 {
            return None;
        }
        let v = try_parse_date_uint64(&s[..n])?;
        if v > 255 {
            return None;
        }
        *octet = v as u8;
        s = &s[n + 1..];
    }

    // Parse octet 4
    let v = try_parse_date_uint64(s)?;
    if v > 255 {
        return None;
    }
    octets[3] = v as u8;

    Some(u32::from_be_bytes(octets))
}

fn try_float64_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
) -> (ValueType, u64, u64) {
    let mut u64s = encoding::get_uint64s(src_values.len());
    let mut min_value: f64 = 0.0;
    let mut max_value: f64 = 0.0;
    for (i, v) in src_values.iter().enumerate() {
        let Some(f) = try_parse_float64_exact_bytes(v) else {
            encoding::put_uint64s(u64s);
            return (ValueType::UNKNOWN, 0, 0);
        };
        u64s.a[i] = f.to_bits();
        if i == 0 || f < min_value {
            min_value = f;
        }
        if i == 0 || f > max_value {
            max_value = f;
        }
    }
    for &n in &u64s.a {
        let start = dst_buf.len();
        encoding::marshal_uint64(dst_buf, n);
        dst_values.push((start, dst_buf.len()));
    }
    encoding::put_uint64s(u64s);
    (ValueType::FLOAT64, min_value.to_bits(), max_value.to_bits())
}

/// Tries parsing float64 number at the beginning of s and returns it with the remaining tail.
pub fn try_parse_float64_prefix(s: &str) -> Option<(f64, &str)> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.' || b[i] == b'_') {
        i += 1;
    }
    if i == 0 {
        return None;
    }

    let f = try_parse_float64(&s[..i])?;
    Some((f, &s[i..]))
}

/// Tries parsing s as f64.
///
/// The parsed result may lose precision, e.g. it may not match the original value
/// when converting back to string.
/// Use try_parse_float64_exact when lossless parsing is needed.
pub fn try_parse_float64(s: &str) -> Option<f64> {
    try_parse_float64_internal(s.as_bytes(), false)
}

/// Byte-native variant of [`try_parse_float64`].
pub fn try_parse_float64_bytes(s: &[u8]) -> Option<f64> {
    try_parse_float64_internal(s, false)
}

/// Tries parsing s as f64.
pub fn try_parse_float64_exact(s: &str) -> Option<f64> {
    try_parse_float64_internal(s.as_bytes(), true)
}

/// Byte-native variant of [`try_parse_float64_exact`].
pub fn try_parse_float64_exact_bytes(s: &[u8]) -> Option<f64> {
    try_parse_float64_internal(s, true)
}

fn try_parse_float64_internal(s: &[u8], is_exact: bool) -> Option<f64> {
    if s.is_empty() || s.len() > "-18_446_744_073_709_551_615".len() {
        return None;
    }
    // Allow only decimal digits, minus and a dot.
    // Do not allows scientific notation (for example 1.23E+05),
    // since it cannot be converted back to the same string form.

    let minus = s[0] == b'-';
    let s = if minus { &s[1..] } else { s };
    let Some(n) = s.iter().position(|&c| c == b'.') else {
        // fast path - there are no dots
        let n = try_parse_uint64_bytes(s)?;
        if is_exact && n >= (1 << 53) {
            // The integer cannot be represented as float64 without precision loss.
            return None;
        }
        let f = n as f64;
        return Some(if minus { -f } else { f });
    };
    if n == 0 || n == s.len() - 1 {
        // Do not allow dots at the beginning and at the end of s,
        // since they cannot be converted back to the same string form.
        return None;
    }
    let s_int = &s[..n];
    let s_frac = &s[n + 1..];

    let n_int = try_parse_uint64_bytes(s_int)?;

    // Skip leading zeroes at s_frac, since try_parse_uint64 rejects them.
    // This fixes https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8464
    let fb = s_frac;
    let mut n = 0;
    while n < fb.len() - 1 && fb[n] == b'0' {
        n += 1;
    }

    let n_frac = try_parse_uint64_bytes(&s_frac[n..])?;

    let underscores = fb.iter().filter(|&&c| c == b'_').count() as i64;
    let p10 = pow10(underscores - s_frac.len() as i64);
    // PORT NOTE: f64::mul_add is the IEEE 754 fusedMultiplyAdd, same as Go's math.FMA.
    let f = (n_frac as f64).mul_add(p10, n_int as f64);
    Some(if minus { -f } else { f })
}

/// Computes 10^n exactly like Go's `math.Pow10` for the exponent range
/// reachable from this module (|n| <= 31 given the input length limits).
fn pow10(n: i64) -> f64 {
    const POW10_TAB: [f64; 32] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
    ];
    if (0..=31).contains(&n) {
        return POW10_TAB[n as usize];
    }
    if (-31..0).contains(&n) {
        return 1.0 / POW10_TAB[(-n) as usize];
    }
    // PORT NOTE: unreachable for the callers in this module; kept as a fallback.
    10f64.powi(n as i32)
}

/// Parses user-readable bytes representation in s.
///
/// Supported suffixes:
///
/// K, KB - for 1000
pub fn try_parse_bytes(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }

    let is_minus = s.as_bytes()[0] == b'-';
    let mut s = if is_minus { &s[1..] } else { s };

    let mut n: i64 = 0;
    while !s.is_empty() {
        let (f, tail) = try_parse_float64_prefix(s)?;
        if tail.is_empty() && f.fract() != 0.0 {
            // deny floating-point numbers without any suffix.
            return None;
        }
        s = tail;
        if s.is_empty() {
            n = add_int64_no_overflow(n, f);
            continue;
        }
        if s.len() >= 3 {
            let mut matched = true;
            match &s[..3] {
                "KiB" => n = add_int64_no_overflow(n, f * (1u64 << 10) as f64),
                "MiB" => n = add_int64_no_overflow(n, f * (1u64 << 20) as f64),
                "GiB" => n = add_int64_no_overflow(n, f * (1u64 << 30) as f64),
                "TiB" => n = add_int64_no_overflow(n, f * (1u64 << 40) as f64),
                _ => matched = false,
            }
            if matched {
                s = &s[3..];
                continue;
            }
        }
        if s.len() >= 2 {
            let mut matched = true;
            match &s[..2] {
                "Ki" => n = add_int64_no_overflow(n, f * (1u64 << 10) as f64),
                "Mi" => n = add_int64_no_overflow(n, f * (1u64 << 20) as f64),
                "Gi" => n = add_int64_no_overflow(n, f * (1u64 << 30) as f64),
                "Ti" => n = add_int64_no_overflow(n, f * (1u64 << 40) as f64),
                "KB" => n = add_int64_no_overflow(n, f * 1_000.0),
                "MB" => n = add_int64_no_overflow(n, f * 1_000_000.0),
                "GB" => n = add_int64_no_overflow(n, f * 1_000_000_000.0),
                "TB" => n = add_int64_no_overflow(n, f * 1_000_000_000_000.0),
                _ => matched = false,
            }
            if matched {
                s = &s[2..];
                continue;
            }
        }
        let mut matched = true;
        match s.as_bytes()[0] {
            b'B' => n = add_int64_no_overflow(n, f),
            b'K' => n = add_int64_no_overflow(n, f * 1_000.0),
            b'M' => n = add_int64_no_overflow(n, f * 1_000_000.0),
            b'G' => n = add_int64_no_overflow(n, f * 1_000_000_000.0),
            b'T' => n = add_int64_no_overflow(n, f * 1_000_000_000_000.0),
            _ => matched = false,
        }
        if matched {
            s = &s[1..];
        }
        // When no suffix matched, the next try_parse_float64_prefix call
        // fails, exactly like in Go.
    }

    Some(if is_minus { -n } else { n })
}

fn add_int64_no_overflow(n: i64, f: f64) -> i64 {
    // PORT NOTE: Rust's saturating `as i64` cast and Go's platform-defined
    // out-of-range conversion take different values for huge f, but both are
    // caught by the checks below, producing identical results.
    let x = f as i64;
    if n < 0 || x < 0 || x > i64::MAX - n {
        return i64::MAX;
    }
    n + x
}

/// Parses '/num' ipv4 mask and returns (1<<(32-num))
pub fn try_parse_ipv4_mask(s: &str) -> Option<u64> {
    let s = s.strip_prefix('/')?;
    let n = try_parse_uint64(s)?;
    if n > 32 {
        return None;
    }
    Some(1u64 << (32 - n))
}

/// Parses the given duration in nanoseconds and returns the result.
pub fn try_parse_duration(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let is_minus = s.as_bytes()[0] == b'-';
    let mut s = if is_minus { &s[1..] } else { s };

    let mut nsecs: i64 = 0;
    while !s.is_empty() {
        let (f, tail) = try_parse_float64_prefix(s)?;
        s = tail;
        if s.is_empty() {
            return None;
        }
        if s.len() >= 3 && s.starts_with("µs") {
            nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_MICROSECOND as f64);
            s = &s[3..];
            continue;
        }
        if s.len() >= 2 {
            match &s[..2] {
                "ms" => {
                    nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_MILLISECOND as f64);
                    s = &s[2..];
                    continue;
                }
                "ns" => {
                    nsecs = add_int64_no_overflow(nsecs, f);
                    s = &s[2..];
                    continue;
                }
                _ => {}
            }
        }
        match s.as_bytes()[0] {
            b'y' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_YEAR as f64);
                s = &s[1..];
            }
            b'w' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_WEEK as f64);
                s = &s[1..];
            }
            b'd' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_DAY as f64);
                s = &s[1..];
            }
            b'h' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_HOUR as f64);
                s = &s[1..];
            }
            b'm' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_MINUTE as f64);
                s = &s[1..];
            }
            b's' => {
                nsecs = add_int64_no_overflow(nsecs, f * NSECS_PER_SECOND as f64);
                s = &s[1..];
            }
            _ => return None,
        }
    }

    Some(if is_minus { -nsecs } else { nsecs })
}

/// Appends string representation of nsecs duration to dst.
pub fn marshal_duration_string(dst: &mut Vec<u8>, mut nsecs: i64) {
    if nsecs == 0 {
        dst.push(b'0');
        return;
    }

    if nsecs < 0 {
        dst.push(b'-');
        // PORT NOTE: wrapping_neg mirrors Go's silently-wrapping `nsecs = -nsecs`
        // for i64::MIN.
        nsecs = nsecs.wrapping_neg();
    }
    let format_float64_seconds = nsecs >= NSECS_PER_SECOND;

    if nsecs >= NSECS_PER_WEEK {
        let weeks = nsecs / NSECS_PER_WEEK;
        nsecs -= weeks * NSECS_PER_WEEK;
        marshal_uint64_string(dst, weeks as u64);
        dst.push(b'w');
    }
    if nsecs >= NSECS_PER_DAY {
        let days = nsecs / NSECS_PER_DAY;
        nsecs -= days * NSECS_PER_DAY;
        marshal_uint8_string(dst, days as u8);
        dst.push(b'd');
    }
    if nsecs >= NSECS_PER_HOUR {
        let hours = nsecs / NSECS_PER_HOUR;
        nsecs -= hours * NSECS_PER_HOUR;
        marshal_uint8_string(dst, hours as u8);
        dst.push(b'h');
    }
    if nsecs >= NSECS_PER_MINUTE {
        let minutes = nsecs / NSECS_PER_MINUTE;
        nsecs -= minutes * NSECS_PER_MINUTE;
        marshal_uint8_string(dst, minutes as u8);
        dst.push(b'm');
    }
    if nsecs >= NSECS_PER_SECOND {
        if format_float64_seconds {
            let seconds = nsecs as f64 / NSECS_PER_SECOND as f64;
            marshal_float64_string(dst, seconds);
            dst.push(b's');
            return;
        }
        let seconds = nsecs / NSECS_PER_SECOND;
        nsecs -= seconds * NSECS_PER_SECOND;
        marshal_uint8_string(dst, seconds as u8);
        dst.push(b's');
    }
    if nsecs >= NSECS_PER_MILLISECOND {
        let msecs = nsecs / NSECS_PER_MILLISECOND;
        nsecs -= msecs * NSECS_PER_MILLISECOND;
        marshal_uint16_string(dst, msecs as u16);
        dst.extend_from_slice(b"ms");
    }
    if nsecs >= NSECS_PER_MICROSECOND {
        let usecs = nsecs / NSECS_PER_MICROSECOND;
        nsecs -= usecs * NSECS_PER_MICROSECOND;
        marshal_uint16_string(dst, usecs as u16);
        dst.extend_from_slice("µs".as_bytes());
    }
    if nsecs > 0 {
        marshal_uint16_string(dst, nsecs as u16);
        dst.extend_from_slice(b"ns");
    }
}

pub const NSECS_PER_YEAR: i64 = 365 * 24 * 3600 * 1_000_000_000;
pub const NSECS_PER_WEEK: i64 = 7 * 24 * 3600 * 1_000_000_000;
pub const NSECS_PER_DAY: i64 = 24 * 3600 * 1_000_000_000;
pub const NSECS_PER_HOUR: i64 = 3600 * 1_000_000_000;
pub const NSECS_PER_MINUTE: i64 = 60 * 1_000_000_000;
pub const NSECS_PER_SECOND: i64 = 1_000_000_000;
pub const NSECS_PER_MILLISECOND: i64 = 1_000_000;
pub const NSECS_PER_MICROSECOND: i64 = 1_000;

/// Calculates a-b and makes sure that the result doesn't overflow int64.
///
/// It clamps the result to the i64 value range.
///
/// PORT NOTE: Go defines SubInt64NoOverflow in parser.go; it is placed here
/// because try_parse_timestamp_rfc3339_nano depends on it while the parser
/// module is not ported yet. The parser port should re-export it from here.
pub fn sub_int64_no_overflow(a: i64, b: i64) -> i64 {
    if b >= 0 {
        if a == i64::MAX {
            // Subtracting any number from +Inf must result in +Inf.
            return a;
        }
        if a < i64::MIN + b {
            return i64::MIN;
        }
        return a - b;
    }

    if a == i64::MIN {
        // Adding any number to -Inf must result in -Inf.
        return a;
    }
    if a > i64::MAX + b {
        return i64::MAX;
    }
    a - b
}

fn try_int_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
) -> (ValueType, u64, u64) {
    let mut i64s = encoding::get_int64s(src_values.len());
    let mut min_value: i64 = 0;
    let mut max_value: i64 = 0;
    for (i, v) in src_values.iter().enumerate() {
        let Some(n) = try_parse_int64_bytes(v) else {
            encoding::put_int64s(i64s);
            return (ValueType::UNKNOWN, 0, 0);
        };
        i64s.a[i] = n;
        if i == 0 || n < min_value {
            min_value = n;
        }
        if i == 0 || n > max_value {
            max_value = n;
        }
    }
    for &n in &i64s.a {
        let start = dst_buf.len();
        encoding::marshal_int64(dst_buf, n);
        dst_values.push((start, dst_buf.len()));
    }
    encoding::put_int64s(i64s);
    (ValueType::INT64, min_value as u64, max_value as u64)
}

fn try_uint_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
) -> (ValueType, u64, u64) {
    let mut u64s = encoding::get_uint64s(src_values.len());
    let mut min_value: u64 = 0;
    let mut max_value: u64 = 0;
    for (i, v) in src_values.iter().enumerate() {
        let Some(n) = try_parse_uint64_bytes(v) else {
            encoding::put_uint64s(u64s);
            return (ValueType::UNKNOWN, 0, 0);
        };
        u64s.a[i] = n;
        if i == 0 || n < min_value {
            min_value = n;
        }
        if i == 0 || n > max_value {
            max_value = n;
        }
    }

    let min_bit_size = 64 - max_value.leading_zeros();
    let vt = if min_bit_size <= 8 {
        for &n in &u64s.a {
            let start = dst_buf.len();
            dst_buf.push(n as u8);
            dst_values.push((start, dst_buf.len()));
        }
        ValueType::UINT8
    } else if min_bit_size <= 16 {
        for &n in &u64s.a {
            let start = dst_buf.len();
            encoding::marshal_uint16(dst_buf, n as u16);
            dst_values.push((start, dst_buf.len()));
        }
        ValueType::UINT16
    } else if min_bit_size <= 32 {
        for &n in &u64s.a {
            let start = dst_buf.len();
            encoding::marshal_uint32(dst_buf, n as u32);
            dst_values.push((start, dst_buf.len()));
        }
        ValueType::UINT32
    } else {
        for &n in &u64s.a {
            let start = dst_buf.len();
            encoding::marshal_uint64(dst_buf, n);
            dst_values.push((start, dst_buf.len()));
        }
        ValueType::UINT64
    };
    encoding::put_uint64s(u64s);
    (vt, min_value, max_value)
}

fn try_dict_encoding(
    dst_buf: &mut Vec<u8>,
    dst_values: &mut Vec<(usize, usize)>,
    src_values: &[Vec<u8>],
    dict: &mut ValuesDict,
) -> ValueType {
    dict.reset();
    let dst_buf_orig_len = dst_buf.len();
    let dst_values_orig_len = dst_values.len();

    for v in src_values {
        let Some(id) = dict.get_or_add(v) else {
            dict.reset();
            dst_buf.truncate(dst_buf_orig_len);
            dst_values.truncate(dst_values_orig_len);
            return ValueType::UNKNOWN;
        };
        let start = dst_buf.len();
        dst_buf.push(id);
        dst_values.push((start, dst_buf.len()));
    }
    ValueType::DICT
}

/// ValuesDict is a dictionary for a small number of unique column values.
#[derive(Debug, Default)]
pub struct ValuesDict {
    pub values: Vec<Vec<u8>>,
}

impl ValuesDict {
    /// Resets the dict.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Copies src into self.
    ///
    /// PORT NOTE: Go has both `copyFrom(a *arena, src)` (copying values into
    /// the arena and keeping unsafe views) and `copyFromNoArena(src)`. The
    /// port stores owned `String`s, which makes the arena variant redundant;
    /// only `copy_from_no_arena` is provided and covers both Go call sites.
    pub fn copy_from_no_arena(&mut self, src: &ValuesDict) {
        self.reset();
        self.values.extend(src.values.iter().cloned());
    }

    /// Returns the id of k in the dict, adding it when not yet present.
    ///
    /// Returns None when the dict is full or k is too long.
    pub fn get_or_add(&mut self, k: &[u8]) -> Option<u8> {
        if k.len() > MAX_DICT_SIZE_BYTES {
            return None;
        }
        let mut dict_size_bytes = 0;
        for (i, v) in self.values.iter().enumerate() {
            if k == v {
                return Some(i as u8);
            }
            dict_size_bytes += v.len();
        }
        if self.values.len() >= MAX_DICT_LEN || dict_size_bytes + k.len() > MAX_DICT_SIZE_BYTES {
            return None;
        }
        self.values.push(k.to_vec());

        Some((self.values.len() - 1) as u8)
    }

    /// Appends the marshaled dict to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        let values = &self.values;
        if values.len() > MAX_DICT_LEN {
            panicf!(
                "BUG: valuesDict may contain max {} items; got {} items",
                MAX_DICT_LEN,
                values.len()
            );
        }
        dst.push(values.len() as u8);
        marshal_strings(dst, values);
    }

    /// Unmarshals the dict from src and returns the remaining tail.
    ///
    /// PORT NOTE: Go's `unmarshalInplace` keeps unsafe string views into src
    /// valid until src is changed; the port copies the values into owned
    /// byte vectors (the name is kept for parity).
    pub fn unmarshal_inplace<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        self.reset();

        if src.is_empty() {
            return Err("cannot umarshal dict len from 0 bytes; need at least 1 byte".to_string());
        }
        let dict_len = src[0] as usize;
        let mut src = &src[1..];
        for i in 0..dict_len {
            let (data, n_size) = encoding::unmarshal_bytes(src);
            if n_size <= 0 {
                return Err(format!(
                    "cannot umarshal value {i} out of {dict_len} from dict"
                ));
            }
            src = &src[n_size as usize..];

            self.values.push(data.unwrap().to_vec());
        }
        Ok(src)
    }
}

/// Appends the marshaled strings a to dst.
///
/// PORT NOTE: Go defines marshalStrings in storage_search.go; it is inlined
/// here since valuesDict.marshal (part of the on-disk columnsHeader format)
/// depends on it while storage_search is far from being ported.
fn marshal_strings(dst: &mut Vec<u8>, a: &[Vec<u8>]) {
    for v in a {
        encoding::marshal_bytes(dst, v);
    }
}

pub fn unmarshal_uint8(v: &[u8]) -> u8 {
    v[0]
}

pub fn unmarshal_uint16(v: &[u8]) -> u16 {
    encoding::unmarshal_uint16(v)
}

pub fn unmarshal_uint32(v: &[u8]) -> u32 {
    encoding::unmarshal_uint32(v)
}

pub fn unmarshal_uint64(v: &[u8]) -> u64 {
    encoding::unmarshal_uint64(v)
}

pub fn unmarshal_int64(v: &[u8]) -> i64 {
    encoding::unmarshal_int64(v)
}

pub fn marshal_float64(dst: &mut Vec<u8>, f: f64) {
    encoding::marshal_uint64(dst, f.to_bits());
}

pub fn unmarshal_float64(v: &[u8]) -> f64 {
    f64::from_bits(unmarshal_uint64(v))
}

pub fn unmarshal_ipv4(v: &[u8]) -> u32 {
    unmarshal_uint32(v)
}

pub fn unmarshal_timestamp_iso8601(v: &[u8]) -> i64 {
    unmarshal_uint64(v) as i64
}

pub fn marshal_uint8_string(dst: &mut Vec<u8>, mut n: u8) {
    if n < 10 {
        dst.push(b'0' + n);
        return;
    }
    if n < 100 {
        dst.push(b'0' + n / 10);
        dst.push(b'0' + n % 10);
        return;
    }

    if n < 200 {
        dst.push(b'1');
        n -= 100;
    } else {
        dst.push(b'2');
        n -= 200;
    }
    if n < 10 {
        dst.push(b'0');
        dst.push(b'0' + n);
        return;
    }
    dst.push(b'0' + n / 10);
    dst.push(b'0' + n % 10);
}

pub fn marshal_uint16_string(dst: &mut Vec<u8>, n: u16) {
    marshal_uint64_string(dst, n as u64);
}

pub fn marshal_uint32_string(dst: &mut Vec<u8>, n: u32) {
    marshal_uint64_string(dst, n as u64);
}

pub fn marshal_uint64_string(dst: &mut Vec<u8>, n: u64) {
    write!(dst, "{n}").unwrap();
}

pub fn marshal_int64_string(dst: &mut Vec<u8>, n: i64) {
    write!(dst, "{n}").unwrap();
}

/// Appends the shortest decimal representation of f that parses back exactly.
///
/// PORT NOTE: Go uses `strconv.AppendFloat(dst, f, 'f', -1, 64)` - the
/// shortest round-trip decimal without exponent notation; Rust's `Display`
/// for f64 produces the same digits for finite values. Non-finite values
/// format differently ("inf" vs "+Inf"), so they are special-cased.
pub fn marshal_float64_string(dst: &mut Vec<u8>, f: f64) {
    if f.is_nan() {
        dst.extend_from_slice(b"NaN");
        return;
    }
    if f.is_infinite() {
        dst.extend_from_slice(if f > 0.0 { b"+Inf" } else { b"-Inf" });
        return;
    }
    write!(dst, "{f}").unwrap();
}

pub fn marshal_ipv4_string(dst: &mut Vec<u8>, n: u32) {
    marshal_uint8_string(dst, (n >> 24) as u8);
    dst.push(b'.');
    marshal_uint8_string(dst, (n >> 16) as u8);
    dst.push(b'.');
    marshal_uint8_string(dst, (n >> 8) as u8);
    dst.push(b'.');
    marshal_uint8_string(dst, n as u8);
}

/// Appends ISO8601-formatted ("2006-01-02T15:04:05.000Z") nsecs to dst.
pub fn marshal_timestamp_iso8601_string(dst: &mut Vec<u8>, nsecs: i64) {
    let (year, month, day, hour, minute, second, nsec) = utc_date_time_from_nsecs(nsecs);
    write!(
        dst,
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        nsec / 1_000_000
    )
    .unwrap();
}

/// Appends RFC3339Nano-formatted nsecs to dst.
pub fn marshal_timestamp_rfc3339_nano_string(dst: &mut Vec<u8>, nsecs: i64) {
    let (year, month, day, hour, minute, second, nsec) = utc_date_time_from_nsecs(nsecs);
    write!(
        dst,
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}"
    )
    .unwrap();
    if nsec != 0 {
        // time.RFC3339Nano removes trailing zeros from the fractional part.
        let mut frac_len = 9;
        let mut nsec = nsec;
        while nsec % 10 == 0 {
            nsec /= 10;
            frac_len -= 1;
        }
        write!(dst, ".{nsec:0frac_len$}").unwrap();
    }
    dst.push(b'Z');
}

/// Appends RFC3339-formatted nsecs with nanosecond precision to dst.
pub fn marshal_timestamp_rfc3339_nano_precise_string(dst: &mut Vec<u8>, nsecs: i64) {
    let (year, month, day, hour, minute, second, nsec) = utc_date_time_from_nsecs(nsecs);
    write!(
        dst,
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nsec:09}Z"
    )
    .unwrap();
}

/// Splits unix nanoseconds into UTC (year, month, day, hour, minute, second, nsec)
/// the way Go's `time.Unix(0, nsecs).UTC()` does.
fn utc_date_time_from_nsecs(nsecs: i64) -> (i64, i64, i64, i64, i64, i64, i64) {
    let secs = nsecs.div_euclid(1_000_000_000);
    let nsec = nsecs.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    (
        year,
        month,
        day,
        rem / 3_600,
        (rem / 60) % 60,
        rem % 60,
        nsec,
    )
}

/// Converts days since the unix epoch to (year, month, day) in the proleptic
/// Gregorian calendar (Howard Hinnant's `civil_from_days` algorithm), which
/// matches Go's time package.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_encoder() {
        fn f(
            values: &[String],
            expected_value_type: ValueType,
            expected_min_value: u64,
            expected_max_value: u64,
        ) {
            let byte_values: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
            let mut ve = get_values_encoder();
            let mut dict = ValuesDict::default();
            let (vt, min_value, max_value) = ve.encode(&byte_values, &mut dict);
            assert_eq!(
                vt, expected_value_type,
                "unexpected value type; got {}; want {}",
                vt.0, expected_value_type.0
            );
            assert_eq!(
                min_value, expected_min_value,
                "unexpected minValue; got {min_value}; want {expected_min_value}"
            );
            assert_eq!(
                max_value, expected_max_value,
                "unexpected maxValue; got {max_value}; want {expected_max_value}"
            );
            let mut encoded_values: Vec<Vec<u8>> = ve.values().map(|v| v.to_vec()).collect();
            put_values_encoder(ve);

            let mut vd = get_values_decoder();
            vd.decode_inplace(&mut encoded_values, vt, &dict.values)
                .expect("unexpected error in decode_inplace()");
            let decoded: Vec<&str> = encoded_values
                .iter()
                .map(|v| std::str::from_utf8(v).unwrap())
                .collect();
            let want: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
            assert_eq!(
                decoded, want,
                "unexpected values decoded\ngot\n{decoded:?}\nwant\n{want:?}"
            );
            put_values_decoder(vd);
        }

        // An empty values list
        f(&[], ValueType::STRING, 0, 0);

        // string values
        let mut values: Vec<String> = (0..MAX_DICT_LEN + 1)
            .map(|i| format!("value_{i}"))
            .collect();
        f(&values, ValueType::STRING, 0, 0);

        // dict values
        f(&["foobar".to_string()], ValueType::DICT, 0, 0);
        f(
            &["foo".to_string(), "bar".to_string()],
            ValueType::DICT,
            0,
            0,
        );
        f(
            &["1".to_string(), "2foo".to_string()],
            ValueType::DICT,
            0,
            0,
        );

        // uint8 values
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("{}", (i + 1) as u64);
        }
        f(&values, ValueType::UINT8, 1, values.len() as u64);

        // uint16 values
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("{}", ((i + 1) as u64) << 8);
        }
        f(
            &values,
            ValueType::UINT16,
            1 << 8,
            (values.len() as u64) << 8,
        );

        // uint32 values
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("{}", ((i + 1) as u64) << 16);
        }
        f(
            &values,
            ValueType::UINT32,
            1 << 16,
            (values.len() as u64) << 16,
        );

        // uint64 values
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("{}", ((i + 1) as u64) << 32);
        }
        f(
            &values,
            ValueType::UINT64,
            1 << 32,
            (values.len() as u64) << 32,
        );

        // float64 values
        for (i, v) in values.iter_mut().enumerate() {
            // PORT NOTE: Go formats with %g; Rust's Display produces the same
            // shortest representation for these magnitudes.
            *v = format!("{}", ((i + 1) as f64).sqrt());
        }
        f(
            &values,
            ValueType::FLOAT64,
            4607182418800017408,
            4613937818241073152,
        );

        // ipv4 values
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("1.2.3.{i}");
        }
        f(&values, ValueType::IPV4, 16909056, 16909064);

        // iso8601 timestamps
        for (i, v) in values.iter_mut().enumerate() {
            *v = format!("2011-04-19T03:44:01.{i:03}Z");
        }
        f(
            &values,
            ValueType::TIMESTAMP_ISO8601,
            1303184641000000000,
            1303184641008000000,
        );
    }

    #[test]
    fn test_values_encoder_invalid_utf8_roundtrip() {
        // Values with invalid UTF-8 must fail every typed encoding (numeric,
        // ipv4, timestamp) and fall through to STRING (or DICT), surviving
        // the encode->decode round trip byte-identically - Go semantics for
        // arbitrary-byte strings.
        let values: Vec<Vec<u8>> = (0..MAX_DICT_LEN + 1)
            .map(|i| {
                let mut v = format!("value_{i}_").into_bytes();
                v.extend_from_slice(b"a\xff\xfeb");
                v
            })
            .collect();

        let mut ve = get_values_encoder();
        let mut dict = ValuesDict::default();
        let (vt, _, _) = ve.encode(&values, &mut dict);
        assert_eq!(vt, ValueType::STRING, "expecting STRING value type");
        let mut encoded_values: Vec<Vec<u8>> = ve.values().map(|v| v.to_vec()).collect();
        put_values_encoder(ve);

        let mut vd = get_values_decoder();
        vd.decode_inplace(&mut encoded_values, vt, &dict.values)
            .expect("unexpected error in decode_inplace()");
        assert_eq!(
            encoded_values, values,
            "invalid UTF-8 values must round-trip byte-identically"
        );
        put_values_decoder(vd);

        // The same for a small set of values, which is DICT-encoded.
        let values: Vec<Vec<u8>> = vec![b"a\xff\xfeb".to_vec(), b"\x80".to_vec()];
        let mut ve = get_values_encoder();
        let mut dict = ValuesDict::default();
        let (vt, _, _) = ve.encode(&values, &mut dict);
        assert_eq!(vt, ValueType::DICT, "expecting DICT value type");
        let mut encoded_values: Vec<Vec<u8>> = ve.values().map(|v| v.to_vec()).collect();
        put_values_encoder(ve);

        let mut vd = get_values_decoder();
        vd.decode_inplace(&mut encoded_values, vt, &dict.values)
            .expect("unexpected error in decode_inplace()");
        assert_eq!(
            encoded_values, values,
            "invalid UTF-8 dict values must round-trip byte-identically"
        );
        put_values_decoder(vd);
    }

    #[test]
    fn test_try_parse_ipv4_string_success() {
        fn f(s: &str) {
            let n = try_parse_ipv4(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            let mut data = Vec::new();
            marshal_ipv4_string(&mut data, n);
            assert_eq!(
                data,
                s.as_bytes(),
                "unexpected ip; got {:?}; want {s:?}",
                String::from_utf8_lossy(&data)
            );
        }

        f("0.0.0.0");
        f("1.2.3.4");
        f("255.255.255.255");
        f("127.0.0.1");
    }

    #[test]
    fn test_try_parse_ipv4_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_ipv4(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        f("");
        f("foo");
        f("a.b.c.d");
        f("127.0.0.x");
        f("127.0.x.0");
        f("127.x.0.0");
        f("x.0.0.0");

        // Too big octets
        f("127.127.127.256");
        f("127.127.256.127");
        f("127.256.127.127");
        f("256.127.127.127");

        // Negative octets
        f("-1.127.127.127");
        f("127.-1.127.127");
        f("127.127.-1.127");
        f("127.127.127.-1");
    }

    #[test]
    fn test_try_parse_timestamp_rfc3339_nano_string_success() {
        fn f(s: &str, timestamp_expected: &str) {
            let nsecs = try_parse_timestamp_rfc3339_nano(s)
                .unwrap_or_else(|| panic!("cannot parse timestamp {s:?}"));
            let mut timestamp = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut timestamp, nsecs);
            assert_eq!(
                timestamp,
                timestamp_expected.as_bytes(),
                "unexpected timestamp; got {:?}; want {timestamp_expected:?}",
                String::from_utf8_lossy(&timestamp)
            );
        }

        // No fractional seconds
        f("2023-01-15T23:45:51Z", "2023-01-15T23:45:51Z");

        // Different number of fractional seconds
        f("2023-01-15T23:45:51.1Z", "2023-01-15T23:45:51.1Z");
        f("2023-01-15T23:45:51.12Z", "2023-01-15T23:45:51.12Z");
        f("2023-01-15T23:45:51.123Z", "2023-01-15T23:45:51.123Z");
        f("2023-01-15T23:45:51.1234Z", "2023-01-15T23:45:51.1234Z");
        f("2023-01-15T23:45:51.12345Z", "2023-01-15T23:45:51.12345Z");
        f("2023-01-15T23:45:51.123456Z", "2023-01-15T23:45:51.123456Z");
        f(
            "2023-01-15T23:45:51.1234567Z",
            "2023-01-15T23:45:51.1234567Z",
        );
        f(
            "2023-01-15T23:45:51.12345678Z",
            "2023-01-15T23:45:51.12345678Z",
        );
        f(
            "2023-01-15T23:45:51.123456789Z",
            "2023-01-15T23:45:51.123456789Z",
        );

        // The minimum possible timestamp
        f("1677-09-21T00:12:44Z", "1677-09-21T00:12:44Z");

        // The maximum possible timestamp
        f(
            "2262-04-11T23:47:15.999999999Z",
            "2262-04-11T23:47:15.999999999Z",
        );

        // timestamp with timezone
        f("2023-01-16T00:45:51+01:00", "2023-01-15T23:45:51Z");
        f("2023-01-16T00:45:51.123-01:00", "2023-01-16T01:45:51.123Z");
        // timestamp with timezone without colon in offset
        f("2023-01-16T00:45:51+0100", "2023-01-15T23:45:51Z");
        f("2023-01-16T00:45:51.123-0130", "2023-01-16T02:15:51.123Z");

        // SQL datetime format
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6721
        f("2023-01-16 00:45:51+01:00", "2023-01-15T23:45:51Z");
        f("2023-01-16 00:45:51.123-01:00", "2023-01-16T01:45:51.123Z");
        f("2023-01-16 00:45:51+0100", "2023-01-15T23:45:51Z");
        f("2023-01-16 00:45:51.123-0130", "2023-01-16T02:15:51.123Z");
    }

    #[test]
    fn test_try_parse_timestamp_rfc3339_nano_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_timestamp_rfc3339_nano(s).is_none(),
                "expecting failure when parsing {s:?}"
            );
        }

        // invalid length
        f("");
        f("foobar");

        // missing fractional part after dot
        f("2023-01-15T22:15:51.Z");

        // too small year
        f("1676-09-21T00:12:43Z");

        // too big year
        f("2263-04-11T23:47:17Z");

        // too small timestamp
        f("1677-09-21T00:12:43.999999999Z");

        // too big timestamp
        f("2262-04-11T23:47:16Z");

        // invalid year
        f("YYYY-04-11T23:47:17Z");

        // invalid moth
        f("2023-MM-11T23:47:17Z");

        // invalid day
        f("2023-01-DDT23:47:17Z");

        // invalid hour
        f("2023-01-23Thh:47:17Z");

        // invalid minute
        f("2023-01-23T23:mm:17Z");

        // invalid second
        f("2023-01-23T23:33:ssZ");
    }

    #[test]
    fn test_try_parse_timestamp_iso8601_string_success() {
        fn f(s: &str) {
            let nsecs = try_parse_timestamp_iso8601(s)
                .unwrap_or_else(|| panic!("cannot parse timestamp {s:?}"));
            let mut data = Vec::new();
            marshal_timestamp_iso8601_string(&mut data, nsecs);
            assert_eq!(
                data,
                s.as_bytes(),
                "unexpected timestamp; got {:?}; want {s:?}",
                String::from_utf8_lossy(&data)
            );
        }

        // regular timestamp
        f("2023-01-15T23:45:51.123Z");

        // The minimum possible timestamp
        f("1677-09-21T00:12:44.000Z");

        // The maximum possible timestamp
        f("2262-04-11T23:47:15.999Z");
    }

    #[test]
    fn test_try_parse_timestamp_iso8601_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_timestamp_iso8601(s).is_none(),
                "expecting failure when parsing {s:?}"
            );
        }

        // invalid length
        f("");
        f("foobar");

        // Missing Z at the end
        f("2023-01-15T22:15:51.123");
        f("2023-01-15T22:15:51.1234");

        // timestamp with timezone
        f("2023-01-16T00:45:51.123+01:00");

        // too small year
        f("1676-09-21T00:12:43.434Z");

        // too big year
        f("2263-04-11T23:47:17.434Z");

        // too small timestamp
        f("1677-09-21T00:12:43.999Z");

        // too big timestamp
        f("2262-04-11T23:47:16.000Z");

        // invalid year
        f("YYYY-04-11T23:47:17.123Z");

        // invalid moth
        f("2023-MM-11T23:47:17.123Z");

        // invalid day
        f("2023-01-DDT23:47:17.123Z");

        // invalid hour
        f("2023-01-23Thh:47:17.123Z");

        // invalid minute
        f("2023-01-23T23:mm:17.123Z");

        // invalid second
        f("2023-01-23T23:33:ss.123Z");
    }

    #[test]
    fn test_try_parse_duration_success() {
        fn f(s: &str, nsecs_expected: i64) {
            let nsecs = try_parse_duration(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert_eq!(
                nsecs, nsecs_expected,
                "unexpected value; got {nsecs}; want {nsecs_expected}"
            );
        }

        // zero duration
        f("0s", 0);
        f("0.0w0d0h0s0.0ms", 0);
        f("-0.0w0.00d0h0s0.0000ms", 0);
        f("-0w", 0);

        // positive duration
        f("1s", NSECS_PER_SECOND);
        f("1.5ms", 1_500_000);
        f("1µs", NSECS_PER_MICROSECOND);
        f("1ns", 1);
        f("1h", NSECS_PER_HOUR);
        f("0.001h", 3_600_000_000);
        f("0.05h", 180_000_000_000);
        f("1.5d", 129_600_000_000_000);
        f("1.5w", 907_200_000_000_000);
        f("2.5y", 78_840_000_000_000_000);
        f(
            "1h5m35s",
            NSECS_PER_HOUR + 5 * NSECS_PER_MINUTE + 35 * NSECS_PER_SECOND,
        );
        f("1m5.123456789s", NSECS_PER_MINUTE + 5_123_456_789);

        // composite duration
        f("1h5m", NSECS_PER_HOUR + 5 * NSECS_PER_MINUTE);
        f("1.1h5m2.5s3_456ns", 4_262_500_003_456);

        // negative duration
        f(
            "-1h5m3s",
            -(NSECS_PER_HOUR + 5 * NSECS_PER_MINUTE + 3 * NSECS_PER_SECOND),
        );

        // max int duration
        f("9_223_372_036_854_775_807ns", i64::MAX);
        f("9223372036854775807ns", i64::MAX);
        f("-9223372036854775808ns", -i64::MAX);

        // too big value is clapped to 1<<63-1
        f("15_223_372_036_854_775_808ns", i64::MAX);
        f("-15_223_372_036_854_775_808ns", -i64::MAX);
    }

    #[test]
    fn test_try_parse_duration_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_duration(s).is_none(),
                "expecting error for parsing {s:?}"
            );
        }

        // empty string
        f("");

        // missing suffix
        f("2");
        f("2.5");

        // invalid string
        f("foobar");
        f("1foo");
        f("1soo");
        f("3.43e");
        f("3.43es");

        // superfluous space
        f(" 2s");
        f("2s ");
        f("2s 3ms");
    }

    #[test]
    fn test_marshal_duration_string() {
        fn f(nsecs: i64, result_expected: &str) {
            let mut result = Vec::new();
            marshal_duration_string(&mut result, nsecs);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f(0, "0");
        f(1, "1ns");
        f(-1, "-1ns");
        f(12345, "12µs345ns");
        f(123456789, "123ms456µs789ns");
        f(12345678901, "12.345678901s");
        f(1234567890143, "20m34.567890143s");
        f(1234567890123457, "2w6h56m7.890123457s");
    }

    #[test]
    fn test_try_parse_bytes_success() {
        fn f(s: &str, result_expected: i64) {
            let result = try_parse_bytes(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f("1_500", 1_500);

        f("2.5B", 2);

        f("1.5K", 1_500);
        f("1.5M", 1_500_000);
        f("1.5G", 1_500_000_000);
        f("1.5T", 1_500_000_000_000);

        f("1.5KB", 1_500);
        f("1.5MB", 1_500_000);
        f("1.5GB", 1_500_000_000);
        f("1.5TB", 1_500_000_000_000);

        f("1.5Ki", 3 * (1 << 10) / 2);
        f("1.5Mi", 3 * (1 << 20) / 2);
        f("1.5Gi", 3 * (1 << 30) / 2);
        f("1.5Ti", 3 * (1i64 << 40) / 2);

        f("1.5KiB", 3 * (1 << 10) / 2);
        f("1.5MiB", 3 * (1 << 20) / 2);
        f("1.5GiB", 3 * (1 << 30) / 2);
        f("1.5TiB", 3 * (1i64 << 40) / 2);

        f("1MiB500KiB200B", (1 << 20) + 500 * (1 << 10) + 200);

        // The maximum bytes value
        f("9_223_372_036_854_775_807", i64::MAX);
        f("9223372036854775807B", i64::MAX);
        f("-9223372036854775808B", -i64::MAX);

        // too big value is clapped to 1<<63-1
        f("15_223_372_036_854_775_808", i64::MAX);
        f("-15_223_372_036_854_775_808", -i64::MAX);
    }

    #[test]
    fn test_try_parse_bytes_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_bytes(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // empty string
        f("");

        // invalid number
        f("foobar");

        // invalid suffix
        f("123q");
        f("123qs");
        f("123qsb");
        f("123sqsb");
        f("123s5qsb");

        // invalid case for the suffix
        f("1b");

        f("1k");
        f("1m");
        f("1g");
        f("1t");

        f("1kb");
        f("1mb");
        f("1gb");
        f("1tb");

        f("1ki");
        f("1mi");
        f("1gi");
        f("1ti");

        f("1kib");
        f("1mib");
        f("1gib");
        f("1tib");

        f("1KIB");
        f("1MIB");
        f("1GIB");
        f("1TIB");

        // fractional number without suffix
        f("123.456");
    }

    fn float64_equal(a: f64, b: f64) -> bool {
        (a - b).abs() * a.max(b).abs() < 1e-15
    }

    #[test]
    fn test_try_parse_float64_success() {
        fn f(s: &str, result_expected: f64) {
            let result = try_parse_float64(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert!(
                float64_equal(result, result_expected),
                "unexpected value; got {result}; want {result_expected}"
            );
        }

        f("0", 0.0);
        f("1", 1.0);
        f("-1", -1.0);
        f("1234567890", 1234567890.0);
        f("1_234_567_890", 1234567890.0);
        f("-1.234_567", -1.234567);

        f("0.345", 0.345);
        f("-0.345", -0.345);
        f("1.0234", 1.0234);
        f("-12.0098", -12.0098);

        // The maximum integer
        f("9007199254740991", ((1u64 << 53) - 1) as f64);
        f("9_007_199_254_740_991", ((1u64 << 53) - 1) as f64);
        f("-9007199254740991", (-(1i64 << 53) + 1) as f64);

        // Too big integer (exceeds 2^53-1). It leads to precision loss.
        f("9_007_199_254_740_992", 9007199254740992.0);
        f("10_007_199_254_740_992", 10007199254740993.0);
        f("-9007199254740992", -9007199254740992.0);
    }

    #[test]
    fn test_try_parse_float64_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_float64(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // Empty value
        f("");

        // Plus in the value isn't allowed, since it cannot be converted back to the same string representation
        f("+123");

        // Dot at the beginning and the end of value isn't allowed, since it cannot converted back to the same string representation
        f(".123");
        f("123.");

        // Multiple dots aren't allowed
        f("123.434.55");

        // Invalid dots
        f("-.123");
        f(".");

        // Scientific notation isn't allowed, since it cannot be converted back to the same string representation
        f("12e5");

        // Minus in the middle of string isn't allowed
        f("12-5");

        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8361
        f("01");

        // NaN and Inf isn't supported
        f("NaN");
        f("nan");
        f("inf");
        f("-inf");
        f("+inf");
        f("Inf");
    }

    #[test]
    fn test_try_parse_float64_exact_success() {
        fn f(s: &str, result_expected: f64) {
            let result = try_parse_float64_exact(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert!(
                float64_equal(result, result_expected),
                "unexpected value; got {result}; want {result_expected}"
            );
        }

        f("0", 0.0);
        f("1", 1.0);
        f("-1", -1.0);
        f("1234567890", 1234567890.0);
        f("1_234_567_890", 1234567890.0);
        f("-1.234_567", -1.234567);

        f("0.345", 0.345);
        f("-0.345", -0.345);

        // The maximum integer
        f("9007199254740991", ((1u64 << 53) - 1) as f64);
        f("9_007_199_254_740_991", ((1u64 << 53) - 1) as f64);
        f("-9007199254740991", (-(1i64 << 53) + 1) as f64);
    }

    #[test]
    fn test_try_parse_float64_exact_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_float64_exact(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // Empty value
        f("");

        // Plus in the value isn't allowed, since it cannot be converted back to the same string representation
        f("+123");

        // Dot at the beginning and the end of value isn't allowed, since it cannot converted back to the same string representation
        f(".123");
        f("123.");

        // Multiple dots aren't allowed
        f("123.434.55");

        // Invalid dots
        f("-.123");
        f(".");

        // Scientific notation isn't allowed, since it cannot be converted back to the same string representation
        f("12e5");

        // Minus in the middle of string isn't allowed
        f("12-5");

        // Too big integer (exceeds 2^53-1)
        f("9_007_199_254_740_992");
        f("-9007199254740992");
    }

    #[test]
    fn test_marshal_float64_string() {
        fn f(v: f64, result_expected: &str) {
            let mut result = Vec::new();
            marshal_float64_string(&mut result, v);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f(0.0, "0");
        f(1234.0, "1234");
        f(-12345678.0, "-12345678");
        f(1.234, "1.234");
        f(-1.234567, "-1.234567");
    }

    #[test]
    fn test_try_parse_uint64_success() {
        fn f(s: &str, result_expected: u64) {
            let result = try_parse_uint64(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert_eq!(
                result, result_expected,
                "unexpected value; got {result}; want {result_expected}"
            );
        }

        f("0", 0);
        f("123", 123);
        f("123456", 123456);
        f("123456789", 123456789);
        f("123456789012", 123456789012);
        f("123456789012345", 123456789012345);
        f("123456789012345678", 123456789012345678);
        f("12345678901234567890", 12345678901234567890);
        f("12_345_678_901_234_567_890", 12345678901234567890);

        // the maximum possible value
        f("18446744073709551615", 18446744073709551615);
    }

    #[test]
    fn test_try_parse_uint64_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_uint64(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // empty value
        f("");

        // too big value
        f("18446744073709551616");

        // invalid value
        f("foo");
        f("1.2");
        f("1e3");

        // uint with leading zeros shouldn't be parsed as uint
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8361
        f("0123");
    }

    #[test]
    fn test_try_parse_int64_success() {
        fn f(s: &str, result_expected: i64) {
            let result = try_parse_int64(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert_eq!(
                result, result_expected,
                "unexpected value; got {result}; want {result_expected}"
            );
        }

        f("0", 0);
        f("-0", 0);
        f("123", 123);
        f("-123", -123);
        f("1345678901234567890", 1345678901234567890);
        f("-1_345_678_901_234_567_890", -1345678901234567890);

        // the maximum possible value
        f("9223372036854775807", 9223372036854775807);

        // the minimum possible value
        f("-9223372036854775808", i64::MIN);
    }

    #[test]
    fn test_try_parse_int64_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_int64(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // empty value
        f("");

        // too big value
        f("9223372036854775808");

        // too small value
        f("-9223372036854775809");

        // invalid value
        f("foo");
        f("1.2");
        f("1e3");

        // int with leading zeros shouldn't be parsed as uint
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8361
        f("-0123");
    }

    #[test]
    fn test_marshal_uint8_string() {
        fn f(n: u8, result_expected: &str) {
            let mut result = Vec::new();
            marshal_uint8_string(&mut result, n);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        for i in 0..=255u8 {
            let result_expected = i.to_string();
            f(i, &result_expected);
        }

        // the maximum possible value
        f(u8::MAX, "255");
    }

    #[test]
    fn test_marshal_uint16_string() {
        fn f(n: u16, result_expected: &str) {
            let mut result = Vec::new();
            marshal_uint16_string(&mut result, n);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f(0, "0");
        f(1, "1");
        f(10, "10");
        f(12, "12");
        f(120, "120");
        f(1203, "1203");
        f(12345, "12345");

        // the maximum possible value
        f(u16::MAX, "65535");
    }

    #[test]
    fn test_marshal_uint32_string() {
        fn f(n: u32, result_expected: &str) {
            let mut result = Vec::new();
            marshal_uint32_string(&mut result, n);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f(0, "0");
        f(1, "1");
        f(10, "10");
        f(12, "12");
        f(120, "120");
        f(1203, "1203");
        f(12034, "12034");
        f(123456, "123456");
        f(1234567, "1234567");
        f(12345678, "12345678");
        f(123456789, "123456789");
        f(1234567890, "1234567890");

        // the maximum possible value
        f(u32::MAX, "4294967295");
    }

    #[test]
    fn test_marshal_uint64_string() {
        fn f(n: u64, result_expected: &str) {
            let mut result = Vec::new();
            marshal_uint64_string(&mut result, n);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f(0, "0");
        f(123456, "123456");

        // the maximum possible value
        f(u64::MAX, "18446744073709551615");
    }

    #[test]
    fn test_try_parse_ipv4_mask_success() {
        fn f(s: &str, result_expected: u64) {
            let result = try_parse_ipv4_mask(s).unwrap_or_else(|| panic!("cannot parse {s:?}"));
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f("/0", 1 << 32);
        f("/1", 1 << 31);
        f("/8", 1 << 24);
        f("/24", 1 << 8);
        f("/32", 1);
    }

    #[test]
    fn test_try_parse_ipv4_mask_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_ipv4_mask(s).is_none(),
                "expecting error when parsing {s:?}"
            );
        }

        // Empty mask
        f("");

        // Invalid prefix
        f("foo");

        // Non-numeric mask
        f("/foo");

        // Too big mask
        f("/33");

        // Negative mask
        f("/-1");
    }
}
