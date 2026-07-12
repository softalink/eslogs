//! Port of Softalink LLC `lib/timeutil`.
//!
//! Durations and timestamps are `i64` values (Go `time.Duration` nanoseconds
//! and unix nanoseconds respectively), since Go durations can be negative
//! while `std::time::Duration` cannot.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_VALID_SECOND: i64 = i64::MAX / 1_000_000_000;
const MAX_VALID_MILLI: i64 = i64::MAX / 1_000_000;
const MAX_VALID_MICRO: i64 = i64::MAX / 1_000;
const MIN_VALID_SECOND: i64 = i64::MIN / 1_000_000_000;
const MIN_VALID_MILLI: i64 = i64::MIN / 1_000_000;
const MIN_VALID_MICRO: i64 = i64::MIN / 1_000;

const MIN_DURATION: i64 = MIN_VALID_MILLI * 1_000_000;
const MAX_DURATION: i64 = MAX_VALID_MILLI * 1_000_000;

/// ParseDuration parses duration string in Prometheus format.
///
/// Returns the duration in nanoseconds (Go `time.Duration`).
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let ms = duration_value(s, 0)?;
    if !(MIN_VALID_MILLI..=MAX_VALID_MILLI).contains(&ms) {
        return Err(format!(
            "duration {s:?} must be in the range [{}, {}]",
            format_go_duration(MIN_DURATION),
            format_go_duration(MAX_DURATION)
        ));
    }
    Ok(ms * 1_000_000)
}

// PORT NOTE: duration_value, parse_single_duration and scan_single_duration
// are ported from github.com/VictoriaMetrics/metricsql (lexer.go), since Go's
// timeutil.ParseDuration delegates to metricsql.DurationValue and the
// metricsql package is not ported.

/// Returns the duration in milliseconds for the given s and the given step.
///
/// Duration in s may be combined, i.e. 2h5m, -2h5m or 2h-5m.
///
/// The returned duration value can be negative.
fn duration_value(s: &str, step: i64) -> Result<i64, String> {
    if s.is_empty() {
        return Err("duration cannot be empty".to_string());
    }
    let last_char = *s.as_bytes().last().unwrap();
    if last_char.is_ascii_digit() || last_char == b'.' {
        // Try parsing floating-point duration
        if let Ok(d) = s.parse::<f64>() {
            // Convert the duration to milliseconds.
            return Ok((d * 1000.0) as i64);
        }
    }
    let mut is_minus = false;
    let mut d = 0f64;
    let mut s = s;
    while !s.is_empty() {
        let n = scan_single_duration(s, true);
        if n <= 0 {
            return Err(format!("cannot parse duration {s:?}"));
        }
        let n = n as usize;
        let ds = &s[..n];
        s = &s[n..];
        let mut d_local = parse_single_duration(ds, step)?;
        if is_minus && d_local > 0.0 {
            d_local = -d_local;
        }
        d += d_local;
        if d_local < 0.0 {
            is_minus = true;
        }
    }
    if d > i64::MAX as f64 {
        // Truncate too big durations.
        return Ok(i64::MAX);
    }
    if d < i64::MIN as f64 {
        // Truncate too small durations.
        return Ok(i64::MIN);
    }
    Ok(d as i64)
}

fn parse_single_duration(s: &str, step: i64) -> Result<f64, String> {
    if s == "$__interval" {
        return Ok(step as f64);
    }
    let lower = s.to_lowercase();
    let s = lower.as_str();
    let mut num_part = &s[..s.len() - 1];
    // Strip trailing m if the duration is in ms
    num_part = num_part.strip_suffix('m').unwrap_or(num_part);
    let f: f64 = num_part
        .parse()
        .map_err(|err| format!("cannot parse duration {s:?}: {err}"))?;
    let mp: f64 = match &s[num_part.len()..] {
        "ms" => 1.0,
        "s" => 1000.0,
        "m" => 60.0 * 1000.0,
        "h" => 60.0 * 60.0 * 1000.0,
        "d" => 24.0 * 60.0 * 60.0 * 1000.0,
        "w" => 7.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        "y" => 365.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        "i" => step as f64,
        _ => return Err(format!("invalid duration suffix in {s:?}")),
    };
    Ok(mp * f)
}

fn scan_single_duration(s: &str, can_be_negative: bool) -> isize {
    if s.is_empty() {
        return -1;
    }
    let b = s.as_bytes();
    let mut i = 0usize;
    if b[0] == b'-' && can_be_negative {
        i += 1;
    }
    if &s[i..] == "$__interval" {
        return (i + "$__interval".len()) as isize;
    }
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 || i == b.len() {
        return -1;
    }
    if b[i] == b'.' {
        let j = i;
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == j || i == b.len() {
            return -1;
        }
    }
    match b[i].to_ascii_lowercase() {
        b'm' => {
            if i + 1 < b.len() {
                match b[i + 1].to_ascii_lowercase() {
                    b's' => {
                        // duration in ms
                        return (i + 2) as isize;
                    }
                    b'i' | b'b' => {
                        // This is not a duration, but Mi or MB suffix.
                        return -1;
                    }
                    _ => {}
                }
            }
            // Allow small m for duration in minutes.
            // Big M means 1e6.
            if b[i] == b'm' { (i + 1) as isize } else { -1 }
        }
        b's' | b'h' | b'd' | b'w' | b'y' | b'i' => (i + 1) as isize,
        _ => -1,
    }
}

// Port of Go time.Duration.String(), used in the parse_duration error message
// so the wording matches Go exactly.
fn format_go_duration(d: i64) -> String {
    let neg = d < 0;
    let mut u = d.unsigned_abs();
    let mut s;
    if u < 1_000_000_000 {
        if u == 0 {
            return "0s".to_string();
        }
        let (prec, unit) = if u < 1_000 {
            (0, "ns")
        } else if u < 1_000_000 {
            (3, "µs")
        } else {
            (6, "ms")
        };
        let (frac, v) = fmt_frac(u, prec);
        s = format!("{v}{frac}{unit}");
    } else {
        let (frac, v) = fmt_frac(u, 9);
        u = v;
        s = format!("{}{frac}s", u % 60);
        u /= 60;
        if u > 0 {
            s = format!("{}m{s}", u % 60);
            u /= 60;
            if u > 0 {
                s = format!("{u}h{s}");
            }
        }
    }
    if neg {
        s = format!("-{s}");
    }
    s
}

// Formats the fraction of v/10**prec (e.g., ".12345") omitting trailing
// zeros. Returns the formatted fraction and v/10**prec.
fn fmt_frac(mut v: u64, prec: u32) -> (String, u64) {
    let mut print = false;
    let mut digits: Vec<u8> = Vec::new();
    for _ in 0..prec {
        let digit = (v % 10) as u8;
        print = print || digit != 0;
        if print {
            digits.push(b'0' + digit);
        }
        v /= 10;
    }
    let mut s = String::new();
    if print {
        s.push('.');
        for &d in digits.iter().rev() {
            s.push(d as char);
        }
    }
    (s, v)
}

/// ParseTimeMsec parses time s in different formats.
///
/// See <https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats>
///
/// It returns unix timestamp in milliseconds.
pub fn parse_time_msec(s: &str) -> Result<i64, String> {
    let current_timestamp = current_unix_nanos();
    let nsecs = parse_time_at(s, current_timestamp)?;
    Ok(((nsecs as f64) / 1e6).round() as i64)
}

fn current_unix_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(_) => 0,
    }
}

/// ParseTimeAt parses time s in different formats, assuming the given current_timestamp.
///
/// See <https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats>
///
/// If s doesn't contain timezone information, then the local timezone is used.
///
/// It returns unix timestamp in nanoseconds.
pub fn parse_time_at(s: &str, current_timestamp: i64) -> Result<i64, String> {
    if s == "now" {
        return Ok(current_timestamp);
    }
    let s_orig = s;
    let mut tz_offset: i64 = 0;
    let mut s = s;
    if s_orig.len() > 6 {
        // Try parsing timezone offset
        let tzb = &s_orig.as_bytes()[s_orig.len() - 6..];
        if (tzb[0] == b'-' || tzb[0] == b'+') && tzb[3] == b':' {
            // Slicing is safe: tzb[0] is ASCII, so len-6 is a char boundary.
            let tz = &s_orig[s_orig.len() - 6..];
            let is_plus = tzb[0] == b'+';
            let hour = parse_tz_part(&tzb[1..3])
                .map_err(|err| format!("cannot parse hour from timezone offset {tz:?}: {err}"))?;
            let minute = parse_tz_part(&tzb[4..6])
                .map_err(|err| format!("cannot parse minute from timezone offset {tz:?}: {err}"))?;
            tz_offset = (hour * 3600 + minute * 60) as i64 * 1_000_000_000;
            if is_plus {
                tz_offset = -tz_offset;
            }
            s = &s_orig[..s_orig.len() - 6];
        } else if !s.ends_with('Z') {
            tz_offset = -get_local_timezone_offset_nsecs();
        } else {
            s = &s[..s.len() - 1];
        }
    }
    let s = s.strip_suffix('Z').unwrap_or(s);
    let b = s.as_bytes();
    if (!b.is_empty() && (b[b.len() - 1] > b'9' || b[0] == b'-')) || s.starts_with("now") {
        // Parse duration relative to the current time
        let s = s.strip_prefix("now").unwrap_or(s);
        let mut d = parse_duration(s)?;
        if d < 0 {
            d = -d;
        }
        return Ok(sub_int64_no_overflow(current_timestamp, d));
    }
    if s.len() == 4 {
        // Parse YYYY
        return parse_time_with_level(s, 1, tz_offset);
    }
    if !s_orig.contains('-') {
        return match try_parse_unix_timestamp(s_orig) {
            Some(nsec) => Ok(nsec),
            None => Err(format!("cannot parse numeric timestamp {s_orig:?}")),
        };
    }
    match s.len() {
        // Parse YYYY-MM
        7 => parse_time_with_level(s, 2, tz_offset),
        // Parse YYYY-MM-DD
        10 => parse_time_with_level(s, 3, tz_offset),
        // Parse YYYY-MM-DDTHH
        13 => parse_time_with_level(s, 4, tz_offset),
        // Parse YYYY-MM-DDTHH:MM
        16 => parse_time_with_level(s, 5, tz_offset),
        // Parse YYYY-MM-DDTHH:MM:SS
        19 => parse_time_with_level(s, 6, tz_offset),
        // Parse RFC3339
        _ => parse_rfc3339(s_orig),
    }
}

fn parse_tz_part(b: &[u8]) -> Result<u64, String> {
    // Go uses strconv.ParseUint, which accepts decimal digits only.
    if b.iter().any(|c| !c.is_ascii_digit()) {
        return Err("invalid syntax".to_string());
    }
    std::str::from_utf8(b)
        .map_err(|_| "invalid syntax".to_string())?
        .parse::<u64>()
        .map_err(|err| err.to_string())
}

fn parse_time_with_level(value: &str, level: u32, tz_offset_nsec: i64) -> Result<i64, String> {
    let c = parse_civil(value, level)?;
    let nsec = civil_unix_nanos(c, 0, 0);
    Ok(sub_int64_no_overflow(nsec, -tz_offset_nsec))
}

// PORT NOTE: Go delegates layout parsing to time.Parse and date math to the
// time package. The port hand-rolls the fixed layouts used by ParseTimeAt and
// converts civil dates via the standard days-from-civil algorithm. Unix
// nanoseconds are computed with wrapping arithmetic, matching Go's
// Time.UnixNano behavior for instants outside the representable range.

#[derive(Clone, Copy)]
struct Civil {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
}

// Parses a prefix of "YYYY-MM-DDTHH:MM:SS" with the given number of
// components (1=YYYY .. 6=full) and requires the whole value to be consumed.
fn parse_civil(s: &str, level: u32) -> Result<Civil, String> {
    let b = s.as_bytes();
    let parse_err = || format!("cannot parse time {s:?}");
    let num = |pos: usize, n: usize| -> Option<i64> {
        let sub = b.get(pos..pos + n)?;
        let mut v: i64 = 0;
        for &c in sub {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + i64::from(c - b'0');
        }
        Some(v)
    };
    let sep = |pos: usize, ch: u8| -> bool { b.get(pos) == Some(&ch) };

    let mut c = Civil {
        year: 0,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
    };
    c.year = num(0, 4).ok_or_else(parse_err)?;
    let mut pos = 4;
    if level >= 2 {
        if !sep(pos, b'-') {
            return Err(parse_err());
        }
        c.month = num(pos + 1, 2).ok_or_else(parse_err)?;
        if !(1..=12).contains(&c.month) {
            return Err(format!("cannot parse time {s:?}: month out of range"));
        }
        pos += 3;
    }
    if level >= 3 {
        if !sep(pos, b'-') {
            return Err(parse_err());
        }
        c.day = num(pos + 1, 2).ok_or_else(parse_err)?;
        if c.day < 1 || c.day > days_in_month(c.year, c.month) {
            return Err(format!("cannot parse time {s:?}: day out of range"));
        }
        pos += 3;
    }
    if level >= 4 {
        if !sep(pos, b'T') {
            return Err(parse_err());
        }
        c.hour = num(pos + 1, 2).ok_or_else(parse_err)?;
        if c.hour > 23 {
            return Err(format!("cannot parse time {s:?}: hour out of range"));
        }
        pos += 3;
    }
    if level >= 5 {
        if !sep(pos, b':') {
            return Err(parse_err());
        }
        c.minute = num(pos + 1, 2).ok_or_else(parse_err)?;
        if c.minute > 59 {
            return Err(format!("cannot parse time {s:?}: minute out of range"));
        }
        pos += 3;
    }
    if level >= 6 {
        if !sep(pos, b':') {
            return Err(parse_err());
        }
        c.second = num(pos + 1, 2).ok_or_else(parse_err)?;
        if c.second > 59 {
            return Err(format!("cannot parse time {s:?}: second out of range"));
        }
        pos += 3;
    }
    if pos != b.len() {
        return Err(parse_err());
    }
    Ok(c)
}

fn is_leap_year(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
    }
}

// Days since the unix epoch for the given civil date (Howard Hinnant's
// days_from_civil algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

// Unix nanoseconds for the given civil time at the given UTC offset,
// wrapping on overflow like Go's Time.UnixNano.
fn civil_unix_nanos(c: Civil, nsec: i64, offset_secs: i64) -> i64 {
    let days = days_from_civil(c.year, c.month, c.day);
    let secs = days * 86400 + c.hour * 3600 + c.minute * 60 + c.second - offset_secs;
    secs.wrapping_mul(1_000_000_000).wrapping_add(nsec)
}

// Parses RFC3339: "YYYY-MM-DDTHH:MM:SS(.fraction)?(Z|±HH:MM)".
fn parse_rfc3339(s: &str) -> Result<i64, String> {
    let err = || format!("cannot parse time {s:?} as RFC3339");
    let b = s.as_bytes();
    if b.len() < 20 {
        return Err(err());
    }
    let head = std::str::from_utf8(&b[..19]).map_err(|_| err())?;
    let c = parse_civil(head, 6)?;
    let mut pos = 19usize;
    let mut nsec: i64 = 0;
    if b[pos] == b'.' {
        pos += 1;
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            pos += 1;
        }
        let ndigits = pos - start;
        if ndigits == 0 || ndigits > 9 {
            return Err(err());
        }
        for &ch in &b[start..pos] {
            nsec = nsec * 10 + i64::from(ch - b'0');
        }
        nsec *= 10i64.pow(9 - ndigits as u32);
    }
    if pos >= b.len() {
        return Err(err());
    }
    let mut offset_secs: i64 = 0;
    match b[pos] {
        b'Z' => pos += 1,
        sign @ (b'+' | b'-') => {
            if pos + 6 != b.len() || b[pos + 3] != b':' {
                return Err(err());
            }
            let hh = parse_tz_part(&b[pos + 1..pos + 3]).map_err(|_| err())? as i64;
            let mm = parse_tz_part(&b[pos + 4..pos + 6]).map_err(|_| err())? as i64;
            if hh > 23 || mm > 59 {
                return Err(err());
            }
            offset_secs = hh * 3600 + mm * 60;
            if sign == b'-' {
                offset_secs = -offset_secs;
            }
            pos += 6;
        }
        _ => return Err(err()),
    }
    if pos != b.len() {
        return Err(err());
    }
    Ok(civil_unix_nanos(c, nsec, offset_secs))
}

fn sub_int64_no_overflow(a: i64, b: i64) -> i64 {
    if b >= 0 {
        if a < i64::MIN + b {
            return i64::MIN;
        }
        return a - b;
    }

    if a > i64::MAX + b {
        return i64::MAX;
    }
    a - b
}

/// TryParseUnixTimestamp parses s as unix timestamp in seconds, milliseconds,
/// microseconds or nanoseconds and returns the parsed timestamp in nanoseconds.
///
/// The supported formats for s:
///
/// - Integer. For example, 1234567890
/// - Fractional. For example, 1234567890.123
/// - Scientific. For example, 1.23456789e9
pub fn try_parse_unix_timestamp(s: &str) -> Option<i64> {
    if let Some(exp_idx) = get_exp_index(s) {
        // The timestamp is a scientific number such as 1.234e5
        let decimal_exp = try_parse_int64(&s[exp_idx + 1..])?;
        let n = try_parse_scientific_number_for_unix_timestamp(&s[..exp_idx], decimal_exp)?;
        return Some(get_unix_timestamp_nanoseconds(n));
    }

    let Some(dot_idx) = s.find('.') else {
        // The timestamp is integer.
        let n = try_parse_int64(s)?;
        return Some(get_unix_timestamp_nanoseconds(n));
    };

    // The timestamp is fractional.
    let int_str = &s[..dot_idx];
    let frac_str = &s[dot_idx + 1..];
    let mut n = try_parse_fractional_number_for_unix_timestamp(int_str, frac_str)?;

    // Adjust the n to multiples of thousands, since this is expected by
    // get_unix_timestamp_nanoseconds.
    let mut decimal_exp = frac_str.len();
    while !decimal_exp.is_multiple_of(3) {
        if !(i64::MIN / 10..=i64::MAX / 10).contains(&n) {
            return None;
        }
        n *= 10;
        decimal_exp += 1;
    }

    Some(get_unix_timestamp_nanoseconds(n))
}

fn get_exp_index(s: &str) -> Option<usize> {
    if let Some(n) = s.find('e') {
        return Some(n);
    }
    s.find('E')
}

fn try_parse_scientific_number_for_unix_timestamp(s: &str, decimal_exp: i64) -> Option<i64> {
    let Some(dot_idx) = s.find('.') else {
        let n = try_parse_int64(s)?;
        return multiply_by_decimal_exp(n, decimal_exp);
    };

    let int_str = &s[..dot_idx];
    let frac_str = &s[dot_idx + 1..];
    if decimal_exp < frac_str.len() as i64 {
        return None;
    }
    let n = try_parse_fractional_number_for_unix_timestamp(int_str, frac_str)?;
    let decimal_exp = decimal_exp - frac_str.len() as i64;
    multiply_by_decimal_exp(n, decimal_exp)
}

fn try_parse_fractional_number_for_unix_timestamp(int_str: &str, frac_str: &str) -> Option<i64> {
    let n = try_parse_int64(int_str)?;

    let decimal_exp = frac_str.len() as i64;
    let mut num = multiply_by_decimal_exp(n, decimal_exp)?;

    let frac = try_parse_int64(frac_str)?;

    if num >= 0 {
        if num > i64::MAX - frac {
            return None;
        }
        num += frac;
    } else {
        if num < i64::MIN + frac {
            return None;
        }
        num -= frac;
    }

    Some(num)
}

fn multiply_by_decimal_exp(n: i64, decimal_exp: i64) -> Option<i64> {
    if decimal_exp < 0 {
        return None;
    }
    if decimal_exp >= DECIMAL_MULTIPLIERS.len() as i64 {
        return None;
    }
    if decimal_exp == 0 {
        return Some(n);
    }

    let m = DECIMAL_MULTIPLIERS[decimal_exp as usize];

    if n >= 0 && n > i64::MAX / m || n < 0 && n < i64::MIN / m {
        return None;
    }

    Some(n * m)
}

const DECIMAL_MULTIPLIERS: [i64; 19] = [
    0,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

fn get_unix_timestamp_nanoseconds(n: i64) -> i64 {
    if (MIN_VALID_SECOND..=MAX_VALID_SECOND).contains(&n) {
        // The timestamp is in seconds.
        return n * 1_000_000_000;
    }
    if (MIN_VALID_MILLI..=MAX_VALID_MILLI).contains(&n) {
        // The timestamp is in milliseconds.
        return n * 1_000_000;
    }
    if (MIN_VALID_MICRO..=MAX_VALID_MICRO).contains(&n) {
        // The timestamp is in microseconds.
        return n * 1_000;
    }
    // The timestamp is in nanoseconds
    n
}

fn try_parse_int64(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

/// AddJitterToDuration adds up to 10% random jitter to d (in nanoseconds) and
/// returns the resulting duration.
///
/// The maximum jitter is limited by 10 seconds.
pub fn add_jitter_to_duration(d: i64) -> i64 {
    let dv = (d / 10).min(10_000_000_000);
    let p = f64::from(fastrand_u32()) / (1u64 << 32) as f64;
    d + (p * dv as f64) as i64
}

// PORT NOTE: Go uses github.com/valyala/fastrand (pooled xorshift RNGs).
// A thread-local xorshift32 seeded from the system clock keeps esl-common
// dependency-free while providing the same distribution.
fn fastrand_u32() -> u32 {
    use std::cell::Cell;
    thread_local! {
        static RNG_STATE: Cell<u32> = const { Cell::new(0) };
    }
    RNG_STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            x = now.subsec_nanos() ^ (now.as_secs() as u32);
            if x == 0 {
                x = now.subsec_nanos() | 1;
            }
        }
        // See https://en.wikipedia.org/wiki/Xorshift
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        state.set(x);
        x
    })
}

/// GetLocalTimezoneOffsetNsecs returns local timezone offset in nanoseconds.
pub fn get_local_timezone_offset_nsecs() -> i64 {
    local_timezone_offset_nsecs().load(Ordering::SeqCst)
}

// PORT NOTE: Go initializes the cached offset and starts the 5-second updater
// goroutine (needed since the offset may change over the year due to DST) in
// the package init(); Rust has no package init, so both happen lazily on the
// first call to get_local_timezone_offset_nsecs().
static LOCAL_TIMEZONE_OFFSET_NSECS: OnceLock<AtomicI64> = OnceLock::new();

fn local_timezone_offset_nsecs() -> &'static AtomicI64 {
    LOCAL_TIMEZONE_OFFSET_NSECS.get_or_init(|| {
        thread::Builder::new()
            .name("timeutil-timezone".to_string())
            .spawn(|| {
                loop {
                    thread::sleep(Duration::from_secs(5));
                    if let Some(v) = LOCAL_TIMEZONE_OFFSET_NSECS.get() {
                        v.store(local_utc_offset_nsecs(), Ordering::SeqCst);
                    }
                }
            })
            .expect("FATAL: cannot spawn timezone updater thread");
        AtomicI64::new(local_utc_offset_nsecs())
    })
}

#[cfg(unix)]
fn local_utc_offset_nsecs() -> i64 {
    // SAFETY: `time` with a null pointer just returns the current time;
    // `localtime_r` fills the zero-initialized `tm` we own and is the
    // thread-safe variant of localtime.
    unsafe {
        let t: libc::time_t = libc::time(std::ptr::null_mut());
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&t, &mut tm).is_null() {
            return 0;
        }
        (tm.tm_gmtoff as i64) * 1_000_000_000
    }
}

// PORT NOTE: Go gets the offset from time.Now().Zone(), backed by the OS.
// windows-sys is compiled here without the Win32_System_Time feature, so
// instead of GetTimeZoneInformation the offset is derived by comparing
// GetLocalTime with GetSystemTime, rounded to the nearest minute to absorb
// the time elapsed between the two calls.
#[cfg(windows)]
fn local_utc_offset_nsecs() -> i64 {
    use windows_sys::Win32::Foundation::SYSTEMTIME;
    use windows_sys::Win32::System::SystemInformation::{GetLocalTime, GetSystemTime};

    fn systemtime_secs(st: &SYSTEMTIME) -> i64 {
        let days = days_from_civil(
            i64::from(st.wYear),
            i64::from(st.wMonth),
            i64::from(st.wDay),
        );
        days * 86400
            + i64::from(st.wHour) * 3600
            + i64::from(st.wMinute) * 60
            + i64::from(st.wSecond)
    }

    // SAFETY: both functions only write into the zero-initialized SYSTEMTIME
    // structs we own.
    unsafe {
        let mut local: SYSTEMTIME = std::mem::zeroed();
        let mut utc: SYSTEMTIME = std::mem::zeroed();
        GetLocalTime(&mut local);
        GetSystemTime(&mut utc);
        let diff_secs = systemtime_secs(&local) - systemtime_secs(&utc);
        let diff_mins = (diff_secs as f64 / 60.0).round() as i64;
        diff_mins * 60 * 1_000_000_000
    }
}

/// BackoffTimer implements an exponential backoff timer with jitter.
///
/// Delays are `i64` nanoseconds (Go `time.Duration`).
pub struct BackoffTimer {
    min: i64,
    max: i64,
    current: i64,
}

impl BackoffTimer {
    /// Returns a new BackoffTimer initialized with the given min_delay and max_delay.
    pub fn new(min_delay: i64, max_delay: i64) -> BackoffTimer {
        let min_delay = if max_delay < min_delay {
            max_delay
        } else {
            min_delay
        };
        BackoffTimer {
            min: min_delay,
            max: max_delay,
            current: min_delay,
        }
    }

    /// Wait sleeps for the current delay with jitter, doubling the delay for
    /// the next wait. Use current_delay to get the current backoff duration.
    ///
    /// Wait returns false if stop_ch is closed (or receives a value).
    // PORT NOTE: Go selects on stopCh vs a pooled timer channel from
    // lib/timerpool. std Rust has no channel select; Receiver::recv_timeout
    // provides identical semantics without a timer, so timerpool is unused.
    pub fn wait(&mut self, stop_ch: &Receiver<()>) -> bool {
        let v = add_jitter_to_duration(self.current);
        self.current = self.current.saturating_mul(2);
        if self.current > self.max {
            self.current = self.max;
        }

        matches!(
            stop_ch.recv_timeout(Duration::from_nanos(v.max(0) as u64)),
            Err(RecvTimeoutError::Timeout)
        )
    }

    /// CurrentDelay returns the current backoff duration.
    pub fn current_delay(&self) -> i64 {
        self.current
    }

    /// SetDelay overrides the current delay. Useful for respecting Retry-After headers.
    pub fn set_delay(&mut self, d: i64) {
        let mut d = d;
        if d < self.min {
            d = self.min;
        }
        if d > self.max {
            d = self.max;
        }
        self.current = d;
    }

    /// Reset sets the backoff delay to its minimum.
    pub fn reset(&mut self) {
        self.current = self.min;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000_000;
    const MINUTE: i64 = 60 * SECOND;
    const HOUR: i64 = 60 * MINUTE;

    #[test]
    fn test_parse_duration() {
        fn f(s: &str, result_expected: i64) {
            let result = parse_duration(s).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f("0", 0);
        f("1s", SECOND);
        f("1m", MINUTE);
        f("1h", HOUR);
        f("1d", HOUR * 24);
        f("1w", HOUR * 24 * 7);
        f("1m30s", MINUTE + SECOND * 30);
        f("-1m30s", -(MINUTE + SECOND * 30));
        f("1d-4h", HOUR * 20);
    }

    #[test]
    fn test_parse_duration_limits() {
        fn f(s: &str, want: i64) {
            let got = parse_duration(s).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(got, want, "unexpected result: got {got}, want {want}");
        }

        f(&format!("{MIN_VALID_MILLI}ms"), MIN_DURATION);
        f(&format!("{MAX_VALID_MILLI}ms"), MAX_DURATION);

        f(&format!("{MIN_VALID_SECOND}s"), MIN_VALID_SECOND * SECOND);
        f(&format!("{MAX_VALID_SECOND}s"), MAX_VALID_SECOND * SECOND);

        // When no unit is specified, seconds are assumed.
        f(&format!("{MIN_VALID_SECOND}"), MIN_VALID_SECOND * SECOND);
        f(&format!("{MAX_VALID_SECOND}"), MAX_VALID_SECOND * SECOND);

        let min_valid_minute = MIN_VALID_SECOND / 60;
        let max_valid_minute = MAX_VALID_SECOND / 60;
        f(&format!("{min_valid_minute}m"), min_valid_minute * MINUTE);
        f(&format!("{max_valid_minute}m"), max_valid_minute * MINUTE);

        let min_valid_hour = min_valid_minute / 60;
        let max_valid_hour = max_valid_minute / 60;
        f(&format!("{min_valid_hour}h"), min_valid_hour * HOUR);
        f(&format!("{max_valid_hour}h"), max_valid_hour * HOUR);

        let min_valid_day = min_valid_hour / 24;
        let max_valid_day = max_valid_hour / 24;
        f(&format!("{min_valid_day}d"), min_valid_day * 24 * HOUR);
        f(&format!("{max_valid_day}d"), max_valid_day * 24 * HOUR);

        let min_valid_week = min_valid_day / 7;
        let max_valid_week = max_valid_day / 7;
        f(
            &format!("{min_valid_week}w"),
            min_valid_week * 7 * 24 * HOUR,
        );
        f(
            &format!("{max_valid_week}w"),
            max_valid_week * 7 * 24 * HOUR,
        );

        let min_valid_year = min_valid_day / 365;
        let max_valid_year = max_valid_day / 365;
        f(
            &format!("{min_valid_year}y"),
            min_valid_year * 365 * 24 * HOUR,
        );
        f(
            &format!("{max_valid_year}y"),
            max_valid_year * 365 * 24 * HOUR,
        );
    }

    #[test]
    fn test_parse_duration_outside_limits() {
        fn f(s: &str) {
            if let Ok(got) = parse_duration(s) {
                panic!("ParseDuration({s}) unexpected result: got {got}, want error");
            }
        }

        f(&format!("{}ms", MIN_VALID_MILLI - 1));
        f(&format!("{}ms", MAX_VALID_MILLI + 1));

        f(&format!("{}s", MIN_VALID_SECOND - 1));
        f(&format!("{}s", MAX_VALID_SECOND + 1));

        let min_valid_minute = MIN_VALID_SECOND / 60 - 1;
        f(&format!("{min_valid_minute}m"));
        let max_valid_minute = MAX_VALID_SECOND / 60 + 1;
        f(&format!("{max_valid_minute}m"));

        let min_valid_hour = min_valid_minute / 60 - 1;
        f(&format!("{min_valid_hour}h"));
        let max_valid_hour = max_valid_minute / 60 + 2;
        f(&format!("{max_valid_hour}h"));

        let min_valid_day = min_valid_hour / 24 - 1;
        f(&format!("{min_valid_day}d"));
        let max_valid_day = max_valid_hour / 24 + 1;
        f(&format!("{max_valid_day}d"));

        let min_valid_week = min_valid_day / 7 - 1;
        f(&format!("{min_valid_week}w"));
        let max_valid_week = max_valid_day / 7 + 1;
        f(&format!("{max_valid_week}w"));

        let min_valid_year = min_valid_day / 365 - 1;
        f(&format!("{min_valid_year}y"));
        let max_valid_year = max_valid_day / 365 + 1;
        f(&format!("{max_valid_year}y"));
    }

    #[test]
    fn test_add_jitter_to_duration() {
        fn f(d: i64) {
            let result = add_jitter_to_duration(d);
            assert!(result >= d, "unexpected negative jitter");
            let variance = (result - d) as f64 / d as f64;
            assert!(
                variance <= 0.1,
                "too big variance={variance:.2} for result={result}, d={d}; mustn't exceed 0.1"
            );
        }

        f(1);
        f(1_000);
        f(1_000_000);
        f(SECOND);
        f(HOUR);
        f(24 * HOUR);
    }

    #[test]
    fn test_try_parse_unix_timestamp_success() {
        fn f(s: &str, timestamp_expected: i64) {
            let timestamp = try_parse_unix_timestamp(s)
                .unwrap_or_else(|| panic!("cannot parse timestamp {s:?}"));
            assert_eq!(
                timestamp, timestamp_expected,
                "unexpected timestamp returned from TryParseUnixTimestamp({s:?}); got {timestamp}; want {timestamp_expected}"
            );
        }

        f("0", 0);

        // nanoseconds
        f("-1234567890123456789", -1234567890123456789);
        f("1234567890123456789", 1234567890123456789);
        f("1234567890123456.789", 1234567890123456789);

        // microseconds
        f("-1234567890123456", -1234567890123456000);
        f("1234567890123456", 1234567890123456000);
        f("1234567890123456.789", 1234567890123456789);

        // milliseconds
        f("-1234567890123", -1234567890123000000);
        f("1234567890123", 1234567890123000000);
        f("1234567890123.456", 1234567890123456000);

        // seconds
        f("-1234567890", -1234567890000000000);
        f("1234567890", 1234567890000000000);
        f("1234567890.123456789", 1234567890123456789);
        f("1234567890.12345678", 1234567890123456780);
        f("1234567890.1234567", 1234567890123456700);
        f("-1234567890.123456", -1234567890123456000);
        f("-1234567890.12345", -1234567890123450000);
        f("-1234567890.1234", -1234567890123400000);
        f("-1234567890.123", -1234567890123000000);
        f("-1234567890.12", -1234567890120000000);
        f("-1234567890.1", -1234567890100000000);

        // scientific notation
        f("1e9", 1000000000000000000);
        f("1.234e9", 1234000000000000000);
        f("-1.23456789e9", -1234567890000000000);
        f("1.234567890123456789e18", 1234567890123456789);
        f("-1.234567890123456789e18", -1234567890123456789);
        f("0.23456789e9", 234567890000000000);
        f("123.456789123e9", 123456789123000000);
        f("-1234.5678912e9", -1234567891200000000);
        f("123.678912e7", 1236789120000000000);
        f("1.23e7", 12300000000000000);
        f("1.23e6", 1230000000000000);
        f("1.23e5", 123000000000000);
        f("1.23e4", 12300000000000);
        f("1.23e3", 1230000000000);
        f("1.23e2", 123000000000);
        f("1.2e1", 12000000000);
        f("1123.456789123456789E15", 1123456789123456789);
    }

    #[test]
    fn test_try_parse_unix_timestamp_failure() {
        fn f(s: &str) {
            assert!(
                try_parse_unix_timestamp(s).is_none(),
                "expecting failure when parsing {s:?}"
            );
        }

        // non-numeric timestamp
        f("");
        f("foobar");
        f("foo.bar");
        f("1.12345671x34");
        f("1.3e12345678x0123");
        f("1xs.12345671");
        f("1xs.12345671e5");
        f("-1xs.12345671e5");

        // missing fractional part
        f("1233344.");

        // too big timestamp
        f("12345678901234567.891");
        f("12345678901234567890");
        f("12345678901234.567891");
        f("12345678901234567890e3");
        f("12345678901234567890.234e3");
        f("-12345678901234567890");
        f("12345678901234567890.235424");
        f("12345678901234567890.235424e3");
        f("-12345678901234567890.235424");
        f("12345678901234567.89");
        f("12345678901234567.8");

        // too big fractional part
        f("0.1234567890123456789123");
        f("-0.1234567890123456789123");

        // too big decimal exponent
        f("1e19");
        f("1.3e123456789090123");

        // too small decimal exponent
        f("1.23e1");
        f("1.234e0");
        f("1E-1");
        f("1.3e-123456789090123");
    }

    #[test]
    fn test_parse_time_at_success() {
        fn f(s: &str, current_time: i64, result_expected: i64) {
            let result = parse_time_at(s, current_time)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(
                result, result_expected,
                "unexpected result for {s:?}; got {result}; want {result_expected}"
            );
        }

        let now = current_unix_nanos();

        // unix timestamp in seconds
        f("1562529662", now, 1562529662 * SECOND);
        f("1562529662.6", now, 1562529662600 * 1_000_000);
        f("1562529662.67", now, 1562529662670 * 1_000_000);
        f("1562529662.678", now, 1562529662678 * 1_000_000);
        f("1562529662.678123", now, 1562529662678123 * 1_000);
        f("1562529662.678123456", now, 1562529662678123456);

        // unix timestamp in milliseconds
        f("1562529662678", now, 1562529662678 * 1_000_000);
        f("1562529662678.9", now, 1562529662678900 * 1_000);
        f("1562529662678.901", now, 1562529662678901 * 1_000);
        f("1562529662678.901324", now, 1562529662678901324);

        // unix timestamp in microseconds
        f("1562529662678901", now, 1562529662678901 * 1_000);
        f("1562529662678901.3", now, 1562529662678901300);
        f("1562529662678901.32", now, 1562529662678901320);
        f("1562529662678901.321", now, 1562529662678901321);

        // unix timestamp in nanoseconds
        f("1562529662678901234", now, 1562529662678901234);

        // duration relative to the current time
        f("now", now, now);
        f("1h5s", now, now - 3605 * SECOND);

        // negative duration relative to the current time
        f("-5m", now, now - 5 * MINUTE);
        f("-123", now, now - 123 * SECOND);
        f("-123.456", now, now - 123456 * 1_000_000);
        f("now-1h5m", now, now - (HOUR + 5 * MINUTE));

        // Year
        f("2023Z", now, 1_672_531_200 * SECOND);
        f("2023+02:00", now, 1_672_524_000 * SECOND);
        f("2023-02:00", now, 1_672_538_400 * SECOND);

        // Year and month
        f("2023-05Z", now, 1_682_899_200 * SECOND);
        f("2023-05+02:00", now, 1_682_892_000 * SECOND);
        f("2023-05-02:00", now, 1_682_906_400 * SECOND);

        // Year, month and day
        f("2023-05-20Z", now, 1_684_540_800 * SECOND);
        f("2023-05-20+02:30", now, 1_684_531_800 * SECOND);
        f("2023-05-20-02:30", now, 1_684_549_800 * SECOND);

        // Year, month, day and hour
        f("2023-05-20T04Z", now, 1_684_555_200 * SECOND);
        f("2023-05-20T04+02:30", now, 1_684_546_200 * SECOND);
        f("2023-05-20T04-02:30", now, 1_684_564_200 * SECOND);

        // Year, month, day, hour and minute
        f("2023-05-20T04:57Z", now, 1_684_558_620 * SECOND);
        f("2023-05-20T04:57+02:30", now, 1_684_549_620 * SECOND);
        f("2023-05-20T04:57-02:30", now, 1_684_567_620 * SECOND);

        // Year, month, day, hour, minute and second
        f("2023-05-20T04:57:43Z", now, 1_684_558_663 * SECOND);
        f("2023-05-20T04:57:43+02:30", now, 1_684_549_663 * SECOND);
        f("2023-05-20T04:57:43-02:30", now, 1_684_567_663 * SECOND);

        // milliseconds
        f("2023-05-20T04:57:43.123Z", now, 1684558663123000000);
        f(
            "2023-05-20T04:57:43.123456789+02:30",
            now,
            1684549663123456789,
        );
        f(
            "2023-05-20T04:57:43.123456789-02:30",
            now,
            1684567663123456789,
        );
    }

    // Unix nanoseconds for the given civil time at the given UTC offset,
    // equivalent to Go's time.Date(...).UnixNano() including its wrapping
    // behavior outside the representable range.
    fn date(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, offset_secs: i64) -> i64 {
        civil_unix_nanos(
            Civil {
                year: y,
                month: mo,
                day: d,
                hour: h,
                minute: mi,
                second: s,
            },
            0,
            offset_secs,
        )
    }

    #[test]
    fn test_parse_time_at_limits() {
        let now = date(2025, 1, 1, 0, 0, 0, 0);

        let f = |s: &str, want: i64| {
            let got = parse_time_at(s, now).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(
                got, want,
                "unexpected result for {s:?}; got {got}; want {want}"
            );
        };

        const EAST: i64 = 14 * 3600; // Etc/GMT-14 == UTC+14:00
        const WEST: i64 = -12 * 3600; // Etc/GMT+12 == UTC-12:00

        // min year
        f("1678Z", date(1678, 1, 1, 0, 0, 0, 0));
        f("1678+14:00", date(1678, 1, 1, 0, 0, 0, EAST));
        f("1678-12:00", date(1678, 1, 1, 0, 0, 0, WEST));

        // min month
        f("1677-10Z", date(1677, 10, 1, 0, 0, 0, 0));
        f("1677-10+14:00", date(1677, 10, 1, 0, 0, 0, EAST));
        f("1677-10-12:00", date(1677, 10, 1, 0, 0, 0, WEST));

        // min day
        f("1677-09-22Z", date(1677, 9, 22, 0, 0, 0, 0));
        f("1677-09-22+14:00", date(1677, 9, 22, 0, 0, 0, EAST));
        f("1677-09-22-12:00", date(1677, 9, 22, 0, 0, 0, WEST));

        // min hour
        f("1677-09-21T01Z", date(1677, 9, 21, 1, 0, 0, 0));
        f("1677-09-21T15+14:00", date(1677, 9, 21, 15, 0, 0, EAST));
        f("1677-09-21T01+14:00", i64::MIN);
        f("1677-09-21T01-12:00", date(1677, 9, 21, 1, 0, 0, WEST));

        // min minute
        f("1677-09-21T00:12Z", date(1677, 9, 21, 0, 12, 0, 0));
        f(
            "1677-09-21T15:12Z+14:00",
            date(1677, 9, 21, 15, 12, 0, EAST),
        );
        f("1677-09-21T00:13Z+14:00", i64::MIN);
        f("1677-09-21T00:13Z-12:00", date(1677, 9, 21, 0, 13, 0, WEST));

        // min second
        f("1677-09-21T00:12:43Z", date(1677, 9, 21, 0, 12, 43, 0));
        f(
            "1677-09-21T15:12:43Z+14:00",
            date(1677, 9, 21, 15, 12, 43, EAST),
        );
        f("1677-09-21T00:12:44Z+14:00", i64::MIN);
        f(
            "1677-09-21T00:12:44Z-12:00",
            date(1677, 9, 21, 0, 12, 44, WEST),
        );

        // max year
        f("2262Z", date(2262, 1, 1, 0, 0, 0, 0));
        f("2262+14:00", date(2262, 1, 1, 0, 0, 0, EAST));
        f("2262-12:00", date(2262, 1, 1, 0, 0, 0, WEST));

        // max month
        f("2262-04Z", date(2262, 4, 1, 0, 0, 0, 0));
        f("2262-04+14:00", date(2262, 4, 1, 0, 0, 0, EAST));
        f("2262-04-12:00", date(2262, 4, 1, 0, 0, 0, WEST));

        // max day
        f("2262-04-11Z", date(2262, 4, 11, 0, 0, 0, 0));
        f("2262-04-11+14:00", date(2262, 4, 11, 0, 0, 0, EAST));
        f("2262-04-11-12:00", date(2262, 4, 11, 0, 0, 0, WEST));

        // max hour
        f("2262-04-11T23Z", date(2262, 4, 11, 23, 0, 0, 0));
        f("2262-04-11T23+14:00", date(2262, 4, 11, 23, 0, 0, EAST));
        f("2262-04-11T11-12:00", date(2262, 4, 11, 11, 0, 0, WEST));
        f("2262-04-11T23-12:00", i64::MAX);

        // max minute
        f("2262-04-11T23:47Z", date(2262, 4, 11, 23, 47, 0, 0));
        f("2262-04-11T23:47+14:00", date(2262, 4, 11, 23, 47, 0, EAST));
        f("2262-04-11T11:47-12:00", date(2262, 4, 11, 11, 47, 0, WEST));
        f("2262-04-11T23:47-12:00", i64::MAX);

        // max second
        f("2262-04-11T23:47:16Z", date(2262, 4, 11, 23, 47, 16, 0));
        f(
            "2262-04-11T23:47:16+14:00",
            date(2262, 4, 11, 23, 47, 16, EAST),
        );
        f(
            "2262-04-11T11:47:16-12:00",
            date(2262, 4, 11, 11, 47, 16, WEST),
        );
        f("2262-04-11T23:47:16-12:00", i64::MAX);

        // max timestamp
        f(
            &format!("{MAX_VALID_SECOND}"),
            date(2262, 4, 11, 23, 47, 16, 0),
        );
        f(
            &format!("{MAX_VALID_MILLI}"),
            date(2262, 4, 11, 23, 47, 16, 0) + 854_000_000,
        );
        f(
            &format!("{MAX_VALID_MICRO}"),
            date(2262, 4, 11, 23, 47, 16, 0) + 854_775_000,
        );
        f(
            &format!("{}", i64::MAX),
            date(2262, 4, 11, 23, 47, 16, 0) + 854_775_807,
        );

        // timestamps beyond max valid second are still valid but are treated as
        // milliseconds.
        f(
            &format!("{}", MAX_VALID_SECOND + 1),
            date(1970, 4, 17, 18, 2, 52, 0) + 37_000_000,
        );

        // timestamps beyond max valid millisecond are still valid but are treated
        // as microseconds.
        f(
            &format!("{}", MAX_VALID_MILLI + 1),
            date(1970, 4, 17, 18, 2, 52, 0) + 36_855_000,
        );

        // timestamps beyond max valid microsecond are still valid but are treated
        // as nanoseconds.
        f(
            &format!("{}", MAX_VALID_MICRO + 1),
            date(1970, 4, 17, 18, 2, 52, 0) + 36_854_776,
        );
    }

    #[test]
    fn test_parse_time_at_outside_limits_nanos() {
        let now = date(2025, 1, 1, 0, 0, 0, 0);

        let f = |s: &str| match parse_time_at(s, now) {
            Ok(got) => panic!("expected error but got {got}"),
            Err(err) => assert!(
                err.contains("cannot parse numeric timestamp"),
                "expected error: {err}"
            ),
        };

        // max unix nano
        f(&format!("{}", i64::MAX as u64 + 1));
    }

    #[test]
    fn test_parse_time_msec_failure() {
        fn f(s: &str) {
            assert!(
                parse_time_msec(s).is_err(),
                "expecting non-nil error for {s:?}"
            );
        }

        f("");
        f("23-45:50");
        f("1223-fo:ba");
        f("1223-12:ba");
        f("23-45");
        f("-123foobar");
        f("2oo5");
        f("2oob-a5");
        f("2oob-ar-a5");
        f("2oob-ar-azTx5");
        f("2oob-ar-azTxx:y5");
        f("2oob-ar-azTxx:yy:z5");
    }
}
