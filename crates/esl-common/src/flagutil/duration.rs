//! Port of Softalink LLC `lib/flagutil/duration.go`.
//!
//! Also contains ports of `metricsql.PositiveDurationValue`/`DurationValue`
//! (the extended duration syntax with `d`/`w`/`y` suffixes) and of Go stdlib
//! `time.ParseDuration`/`time.Duration.String()`, which `ArrayDuration`
//! relies on.

use std::fmt;
use std::time::Duration;

use super::FlagValue;

const MAX_MONTHS: i64 = 12 * 100;
pub(crate) const MSECS_PER_31_DAYS: i64 = 31 * 24 * 3600 * 1000;

/// A flag for holding a duration for a retention period.
///
/// Values without a unit are counted in months. Supported optional suffixes:
/// s (second), h (hour), d (day), w (week), M (month), y (year).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RetentionDuration {
    /// Parsed duration in milliseconds.
    msecs: i64,

    value_string: String,
}

impl RetentionDuration {
    /// Returns the duration as [`std::time::Duration`].
    pub fn duration(&self) -> Duration {
        Duration::from_millis(self.msecs.max(0) as u64)
    }

    /// Returns the duration in milliseconds.
    pub fn milliseconds(&self) -> i64 {
        self.msecs
    }

    /// Serializes the flag value as a JSON string, like Go `MarshalJSON`.
    pub fn marshal_json(&self) -> String {
        super::go_quote(&self.value_string)
    }

    /// Restores the flag value from a JSON string, like Go `UnmarshalJSON`.
    pub fn unmarshal_json(&mut self, data: &str) -> Result<(), String> {
        let s = unquote_json_string(data)?;
        self.set(&s)
    }

    /// Parses `value`, like Go `RetentionDuration.Set`.
    ///
    /// It assumes that a value without a unit should be parsed as a `month`
    /// duration. It returns an error if the value has an `m` unit.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        if value.is_empty() {
            self.msecs = 0;
            self.value_string = String::new();
            return Ok(());
        }

        // An attempt to parse value as months with unit M(onth).
        if let Some(cut_value) = value.strip_suffix('M') {
            let months: f64 = cut_value
                .parse()
                .map_err(|err| format!("cannot parse months from {value:?}: {err}"))?;
            self.set_months(months, value)?;
            return Ok(());
        }

        // An attempt to parse value as a numeric month (without unit).
        // Such values should be treated as months for BC and historical
        // reasons. The format is deprecated; a value with unit M should be
        // used instead.
        if let Ok(months) = value.parse::<f64>() {
            self.set_months(months, value)?;
            return Ok(());
        }

        // Parse duration.
        let value = value.to_lowercase();
        if value.ends_with('m') {
            return Err(format!(
                "duration in months must be set with capital `M` suffix, lower case `m` means minutes and not allowed; got {value}"
            ));
        }
        let msecs = positive_duration_value(&value, 0)?;
        if msecs / MSECS_PER_31_DAYS > MAX_MONTHS {
            return Err(format!(
                "duration must be smaller than {MAX_MONTHS} months; got approx {} months",
                msecs / MSECS_PER_31_DAYS
            ));
        }
        self.msecs = msecs;
        self.value_string = value;
        Ok(())
    }

    fn set_months(&mut self, months: f64, value: &str) -> Result<(), String> {
        if months > MAX_MONTHS as f64 {
            return Err(format!(
                "duration months must be smaller than {MAX_MONTHS}; got {months}"
            ));
        }
        if months < 0.0 {
            return Err(format!("duration months cannot be negative; got {months}"));
        }
        self.msecs = (months * MSECS_PER_31_DAYS as f64) as i64;
        self.value_string = value.to_string();
        Ok(())
    }
}

impl fmt::Display for RetentionDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value_string)
    }
}

impl FlagValue for RetentionDuration {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let mut d = RetentionDuration::default();
        d.set(s)?;
        Ok(d)
    }
}

/// A flag for specifying time durations with explicit unit suffixes.
///
/// Unlike [`RetentionDuration`], it requires explicit unit suffixes and
/// doesn't treat bare numbers as months. Supported units: s (seconds),
/// m (minutes), h (hours), d (days), w (weeks), y (years).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExtendedDuration {
    /// Parsed duration in milliseconds.
    msecs: i64,

    value_string: String,
}

impl ExtendedDuration {
    /// Returns the duration as [`std::time::Duration`].
    pub fn duration(&self) -> Duration {
        Duration::from_millis(self.msecs.max(0) as u64)
    }

    /// Returns the duration in milliseconds.
    pub fn milliseconds(&self) -> i64 {
        self.msecs
    }

    /// Serializes the flag value as a JSON string, like Go `MarshalJSON`.
    pub fn marshal_json(&self) -> String {
        super::go_quote(&self.value_string)
    }

    /// Restores the flag value from a JSON string, like Go `UnmarshalJSON`.
    pub fn unmarshal_json(&mut self, data: &str) -> Result<(), String> {
        let s = unquote_json_string(data)?;
        self.set(&s)
    }

    /// Parses `value`, like Go `ExtendedDuration.Set`.
    ///
    /// It requires explicit unit suffixes and rejects bare numbers (except 0).
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        if value.is_empty() {
            self.msecs = 0;
            self.value_string = String::new();
            return Ok(());
        }

        // Check for bare numbers first.
        if let Ok(f) = value.parse::<f64>() {
            if f != 0.0 {
                return Err(format!(
                    "duration value must have a unit suffix (s, m, h, d, w, y); got bare number {value:?} (0 is allowed)"
                ));
            }
            // Allow 0 as it's unambiguous.
            self.msecs = 0;
            self.value_string = value.to_string();
            return Ok(());
        }

        // Parse duration with units.
        let value = value.to_lowercase();
        let msecs = positive_duration_value(&value, 0)
            .map_err(|err| format!("cannot parse duration {value:?}: {err}"))?;
        self.msecs = msecs;
        self.value_string = value;
        Ok(())
    }
}

impl fmt::Display for ExtendedDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value_string)
    }
}

impl FlagValue for ExtendedDuration {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let mut d = ExtendedDuration::default();
        d.set(s)?;
        Ok(d)
    }
}

fn unquote_json_string(data: &str) -> Result<String, String> {
    let s = data.trim();
    if s.len() < 2 || !s.starts_with('"') || !s.ends_with('"') {
        return Err(format!("cannot parse JSON string from {data:?}"));
    }
    super::array::go_unquote(s).map_err(|()| format!("cannot parse JSON string from {data:?}"))
}

/// Port of `metricsql.PositiveDurationValue`: returns the duration in
/// milliseconds for the given `s` and the given `step` (in milliseconds).
pub fn positive_duration_value(s: &str, step: i64) -> Result<i64, String> {
    let d = duration_value(s, step)?;
    if d < 0 {
        return Err(format!("duration cannot be negative; got {s:?}"));
    }
    Ok(d)
}

/// Port of `metricsql.DurationValue`: returns the duration in milliseconds
/// for the given `s` and the given `step`.
///
/// The duration in `s` may be combined, i.e. `2h5m`, `-2h5m` or `2h-5m`.
/// The returned duration value can be negative.
///
/// Like Go, the Grafana-specific `$__interval` pseudo-duration and the
/// `step`-relative `i` suffix both resolve to `step`.
pub fn duration_value(s: &str, step: i64) -> Result<i64, String> {
    if s.is_empty() {
        return Err("duration cannot be empty".to_string());
    }
    let last_char = *s.as_bytes().last().unwrap();
    if last_char.is_ascii_digit() || last_char == b'.' {
        // Try parsing floating-point duration.
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
    let s = s.to_lowercase();
    let mut num_part = &s[..s.len() - 1];
    // Strip trailing m if the duration is in ms.
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

fn scan_single_duration(s: &str, can_be_negative: bool) -> i64 {
    if s.is_empty() {
        return -1;
    }
    let b = s.as_bytes();
    let mut i = 0usize;
    if b[0] == b'-' && can_be_negative {
        i += 1;
    }
    if &s[i..] == "$__interval" {
        return (i + "$__interval".len()) as i64;
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
                        // Duration in ms.
                        return i as i64 + 2;
                    }
                    b'i' | b'b' => {
                        // This is not a duration, but a Mi or MB suffix.
                        return -1;
                    }
                    _ => {}
                }
            }
            // Allow small m for duration in minutes. Big M means 1e6.
            if b[i] == b'm' {
                return i as i64 + 1;
            }
            -1
        }
        b's' | b'h' | b'd' | b'w' | b'y' | b'i' => i as i64 + 1,
        _ => -1,
    }
}

/// Port of Go stdlib `time.ParseDuration`: parses a duration string like
/// `300ms`, `-1.5h` or `2h45m` and returns the duration in nanoseconds.
///
/// Valid time units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`.
pub fn parse_go_duration(s: &str) -> Result<i64, String> {
    let orig = s;
    let mut s = s;
    let mut d: u64 = 0;
    let mut neg = false;

    // Consume [-+]?
    if !s.is_empty() {
        let c = s.as_bytes()[0];
        if c == b'-' || c == b'+' {
            neg = c == b'-';
            s = &s[1..];
        }
    }
    // Special case: if all that is left is "0", this is zero.
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(format!("time: invalid duration {orig:?}"));
    }
    while !s.is_empty() {
        let mut scale = 1f64;

        // The next character must be [0-9.]
        let c = s.as_bytes()[0];
        if !(c == b'.' || c.is_ascii_digit()) {
            return Err(format!("time: invalid duration {orig:?}"));
        }
        // Consume [0-9]*
        let pl = s.len();
        let (mut v, rest) =
            leading_int(s).map_err(|()| format!("time: invalid duration {orig:?}"))?;
        s = rest;
        let pre = pl != s.len(); // whether we consumed anything before a period

        // Consume (\.[0-9]*)?
        let mut post = false;
        let mut f: u64 = 0;
        if !s.is_empty() && s.as_bytes()[0] == b'.' {
            s = &s[1..];
            let pl = s.len();
            let (ff, sscale, rest) = leading_fraction(s);
            f = ff;
            scale = sscale;
            s = rest;
            post = pl != s.len();
        }
        if !pre && !post {
            // no digits (e.g. ".s" or "-.s")
            return Err(format!("time: invalid duration {orig:?}"));
        }

        // Consume unit.
        let mut i = 0;
        let sb = s.as_bytes();
        while i < sb.len() {
            let c = sb[i];
            if c == b'.' || c.is_ascii_digit() {
                break;
            }
            i += 1;
        }
        if i == 0 {
            return Err(format!("time: missing unit in duration {orig:?}"));
        }
        let u = &s[..i];
        s = &s[i..];
        let unit: u64 = match u {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => {
                return Err(format!("time: unknown unit {u:?} in duration {orig:?}"));
            }
        };
        if v > (1 << 63) / unit {
            // overflow
            return Err(format!("time: invalid duration {orig:?}"));
        }
        v *= unit;
        if f > 0 {
            // f64 is needed to be nanosecond accurate for fractions of hours.
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > 1 << 63 {
                // overflow
                return Err(format!("time: invalid duration {orig:?}"));
            }
        }
        d += v;
        if d > 1 << 63 {
            return Err(format!("time: invalid duration {orig:?}"));
        }
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > i64::MAX as u64 {
        return Err(format!("time: invalid duration {orig:?}"));
    }
    Ok(d as i64)
}

/// Consumes the leading `[0-9]*` from `s`, erroring on overflow.
fn leading_int(s: &str) -> Result<(u64, &str), ()> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut x: u64 = 0;
    while i < b.len() {
        let c = b[i];
        if !c.is_ascii_digit() {
            break;
        }
        if x > (1 << 63) / 10 {
            // overflow
            return Err(());
        }
        x = x * 10 + (c - b'0') as u64;
        if x > 1 << 63 {
            // overflow
            return Err(());
        }
        i += 1;
    }
    Ok((x, &s[i..]))
}

/// Consumes the leading `[0-9]*` from `s` as a fraction.
///
/// It is used only for fractions, so does not return an error on overflow;
/// it just stops accumulating precision.
fn leading_fraction(s: &str) -> (u64, f64, &str) {
    let b = s.as_bytes();
    let mut i = 0;
    let mut x: u64 = 0;
    let mut scale = 1f64;
    let mut overflow = false;
    while i < b.len() {
        let c = b[i];
        if !c.is_ascii_digit() {
            break;
        }
        i += 1;
        if overflow {
            continue;
        }
        if x > (u64::MAX / 2) / 10 {
            // It's possible for overflow to give a positive number, so take care.
            overflow = true;
            continue;
        }
        let y = x * 10 + (c - b'0') as u64;
        if y > 1 << 63 {
            overflow = true;
            continue;
        }
        x = y;
        scale *= 10.0;
    }
    (x, scale, &s[i..])
}

/// Port of Go stdlib `time.Duration.String()`: formats `nanos` like Go, e.g.
/// `72h3m0.5s`, `1m0s`, `10s`, `1.5ms`.
pub fn format_go_duration(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }
    let neg = nanos < 0;
    let u = nanos.unsigned_abs();
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    const SECOND: u64 = 1_000_000_000;
    if u < SECOND {
        // Special case: if the duration is smaller than a second,
        // use smaller units, like 1.2ms.
        if u < 1_000 {
            out.push_str(&format!("{u}ns"));
        } else if u < 1_000_000 {
            out.push_str(&format_with_frac(u / 1_000, u % 1_000, 3));
            out.push_str("µs");
        } else {
            out.push_str(&format_with_frac(u / 1_000_000, u % 1_000_000, 6));
            out.push_str("ms");
        }
        return out;
    }
    let frac = u % SECOND;
    let total_secs = u / SECOND;
    let secs = total_secs % 60;
    let mins = (total_secs / 60) % 60;
    let hours = total_secs / 3600;
    if hours > 0 {
        out.push_str(&format!("{hours}h{mins}m"));
    } else if mins > 0 {
        out.push_str(&format!("{mins}m"));
    }
    out.push_str(&format_with_frac(secs, frac, 9));
    out.push('s');
    out
}

/// Formats `int_part` followed by a fractional part of `frac` scaled to
/// `digits` decimal places, with trailing zeros (and a bare decimal point)
/// trimmed.
fn format_with_frac(int_part: u64, frac: u64, digits: usize) -> String {
    let mut s = format!("{int_part}");
    if frac > 0 {
        let mut fs = format!("{frac:0>width$}", width = digits);
        while fs.ends_with('0') {
            fs.pop();
        }
        if !fs.is_empty() {
            s.push('.');
            s.push_str(&fs);
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_set_failure() {
        fn f(value: &str) {
            let mut d = RetentionDuration::default();
            assert!(
                d.set(value).is_err(),
                "expecting non-nil error in d.set({value:?})"
            );
        }
        f("foobar");
        f("5foobar");
        f("ah");
        f("134xd");
        f("2.43sdfw");

        // Too big value in months
        f("12345");

        // Too big duration
        f("999y");
        f("100000000000y");

        // Negative duration
        f("-1");
        f("-34h");

        f("1mM");

        // RetentionDuration in minutes is confused with duration in months
        f("1m");
    }

    #[test]
    fn test_duration_set_success() {
        fn f(value: &str, expected_msecs: i64, expected_value_string: &str) {
            let mut d = RetentionDuration::default();
            d.set(value)
                .unwrap_or_else(|err| panic!("unexpected error in d.set({value:?}): {err}"));
            assert_eq!(
                d.milliseconds(),
                expected_msecs,
                "unexpected result for {value:?}"
            );
            assert_eq!(
                d.to_string(),
                expected_value_string,
                "unexpected value_string"
            );
        }
        f("", 0, "");
        f("0", 0, "0");
        f("1", MSECS_PER_31_DAYS, "1");
        f(
            "123.456",
            (123.456f64 * MSECS_PER_31_DAYS as f64) as i64,
            "123.456",
        );
        f("1h", 3600 * 1000, "1h");
        f("1.5d", 129_600_000, "1.5d");
        f("2.3W", 1_391_040_000, "2.3w");
        f("1w", 7 * 24 * 3600 * 1000, "1w");
        f("0.25y", 7_884_000_000, "0.25y");
        f("3M", 93 * 24 * 3600 * 1000, "3M");
        f("100y", 100 * 365 * 24 * 3600 * 1000, "100y");
    }

    #[test]
    fn test_duration_duration() {
        fn f(value: &str, expected: Duration) {
            let mut d = RetentionDuration::default();
            d.set(value)
                .unwrap_or_else(|err| panic!("unexpected error in d.set({value:?}): {err}"));
            assert_eq!(d.duration(), expected, "unexpected result for {value:?}");
        }
        f("0", Duration::ZERO);
        f("1", Duration::from_secs(31 * 24 * 3600));
        f("1h", Duration::from_secs(3600));
        f("1.5d", Duration::from_secs(36 * 3600));
        f("1w", Duration::from_secs(7 * 24 * 3600));
        f("0.25y", Duration::from_millis(7_884_000_000));
    }

    #[test]
    fn test_extended_duration_set_failure() {
        fn f(value: &str) {
            let mut d = ExtendedDuration::default();
            assert!(
                d.set(value).is_err(),
                "expecting non-nil error in d.set({value:?})"
            );
        }
        // Invalid format
        f("foobar");
        f("5foobar");
        f("ah");
        f("134xd");
        f("2.43sdfw");

        // Bare numbers are not allowed (except 0)
        f("1");
        f("5");
        f("123.456");

        // Negative duration
        f("-1h");
        f("-34d");

        // Invalid duration syntax
        f("1x");
        f("abc5d");
    }

    #[test]
    fn test_extended_duration_set_success() {
        fn f(value: &str, expected_msecs: i64) {
            let mut d = ExtendedDuration::default();
            d.set(value)
                .unwrap_or_else(|err| panic!("unexpected error in d.set({value:?}): {err}"));
            assert_eq!(
                d.milliseconds(),
                expected_msecs,
                "unexpected result for {value:?}"
            );
            let value_string = d.to_string();
            let value_expected = value.to_lowercase();
            assert_eq!(value_string, value_expected, "unexpected value_string");
        }
        // Empty and zero values
        f("", 0);
        f("0", 0);

        // Time units
        f("1s", 1000);
        f("30s", 30 * 1000);
        f("1h", 3600 * 1000);
        f("2h", 2 * 3600 * 1000);
        f("1.5h", (1.5 * 3600.0 * 1000.0) as i64);

        // Extended units
        f("1d", 24 * 3600 * 1000);
        f("1.5d", (1.5 * 24.0 * 3600.0 * 1000.0) as i64);
        f("7d", 7 * 24 * 3600 * 1000);
        f("1w", 7 * 24 * 3600 * 1000);
        f("2w", 2 * 7 * 24 * 3600 * 1000);
        f("1y", 365 * 24 * 3600 * 1000);
        f("0.25y", 7_884_000_000);

        // Case insensitive
        f("1D", 24 * 3600 * 1000);
        f("1W", 7 * 24 * 3600 * 1000);
        f("1Y", 365 * 24 * 3600 * 1000);

        // Minutes are allowed (no ambiguity like in RetentionDuration)
        f("1m", 60 * 1000);
        f("30m", 30 * 60 * 1000);
    }

    #[test]
    fn test_extended_duration_duration() {
        fn f(value: &str, expected: Duration) {
            let mut d = ExtendedDuration::default();
            d.set(value)
                .unwrap_or_else(|err| panic!("unexpected error in d.set({value:?}): {err}"));
            assert_eq!(d.duration(), expected, "unexpected result for {value:?}");
        }
        f("0", Duration::ZERO);
        f("1s", Duration::from_secs(1));
        f("1m", Duration::from_secs(60));
        f("1h", Duration::from_secs(3600));
        f("1d", Duration::from_secs(24 * 3600));
        f("1w", Duration::from_secs(7 * 24 * 3600));
        f("1y", Duration::from_secs(365 * 24 * 3600));
        f("1.5d", Duration::from_secs(36 * 3600));
    }

    #[test]
    fn test_extended_duration_json() {
        fn f(value: &str) {
            let mut d = ExtendedDuration::default();
            d.set(value)
                .unwrap_or_else(|err| panic!("unexpected error in d.set({value:?}): {err}"));

            let data = d.marshal_json();

            let mut d2 = ExtendedDuration::default();
            d2.unmarshal_json(&data)
                .unwrap_or_else(|err| panic!("unexpected error in unmarshal_json(): {err}"));

            assert_eq!(
                d.milliseconds(),
                d2.milliseconds(),
                "unexpected result after JSON roundtrip"
            );
            assert_eq!(
                d.to_string(),
                d2.to_string(),
                "unexpected string after JSON roundtrip"
            );
        }
        f("0");
        f("1h");
        f("1d");
        f("1w");
        f("1y");
    }

    #[test]
    fn test_duration_value_grafana_interval() {
        // metricsql resolves the Grafana `$__interval` pseudo-duration (and
        // the `i` suffix) to `step`, like Go.
        assert_eq!(duration_value("$__interval", 30_000), Ok(30_000));
        assert_eq!(duration_value("2i", 30_000), Ok(60_000));
        assert_eq!(positive_duration_value("$__interval", 0), Ok(0));
        // `-$__interval` scans but fails to parse, like Go.
        assert!(duration_value("-$__interval", 30_000).is_err());

        // Flag types accept it through PositiveDurationValue, like Go.
        let mut d = ExtendedDuration::default();
        d.set("$__interval").unwrap();
        assert_eq!(d.milliseconds(), 0);
        let mut r = RetentionDuration::default();
        r.set("$__interval").unwrap();
        assert_eq!(r.milliseconds(), 0);
    }

    #[test]
    fn test_parse_go_duration() {
        fn f(s: &str, expected_nanos: i64) {
            let nanos = parse_go_duration(s).unwrap_or_else(|err| {
                panic!("unexpected error in parse_go_duration({s:?}): {err}")
            });
            assert_eq!(nanos, expected_nanos, "unexpected result for {s:?}");
        }
        f("0", 0);
        f("5s", 5_000_000_000);
        f("30s", 30_000_000_000);
        f("1478s", 1_478_000_000_000);
        f("-5s", -5_000_000_000);
        f("+5s", 5_000_000_000);
        f("5.0s", 5_000_000_000);
        f("5.6s", 5_600_000_000);
        f("5.s", 5_000_000_000);
        f(".5s", 500_000_000);
        f("1.0s", 1_000_000_000);
        f("1.00s", 1_000_000_000);
        f("1.004s", 1_004_000_000);
        f("100.00100s", 100_001_000_000);
        f("10ns", 10);
        f("11us", 11_000);
        f("12µs", 12_000);
        f("13ms", 13_000_000);
        f("14s", 14_000_000_000);
        f("15m", 15 * 60_000_000_000);
        f("16h", 16 * 3_600_000_000_000);
        f("3h30m", 3 * 3_600_000_000_000 + 30 * 60_000_000_000);
        f("10.5s4m", 4 * 60_000_000_000 + 10_500_000_000);
        f("-2m3.4s", -(2 * 60_000_000_000 + 3_400_000_000));
        f(
            "1h2m3s4ms5us6ns",
            3_600_000_000_000 + 2 * 60_000_000_000 + 3_000_000_000 + 4_000_000 + 5_000 + 6,
        );

        fn err(s: &str) {
            assert!(
                parse_go_duration(s).is_err(),
                "expecting error in parse_go_duration({s:?})"
            );
        }
        err("");
        err("3");
        err("-");
        err("s");
        err(".");
        err("-.");
        err(".s");
        err("+.s");
        err("1d");
        err("1y");
    }

    #[test]
    fn test_format_go_duration() {
        fn f(nanos: i64, expected: &str) {
            assert_eq!(format_go_duration(nanos), expected);
        }
        f(0, "0s");
        f(1, "1ns");
        f(1_100, "1.1µs");
        f(2_200_000, "2.2ms");
        f(3_300_000_000, "3.3s");
        f(4 * 60_000_000_000 + 5_000_000_000, "4m5s");
        f(4 * 60_000_000_000 + 5_001_000_000, "4m5.001s");
        f(
            5 * 3_600_000_000_000 + 6 * 60_000_000_000 + 7_001_000_000,
            "5h6m7.001s",
        );
        f(8 * 60_000_000_000 + 1, "8m0.000000001s");
        f(-(8 * 60_000_000_000 + 1), "-8m0.000000001s");
        f(60_000_000_000, "1m0s");
        f(300_000_000_000, "5m0s");
        f(3_600_000_000_000, "1h0m0s");
    }
}
