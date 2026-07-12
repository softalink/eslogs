//! Port of Softalink LLC `lib/flagutil/bytes.go`.

use std::fmt;

use super::FlagValue;

/// A flag for holding size in bytes.
///
/// It supports the following optional suffixes for values:
/// KB, MB, GB, TB, KiB, MiB, GiB, TiB.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Bytes {
    /// Parsed value for the given flag.
    pub n: i64,

    /// Flag name.
    pub name: String,

    value_string: String,
}

impl Bytes {
    /// Returns a new Bytes with the given default value, like Go `NewBytes`.
    pub fn with_default(default_value: i64) -> Self {
        Bytes {
            n: default_value,
            name: String::new(),
            value_string: default_value.to_string(),
        }
    }

    /// Returns the stored value capped by the `isize` type, like Go
    /// `Bytes.IntN`.
    pub fn int_n(&self) -> isize {
        self.n.clamp(isize::MIN as i64, isize::MAX as i64) as isize
    }

    /// Parses `value`, like Go `Bytes.Set`.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        if value.is_empty() {
            self.n = 0;
            self.value_string = String::new();
            return Ok(());
        }
        let value = normalize_bytes_string(value);
        let n = parse_bytes_normalized(&value)?;
        self.n = n;
        self.value_string = value;
        Ok(())
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value_string)
    }
}

impl FlagValue for Bytes {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let mut b = Bytes::default();
        b.set(s)?;
        Ok(b)
    }
}

/// Returns the number of bytes in the parsed string with unit suffix.
pub fn parse_bytes(value: &str) -> Result<i64, String> {
    let value = normalize_bytes_string(value);
    parse_bytes_normalized(&value)
}

fn parse_float_prefix(value: &str, suffix_len: usize) -> Result<f64, String> {
    let num = &value[..value.len() - suffix_len];
    num.parse::<f64>()
        .map_err(|err| format!("cannot parse {num:?} as float: {err}"))
}

fn parse_bytes_normalized(value: &str) -> Result<i64, String> {
    let scaled = |f: f64, scale: f64| -> i64 { (f * scale) as i64 };
    if value.ends_with("KiB") {
        return Ok(scaled(parse_float_prefix(value, 3)?, 1024.0));
    }
    if value.ends_with("MiB") {
        return Ok(scaled(parse_float_prefix(value, 3)?, 1024.0 * 1024.0));
    }
    if value.ends_with("GiB") {
        return Ok(scaled(
            parse_float_prefix(value, 3)?,
            1024.0 * 1024.0 * 1024.0,
        ));
    }
    if value.ends_with("TiB") {
        return Ok(scaled(
            parse_float_prefix(value, 3)?,
            1024.0 * 1024.0 * 1024.0 * 1024.0,
        ));
    }
    if value.ends_with("KB") {
        return Ok(scaled(parse_float_prefix(value, 2)?, 1000.0));
    }
    if value.ends_with("MB") {
        return Ok(scaled(parse_float_prefix(value, 2)?, 1000.0 * 1000.0));
    }
    if value.ends_with("GB") {
        return Ok(scaled(
            parse_float_prefix(value, 2)?,
            1000.0 * 1000.0 * 1000.0,
        ));
    }
    if value.ends_with("TB") {
        return Ok(scaled(
            parse_float_prefix(value, 2)?,
            1000.0 * 1000.0 * 1000.0 * 1000.0,
        ));
    }
    let f = parse_float_prefix(value, 0)?;
    Ok(f as i64)
}

fn normalize_bytes_string(s: &str) -> String {
    s.to_uppercase().replace('I', "i")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_set_failure() {
        fn f(value: &str) {
            let mut b = Bytes::default();
            assert!(
                b.set(value).is_err(),
                "expecting non-nil error in b.set({value:?})"
            );
        }
        f("foobar");
        f("5foobar");
        f("aKB");
        f("134xMB");
        f("2.43sdfGb");
        f("aKiB");
        f("134xMiB");
        f("2.43sdfGIb");
    }

    #[test]
    fn test_bytes_set_success() {
        fn f(value: &str, expected_result: i64) {
            let mut b = Bytes::default();
            b.set(value).unwrap_or_else(|err| {
                panic!("unexpected error in b.set({value:?}): {err}");
            });
            assert_eq!(b.n, expected_result, "unexpected result for {value:?}");
            let value_string = b.to_string();
            let value_expected = normalize_bytes_string(value);
            assert_eq!(value_string, value_expected, "unexpected value_string");
        }
        f("", 0);
        f("0", 0);
        f("1", 1);
        f("-1234", -1234);
        f("123.456", 123);
        f("1KiB", 1024);
        f("1.5kib", (1.5 * 1024.0) as i64);
        f("23MiB", 23 * 1024 * 1024);
        f("0.25GiB", (0.25 * 1024.0 * 1024.0 * 1024.0) as i64);
        f("1.25TiB", (1.25 * 1024.0 * 1024.0 * 1024.0 * 1024.0) as i64);
        f("1KB", 1000);
        f("1.5kb", (1.5 * 1000.0) as i64);
        f("23MB", 23 * 1000 * 1000);
        f("0.25GB", (0.25 * 1000.0 * 1000.0 * 1000.0) as i64);
        f("1.25TB", (1.25 * 1000.0 * 1000.0 * 1000.0 * 1000.0) as i64);
    }
}
