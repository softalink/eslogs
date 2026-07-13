//! Port of Softalink LLC `lib/flagutil/array.go`.
//!
//! Array-valued flags: comma-separated values with single-quote/double-quote/
//! `{}`/`[]`/`()` aware splitting. Repeated `-foo=...` occurrences append,
//! like Go (`set_flag_occurrences` calls `set()` once per occurrence, the
//! same way Go's `flag.Parse` calls `flag.Value.Set`).

use std::fmt;
use std::time::Duration;

use super::duration::{format_go_duration, parse_go_duration};
use super::go_quote;
use super::{Bytes, FlagParseError, FlagValue};

/// Implements `FlagValue` for an array-valued flag type: repeated
/// command-line occurrences append via `set()`, like Go array flags.
macro_rules! impl_array_flag_value {
    ($t:ty) => {
        impl FlagValue for $t {
            fn parse_flag(s: &str) -> Result<Self, String> {
                let mut a = <$t>::default();
                a.set(s)?;
                Ok(a)
            }

            fn set_flag_occurrences(
                &mut self,
                occurrences: &[String],
            ) -> Result<(), FlagParseError> {
                for s in occurrences {
                    self.set(s).map_err(|err| FlagParseError {
                        value: s.clone(),
                        err,
                    })?;
                }
                Ok(())
            }
        }
    };
}

/// A flag that holds an array of strings.
///
/// Values are comma-separated; each value may contain commas inside single
/// quotes, double quotes, `[]`, `()` or `{}` braces, and may be quoted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArrayString(pub Vec<String>);

impl std::ops::Deref for ArrayString {
    type Target = Vec<String>;
    fn deref(&self) -> &Vec<String> {
        &self.0
    }
}

impl std::ops::DerefMut for ArrayString {
    fn deref_mut(&mut self) -> &mut Vec<String> {
        &mut self.0
    }
}

impl ArrayString {
    /// Appends the parsed values of `value`, like Go `ArrayString.Set`.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        self.0.extend(parse_array_values(value));
        Ok(())
    }

    /// Returns the optional arg under the given `arg_idx`.
    pub fn get_optional_arg(&self, arg_idx: usize) -> &str {
        let x = &self.0;
        if arg_idx >= x.len() {
            if x.len() == 1 {
                return &x[0];
            }
            return "";
        }
        &x[arg_idx]
    }
}

impl fmt::Display for ArrayString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let escaped: Vec<String> = self
            .0
            .iter()
            .map(|v| {
                if v.contains([',', '\'', '"', '{', '[', '(', '\n']) {
                    go_quote(v)
                } else {
                    v.clone()
                }
            })
            .collect();
        f.write_str(&escaped.join(","))
    }
}

impl_array_flag_value!(ArrayString);

pub(crate) fn parse_array_values(s: &str) -> Vec<String> {
    if s.is_empty() {
        return vec![String::new()];
    }
    let mut values = Vec::new();
    let mut s = s;
    loop {
        let (v, tail) = get_next_array_value(s);
        values.push(v);
        if tail.is_empty() {
            return values;
        }
        s = tail;
        if s.as_bytes()[0] == b',' {
            s = &s[1..];
        }
    }
}

fn close_quote_for(ch: u8) -> u8 {
    match ch {
        b'"' => b'"',
        b'\'' => b'\'',
        b'[' => b']',
        b'{' => b'}',
        b'(' => b')',
        // Mirrors Go's `closeQuotes[ch]` zero value for unknown chars.
        _ => 0,
    }
}

fn get_next_array_value(s: &str) -> (String, &str) {
    let (v, tail) = get_next_array_value_maybe_quoted(s);
    if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
        if let Ok(unquoted) = go_unquote(v) {
            return (unquoted, tail);
        }
        let inner = &v[1..v.len() - 1];
        let inner = inner.replace("\\\"", "\"").replace("\\\\", "\\");
        return (inner, tail);
    }
    if v.len() >= 2 && v.starts_with('\'') && v.ends_with('\'') {
        let inner = &v[1..v.len() - 1];
        let inner = inner.replace("\\'", "'").replace("\\\\", "\\");
        return (inner, tail);
    }
    (v.to_string(), tail)
}

fn get_next_array_value_maybe_quoted(s: &str) -> (&str, &str) {
    let mut idx = 0;
    loop {
        match s[idx..].find([',', '"', '\'', '[', '{', '(']) {
            None => {
                // The last item.
                return (s, "");
            }
            Some(n) => {
                idx += n;
                let ch = s.as_bytes()[idx];
                if ch == b',' {
                    // The next item.
                    return (&s[..idx], &s[idx..]);
                }
                idx += 1;
                let m = index_close_quote(&s[idx..], close_quote_for(ch));
                idx += m;
            }
        }
    }
}

fn index_close_quote(s: &str, close_quote: u8) -> usize {
    if close_quote == b'"' || close_quote == b'\'' {
        let mut idx = 0;
        loop {
            match s[idx..].find(close_quote as char) {
                None => return 0,
                Some(n) => {
                    idx += n;
                    if trailing_backslashes_count(&s[..idx]) % 2 == 1 {
                        // The quote is escaped with backslash. Skip it.
                        idx += 1;
                        continue;
                    }
                    return idx + 1;
                }
            }
        }
    }
    let mut idx = 0;
    loop {
        match s[idx..].find(['"', '\'', '[', '{', '(', ')', '}', ']']) {
            None => return 0,
            Some(n) => {
                idx += n;
                let ch = s.as_bytes()[idx];
                if ch == close_quote {
                    return idx + 1;
                }
                idx += 1;
                let m = index_close_quote(&s[idx..], close_quote_for(ch));
                if m == 0 {
                    return 0;
                }
                idx += m;
            }
        }
    }
}

fn trailing_backslashes_count(s: &str) -> usize {
    let b = s.as_bytes();
    let mut n = b.len();
    while n > 0 && b[n - 1] == b'\\' {
        n -= 1;
    }
    b.len() - n
}

/// Unquotes a double-quoted string like Go's `strconv.Unquote`.
///
/// PORT NOTE: `\xHH` and octal escapes with values >= 0x80 are decoded to the
/// corresponding Unicode scalar instead of Go's raw byte, since Rust strings
/// must stay valid UTF-8: Go `strconv.Unquote("\"a\\x80b\"")` yields the
/// bytes `61 80 62` (invalid UTF-8), while this port yields `a\u{80}b`
/// (`61 c2 80 62`). Matching Go exactly would require byte-valued flag
/// strings (`Vec<u8>`) throughout the flag layer. Escapes < 0x80 are
/// identical.
pub(crate) fn go_unquote(s: &str) -> Result<String, ()> {
    let b = s.as_bytes();
    if b.len() < 2 || b[0] != b'"' || b[b.len() - 1] != b'"' {
        return Err(());
    }
    let mut inner = &s[1..s.len() - 1];
    let mut out = String::with_capacity(inner.len());
    while !inner.is_empty() {
        let (c, rest) = unquote_char(inner, '"')?;
        out.push(c);
        inner = rest;
    }
    Ok(out)
}

fn unquote_char(s: &str, quote: char) -> Result<(char, &str), ()> {
    let mut chars = s.chars();
    let c = chars.next().ok_or(())?;
    if c == '\n' || c == quote {
        return Err(());
    }
    if c != '\\' {
        return Ok((c, chars.as_str()));
    }
    let e = chars.next().ok_or(())?;
    let rest = chars.as_str();
    match e {
        'a' => Ok(('\x07', rest)),
        'b' => Ok(('\x08', rest)),
        'f' => Ok(('\x0c', rest)),
        'n' => Ok(('\n', rest)),
        'r' => Ok(('\r', rest)),
        't' => Ok(('\t', rest)),
        'v' => Ok(('\x0b', rest)),
        '\\' => Ok(('\\', rest)),
        '\'' | '"' => {
            // Like Go: `\'` is only valid inside single quotes, `\"` inside
            // double quotes.
            if e == quote { Ok((e, rest)) } else { Err(()) }
        }
        'x' => hex_char(rest, 2),
        'u' => hex_char(rest, 4),
        'U' => hex_char(rest, 8),
        '0'..='7' => {
            // Octal escape: exactly 3 octal digits, value <= 255.
            let b = rest.as_bytes();
            if b.len() < 2 || !b[..2].iter().all(|c| (b'0'..=b'7').contains(c)) {
                return Err(());
            }
            let v = (e as u32 - '0' as u32) * 64 + (b[0] - b'0') as u32 * 8 + (b[1] - b'0') as u32;
            if v > 255 {
                return Err(());
            }
            let c = char::from_u32(v).ok_or(())?;
            Ok((c, &rest[2..]))
        }
        _ => Err(()),
    }
}

fn hex_char(s: &str, ndigits: usize) -> Result<(char, &str), ()> {
    let b = s.as_bytes();
    if b.len() < ndigits || !b[..ndigits].iter().all(u8::is_ascii_hexdigit) {
        return Err(());
    }
    let v = u32::from_str_radix(&s[..ndigits], 16).map_err(|_| ())?;
    let c = char::from_u32(v).ok_or(())?;
    Ok((c, &s[ndigits..]))
}

/// A flag that holds an array of boolean values.
///
/// Has the same API as [`ArrayString`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArrayBool(pub Vec<bool>);

impl std::ops::Deref for ArrayBool {
    type Target = Vec<bool>;
    fn deref(&self) -> &Vec<bool> {
        &self.0
    }
}

impl ArrayBool {
    /// Appends the parsed values of `value`. Empty values are set to false.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        for v in parse_array_values(value) {
            let v = if v.is_empty() { "false".to_string() } else { v };
            let b = bool::parse_flag(&v)?;
            self.0.push(b);
        }
        Ok(())
    }

    /// Returns the optional arg under the given `arg_idx`.
    pub fn get_optional_arg(&self, arg_idx: usize) -> bool {
        let x = &self.0;
        if arg_idx >= x.len() {
            if x.len() == 1 {
                return x[0];
            }
            return false;
        }
        x[arg_idx]
    }
}

impl fmt::Display for ArrayBool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted: Vec<String> = self.0.iter().map(|v| v.to_string()).collect();
        f.write_str(&formatted.join(","))
    }
}

impl_array_flag_value!(ArrayBool);

/// A flag that holds an array of `time.Duration` values.
///
/// Has the same API as [`ArrayString`]. Values use Go `time.ParseDuration`
/// syntax (`300ms`, `1h30m`, ...).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArrayDuration {
    default_value_nanos: i64,
    a: Vec<i64>,
}

impl ArrayDuration {
    /// Returns an empty ArrayDuration which substitutes `default_value` for
    /// empty items, like Go `NewArrayDuration`.
    pub fn with_default(default_value: Duration) -> Self {
        ArrayDuration {
            default_value_nanos: default_value.as_nanos() as i64,
            a: Vec::new(),
        }
    }

    /// Appends the parsed values of `value`. Empty values are set to the
    /// default value.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        for v in parse_array_values(value) {
            let v = if v.is_empty() {
                format_go_duration(self.default_value_nanos)
            } else {
                v
            };
            let nanos = parse_go_duration(&v)?;
            self.a.push(nanos);
        }
        Ok(())
    }

    /// Returns the number of parsed values.
    pub fn len(&self) -> usize {
        self.a.len()
    }

    /// Returns true if no values are parsed.
    pub fn is_empty(&self) -> bool {
        self.a.is_empty()
    }

    /// Returns the optional arg under the given `arg_idx`, or the default
    /// value if `arg_idx` is not found.
    ///
    /// PORT NOTE: negative durations (allowed by Go `time.ParseDuration`,
    /// e.g. `-foo=-5s`) are clamped to zero here, since `std::time::Duration`
    /// is unsigned. Callers that need Go's signed `time.Duration` semantics
    /// must use [`ArrayDuration::get_optional_arg_nanos`] instead.
    pub fn get_optional_arg(&self, arg_idx: usize) -> Duration {
        Duration::from_nanos(self.get_optional_arg_nanos(arg_idx).max(0) as u64)
    }

    /// Returns the optional arg under the given `arg_idx` in nanoseconds
    /// (signed, like Go `time.Duration`), or the default value if `arg_idx`
    /// is not found. Exact port of Go `ArrayDuration.GetOptionalArg`.
    pub fn get_optional_arg_nanos(&self, arg_idx: usize) -> i64 {
        let x = &self.a;
        if arg_idx >= x.len() {
            if x.len() == 1 {
                return x[0];
            }
            return self.default_value_nanos;
        }
        x[arg_idx]
    }
}

impl fmt::Display for ArrayDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted: Vec<String> = self.a.iter().map(|&v| format_go_duration(v)).collect();
        f.write_str(&formatted.join(","))
    }
}

impl_array_flag_value!(ArrayDuration);

/// A flag that holds an array of ints.
///
/// Has the same API as [`ArrayString`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArrayInt {
    default_value: i64,
    a: Vec<i64>,
}

impl ArrayInt {
    /// Returns an empty ArrayInt which substitutes `default_value` for empty
    /// items, like Go `NewArrayInt`.
    pub fn with_default(default_value: i64) -> Self {
        ArrayInt {
            default_value,
            a: Vec::new(),
        }
    }

    /// Returns all the values.
    pub fn values(&self) -> &[i64] {
        &self.a
    }

    /// Appends the parsed values of `value`. Empty values are set to the
    /// default value.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        for v in parse_array_values(value) {
            let v = if v.is_empty() {
                self.default_value.to_string()
            } else {
                v
            };
            let n: i64 = v
                .parse()
                .map_err(|err| format!("cannot parse {v:?} as int: {err}"))?;
            self.a.push(n);
        }
        Ok(())
    }

    /// Returns the optional arg under the given `arg_idx` or the default
    /// value.
    pub fn get_optional_arg(&self, arg_idx: usize) -> i64 {
        let x = &self.a;
        if arg_idx < x.len() {
            return x[arg_idx];
        }
        if x.len() == 1 {
            return x[0];
        }
        self.default_value
    }
}

impl fmt::Display for ArrayInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted: Vec<String> = self.a.iter().map(|v| v.to_string()).collect();
        f.write_str(&formatted.join(","))
    }
}

impl_array_flag_value!(ArrayInt);

/// A flag that holds an array of [`Bytes`].
///
/// Has the same API as [`ArrayString`].
#[derive(Debug, Default, Clone)]
pub struct ArrayBytes {
    default_value: i64,
    a: Vec<Bytes>,
}

impl ArrayBytes {
    /// Returns an empty ArrayBytes which substitutes `default_value` for
    /// empty items, like Go `NewArrayBytes`.
    pub fn with_default(default_value: i64) -> Self {
        ArrayBytes {
            default_value,
            a: Vec::new(),
        }
    }

    /// Appends the parsed values of `value`. Empty values are set to the
    /// default value.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        for v in parse_array_values(value) {
            let v = if v.is_empty() {
                self.default_value.to_string()
            } else {
                v
            };
            let mut b = Bytes::default();
            b.set(&v)?;
            self.a.push(b);
        }
        Ok(())
    }

    /// Returns the number of parsed values.
    pub fn len(&self) -> usize {
        self.a.len()
    }

    /// Returns true if no values are parsed.
    pub fn is_empty(&self) -> bool {
        self.a.is_empty()
    }

    /// Returns the optional arg under the given `arg_idx`, or the default
    /// value.
    pub fn get_optional_arg(&self, arg_idx: usize) -> i64 {
        let x = &self.a;
        if arg_idx < x.len() {
            return x[arg_idx].n;
        }
        if x.len() == 1 {
            return x[0].n;
        }
        self.default_value
    }
}

impl fmt::Display for ArrayBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted: Vec<String> = self.a.iter().map(|v| v.to_string()).collect();
        f.write_str(&formatted.join(","))
    }
}

impl_array_flag_value!(ArrayBytes);

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: the Go tests set the flag values via repeated `os.Args`
    // entries parsed by `flag.Parse`; the raw-map parser doesn't support
    // repeated flags, so the equivalent `set()` calls are used instead.

    #[test]
    fn test_array_string() {
        let mut foo_flag_string = ArrayString::default();
        foo_flag_string.set("foo").unwrap();
        foo_flag_string.set("bar").unwrap();
        let expected = ArrayString(vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(expected, foo_flag_string);
    }

    #[test]
    fn test_array_string_set() {
        fn f(s: &str, expected_result: &str) {
            let mut a = ArrayString::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(
                result, expected_result,
                "unexpected values parsed from {s:?}"
            );
        }
        // Zero args
        f("", "");

        // Single arg
        f(r#"foo"#, r#"foo"#);
        f(r#"fo"o"#, r#""fo\"o""#);
        f(r#"fo'o"#, r#""fo'o""#);
        f(r#"fo{o"#, r#""fo{o""#);
        f(r#"fo[o"#, r#""fo[o""#);
        f(r#"fo(o"#, r#""fo(o""#);

        // Single arg with Prometheus label filters
        f(r#"foo{bar="baz",x="y"}"#, r#""foo{bar=\"baz\",x=\"y\"}""#);
        f(r#"foo{bar="ba}z",x="y"}"#, r#""foo{bar=\"ba}z\",x=\"y\"}""#);
        f(r#"foo{bar='baz',x="y"}"#, r#""foo{bar='baz',x=\"y\"}""#);
        f(r#"foo{bar='baz',x='y'}"#, r#""foo{bar='baz',x='y'}""#);
        f(r#"foo{bar='ba}z',x='y'}"#, r#""foo{bar='ba}z',x='y'}""#);
        f(r#"{foo="ba[r",baz='a'}"#, r#""{foo=\"ba[r\",baz='a'}""#);

        // Single arg with JSON
        f(r#"[1,2,3]"#, r#""[1,2,3]""#);
        f(r#"{"foo":"ba,r",baz:x}"#, r#""{\"foo\":\"ba,r\",baz:x}""#);

        // Single quoted arg
        f(r#""foo""#, r#"foo"#);
        f(r#""fo,'o""#, r#""fo,'o""#);
        f(r#""f\\o,\'\"o""#, r#""f\\o,\\'\"o""#);
        f(r#""foo{bar='baz',x='y'}""#, r#""foo{bar='baz',x='y'}""#);
        f(r#"'foo'"#, r#"foo"#);
        f(r#"'fo,"o'"#, r#""fo,\"o""#);
        f(r#"'f\\o,\'\"o'"#, r#""f\\o,'\\\"o""#);
        f(r#"'foo{bar="baz",x="y"}'"#, r#""foo{bar=\"baz\",x=\"y\"}""#);

        // Multiple args
        f(r#"foo,bar,baz"#, r#"foo,bar,baz"#);
        f(r#""foo",'bar',{[(ba'",z""#, r#"foo,bar,"{[(ba'\",z\"""#);
        f(r#"foo,b"'ar,"baz,d"#, r#"foo,"b\"'ar,\"baz",d"#);
        f(
            r#"{foo="b,ar"},baz{x="y",z="d"}"#,
            r#""{foo=\"b,ar\"}","baz{x=\"y\",z=\"d\"}""#,
        );

        // Empty args
        f(r#""""#, r#""#);
        f(r#"''"#, r#""#);
        f(r#","#, r#","#);
        f(r#",foo,,ba"r,"#, r#",foo,,"ba\"r","#);

        // Special chars inside double quotes
        f(r#""foo,b\nar""#, r#""foo,b\nar""#);
        f(r#""foo\x23bar""#, "foo\u{23}bar");
    }

    #[test]
    fn test_go_unquote_high_byte_escapes() {
        // PORT NOTE divergence pin: Go strconv.Unquote emits the raw byte for
        // \xHH/octal escapes >= 0x80 (here bytes 61 80 62); Rust strings must
        // stay valid UTF-8, so the port yields the Unicode scalar U+0080
        // (bytes 61 c2 80 62).
        assert_eq!(go_unquote(r#""a\x80b""#), Ok("a\u{80}b".to_string()));
        assert_eq!(go_unquote(r#""a\200b""#), Ok("a\u{80}b".to_string()));
        // Escapes < 0x80 are byte-identical with Go.
        assert_eq!(go_unquote(r#""a\x7fb""#), Ok("a\u{7f}b".to_string()));
        assert_eq!(go_unquote(r#""a\101b""#), Ok("aAb".to_string()));
    }

    #[test]
    fn test_array_flags_append_across_occurrences() {
        // Go's flag.Parse calls Set once per `-foo=...` occurrence; array
        // flags append. Mirrors TestArrayString/TestArrayDuration/... driving
        // repeated os.Args entries through flag.Parse.
        fn occ(list: &[&str]) -> Vec<String> {
            list.iter().map(|s| s.to_string()).collect()
        }

        let mut a = ArrayString::default();
        a.set_flag_occurrences(&occ(&["foo", "bar,baz"])).unwrap();
        assert_eq!(
            a.0,
            vec!["foo".to_string(), "bar".to_string(), "baz".to_string()]
        );

        let mut d = ArrayDuration::with_default(Duration::from_secs(42));
        d.set_flag_occurrences(&occ(&["10s", "5m,"])).unwrap();
        assert_eq!(d.to_string(), "10s,5m0s,42s");

        let mut b = ArrayBool::default();
        b.set_flag_occurrences(&occ(&["true", "false,true"]))
            .unwrap();
        assert_eq!(b.0, vec![true, false, true]);

        let mut i = ArrayInt::with_default(7);
        i.set_flag_occurrences(&occ(&["1,2", "3"])).unwrap();
        assert_eq!(i.values(), &[1, 2, 3]);

        let mut y = ArrayBytes::with_default(42);
        y.set_flag_occurrences(&occ(&["10MB", "23"])).unwrap();
        assert_eq!(y.to_string(), "10MB,23");

        // An invalid occurrence errors with the offending raw value, like Go.
        let mut i = ArrayInt::with_default(7);
        let err = i.set_flag_occurrences(&occ(&["1", "oops"])).unwrap_err();
        assert_eq!(err.value, "oops");
    }

    #[test]
    fn test_array_duration_negative_values() {
        // Go time.ParseDuration accepts negative durations and
        // GetOptionalArg returns them as-is; the Duration-returning
        // convenience accessor clamps to zero (PORT NOTE at the site).
        let mut a = ArrayDuration::with_default(Duration::from_secs(42));
        a.set("-5s,1m").unwrap();
        assert_eq!(a.get_optional_arg_nanos(0), -5_000_000_000);
        assert_eq!(a.get_optional_arg_nanos(1), 60_000_000_000);
        assert_eq!(a.get_optional_arg(0), Duration::ZERO);
        assert_eq!(a.to_string(), "-5s,1m0s");
    }

    #[test]
    fn test_array_string_get_optional_arg() {
        fn f(s: &str, arg_idx: usize, expected_value: &str) {
            let mut a = ArrayString::default();
            a.set(s).unwrap();
            let v = a.get_optional_arg(arg_idx);
            assert_eq!(v, expected_value, "unexpected value for {s:?}[{arg_idx}]");
        }
        f("", 0, "");
        f("", 1, "");
        f("foo", 0, "foo");
        f("foo", 23, "foo");
        f("foo,bar", 0, "foo");
        f("foo,bar", 1, "bar");
        f("foo,bar", 2, "");
    }

    #[test]
    fn test_array_string_string() {
        fn f(s: &str) {
            let mut a = ArrayString::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, s, "unexpected string");
        }
        f("");
        f("foo");
        f("foo,bar");
        f(",");
        f(",foo,");
        f(r#"", foo","b\"ar","#);
        f(r#","\nfoo\\",bar"#);
        f(r#""foo{bar=~\"baz\",a!=\"b\"}","{a='b,{[(c'}""#);
    }

    #[test]
    fn test_array_duration() {
        let mut foo_flag_duration = ArrayDuration::default();
        foo_flag_duration.set("10s").unwrap();
        foo_flag_duration.set("5m").unwrap();
        let expected = vec![Duration::from_secs(10), Duration::from_secs(5 * 60)];
        let got: Vec<Duration> = (0..foo_flag_duration.len())
            .map(|i| foo_flag_duration.get_optional_arg(i))
            .collect();
        assert_eq!(expected, got);
    }

    #[test]
    fn test_array_duration_set() {
        fn f(s: &str, expected_result: &str) {
            let mut a = ArrayDuration::with_default(Duration::from_secs(42));
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, expected_result, "unexpected values parsed");
        }
        f("", "42s");
        f("1m", "1m0s");
        f("5m,1s,1h", "5m0s,1s,1h0m0s");
        f("5m,,1h", "5m0s,42s,1h0m0s");
    }

    #[test]
    fn test_array_duration_get_optional_arg() {
        fn f(s: &str, arg_idx: usize, default_value: Duration, expected_value: Duration) {
            let mut a = ArrayDuration::with_default(default_value);
            a.set(s).unwrap();
            let v = a.get_optional_arg(arg_idx);
            assert_eq!(v, expected_value, "unexpected value");
        }
        f("", 0, Duration::from_secs(1), Duration::from_secs(1));
        f("", 1, Duration::from_secs(60), Duration::from_secs(60));
        f(
            "10s,1m",
            1,
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        f("10s", 3, Duration::from_secs(60), Duration::from_secs(10));
    }

    #[test]
    fn test_array_duration_string() {
        fn f(s: &str) {
            let mut a = ArrayDuration::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, s, "unexpected string");
        }
        f("10s,1m0s");
        f("5m0s,1s");
    }

    #[test]
    fn test_array_bool() {
        let mut foo_flag_bool = ArrayBool::default();
        foo_flag_bool.set("true").unwrap();
        foo_flag_bool.set("false,true").unwrap();
        foo_flag_bool.set("true").unwrap();
        let expected = ArrayBool(vec![true, false, true, true]);
        assert_eq!(expected, foo_flag_bool);
    }

    #[test]
    fn test_array_bool_set() {
        fn f(s: &str, expected_result: &str) {
            let mut a = ArrayBool::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, expected_result, "unexpected values parsed");
        }
        f("", "false");
        f("true", "true");
        f("false,True,False", "false,true,false");
        f("1,,False", "true,false,false");
    }

    #[test]
    fn test_array_bool_get_optional_arg() {
        fn f(s: &str, arg_idx: usize, expected_value: bool) {
            let mut a = ArrayBool::default();
            a.set(s).unwrap();
            let v = a.get_optional_arg(arg_idx);
            assert_eq!(v, expected_value, "unexpected value");
        }
        f("", 0, false);
        f("", 1, false);
        f("true,true,false", 1, true);
        f("true", 2, true);
    }

    #[test]
    fn test_array_bool_string() {
        fn f(s: &str) {
            let mut a = ArrayBool::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, s, "unexpected string");
        }
        f("true");
        f("true,false");
        f("false,true");
    }

    #[test]
    fn test_array_int() {
        let mut foo_flag_int = ArrayInt::default();
        foo_flag_int.set("1").unwrap();
        foo_flag_int.set("2,3").unwrap();
        assert_eq!(foo_flag_int.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_array_int_set() {
        fn f(s: &str, expected_result: &str, expected_values: &[i64]) {
            let mut a = ArrayInt::with_default(42);
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, expected_result, "unexpected values parsed");
            assert_eq!(a.values(), expected_values, "unexpected values");
        }
        f("", "42", &[42]);
        f("1", "1", &[1]);
        f("-2,3,-64", "-2,3,-64", &[-2, 3, -64]);
        f(",,-64,", "42,42,-64,42", &[42, 42, -64, 42]);
    }

    #[test]
    fn test_array_int_get_optional_arg() {
        fn f(s: &str, arg_idx: usize, default_value: i64, expected_value: i64) {
            let mut a = ArrayInt::with_default(default_value);
            a.set(s).unwrap();
            let v = a.get_optional_arg(arg_idx);
            assert_eq!(v, expected_value, "unexpected value");
        }
        f("", 0, 123, 123);
        f("", 1, -34, -34);
        f("10,1", 1, 234, 1);
        f("10", 3, -34, 10);
    }

    #[test]
    fn test_array_int_string() {
        fn f(s: &str) {
            let mut a = ArrayInt::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, s, "unexpected string");
        }
        f("10,1");
        f("-5,1,123");
    }

    #[test]
    fn test_array_bytes() {
        let mut foo_flag_bytes = ArrayBytes::default();
        foo_flag_bytes.set("10MB").unwrap();
        foo_flag_bytes.set("23,10kib").unwrap();
        let expected: &[i64] = &[10_000_000, 23, 10_240];
        let result: Vec<i64> = foo_flag_bytes.a.iter().map(|b| b.n).collect();
        assert_eq!(expected, result.as_slice(), "unexpected flag values");
    }

    #[test]
    fn test_array_bytes_set() {
        fn f(s: &str, expected_result: &str) {
            let mut a = ArrayBytes::with_default(42);
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, expected_result, "unexpected values parsed");
        }
        f("", "42");
        f("1", "1");
        f("-2,3,10kb", "-2,3,10KB");
        f(",,10kb", "42,42,10KB");
    }

    #[test]
    fn test_array_bytes_get_optional_arg() {
        fn f(s: &str, arg_idx: usize, default_value: i64, expected_value: i64) {
            let mut a = ArrayBytes::with_default(default_value);
            a.set(s).unwrap();
            let v = a.get_optional_arg(arg_idx);
            assert_eq!(v, expected_value, "unexpected value");
        }
        f("", 0, 123, 123);
        f("", 1, -34, -34);
        f("10,1", 1, 234, 1);
        f("10,1", 3, 234, 234);
        f("10Kb", 3, -34, 10_000);
    }

    #[test]
    fn test_array_bytes_string() {
        fn f(s: &str) {
            let mut a = ArrayBytes::default();
            a.set(s).unwrap();
            let result = a.to_string();
            assert_eq!(result, s, "unexpected string");
        }
        f("10.5KiB,1");
        f("-5,1,123MB");
    }
}
