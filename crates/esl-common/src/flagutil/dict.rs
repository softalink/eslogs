//! Port of Softalink LLC `lib/flagutil/dict.go`.

use std::collections::BTreeMap;
use std::fmt;

use super::FlagValue;
use super::array::parse_array_values;

/// A flag for specifying a dictionary of named ints in the form
/// `name1:value1,...,nameN:valueN`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DictInt {
    default_value: i64,
    kvs: Vec<(String, i64)>,
}

impl DictInt {
    /// Returns an empty DictInt with the given default value, like Go
    /// `NewDictInt`.
    pub fn with_default(default_value: i64) -> Self {
        DictInt {
            default_value,
            kvs: Vec::new(),
        }
    }

    /// Parses `value`, like Go `DictInt.Set`.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        let values = parse_array_values(value);
        if self.kvs.is_empty() && values.len() == 1 && !values[0].contains(':') {
            let v: i64 = values[0]
                .parse()
                .map_err(|err| format!("cannot parse {:?} as int: {err}", values[0]))?;
            self.kvs.push((String::new(), v));
            self.default_value = v;
            return Ok(());
        }
        for x in values {
            let Some(n) = x.find(':') else {
                return Err(format!("missing ':' in {x:?}"));
            };
            let k = &x[..n];
            let v: i64 = x[n + 1..]
                .parse()
                .map_err(|err| format!("cannot parse value for key={k:?}: {err}"))?;
            if self.contains(k) {
                return Err(format!("duplicate value for key={k:?}: {v}"));
            }
            self.kvs.push((k.to_string(), v));
        }
        Ok(())
    }

    fn contains(&self, key: &str) -> bool {
        self.kvs.iter().any(|(k, _)| k == key)
    }

    /// Returns the value for the given key.
    ///
    /// The default value is returned if the key isn't found.
    pub fn get(&self, key: &str) -> i64 {
        for (k, v) in &self.kvs {
            if k == key {
                return *v;
            }
        }
        self.default_value
    }
}

impl fmt::Display for DictInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kvs.len() == 1 && self.kvs[0].0.is_empty() {
            // Short form - a single int value.
            return write!(f, "{}", self.kvs[0].1);
        }
        let formatted: Vec<String> = self.kvs.iter().map(|(k, v)| format!("{k}:{v}")).collect();
        f.write_str(&formatted.join(","))
    }
}

impl FlagValue for DictInt {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let mut di = DictInt::default();
        di.set(s)?;
        Ok(di)
    }
}

/// Parses `s`, which must contain a JSON map of `{"k1":"v1",...,"kN":"vN"}`.
///
/// Returns `None` for an empty `s` (special case, like Go returning a nil
/// map).
///
/// PORT NOTE: implemented with a minimal hand-rolled JSON parser instead of
/// `encoding/json`, to avoid a serde dependency; only string-to-string maps
/// are accepted, like in Go.
pub fn parse_json_map(s: &str) -> Result<Option<BTreeMap<String, String>>, String> {
    if s.is_empty() {
        // Special case
        return Ok(None);
    }
    let mut p = JsonParser {
        b: s.as_bytes(),
        i: 0,
    };
    let m = p.parse_map()?;
    p.skip_ws();
    if p.i != p.b.len() {
        return Err(format!("unexpected trailing data in {s:?}"));
    }
    Ok(Some(m))
}

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && (self.b[self.i] as char).is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        self.skip_ws();
        if self.i >= self.b.len() || self.b[self.i] != c {
            return Err(format!(
                "expecting {:?} at position {} in JSON map",
                c as char, self.i
            ));
        }
        self.i += 1;
        Ok(())
    }

    fn parse_map(&mut self) -> Result<BTreeMap<String, String>, String> {
        let mut m = BTreeMap::new();
        self.expect(b'{')?;
        self.skip_ws();
        if self.i < self.b.len() && self.b[self.i] == b'}' {
            self.i += 1;
            return Ok(m);
        }
        loop {
            let k = self.parse_string()?;
            self.expect(b':')?;
            let v = self.parse_string()?;
            m.insert(k, v);
            self.skip_ws();
            if self.i >= self.b.len() {
                return Err("unexpected end of JSON map".to_string());
            }
            match self.b[self.i] {
                b',' => self.i += 1,
                b'}' => {
                    self.i += 1;
                    return Ok(m);
                }
                c => {
                    return Err(format!(
                        "unexpected char {:?} in JSON map at position {}",
                        c as char, self.i
                    ));
                }
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    if self.i >= self.b.len() {
                        return Err("unexpected end of JSON string".to_string());
                    }
                    let c = self.b[self.i];
                    self.i += 1;
                    match c {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\x08'),
                        b'f' => out.push('\x0c'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            if self.i + 4 > self.b.len() {
                                return Err("truncated \\u escape in JSON string".to_string());
                            }
                            let hex = std::str::from_utf8(&self.b[self.i..self.i + 4])
                                .map_err(|_| "invalid \\u escape".to_string())?;
                            let v = u32::from_str_radix(hex, 16)
                                .map_err(|_| "invalid \\u escape".to_string())?;
                            let c = char::from_u32(v)
                                .ok_or_else(|| "invalid \\u escape".to_string())?;
                            out.push(c);
                            self.i += 4;
                        }
                        c => {
                            return Err(format!(
                                "unsupported escape char {:?} in JSON string",
                                c as char
                            ));
                        }
                    }
                }
                _ => {
                    // Consume one full UTF-8 character.
                    let rest = std::str::from_utf8(&self.b[self.i..])
                        .map_err(|_| "invalid UTF-8 in JSON string".to_string())?;
                    let c = rest.chars().next().unwrap();
                    out.push(c);
                    self.i += c.len_utf8();
                }
            }
        }
        Err("unexpected end of JSON string".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marshal_json_map(m: &BTreeMap<String, String>) -> String {
        let pairs: Vec<String> = m
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}:{}",
                    super::super::go_quote(k),
                    super::super::go_quote(v)
                )
            })
            .collect();
        format!("{{{}}}", pairs.join(","))
    }

    #[test]
    fn test_parse_json_map_success() {
        fn f(s: &str) {
            let m = parse_json_map(s).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            if s.is_empty() {
                assert!(m.is_none());
                return;
            }
            let data = marshal_json_map(&m.unwrap());
            assert_eq!(s, data, "unexpected result");
        }

        f("");
        f("{}");
        f(r#"{"foo":"bar"}"#);
        f(r#"{"a":"b","c":"d"}"#);
    }

    #[test]
    fn test_parse_json_map_failure() {
        fn f(s: &str) {
            let m = parse_json_map(s);
            assert!(m.is_err(), "expecting non-nil error for {s:?}");
        }

        f("foo");
        f("123");
        f("{");
        f(r#"{foo:bar}"#);
        f(r#"{"foo":1}"#);
        f(r#"[]"#);
        f(r#"{"foo":"bar","a":[123]}"#);
    }

    #[test]
    fn test_dict_int_set_success() {
        fn f(s: &str) {
            let mut di = DictInt::default();
            di.set(s)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            let result = di.to_string();
            assert_eq!(result, s, "unexpected DictInt.to_string()");
        }

        f("123");
        f("-234");
        f("foo:123");
        f("foo:123,bar:-42,baz:0,aa:43");
    }

    #[test]
    fn test_dict_int_failure() {
        fn f(s: &str) {
            let mut di = DictInt::default();
            assert!(di.set(s).is_err(), "expecting non-nil error for {s:?}");
        }

        // missing values
        f("foo");
        f("foo:");

        // non-integer values
        f("foo:bar");
        f("12.34");
        f("foo:123.34");

        // duplicate keys
        f("a:234,k:123,k:432");
    }

    #[test]
    fn test_dict_int_get() {
        fn f(s: &str, key: &str, default_value: i64, expected_value: i64) {
            let mut di = DictInt::with_default(default_value);
            di.set(s)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            let value = di.get(key);
            assert_eq!(value, expected_value, "unexpected value");
        }

        f("foo:42", "", 123, 123);
        f("foo:42", "foo", 123, 42);
        f("532", "", 123, 532);
        f("532", "foo", 123, 532);
    }
}
