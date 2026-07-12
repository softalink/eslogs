//! Port of Softalink LLC `lib/stringsutil`.

use std::borrow::Cow;
use std::fmt::Write as _;

/// Returns JSON-quoted `s`.
///
/// PORT NOTE: Go delegates to `quicktemplate.AppendJSONString(nil, s, true)`;
/// the escaping table below reproduces it exactly: `\n \r \t \b \f " \\` get
/// their two-char escapes, `<` becomes `<`, `'` becomes `'`, and any
/// other control char below 0x20 becomes `\u00xx`.
pub fn json_string(s: &str) -> String {
    let mut dst = String::with_capacity(s.len() + 2);
    dst.push('"');
    for c in s.chars() {
        match c {
            '\n' => dst.push_str("\\n"),
            '\r' => dst.push_str("\\r"),
            '\t' => dst.push_str("\\t"),
            '\u{0008}' => dst.push_str("\\b"),
            '\u{000c}' => dst.push_str("\\f"),
            '"' => dst.push_str("\\\""),
            '\\' => dst.push_str("\\\\"),
            '<' => dst.push_str("\\u003c"),
            '\'' => dst.push_str("\\u0027"),
            c if (c as u32) < 0x20 => {
                write!(dst, "\\u{:04x}", c as u32).unwrap();
            }
            c => dst.push(c),
        }
    }
    dst.push('"');
    dst
}

/// Returns true if `a` is less than `b` using natural sort comparison.
///
/// See <https://en.wikipedia.org/wiki/Natural_sort_order>
pub fn less_natural(a: &str, b: &str) -> bool {
    let mut a = a.as_bytes();
    let mut b = b.as_bytes();
    let mut is_reverse = false;
    loop {
        if a.len() > b.len() {
            std::mem::swap(&mut a, &mut b);
            is_reverse = !is_reverse;
        }

        // Skip common prefix except of decimal digits
        let mut i = 0;
        while i < a.len() {
            let c_a = a[i];
            let c_b = b[i];

            if c_a.is_ascii_digit() {
                if c_b.is_ascii_digit() {
                    break;
                }
                return !is_reverse;
            }
            if c_b.is_ascii_digit() {
                return is_reverse;
            }
            if c_a != c_b {
                // This should work properly for utf8 bytes in the middle of encoded
                // unicode char, since:
                // - utf8 bytes for multi-byte chars are bigger than decimal digit chars
                // - sorting of utf8-encoded strings works properly thanks to utf8 properties
                if is_reverse {
                    return c_b < c_a;
                }
                return c_a < c_b;
            }

            i += 1;
        }
        a = &a[i..];
        b = &b[i..];
        if a.is_empty() {
            if is_reverse {
                return false;
            }
            return !b.is_empty();
        }

        // Collect digit prefixes for a and b and then compare them.

        let mut i_a = 1;
        let mut n_a = u64::from(a[0] - b'0');
        while i_a < a.len() {
            let c = a[i_a];
            if !c.is_ascii_digit() {
                break;
            }
            if n_a > (u64::MAX - 9) / 10 {
                // Too big integer. Fall back to string comparison
                if is_reverse {
                    return b < a;
                }
                return a < b;
            }
            n_a *= 10;
            n_a += u64::from(c - b'0');
            i_a += 1;
        }

        let mut i_b = 1;
        let mut n_b = u64::from(b[0] - b'0');
        while i_b < b.len() {
            let c = b[i_b];
            if !c.is_ascii_digit() {
                break;
            }
            if n_b > (u64::MAX - 9) / 10 {
                // Too big integer. Fall back to string comparison
                if is_reverse {
                    return b < a;
                }
                return a < b;
            }
            n_b *= 10;
            n_b += u64::from(c - b'0');
            i_b += 1;
        }

        if n_a != n_b {
            if is_reverse {
                return n_b < n_a;
            }
            return n_a < n_b;
        }

        if i_a != i_b {
            if is_reverse {
                return i_b < i_a;
            }
            return i_a < i_b;
        }

        a = &a[i_a..];
        b = &b[i_b..];
    }
}

/// Limits the length of `s` with `max_len`.
///
/// If `s.len() > max_len`, then `s` is replaced with `"s_prefix..s_suffix"`,
/// so the total length of the returned string doesn't exceed `max_len`.
///
/// PORT NOTE: Go slices the string by bytes, which may split a multi-byte
/// UTF-8 char and keep the raw bytes; Rust strings must stay valid UTF-8, so
/// split multi-byte chars are replaced via `String::from_utf8_lossy`. The
/// results are identical for ASCII inputs.
pub fn limit_string_len(s: &str, max_len: usize) -> Cow<'_, str> {
    let max_len = max_len.max(4);
    if s.len() <= max_len {
        return Cow::Borrowed(s);
    }
    let n = (max_len / 2) - 1;
    let mut b = Vec::with_capacity(2 * n + 2);
    b.extend_from_slice(&s.as_bytes()[..n]);
    b.extend_from_slice(b"..");
    b.extend_from_slice(&s.as_bytes()[s.len() - n..]);
    Cow::Owned(String::from_utf8_lossy(&b).into_owned())
}

/// Appends lowercase `s` to `dst`.
///
/// It is faster alternative to allocating a lowercased `String`.
///
/// PORT NOTE: Go returns the extended slice; the port appends to `dst` in
/// place. Go's `unicode.ToLower` maps a rune to a single rune, while Rust's
/// `char::to_lowercase` applies full Unicode lowercasing (which may expand,
/// e.g. `İ`); both agree on all 1:1 mappings.
pub fn append_lowercase(dst: &mut Vec<u8>, s: &str) {
    let dst_len = dst.len();

    // Try fast path at first by assuming that s contains only ASCII chars.
    let mut has_unicode_chars = false;
    for &c in s.as_bytes() {
        if !c.is_ascii() {
            has_unicode_chars = true;
            break;
        }
        dst.push(c.to_ascii_lowercase());
    }
    if has_unicode_chars {
        // Slow path - s contains non-ASCII chars. Use Unicode encoding.
        dst.truncate(dst_len);
        let mut buf = [0u8; 4];
        for r in s.chars() {
            for lc in r.to_lowercase() {
                dst.extend_from_slice(lc.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_string() {
        fn f(s: &str, result_expected: &str) {
            let result = json_string(s);
            assert_eq!(
                result, result_expected,
                "unexpected result\ngot\n{result}\nwant\n{result_expected}"
            );
        }

        f("", r#""""#);
        f("foo", r#""foo""#);
        f(
            "\n\u{8}\u{c}\t\"acЫВА'\\",
            "\"\\n\\b\\f\\t\\\"acЫВА\\u0027\\\\\"",
        );
    }

    #[test]
    fn test_less_natural() {
        fn f(a: &str, b: &str, result_expected: bool) {
            let result = less_natural(a, b);
            assert_eq!(
                result, result_expected,
                "unexpected result for less_natural({a:?}, {b:?}); got {result}; want {result_expected}"
            );
        }

        // comparison with empty string
        f("", "", false);
        f("", "foo", true);
        f("foo", "", false);
        f("", "123", true);
        f("123", "", false);

        // identical values
        f("foo", "foo", false);
        f("123", "123", false);
        f("foo123", "foo123", false);
        f("123foo", "123foo", false);
        f("000", "000", false);
        f("00123", "00123", false);
        f("00foo", "00foo", false);
        f("abc00foo0123", "abc00foo0123", false);

        // identical values with different number of zeroes in front of them
        f("00123", "0123", false);
        f("0123", "00123", true);

        // numeric comparison
        f("123", "99", false);
        f("99", "123", true);

        // negative numbers (works unexpectedly - this is OK for natural sort order)
        f("-93", "5", false);
        f("5", "-93", true);
        f("-9", "-5", false);
        f("-5", "-9", true);
        f("-93", "foo", true);
        f("foo", "-93", false);
        f("foo-9", "foo-10", true);
        f("foo-10", "foo-9", false);

        // floating-point comparison (works unexpectedly - this is OK for natural sort order)
        f("1.23", "1.123", true);
        f("1.123", "1.23", false);

        // non-numeric comparison
        f("foo", "bar", false);
        f("fo", "bar", false);
        f("bar", "foo", true);
        f("bar", "fo", true);

        // comparison with common non-numeric prefix
        f("abc_foo", "abc_bar", false);
        f("abc_bar", "abc_foo", true);
        f("abc_foo", "abc_", false);
        f("abc_", "abc_foo", true);
        f("abc_123", "abc_foo", true);
        f("abc_foo", "abc_123", false);

        // comparison with common numeric prefix
        f("123foo", "123bar", false);
        f("123bar", "123foo", true);
        f("123", "123bar", true);
        f("123bar", "123", false);
        f("123_456", "123_78", false);
        f("123_78", "123_456", true);

        // too big integers - fall back to string order
        f(
            "1234567890123456789012345",
            "1234567890123456789012345",
            false,
        );
        f(
            "1234567890123456789012345",
            "123456789012345678901234",
            false,
        );
        f(
            "123456789012345678901234",
            "1234567890123456789012345",
            true,
        );
        f(
            "193456789012345678901234",
            "1234567890123456789012345",
            false,
        );
        f(
            "123456789012345678901234",
            "1934567890123456789012345",
            true,
        );
        f("1934", "1234567890123456789012345", false);
        f("1234567890123456789012345", "1934", true);

        // integers with many zeroes in front
        f(
            "00000000000000000000000000123",
            "0000000000000000000000000045",
            false,
        );
        f(
            "0000000000000000000000000045",
            "00000000000000000000000000123",
            true,
        );

        // unicode strings
        f("бвг", "мирг", true);
        f("мирг", "бвг", false);
        f("abcde", "мирг", true);
        f("мирг", "abcde", false);
        f("123", "мирг", true);
        f("мирг", "123", false);
        f("12345", "мирг", true);
        f("мирг", "12345", false);
    }

    #[test]
    fn test_limit_string_len() {
        fn f(s: &str, max_len: usize, result_expected: &str) {
            let result = limit_string_len(s, max_len);
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result:?}; want {result_expected:?}"
            );
        }

        f("", 1, "");
        f("a", 10, "a");
        f("abc", 2, "abc");
        f("abcd", 3, "abcd");
        f("abcde", 3, "a..e");
        f("abcde", 4, "a..e");
        f("abcde", 5, "abcde");
    }

    #[test]
    fn test_append_lowercase() {
        fn f(s: &str, result_expected: &str) {
            let mut result = Vec::new();
            append_lowercase(&mut result, s);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result; got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        }

        f("", "");
        f("foo", "foo");
        f("FOO", "foo");
        f("foo БаР baz 123", "foo бар baz 123");
    }
}
