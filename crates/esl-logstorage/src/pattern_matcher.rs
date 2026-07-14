//! Port of EsLogs `lib/logstorage/pattern_matcher.go`.

// TODO: remove once the upstream consumer of this module
// (filter_pattern_match.go) is ported; until then the crate-private API is
// only exercised by the tests below.
#![allow(dead_code)]

use std::fmt;

use crate::pattern::{quoted_prefix, unquote_char};
use crate::tokenizer::{is_token_char, is_token_rune};

pub(crate) struct PatternMatcher {
    pmo: PatternMatcherOption,

    separators: Vec<String>,
    placeholders: Vec<PatternMatcherPlaceholder>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PatternMatcherOption {
    Any,
    Full,
    Prefix,
    Suffix,
}

impl fmt::Display for PatternMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, sep) in self.separators.iter().enumerate() {
            f.write_str(sep)?;
            if i < self.placeholders.len() {
                write!(f, "{}", self.placeholders[i])?;
            }
        }
        Ok(())
    }
}

// See appendPrettifyCollapsedNums()
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PatternMatcherPlaceholder {
    Unknown,
    Num,
    Uuid,
    Ip4,
    Time,
    Date,
    DateTime,
    Word,
}

fn get_pattern_matcher_placeholder(s: &str) -> PatternMatcherPlaceholder {
    match s {
        "<N>" => PatternMatcherPlaceholder::Num,
        "<UUID>" => PatternMatcherPlaceholder::Uuid,
        "<IP4>" => PatternMatcherPlaceholder::Ip4,
        "<TIME>" => PatternMatcherPlaceholder::Time,
        "<DATE>" => PatternMatcherPlaceholder::Date,
        "<DATETIME>" => PatternMatcherPlaceholder::DateTime,
        "<W>" => PatternMatcherPlaceholder::Word,
        _ => PatternMatcherPlaceholder::Unknown,
    }
}

impl fmt::Display for PatternMatcherPlaceholder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PatternMatcherPlaceholder::Unknown => "<UNKNOWN>",
            PatternMatcherPlaceholder::Num => "<N>",
            PatternMatcherPlaceholder::Uuid => "<UUID>",
            PatternMatcherPlaceholder::Ip4 => "<IP4>",
            PatternMatcherPlaceholder::Time => "<TIME>",
            PatternMatcherPlaceholder::Date => "<DATE>",
            PatternMatcherPlaceholder::DateTime => "<DATETIME>",
            PatternMatcherPlaceholder::Word => "<W>",
        };
        f.write_str(s)
    }
}

pub(crate) fn new_pattern_matcher(s: &str, pmo: PatternMatcherOption) -> PatternMatcher {
    let mut separators = Vec::new();
    let mut placeholders = Vec::new();

    let mut offset = 0;
    let mut separator = String::new();
    while offset < s.len() {
        let Some(n) = s[offset..].find('<') else {
            separator.push_str(&s[offset..]);
            break;
        };
        separator.push_str(&s[offset..offset + n]);
        offset += n;

        let Some(n) = s[offset..].find('>') else {
            separator.push_str(&s[offset..]);
            break;
        };
        let placeholder = &s[offset..offset + n + 1];
        offset += n + 1;

        let ph = get_pattern_matcher_placeholder(placeholder);
        if ph == PatternMatcherPlaceholder::Unknown {
            separator.push_str(placeholder);
            continue;
        }

        separators.push(std::mem::take(&mut separator));
        placeholders.push(ph);
    }
    separators.push(separator);

    PatternMatcher {
        pmo,
        separators,
        placeholders,
    }
}

impl PatternMatcher {
    /// Returns true if s matches the given pm.
    ///
    /// PORT NOTE: named `matches` because `match` is a Rust keyword (Go:
    /// `Match`).
    pub(crate) fn matches(&self, s: &str) -> bool {
        match self.pmo {
            PatternMatcherOption::Any => self.index_start_end(s, 0).is_some(),
            PatternMatcherOption::Full => self.index_end(s, 0) == Some(s.len()),
            PatternMatcherOption::Prefix => self.index_end(s, 0).is_some(),
            PatternMatcherOption::Suffix => {
                if self.is_empty() {
                    // Empty pattern matches any string.
                    return true;
                }
                // Optimization: verify that the string ends with the last separator.
                let last_separator = &self.separators[self.separators.len() - 1];
                if !s.ends_with(last_separator.as_str()) {
                    return false;
                }

                let mut offset = 0;
                loop {
                    let Some((start, end)) = self.index_start_end(s, offset) else {
                        return false;
                    };
                    if end == s.len() {
                        return true;
                    }
                    offset = start + 1;
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.separators.len() == 1 && self.separators[0].is_empty()
    }

    /// PORT NOTE: Go returns `(-1, -1)` on mismatch; the port returns `None`.
    /// The same applies to the other index helpers below.
    fn index_start_end(&self, s: &str, mut offset: usize) -> Option<(usize, usize)> {
        loop {
            let start = self.index_start(s, offset)?;
            if let Some(end) = self.index_end(s, start) {
                return Some((start, end));
            }
            offset = start + 1;
        }
    }

    fn index_start(&self, s: &str, offset: usize) -> Option<usize> {
        let first_sep = &self.separators[0];
        if !first_sep.is_empty() {
            let n = index_of(&s.as_bytes()[offset..], first_sep.as_bytes())?;
            return Some(offset + n);
        }

        let placeholders = &self.placeholders;
        if placeholders.is_empty() {
            return Some(0);
        }

        if placeholders[0] == PatternMatcherPlaceholder::Word {
            return index_word_start(s.as_bytes(), offset);
        }
        index_num_start(s.as_bytes(), offset)
    }

    fn index_end(&self, s: &str, mut offset: usize) -> Option<usize> {
        let placeholders = &self.placeholders;

        for (i, sep) in self.separators.iter().enumerate() {
            if !sep.is_empty() {
                if !s.as_bytes()[offset..].starts_with(sep.as_bytes()) {
                    return None;
                }
                offset += sep.len();
            }

            if i >= placeholders.len() {
                return Some(offset);
            }

            offset = placeholders[i].index_end(s, offset)?;
        }
        Some(offset)
    }
}

impl PatternMatcherPlaceholder {
    fn index_end(&self, s: &str, offset: usize) -> Option<usize> {
        match self {
            PatternMatcherPlaceholder::Num => index_placeholder_num_end(s.as_bytes(), offset),
            PatternMatcherPlaceholder::Uuid => index_placeholder_uuid_end(s.as_bytes(), offset),
            PatternMatcherPlaceholder::Ip4 => index_placeholder_ip4_end(s.as_bytes(), offset),
            PatternMatcherPlaceholder::Time => index_placeholder_time_end(s.as_bytes(), offset),
            PatternMatcherPlaceholder::Date => index_placeholder_date_end(s.as_bytes(), offset),
            PatternMatcherPlaceholder::DateTime => index_placeholder_date_time_end(s, offset),
            PatternMatcherPlaceholder::Word => index_placeholder_word_end(s, offset),
            PatternMatcherPlaceholder::Unknown => {
                esl_common::panicf!("BUG: unexpected patternMatcherPlaceholder=UNKNOWN");
                unreachable!()
            }
        }
    }
}

fn index_placeholder_num_end(b: &[u8], start: usize) -> Option<usize> {
    let end = index_num_end(b, start);
    if !is_valid_num(b, start, end) {
        return None;
    }
    Some(end)
}

fn index_placeholder_uuid_end(b: &[u8], start: usize) -> Option<usize> {
    // <UUID> is <N>-<N>-<N>-<N>-<N>
    index_generic_placeholder_end(b, start, 5, b'-')
}

fn index_placeholder_ip4_end(b: &[u8], start: usize) -> Option<usize> {
    // <IP4> is <N>.<N>.<N>.<N>
    index_generic_placeholder_end(b, start, 4, b'.')
}

fn index_placeholder_time_end(b: &[u8], start: usize) -> Option<usize> {
    // <TIME> is <N>:<N>:<N> with optional subseconds .<N> or ,<N>
    let end = index_generic_placeholder_end(b, start, 3, b':')?;

    // Check optional subseconds
    if end < b.len()
        && (b[end] == b'.' || b[end] == b',')
        && let Some(n) = index_placeholder_num_end(b, end + 1)
    {
        return Some(n);
    }

    Some(end)
}

fn index_placeholder_date_end(b: &[u8], start: usize) -> Option<usize> {
    // <DATE> is <N>-<N>-<N> or <N>/<N>/<N>
    if let Some(end) = index_generic_placeholder_end(b, start, 3, b'-') {
        return Some(end);
    }
    index_generic_placeholder_end(b, start, 3, b'/')
}

fn index_placeholder_date_time_end(s: &str, start: usize) -> Option<usize> {
    // <DATETIME> is '<DATE>T<TIME>' or '<DATE> <TIME>' with optional timezone
    let b = s.as_bytes();
    let end = index_placeholder_date_end(b, start)?;
    if end >= b.len() || (b[end] != b'T' && b[end] != b' ') {
        return None;
    }

    let end = index_placeholder_time_end(b, end + 1)?;

    if end >= b.len() {
        return Some(end);
    }

    // Check optional timezone
    if b[end] == b'Z' {
        return Some(end + 1);
    }
    if (b[end] == b'-' || b[end] == b'+')
        && let Some(n) = index_timezone_end(b, end + 1)
    {
        return Some(n);
    }

    Some(end)
}

fn index_placeholder_word_end(s: &str, start: usize) -> Option<usize> {
    // <W> is a word or a quoted string
    let b = s.as_bytes();
    if start >= b.len() {
        return None;
    }
    if b[start] == b'"' || b[start] == b'\'' || b[start] == b'`' {
        return index_quoted_string_end(s, start);
    }
    index_word_end(b, start)
}

fn index_word_end(b: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < b.len() {
        let (r, size) = decode_rune(&b[i..]);
        if !is_token_rune(r) {
            return Some(i);
        }
        i += size;
    }
    Some(b.len())
}

fn index_word_start(b: &[u8], offset: usize) -> Option<usize> {
    let mut i = offset;
    while i < b.len() {
        let (r, size) = decode_rune(&b[i..]);
        if is_token_rune(r) {
            return Some(i);
        }
        i += size;
    }
    None
}

fn index_quoted_string_end(s: &str, start: usize) -> Option<usize> {
    let b = s.as_bytes();
    match b[start] {
        b'"' | b'`' => {
            // start is at an ASCII quote byte, so it is a char boundary.
            let qp = quoted_prefix(&s[start..]).ok()?;
            Some(start + qp.len())
        }
        b'\'' => {
            let mut end = start + 1;
            while !s.as_bytes()[end..].starts_with(b"'") {
                let (_, _, tail) = unquote_char(&s[end..], b'\'').ok()?;
                end = s.len() - tail.len();
            }
            Some(end + 1)
        }
        _ => {
            esl_common::panicf!(
                "BUG: unexpected starting char for quoted string: {}",
                b[start] as char
            );
            unreachable!()
        }
    }
}

fn index_timezone_end(b: &[u8], start: usize) -> Option<usize> {
    // Timezone is <N>:<N>
    index_generic_placeholder_end(b, start, 2, b':')
}

fn index_generic_placeholder_end(
    b: &[u8],
    start: usize,
    nums: usize,
    separator: u8,
) -> Option<usize> {
    let mut end = index_placeholder_num_end(b, start)?;
    for _ in 0..nums - 1 {
        if end >= b.len() || b[end] != separator {
            return None;
        }
        end = index_placeholder_num_end(b, end + 1)?;
    }
    Some(end)
}

// ---------------------------------------------------------------------------
// PORT NOTE: the numeric-token helpers below (index_num_start, index_num_end,
// is_valid_num and their supporting functions) live in
// `pipe_collapse_nums.go` upstream; that file has no Rust module in this
// crate yet, so private copies are kept here until `pipe_collapse_nums.rs`
// lands.
// ---------------------------------------------------------------------------

fn index_num_start(b: &[u8], offset: usize) -> Option<usize> {
    // It is safe iterating by chars instead of Unicode runes, since decimal and hex chars are ASCII
    // and they cannot clash with utf-8 encoded Unicode runes.
    let mut n = offset;
    while n < b.len() {
        if !is_decimal_or_hex_char(b[n]) {
            n += 1;
            continue;
        }
        if n == 0 {
            return Some(0);
        }
        if !is_token_char(b[n - 1]) || is_special_num_start(b[n - 1]) {
            return Some(n);
        }
        n += 1;
    }
    None
}

fn index_num_end(b: &[u8], offset: usize) -> usize {
    // It is safe iterating by chars instead of Unicode runes, since decimal and hex chars are ASCII
    // and they cannot clash with utf-8 encoded Unicode runes.
    let mut n = offset;
    while n < b.len() && is_decimal_or_hex_char(b[n]) {
        n += 1;
    }
    n
}

fn is_valid_num(b: &[u8], start: usize, end: usize) -> bool {
    if end < b.len() && is_token_char(b[end]) && !is_special_num_end(b[end]) {
        return false;
    }
    can_be_treated_as_num(&b[start..end])
}

fn is_decimal_or_hex_char(ch: u8) -> bool {
    if ch.is_ascii_digit() {
        return true;
    }
    is_hex_char(ch)
}

fn is_hex_char(ch: u8) -> bool {
    (b'a'..=b'f').contains(&ch) || (b'A'..=b'F').contains(&ch)
}

fn can_be_treated_as_num(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    if !has_hex_chars(s) {
        // Decimal number can contain any number of chars
        return true;
    }

    // The most of hex nums contain 4 and more chars, and the number of chars are usually even.
    // This prevents from incorrect detection of hex numbers such as "be", "ad", "foo", "abc", etc.
    if s.len() < 4 || s.len() % 2 == 1 {
        return false;
    }
    true
}

fn has_hex_chars(s: &[u8]) -> bool {
    for &ch in s {
        if is_hex_char(ch) {
            return true;
        }
    }
    false
}

fn is_special_num_start(ch: u8) -> bool {
    ch == b'_'
        || ch == b'T'
        || ch == b'X'
        || ch == b'x'
        || ch == b'v'
        || ch == b's'
        || ch == b'h'
        || ch == b'm'
}

fn is_special_num_end(ch: u8) -> bool {
    ch == b'_'
        || ch == b'T'
        || ch == b'Z'
        || ch == b's'
        || ch == b'm'
        || ch == b'h'
        || ch == b'u'
        || ch == b'n'
}

// ---------------------------------------------------------------------------
// Byte-level helpers.
// ---------------------------------------------------------------------------

/// Byte-level substring search (Go `strings.Index` operates on bytes; Rust
/// `str::find` would panic when `haystack` starts at a non-char boundary).
fn index_of(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Port of Go `utf8.DecodeRune` semantics for iterating runes from an
/// arbitrary byte offset: invalid or truncated sequences decode as
/// (U+FFFD, 1).
pub(crate) fn decode_rune(b: &[u8]) -> (char, usize) {
    let n = b.len().min(4);
    match std::str::from_utf8(&b[..n]) {
        Ok(s) => match s.chars().next() {
            Some(c) => (c, c.len_utf8()),
            None => ('\u{FFFD}', 1),
        },
        Err(e) => {
            let valid = e.valid_up_to();
            if valid > 0 {
                let c = std::str::from_utf8(&b[..valid])
                    .expect("BUG: valid_up_to prefix must be valid UTF-8")
                    .chars()
                    .next()
                    .expect("BUG: non-empty valid UTF-8 prefix must contain a char");
                (c, c.len_utf8())
            } else {
                ('\u{FFFD}', 1)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PatternMatcherOption::*;
    use super::PatternMatcherPlaceholder::*;
    use super::*;

    #[test]
    fn test_new_pattern_matcher() {
        fn f(
            s: &str,
            separators_expected: &[&str],
            placeholders_expected: &[PatternMatcherPlaceholder],
        ) {
            let pm = new_pattern_matcher(s, Any);

            let pm_str = pm.to_string();
            assert_eq!(
                s, pm_str,
                "unexpected string representation of patternMatcher\ngot\n{pm_str:?}\nwant\n{s:?}"
            );

            assert_eq!(
                pm.separators, separators_expected,
                "unexpected separators; got {:?}; want {separators_expected:?}",
                pm.separators
            );
            assert_eq!(
                pm.placeholders, placeholders_expected,
                "unexpected placeholders; got {:?}; want {placeholders_expected:?}",
                pm.placeholders
            );
        }

        f("", &[""], &[]);
        f("foobar", &["foobar"], &[]);
        f("<N>", &["", ""], &[Num]);
        f("foo<N>", &["foo", ""], &[Num]);
        f("<N>foo", &["", "foo"], &[Num]);
        f(
            "<N><UUID>foo<IP4><TIME>bar<DATETIME><DATE><W>",
            &["", "", "foo", "", "bar", "", "", ""],
            &[Num, Uuid, Ip4, Time, DateTime, Date, Word],
        );

        // unknown placeholders
        f("<foo><BAR> baz<X>y:<M>", &["<foo><BAR> baz<X>y:<M>"], &[]);
        f(
            "<foo><BAR> baz<X>y<N>:<M>",
            &["<foo><BAR> baz<X>y", ":<M>"],
            &[Num],
        );
    }

    #[test]
    fn test_pattern_matcher_match() {
        fn f(pattern: &str, s: &str, pmo: PatternMatcherOption, result_expected: bool) {
            let pm = new_pattern_matcher(pattern, pmo);
            let result = pm.matches(s);
            assert_eq!(
                result, result_expected,
                "unexpected result for pattern {pattern:?}, s {s:?}, pmo {pmo:?}; got {result}; want {result_expected}"
            );
        }

        // an empty pattern matches an empty string
        f("", "", Any, true);
        f("", "", Full, true);
        f("", "", Prefix, true);
        f("", "", Suffix, true);

        // an empty pattern matches any string in non-full mode
        f("", "foo", Any, true);
        f("", "foo", Prefix, true);
        f("", "foo", Suffix, true);

        // an empty pattern doesn't match non-empty string in full mode
        f("", "foo", Full, false);

        // pattern without paceholders, which doesn't match the given string
        f("foo", "abcd", Any, false);
        f("foo", "abcd", Full, false);
        f("foo", "abcd", Prefix, false);
        f("foo", "abcd", Suffix, false);
        f("foo", "afoo bc", Full, false);

        // pattern without placeholders, which matches the given string
        f("foo", "foo", Any, true);
        f("foo", "foo", Full, true);
        f("foo", "foo", Prefix, true);
        f("foo", "foo", Suffix, true);
        f("foo", "afoo bc", Any, true);
        f("afoo", "afoo bc", Prefix, true);
        f("bc", "afoo bc", Suffix, true);

        // pattern with placeholders
        f("<N>sec at <DATE>", "123sec at 2025-12-20", Any, true);
        f("<N>sec at <DATE>", "123sec at 2025-12-20", Full, true);
        f("<N>sec at <DATE>", "123sec at 2025-12-20", Prefix, true);
        f("<N>sec at <DATE>", "123sec at 2025-12-20", Suffix, true);

        // superflouos prefix in the string
        f("<N>sec at <DATE>", "3 123sec at 2025-12-20", Full, false);
        f("<N>sec at <DATE>", "3 123sec at 2025-12-20", Any, true);
        f("<N>sec at <DATE>", "3 123sec at 2025-12-20", Prefix, false);
        f("<N>sec at <DATE>", "3 123sec at 2025-12-20", Suffix, true);

        // superflouous suffix in the string
        f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", Full, false);
        f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", Any, true);
        f("<N>sec at <DATE>", "123sec at 2025-12-20 sss", Prefix, true);
        f(
            "<N>sec at <DATE>",
            "123sec at 2025-12-20 sss",
            Suffix,
            false,
        );

        // pattern with placeholders doesn't match the string
        f("<N> <DATE> foo", "123 456 foo", Full, false);
        f("<N> <DATE> foo", "123 456 foo", Any, false);
        f("<N> <DATE> foo", "123 456 foo", Prefix, false);
        f("<N> <DATE> foo", "123 456 foo", Suffix, false);

        // verify all the placeholders
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: '`\"\\', end', end",
            Any,
            true,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end",
            Full,
            true,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end",
            Prefix,
            true,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: `f\"'oo`, end",
            Suffix,
            true,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end",
            Any,
            true,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end",
            Prefix,
            false,
        );
        f(
            "n: <N>.<N>, uuid: <UUID>, ip4: <IP4>, time: <TIME>, date: <DATE>, datetime: <DATETIME>, user: <W>, end",
            "some 123 prefix 10:20:30, n: 123.324, uuid: 2edfed59-3e98-4073-bbb2-28d321ca71a7, ip4: 123.45.67.89, time: 10:20:30, date: 2025-10-20, datetime: 2025-10-20T10:20:30Z, user: \"f\\\"o'\", end",
            Suffix,
            true,
        );

        // verify different cases for DATE
        f(
            "<DATE>, <DATE>",
            "foo 2025/10/20, 2025-10-20 bar",
            Any,
            true,
        );
        f(
            "<DATE>, <DATE>",
            "foo 2025/10/20, 2025-10-20 bar",
            Full,
            false,
        );
        f(
            "<DATE>, <DATE>",
            "foo 2025/10/20, 2025-10-20 bar",
            Prefix,
            false,
        );
        f(
            "<DATE>, <DATE>",
            "foo 2025/10/20, 2025-10-20 bar",
            Suffix,
            false,
        );

        // verify different cases for TIME
        f(
            "<TIME>, <TIME>, <TIME>",
            "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa",
            Any,
            true,
        );
        f(
            "<TIME>, <TIME>, <TIME>",
            "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa",
            Full,
            false,
        );
        f(
            "<TIME>, <TIME>, <TIME>",
            "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa",
            Prefix,
            false,
        );
        f(
            "<TIME>, <TIME>, <TIME>",
            "foo 10:20:30, 10:20:30.12345, 10:20:30,23434 aaa",
            Suffix,
            false,
        );

        // verify different cases for DATETIME
        f(
            "<DATETIME>, <DATETIME>, <DATETIME>, <DATETIME>",
            "foo 2025-09-20T10:20:30Z, 2025/10/20 10:20:30.2343, 2025-10-20T30:40:50-05:10, 2025-10-20T30:40:50.1324+05:00 bar",
            Any,
            true,
        );

        // verify different cases for W
        f("email: <W>@<W>", "email: foo@bar.com", Any, true);
        f("email: <W>@<W>", "email: foo@bar.com", Full, false);
        f("email: <W>@<W>", "email: foo@bar.com", Prefix, true);
        f("email: <W>@<W>", "email: foo@bar.com", Suffix, false);
        f("email: <W>@<W>.<W>", "email: foo@bar.com", Full, true);
        f("email: <W>@<W>.<W>", "email: foo@bar.com", Prefix, true);
        f("email: <W>@<W>.<W>", "email: foo@bar.com", Suffix, true);
        f("email: <W>@<W>", "a email: foo@bar.com", Full, false);
        f("<W> foo", " foo", Any, false);
        f("<W> foo", ",,, foo", Any, false);
        f("<W> foo", ",,,abc foo", Any, true);

        f("\"foo\":<W>", "{\"foo\":\"bar\", \"baz\": 123}", Any, true);
        f(
            "\"foo\":<W>",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Full,
            false,
        );
        f(
            "\"foo\":<W>",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Prefix,
            false,
        );
        f(
            "\"foo\":<W>",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Suffix,
            false,
        );
        f(
            "{\"foo\":<W>",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Prefix,
            true,
        );
        f(
            "\"baz\": <N>}",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Suffix,
            true,
        );
        f(
            "\"baz\": <N>",
            "{\"foo\":\"bar\", \"baz\": 123}",
            Suffix,
            false,
        );

        // match the suffix not at the end
        f("foo:<N>", "abc foo:123 abc foo:42", Suffix, true);
        f("foo:<N>", "abc foo:123 abc foo:", Suffix, false);
        f("foo:<N>", "abc foo:123 abc", Suffix, false);
        f("foo:<N> xx", "abc foo:123 xx foo:42 xx", Suffix, true);
        f("foo:<N> xx", "abc foo:123 xx foo:42", Suffix, false);

        // regression: leading separator present many times but placeholder doesn't match after it
        f("xx<N>", "xxxxxxxxxxxxxxxx", Any, false);
        f("xx<N>", "xxxxxxxxxxxxxxxx", Full, false);
        f("xx<N>", "xxxxxxxxxxxxxxxx", Prefix, false);
        f("xx<N>", "xxxxxxxxxxxxxxxx", Suffix, false);
        f("xx<N>", "xxxxxx123", Any, true);
        f("xx<N>", "xxxxxx123", Full, false);
        f("xx<N>", "xxxxxx123", Prefix, false);
        f("xx<N>", "xxxxxx123", Suffix, true);
    }
}
