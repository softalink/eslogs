//! Port of EsLogs `lib/logstorage/pattern.go`.
//!
//! Also hosts private ports of the Go stdlib helpers the upstream file relies
//! on: `strconv.QuotedPrefix` / `strconv.Unquote` / `strconv.UnquoteChar`
//! (Go quoted-string syntax) and `html.UnescapeString`.

// TODO: remove once the upstream consumers of this module (pipe_extract.go,
// filter_pattern_match.go, ...) are ported; until then the crate-private API
// is only exercised by the tests below and by sibling parser modules.
#![allow(dead_code)]

use crate::prefix_filter;

/// Pattern represents text pattern in the form `some_text<some_field>other_text...`
#[derive(Debug)]
pub(crate) struct Pattern {
    /// steps contains steps for extracting fields from string
    pub(crate) steps: Vec<PatternStep>,

    /// matches contains matches for every step in steps
    pub(crate) matches: Vec<String>,

    /// fields contains matches for non-empty fields
    pub(crate) fields: Vec<PatternField>,
}

#[derive(Debug)]
pub(crate) struct PatternField {
    pub(crate) name: String,

    /// PORT NOTE: Go stores `value *string` pointing into `pattern.matches`;
    /// the port stores the index into `matches` instead. Use
    /// `Pattern::field_value()` to read the matched value.
    pub(crate) match_idx: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PatternStep {
    pub(crate) prefix: String,

    pub(crate) field: String,
    pub(crate) field_opt: String,
}

impl Clone for Pattern {
    /// PORT NOTE: Go's `pattern.clone()` shares the immutable `steps` slice
    /// between clones and allocates fresh `matches`/`fields`; the port
    /// deep-clones `steps`. `matches` contents are not copied — they are
    /// transient per-`apply()` state, exactly as in Go.
    fn clone(&self) -> Pattern {
        let matches = vec![String::new(); self.steps.len()];
        let mut fields = Vec::new();
        for (i, step) in self.steps.iter().enumerate() {
            if !step.field.is_empty() {
                fields.push(PatternField {
                    name: step.field.clone(),
                    match_idx: i,
                });
            }
        }

        Pattern {
            steps: self.steps.clone(),
            matches,
            fields,
        }
    }
}

pub(crate) fn parse_pattern(s: &str) -> Result<Pattern, String> {
    let steps = parse_pattern_steps(s)?;

    // Verify that prefixes are non-empty between fields. The first prefix may be empty.
    for i in 1..steps.len() {
        if steps[i].prefix.is_empty() {
            return Err(format!(
                "missing delimiter between <{}> and <{}>",
                steps[i - 1].field,
                steps[i].field
            ));
        }
    }

    // Verify that fields do not end with '*'
    for step in &steps {
        if prefix_filter::is_wildcard_filter(&step.field) {
            return Err(format!("wildcard field {:?} isn't supported", step.field));
        }
    }

    // Build pattern struct

    let matches = vec![String::new(); steps.len()];

    let mut fields = Vec::new();
    for (i, step) in steps.iter().enumerate() {
        if !step.field.is_empty() {
            fields.push(PatternField {
                name: step.field.clone(),
                match_idx: i,
            });
        }
    }
    if fields.is_empty() {
        return Err(format!(
            "pattern {s:?} must contain at least a single named field in the form <field_name>"
        ));
    }

    Ok(Pattern {
        steps,
        matches,
        fields,
    })
}

impl Pattern {
    /// Returns the value matched for the given field.
    pub(crate) fn field_value(&self, f: &PatternField) -> &str {
        &self.matches[f.match_idx]
    }

    pub(crate) fn apply(&mut self, s: &str) {
        let Pattern { steps, matches, .. } = self;
        for m in matches.iter_mut() {
            m.clear();
        }

        let Some((n, prefix_len)) = prefix_index(s, &steps[0].prefix) else {
            // Mismatch
            return;
        };
        let mut s = &s[n + prefix_len..];

        for i in 0..steps.len() {
            let next_prefix = if i + 1 < steps.len() {
                steps[i + 1].prefix.as_str()
            } else {
                ""
            };

            if let Some((us, n_offset)) = try_unquote_string(s, &steps[i].field_opt) {
                // Matched quoted string
                matches[i].push_str(&us);
                s = &s[n_offset..];
                if !s.starts_with(next_prefix) {
                    // Mismatch
                    return;
                }
                s = &s[next_prefix.len()..];
            } else {
                // Match unquoted string until the nextPrefix
                if next_prefix.is_empty() {
                    matches[i].push_str(s);
                    return;
                }
                let Some((n, prefix_len)) = prefix_index(s, next_prefix) else {
                    // Mismatch
                    return;
                };
                matches[i].push_str(&s[..n]);
                s = &s[n + prefix_len..];
            }
        }
    }
}

/// PORT NOTE: Go returns `(-1, 0)` on mismatch; the port returns `None`.
fn prefix_index(s: &str, prefix: &str) -> Option<(usize, usize)> {
    if prefix.is_empty() {
        return Some((0, 0));
    }
    let n = s.find(prefix)?;
    Some((n, prefix.len()))
}

/// PORT NOTE: Go returns `("", -1)` on mismatch; the port returns `None`.
/// The unquoted string is copied into an owned `String` (Go's
/// `strconv.Unquote` allocates as well).
pub(crate) fn try_unquote_string(s: &str, opt: &str) -> Option<(String, usize)> {
    if opt == "plain" {
        return None;
    }
    if s.is_empty() {
        return None;
    }

    match s.as_bytes()[0] {
        b'"' | b'`' => {
            let qp = quoted_prefix(s).ok()?;
            let us = unquote(qp).ok()?;
            Some((us, qp.len()))
        }
        b'\'' => {
            let mut b = String::new();
            let mut tail = &s[1..];
            while !tail.starts_with('\'') {
                let (ch, _, rest) = unquote_char(tail, b'\'').ok()?;
                b.push(ch);
                tail = rest;
            }
            Some((b, s.len() - tail.len() + 1))
        }
        _ => None,
    }
}

pub(crate) fn parse_pattern_steps(s: &str) -> Result<Vec<PatternStep>, String> {
    let mut steps = parse_pattern_steps_internal(s)?;

    // unescape prefixes
    for step in &mut steps {
        step.prefix = html_unescape_string(&step.prefix);
    }

    // extract options part from fields
    for step in &mut steps {
        let field = std::mem::take(&mut step.field);
        let mut fs: &str = &field;
        if let Some(n) = fs.find(':') {
            step.field_opt = fs[..n].trim().to_string();
            fs = &fs[n + 1..];
        }
        step.field = fs.trim().to_string();
    }

    Ok(steps)
}

fn parse_pattern_steps_internal(s: &str) -> Result<Vec<PatternStep>, String> {
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut steps = Vec::new();

    let Some(n) = s.find('<') else {
        steps.push(PatternStep {
            prefix: s.to_string(),
            ..Default::default()
        });
        return Ok(steps);
    };
    let mut prefix = &s[..n];
    let mut s = &s[n + 1..];
    loop {
        let Some(n) = s.find('>') else {
            return Err(format!("missing '>' for <{s}"));
        };
        let mut field = &s[..n];
        s = &s[n + 1..];

        if field == "_" || field == "*" {
            field = "";
        }
        steps.push(PatternStep {
            prefix: prefix.to_string(),
            field: field.to_string(),
            field_opt: String::new(),
        });
        if s.is_empty() {
            break;
        }

        match s.find('<') {
            None => {
                steps.push(PatternStep {
                    prefix: s.to_string(),
                    ..Default::default()
                });
                break;
            }
            Some(n) => {
                prefix = &s[..n];
                s = &s[n + 1..];
            }
        }
    }

    Ok(steps)
}

// ---------------------------------------------------------------------------
// Ports of Go stdlib `strconv` quoted-string helpers used by the upstream
// code (strconv.QuotedPrefix / strconv.Unquote / strconv.UnquoteChar).
// ---------------------------------------------------------------------------

/// Port of Go `strconv.QuotedPrefix`: returns the quoted string (including
/// quotes) at the start of `s`.
pub(crate) fn quoted_prefix(s: &str) -> Result<&str, ()> {
    let (_, n) = go_unquote_inner(s, false)?;
    Ok(&s[..n])
}

/// Port of Go `strconv.Unquote`: interprets `s` as a Go quoted string
/// literal, returning the value that `s` quotes.
fn unquote(s: &str) -> Result<String, ()> {
    let (out, n) = go_unquote_inner(s, true)?;
    if n != s.len() {
        return Err(());
    }
    Ok(out)
}

/// Port of Go `strconv/quote.go`'s `unquote(in, unescape)`. Returns the
/// unescaped contents (empty when `unescape` is false) and the number of
/// bytes consumed (the quoted prefix length).
fn go_unquote_inner(input: &str, unescape: bool) -> Result<(String, usize), ()> {
    // Determine the quote form and optimistically find the terminating quote.
    if input.len() < 2 {
        return Err(());
    }
    let quote = input.as_bytes()[0];
    let Some(end0) = input[1..].find(quote as char) else {
        return Err(());
    };
    let end = end0 + 2; // position after terminating quote; may be wrong if escape sequences are present

    match quote {
        b'`' => {
            if !unescape {
                Ok((String::new(), end))
            } else {
                let inner = &input[1..end - 1];
                if !inner.contains('\r') {
                    Ok((inner.to_string(), end))
                } else {
                    // Carriage return characters ('\r') inside raw string
                    // literals are discarded from the raw string value.
                    let buf: String = inner.chars().filter(|&c| c != '\r').collect();
                    Ok((buf, end))
                }
            }
        }
        b'"' | b'\'' => {
            // Handle quoted strings without any escape sequences.
            if !input[..end].contains('\\') && !input[..end].contains('\n') {
                let valid = match quote {
                    // PORT NOTE: Go verifies utf8.ValidString here; Rust
                    // `&str` is valid UTF-8 by construction.
                    b'"' => true,
                    _ => {
                        let inner = &input[1..end - 1];
                        match inner.chars().next() {
                            Some(r) => 1 + r.len_utf8() + 1 == end,
                            None => false,
                        }
                    }
                };
                if valid {
                    if unescape {
                        return Ok((input[1..end - 1].to_string(), end));
                    }
                    return Ok((String::new(), end));
                }
            }

            // Handle quoted strings with escape sequences.
            let mut buf = String::new();
            let in0_len = input.len();
            let mut rest = &input[1..]; // skip starting quote
            while !rest.is_empty() && rest.as_bytes()[0] != quote {
                // Process the next character,
                // rejecting any unescaped newline characters which are invalid.
                if rest.as_bytes()[0] == b'\n' {
                    return Err(());
                }
                let (r, _multibyte, rem) = unquote_char(rest, quote)?;
                rest = rem;

                // Append the character if unescaping the input.
                if unescape {
                    // PORT NOTE: for non-multibyte values >= 0x80 (\x and
                    // octal escapes) Go appends the raw byte, which may
                    // produce invalid UTF-8; Rust strings must stay valid
                    // UTF-8, so the rune is UTF-8 encoded instead.
                    buf.push(r);
                }

                // Single quoted strings must be a single character.
                if quote == b'\'' {
                    break;
                }
            }

            // Verify that the string ends with a terminating quote.
            if rest.is_empty() || rest.as_bytes()[0] != quote {
                return Err(());
            }
            rest = &rest[1..]; // skip terminating quote

            Ok((buf, in0_len - rest.len()))
        }
        _ => Err(()),
    }
}

/// Port of Go `strconv.UnquoteChar`: decodes the first character or escape
/// sequence in `s`, returning `(value, multibyte, tail)`.
pub(crate) fn unquote_char(s: &str, quote: u8) -> Result<(char, bool, &str), ()> {
    // easy cases
    if s.is_empty() {
        return Err(());
    }
    let c = s.as_bytes()[0];
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(());
    }
    if c >= 0x80 {
        let ch = s.chars().next().ok_or(())?;
        return Ok((ch, true, &s[ch.len_utf8()..]));
    }
    if c != b'\\' {
        return Ok((c as char, false, &s[1..]));
    }

    // hard case: c is backslash
    if s.len() <= 1 {
        return Err(());
    }
    let c = s.as_bytes()[1];
    let mut s = &s[2..];

    let (value, multibyte) = match c {
        b'a' => ('\x07', false),
        b'b' => ('\x08', false),
        b'f' => ('\x0c', false),
        b'n' => ('\n', false),
        b'r' => ('\r', false),
        b't' => ('\t', false),
        b'v' => ('\x0b', false),
        b'x' | b'u' | b'U' => {
            let n = match c {
                b'x' => 2,
                b'u' => 4,
                _ => 8,
            };
            if s.len() < n {
                return Err(());
            }
            let mut v: u32 = 0;
            let bs = s.as_bytes();
            for &b in &bs[..n] {
                let x = unhex(b).ok_or(())?;
                v = v << 4 | x;
            }
            s = &s[n..];
            if c == b'x' {
                // single-byte value; see the PORT NOTE in go_unquote_inner
                // about the difference with Go for values >= 0x80
                (char::from_u32(v).ok_or(())?, false)
            } else {
                // char::from_u32 rejects surrogates and values above the max
                // rune, matching Go's utf8.ValidRune check.
                (char::from_u32(v).ok_or(())?, true)
            }
        }
        b'0'..=b'7' => {
            let mut v = (c - b'0') as u32;
            if s.len() < 2 {
                return Err(());
            }
            let bs = s.as_bytes();
            for &b in &bs[..2] {
                // one digit already; two more
                if !(b'0'..=b'7').contains(&b) {
                    return Err(());
                }
                v = (v << 3) | (b - b'0') as u32;
            }
            s = &s[2..];
            if v > 255 {
                return Err(());
            }
            (char::from_u32(v).ok_or(())?, false)
        }
        b'\\' => ('\\', false),
        b'\'' | b'"' => {
            if c != quote {
                return Err(());
            }
            (c as char, false)
        }
        _ => return Err(()),
    };
    Ok((value, multibyte, s))
}

fn unhex(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some((b - b'0') as u32),
        b'a'..=b'f' => Some((b - b'a') as u32 + 10),
        b'A'..=b'F' => Some((b - b'A') as u32 + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Port of Go `html.UnescapeString` used for unescaping pattern prefixes.
//
// PORT NOTE: Go supports the full HTML5 named-entity table (~2200 entries);
// the port implements the identical algorithm (including numeric character
// references and entities without a trailing semicolon) but only the named
// entities relevant for LogsQL pattern escaping: lt, gt, amp, quot, apos,
// nbsp. Extend `lookup_entity` if more are ever required.
// ---------------------------------------------------------------------------

fn html_unescape_string(s: &str) -> String {
    let Some(i) = s.find('&') else {
        return s.to_string();
    };

    let mut b = s.as_bytes().to_vec();
    let (mut dst, mut src) = unescape_entity(&mut b, i, i);
    while src < b.len() {
        let i = if b[src] == b'&' {
            Some(0)
        } else {
            b[src..].iter().position(|&c| c == b'&')
        };
        let Some(i) = i else {
            b.copy_within(src.., dst);
            dst += b.len() - src;
            break;
        };
        if i > 0 {
            b.copy_within(src..src + i, dst);
        }
        (dst, src) = unescape_entity(&mut b, dst + i, src + i);
    }
    b.truncate(dst);
    String::from_utf8(b).expect("BUG: html_unescape_string produced invalid UTF-8")
}

/// Port of Go `html.unescapeEntity` (non-attribute mode): `b[src]` is known
/// to be `'&'`; writes the unescaped entity at `b[dst..]` and returns the new
/// `(dst, src)` positions.
fn unescape_entity(b: &mut [u8], dst: usize, src: usize) -> (usize, usize) {
    // i starts at 1 because we already know that b[src] == '&'.
    let s_len = b.len() - src;
    let mut i = 1usize;

    if s_len <= 1 {
        b[dst] = b[src];
        return (dst + 1, src + 1);
    }

    if b[src + i] == b'#' {
        if s_len <= 3 {
            // We need to have at least "&#.".
            b[dst] = b[src];
            return (dst + 1, src + 1);
        }
        i += 1;
        let mut c = b[src + i];
        let mut hex = false;
        if c == b'x' || c == b'X' {
            hex = true;
            i += 1;
        }

        // PORT NOTE: Go accumulates into a rune (int32) with wrapping
        // overflow; mirrored with wrapping i32 arithmetic.
        let mut x: i32 = 0;
        while i < s_len {
            c = b[src + i];
            i += 1;
            if hex {
                if c.is_ascii_digit() {
                    x = x.wrapping_mul(16).wrapping_add((c - b'0') as i32);
                    continue;
                } else if (b'a'..=b'f').contains(&c) {
                    x = x.wrapping_mul(16).wrapping_add((c - b'a') as i32 + 10);
                    continue;
                } else if (b'A'..=b'F').contains(&c) {
                    x = x.wrapping_mul(16).wrapping_add((c - b'A') as i32 + 10);
                    continue;
                }
            } else if c.is_ascii_digit() {
                x = x.wrapping_mul(10).wrapping_add((c - b'0') as i32);
                continue;
            }
            if c != b';' {
                i -= 1;
            }
            break;
        }

        if i <= 3 {
            // No characters matched.
            b[dst] = b[src];
            return (dst + 1, src + 1);
        }

        if (0x80..=0x9F).contains(&x) {
            // Replace characters from Windows-1252 with UTF-8 equivalents.
            x = REPLACEMENT_TABLE[(x - 0x80) as usize] as i32;
        } else if x == 0 || (0xD800..=0xDFFF).contains(&x) || x > 0x10FFFF {
            // Replace invalid characters with the replacement character.
            x = 0xFFFD;
        }
        // Out-of-range values (e.g. negative after overflow) encode as the
        // replacement character, matching Go's utf8.EncodeRune.
        let ch = char::from_u32(x as u32).unwrap_or('\u{FFFD}');
        return (write_char(b, dst, ch), src + i);
    }

    // Consume the maximum number of characters possible, with the
    // consumed characters matching one of the named references.
    while i < s_len {
        let c = b[src + i];
        i += 1;
        if c.is_ascii_alphanumeric() {
            continue;
        }
        if c != b';' {
            i -= 1;
        }
        break;
    }

    let entity_len = i - 1;
    if entity_len == 0 {
        // No-op.
    } else if let Some(x) = lookup_entity(&b[src + 1..src + i]) {
        return (write_char(b, dst, x), src + i);
    } else {
        let mut max_len = entity_len - 1;
        if max_len > LONGEST_ENTITY_WITHOUT_SEMICOLON {
            max_len = LONGEST_ENTITY_WITHOUT_SEMICOLON;
        }
        let mut j = max_len;
        while j > 1 {
            if let Some(x) = lookup_entity(&b[src + 1..src + 1 + j]) {
                return (write_char(b, dst, x), src + j + 1);
            }
            j -= 1;
        }
    }

    let (dst1, src1) = (dst + i, src + i);
    b.copy_within(src..src1, dst);
    (dst1, src1)
}

fn write_char(b: &mut [u8], dst: usize, ch: char) -> usize {
    let mut tmp = [0u8; 4];
    let enc = ch.encode_utf8(&mut tmp).as_bytes();
    b[dst..dst + enc.len()].copy_from_slice(enc);
    dst + enc.len()
}

/// Named entities supported by the port; keys may include the trailing ';'
/// (Go's entity map contains both forms where HTML5 defines them).
fn lookup_entity(name: &[u8]) -> Option<char> {
    let ch = match name {
        b"lt;" | b"lt" => '<',
        b"gt;" | b"gt" => '>',
        b"amp;" | b"amp" => '&',
        b"quot;" | b"quot" => '"',
        b"apos;" => '\'',
        b"nbsp;" | b"nbsp" => '\u{A0}',
        _ => return None,
    };
    Some(ch)
}

const LONGEST_ENTITY_WITHOUT_SEMICOLON: usize = 6;

/// Port of Go `html`'s replacementTable: Windows-1252 mappings for numeric
/// references in the 0x80..=0x9F range.
const REPLACEMENT_TABLE: [char; 32] = [
    '\u{20AC}', // First entry is what 0x80 should be replaced with.
    '\u{0081}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}', '\u{02C6}',
    '\u{2030}', '\u{0160}', '\u{2039}', '\u{0152}', '\u{008D}', '\u{017D}', '\u{008F}', '\u{0090}',
    '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}', '\u{2022}', '\u{2013}', '\u{2014}', '\u{02DC}',
    '\u{2122}', '\u{0161}', '\u{203A}', '\u{0153}', '\u{009D}', '\u{017E}', '\u{0178}',
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_apply() {
        fn f(pattern_str: &str, s: &str, results_expected: &[&str]) {
            let check_fields = |ptn: &Pattern| {
                assert_eq!(
                    ptn.fields.len(),
                    results_expected.len(),
                    "unexpected number of results; got {}; want {}",
                    ptn.fields.len(),
                    results_expected.len()
                );
                for (i, fld) in ptn.fields.iter().enumerate() {
                    let v = ptn.field_value(fld);
                    assert_eq!(
                        v, results_expected[i],
                        "unexpected value for field {:?}; got {:?}; want {:?}",
                        fld.name, v, results_expected[i]
                    );
                }
            };

            let mut ptn = parse_pattern(pattern_str)
                .unwrap_or_else(|e| panic!("cannot parse {pattern_str:?}: {e}"));
            ptn.apply(s);
            check_fields(&ptn);

            // clone pattern and check fields again
            let mut ptn_copy = ptn.clone();
            ptn_copy.apply(s);
            check_fields(&ptn);
        }

        f("<foo>", "", &[""]);
        f("<foo>", "abc", &["abc"]);
        f("<foo>bar", "", &[""]);
        f("<foo>bar", "bar", &[""]);
        f("<foo>bar", "bazbar", &["baz"]);
        f("<foo>bar", "a bazbar xdsf", &["a baz"]);
        f("<foo>bar<>", "a bazbar xdsf", &["a baz"]);
        f("<foo>bar<>x", "a bazbar xdsf", &["a baz"]);
        f("foo<bar>", "", &[""]);
        f("foo<bar>", "foo", &[""]);
        f("foo<bar>", "a foo xdf sdf", &[" xdf sdf"]);
        f("foo<bar>", "a foo foobar", &[" foobar"]);
        f("foo<bar>baz", "a foo foobar", &[""]);
        f("foo<bar>baz", "a foobaz bar", &[""]);
        f("foo<bar>baz", "a foo foobar baz", &[" foobar "]);
        f("foo<bar>baz", "a foo foobar bazabc", &[" foobar "]);

        f(
            "ip=<ip> <> path=<path> ",
            "x=a, ip=1.2.3.4 method=GET host='abc' path=/foo/bar some tail here",
            &["1.2.3.4", "/foo/bar"],
        );

        // escaped pattern
        f("ip=&lt;<ip>&gt;", "foo ip=<1.2.3.4> bar", &["1.2.3.4"]);
        f(
            "ip=&lt;<ip>&gt;",
            "foo ip=<foo&amp;bar> bar",
            &["foo&amp;bar"],
        );

        // quoted fields
        f(
            r#""msg":<msg>,"#,
            "{\"foo\":\"bar\",\"msg\":\"foo,b\\\"ar\\n\\t\",\"baz\":\"x\"}",
            &["foo,b\"ar\n\t"],
        );
        f("foo=<bar>", "foo=`bar baz,abc` def", &["bar baz,abc"]);
        f("foo=<bar> ", "foo=`bar baz,abc` def", &["bar baz,abc"]);
        f("foo=<bar> ", "foo='bar baz,abc' def", &["bar baz,abc"]);
        f("<foo>", r#""foo,\"bar""#, &[r#"foo,"bar"#]);
        f(r#"<foo>,"bar"#, r#""foo,\"bar""#, &[r#"foo,"bar"#]);

        // disable automatic unquoting of quoted field
        f(r#"[<plain:foo>]"#, r#"["foo","bar"]"#, &[r#""foo","bar""#]);
    }

    #[test]
    fn test_parse_pattern_failure() {
        fn f(pattern_str: &str) {
            let ptn = parse_pattern(pattern_str);
            assert!(
                ptn.is_err(),
                "expecting error when parsing {pattern_str:?}; got {ptn:?}"
            );
        }

        // Missing named fields
        f("");
        f("foobar");
        f("<>");
        f("<>foo<>bar");

        // Missing delimiter between fields
        f("<foo><bar>");
        f("abc<foo><bar>def");
        f("abc<foo><bar>");
        f("abc<foo><_>");
        f("abc<_><_>");
    }

    #[test]
    fn test_parse_pattern_steps_success() {
        fn f(s: &str, steps_expected: &[PatternStep]) {
            let steps = parse_pattern_steps(s)
                .unwrap_or_else(|e| panic!("unexpected error when parsing {s:?}: {e}"));
            assert_eq!(
                steps, steps_expected,
                "unexpected steps for [{s}]; got {steps:?}; want {steps_expected:?}"
            );
        }

        fn step(prefix: &str, field: &str, field_opt: &str) -> PatternStep {
            PatternStep {
                prefix: prefix.to_string(),
                field: field.to_string(),
                field_opt: field_opt.to_string(),
            }
        }

        f("", &[]);

        f("foobar", &[step("foobar", "", "")]);

        f("<>", &[step("", "", "")]);

        f("foo<>", &[step("foo", "", "")]);

        f("<foo><bar>", &[step("", "foo", ""), step("", "bar", "")]);

        f("<foo>", &[step("", "foo", "")]);
        f("<foo>bar", &[step("", "foo", ""), step("bar", "", "")]);
        f("<>bar<foo>", &[step("", "", ""), step("bar", "foo", "")]);
        f("bar<foo>", &[step("bar", "foo", "")]);
        f(
            "bar<foo>abc",
            &[step("bar", "foo", ""), step("abc", "", "")],
        );
        f(
            "bar<foo>abc<_>",
            &[step("bar", "foo", ""), step("abc", "", "")],
        );
        f(
            "<foo>bar<baz>",
            &[step("", "foo", ""), step("bar", "baz", "")],
        );
        f(
            "bar<foo>baz",
            &[step("bar", "foo", ""), step("baz", "", "")],
        );
        f("&lt;&amp;&gt;", &[step("<&>", "", "")]);
        f(
            "&lt;< foo >&amp;gt;",
            &[step("<", "foo", ""), step("&gt;", "", "")],
        );
        f(
            "< q : foo >bar<plain : baz:c:y>f<:foo:bar:baz>",
            &[
                step("", "foo", "q"),
                step("bar", "baz:c:y", "plain"),
                step("f", "foo:bar:baz", ""),
            ],
        );
    }

    #[test]
    fn test_parse_pattern_steps_failure() {
        fn f(s: &str) {
            let steps = parse_pattern_steps(s);
            assert!(
                steps.is_err(),
                "expecting non-nil error when parsing {s:?}; got steps: {steps:?}"
            );
        }

        // missing >
        f("<foo");
        f("foo<bar");
    }
}
