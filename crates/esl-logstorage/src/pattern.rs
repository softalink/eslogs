//! Port of EsLogs `lib/logstorage/pattern.go`.
//!
//! Also hosts private ports of the Go stdlib helpers the upstream file relies
//! on: `strconv.QuotedPrefix` / `strconv.Unquote` / `strconv.UnquoteChar`
//! (Go quoted-string syntax) and `html.UnescapeString`.

// TODO: remove once the upstream consumers of this module (pipe_extract.go,
// filter_pattern_match.go, ...) are ported; until then the crate-private API
// is only exercised by the tests below and by sibling parser modules.
#![allow(dead_code)]

use crate::html_entities::{LONGEST_ENTITY_WITHOUT_SEMICOLON, lookup_entity, lookup_entity2};
use crate::pattern_matcher::decode_rune;
use crate::prefix_filter;

/// Pattern represents text pattern in the form `some_text<some_field>other_text...`
///
/// PORT NOTE: Go `pattern` matches and stores raw byte strings; `matches` and
/// `PatternStep.prefix` are `Vec<u8>` so values containing invalid UTF-8 match
/// byte-for-byte like Go. Field/option NAMES remain `String`.
#[derive(Debug)]
pub(crate) struct Pattern {
    /// steps contains steps for extracting fields from string
    pub(crate) steps: Vec<PatternStep>,

    /// matches contains matches for every step in steps
    pub(crate) matches: Vec<Vec<u8>>,

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
    pub(crate) prefix: Vec<u8>,

    pub(crate) field: String,
    pub(crate) field_opt: String,
}

impl Clone for Pattern {
    /// PORT NOTE: Go's `pattern.clone()` shares the immutable `steps` slice
    /// between clones and allocates fresh `matches`/`fields`; the port
    /// deep-clones `steps`. `matches` contents are not copied — they are
    /// transient per-`apply()` state, exactly as in Go.
    fn clone(&self) -> Pattern {
        let matches = vec![Vec::new(); self.steps.len()];
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

    let matches = vec![Vec::new(); steps.len()];

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
    pub(crate) fn field_value(&self, f: &PatternField) -> &[u8] {
        &self.matches[f.match_idx]
    }

    pub(crate) fn apply(&mut self, s: &[u8]) {
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
            let next_prefix: &[u8] = if i + 1 < steps.len() {
                &steps[i + 1].prefix
            } else {
                b""
            };

            if let Some((us, n_offset)) = try_unquote_bytes(s, &steps[i].field_opt) {
                // Matched quoted string
                matches[i].extend_from_slice(&us);
                s = &s[n_offset..];
                if !s.starts_with(next_prefix) {
                    // Mismatch
                    return;
                }
                s = &s[next_prefix.len()..];
            } else {
                // Match unquoted string until the nextPrefix
                if next_prefix.is_empty() {
                    matches[i].extend_from_slice(s);
                    return;
                }
                let Some((n, prefix_len)) = prefix_index(s, next_prefix) else {
                    // Mismatch
                    return;
                };
                matches[i].extend_from_slice(&s[..n]);
                s = &s[n + prefix_len..];
            }
        }
    }
}

/// PORT NOTE: Go returns `(-1, 0)` on mismatch; the port returns `None`.
/// Byte-level search, like Go's `strings.Index`.
fn prefix_index(s: &[u8], prefix: &[u8]) -> Option<(usize, usize)> {
    if prefix.is_empty() {
        return Some((0, 0));
    }
    if prefix.len() > s.len() {
        return None;
    }
    let n = s.windows(prefix.len()).position(|w| w == prefix)?;
    Some((n, prefix.len()))
}

/// Byte-native port of Go `tryUnquoteString` (pattern.go), used by
/// [`Pattern::apply`]. Matches Go's `strconv.Unquote` exactly: `\xNN`/octal
/// escapes >= 0x80 inside double-quoted strings produce the RAW byte in the
/// output (the result may be invalid UTF-8). The single-quoted path uses
/// `utf8.AppendRune` semantics like Go's own loop.
///
/// PORT NOTE: Go returns `("", -1)` on mismatch; the port returns `None`.
pub(crate) fn try_unquote_bytes(s: &[u8], opt: &str) -> Option<(Vec<u8>, usize)> {
    if opt == "plain" {
        return None;
    }
    if s.is_empty() {
        return None;
    }

    match s[0] {
        b'"' | b'`' => {
            let qp_len = quoted_prefix_len(s).ok()?;
            let us = unquote_bytes(&s[..qp_len]).ok()?;
            Some((us, qp_len))
        }
        b'\'' => {
            let mut b = Vec::new();
            let mut tail = &s[1..];
            while !tail.starts_with(b"'") {
                let (ch, _, rest) = unquote_char_bytes(tail, b'\'').ok()?;
                let mut tmp = [0u8; 4];
                b.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                tail = rest;
            }
            Some((b, s.len() - tail.len() + 1))
        }
        _ => None,
    }
}

/// String form of [`try_unquote_bytes`] kept for the callers whose values are
/// still `String`-typed (`storage_search`, `stream_tags`, `logfmt_parser`).
///
/// PORT NOTE: unlike the byte form, `\xNN`/octal escapes >= 0x80 inside
/// double-quoted strings are UTF-8-encoded as scalars here (Go emits the raw
/// byte) because the result must stay valid UTF-8 — documented residual for
/// those callers. Go returns `("", -1)` on mismatch; the port returns `None`.
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
        // Prefixes are slices of the pattern text (`&str`), so they are valid
        // UTF-8 at this point; the byte type matches Go's byte-level matching.
        let prefix = std::str::from_utf8(&step.prefix)
            .expect("BUG: pattern prefixes come from the pattern text, which is valid UTF-8");
        step.prefix = html_unescape_string(prefix).into_bytes();
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
            prefix: s.as_bytes().to_vec(),
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
            prefix: prefix.as_bytes().to_vec(),
            field: field.to_string(),
            field_opt: String::new(),
        });
        if s.is_empty() {
            break;
        }

        match s.find('<') {
            None => {
                steps.push(PatternStep {
                    prefix: s.as_bytes().to_vec(),
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
    // The consumed prefix always ends right after an ASCII quote byte, so the
    // slice below is on a char boundary.
    let n = quoted_prefix_len(s.as_bytes())?;
    Ok(&s[..n])
}

/// Byte form of [`quoted_prefix`]: returns the length of the quoted string
/// (including quotes) at the start of `s`.
pub(crate) fn quoted_prefix_len(s: &[u8]) -> Result<usize, ()> {
    let (_, n) = go_unquote_inner(s, false, false)?;
    Ok(n)
}

/// Port of Go `strconv.Unquote` over raw bytes: interprets `s` as a Go quoted
/// string literal, returning the value that `s` quotes. Matches Go exactly:
/// `\xNN`/octal escapes >= 0x80 inside double-quoted strings emit the raw
/// byte, so the result may be invalid UTF-8.
pub(crate) fn unquote_bytes(s: &[u8]) -> Result<Vec<u8>, ()> {
    let (out, n) = go_unquote_inner(s, true, false)?;
    if n != s.len() {
        return Err(());
    }
    Ok(out)
}

/// String form of `strconv.Unquote` kept for [`try_unquote_string`].
///
/// PORT NOTE: `\xNN`/octal escapes >= 0x80 are UTF-8-encoded as scalars here
/// (Go emits the raw byte); see [`try_unquote_string`].
pub(crate) fn unquote(s: &str) -> Result<String, ()> {
    let (out, n) = go_unquote_inner(s.as_bytes(), true, true)?;
    if n != s.len() {
        return Err(());
    }
    Ok(String::from_utf8(out).expect("BUG: scalar-mode unquote of a str produced invalid UTF-8"))
}

/// Port of Go `strconv/quote.go`'s `unquote(in, unescape)` over raw bytes.
/// Returns the unescaped contents (empty when `unescape` is false) and the
/// number of bytes consumed (the quoted prefix length).
///
/// PORT NOTE: with `scalar_high_escapes == false` this matches Go exactly —
/// Go appends `byte(r)` when `r < utf8.RuneSelf || !multibyte`, so the
/// escapes `\x80`..`\xFF` and octal `\200`..`\377` inside double-quoted
/// values produce a raw (possibly invalid-UTF-8) byte, and raw invalid UTF-8
/// sequences inside double quotes decode as U+FFFD (via `utf8.DecodeRune`).
/// `scalar_high_escapes == true` keeps the legacy `String`-typed behavior
/// (UTF-8-encode the scalar, e.g. `\x80` → 0xC2 0x80 vs Go's lone 0x80) for
/// the callers whose values are still `String` (logfmt_parser, stream_tags,
/// storage_search); the two modes are identical for inputs whose unquoted
/// value stays valid UTF-8.
fn go_unquote_inner(
    input: &[u8],
    unescape: bool,
    scalar_high_escapes: bool,
) -> Result<(Vec<u8>, usize), ()> {
    // Determine the quote form and optimistically find the terminating quote.
    if input.len() < 2 {
        return Err(());
    }
    let quote = input[0];
    let Some(end0) = input[1..].iter().position(|&b| b == quote) else {
        return Err(());
    };
    let end = end0 + 2; // position after terminating quote; may be wrong if escape sequences are present

    match quote {
        b'`' => {
            if !unescape {
                Ok((Vec::new(), end))
            } else {
                let inner = &input[1..end - 1];
                if !inner.contains(&b'\r') {
                    Ok((inner.to_vec(), end))
                } else {
                    // Carriage return characters ('\r') inside raw string
                    // literals are discarded from the raw string value.
                    let buf: Vec<u8> = inner.iter().copied().filter(|&c| c != b'\r').collect();
                    Ok((buf, end))
                }
            }
        }
        b'"' | b'\'' => {
            // Handle quoted strings without any escape sequences.
            if !input[..end].contains(&b'\\') && !input[..end].contains(&b'\n') {
                let inner = &input[1..end - 1];
                let valid = match quote {
                    b'"' => std::str::from_utf8(inner).is_ok(),
                    _ => {
                        let (r, n) = decode_rune(inner);
                        inner.len() == n && (r != '\u{FFFD}' || n != 1)
                    }
                };
                if valid {
                    if unescape {
                        return Ok((inner.to_vec(), end));
                    }
                    return Ok((Vec::new(), end));
                }
            }

            // Handle quoted strings with escape sequences.
            let mut buf = Vec::new();
            let in0_len = input.len();
            let mut rest = &input[1..]; // skip starting quote
            while !rest.is_empty() && rest[0] != quote {
                // Process the next character,
                // rejecting any unescaped newline characters which are invalid.
                if rest[0] == b'\n' {
                    return Err(());
                }
                let (r, multibyte, rem) = unquote_char_bytes(rest, quote)?;
                rest = rem;

                // Append the character if unescaping the input.
                if unescape {
                    if !scalar_high_escapes && ((r as u32) < 0x80 || !multibyte) {
                        // Go: buf = append(buf, byte(r)). When !multibyte the
                        // value is at most 0xFF (\xNN or octal escape).
                        buf.push(r as u32 as u8);
                    } else {
                        let mut tmp = [0u8; 4];
                        buf.extend_from_slice(r.encode_utf8(&mut tmp).as_bytes());
                    }
                }

                // Single quoted strings must be a single character.
                if quote == b'\'' {
                    break;
                }
            }

            // Verify that the string ends with a terminating quote.
            if rest.is_empty() || rest[0] != quote {
                return Err(());
            }
            rest = &rest[1..]; // skip terminating quote

            Ok((buf, in0_len - rest.len()))
        }
        _ => Err(()),
    }
}

/// String form of [`unquote_char_bytes`] kept for `pattern_matcher` and the
/// `String`-typed single-quote loop in [`try_unquote_string`].
pub(crate) fn unquote_char(s: &str, quote: u8) -> Result<(char, bool, &str), ()> {
    let (value, multibyte, tail) = unquote_char_bytes(s.as_bytes(), quote)?;
    // The consumed prefix is either a whole rune or an ASCII escape sequence,
    // so the tail starts on a char boundary for valid `&str` input.
    Ok((value, multibyte, &s[s.len() - tail.len()..]))
}

/// Port of Go `strconv.UnquoteChar` over raw bytes: decodes the first
/// character or escape sequence in `s`, returning `(value, multibyte, tail)`.
/// Raw bytes >= 0x80 decode with `utf8.DecodeRune` semantics (invalid
/// sequences yield U+FFFD, size 1), exactly like Go.
pub(crate) fn unquote_char_bytes(s: &[u8], quote: u8) -> Result<(char, bool, &[u8]), ()> {
    // easy cases
    if s.is_empty() {
        return Err(());
    }
    let c = s[0];
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(());
    }
    if c >= 0x80 {
        let (ch, size) = decode_rune(s);
        return Ok((ch, true, &s[size..]));
    }
    if c != b'\\' {
        return Ok((c as char, false, &s[1..]));
    }

    // hard case: c is backslash
    if s.len() <= 1 {
        return Err(());
    }
    let c = s[1];
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
            for &b in &s[..n] {
                let x = unhex(b).ok_or(())?;
                v = v << 4 | x;
            }
            s = &s[n..];
            if c == b'x' {
                // single-byte value, possibly not UTF-8 (Go emits `byte(r)`;
                // see go_unquote_inner). v <= 0xFF, so from_u32 cannot fail.
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
            for &b in &s[..2] {
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
// The full HTML5 named-entity tables (Go's `entity`/`entity2` maps from
// `html/entity.go`, incl. entities without a trailing semicolon and
// two-codepoint entities) live in `crate::html_entities`.
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
    } else if let Some([x0, x1]) = lookup_entity2(&b[src + 1..src + i]) {
        let dst1 = write_char(b, dst, x0);
        return (write_char(b, dst1, x1), src + i);
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
                        v,
                        results_expected[i].as_bytes(),
                        "unexpected value for field {:?}; got {:?}; want {:?}",
                        fld.name,
                        String::from_utf8_lossy(v),
                        results_expected[i]
                    );
                }
            };

            let mut ptn = parse_pattern(pattern_str)
                .unwrap_or_else(|e| panic!("cannot parse {pattern_str:?}: {e}"));
            ptn.apply(s.as_bytes());
            check_fields(&ptn);

            // clone pattern and check fields again
            let mut ptn_copy = ptn.clone();
            ptn_copy.apply(s.as_bytes());
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

    /// Byte-native `apply` helper for inputs/outputs that are not valid UTF-8.
    fn apply_bytes(pattern_str: &str, s: &[u8], results_expected: &[&[u8]]) {
        let mut ptn = parse_pattern(pattern_str)
            .unwrap_or_else(|e| panic!("cannot parse {pattern_str:?}: {e}"));
        ptn.apply(s);
        assert_eq!(ptn.fields.len(), results_expected.len());
        for (i, fld) in ptn.fields.iter().enumerate() {
            let v = ptn.field_value(fld);
            assert_eq!(
                v, results_expected[i],
                "unexpected value for field {:?}; got {v:?}; want {:?}",
                fld.name, results_expected[i]
            );
        }
    }

    /// Go's `strconv.Unquote` emits the RAW byte for `\xNN`/octal
    /// escapes >= 0x80 inside double-quoted values (parser.go relies on the
    /// same semantics); the extracted field value may therefore be invalid
    /// UTF-8.
    #[test]
    fn test_pattern_apply_hex_escape_emits_raw_byte() {
        // `\xff` and `\x80` inside a double-quoted value -> raw bytes.
        apply_bytes(
            "foo=<bar> baz=<qux>",
            b"foo=\"\\xff\\x80\" baz=abc",
            &[b"\xff\x80", b"abc"],
        );
        // Octal escapes >= 0o200 behave the same.
        apply_bytes(
            "foo=<bar> baz=<qux>",
            b"foo=\"\\377\" baz=abc",
            &[b"\xff", b"abc"],
        );
        // Single-quoted values keep Go's utf8.AppendRune semantics:
        // '\xff' -> UTF-8 encoding of U+00FF (0xC3 0xBF), exactly like Go.
        apply_bytes(
            "foo=<bar> baz=<qux>",
            b"foo='\\xff' baz=abc",
            &[b"\xc3\xbf", b"abc"],
        );
        // Raw invalid UTF-8 inside a double-quoted value decodes as U+FFFD
        // (Go's utf8.DecodeRune semantics in strconv.UnquoteChar).
        apply_bytes(
            "foo=<bar> baz=<qux>",
            b"foo=\"\\t\xff\" baz=abc",
            &[b"\t\xef\xbf\xbd", b"abc"],
        );
    }

    /// Unquoted (plain) pieces of a value containing invalid UTF-8 are
    /// extracted verbatim, byte-for-byte like Go.
    #[test]
    fn test_pattern_apply_invalid_utf8_value() {
        apply_bytes(
            "ip=<ip> code=<code>;",
            b"ip=\xff\xfe\x80 code=200;",
            &[b"\xff\xfe\x80", b"200"],
        );
        // Prefix search is a byte search: invalid UTF-8 before the first
        // prefix must not prevent a match.
        apply_bytes("foo=<bar>", b"\x80\xffzz foo=abc", &[b"abc"]);
        // Trailing field captures the raw tail bytes.
        apply_bytes("foo=<bar>", b"foo=\xf0\x28\x8c\x28", &[b"\xf0\x28\x8c\x28"]);
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
                prefix: prefix.as_bytes().to_vec(),
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

    /// Port of Go stdlib `html/escape_test.go` `TestUnescape` (drives the
    /// `html.UnescapeString` port used for pattern prefixes).
    #[test]
    fn test_html_unescape_string() {
        fn f(html: &str, unescaped: &str) {
            let got = html_unescape_string(html);
            assert_eq!(
                got, unescaped,
                "unexpected unescape result for {html:?}; got {got:?}; want {unescaped:?}"
            );
        }

        // Handle no entities.
        f("A\ttext\nstring", "A\ttext\nstring");
        // Handle simple named entities.
        f("&amp; &gt; &lt;", "& > <");
        // Handle hitting the end of the string.
        f("&amp &amp", "& &");
        // Handle entities with two codepoints.
        f("text &gesl; blah", "text \u{22db}\u{fe00} blah");
        // Handle decimal numeric entities.
        f("Delta = &#916; ", "Delta = \u{394} ");
        // Handle hexadecimal numeric entities.
        f("Lambda = &#x3bb; = &#X3Bb ", "Lambda = \u{3bb} = \u{3bb} ");
        // Handle numeric early termination.
        f(
            "&# &#x &#128;43 &copy = &#169f = &#xa9",
            "&# &#x \u{20ac}43 \u{a9} = \u{a9}f = \u{a9}",
        );
        // Handle numeric ISO-8859-1 entity replacements.
        f("Footnote&#x87;", "Footnote\u{2021}");
        // Handle single ampersand.
        f("&", "&");
        // Handle ampersand followed by non-entity.
        f("text &test", "text &test");
        // Handle "&#".
        f("text &#", "text &#");

        // Extra spot checks for the full named-entity table (not in the Go
        // test file): longest-match truncation of a no-semicolon entity and
        // an accented named entity.
        f("&notit;", "\u{ac}it;");
        f("a+acute=&aacute;", "a+acute=\u{e1}");
        // Invalid numeric references collapse to U+FFFD like Go.
        f("&#0; &#xD800; &#x110000;", "\u{fffd} \u{fffd} \u{fffd}");
    }
}
