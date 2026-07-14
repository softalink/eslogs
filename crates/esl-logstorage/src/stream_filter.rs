//! Port of EsLogs `lib/logstorage/stream_filter.go`.
//!
//! PORT NOTE: `allow(dead_code)` because the parse entry points and the
//! embedded minimal lexer are consumed by parser.go (unported Layer 4) and by
//! indexdb's cache path; the `Indexdb` consumer is itself ported ahead of its
//! own consumers. Remove once parser.go lands and wires these in.
#![allow(dead_code)]

use std::fmt;
use std::sync::Mutex;

use esl_common::encoding;
use esl_common::regexutil::PromRegex;

use crate::rows::Field;
use crate::tokenizer::is_token_rune;

/// StreamFilter is a filter for streams, e.g. `_stream:{...}`
///
/// `Default` (an empty, match-all filter) supports
/// `Filter::take_stream_filter`, which moves the filter out of a
/// `FilterStream` during `Query::optimize` (Go `mergeFiltersStream`).
#[derive(Default)]
pub struct StreamFilter {
    pub(crate) or_filters: Vec<AndStreamFilter>,
}

impl StreamFilter {
    pub fn match_stream_name(&self, s: &[u8]) -> bool {
        // Go parses the stream name via strconv.UnquoteChar, which maps invalid
        // UTF-8 bytes rune-wise to U+FFFD (utf8.DecodeRuneInString), so a lossy
        // view reproduces Go for the quoted tag-value portions. (Residual: Go
        // keeps raw bytes in the tag-NAME portion; canonical stream names are
        // strconv.Quote output and never contain invalid UTF-8 there.)
        let s = String::from_utf8_lossy(s);
        let mut sn = get_stream_name();

        let mut result = false;
        if sn.parse(&s) {
            for of in &self.or_filters {
                let mut match_and_filters = true;
                for tf in &of.tag_filters {
                    if !sn.matches(tf) {
                        match_and_filters = false;
                        break;
                    }
                }
                if match_and_filters {
                    result = true;
                    break;
                }
            }
        }

        put_stream_name(sn);
        result
    }

    pub fn is_empty(&self) -> bool {
        for af in &self.or_filters {
            if !af.tag_filters.is_empty() {
                return false;
            }
        }
        true
    }

    pub fn marshal_for_cache_key(&self, dst: &mut Vec<u8>) {
        encoding::marshal_var_uint64(dst, self.or_filters.len() as u64);
        for af in &self.or_filters {
            encoding::marshal_var_uint64(dst, af.tag_filters.len() as u64);
            for f in &af.tag_filters {
                encoding::marshal_bytes(dst, f.tag_name.as_bytes());
                encoding::marshal_bytes(dst, f.op.as_bytes());
                encoding::marshal_bytes(dst, f.value.as_bytes());
            }
        }
    }
}

impl fmt::Display for StreamFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a: Vec<String> = self.or_filters.iter().map(|of| of.to_string()).collect();
        write!(f, "{{{}}}", a.join(" or "))
    }
}

pub(crate) struct AndStreamFilter {
    pub(crate) tag_filters: Vec<StreamTagFilter>,
}

impl fmt::Display for AndStreamFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a: Vec<String> = self.tag_filters.iter().map(|tf| tf.to_string()).collect();
        f.write_str(&a.join(","))
    }
}

/// streamTagFilter is a filter for `tagName op value`
pub(crate) struct StreamTagFilter {
    /// tag_name is the name for the tag to filter
    pub(crate) tag_name: String,

    /// op is operation such as `=`, `!=`, `=~`, `!~` or `:`
    pub(crate) op: String,

    /// value is the value
    pub(crate) value: String,

    /// regexp is initialized for `=~` and `!~` op.
    pub(crate) regexp: Option<PromRegex>,
}

impl fmt::Display for StreamTagFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            quote_token_if_needed(&self.tag_name),
            self.op,
            go_quote(&self.value)
        )
    }
}

pub(crate) fn parse_stream_filter(lex: &mut Lexer) -> Result<StreamFilter, String> {
    if !lex.is_keyword(&["{"]) {
        return Err(format!(
            "unexpected token {} instead of '{{' in _stream filter",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let mut filters = Vec::new();
    loop {
        let f = parse_and_stream_filter(lex)?;
        filters.push(f);
        if lex.is_keyword(&["}"]) {
            lex.next_token();
            let sf = StreamFilter {
                or_filters: filters,
            };
            return Ok(sf);
        } else if lex.is_keyword(&["or"]) {
            lex.next_token();
            if lex.is_keyword(&["}"]) {
                return Err("unexpected '}' after 'or' in _stream filter".to_string());
            }
        } else {
            return Err(format!(
                "unexpected token in _stream filter: {}; want '}}' or 'or'",
                go_quote(&lex.token)
            ));
        }
    }
}

fn parse_and_stream_filter(lex: &mut Lexer) -> Result<AndStreamFilter, String> {
    let mut filters = Vec::new();
    loop {
        if lex.is_keyword(&["}"]) {
            let asf = AndStreamFilter {
                tag_filters: filters,
            };
            return Ok(asf);
        }
        let f = parse_stream_tag_filter(lex)?;
        filters.push(f);
        if lex.is_keyword(&["or", "}"]) {
            let asf = AndStreamFilter {
                tag_filters: filters,
            };
            return Ok(asf);
        } else if lex.is_keyword(&[","]) {
            lex.next_token();
        } else {
            let f = filters.last().unwrap();
            return Err(format!(
                "unexpected token {} in _stream filter after {}; want 'or', 'and', '}}' or ','",
                go_quote(&lex.token),
                go_quote(&f.to_string())
            ));
        }
    }
}

fn parse_stream_tag_filter(lex: &mut Lexer) -> Result<StreamTagFilter, String> {
    // parse tagName
    let tag_name = lex
        .next_compound_token()
        .map_err(|err| format!("cannot parse stream tag name inside {{...}}: {err}"))?;
    if !lex.is_keyword(&["=", "!=", "=~", "!~", "in", "not_in"]) {
        return Err(format!(
            "unsupported operation {} inside {{...}} for {} field; supported operations: =, !=, =~, !~, in, not_in",
            go_quote(&lex.token),
            go_quote(&tag_name)
        ));
    }

    // parse op
    let mut op = lex.token.clone();
    lex.next_token();

    // parse tag value
    let mut value = String::new();
    if op == "in" || op == "not_in" {
        let (args, is_wildcard) = parse_args_in_parens_possible_wildcard(lex)
            .map_err(|err| format!("cannot read {op}() args inside {{...}}: {err}"))?;
        if op == "in" {
            op = "=~".to_string();
        } else {
            op = "!~".to_string();
        }
        if is_wildcard {
            value = ".*".to_string();
        } else {
            let args_escaped: Vec<String> = args.iter().map(|arg| regexp_quote_meta(arg)).collect();
            value = args_escaped.join("|");
        }
    } else {
        let v = lex.next_compound_token().map_err(|err| {
            format!(
                "cannot parse value for tag {} inside {{...}}: {err}",
                go_quote(&tag_name)
            )
        })?;
        value.push_str(&v);
    }

    let mut stf = StreamTagFilter {
        tag_name,
        op,
        value,
        regexp: None,
    };
    if stf.op == "=~" || stf.op == "!~" {
        let re = PromRegex::new(&stf.value).map_err(|err| {
            format!(
                "invalid regexp {} for {} inside {{...}}: {err}",
                go_quote(&stf.value),
                go_quote(&stf.tag_name)
            )
        })?;
        stf.regexp = Some(re);
    }
    Ok(stf)
}

fn get_stream_name() -> StreamName {
    STREAM_NAME_POOL.lock().unwrap().pop().unwrap_or_default()
}

fn put_stream_name(mut sn: StreamName) {
    sn.reset();
    STREAM_NAME_POOL.lock().unwrap().push(sn);
}

// PORT NOTE: Go uses `sync.Pool`; the port keeps the reuse pattern with a
// `Mutex<Vec<..>>` pool handing streamNames out by value.
static STREAM_NAME_POOL: Mutex<Vec<StreamName>> = Mutex::new(Vec::new());

#[derive(Default)]
struct StreamName {
    tags: Vec<Field>,
}

impl StreamName {
    fn reset(&mut self) {
        self.tags.clear();
    }

    fn parse(&mut self, s: &str) -> bool {
        if s.len() < 2 || !s.starts_with('{') || !s.ends_with('}') {
            return false;
        }
        let mut s = &s[1..s.len() - 1];
        if s.is_empty() {
            return true;
        }

        loop {
            // Parse tag name
            let Some(n) = s.find('=') else {
                // cannot find tag name
                return false;
            };
            let name = &s[..n];
            s = &s[n + 1..];

            // Parse tag value
            if !s.starts_with('"') {
                return false;
            }
            let Ok(q_prefix) = crate::pattern::quoted_prefix(s) else {
                return false;
            };
            let Ok(value) = crate::pattern::unquote(q_prefix) else {
                return false;
            };
            s = &s[q_prefix.len()..];

            self.tags.push(Field {
                name: name.as_bytes().to_vec(),
                value: value.into_bytes(),
            });

            if s.is_empty() {
                return true;
            }
            let Some(tail) = s.strip_prefix(',') else {
                return false;
            };
            s = tail;
        }
    }

    /// PORT NOTE: Go names this method `match`, which is a Rust keyword.
    fn matches(&self, tf: &StreamTagFilter) -> bool {
        let v = self.get_tag_value_by_tag_name(&tf.tag_name);
        match tf.op.as_str() {
            "=" => v == tf.value.as_bytes(),
            "!=" => v != tf.value.as_bytes(),
            "=~" => tf.regexp.as_ref().unwrap().match_string(tag_value_str(v)),
            "!~" => !tf.regexp.as_ref().unwrap().match_string(tag_value_str(v)),
            _ => {
                esl_common::panicf!("BUG: unexpected tagFilter operation: {}", go_quote(&tf.op));
                false
            }
        }
    }

    fn get_tag_value_by_tag_name(&self, name: &str) -> &[u8] {
        for t in &self.tags {
            if t.name == name.as_bytes() {
                return &t.value;
            }
        }
        b""
    }
}

/// Checked `&str` view of a parsed stream tag value for regexp matching.
///
/// The tags are produced by `go_unquote` (a `String`), so the value is always
/// valid UTF-8 and the fallback is unreachable; it is kept for safety and
/// returns an empty match input rather than panicking.
fn tag_value_str(v: &[u8]) -> &str {
    std::str::from_utf8(v).unwrap_or("")
}

// ---------------------------------------------------------------------------
// Minimal LogsQL lexer.
//
// PORT NOTE(parser.go porter): stream_filter.go needs the LogsQL lexer from
// lib/logstorage/parser.go, which is not ported yet (Layer 4). The subset
// below is a faithful port of only what stream filters use:
//
//   - `lexer` (fields s, token, rawToken, prevRawToken, isSkippedSpace),
//     newLexer, nextToken, isKeyword/isKeywordAny, nextCompoundToken
//     (= nextCompoundTokenExt(nil): stop-token support is omitted),
//   - parseArgsInParensPossibleWildcard,
//   - quoteTokenIfNeeded / needQuoteToken / reservedKeywords / isWord,
//   - Go strconv.Quote / QuotedPrefix / Unquote / UnquoteChar and
//     regexp.QuoteMeta equivalents.
//
// Omitted vs Go: sOrig/context(), currentTimestamp (the newLexer timestamp
// arg), the queryOptions stack, backup/restoreState, and the
// isPipeName/isStatsFuncName checks inside needQuoteToken (pipes and stats
// functions are unported; tag names equal to a pipe/stats-function name are
// therefore not quoted by Display yet). When porting parser.go, move this
// lexer there, re-add the omitted pieces and delete this copy.
// ---------------------------------------------------------------------------

// PORT NOTE(parser.go porter): the parser (parser/ module) completes this
// lexer per the PORT NOTE above. Rather than relocate the whole type (it is
// imported by many filter_*/pipe_* modules), the parser porter added the
// missing `current_timestamp`/`s_orig` fields, a `new_at` constructor,
// `#[derive(Clone)]` for backupState/restoreState, and pub(crate) accessors
// below. The higher-level lexer helpers Go keeps on `*lexer`
// (nextCompoundTokenExt with stop-tokens, isQueryPartTrailer, context,
// checkPrevAdjacentToken, isEnd, ...) live in `parser::lexer_ext::LexerExt`,
// an extension trait over this type, so they did not need to be added here.
#[derive(Clone)]
pub(crate) struct Lexer<'a> {
    /// s contains unparsed tail of the original string
    s: &'a str,

    /// s_orig contains the original string (Go `sOrig`); used by `context()`.
    s_orig: &'a str,

    /// token contains the current token
    ///
    /// an empty token means the end of s
    ///
    /// PORT NOTE: Go's token is a `string` = raw bytes; this `String` form
    /// keeps the legacy scalar decoding (`\xNN` escapes >= 0x80 in
    /// double-quoted tokens decode to the code point U+00NN) for keyword
    /// checks, error messages and the consumers whose payloads are still
    /// `String`-typed (field names, pipe args, ...). Byte-exact payloads
    /// (phrases/values) must read [`Lexer::token_bytes`] instead.
    pub(crate) token: String,

    /// The Go-parity raw-byte payload of `token` (Go parser.go `lex.token`,
    /// unquoted via `strconv.Unquote`): `\xNN`/octal escapes >= 0x80 inside
    /// double-quoted/backquoted tokens denote RAW bytes, so this may be
    /// invalid UTF-8. Equals `token.as_bytes()` for every other token kind.
    pub(crate) token_bytes: Vec<u8>,

    /// raw_token contains raw token before unquoting
    raw_token: String,

    /// prev_raw_token contains the previously parsed token before unquoting
    prev_raw_token: String,

    /// is_skipped_space is set to true if there was a whitespace before the token in s
    is_skipped_space: bool,

    /// current_timestamp is the current timestamp in nanoseconds (Go
    /// `currentTimestamp`). It is used for parsing relative `_time` filters.
    current_timestamp: i64,
}

impl<'a> Lexer<'a> {
    /// Returns new lexer for the given s (Go `newLexer` with a zero timestamp).
    ///
    /// The lex.token points to the first token in s.
    pub(crate) fn new(s: &'a str) -> Lexer<'a> {
        Lexer::new_at(s, 0)
    }

    /// Returns a new lexer for `s` at the given timestamp (Go `newLexer`).
    pub(crate) fn new_at(s: &'a str, timestamp: i64) -> Lexer<'a> {
        let mut lex = Lexer {
            s,
            s_orig: s,
            token: String::new(),
            token_bytes: Vec::new(),
            raw_token: String::new(),
            prev_raw_token: String::new(),
            is_skipped_space: false,
            current_timestamp: timestamp,
        };
        lex.next_token();
        lex
    }

    // ---- Accessors used by parser::lexer_ext::LexerExt (Go reads these
    // fields directly; they are private here so expose them for the parser). ----

    #[inline]
    pub(crate) fn raw_token(&self) -> &str {
        &self.raw_token
    }

    #[inline]
    pub(crate) fn prev_raw_token(&self) -> &str {
        &self.prev_raw_token
    }

    #[inline]
    pub(crate) fn is_skipped_space(&self) -> bool {
        self.is_skipped_space
    }

    #[inline]
    pub(crate) fn current_timestamp(&self) -> i64 {
        self.current_timestamp
    }

    /// Returns the unparsed tail (Go `lex.s`).
    #[inline]
    pub(crate) fn tail(&self) -> &str {
        self.s
    }

    /// Returns the original input (Go `lex.sOrig`).
    #[inline]
    pub(crate) fn s_orig(&self) -> &str {
        self.s_orig
    }

    #[inline]
    pub(crate) fn is_quoted_token(&self) -> bool {
        self.token != self.raw_token
    }

    /// Byte form of [`Lexer::next_compound_token`] for phrase/value payloads:
    /// a quoted token returns its Go-parity raw-byte payload
    /// ([`Lexer::token_bytes`], possibly invalid UTF-8); unquoted compound
    /// tokens are slices of the query text (always valid UTF-8), so the
    /// `String` path is byte-exact for them.
    pub(crate) fn next_compound_token_bytes(&mut self) -> Result<Vec<u8>, String> {
        if self.is_quoted_token() {
            // Quoted tokens cannot be a part of compound token, so return them as is.
            let b = self.token_bytes.clone();
            self.next_token();
            return Ok(b);
        }
        self.next_compound_token().map(String::into_bytes)
    }

    pub(crate) fn next_compound_token(&mut self) -> Result<String, String> {
        if self.is_quoted_token() {
            // Quoted tokens cannot be a part of compound token, so return them as is.
            let s = self.token.clone();
            self.next_token();
            return Ok(s);
        }

        if !self.is_skipped_space
            && self.is_keyword(DENIED_FIRST_COMPOUND_TOKENS)
            && is_word(&self.prev_raw_token)
        {
            return Err(format!(
                "missing whitespace between {} and {}",
                go_quote(&self.prev_raw_token),
                go_quote(&self.token)
            ));
        }

        if !self.is_allowed_compound_token() {
            return Err(format!(
                "compound token cannot start with {}; put it into quotes if needed",
                go_quote(&self.token)
            ));
        }

        let mut s = self.token.clone();
        self.next_token();

        while !self.is_skipped_space && self.is_allowed_compound_token() {
            s += &self.raw_token;
            self.next_token();
        }

        if GLUE_COMPOUND_TOKENS.contains(&s.as_str()) {
            // Disallow a single-char compound token with glue chars, since this is error-prone.
            // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/590
            return Err(format!(
                "compound token cannot be equal to {}; put it into quotes if needed",
                go_quote(&s)
            ));
        }

        Ok(s)
    }

    fn is_allowed_compound_token(&self) -> bool {
        if self.is_quoted_token() {
            // Quoted token cannot be a part of compound token
            return false;
        }

        if self.token.is_empty() {
            // Missing token (EOF).
            return false;
        }

        // PORT NOTE: the stopTokens check of nextCompoundTokenExt is omitted —
        // stream filters always pass nil stopTokens.

        // Glue tokens are allowed to be a part of compound token.
        if self.is_keyword(GLUE_COMPOUND_TOKENS) {
            return true;
        }

        // Regular word token is allowed to be a part of compound token.
        is_word(&self.token)
    }

    pub(crate) fn is_keyword(&self, keywords: &[&str]) -> bool {
        if self.is_quoted_token() {
            return false;
        }
        let token_lower = self.token.to_lowercase();
        keywords.contains(&token_lower.as_str())
    }

    fn next_char_token(&mut self, s: &'a str, size: usize) {
        self.token = s[..size].to_string();
        self.token_bytes = self.token.clone().into_bytes();
        self.raw_token = self.token.clone();
        self.s = &s[size..];
    }

    /// Updates lex.token to the next token.
    pub(crate) fn next_token(&mut self) {
        let mut s = self.s;
        self.prev_raw_token = std::mem::take(&mut self.raw_token);
        self.token.clear();
        self.token_bytes.clear();
        self.is_skipped_space = false;

        if s.is_empty() {
            return;
        }

        loop {
            // Skip whitespace
            let trimmed = s.trim_start();
            if trimmed.len() != s.len() {
                self.is_skipped_space = true;
            }
            s = trimmed;

            if let Some(tail) = s.strip_prefix('#') {
                // skip comment till \n
                match tail.find('\n') {
                    Some(n) => s = &tail[n + 1..],
                    None => s = "",
                }
                continue;
            }
            break;
        }

        if s.is_empty() {
            self.s = s;
            return;
        }

        // Try decoding simple token
        let token_len = s
            .char_indices()
            .find(|&(_, r)| !is_token_rune(r))
            .map_or(s.len(), |(i, _)| i);
        if token_len > 0 {
            self.next_char_token(s, token_len);
            return;
        }

        let r = s.chars().next().unwrap();
        match r {
            '"' | '`' => {
                let Ok(prefix) = crate::pattern::quoted_prefix(s) else {
                    self.next_char_token(s, 1);
                    return;
                };
                // Go parser.go:329 `strconv.Unquote`: `\xNN`/octal escapes
                // >= 0x80 inside double-quoted tokens denote RAW bytes in the
                // token payload (`token_bytes`); the `token` String form keeps
                // the scalar decoding for the still-String-typed consumers
                // (see the field docs). Both decode the same syntax, so the
                // scalar form cannot fail once the byte form succeeded.
                let Ok(token_bytes) = crate::pattern::unquote_bytes(prefix.as_bytes()) else {
                    self.next_char_token(s, 1);
                    return;
                };
                self.token = crate::pattern::unquote(prefix)
                    .expect("BUG: scalar unquote failed on input accepted by unquote_bytes");
                self.token_bytes = token_bytes;
                self.raw_token = prefix.to_string();
                self.s = &s[prefix.len()..];
            }
            '\'' => {
                // Go parser.go:341-346: single-quoted tokens decode via
                // strconv.UnquoteChar + utf8.AppendRune, so `\xNN` escapes
                // >= 0x80 are UTF-8-encoded scalars — the String form is
                // byte-exact here.
                let mut b = String::new();
                let mut tail = &s[1..];
                while !tail.starts_with('\'') {
                    let Ok((ch, _, new_tail)) = crate::pattern::unquote_char(tail, b'\'') else {
                        self.next_char_token(s, 1);
                        return;
                    };
                    b.push(ch);
                    tail = new_tail;
                }
                let size = s.len() - tail.len() + 1;
                self.token = b;
                self.token_bytes = self.token.clone().into_bytes();
                self.raw_token = s[..size].to_string();
                self.s = &s[size..];
            }
            '=' if s[1..].starts_with('~') => self.next_char_token(s, 2),
            '!' if s[1..].starts_with('~') || s[1..].starts_with('=') => self.next_char_token(s, 2),
            _ => self.next_char_token(s, r.len_utf8()),
        }
    }
}

fn is_word(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.chars().all(is_token_rune)
}

/// deniedFirstCompoundTokens contains disallowed starting tokens for compound
/// tokens without the whitespace in front of these tokens.
const DENIED_FIRST_COMPOUND_TOKENS: &[&str] = &["/", ".", "$"];

/// glueCompoundTokens contains tokens allowed inside unquoted compound tokens.
const GLUE_COMPOUND_TOKENS: &[&str] = &[
    "+", // Seen in time formats: 2025-07-20T10:20:30+03:00
    "-", // Seen in hostnames: foo-bar-baz
    "/", // Seen in paths: foo/bar/baz
    ":", // Seen in tcp addresses: foo:1235
    ".", // Seen in hostnames: foobar.com
    "$", // Seen in PHP-like vars: $foo
];

pub(crate) fn parse_args_in_parens_possible_wildcard(
    lex: &mut Lexer,
) -> Result<(Vec<String>, bool), String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();

    let mut args = Vec::new();
    let mut is_wildcard = false;
    while !lex.is_keyword(&[")"]) {
        if lex.is_keyword(&[","]) {
            return Err("unexpected ','".to_string());
        }
        if lex.is_keyword(&["("]) {
            return Err("unexpected '('".to_string());
        }
        let arg;
        if lex.is_keyword(&["*"]) {
            lex.next_token();
            is_wildcard = true;
            arg = "*".to_string();
        } else {
            let token = lex
                .next_compound_token()
                .map_err(|err| format!("cannot parse arg: {err}"))?;
            arg = token;
        }
        args.push(arg);
        if lex.is_keyword(&[")"]) {
            break;
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "missing ',' after {}; got {} instead",
                go_quote(args.last().unwrap()),
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
    lex.next_token();

    if is_wildcard {
        return Ok((Vec::new(), is_wildcard));
    }
    Ok((args, false))
}

pub(crate) fn quote_token_if_needed(s: &str) -> String {
    if !need_quote_token(s) {
        return s.to_string();
    }
    go_quote(s)
}

fn need_quote_token(s: &str) -> bool {
    // Delegates to the parser's port of Go `needQuoteToken`, which also
    // quotes pipe names (`isPipeName`) and stats function names
    // (`isStatsFuncName`) — required since the pipe/stats parsers landed
    // (e.g. `blocks_count` must render as `"blocks_count"`).
    crate::parser::need_quote_token(s)
}

// ---------------------------------------------------------------------------
// Go strconv / regexp helpers.
//
// PORT NOTE(parser.go porter): Rust ports of Go strconv.Quote,
// strconv.QuotedPrefix, strconv.Unquote, strconv.UnquoteChar and
// regexp.QuoteMeta. Lift them into a shared location when porting parser.go.
// ---------------------------------------------------------------------------

/// Port of Go `strconv.Quote`.
pub(crate) fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        append_escaped_rune(&mut out, c, '"');
    }
    out.push('"');
    out
}

/// Quotes a raw byte value for rendering into query text.
///
/// Valid UTF-8 renders via [`quote_token_if_needed`] (bit-identical to the
/// `&str` behavior); invalid UTF-8 is quoted with Go `strconv.Quote` byte
/// semantics ([`go_quote_bytes`]), where each invalid byte renders as a
/// `\xNN` escape — exactly what Go produces when rendering such values.
pub(crate) fn quote_value_bytes_if_needed(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => quote_token_if_needed(s),
        Err(_) => go_quote_bytes(v),
    }
}

/// Port of Go `strconv.Quote` over raw bytes: decodes runes like Go
/// (`utf8.DecodeRuneInString`), escaping each invalid byte as `\xNN`.
pub(crate) fn go_quote_bytes(v: &[u8]) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    let mut b = v;
    while !b.is_empty() {
        let (r, size) = crate::pattern_matcher::decode_rune(b);
        if r == '\u{FFFD}' && size == 1 {
            // Invalid UTF-8 byte (Go: `r == utf8.RuneError && width == 1`).
            out.push_str(&format!("\\x{:02x}", b[0]));
        } else {
            append_escaped_rune(&mut out, r, '"');
        }
        b = &b[size..];
    }
    out.push('"');
    out
}

fn append_escaped_rune(out: &mut String, c: char, quote: char) {
    if c == quote || c == '\\' {
        out.push('\\');
        out.push(c);
        return;
    }
    if (' '..='~').contains(&c) {
        // ASCII printable
        out.push(c);
        return;
    }
    match c {
        '\x07' => out.push_str("\\a"),
        '\x08' => out.push_str("\\b"),
        '\x0c' => out.push_str("\\f"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '\x0b' => out.push_str("\\v"),
        _ => {
            let n = c as u32;
            if n < 0x20 || n == 0x7f {
                out.push_str(&format!("\\x{n:02x}"));
            } else if is_go_print(c) {
                out.push(c);
            } else if n < 0x10000 {
                out.push_str(&format!("\\u{n:04x}"));
            } else {
                out.push_str(&format!("\\U{n:08x}"));
            }
        }
    }
}

/// PORT NOTE: approximation of Go `unicode.IsPrint` for non-ASCII runes
/// (Rust std has no direct equivalent): control chars and non-ASCII
/// whitespace are treated as non-printable.
fn is_go_print(c: char) -> bool {
    !c.is_control() && !c.is_whitespace()
}

// PORT NOTE: the private `go_quoted_prefix` / `go_unquote` / `go_unquote_char`
// copies that used to live here were duplicates of the byte-native Go
// `strconv` engine in `pattern.rs`; the lexer and `StreamName::parse` now call
// `crate::pattern::{quoted_prefix, quoted_prefix_len, unquote, unquote_bytes,
// unquote_char}` directly.

/// Port of Go `regexp.QuoteMeta` (escapes Go's regexp special bytes
/// ``\.+*?()|[]{}^$``).
fn regexp_quote_meta(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(
            c,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Test helpers (Go stream_filter_test.go), shared with the indexdb tests.
// ---------------------------------------------------------------------------

/// PORT NOTE: Go's newTestStreamFilter goes through
/// `parseFilterStreamInternal` + `filterStream` (parser.go and
/// filter_stream.go — unported Layer 4); the port calls parse_stream_filter
/// directly, which yields the same StreamFilter and errors for the ported
/// test cases.
pub(crate) fn new_test_stream_filter(s: &str) -> Result<StreamFilter, String> {
    let mut lex = Lexer::new(s);
    parse_stream_filter(&mut lex)
}

#[cfg(test)]
pub(crate) fn must_new_test_stream_filter(s: &str) -> StreamFilter {
    match new_test_stream_filter(s) {
        Ok(sf) => sf,
        Err(err) => panic!("unexpected error in newTestStreamFilter({s:?}): {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_filter_match_stream_name() {
        let f = |filter: &str, stream_name: &str, result_expected: bool| {
            let sf = must_new_test_stream_filter(filter);
            let result = sf.match_stream_name(stream_name.as_bytes());
            assert_eq!(
                result, result_expected,
                "unexpected result for matching {stream_name} against {sf}; got {result}; want {result_expected}"
            );
        };

        // Empty filter matches anything
        f(r#"{}"#, r#"{}"#, true);
        f(r#"{}"#, r#"{foo="bar"}"#, true);
        f(r#"{}"#, r#"{foo="bar",a="b",c="d"}"#, true);

        // empty '=' filter
        f(r#"{foo=""}"#, r#"{}"#, true);
        f(r#"{foo=""}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo=""}"#, r#"{a="b",c="d"}"#, true);

        // non-empty '=' filter
        f(r#"{foo="bar"}"#, r#"{}"#, false);
        f(r#"{foo="bar"}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo="bar"}"#, r#"{foo="barbaz"}"#, false);
        f(r#"{foo="bar"}"#, r#"{foo="bazbar"}"#, false);
        f(r#"{foo="bar"}"#, r#"{a="b",foo="bar"}"#, true);
        f(r#"{foo="bar"}"#, r#"{foo="bar",a="b"}"#, true);
        f(r#"{foo="bar"}"#, r#"{a="b",foo="bar",c="d"}"#, true);
        f(r#"{foo="bar"}"#, r#"{foo="baz"}"#, false);
        f(r#"{foo="bar"}"#, r#"{foo="baz",a="b"}"#, false);
        f(r#"{foo="bar"}"#, r#"{a="b",foo="baz"}"#, false);
        f(r#"{foo="bar"}"#, r#"{a="b",foo="baz",b="c"}"#, false);
        f(r#"{foo="bar"}"#, r#"{zoo="bar"}"#, false);
        f(r#"{foo="bar"}"#, r#"{a="b",zoo="bar"}"#, false);

        // empty '!=' filter
        f(r#"{foo!=""}"#, r#"{}"#, false);
        f(r#"{foo!=""}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo!=""}"#, r#"{a="b",c="d"}"#, false);

        // non-empty '!=' filter
        f(r#"{foo!="bar"}"#, r#"{}"#, true);
        f(r#"{foo!="bar"}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo!="bar"}"#, r#"{foo="barbaz"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{foo="bazbar"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{a="b",foo="bar"}"#, false);
        f(r#"{foo!="bar"}"#, r#"{foo="bar",a="b"}"#, false);
        f(r#"{foo!="bar"}"#, r#"{a="b",foo="bar",c="d"}"#, false);
        f(r#"{foo!="bar"}"#, r#"{foo="baz"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{foo="baz",a="b"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{a="b",foo="baz"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{a="b",foo="baz",b="c"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{zoo="bar"}"#, true);
        f(r#"{foo!="bar"}"#, r#"{a="b",zoo="bar"}"#, true);

        // empty '=~' filter
        f(r#"{foo=~""}"#, r#"{}"#, true);
        f(r#"{foo=~""}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo=~""}"#, r#"{a="b",c="d"}"#, true);
        f(r#"{foo=~".*"}"#, r#"{}"#, true);
        f(r#"{foo=~".*"}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo=~".*"}"#, r#"{a="b",c="d"}"#, true);

        // non-empty '=~` filter
        f(r#"{foo=~".+"}"#, r#"{}"#, false);
        f(r#"{foo=~".+"}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo=~".+"}"#, r#"{a="b",c="d"}"#, false);

        f(r#"{foo=~"bar"}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo=~"bar"}"#, r#"{foo="barbaz"}"#, false);
        f(r#"{foo=~"bar"}"#, r#"{foo="bazbar"}"#, false);
        f(r#"{foo=~"bar"}"#, r#"{a="b",foo="bar"}"#, true);
        f(r#"{foo=~"bar"}"#, r#"{foo="bar",a="b"}"#, true);
        f(r#"{foo=~"bar"}"#, r#"{a="b",foo="bar",b="c"}"#, true);
        f(r#"{foo=~"bar"}"#, r#"{foo="baz"}"#, false);
        f(r#"{foo=~"bar"}"#, r#"{foo="baz",a="b"}"#, false);
        f(r#"{foo=~"bar"}"#, r#"{zoo="bar"}"#, false);
        f(r#"{foo=~"bar"}"#, r#"{a="b",zoo="bar"}"#, false);

        f(r#"{foo=~".*a.+"}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{foo="barboz"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{foo="bazbor"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{a="b",foo="bar"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{foo="bar",a="b"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{a="b",foo="bar",b="c"}"#, true);
        f(r#"{foo=~".*a.+"}"#, r#"{foo="boz"}"#, false);
        f(r#"{foo=~".*a.+"}"#, r#"{foo="boz",a="b"}"#, false);
        f(r#"{foo=~".*a.+"}"#, r#"{zoo="bar"}"#, false);
        f(r#"{foo=~".*a.+"}"#, r#"{a="b",zoo="bar"}"#, false);

        // empty '!~' filter
        f(r#"{foo!~""}"#, r#"{}"#, false);
        f(r#"{foo!~""}"#, r#"{foo="bar"}"#, true);
        f(r#"{foo!~""}"#, r#"{a="b",c="d"}"#, false);
        f(r#"{foo!~".*"}"#, r#"{}"#, false);
        f(r#"{foo!~".*"}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo!~".*"}"#, r#"{a="b",c="d"}"#, false);

        f(r#"{foo!~"bar"}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo!~"bar"}"#, r#"{foo="barbaz"}"#, true);
        f(r#"{foo!~"bar"}"#, r#"{foo="bazbar"}"#, true);
        f(r#"{foo!~"bar"}"#, r#"{a="b",foo="bar"}"#, false);
        f(r#"{foo!~"bar"}"#, r#"{foo="bar",a="b"}"#, false);
        f(r#"{foo!~"bar"}"#, r#"{a="b",foo="bar",b="c"}"#, false);
        f(r#"{foo!~"bar"}"#, r#"{foo="baz"}"#, true);
        f(r#"{foo!~"bar"}"#, r#"{foo="baz",a="b"}"#, true);
        f(r#"{foo!~"bar"}"#, r#"{zoo="bar"}"#, true);
        f(r#"{foo!~"bar"}"#, r#"{a="b",zoo="bar"}"#, true);

        f(r#"{foo!~".*a.+"}"#, r#"{foo="bar"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{foo="barboz"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{foo="bazbor"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{a="b",foo="bar"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{foo="bar",a="b"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{a="b",foo="bar",b="c"}"#, false);
        f(r#"{foo!~".*a.+"}"#, r#"{foo="boz"}"#, true);
        f(r#"{foo!~".*a.+"}"#, r#"{foo="boz",a="b"}"#, true);
        f(r#"{foo!~".*a.+"}"#, r#"{zoo="bar"}"#, true);
        f(r#"{foo!~".*a.+"}"#, r#"{a="b",zoo="bar"}"#, true);

        // multiple 'and' filters
        f(r#"{a="b",b="c"}"#, r#"{a="b"}"#, false);
        f(r#"{a="b",b="c"}"#, r#"{b="c",a="b"}"#, true);
        f(r#"{a="b",b="c"}"#, r#"{x="y",b="c",a="b",d="e"}"#, true);
        f(r#"{a=~"foo.+",a!~".+bar"}"#, r#"{a="foobar"}"#, false);
        f(r#"{a=~"foo.+",a!~".+bar"}"#, r#"{a="foozar"}"#, true);

        // multiple `or` filters
        f(r#"{a="b" or b="c"}"#, r#"{x="y"}"#, false);
        f(r#"{a="b" or b="c"}"#, r#"{x="y",b="c"}"#, true);
        f(r#"{a="b" or b="c"}"#, r#"{a="b",x="y",b="c"}"#, true);
        f(r#"{a="b",b="c" or a=~"foo.+"}"#, r#"{}"#, false);
        f(
            r#"{a="b",b="c" or a=~"foo.+"}"#,
            r#"{x="y",a="foobar"}"#,
            true,
        );
        f(r#"{a="b",b="c" or a=~"foo.+"}"#, r#"{x="y",a="b"}"#, false);
        f(
            r#"{a="b",b="c" or a=~"foo.+"}"#,
            r#"{x="y",b="c",a="b"}"#,
            true,
        );
        f(r#"{a="b" or c=""}"#, r#"{}"#, true);
        f(r#"{a="b" or c=""}"#, r#"{c="x"}"#, false);
        f(r#"{a="b" or c=""}"#, r#"{a="b"}"#, true);

        // `in` operator
        f(r#"{a in (b, "c")}"#, r#"{a="c"}"#, true);
        f(r#"{a in (b, "c")}"#, r#"{a="b"}"#, true);
        f(r#"{a in (b, "c")}"#, r#"{a="d"}"#, false);
        f(r#"{x="y" or a in (b, "c")}"#, r#"{a="d",x="y"}"#, true);
        f(r#"{a in (*)}"#, r#"{b="c"}"#, true);
        f(r#"{a in (*)}"#, r#"{a="c"}"#, true);

        // `not_in` operator
        f(r#"{a not_in (b, "c")}"#, r#"{a="c"}"#, false);
        f(r#"{a not_in (b, "c")}"#, r#"{a="b"}"#, false);
        f(r#"{a not_in (b, "c")}"#, r#"{a="d"}"#, true);
        f(r#"{x="y", a not_in (b, "c")}"#, r#"{a="b",x="y"}"#, false);
        f(r#"{x="y", a not_in (b, "c")}"#, r#"{a="d",x="y"}"#, true);
        f(r#"{a not_in (*)}"#, r#"{b="c"}"#, false);
        f(r#"{a not_in (*)}"#, r#"{a="c"}"#, false);
    }

    #[test]
    fn test_new_test_stream_filter_success() {
        let f = |s: &str, result_expected: &str| {
            let sf = match new_test_stream_filter(s) {
                Ok(sf) => sf,
                Err(err) => panic!("unexpected error: {err}"),
            };
            let result = sf.to_string();
            assert_eq!(
                result, result_expected,
                "unexpected StreamFilter; got {result}; want {result_expected}"
            );
        };

        f("{}", "{}");
        f(r#"{foo="bar"}"#, r#"{foo="bar"}"#);
        f(
            r#"{ "foo" =~ "bar.+" , baz!="a" or x="y"}"#,
            r#"{foo=~"bar.+",baz!="a" or x="y"}"#,
        );
        f(
            r#"{"a b"='c}"d' OR de="aaa"}"#,
            r#"{"a b"="c}\"d" or de="aaa"}"#,
        );
        f(
            "{a-q:w.z=\"b\", c=\"d\" or 'x a'=`y-z=q`}",
            r#"{"a-q:w.z"="b",c="d" or "x a"="y-z=q"}"#,
        );
        f(r#"{a in (a, "b.c|d")}"#, r#"{a=~"a|b\\.c\\|d"}"#);
        f(r#"{a not_in (a, "b.c|d")}"#, r#"{a!~"a|b\\.c\\|d"}"#);
        f(r#"{a in (*)}"#, r#"{a=~".*"}"#);
        f(r#"{a not_in (*)}"#, r#"{a!~".*"}"#);
    }

    #[test]
    fn test_new_test_stream_filter_failure() {
        let f = |s: &str| {
            let result = new_test_stream_filter(s);
            assert!(
                result.is_err(),
                "expecting non-nil error for {s:?}; got {:?}",
                result.map(|sf| sf.to_string())
            );
        };

        f("");
        f("}");
        f("{");
        f("{foo");
        f("{foo}");
        f("{'foo");
        f("{foo=");
        f("{foo or bar}");
        f("{foo=bar");
        f("{foo=bar baz}");
        f("{foo='bar' baz='x'}");
        f("{foo=(a}");
        f("{foo=(a)}");
        f("{foo in (a");
        f("{foo in (a,");
        f("{foo in (a,}");
    }
}
