//! Port of Softalink LLC `lib/regexutil`.
//!
//! The Go package leans on `regexp/syntax` (parse tree, `Simplify`, exact
//! `String()` serialization); the needed subset of that package is ported in
//! the private `goparse`/`gosyntax`/`goclass`/`gofold` submodules, since the
//! Rust `regex` crate does not expose an equivalent AST and the simplified
//! prefix/suffix strings are part of this package's contract (tests assert
//! them verbatim). Actual matching is delegated to the `regex` crate.
//!
//! PORT NOTE (syntax gaps vs Go's regexp):
//! - `\p{...}` / `\P{...}` Unicode classes are parsed as opaque tokens
//!   (`Op::UnicodeClass`) and resolved by the regex crate when the matcher
//!   is compiled. Match semantics therefore follow the regex crate's Unicode
//!   tables (UTS#18 names) instead of Go's `unicode` package: the name sets
//!   overlap on general categories and scripts, but a name unknown to the
//!   regex crate fails at [`Regex::new`]/[`PromRegex::new`] with a
//!   regex-crate error message where Go fails at parse time with
//!   "invalid character class range"; and small classes are never expanded
//!   into or-values the way Go expands classes with <= 100 runes (fast-path
//!   only — match results are identical).
//! - `\b` / `\B` are rewritten to `(?-u:\b)` / `(?-u:\B)` before compiling
//!   the matcher (`ascii_word_boundaries`), matching Go's ASCII-only word
//!   boundaries.
//! - Lookarounds and backreferences are unsupported in both Go and Rust.
//!
//! PORT NOTE (byte matching / invalid UTF-8 haystacks): matching is done with
//! [`regex::bytes::Regex`] on raw value bytes, mirroring Go's `regexp` which
//! matches `[]byte`/`string` payloads byte-wise. For valid-UTF-8 haystacks the
//! two engines agree exactly. For haystacks containing invalid UTF-8, Go decodes
//! each invalid byte as U+FFFD (`utf8.DecodeRune` returns `(RuneError, 1)` per
//! invalid byte); `regex::bytes` in Unicode mode would instead not match invalid
//! bytes with rune-oriented constructs. [`Regex::match_bytes`] /
//! [`PromRegex::match_bytes`] therefore run [`go_utf8_replace`] first — one
//! U+FFFD per invalid byte via `slice::utf8_chunks` — so `.`, negated classes,
//! `\p{...}` and a literal U+FFFD all match invalid bytes exactly like Go, while
//! valid-UTF-8 haystacks are returned unchanged (byte-identical). See
//! `tests::test_regex_match_bytes_invalid_utf8` (the port matchers) and
//! `tests::test_bytes_regex_invalid_utf8_probe` (the underlying `regex::bytes`
//! behavior the wrapper compensates for).

mod goclass;
mod gofold;
mod goparse;
mod gosyntax;

use gosyntax::{FOLD_CASE, Op, Regexp, simplify};

/// Removes '^' at the start of expr and '$' at the end of the expr.
pub fn remove_start_end_anchors(expr: &str) -> &str {
    let mut expr = expr;
    while let Some(rest) = expr.strip_prefix('^') {
        expr = rest;
    }
    while expr.ends_with('$') && !expr.ends_with("\\$") {
        expr = &expr[..expr.len() - 1];
    }
    expr
}

/// Returns "or" values from the given regexp expr.
///
/// It returns `["foo", "bar"]` for "foo|bar" regexp.
/// It returns `["foo"]` for "foo" regexp.
/// It returns `[""]` for "" regexp.
/// It returns an empty list if it is impossible to extract "or" values from
/// the regexp.
pub fn get_or_values_regex(expr: &str) -> Vec<String> {
    get_or_values_regex_impl(expr, true)
}

/// Returns "or" values from the given Prometheus-like regexp expr.
///
/// It ignores start and end anchors ('^') and ('$') at the start and the end
/// of expr.
pub fn get_or_values_prom_regex(expr: &str) -> Vec<String> {
    let expr = remove_start_end_anchors(expr);
    get_or_values_regex_impl(expr, false)
}

fn get_or_values_regex_impl(expr: &str, keep_anchors: bool) -> Vec<String> {
    let (prefix, tail_expr) = simplify_regex_impl(expr, keep_anchors);
    if tail_expr.is_empty() {
        return vec![prefix];
    }
    let Ok(sre) = parse_regexp(&tail_expr) else {
        return Vec::new();
    };
    let mut or_values = get_or_values(&sre);

    // Sort orValues for faster index seek later
    or_values.sort();

    if !prefix.is_empty() {
        // Add prefix to orValues
        for or_value in or_values.iter_mut() {
            *or_value = format!("{prefix}{or_value}");
        }
    }

    or_values
}

/// Converts the rune `r` to a string the way Go's `string(rune)` does
/// (invalid code points map to U+FFFD).
fn rune_string(r: i32) -> String {
    match u32::try_from(r).ok().and_then(char::from_u32) {
        Some(c) => c.to_string(),
        None => '\u{FFFD}'.to_string(),
    }
}

fn get_or_values(sre: &Regexp) -> Vec<String> {
    match sre.op {
        Op::Capture => get_or_values(&sre.sub[0]),
        Op::Literal => match get_literal(sre) {
            Some(v) => vec![v],
            None => Vec::new(),
        },
        Op::EmptyMatch => vec![String::new()],
        Op::Alternate => {
            let mut a: Vec<String> = Vec::with_capacity(sre.sub.len());
            for re_sub in &sre.sub {
                let ca = get_or_values(re_sub);
                if ca.is_empty() {
                    return Vec::new();
                }
                a.extend(ca);
                if a.len() > MAX_OR_VALUES {
                    // It is cheaper to use regexp here.
                    return Vec::new();
                }
            }
            a
        }
        Op::CharClass => {
            let mut a: Vec<String> = Vec::with_capacity(sre.rune.len() / 2);
            let mut i = 0;
            while i < sre.rune.len() {
                let mut start = sre.rune[i];
                let end = sre.rune[i + 1];
                while start <= end {
                    a.push(rune_string(start));
                    start += 1;
                    if a.len() > MAX_OR_VALUES {
                        // It is cheaper to use regexp here.
                        return Vec::new();
                    }
                }
                i += 2;
            }
            a
        }
        Op::Concat => get_or_values_concat(&sre.sub),
        _ => Vec::new(),
    }
}

fn get_or_values_concat(subs: &[Regexp]) -> Vec<String> {
    if subs.is_empty() {
        return vec![String::new()];
    }
    let prefixes = get_or_values(&subs[0]);
    if prefixes.is_empty() {
        return Vec::new();
    }
    if subs.len() == 1 {
        return prefixes;
    }
    let suffixes = get_or_values_concat(&subs[1..]);
    if suffixes.is_empty() {
        return Vec::new();
    }
    if prefixes.len() * suffixes.len() > MAX_OR_VALUES {
        // It is cheaper to use regexp here.
        return Vec::new();
    }
    let mut a: Vec<String> = Vec::with_capacity(prefixes.len() * suffixes.len());
    for prefix in &prefixes {
        for suffix in &suffixes {
            a.push(format!("{prefix}{suffix}"));
        }
    }
    a
}

fn get_literal(sre: &Regexp) -> Option<String> {
    if sre.op == Op::Capture {
        return get_literal(&sre.sub[0]);
    }
    if sre.op == Op::Literal && sre.flags & FOLD_CASE == 0 {
        return Some(sre.rune.iter().map(|&r| rune_string(r)).collect());
    }
    None
}

const MAX_OR_VALUES: usize = 100;

/// Simplifies the given regexp expr.
///
/// It returns plaintext prefix and the remaining regular expression
/// without capturing parens.
pub fn simplify_regex(expr: &str) -> (String, String) {
    let (prefix, suffix) = simplify_regex_impl(expr, true);
    let mut sre = must_parse_regexp(&suffix);

    if is_dot_op(&sre, Op::Star) {
        return (prefix, String::new());
    }
    if sre.op == Op::Concat {
        let mut subs = std::mem::take(&mut sre.sub);
        if prefix.is_empty() {
            // Drop .* at the start
            while subs.first().is_some_and(|s| is_dot_op(s, Op::Star)) {
                subs.remove(0);
            }
        }

        // Drop .* at the end.
        while subs.last().is_some_and(|s| is_dot_op(s, Op::Star)) {
            subs.pop();
        }

        if subs.is_empty() {
            return (prefix, String::new());
        }
        sre.sub = subs;
        return (prefix, sre.to_string());
    }
    (prefix, suffix)
}

/// Simplifies the given Prometheus-like expr.
///
/// It returns plaintext prefix and the remaining regular expression
/// with dropped '^' and '$' anchors at the beginning and at the end
/// of the regular expression.
///
/// The function removes capturing parens from the expr,
/// so it cannot be used when capturing parens are necessary.
pub fn simplify_prom_regex(expr: &str) -> (String, String) {
    simplify_regex_impl(expr, false)
}

fn simplify_regex_impl(expr: &str, keep_anchors: bool) -> (String, String) {
    let Ok(sre) = parse_regexp(expr) else {
        // Cannot parse the regexp. Return it all as prefix.
        return (expr.to_string(), String::new());
    };
    let Some(mut sre) = simplify_regexp(sre, keep_anchors, keep_anchors) else {
        // The regexp is valid but cannot be simplified. Return it all as suffix.
        return (String::new(), expr.to_string());
    };
    if sre.op == Op::EmptyMatch {
        return (String::new(), String::new());
    }
    if let Some(v) = get_literal(&sre) {
        return (v, String::new());
    }
    let mut prefix = String::new();
    if sre.op == Op::Concat
        && let Some(v) = get_literal(&sre.sub[0])
    {
        prefix = v;
        sre.sub.remove(0);
        if sre.sub.is_empty() {
            return (prefix, String::new());
        }
        if let Some(sre_new) = simplify_regexp(sre.clone(), true, keep_anchors) {
            sre = sre_new;
        }
    }
    // PORT NOTE: Go additionally runs syntax.Compile(sre) as a defensive
    // check, but Compile never fails for parser-produced trees (its error
    // return is vestigial), so the check is omitted here.
    let s = sre.to_string();
    let s = s.replace("(?:)", "");
    let s = s.replace("(?s:.)", ".");
    let s = s.replace("(?m:$)", "$");
    (prefix, s)
}

fn simplify_regexp(sre: Regexp, keep_begin_op: bool, keep_end_op: bool) -> Option<Regexp> {
    let mut s = sre.to_string();
    let mut sre = sre;
    loop {
        sre = simplify_regexp_ext(sre, keep_begin_op, keep_end_op);
        sre = simplify(&sre);
        if (!keep_begin_op && sre.op == Op::BeginText) || (!keep_end_op && sre.op == Op::EndText) {
            sre = Regexp::empty_match();
        }
        let s_new = sre.to_string();
        if s_new == s {
            return Some(sre);
        }
        s = s_new;

        match parse_regexp(&s) {
            Ok(sre_new) => sre = sre_new,
            Err(_) => {
                // Parsing errors can occur due to deep nesting limits or other
                // validation parameters. This usually happens when the Simplify
                // method returns an optimized regex that is technically valid
                // but exceeds internal complexity thresholds.
                // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1112
                return None;
            }
        }
    }
}

fn simplify_regexp_ext(mut sre: Regexp, keep_begin_op: bool, keep_end_op: bool) -> Regexp {
    // PORT NOTE: Go compares subexpressions against a sentinel `emptyRegexp`
    // pointer; in this port every OpEmptyMatch produced by this function is
    // equivalent to that sentinel, so `op == EmptyMatch` is the same check.
    match sre.op {
        Op::Capture => {
            // Substitute all the capture regexps with non-capture regexps.
            sre.op = Op::Alternate;
            let sub0 = sre.sub.remove(0);
            let sub0 = simplify_regexp_ext(sub0, keep_begin_op, keep_end_op);
            if sub0.op == Op::EmptyMatch {
                return Regexp::empty_match();
            }
            sre.sub.insert(0, sub0);
            sre
        }
        Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
            let sub0 = sre.sub.remove(0);
            let sub0 = simplify_regexp_ext(sub0, keep_begin_op, keep_end_op);
            if sub0.op == Op::EmptyMatch {
                return Regexp::empty_match();
            }
            sre.sub.insert(0, sub0);
            sre
        }
        Op::Alternate => {
            // Do not remove empty captures from OpAlternate, since this may break regexp.
            let subs = std::mem::take(&mut sre.sub);
            sre.sub = subs
                .into_iter()
                .map(|sub| simplify_regexp_ext(sub, keep_begin_op, keep_end_op))
                .collect();
            sre
        }
        Op::Concat => {
            let old = std::mem::take(&mut sre.sub);
            let old_len = old.len();
            let mut subs: Vec<Regexp> = Vec::with_capacity(old_len);
            for (i, sub) in old.into_iter().enumerate() {
                let sub = simplify_regexp_ext(
                    sub,
                    keep_begin_op || !subs.is_empty(),
                    keep_end_op || i + 1 < old_len,
                );
                if sub.op != Op::EmptyMatch {
                    subs.push(sub);
                }
            }
            // Remove anchors from the beginning and the end of regexp, since they
            // will be added later.
            if !keep_begin_op {
                while subs.first().is_some_and(|s| s.op == Op::BeginText) {
                    subs.remove(0);
                }
            }
            if !keep_end_op {
                while subs.last().is_some_and(|s| s.op == Op::EndText) {
                    subs.pop();
                }
            }
            if subs.is_empty() {
                return Regexp::empty_match();
            }
            if subs.len() == 1 {
                return subs.pop().unwrap();
            }
            sre.sub = subs;
            sre
        }
        Op::EmptyMatch => Regexp::empty_match(),
        _ => sre,
    }
}

/// Returns regex part from `sre` surrounded by .+ or .* depending on the
/// `prefix_suffix_op`.
///
/// For example, if sre=".+foo.+" and prefixSuffix=OpPlus, then the function
/// returns "foo".
///
/// An empty string is returned if `sre` doesn't contain the given
/// `prefix_suffix_op` prefix and suffix.
fn get_substring_literal(sre: &Regexp, prefix_suffix_op: Op) -> String {
    if sre.op != Op::Concat || sre.sub.len() != 3 {
        return String::new();
    }
    if !is_dot_op(&sre.sub[0], prefix_suffix_op) || !is_dot_op(&sre.sub[2], prefix_suffix_op) {
        return String::new();
    }
    get_literal(&sre.sub[1]).unwrap_or_default()
}

fn is_dot_op(sre: &Regexp, op: Op) -> bool {
    if sre.op != op {
        return false;
    }
    sre.sub[0].op == Op::AnyChar
}

fn parse_regexp(expr: &str) -> Result<Regexp, gosyntax::Error> {
    goparse::parse(expr, gosyntax::PERL | gosyntax::DOT_NL)
}

fn must_parse_regexp(expr: &str) -> Regexp {
    match parse_regexp(expr) {
        Ok(sre) => sre,
        Err(err) => panic!("BUG: cannot parse already verified regexp {expr:?}: {err}"),
    }
}

/// Rewrites `\b` / `\B` to `(?-u:\b)` / `(?-u:\B)` for the regex crate,
/// which treats bare word boundaries as Unicode-aware while Go's are
/// ASCII-only (e.g. Go sees a boundary between `é` and `f` in "caféfoo").
///
/// The input has already been validated by the ported Go-syntax parser, so
/// `\b` cannot occur inside a character class (Go rejects it there) and a
/// bare scan over escape pairs is sufficient.
fn ascii_word_boundaries(expr: &str) -> std::borrow::Cow<'_, str> {
    if !expr.contains(r"\b") && !expr.contains(r"\B") {
        return std::borrow::Cow::Borrowed(expr);
    }
    let mut out = String::with_capacity(expr.len() + 16);
    let mut chars = expr.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('b') => out.push_str(r"(?-u:\b)"),
            Some('B') => out.push_str(r"(?-u:\B)"),
            Some(e) => {
                out.push('\\');
                out.push(e);
            }
            None => out.push('\\'),
        }
    }
    std::borrow::Cow::Owned(out)
}

/// PromRegex implements an optimized string matching for Prometheus-like regex.
///
/// The following regexs are optimized:
///
/// - plain string such as "foobar"
/// - alternate strings such as "foo|bar|baz"
/// - prefix match such as "foo.*" or "foo.+"
/// - substring match such as ".*foo.*" or ".+bar.+"
#[derive(Debug)]
pub struct PromRegex {
    /// The original expression.
    expr_str: String,

    /// Literal prefix for the regex.
    /// For example, prefix="foo" for regex="foo(a|b)"
    prefix: String,

    /// Set to true if the regex contains only the prefix.
    is_only_prefix: bool,

    /// Set to true if suffix is ".*"
    is_suffix_dot_star: bool,

    /// Set to true if suffix is ".+"
    is_suffix_dot_plus: bool,

    /// Literal string for regex suffix=".*string.*"
    substr_dot_star: String,

    /// Literal string for regex suffix=".+string.+"
    substr_dot_plus: String,

    /// "or" values for the suffix regex.
    /// For example, orValues contain ["foo","bar","baz"] for regex="foo|bar|baz"
    or_values: Vec<String>,

    /// Matcher for "^suffix$".
    ///
    /// PORT NOTE: Go wraps this in bytesutil.FastStringMatcher (a cache of
    /// match results); `lib/bytesutil` is not ported yet, so the anchored
    /// regexp is matched directly. Performance-only divergence.
    /// Byte-matching (`regex::bytes`) like Go's regexp; see the module-level
    /// PORT NOTE on invalid-UTF-8 haystacks.
    re_suffix: regex::bytes::Regex,
}

impl PromRegex {
    /// Returns PromRegex for the given expr (Go `NewPromRegex`).
    pub fn new(expr: &str) -> Result<PromRegex, String> {
        // PORT NOTE: Go validates with regexp.Compile; this port validates
        // with the ported Go-syntax parser (same grammar and limits).
        if let Err(err) = parse_regexp(expr) {
            return Err(err.to_string());
        }
        let (prefix, suffix) = simplify_prom_regex(expr);
        let sre = must_parse_regexp(&suffix);
        let or_values = get_or_values(&sre);
        let is_only_prefix = or_values.len() == 1 && or_values[0].is_empty();
        let is_suffix_dot_star = is_dot_op(&sre, Op::Star);
        let is_suffix_dot_plus = is_dot_op(&sre, Op::Plus);
        let substr_dot_star = get_substring_literal(&sre, Op::Star);
        let substr_dot_plus = get_substring_literal(&sre, Op::Plus);
        // It is expected that Optimize returns valid regexp in suffix, so use MustCompile here.
        // Anchor suffix to the beginning and the end of the matching string.
        //
        // PORT NOTE: Go uses regexp.MustCompile (compile failure = bug). The
        // regex crate can still fail here on syntax Go accepts — in practice
        // a `\p{...}` class name known to Go's unicode package but not to
        // the regex crate — so the failure is reported as an error to the
        // caller instead of a panic (the query still fails at the same
        // user-visible point, with a different message).
        let suffix_expr = format!("^(?:{suffix})$");
        let re_suffix = regex::bytes::Regex::new(&ascii_word_boundaries(&suffix_expr))
            .map_err(|e| e.to_string())?;
        Ok(PromRegex {
            expr_str: expr.to_string(),
            prefix,
            is_only_prefix,
            is_suffix_dot_star,
            is_suffix_dot_plus,
            substr_dot_star,
            substr_dot_plus,
            or_values,
            re_suffix,
        })
    }

    /// Returns true if `s` matches the regex.
    ///
    /// The regex is automatically anchored to the beginning and to the end
    /// of the matching string with '^' and '$'.
    pub fn match_string(&self, s: &str) -> bool {
        self.match_bytes(s.as_bytes())
    }

    /// Returns true if the raw bytes `s` match the regex (Go matches
    /// `string`/`[]byte` payloads byte-wise; this is the primary matcher).
    ///
    /// The regex is automatically anchored to the beginning and to the end
    /// of the matching string with '^' and '$'.
    pub fn match_bytes(&self, s: &[u8]) -> bool {
        // Match over the haystack as Go's `regexp` decodes a `[]byte` (invalid
        // bytes → U+FFFD); a no-op for valid UTF-8 (the common case).
        let s = go_utf8_replace(s);
        let s: &[u8] = &s;
        if self.is_only_prefix {
            return s == self.prefix.as_bytes();
        }

        let mut s = s;
        if !self.prefix.is_empty() {
            if !s.starts_with(self.prefix.as_bytes()) {
                // Fast path - s has another prefix than pr.
                return false;
            }
            s = &s[self.prefix.len()..];
        }

        if self.is_suffix_dot_star {
            // Fast path - the pr contains "prefix.*"
            return true;
        }
        if self.is_suffix_dot_plus {
            // Fast path - the pr contains "prefix.+"
            return !s.is_empty();
        }
        if !self.substr_dot_star.is_empty() {
            // Fast path - pr contains ".*someText.*"
            return find_bytes(s, 0, self.substr_dot_star.as_bytes()).is_some();
        }
        if !self.substr_dot_plus.is_empty() {
            // Fast path - pr contains ".+someText.+"
            return match find_bytes(s, 0, self.substr_dot_plus.as_bytes()) {
                Some(n) => n > 0 && n + self.substr_dot_plus.len() < s.len(),
                None => false,
            };
        }

        if !self.or_values.is_empty() {
            // Fast path - pr contains only alternate strings such as 'foo|bar|baz'
            return self.or_values.iter().any(|v| v.as_bytes() == s);
        }

        // Fall back to slow path by matching the original regexp.
        self.re_suffix.is_match(s)
    }
}

/// Rewrites `s` the way Go's `regexp` sees a `[]byte` haystack: valid UTF-8 is
/// left as-is, and every invalid byte becomes `U+FFFD` — Go's `utf8.DecodeRune`
/// returns `(RuneError, 1)` per invalid byte, so `slice::utf8_chunks` emitting
/// one `U+FFFD` per byte of each invalid chunk reproduces it exactly.
///
/// Returns `s` unchanged when it is already valid UTF-8 (the common case), so
/// valid haystacks still match byte-for-byte. Applied only on the regex-engine
/// slow path, so the byte-literal fast paths (prefix/substr/or) stay byte-exact;
/// it makes rune-oriented constructs (`.`, negated classes, `\p{…}`) match
/// invalid bytes as Go does. `re()`/`=~`/`!~` filtering only — capture
/// extraction still returns the raw bytes.
fn go_utf8_replace(s: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if std::str::from_utf8(s).is_ok() {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = Vec::with_capacity(s.len());
    for chunk in s.utf8_chunks() {
        out.extend_from_slice(chunk.valid().as_bytes());
        for _ in 0..chunk.invalid().len() {
            out.extend_from_slice("\u{FFFD}".as_bytes());
        }
    }
    std::borrow::Cow::Owned(out)
}

impl std::fmt::Display for PromRegex {
    /// Returns string representation of the regex (Go `String`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expr_str)
    }
}

/// Regex implements an optimized string matching for Go regex.
///
/// The following regexs are optimized:
///
/// - plain string such as "foobar"
/// - alternate strings such as "foo|bar|baz"
/// - prefix match such as "foo.*" or "foo.+"
/// - substring match such as ".*foo.*" or ".+bar.+"
#[derive(Debug)]
pub struct Regex {
    /// The original expression.
    expr_str: String,

    /// Literal prefix for the regex.
    prefix: String,

    /// Set to true if the regex contains only the prefix.
    is_only_prefix: bool,

    /// Set to true if suffix is ".*"
    is_suffix_dot_star: bool,

    /// Set to true if suffix is ".+"
    is_suffix_dot_plus: bool,

    /// Literal string for regex suffix=".*string.*"
    substr_dot_star: String,

    /// Literal string for regex suffix=".+string.+"
    substr_dot_plus: String,

    /// "or" values for the suffix regex.
    or_values: Vec<String>,

    /// The regexp for suffix.
    ///
    /// PORT NOTE: byte-matching (`regex::bytes`) like Go's regexp; see the
    /// module-level PORT NOTE on invalid-UTF-8 haystacks.
    suffix_re: regex::bytes::Regex,
}

impl Regex {
    /// Returns Regex for the given expr (Go `NewRegex`).
    pub fn new(expr: &str) -> Result<Regex, String> {
        // PORT NOTE: Go validates with regexp.Compile; this port validates
        // with the ported Go-syntax parser (same grammar and limits).
        if let Err(err) = parse_regexp(expr) {
            return Err(err.to_string());
        }

        let (prefix, suffix) = simplify_regex(expr);
        let sre = must_parse_regexp(&suffix);
        let or_values = get_or_values(&sre);
        let is_only_prefix = or_values.len() == 1 && or_values[0].is_empty();
        let is_suffix_dot_star = is_dot_op(&sre, Op::Star);
        let is_suffix_dot_plus = is_dot_op(&sre, Op::Plus);
        let substr_dot_star = get_substring_literal(&sre, Op::Star);
        let substr_dot_plus = get_substring_literal(&sre, Op::Plus);

        let suffix_anchored = if !prefix.is_empty() {
            format!("^(?:{suffix})")
        } else {
            suffix.clone()
        };
        // The suffixAnchored must be properly compiled, since it has been already checked above.
        //
        // PORT NOTE: Go uses regexp.MustCompile (compile failure = bug). The
        // regex crate can still fail here on syntax Go accepts — in practice
        // a `\p{...}` class name known to Go's unicode package but not to
        // the regex crate — so the failure is reported as an error to the
        // caller instead of a panic (the query still fails at the same
        // user-visible point, with a different message).
        let suffix_re = regex::bytes::Regex::new(&ascii_word_boundaries(&suffix_anchored))
            .map_err(|e| e.to_string())?;

        Ok(Regex {
            expr_str: expr.to_string(),
            prefix,
            is_only_prefix,
            is_suffix_dot_star,
            is_suffix_dot_plus,
            substr_dot_star,
            substr_dot_plus,
            or_values,
            suffix_re,
        })
    }

    /// Returns true if `s` matches the regex.
    pub fn match_string(&self, s: &str) -> bool {
        self.match_bytes(s.as_bytes())
    }

    /// Returns true if the raw bytes `s` match the regex (Go matches
    /// `string`/`[]byte` payloads byte-wise; this is the primary matcher).
    pub fn match_bytes(&self, s: &[u8]) -> bool {
        // Match over the haystack as Go's `regexp` decodes a `[]byte` (invalid
        // bytes → U+FFFD); a no-op for valid UTF-8 (the common case).
        let s = go_utf8_replace(s);
        let s: &[u8] = &s;
        if self.is_only_prefix {
            if self.prefix.is_empty() {
                return true;
            }
            return find_bytes(s, 0, self.prefix.as_bytes()).is_some();
        }

        if self.prefix.is_empty() {
            return self.match_bytes_no_prefix(s);
        }
        self.match_bytes_with_prefix(s)
    }

    /// Returns literals for the regex (Go `GetLiterals`).
    pub fn get_literals(&self) -> Vec<String> {
        let mut sre = must_parse_regexp(&self.expr_str);
        while sre.op == Op::Capture {
            sre = sre.sub.remove(0);
        }

        if let Some(v) = get_literal(&sre) {
            return vec![v];
        }

        if sre.op != Op::Concat {
            return Vec::new();
        }

        let mut a: Vec<String> = Vec::new();
        for sub in &sre.sub {
            if let Some(v) = get_literal(sub) {
                a.push(v);
            }
        }
        a
    }

    fn match_bytes_no_prefix(&self, s: &[u8]) -> bool {
        if self.is_suffix_dot_star {
            return true;
        }
        if self.is_suffix_dot_plus {
            return !s.is_empty();
        }
        if !self.substr_dot_star.is_empty() {
            // Fast path - r contains ".*someText.*"
            return find_bytes(s, 0, self.substr_dot_star.as_bytes()).is_some();
        }
        if !self.substr_dot_plus.is_empty() {
            // Fast path - r contains ".+someText.+"
            return match find_bytes(s, 0, self.substr_dot_plus.as_bytes()) {
                Some(n) => n > 0 && n + self.substr_dot_plus.len() < s.len(),
                None => false,
            };
        }

        if self.or_values.is_empty() {
            // Fall back to slow path by matching the suffix regexp.
            return self.suffix_re.is_match(s);
        }

        // Fast path - compare s to r.orValues
        self.or_values
            .iter()
            .any(|v| find_bytes(s, 0, v.as_bytes()).is_some())
    }

    fn match_bytes_with_prefix(&self, s: &[u8]) -> bool {
        // Go retries the prefix search from the next byte (`s[n+1:]`); the
        // byte-level search below is the same loop.
        let pb = self.prefix.as_bytes();
        let Some(n) = find_bytes(s, 0, pb) else {
            // Fast path - s doesn't contain the needed prefix
            return false;
        };
        let mut next_pos = n + 1;
        let mut cur = n + pb.len();

        {
            let s = &s[cur..];
            if self.is_suffix_dot_star {
                return true;
            }
            if self.is_suffix_dot_plus {
                return !s.is_empty();
            }
            if !self.substr_dot_star.is_empty() {
                // Fast path - r contains ".*someText.*"
                return find_bytes(s, 0, self.substr_dot_star.as_bytes()).is_some();
            }
            if !self.substr_dot_plus.is_empty() {
                // Fast path - r contains ".+someText.+"
                return match find_bytes(s, 0, self.substr_dot_plus.as_bytes()) {
                    Some(n) => n > 0 && n + self.substr_dot_plus.len() < s.len(),
                    None => false,
                };
            }
        }

        loop {
            let stail = &s[cur..];
            if self.or_values.is_empty() {
                // Fall back to slow path by matching the suffix regexp.
                if self.suffix_re.is_match(stail) {
                    return true;
                }
            } else {
                // Fast path - compare s to r.orValues
                for v in &self.or_values {
                    if stail.starts_with(v.as_bytes()) {
                        return true;
                    }
                }
            }

            // Mismatch. Try again starting from the next char.
            let Some(n) = find_bytes(s, next_pos, pb) else {
                // Fast path - s doesn't contain the needed prefix
                return false;
            };
            next_pos = n + 1;
            cur = n + pb.len();
        }
    }
}

impl std::fmt::Display for Regex {
    /// Returns string representation of the regex (Go `String`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.expr_str)
    }
}

/// Byte-level substring search starting at `from`, returning the absolute
/// index of the first occurrence of `needle` in `haystack[from..]`.
fn find_bytes(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if from > haystack.len() {
        return None;
    }
    if needle.is_empty() {
        return Some(from);
    }
    haystack[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|i| from + i)
}

#[cfg(test)]
#[path = "regexutil/tests.rs"]
mod tests;
