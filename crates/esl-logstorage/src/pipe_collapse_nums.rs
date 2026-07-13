//! Port of `pipe_collapse_nums.go` — the `| collapse_nums ...` pipe, which
//! replaces number-looking substrings in a field value with the `<N>`
//! placeholder (optionally prettifying common composite placeholders such as
//! `<UUID>`, `<IP4>`, `<DATETIME>`).
//!
//! The number-boundary scanners (`index_num_start` / `index_num_end` /
//! `is_valid_num` / `is_special_num_start` / `is_special_num_end` / ...) are
//! exposed as `pub(crate)` because they are also needed by
//! `filter_pattern_match` / `pattern_matcher` (Go shares them from this file).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::{
    IfFilter, new_pipe_update_processor, update_needed_fields_for_update_pipe,
};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::tokenizer::is_token_char;

/// `pipeCollapseNums` implements `| collapse_nums ...`.
pub struct PipeCollapseNums {
    /// the field to collapse nums at
    pub(crate) field: String,

    /// if set, collapsed nums are prettified with common placeholders
    pub(crate) is_prettify: bool,

    /// optional filter for skipping the collapse_nums operation
    pub(crate) iff: Option<Arc<IfFilter>>,
}

/// Constructs a `collapse_nums` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeCollapseNums` is lexer-dependent and deferred; this
/// constructor takes the parsed field, prettify flag and optional `if` filter
/// directly.
pub(crate) fn new_pipe_collapse_nums(
    field: String,
    is_prettify: bool,
    iff: Option<Arc<IfFilter>>,
) -> PipeCollapseNums {
    PipeCollapseNums {
        field,
        is_prettify,
        iff,
    }
}

impl Pipe for PipeCollapseNums {
    /// Port of Go `pipeCollapseNums.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    /// Go `hasFilterInWithQuery` for this pipe: checks the `if (...)` filter.
    fn has_filter_in_with_query(&self) -> bool {
        self.iff
            .as_ref()
            .is_some_and(|iff| iff.has_filter_in_with_query())
    }

    /// Go `initFilterInValues` for this pipe: rewrites the `if (...)` filter.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.init_filter_in_values(get_values, timestamp)?
        {
            self.iff = Some(Arc::new(iff_new));
        }
        Ok(())
    }

    /// Go `visitSubqueries` for this pipe: propagates into the `if (...)` filter.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.visit_subqueries_mut(timestamp, visit)
        {
            self.iff = Some(Arc::new(iff_new));
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::from("collapse_nums");
        if let Some(iff) = &self.iff {
            s += " ";
            s += &iff.to_string();
        }
        if self.field != "_msg" {
            s += " at ";
            s += &quote_token_if_needed(&self.field);
        }
        if self.is_prettify {
            s += " prettify";
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        update_needed_fields_for_update_pipe(pf, &self.field, self.iff.as_deref());
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let is_prettify = self.is_prettify;
        // PORT NOTE: Go threads a pooled `arena` through `updateFunc`; the Rust
        // port returns an owned `String` (see pipe_update.rs module docs).
        let update_func: crate::pipe_update::UpdateFunc = Arc::new(move |v: &str| {
            let mut b = Vec::new();
            append_collapse_nums(&mut b, v.as_bytes());
            if is_prettify {
                b = prettify_collapsed_nums(&b);
            }
            // Input is valid UTF-8; collapsing only rewrites ASCII digit/hex
            // runs and copies other bytes verbatim, so the result is UTF-8.
            String::from_utf8_lossy(&b).into_owned()
        });

        new_pipe_update_processor(
            update_func,
            pp_next,
            self.field.clone(),
            self.iff.clone(),
            concurrency,
        )
    }
}

// ---------------------------------------------------------------------------
// Number-collapsing helpers (Go `appendCollapseNums` and friends).
// ---------------------------------------------------------------------------

/// Port of Go `appendCollapseNums`.
pub(crate) fn append_collapse_nums(dst: &mut Vec<u8>, s: &[u8]) {
    let mut offset = 0;
    while offset < s.len() {
        let num_start = index_num_start(s, offset);
        if num_start < 0 {
            dst.extend_from_slice(&s[offset..]);
            return;
        }
        let num_start = num_start as usize;
        dst.extend_from_slice(&s[offset..num_start]);

        let num_end = index_num_end(s, num_start);
        if !is_valid_num(s, num_start, num_end) {
            dst.extend_from_slice(&s[num_start..num_end]);
        } else {
            dst.extend_from_slice(b"<N>");
        }
        offset = num_end;
    }
}

/// Port of Go `indexNumStart`. Returns `-1` when no number start is found.
///
/// It is safe iterating by bytes instead of Unicode runes, since decimal and
/// hex chars are ASCII and cannot clash with UTF-8 encoded Unicode runes.
pub(crate) fn index_num_start(s: &[u8], offset: usize) -> isize {
    let mut n = offset;
    while n < s.len() {
        if !is_decimal_or_hex_char(s[n]) {
            n += 1;
            continue;
        }
        if n == 0 {
            return 0;
        }
        if !is_token_char(s[n - 1]) || is_special_num_start(s[n - 1]) {
            return n as isize;
        }
        n += 1;
    }
    -1
}

/// Port of Go `indexNumEnd`.
pub(crate) fn index_num_end(s: &[u8], offset: usize) -> usize {
    let mut n = offset;
    while n < s.len() && is_decimal_or_hex_char(s[n]) {
        n += 1;
    }
    n
}

/// Port of Go `isValidNum`.
pub(crate) fn is_valid_num(s: &[u8], start: usize, end: usize) -> bool {
    if end < s.len() && is_token_char(s[end]) && !is_special_num_end(s[end]) {
        return false;
    }
    can_be_treated_as_num(&s[start..end])
}

/// Port of Go `isDecimalOrHexChar`.
pub(crate) fn is_decimal_or_hex_char(ch: u8) -> bool {
    if ch.is_ascii_digit() {
        return true;
    }
    is_hex_char(ch)
}

/// Port of Go `isHexChar`.
pub(crate) fn is_hex_char(ch: u8) -> bool {
    (b'a'..=b'f').contains(&ch) || (b'A'..=b'F').contains(&ch)
}

/// Port of Go `canBeTreatedAsNum`.
pub(crate) fn can_be_treated_as_num(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    if !has_hex_chars(s) {
        // Decimal number can contain any number of chars.
        return true;
    }
    // Most hex nums contain 4+ chars, and the number of chars is usually even.
    // This prevents incorrect detection of hex nums such as "be", "ad", etc.
    if s.len() < 4 || s.len() % 2 == 1 {
        return false;
    }
    true
}

/// Port of Go `hasHexChars`.
pub(crate) fn has_hex_chars(s: &[u8]) -> bool {
    s.iter().any(|&c| is_hex_char(c))
}

/// Port of Go `isSpecialNumStart`.
pub(crate) fn is_special_num_start(ch: u8) -> bool {
    ch == b'_'
        || ch == b'T'
        || ch == b'X'
        || ch == b'x'
        || ch == b'v'
        || ch == b's'
        || ch == b'h'
        || ch == b'm'
}

/// Port of Go `isSpecialNumEnd`.
pub(crate) fn is_special_num_end(ch: u8) -> bool {
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
// Prettify helpers (Go `appendPrettifyCollapsedNums` and friends).
// ---------------------------------------------------------------------------

/// Port of Go `appendPrettifyCollapsedNums`: rewrites composite `<N>` sequences
/// into higher-level placeholders. `src` is the already-collapsed input.
///
/// PORT NOTE: Go ping-pongs a single append buffer (`dst[:dstLen]` /
/// `dst[dstLen:]`); the Rust port threads owned `Vec<u8>`s between stages, which
/// is behaviorally identical.
fn prettify_collapsed_nums(src: &[u8]) -> Vec<u8> {
    let mut cur = replace_with_skip_tail(src, "<N>-<N>-<N>-<N>-<N>", "<UUID>", None);
    cur = replace_with_skip_tail(&cur, "<N>.<N>.<N>.<N>", "<IP4>", None);
    cur = replace_with_skip_tail(&cur, "<N>:<N>:<N>", "<TIME>", Some(skip_trailing_subsecs));
    cur = replace_with_skip_tail(&cur, "<N>-<N>-<N>", "<DATE>", None);
    cur = replace_with_skip_tail(&cur, "<N>/<N>/<N>", "<DATE>", None);
    cur = replace_with_skip_tail(
        &cur,
        "<DATE>T<TIME>",
        "<DATETIME>",
        Some(skip_trailing_timezone),
    );
    cur = replace_with_skip_tail(
        &cur,
        "<DATE> <TIME>",
        "<DATETIME>",
        Some(skip_trailing_timezone),
    );
    cur
}

/// Port of Go `appendReplaceWithSkipTail`.
fn replace_with_skip_tail(
    src: &[u8],
    old: &str,
    replacement: &str,
    skip_tail: Option<fn(&[u8]) -> usize>,
) -> Vec<u8> {
    let old = old.as_bytes();
    let replacement = replacement.as_bytes();
    if replacement.len() > old.len() {
        panic!(
            "BUG: len(replacement)={} cannot exceed len(old)={}",
            replacement.len(),
            old.len()
        );
    }

    let mut dst = Vec::with_capacity(src.len());
    let mut offset = 0;
    while offset < src.len() {
        match find_subslice(&src[offset..], old) {
            None => break,
            Some(n) => {
                dst.extend_from_slice(&src[offset..offset + n]);
                dst.extend_from_slice(replacement);
                offset += n + old.len();
                if let Some(skip) = skip_tail {
                    offset += skip(&src[offset..]);
                }
            }
        }
    }
    dst.extend_from_slice(&src[offset..]);
    dst
}

/// Returns the byte index of the first occurrence of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Port of Go `skipTrailingSubsecs`.
fn skip_trailing_subsecs(s: &[u8]) -> usize {
    if s.starts_with(b".<N>") || s.starts_with(b",<N>") {
        return ".<N>".len();
    }
    0
}

/// Port of Go `skipTrailingTimezone`.
fn skip_trailing_timezone(s: &[u8]) -> usize {
    if s.starts_with(b"Z") {
        return 1;
    }
    if s.starts_with(b"-<N>:<N>") || s.starts_with(b"+<N>:<N>") {
        return "-<N>:<N>".len();
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Filter;
    use crate::filter_not::new_filter_not;
    use crate::filter_phrase::new_filter_phrase;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeCollapseNumsSuccess` / `TestParsePipeCollapseNumsFailure`
    // exercise the lexer-based `parsePipeCollapseNums`, which is deferred; they
    // are omitted until the LogsQL parser is ported.

    fn phrase_iff(field: &str, phrase: &str) -> Arc<IfFilter> {
        let f: Arc<dyn Filter> = Arc::new(new_filter_phrase(field, phrase));
        Arc::new(IfFilter::new(f))
    }

    fn collapse(field: &str, is_prettify: bool, iff: Option<Arc<IfFilter>>) -> PipeCollapseNums {
        new_pipe_collapse_nums(field.to_string(), is_prettify, iff)
    }

    fn s(v: &str) -> String {
        String::from_utf8_lossy(&{
            let mut b = Vec::new();
            append_collapse_nums(&mut b, v.as_bytes());
            b
        })
        .into_owned()
    }

    fn s_pretty(v: &str) -> String {
        let mut b = Vec::new();
        append_collapse_nums(&mut b, v.as_bytes());
        let b = prettify_collapsed_nums(&b);
        String::from_utf8_lossy(&b).into_owned()
    }

    #[test]
    fn test_pipe_collapse_nums() {
        // collapse_nums prettify (no if / at)
        let p = collapse("_msg", true, None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_msg", "2004-10-12T43:23:12Z abc:345"), ("bar", "cde")],
                    &[("_msg", "a_bc_def")],
                    &[("_msg", "1234")],
                ]),
            ),
            &rows(&[
                &[("_msg", "<DATETIME> abc:<N>"), ("bar", "cde")],
                &[("_msg", "a_bc_def")],
                &[("_msg", "<N>")],
            ]),
        );

        // collapse_nums at bar prettify
        let p = collapse("bar", true, None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_msg", "2004-10-12T43:23:12Z abc:345"), ("bar", "cde")],
                    &[("_msg", "a_bc_def"), ("bar", "ip: 12.34.56.78")],
                    &[("_msg", "1234")],
                ]),
            ),
            &rows(&[
                &[("_msg", "2004-10-12T43:23:12Z abc:345"), ("bar", "cde")],
                &[("_msg", "a_bc_def"), ("bar", "ip: <IP4>")],
                &[("_msg", "1234"), ("bar", "")],
            ]),
        );

        // collapse_nums if (-abc)
        let iff = Arc::new(IfFilter::new(Arc::new(new_filter_not(Box::new(
            new_filter_phrase("_msg", "abc"),
        )))));
        let p = collapse("_msg", false, Some(iff));
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_msg", "2004-10-12T43:23:12Z abc:345"), ("bar", "cde")],
                    &[("_msg", "a_bc_def")],
                    &[("_msg", "1234")],
                ]),
            ),
            &rows(&[
                &[("_msg", "2004-10-12T43:23:12Z abc:345"), ("bar", "cde")],
                &[("_msg", "a_bc_def")],
                &[("_msg", "<N>")],
            ]),
        );

        // underscore-delimited numbers collapse
        let p = collapse("_msg", false, None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("_msg", "temp_23_175863242537_93_98_ abc 123 test")]]),
            ),
            &rows(&[&[("_msg", "temp_<N>_<N>_<N>_<N>_ abc <N> test")]]),
        );
    }

    #[test]
    fn test_pipe_collapse_nums_update_needed_fields() {
        // all the needed fields
        let p = collapse("_msg", false, None);
        assert_needed_fields(&p, "*", "", "*", "");
        let p = collapse("x", false, Some(phrase_iff("f1", "q")));
        assert_needed_fields(&p, "*", "", "*", "");

        // unneeded fields do not intersect with at field
        let p = collapse("x", false, None);
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");
        let p = collapse("x", false, Some(phrase_iff("f3", "q")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");
        let p = collapse("x", false, Some(phrase_iff("f2", "q")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1");

        // unneeded fields intersect with at field
        let p = collapse("x", false, None);
        assert_needed_fields(&p, "*", "x,y", "*", "x,y");
        let p = collapse("x", false, Some(phrase_iff("f1", "q")));
        assert_needed_fields(&p, "*", "x,y", "*", "x,y");
        let p = collapse("x", false, Some(phrase_iff("x", "q")));
        assert_needed_fields(&p, "*", "x,y", "*", "x,y");
        let p = collapse("x", false, Some(phrase_iff("y", "q")));
        assert_needed_fields(&p, "*", "x,y", "*", "x,y");

        // needed fields do not intersect with at field
        let p = collapse("x", false, None);
        assert_needed_fields(&p, "f2,y", "", "f2,y", "");
        let p = collapse("x", false, Some(phrase_iff("f1", "q")));
        assert_needed_fields(&p, "f2,y", "", "f2,y", "");

        // needed fields intersect with at field
        let p = collapse("y", false, None);
        assert_needed_fields(&p, "f2,y", "", "f2,y", "");
        let p = collapse("y", false, Some(phrase_iff("f1", "q")));
        assert_needed_fields(&p, "f2,y", "", "f1,f2,y", "");
    }

    #[test]
    fn test_append_collapse_nums() {
        assert_eq!(s(""), "");
        assert_eq!(s("foo"), "foo");
        assert_eq!(s("ad"), "ad");
        assert_eq!(s("abc"), "abc");
        assert_eq!(s("deadbeef"), "<N>");
        assert_eq!(
            s("a b c d e f ad be:eac,dead beef ab"),
            "a b c d e f ad be:eac,<N> <N> ab"
        );
        assert_eq!(s("ыва"), "ыва");
        assert_eq!(s("0"), "<N>");
        assert_eq!(s("1234567890"), "<N>");
        assert_eq!(s("1foo"), "1foo");
        assert_eq!(s("1 foo"), "<N> foo");
        assert_eq!(s("a1foo2bar34"), "a1foo2bar34");
        assert_eq!(s("a.1Zfoo.2Tbar:34"), "a.<N>Zfoo.<N>Tbar:<N>");
        assert_eq!(s("ЫВА123bar45.78"), "ЫВА123bar45.<N>");
        assert_eq!(s("ЫВА.123.bar.45.78"), "ЫВА.<N>.bar.<N>.<N>");
        assert_eq!(s("1.23.45.67"), "<N>.<N>.<N>.<N>");
        assert_eq!(
            s("2024-12-25T10:20:30Z foo"),
            "<N>-<N>-<N>T<N>:<N>:<N>Z foo"
        );
        assert_eq!(
            s("2024-12-25T10:20:30.123324+05:00 foo"),
            "<N>-<N>-<N>T<N>:<N>:<N>.<N>+<N>:<N> foo"
        );
        assert_eq!(s("release v1.2.3"), "release v<N>.<N>.<N>");
        assert_eq!(
            s("2004-10-12T43:23:12Z abc:345"),
            "<N>-<N>-<N>T<N>:<N>:<N>Z abc:<N>"
        );
        assert_eq!(s("123.43s"), "<N>.<N>s");
        assert_eq!(
            s("123ms 2us 3h5m6s43ms43μs324ns"),
            "<N>ms <N>us <N>h<N>m<N>s<N>ms<N>μs<N>ns"
        );
        assert_eq!(s("0x1234 0XFEAD12"), "0x<N> 0X<N>");

        // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/703
        assert_eq!(s("foo123_456_789"), "foo123_<N>_<N>");
        assert_eq!(
            s("temp_23_175863242537_93_98_ abc 123 test"),
            "temp_<N>_<N>_<N>_<N>_ abc <N> test"
        );

        // non-ascii chars must be treated as number delimiters
        assert_eq!(s("ЙЦ123ук"), "ЙЦ<N>ук");
    }

    #[test]
    fn test_append_collapse_nums_prettified() {
        assert_eq!(s_pretty(""), "");
        assert_eq!(s_pretty("foo"), "foo");
        assert_eq!(
            s_pretty(
                "35.191.193.225:51648 - 2edfed59-3e98-4073-bbb2-28d321ca71a7 - - [2024/12/08 15:21:02] 10.71.20.32 GET /foo 200"
            ),
            "<IP4>:<N> - <UUID> - - [<DATETIME>] <IP4> GET /foo <N>"
        );
        assert_eq!(
            s_pretty("E1208 15:21:02.748877 62 metric_reporter.go:182"),
            "E1208 <TIME> <N> metric_reporter.go:<N>"
        );
        assert_eq!(
            s_pretty("2024-12-08T15:22:32.342Z error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
        assert_eq!(
            s_pretty("2024-12-08 15:22:32Z error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
        assert_eq!(
            s_pretty("2024-12-08 15:22:32,123 error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
        assert_eq!(
            s_pretty("2024-12-08 15:22:32.123+10:30 error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
        assert_eq!(
            s_pretty("2024-12-08 15:22:32.123-10:30 error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
        assert_eq!(
            s_pretty("2024/12/08T15:22:32-10:30 error exporterhelper/queued_retry.go:101"),
            "<DATETIME> error exporterhelper/queued_retry.go:<N>"
        );
    }
}
