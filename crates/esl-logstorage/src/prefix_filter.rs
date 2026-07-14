//! Port of `lib/prefixfilter/filter.go` from EsLogs v1.51.0.
//!
//! Filtering by full strings and by prefixes ending with '*'.
//!
//! PORT NOTE: Go strings are raw bytes and every comparison here is byte-wise
//! (`strings.HasPrefix`/`==`), so the port stores filters as `Vec<u8>` and the
//! public API accepts any byte-like input (`impl AsRef<[u8]>`), letting both
//! `&str` literals and raw `Field.name` bytes flow through one byte-native
//! surface.

use std::fmt;

/// Filter allows filtering by full strings and by prefixes ending with '*'.
///
/// PORT NOTE: Go's `Filter.Clone()` copies via `copyFrom`; the derived `Clone`
/// impl is semantically identical.
#[derive(Clone, Debug, Default)]
pub struct Filter {
    allow: InnerFilter,
    deny: InnerFilter,
}

impl Filter {
    /// Resets the filter to the initial zero state.
    pub fn reset(&mut self) {
        self.allow.reset();
        self.deny.reset();
    }

    /// Returns the list of allow strings if there are no wildcard filters in the allow list.
    ///
    /// It returns `None` if there are wildcard filters.
    pub fn get_allow_strings(&self) -> Option<&[Vec<u8>]> {
        if self.allow.wildcards.is_empty() {
            return Some(&self.allow.full_strings);
        }
        None
    }

    /// Returns allow filters from the filter.
    pub fn get_allow_filters(&self) -> Vec<Vec<u8>> {
        self.allow.get_filters()
    }

    /// Returns deny filters from the filter.
    pub fn get_deny_filters(&self) -> Vec<Vec<u8>> {
        self.deny.get_filters()
    }

    /// Returns true if the filter doesn't match anything.
    pub fn match_nothing(&self) -> bool {
        self.allow.match_nothing()
    }

    /// Returns true if the filter matches any string.
    pub fn match_all(&self) -> bool {
        if !self.allow.match_all() {
            return false;
        }
        self.deny.match_nothing()
    }

    /// Returns true if s matches the filter.
    ///
    /// s may be either a regular string or a wildcard ending with '*'.
    /// If s is a wildcard, then true is returned if at least a single string
    /// matching this wildcard matches the filter.
    pub fn match_string_or_wildcard(&self, s: impl AsRef<[u8]>) -> bool {
        let s = s.as_ref();
        if !is_wildcard_filter(s) {
            return self.match_string(s);
        }

        let wildcard = &s[..s.len() - 1];
        if !self.allow.match_wildcard_filter(wildcard) {
            return false;
        }
        !self.deny.match_wildcard(wildcard)
    }

    /// Returns true if s matches the filter.
    ///
    /// PORT NOTE: Go's `MatchString` returns false for a nil `*Filter`
    /// receiver; Rust callers holding an `Option<&Filter>` map `None` to
    /// false at the call site instead.
    pub fn match_string(&self, s: impl AsRef<[u8]>) -> bool {
        let s = s.as_ref();
        if !self.allow.match_string(s) {
            return false;
        }
        !self.deny.match_string(s)
    }

    fn normalize(&mut self) {
        if self.allow.wildcards.is_empty() {
            self.deny.reset();
        }
    }

    /// Adds the given filters to allowlist at the filter.
    ///
    /// Every filter may end with '*'. In this case it matches all the strings
    /// starting with the prefix before '*'.
    pub fn add_allow_filters<S: AsRef<[u8]>>(&mut self, filters: &[S]) {
        for filter in filters {
            self.add_allow_filter(filter.as_ref());
        }
    }

    /// Adds the given filter to allowlist at the filter.
    ///
    /// The filter may end with '*'. In this case it matches all the strings
    /// starting with the prefix before '*'.
    pub fn add_allow_filter(&mut self, filter: impl AsRef<[u8]>) {
        let filter = filter.as_ref();
        self.allow.add_filter(filter);
        self.deny.remove_filter(filter, true);

        self.normalize();
    }

    /// Adds the given filters to denylist at the filter.
    ///
    /// Every filter may end with '*'. In this case it stops matching all the
    /// strings starting with the prefix before '*'.
    pub fn add_deny_filters<S: AsRef<[u8]>>(&mut self, filters: &[S]) {
        for filter in filters {
            self.add_deny_filter(filter.as_ref());
        }
    }

    /// Adds the given filter to denylist at the filter.
    ///
    /// Every filter may end with '*'. In this case it stops matching all the
    /// strings starting with the prefix before '*'.
    pub fn add_deny_filter(&mut self, filter: impl AsRef<[u8]>) {
        let filter = filter.as_ref();
        if !self.match_string_or_wildcard(filter) {
            // Nothing to deny.
            return;
        }

        self.allow.remove_filter(filter, false);
        self.deny.add_filter(filter);

        self.normalize();
    }
}

impl fmt::Display for Filter {
    /// Returns human-readable representation of the filter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let allow = self.get_allow_filters();
        let deny = self.get_deny_filters();

        write!(
            f,
            "allow=[{}], deny=[{}]",
            join_quoted_strings(&allow),
            join_quoted_strings(&deny)
        )
    }
}

// Go `joinQuotedStrings`: quotes via strconv.Quote (`go_quote_bytes`), so
// non-ASCII/control/invalid-UTF-8 bytes render exactly like Go.
fn join_quoted_strings(a: &[Vec<u8>]) -> String {
    let tmp: Vec<String> = a
        .iter()
        .map(|s| crate::stream_filter::go_quote_bytes(s))
        .collect();
    tmp.join(",")
}

#[derive(Clone, Debug, Default)]
struct InnerFilter {
    full_strings: Vec<Vec<u8>>,
    wildcards: Vec<Vec<u8>>,
}

impl InnerFilter {
    fn reset(&mut self) {
        self.full_strings.clear();
        self.wildcards.clear();
    }

    fn get_filters(&self) -> Vec<Vec<u8>> {
        let mut filters = self.full_strings.clone();
        for wc in &self.wildcards {
            let mut f = wc.clone();
            f.push(b'*');
            filters.push(f);
        }
        filters.sort();
        filters
    }

    fn match_all(&self) -> bool {
        self.wildcards.iter().any(|wc| wc.is_empty())
    }

    fn match_nothing(&self) -> bool {
        self.full_strings.is_empty() && self.wildcards.is_empty()
    }

    fn add_filter(&mut self, filter: &[u8]) {
        if !is_wildcard_filter(filter) {
            self.add_full_string(filter);
            return;
        }

        let wildcard = &filter[..filter.len() - 1];
        self.add_wildcard(wildcard);
    }

    fn add_wildcard(&mut self, wildcard: &[u8]) {
        if !self.match_wildcard(wildcard) {
            self.drop_wildcard(wildcard);
            self.wildcards.push(wildcard.to_vec());
        }
    }

    fn remove_filter(&mut self, filter: &[u8], remove_broader_wildcards: bool) {
        if !is_wildcard_filter(filter) {
            self.remove_full_string(filter);
        } else {
            let wildcard = &filter[..filter.len() - 1];
            self.drop_wildcard(wildcard);
        }

        if remove_broader_wildcards {
            let s = filter.strip_suffix(b"*").unwrap_or(filter);
            self.wildcards.retain(|wc| !s.starts_with(wc.as_slice()));
        }
    }

    fn drop_wildcard(&mut self, wildcard: &[u8]) {
        // drop the wildcard together with weaker wildcards
        self.wildcards.retain(|wc| !wc.starts_with(wildcard));

        // drop full strings matching the wildcard
        self.full_strings.retain(|s| !s.starts_with(wildcard));
    }

    fn add_full_string(&mut self, s: &[u8]) {
        if !self.match_string(s) {
            self.full_strings.push(s.to_vec());
        }
    }

    fn remove_full_string(&mut self, s: &[u8]) {
        self.full_strings.retain(|x| x != s);
    }

    fn match_string(&self, s: &[u8]) -> bool {
        if self.match_nothing() {
            // Fast path for common case when there are no filters.
            return false;
        }

        // Slower path for regular case.
        if self.match_wildcard(s) {
            return true;
        }
        self.full_strings.iter().any(|x| x == s)
    }

    fn match_wildcard_filter(&self, wildcard: &[u8]) -> bool {
        for wc in &self.wildcards {
            if wildcard.starts_with(wc.as_slice()) || wc.starts_with(wildcard) {
                return true;
            }
        }
        for s in &self.full_strings {
            if s.starts_with(wildcard) {
                return true;
            }
        }
        false
    }

    fn match_wildcard(&self, wildcard: &[u8]) -> bool {
        self.wildcards
            .iter()
            .any(|wc| wildcard.starts_with(wc.as_slice()))
    }
}

/// Returns true if the filter ends with '*', e.g. it matches any string containing the prefix in front of '*'.
pub fn is_wildcard_filter(filter: impl AsRef<[u8]>) -> bool {
    filter.as_ref().last() == Some(&b'*')
}

/// Returns true if s matches filter.
pub fn match_filter(filter: impl AsRef<[u8]>, s: impl AsRef<[u8]>) -> bool {
    let filter = filter.as_ref();
    let s = s.as_ref();
    if !is_wildcard_filter(filter) {
        return filter == s;
    }
    let wildcard = &filter[..filter.len() - 1];
    s.starts_with(wildcard)
}

/// Returns true if s matches any filter from filters.
pub fn match_filters<S: AsRef<[u8]>>(filters: &[S], s: impl AsRef<[u8]>) -> bool {
    let s = s.as_ref();
    filters.iter().any(|filter| match_filter(filter, s))
}

/// Returns true if filters match any string.
pub fn match_all<S: AsRef<[u8]>>(filters: &[S]) -> bool {
    filters.iter().any(|filter| filter.as_ref() == b"*")
}

/// Replaces `src_filter` prefix with `dst_filter` prefix at s and appends the result to dst.
///
/// PORT NOTE: Go returns the (possibly reallocated) dst slice; Rust appends to
/// the `Vec` in place, following the esl-common `marshal_*` convention.
pub fn append_replace(
    dst: &mut Vec<u8>,
    src_filter: impl AsRef<[u8]>,
    dst_filter: impl AsRef<[u8]>,
    s: impl AsRef<[u8]>,
) {
    let src_filter = src_filter.as_ref();
    let dst_filter = dst_filter.as_ref();
    let s = s.as_ref();
    if !is_wildcard_filter(src_filter) {
        if s == src_filter {
            dst.extend_from_slice(dst_filter);
        } else {
            dst.extend_from_slice(s);
        }
        return;
    }

    let src_prefix = &src_filter[..src_filter.len() - 1];
    if !s.starts_with(src_prefix) {
        dst.extend_from_slice(s);
        return;
    }
    if !is_wildcard_filter(dst_filter) {
        dst.extend_from_slice(dst_filter);
        return;
    }

    let src_suffix = &s[src_prefix.len()..];
    let dst_prefix = &dst_filter[..dst_filter.len() - 1];
    dst.extend_from_slice(dst_prefix);
    dst.extend_from_slice(src_suffix);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_wildcard_filter() {
        let f = |filter: &str, result_expected: bool| {
            let result = is_wildcard_filter(filter);
            assert_eq!(
                result, result_expected,
                "unexpected result for is_wildcard_filter({filter:?}); got {result}; want {result_expected}"
            );
        };

        f("", false);
        f("foo", false);
        f("*", true);
        f("foo*", true);
        f("*f", false);
        f("*f*", true);
    }

    #[test]
    fn test_match_filter() {
        let f = |filter: &str, s: &str, result_expected: bool| {
            let result = match_filter(filter, s);
            assert_eq!(
                result, result_expected,
                "unexpected result for match_filter({filter:?}, {s:?}); got {result}; want {result_expected}"
            );
        };

        f("", "", true);
        f("", "foo", false);
        f("foo", "", false);
        f("foo", "foo", true);
        f("foo", "foobar", false);
        f("foo", "bar", false);

        f("*", "", true);
        f("*", "foo", true);
        f("a*", "", false);
        f("a*", "a", true);
        f("a*", "abc", true);
        f("a*", "foo", false);
    }

    #[test]
    fn test_match_filters() {
        let f = |filters: &[&str], s: &str, result_expected: bool| {
            let result = match_filters(filters, s);
            assert_eq!(
                result, result_expected,
                "unexpected result for match_filters({filters:?}, {s:?}); got {result}; want {result_expected}"
            );
        };

        f(&[], "", false);
        f(&[], "foo", false);
        f(&[""], "", true);
        f(&[""], "foo", false);
        f(&["foo"], "", false);
        f(&["foo", ""], "", true);
        f(&["foo", "ba*"], "", false);
        f(&["foo", "ba*"], "foo", true);
        f(&["foo", "ba*"], "foobar", false);
        f(&["foo", "ba*"], "ba", true);
        f(&["foo", "ba*"], "bar", true);
    }

    #[test]
    fn test_match_all() {
        let f = |filters: &[&str], result_expected: bool| {
            let result = match_all(filters);
            assert_eq!(
                result, result_expected,
                "unexpected result for match_all({filters:?}); got {result}; want {result_expected}"
            );
        };

        f(&[], false);
        f(&["foo"], false);
        f(&["foo", "bar*"], false);
        f(&["foo", "*", "abc"], true);
        f(&["*"], true);
    }

    #[test]
    fn test_append_replace() {
        let f = |src_filter: &str, dst_filter: &str, s: &str, result_expected: &str| {
            let mut result = Vec::new();
            append_replace(&mut result, src_filter, dst_filter, s);
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result for append_replace({src_filter:?}, {dst_filter:?}, {s:?}); got {:?}; want {result_expected:?}",
                String::from_utf8_lossy(&result)
            );
        };

        // Full string
        f("", "", "", "");
        f("", "", "foo", "foo");
        f("foo", "bar", "baz", "baz");
        f("foo", "bar", "foo", "bar");

        // Prefix only at src_filter
        f("foo.*", "bar", "foo", "foo");
        f("foo.*", "bar", "foo.", "bar");
        f("foo.*", "bar", "foo.xyz", "bar");

        // Prefix only at dst_filter
        f("foo", "bar.*", "a", "a");
        f("foo", "bar.*", "foo", "bar.*");
        f("foo", "bar.*", "foo.", "foo.");

        // Prefix at both src_filter and dst_filter
        f("foo.*", "bar.baz.*", "foo", "foo");
        f("foo.*", "bar.baz.*", "foo.", "bar.baz.");
        f("foo.*", "bar.baz.*", "foo.x", "bar.baz.x");
        f("foo.*", "bar.baz.*", "foo.xyz", "bar.baz.xyz");
    }

    #[test]
    fn test_filter_match_nothing() {
        let mut f = Filter::default();

        assert!(
            f.match_nothing(),
            "match_nothing must return true for empty filter"
        );

        // Allow some
        f.add_allow_filters(&["foo", "bar*"]);
        assert!(
            !f.match_nothing(),
            "match_nothing must return false for non-empty filter"
        );

        // Deny some
        f.add_deny_filters(&["abc", "def*"]);
        assert!(
            !f.match_nothing(),
            "match_nothing must return false for non-empty filter"
        );

        // Deny all
        f.add_deny_filter("*");
        assert!(
            f.match_nothing(),
            "match_nothing must return true for empty filter"
        );

        // Allow some and then reset
        f.add_allow_filter("foo*");
        f.add_allow_filter("bar");
        f.reset();
        assert!(
            f.match_nothing(),
            "match_nothing must return true for empty filter"
        );
    }

    #[test]
    fn test_filter_match_all() {
        let mut f = Filter::default();

        assert!(
            !f.match_all(),
            "match_all() must return false for empty filter"
        );

        f.add_allow_filter("foo");
        assert!(
            !f.match_all(),
            "match_all() must return false for filter without *"
        );

        f.add_allow_filter("bar*");
        assert!(
            !f.match_all(),
            "match_all() must return false for filter without *"
        );

        f.add_allow_filter("*");
        assert!(f.match_all(), "match_all() must return true for * filter");

        f.add_deny_filter("foo");
        assert!(
            !f.match_all(),
            "match_all() must return false for filter with non-empty deny filters"
        );

        f.add_deny_filter("bar*");
        assert!(
            !f.match_all(),
            "match_all() must return false for filter with non-empty deny filters"
        );

        f.add_allow_filter("*");
        assert!(f.match_all(), "match_all() must return true for * filter");

        f.reset();
        assert!(
            !f.match_all(),
            "match_all() must return false for empty filter"
        );
    }

    #[test]
    fn test_filter_match_string_nil_filter() {
        // PORT NOTE: Go calls MatchString on a nil *Filter and expects false;
        // the Rust equivalent is an Option<&Filter> mapping None to false.
        let f = |s: &str| {
            let filter: Option<&Filter> = None;
            assert!(
                !filter.is_some_and(|f| f.match_string(s)),
                "unexpected match_string({s:?}) for nil Filter; got true; want false"
            );
        };

        f("");
        f("foo");
    }

    #[test]
    fn test_filter_clone() {
        let f = |allow: &[&str], deny: &[&str]| {
            let mut f = Filter::default();
            f.add_allow_filters(allow);
            f.add_deny_filters(deny);
            let f_copy = f.clone();

            let f_str = f.to_string();
            let f_copy_str = f_copy.to_string();

            assert_eq!(
                f_str, f_copy_str,
                "unexpected result; got\n{f_str}\nwant\n{f_copy_str}"
            );
        };

        f(&[], &[]);
        f(&["foo", "bar*"], &[]);
        f(&["foo", "bar*"], &["baz", "x*"]);
    }

    #[test]
    fn test_filter_get_allow_strings() {
        let f = |allow: &[&str], deny: &[&str], result_expected: Option<&[&str]>| {
            let mut f = Filter::default();

            f.add_allow_filters(allow);
            f.add_deny_filters(deny);

            let result = f.get_allow_strings().map(|ss| {
                ss.iter()
                    .map(|s| std::str::from_utf8(s).unwrap())
                    .collect::<Vec<_>>()
            });
            let expected = result_expected.map(<[&str]>::to_vec);
            assert_eq!(
                result, expected,
                "unexpected result; got\n{result:?}\nwant\n{expected:?}"
            );
        };

        f(&[], &[], Some(&[]));
        f(&["*"], &[], None);
        f(&["foo", "bar", "baz*"], &[], None);
        f(&["foo", "bar"], &[], Some(&["foo", "bar"]));
        f(&["foo", "bar"], &["foobar*"], Some(&["foo", "bar"]));
        f(&["foo", "bar"], &["fo*"], Some(&["bar"]));
    }

    #[test]
    fn test_filter_get_allow_filters() {
        let f = |allow: &[&str], deny: &[&str], result_expected: &[&str]| {
            let mut f = Filter::default();

            f.add_allow_filters(allow);
            f.add_deny_filters(deny);

            let result = f.get_allow_filters();
            let result: Vec<&str> = result
                .iter()
                .map(|s| std::str::from_utf8(s).unwrap())
                .collect();
            assert_eq!(
                result, result_expected,
                "unexpected result; got\n{result:?}\nwant\n{result_expected:?}"
            );
        };

        f(&[], &[], &[]);
        f(&["*"], &[], &["*"]);
        f(&["foo", "bar*"], &[], &["bar*", "foo"]);
        f(&["foo", "*"], &[], &["*"]);
        f(&["foo", "bar*"], &["barz", "f*"], &["bar*"]);
        f(&["*"], &["*"], &[]);
        f(&["*"], &["foo*"], &["*"]);
    }

    #[test]
    fn test_filter_get_deny_filters() {
        let f = |allow: &[&str], deny: &[&str], result_expected: &[&str]| {
            let mut f = Filter::default();

            f.add_allow_filters(allow);
            f.add_deny_filters(deny);

            let result = f.get_deny_filters();
            let result: Vec<&str> = result
                .iter()
                .map(|s| std::str::from_utf8(s).unwrap())
                .collect();
            assert_eq!(
                result, result_expected,
                "unexpected result; got\n{result:?}\nwant\n{result_expected:?}"
            );
        };

        f(&[], &[], &[]);
        f(&["*"], &[], &[]);
        f(&[], &["foo", "bar*"], &[]);
        f(&["*"], &["foo", "bar*"], &["bar*", "foo"]);
        f(&[], &["foo", "*"], &[]);
        f(&["*"], &["foo", "*"], &[]);
        f(&["foo"], &["f*", "barz", "f*"], &[]);
        f(&["foo", "bar*"], &["f*", "barz", "f*"], &["barz", "f*"]);

        // Zero intersection between allow and deny filters
        f(&["foo"], &["bar"], &[]);
        f(&["foo*"], &["bar"], &[]);
        f(&["foo"], &["bar*"], &[]);
        f(&["foo*"], &["bar*"], &[]);
    }

    #[test]
    fn test_filter_match_string_or_wildcard() {
        let f = |allow: &[&str], deny: &[&str], s: &str, result_expected: bool| {
            let mut f = Filter::default();

            f.add_allow_filters(allow);
            f.add_deny_filters(deny);

            let result = f.match_string_or_wildcard(s);
            assert_eq!(
                result, result_expected,
                "unexpected result for {s:?}; got {result}; want {result_expected}"
            );
        };

        // Empty allow
        f(&[], &[], "", false);
        f(&[], &[], "foo", false);
        f(&[], &[], "*", false);
        f(&[], &[], "foo*", false);

        // Allow all
        f(&["*"], &[], "", true);
        f(&["*"], &[], "foo", true);
        f(&["*", "a", "b*"], &[], "", true);
        f(&["*", "a", "b*"], &[], "foo", true);
        f(&["*", "a", "b*"], &[], "*", true);
        f(&["*", "a", "b*"], &[], "foo*", true);

        // Allow all, deny some
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "foo", false);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "bar", false);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "baz", false);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "bam", false);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "bamp", false);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "abc", true);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "*", true);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "f*", true);
        f(&["*"], &["foo", "ba*", "baz*", "bam"], "ba*", false);

        // Deny all
        f(&["*"], &["*"], "", false);
        f(&["*"], &["*"], "foo", false);
        f(&["foo", "ba*"], &["*"], "", false);
        f(&["foo", "ba*"], &["*"], "foo", false);
        f(&["foo", "ba*"], &["*"], "bar", false);
        f(&["foo", "ba*"], &["*"], "abc", false);
        f(&["foo", "ba*"], &["*"], "*", false);
        f(&["foo", "ba*"], &["*"], "b*", false);
        f(&["foo", "ba*"], &["*"], "ba*", false);
        f(&["foo", "ba*"], &["*"], "bar*", false);
        f(&["foo", "ba*"], &["*"], "f*", false);
        f(&["foo", "ba*"], &["*"], "foo*", false);

        // Allow some
        f(&["foo", "ba*"], &[], "", false);
        f(&["foo", "ba*"], &[], "foo", true);
        f(&["foo", "ba*"], &[], "foobar", false);
        f(&["foo", "ba*"], &[], "ba", true);
        f(&["foo", "ba*"], &[], "bar", true);
        f(&["foo", "ba*"], &[], "abc", false);
        f(&["foo", "ba*"], &[], "*", true);
        f(&["foo", "ba*"], &[], "f*", true);
        f(&["foo", "ba*"], &[], "foo*", true);
        f(&["foo", "ba*"], &[], "z*", false);
        f(&["foo", "ba*"], &[], "b*", true);
        f(&["foo", "ba*"], &[], "ba*", true);
        f(&["foo", "ba*"], &[], "bar*", true);

        // Mix allow / deny
        f(&["foo", "ba*"], &["bar"], "abc", false);
        f(&["foo", "ba*"], &["bar"], "foo", true);
        f(&["foo", "ba*"], &["bar"], "bar", false);
        f(&["foo", "ba*"], &["bar"], "baz", true);
        f(&["foo", "ba*"], &["bar"], "barz", true);
        f(&["foo", "ba*"], &["bar"], "*", true);
        f(&["foo", "ba*"], &["bar"], "f*", true);
        f(&["foo", "ba*"], &["bar"], "foo*", true);
        f(&["foo", "ba*"], &["bar"], "b*", true);
        f(&["foo", "ba*"], &["bar"], "ba*", true);
        f(&["foo", "ba*"], &["bar"], "bar*", true);
        f(&["foo", "ba*"], &["bar*"], "ba*", true);
        f(&["foo", "ba*"], &["bar*"], "bar*", false);
        f(&["foo", "ba*"], &["bar*"], "barz*", false);

        // Deny overrides everything
        f(&["foo", "ba*"], &["b*", "f*"], "abc", false);
        f(&["foo", "ba*"], &["b*", "f*"], "foo", false);
        f(&["foo", "ba*"], &["b*", "f*"], "ba", false);
        f(&["foo", "ba*"], &["b*", "f*"], "bar", false);
        f(&["foo", "ba*"], &["b*", "f*"], "*", false);
        f(&["foo", "ba*"], &["b*", "f*"], "f*", false);
        f(&["foo", "ba*"], &["b*", "f*"], "foo*", false);
        f(&["foo", "ba*"], &["b*", "f*"], "b*", false);
        f(&["foo", "ba*"], &["b*", "f*"], "ba*", false);
        f(&["foo", "ba*"], &["b*", "f*"], "bar*", false);

        // Deny overrides some
        f(&["foo", "ba*"], &["bar*", "baz*"], "abc", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "foo", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "bar", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "baz", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "barz", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "bam", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "*", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "b*", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "ba*", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "bar*", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "barz*", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "baz*", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "fo*", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "foo*", true);
        f(&["foo", "ba*"], &["bar*", "baz*"], "foobar*", false);
        f(&["foo", "ba*"], &["bar*", "baz*"], "zoo*", false);

        // Deny equals allow
        f(&["foo", "bar"], &["foo", "bar"], "foo", false);
        f(&["foo", "bar"], &["foo", "bar"], "bar", false);
        f(&["foo", "bar"], &["foo", "bar"], "abc", false);
        f(&["foo", "bar"], &["foo", "bar"], "", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "foo", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "bar", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "abc", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "*", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "foo*", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "ba*", false);
        f(&["foo", "bar*"], &["foo", "bar*"], "bar*", false);
    }

    #[test]
    fn test_filter_drop_broader_deny_filters() {
        let f = |deny: &[&str], allow: &[&str], deny_expected: &[&str], allow_expected: &[&str]| {
            let mut f = Filter::default();
            f.add_allow_filter("*");
            f.add_deny_filters(deny);
            f.add_allow_filters(allow);

            let deny_result = f.get_deny_filters();
            let deny_result: Vec<&str> = deny_result
                .iter()
                .map(|s| std::str::from_utf8(s).unwrap())
                .collect();
            let allow_result = f.get_allow_filters();
            let allow_result: Vec<&str> = allow_result
                .iter()
                .map(|s| std::str::from_utf8(s).unwrap())
                .collect();

            assert_eq!(
                deny_result, deny_expected,
                "unexpected deny filters\ngot\n{deny_result:?}\nwant\n{deny_expected:?}"
            );
            assert_eq!(
                allow_result, allow_expected,
                "unexpected allow filters\ngot\n{allow_result:?}\nwant\n{allow_expected:?}"
            );
        };

        f(&["*"], &["foo"], &[], &["foo"]);
        f(&["*"], &["foo*"], &[], &["foo*"]);
        f(&["*"], &["ab", "foo*"], &[], &["ab", "foo*"]);
        f(&["a*", "b"], &["foo"], &["a*", "b"], &["*"]);
        f(&["a*", "b"], &["foo*", "abc"], &["b"], &["*"]);
        f(&["a*", "b"], &["*"], &[], &["*"]);
        f(&["a*", "b"], &["b*"], &["a*"], &["*"]);
        f(&["a*", "b"], &["b*", "a"], &[], &["*"]);
        f(&["a*", "b"], &["bc*", "ab"], &["b"], &["*"]);
        f(&["a*", "b"], &["b*", "abc"], &[], &["*"]);
    }
}
