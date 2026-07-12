//! Port of `lib/regexutil` tests (`regexutil_test.go`, `promregex_test.go`,
//! `regex_test.go`). Timing tests are skipped.

use super::*;

#[test]
fn test_get_or_values_regex() {
    fn f(s: &str, values_expected: &[&str]) {
        let values = get_or_values_regex(s);
        assert_eq!(
            values, values_expected,
            "unexpected values for s={s:?}; got {values:?}; want {values_expected:?}"
        );
    }

    f("", &[""]);
    f("foo", &["foo"]);
    f("^foo$", &[]);
    f("|foo", &["", "foo"]);
    f("|foo|", &["", "", "foo"]);
    f("foo.+", &[]);
    f("foo.*", &[]);
    f(".*", &[]);
    f("foo|.*", &[]);
    f("(fo((o)))|(bar)", &["bar", "foo"]);
    f("foobar", &["foobar"]);
    f("z|x|c", &["c", "x", "z"]);
    f("foo|bar", &["bar", "foo"]);
    f("(foo|bar)", &["bar", "foo"]);
    f("(foo|bar)baz", &["barbaz", "foobaz"]);
    f("[a-z][a-z]", &[]);
    f("[a-d]", &["a", "b", "c", "d"]);
    f("x[a-d]we", &["xawe", "xbwe", "xcwe", "xdwe"]);
    f("foo(bar|baz)", &["foobar", "foobaz"]);
    f("foo(ba[rz]|(xx|o))", &["foobar", "foobaz", "fooo", "fooxx"]);
    f(
        "foo(?:bar|baz)x(qwe|rt)",
        &["foobarxqwe", "foobarxrt", "foobazxqwe", "foobazxrt"],
    );
    f("foo(bar||baz)", &["foo", "foobar", "foobaz"]);
    f("(a|b|c)(d|e|f|0|1|2)(g|h|k|x|y|z)", &[]);
    f("(?i)foo", &[]);
    f("(?i)(foo|bar)", &[]);
    f("^foo|bar$", &[]);
    f("^(foo|bar)$", &[]);
    f("^a(foo|b(?:a|r))$", &[]);
    f("^a(foo$|b(?:a$|r))$", &[]);
    f("^a(^foo|bar$)z$", &[]);
}

#[test]
fn test_get_or_values_prom_regex() {
    fn f(s: &str, values_expected: &[&str]) {
        let values = get_or_values_prom_regex(s);
        assert_eq!(
            values, values_expected,
            "unexpected values for s={s:?}; got {values:?}; want {values_expected:?}"
        );
    }

    f("", &[""]);
    f("foo", &["foo"]);
    f("^foo$", &["foo"]);
    f("|foo", &["", "foo"]);
    f("|foo|", &["", "", "foo"]);
    f("foo.+", &[]);
    f("foo.*", &[]);
    f(".*", &[]);
    f("foo|.*", &[]);
    f("(fo((o)))|(bar)", &["bar", "foo"]);
    f("foobar", &["foobar"]);
    f("z|x|c", &["c", "x", "z"]);
    f("foo|bar", &["bar", "foo"]);
    f("(foo|bar)", &["bar", "foo"]);
    f("(foo|bar)baz", &["barbaz", "foobaz"]);
    f("[a-z][a-z]", &[]);
    f("[a-d]", &["a", "b", "c", "d"]);
    f("x[a-d]we", &["xawe", "xbwe", "xcwe", "xdwe"]);
    f("foo(bar|baz)", &["foobar", "foobaz"]);
    f("foo(ba[rz]|(xx|o))", &["foobar", "foobaz", "fooo", "fooxx"]);
    f(
        "foo(?:bar|baz)x(qwe|rt)",
        &["foobarxqwe", "foobarxrt", "foobazxqwe", "foobazxrt"],
    );
    f("foo(bar||baz)", &["foo", "foobar", "foobaz"]);
    f("(a|b|c)(d|e|f|0|1|2)(g|h|k|x|y|z)", &[]);
    f("(?i)foo", &[]);
    f("(?i)(foo|bar)", &[]);
    f("^foo|bar$", &["bar", "foo"]);
    f("^(foo|bar)$", &["bar", "foo"]);
    f("^a(foo|b(?:a|r))$", &["aba", "abr", "afoo"]);
    f("^a(foo$|b(?:a$|r))$", &["aba", "abr", "afoo"]);
    f("^a(^foo|bar$)z$", &[]);
}

#[test]
fn test_simplify_regex() {
    fn f(s: &str, expected_prefix: &str, expected_suffix: &str) {
        let (prefix, suffix) = simplify_regex(s);
        assert_eq!(
            prefix, expected_prefix,
            "unexpected prefix for s={s:?}; got {prefix:?}; want {expected_prefix:?}"
        );
        assert_eq!(
            suffix, expected_suffix,
            "unexpected suffix for s={s:?}; got {suffix:?}; want {expected_suffix:?}"
        );
    }

    f("", "", "");
    f(".*", "", "");
    f(".*(.*).*", "", "");
    f("foo.*", "foo", "");
    f(".*foo.*", "", "foo");
    f("^", "", "\\A");
    f("$", "", "(?-m:$)");
    f("^()$", "", "(?-m:\\A$)");
    f("^(?:)$", "", "(?-m:\\A$)");
    f("^foo|^bar$|baz", "", "(?-m:\\Afoo|\\Abar$|baz)");
    f("^(foo$|^bar)$", "", "(?-m:\\A(?:foo$|\\Abar)$)");
    f("^a(foo$|bar)$", "", "(?-m:\\Aa(?:foo$|bar)$)");
    f("^a(^foo|bar$)z$", "", "(?-m:\\Aa(?:\\Afoo|bar$)z$)");
    f("foobar", "foobar", "");
    f("foo$|^foobar", "", "(?-m:foo$|\\Afoobar)");
    f("^(foo$|^foobar)$", "", "(?-m:\\A(?:foo$|\\Afoobar)$)");
    f("foobar|foobaz", "fooba", "[rz]");
    f("(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x");
    f("(тестЧЧ|тест)", "тест", "ЧЧ|");
    f("foo(bar|baz|bana)", "fooba", "[rz]|na");
    f("^foobar|foobaz", "", "\\Afoobar|foobaz");
    f("^foobar|^foobaz$", "", "(?-m:\\Afoobar|\\Afoobaz$)");
    f("foobar|foobaz", "fooba", "[rz]");
    f("(?:^foobar|^foobaz)aa.*", "", "(?:\\Afoobar|\\Afoobaz)aa");
    f("foo[bar]+", "foo", "[abr]+");
    f("foo[a-z]+", "foo", "[a-z]+");
    f("foo[bar]*", "foo", "[abr]*");
    f("foo[a-z]*", "foo", "[a-z]*");
    f("foo[x]+", "foo", "x+");
    f("foo[^x]+", "foo", "[^x]+");
    f("foo[x]*", "foo", "x*");
    f("foo[^x]*", "foo", "[^x]*");
    f("foo[x]*bar", "foo", "x*bar");
    f("fo\\Bo[x]*bar?", "fo", "\\Box*bar?");
    f("foo.+bar", "foo", "(?s:.+bar)");
    f("a(b|c.*).+", "a", "(?s:(?:b|c.*).+)");
    f("ab|ac", "a", "[bc]");
    f("(?i)xyz", "", "(?i:XYZ)");
    f("(?i)foo|bar", "", "(?i:FOO|BAR)");
    f("(?i)up.+x", "", "(?is:UP.+X)");
    f("(?smi)xy.*z$", "", "(?ims:XY.*Z$)");

    // test invalid regexps
    f("a(", "a(", "");
    f("a[", "a[", "");
    f("a[]", "a[]", "");
    f("a{", "a{", "");
    f("a{}", "a{}", "");
    f("invalid(regexp", "invalid(regexp", "");

    // The transformed regexp mustn't match aba
    f("a?(^ba|c)", "", "a?(?:\\Aba|c)");

    // The transformed regexp mustn't match barx
    f("(foo|bar$)x*", "", "(?-m:(?:foo|bar$)x*)");

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
    f(".+;|;.+", "", "(?s:.+;|;.+)");
    f("^(.+);|;(.+)$", "", "(?s-m:\\A.+;|;.+$)");
    f("^(.+);$|^;(.+)$", "", "(?s-m:\\A.+;$|\\A;.+$)");
    f(".*;|;.*", "", "(?s:.*;|;.*)");
    f("^(.*);|;(.*)$", "", "(?s-m:\\A.*;|;.*$)");
    f("^(.*);$|^;(.*)$", "", "(?s-m:\\A.*;$|\\A;.*$)");
}

#[test]
fn test_simplify_prom_regex() {
    fn f(s: &str, expected_prefix: &str, expected_suffix: &str) {
        let (prefix, suffix) = simplify_prom_regex(s);
        assert_eq!(
            prefix, expected_prefix,
            "unexpected prefix for s={s:?}; got {prefix:?}; want {expected_prefix:?}"
        );
        assert_eq!(
            suffix, expected_suffix,
            "unexpected suffix for s={s:?}; got {suffix:?}; want {expected_suffix:?}"
        );
    }

    f("", "", "");
    f("^", "", "");
    f("$", "", "");
    f("^()$", "", "");
    f("^(?:)$", "", "");
    f("^foo|^bar$|baz", "", "foo|ba[rz]");
    f("^(foo$|^bar)$", "", "foo|bar");
    f("^a(foo$|bar)$", "a", "foo|bar");
    f("^a(^foo|bar$)z$", "a", "(?-m:(?:\\Afoo|bar$)z)");
    f("foobar", "foobar", "");
    f("foo$|^foobar", "foo", "|bar");
    f("^(foo$|^foobar)$", "foo", "|bar");
    f("foobar|foobaz", "fooba", "[rz]");
    f("(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x");
    f("(тестЧЧ|тест)", "тест", "ЧЧ|");
    f("foo(bar|baz|bana)", "fooba", "[rz]|na");
    f("^foobar|foobaz", "fooba", "[rz]");
    f("^foobar|^foobaz$", "fooba", "[rz]");
    f("foobar|foobaz", "fooba", "[rz]");
    f("(?:^foobar|^foobaz)aa.*", "fooba", "(?s:[rz]aa.*)");
    f("foo[bar]+", "foo", "[abr]+");
    f("foo[a-z]+", "foo", "[a-z]+");
    f("foo[bar]*", "foo", "[abr]*");
    f("foo[a-z]*", "foo", "[a-z]*");
    f("foo[x]+", "foo", "x+");
    f("foo[^x]+", "foo", "[^x]+");
    f("foo[x]*", "foo", "x*");
    f("foo[^x]*", "foo", "[^x]*");
    f("foo[x]*bar", "foo", "x*bar");
    f("fo\\Bo[x]*bar?", "fo", "\\Box*bar?");
    f("foo.+bar", "foo", "(?s:.+bar)");
    f("a(b|c.*).+", "a", "(?s:(?:b|c.*).+)");
    f("ab|ac", "a", "[bc]");
    f("(?i)xyz", "", "(?i:XYZ)");
    f("(?i)foo|bar", "", "(?i:FOO|BAR)");
    f("(?i)up.+x", "", "(?is:UP.+X)");
    f("(?smi)xy.*z$", "", "(?ims:XY.*Z$)");

    // test invalid regexps
    f("a(", "a(", "");
    f("a[", "a[", "");
    f("a[]", "a[]", "");
    f("a{", "a{", "");
    f("a{}", "a{}", "");
    f("invalid(regexp", "invalid(regexp", "");

    // The transformed regexp mustn't match aba
    f("a?(^ba|c)", "", "a?(?:\\Aba|c)");

    // The transformed regexp mustn't match barx
    f("(foo|bar$)x*", "", "(?-m:(?:foo|bar$)x*)");

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
    f(".+;|;.+", "", "(?s:.+;|;.+)");
    f("^(.+);|;(.+)$", "", "(?s:.+;|;.+)");
    f("^(.+);$|^;(.+)$", "", "(?s:.+;|;.+)");
    f(".*;|;.*", "", "(?s:.*;|;.*)");
    f("^(.*);|;(.*)$", "", "(?s:.*;|;.*)");
    f("^(.*);$|^;(.*)$", "", "(?s:.*;|;.*)");
}

#[test]
fn test_remove_start_end_anchors() {
    fn f(s: &str, result_expected: &str) {
        let result = remove_start_end_anchors(s);
        assert_eq!(
            result, result_expected,
            "unexpected result for RemoveStartEndAnchors({s:?}); got {result:?}; want {result_expected:?}"
        );
    }
    f("", "");
    f("a", "a");
    f("^^abc", "abc");
    f("a^b$c", "a^b$c");
    f("$$abc^", "$$abc^");
    f("^abc|de$", "abc|de");
    f("abc\\$", "abc\\$");
    f("^abc\\$$$", "abc\\$");
    f("^a\\$b\\$$", "a\\$b\\$");
}

#[test]
fn test_prom_regex_parse_failure() {
    fn f(expr: &str) {
        let pr = PromRegex::new(expr);
        assert!(pr.is_err(), "expecting non-nil error for expr={expr}");
    }
    f("fo[bar");
    f("foo(bar");
}

#[test]
fn test_prom_regex() {
    fn f(expr: &str, s: &str, result_expected: bool) {
        let pr = PromRegex::new(expr).expect("unexpected error");
        let expr_result = pr.to_string();
        assert_eq!(
            expr_result, expr,
            "unexpected string representation for {expr:?}: {expr_result:?}"
        );
        let result = pr.match_string(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {expr:?} against {s:?}; got {result}; want {result_expected}"
        );

        // Make sure the result is the same for regular regexp
        let expr_anchored = format!("^(?:{expr})$");
        let re = regex::Regex::new(&expr_anchored).unwrap();
        let result = re.is_match(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {expr_anchored:?} against {s:?} during sanity check; got {result}; want {result_expected}"
        );
    }

    f("", "", true);
    f("", "foo", false);
    f("foo", "", false);
    f(".*", "", true);
    f(".*", "foo", true);
    f(".+", "", false);
    f(".+", "foo", true);
    f("foo.*", "bar", false);
    f("foo.*", "foo", true);
    f("foo.*", "foobar", true);
    f("foo.+", "bar", false);
    f("foo.+", "foo", false);
    f("foo.+", "foobar", true);
    f("foo|bar", "", false);
    f("foo|bar", "a", false);
    f("foo|bar", "foo", true);
    f("foo|bar", "bar", true);
    f("foo|bar", "foobar", false);
    f("foo(bar|baz)", "a", false);
    f("foo(bar|baz)", "foobar", true);
    f("foo(bar|baz)", "foobaz", true);
    f("foo(bar|baz)", "foobaza", false);
    f("foo(bar|baz)", "foobal", false);
    f("^foo|b(ar)$", "foo", true);
    f("^foo|b(ar)$", "bar", true);
    f("^foo|b(ar)$", "ar", false);
    f(".*foo.*", "foo", true);
    f(".*foo.*", "afoobar", true);
    f(".*foo.*", "abc", false);
    f("foo.*bar.*", "foobar", true);
    f("foo.*bar.*", "foo_bar_", true);
    f("foo.*bar.*", "foobaz", false);
    f(".+foo.+", "foo", false);
    f(".+foo.+", "afoobar", true);
    f(".+foo.+", "afoo", false);
    f(".+foo.+", "abc", false);
    f("foo.+bar.+", "foobar", false);
    f("foo.+bar.+", "foo_bar_", true);
    f("foo.+bar.+", "foobaz", false);
    f(".+foo.*", "foo", false);
    f(".+foo.*", "afoo", true);
    f(".+foo.*", "afoobar", true);
    f(".*(a|b).*", "a", true);
    f(".*(a|b).*", "ax", true);
    f(".*(a|b).*", "xa", true);
    f(".*(a|b).*", "xay", true);
    f(".*(a|b).*", "xzy", false);
    f("^(?:true)$", "true", true);
    f("^(?:true)$", "false", false);

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5297
    f(".+;|;.+", ";", false);
    f(".+;|;.+", "foo", false);
    f(".+;|;.+", "foo;bar", false);
    f(".+;|;.+", "foo;", true);
    f(".+;|;.+", ";foo", true);
    f(".+foo|bar|baz.+", "foo", false);
    f(".+foo|bar|baz.+", "afoo", true);
    f(".+foo|bar|baz.+", "fooa", false);
    f(".+foo|bar|baz.+", "afooa", false);
    f(".+foo|bar|baz.+", "bar", true);
    f(".+foo|bar|baz.+", "abar", false);
    f(".+foo|bar|baz.+", "abara", false);
    f(".+foo|bar|baz.+", "bara", false);
    f(".+foo|bar|baz.+", "baz", false);
    f(".+foo|bar|baz.+", "baza", true);
    f(".+foo|bar|baz.+", "abaz", false);
    f(".+foo|bar|baz.+", "abaza", false);
    f(".+foo|bar|baz.+", "afoo|bar|baza", false);
    f(".+(foo|bar|baz).+", "abara", true);
    f(".+(foo|bar|baz).+", "afooa", true);
    f(".+(foo|bar|baz).+", "abaza", true);

    f(".*;|;.*", ";", true);
    f(".*;|;.*", "foo", false);
    f(".*;|;.*", "foo;bar", false);
    f(".*;|;.*", "foo;", true);
    f(".*;|;.*", ";foo", true);

    f(".*foo(bar|baz)", "fooxfoobaz", true);
    f(".*foo(bar|baz)", "fooxfooban", false);
    f(".*foo(bar|baz)", "fooxfooban foobar", true);
}

#[test]
fn test_new_regex_failure() {
    fn f(expr: &str) {
        let r = Regex::new(expr);
        assert!(r.is_err(), "expecting non-nil error when parsing {expr:?}");
    }

    f("[foo");
    f("(foo");
    // Trigger syntax.ErrInvalidRepeatOp
    f("a{0,10000}");
}

#[test]
fn test_regex_match_string() {
    fn f(expr: &str, s: &str, result_expected: bool) {
        let r = Regex::new(expr).unwrap_or_else(|err| panic!("cannot parse {expr:?}: {err}"));
        let expr_result = r.to_string();
        assert_eq!(
            expr_result, expr,
            "unexpected string representation for {expr:?}: {expr_result:?}"
        );
        let result = r.match_string(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {s:?} against regex={expr:?}; got {result}; want {result_expected}"
        );
    }

    f("", "", true);
    f("", "foo", true);
    f("foo", "", false);
    f(".*", "", true);
    f(".*", "foo", true);
    f(".+", "", false);
    f(".+", "foo", true);
    f("foo.*", "bar", false);
    f("foo.*", "foo", true);
    f("foo.*", "a foo", true);
    f("foo.*", "a foo a", true);
    f("foo.*", "foobar", true);
    f("foo.*", "a foobar", true);
    f("foo.+", "bar", false);
    f("foo.+", "foo", false);
    f("foo.+", "a foo", false);
    f("foo.+", "foobar", true);
    f("foo.+", "a foobar", true);
    f("foo|bar", "", false);
    f("foo|bar", "a", false);
    f("foo|bar", "foo", true);
    f("foo|bar", "a foo", true);
    f("foo|bar", "foo a", true);
    f("foo|bar", "a foo a", true);
    f("foo|bar", "bar", true);
    f("foo|bar", "foobar", true);
    f("foo(bar|baz)", "a", false);
    f("foo(bar|baz)", "foobar", true);
    f("foo(bar|baz)", "foobaz", true);
    f("foo(bar|baz)", "foobaza", true);
    f("foo(bar|baz)", "a foobaz a", true);
    f("foo(bar|baz)", "foobal", false);
    f("^foo|b(ar)$", "foo", true);
    f("^foo|b(ar)$", "foo a", true);
    f("^foo|b(ar)$", "a foo", false);
    f("^foo|b(ar)$", "bar", true);
    f("^foo|b(ar)$", "a bar", true);
    f("^foo|b(ar)$", "barz", false);
    f("^foo|b(ar)$", "ar", false);
    f(".*foo.*", "foo", true);
    f(".*foo.*", "afoobar", true);
    f(".*foo.*", "abc", false);
    f("foo.*bar.*", "foobar", true);
    f("foo.*bar.*", "foo_bar_", true);
    f("foo.*bar.*", "a foo bar baz", true);
    f("foo.*bar.*", "foobaz", false);
    f("foo.*bar.*", "baz foo", false);
    f(".+foo.+", "foo", false);
    f(".+foo.+", "afoobar", true);
    f(".+foo.+", "afoo", false);
    f(".+foo.+", "abc", false);
    f("foo.+bar.+", "foobar", false);
    f("foo.+bar.+", "foo_bar_", true);
    f("foo.+bar.+", "a foo_bar_", true);
    f("foo.+bar.+", "foobaz", false);
    f("foo.+bar.+", "abc", false);
    f(".+foo.*", "foo", false);
    f(".+foo.*", "afoo", true);
    f(".+foo.*", "afoobar", true);
    f(".*(a|b).*", "a", true);
    f(".*(a|b).*", "ax", true);
    f(".*(a|b).*", "xa", true);
    f(".*(a|b).*", "xay", true);
    f(".*(a|b).*", "xzy", false);
    f("^(?:true)$", "true", true);
    f("^(?:true)$", "false", false);

    f(".+;|;.+", ";", false);
    f(".+;|;.+", "foo", false);
    f(".+;|;.+", "foo;bar", true);
    f(".+;|;.+", "foo;", true);
    f(".+;|;.+", ";foo", true);
    f(".+foo|bar|baz.+", "foo", false);
    f(".+foo|bar|baz.+", "afoo", true);
    f(".+foo|bar|baz.+", "fooa", false);
    f(".+foo|bar|baz.+", "afooa", true);
    f(".+foo|bar|baz.+", "bar", true);
    f(".+foo|bar|baz.+", "abar", true);
    f(".+foo|bar|baz.+", "abara", true);
    f(".+foo|bar|baz.+", "bara", true);
    f(".+foo|bar|baz.+", "baz", false);
    f(".+foo|bar|baz.+", "baza", true);
    f(".+foo|bar|baz.+", "abaz", false);
    f(".+foo|bar|baz.+", "abaza", true);
    f(".+foo|bar|baz.+", "afoo|bar|baza", true);
    f(".+(foo|bar|baz).+", "bar", false);
    f(".+(foo|bar|baz).+", "bara", false);
    f(".+(foo|bar|baz).+", "abar", false);
    f(".+(foo|bar|baz).+", "abara", true);
    f(".+(foo|bar|baz).+", "afooa", true);
    f(".+(foo|bar|baz).+", "abaza", true);

    f(".*;|;.*", ";", true);
    f(".*;|;.*", "foo", false);
    f(".*;|;.*", "foo;bar", true);
    f(".*;|;.*", "foo;", true);
    f(".*;|;.*", ";foo", true);

    f("^bar", "foobarbaz", false);
    f("^foo", "foobarbaz", true);
    f("bar$", "foobarbaz", false);
    f("baz$", "foobarbaz", true);
    f("(bar$|^foo)", "foobarbaz", true);
    f("(bar$^boo)", "foobarbaz", false);
    f("foo(bar|baz)", "a fooxfoobaz a", true);
    f("foo(bar|baz)", "a fooxfooban a", false);
    f("foo(bar|baz)", "a fooxfooban foobar a", true);

    // Trigger syntax.ErrNestingDepth
    // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1112
    f("a{0,1000}", "a", true);
}

#[test]
fn test_get_literals() {
    fn f(expr: &str, literals_expected: &[&str]) {
        let r = Regex::new(expr).unwrap_or_else(|err| panic!("cannot parse {expr:?}: {err}"));
        let literals = r.get_literals();
        assert_eq!(
            literals, literals_expected,
            "unexpected literals; got {literals:?}; want {literals_expected:?}"
        );
    }

    f("", &[]);
    f("foo bar baz", &["foo bar baz"]);
    f("foo.*bar(a|b)baz.+", &["foo", "bar", "baz"]);
    f("(foo[ab](?:bar))", &["foo", "bar"]);
    f("foo|bar", &[]);
    f("(?i)foo", &[]);
    f("foo((?i)bar)baz", &["foo", "baz"]);
    f("((foo|bar)baz xxx(?:yzabc))", &["baz xxxyzabc"]);
    f("((foo|bar)baz xxx(?:yzabc)*)", &["baz xxx"]);
    f("((foo|bar)baz? xxx(?:yzabc)*)", &["ba", " xxx"]);
}
