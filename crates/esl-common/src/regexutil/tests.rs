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

// The tests below are port-only (upstream regexutil tests have no `\p{...}`,
// `\b` or Unicode-folding cases); expected values follow Go's
// regexp/regexutil behavior at v1.51.0.

#[test]
fn test_unicode_class_regex() {
    // Go resolves \p{...} via the unicode package tables; the port keeps the
    // token opaque and lets the regex crate resolve it at compile time.
    fn f(expr: &str, s: &str, result_expected: bool) {
        let r = Regex::new(expr).expect("unexpected error");
        let result = r.match_string(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {expr:?} against {s:?}"
        );
    }

    // Unanchored (contains) semantics, as in Go's Regex.MatchString.
    f(r"\p{Han}", "a日b", true);
    f(r"\p{Han}", "abc", false);
    f(r"\P{Han}", "日", false);
    f(r"\P{Han}", "日a", true);
    f(r"\pL+", "123abc", true);
    f(r"\pL", "123", false);
    f(r"foo\p{Greek}", "fooα", true);
    f(r"foo\p{Greek}", "fooa", false);
    f(r"(?i)\p{Lu}", "abc", true); // folded: matches lowercase too
    f(r"\p{Lu}", "abc", false);
    f(r"\p{Lu}+bar", "ABCbar", true);
    // Whole char classes containing a Unicode group stay opaque.
    f(r"[\p{L}0]+", "a0б", true);
    f(r"[\p{L}0]", "-;", false);
    f(r"[^\p{L}]", "abc", false);
    f(r"[^\p{L}]", "ab1", true);
    // \p{^Han} is canonicalized to \P{Han} (Go treats them identically).
    f(r"\p{^Han}", "日", false);
    f(r"\p{^Han}", "a", true);
    f(r"\P{^Han}", "日", true);
}

#[test]
fn test_unicode_class_prom_regex() {
    fn f(expr: &str, s: &str, result_expected: bool) {
        let pr = PromRegex::new(expr).expect("unexpected error");
        let result = pr.match_string(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {expr:?} against {s:?}"
        );
    }

    // Anchored semantics, as in Go's PromRegex.MatchString.
    f(r"\p{Lu}+", "ABC", true);
    f(r"\p{Lu}+", "ABc", false);
    f(r"метрика\p{Nd}", "метрика5", true);
    f(r"метрика\p{Nd}", "метрикаx", false);
}

#[test]
fn test_unicode_class_failure() {
    // Malformed tokens fail at parse time like Go (invalid char range);
    // names unknown to the regex crate fail at matcher-compile time
    // (PORT NOTE divergence: Go rejects unknown names at parse time).
    for expr in [
        r"\p",
        r"\p{",
        r"\p{}",
        r"\p{^}",
        r"[\p{]",
        r"\p{NoSuchClassName}",
    ] {
        assert!(
            Regex::new(expr).is_err(),
            "expecting error when parsing {expr:?}"
        );
    }
}

#[test]
fn test_regex_ascii_word_boundary() {
    // Go's \b/\B are ASCII-only: 'é' is not an ASCII word char, so Go sees a
    // word boundary between 'é' and 'f' in "caféfoo". The port rewrites \b
    // to (?-u:\b) before compiling (the regex crate's bare \b is
    // Unicode-aware and would disagree).
    fn f(expr: &str, s: &str, result_expected: bool) {
        let r = Regex::new(expr).expect("unexpected error");
        let result = r.match_string(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {expr:?} against {s:?}"
        );
    }

    f(r"\bfoo\b", "foo", true);
    f(r"\bfoo\b", "a foo b", true);
    f(r"\bfoo\b", "xfoo", false);
    f(r"\bfoo\b", "caféfoo", true);
    f(r"\Bfoo", "xfoo", true);
    f(r"\Bfoo", "caféfoo", false);
    f(r"\Bfoo", " foo", false);
    // Escaped backslash before 'b' is a literal backslash + 'b', not \b.
    f(r"a\\b", "a\\b", true);
}

#[test]
fn test_get_or_values_regex_unicode_fold() {
    // Case folding during class parsing walks Go's SimpleFold orbits.
    fn f(s: &str, values_expected: &[&str]) {
        let values = get_or_values_regex(s);
        assert_eq!(values, values_expected, "unexpected values for regex {s:?}");
    }

    // k folds through K, k, KELVIN SIGN (U+212A).
    f("(?i)[k]", &["K", "k", "\u{212A}"]);
    // s folds through S, s, LONG S (U+017F).
    f("(?i)[s]", &["S", "s", "\u{17F}"]);
    // İ (U+0130) folds only to itself.
    f("(?i)[\u{130}]", &["\u{130}"]);
    // ᾀ (U+1F80) folds to ᾈ (U+1F88) via the simple uppercase mapping; the
    // two-element orbit turns the class into a case-insensitive literal,
    // which yields no or-values (same as Go).
    f("(?i)[\u{1F80}]", &[]);
    // Unicode classes are never expanded into or-values (PORT NOTE: Go
    // expands classes with <= 100 runes; \p{Han} is far larger, so Go also
    // returns none here).
    f(r"\p{Han}", &[]);
}

#[test]
fn test_simplify_regex_unicode() {
    fn f(s: &str, expected_prefix: &str, expected_suffix: &str) {
        let (prefix, suffix) = simplify_regex(s);
        assert_eq!(
            (prefix.as_str(), suffix.as_str()),
            (expected_prefix, expected_suffix),
            "unexpected prefix/suffix for regex {s:?}"
        );
    }

    // Opaque Unicode class tokens round-trip through simplification.
    f(r"foo\p{Han}", "foo", r"\p{Han}");
    f(r"foo(\p{Han}|\p{Latin})", "foo", r"\p{Han}|\p{Latin}");
    f(r"\p{Han}+", "", r"\p{Han}+");
    f(r"[\p{L}0]{2,3}", "", r"[\p{L}0][\p{L}0][\p{L}0]?");
    // ᾀ folds with ᾈ (two-element orbit → case-insensitive literal), same
    // as Go's SimplifyRegex output.
    f("(?i)[\u{1F80}]", "", "(?i:\u{1F80})");
}

// ---------------------------------------------------------------------------
// Raw-byte matching (regex::bytes migration).
// ---------------------------------------------------------------------------

/// Behavior probe pinning `regex::bytes` semantics on invalid-UTF-8 haystacks
/// against Go's `regexp` (which decodes each invalid byte as U+FFFD via
/// `utf8.DecodeRune`). Documented residual: rune-oriented constructs (`.`,
/// negated classes, a literal `\x{FFFD}`) do NOT match invalid bytes here,
/// while Go matches them as U+FFFD. Literal and positive-class matching is
/// byte-exact and identical to Go. See the module-level PORT NOTE.
#[test]
fn test_bytes_regex_invalid_utf8_probe() {
    fn m(pat: &str, hay: &[u8]) -> bool {
        regex::bytes::Regex::new(pat).unwrap().is_match(hay)
    }

    // Byte-exact literal matching through surrounding invalid bytes:
    // identical to Go.
    assert!(m("abc", b"\xffabc\xff"));
    assert!(m("^a", b"a\xff"));
    assert!(m("c$", b"\xffc"));
    // Positive ASCII classes skip invalid bytes exactly like Go.
    assert!(m("[a-c]+", b"a\xffb"));
    // A real (well-formed) U+FFFD in the haystack matches `.` like Go.
    assert!(m("a.c", "a\u{FFFD}c".as_bytes()));

    // Residual divergences (Go: all of these match, decoding \xff/\x80 as
    // U+FFFD; regex::bytes Unicode mode: rune-oriented constructs only match
    // well-formed UTF-8):
    assert!(!m("a.c", b"a\xffc")); // Go: true
    assert!(!m("(?s)a.c", b"a\xffc")); // Go: true
    assert!(!m("a[^b]c", b"a\xffc")); // Go: true
    assert!(!m("a\u{FFFD}c", b"a\xffc")); // Go: true
    assert!(!m("a.c", b"a\x80c")); // Go: true
    assert!(!m("(?s)^a.*c$", b"a\xff\xffc")); // Go: true
}

#[test]
fn test_regex_match_bytes() {
    fn f(expr: &str, s: &[u8], result_expected: bool) {
        let r = Regex::new(expr).unwrap_or_else(|err| panic!("cannot parse {expr:?}: {err}"));
        let result = r.match_bytes(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {s:?} against regex={expr:?}; got {result}; want {result_expected}"
        );
    }

    // match_bytes agrees with match_string on valid UTF-8.
    f("foo.*", b"afoo", true);
    f("foo.*", b"abc", false);

    // Raw (invalid-UTF-8) bytes around/inside values; literal matching is
    // byte-exact, like Go.
    f("foo", b"\xfffoo\xfe", true); // prefix-only fast path (contains)
    f("bar|baz", b"\xff baz \xfe", true); // or-values fast path
    f("bar|baz", b"\xff bazz \xfe", true); // contains semantics
    f("bar|qux", b"\xff baz \xfe", false);
    f("foo.+", b"pre foo\xff", true); // prefix + ".+" fast path over a raw byte
    f(".*text.*", b"\xfftext\xff", true); // substr fast path
    f("x=[0-9]+", b"\xff x=123;\xfe", true); // slow path (suffix_re) on raw bytes
    f("x=[0-9]+", b"\xff x=;\xfe", false);
}

#[test]
fn test_prom_regex_match_bytes() {
    fn f(expr: &str, s: &[u8], result_expected: bool) {
        let pr = PromRegex::new(expr).expect("unexpected error");
        let result = pr.match_bytes(s);
        assert_eq!(
            result, result_expected,
            "unexpected result when matching {s:?} against {expr:?}; got {result}; want {result_expected}"
        );
    }

    // Anchored semantics on raw bytes.
    f("foo", b"foo", true);
    f("foo", b"foo\xff", false); // trailing raw byte breaks the exact match
    f("foo.*", b"foo\xff", true); // "prefix.*" fast path admits raw bytes
    f("foo.+", b"foo\xff", true);
    f("foo|bar", b"bar", true);
    f("foo|bar", b"bar\xff", false);
    f(".*text.*", b"\xfftext\xff", true); // ".*substr.*" fast path
    f(".+text.+", b"\xfftext\xff", true);
    f(".+text.+", b"text\xff", false);
}
