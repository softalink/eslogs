//! Ported subset of `lib/logstorage/parser_test.go`.
//!
//! PORT NOTE: `parser_test.go` cases that depend on the still-deferred `!!x`
//! double-negation collapse in `optimize()` are omitted (see
//! `Query::optimize_no_subqueries`). The ported passes (and/or flattening,
//! `*`-filter removal, stream-filter merging, offset/limit pipe merging,
//! `uniq ... | limit N` merging, `filter` pipe merging) are covered by the
//! `test_parse_query_optimize_*` tests below.

use crate::parser::ParseQuery;
use crate::stream_filter::Lexer;

/// Port of Go `TestLexer`.
#[test]
fn test_lexer() {
    fn f(s: &str, tokens_expected: &[&str]) {
        let mut lex = Lexer::new_at(s, 0);
        for &t in tokens_expected {
            assert_eq!(lex.token, t, "unexpected token for input {s:?}");
            lex.next_token();
        }
        assert_eq!(lex.token, "", "unexpected tail token for input {s:?}");
    }

    f("", &[]);
    f("  ", &[]);
    f("foo", &["foo"]);
    f("тест123", &["тест123"]);
    f("foo:bar", &["foo", ":", "bar"]);
    f(r#" re   (  "тест(\":"  )  "#, &["re", "(", "тест(\":", ")"]);
    f(
        " `foo, bar`* AND baz:(abc or 'd\\'\"ЙЦУК `'*)",
        &[
            "foo, bar",
            "*",
            "AND",
            "baz",
            ":",
            "(",
            "abc",
            "or",
            "d'\"ЙЦУК `",
            "*",
            ")",
        ],
    );
    f(
        r#"{foo="bar",a=~"baz", b != 'cd',"d,}a"!~abc} def"#,
        &[
            "{", "foo", "=", "bar", ",", "a", "=~", "baz", ",", "b", "!=", "cd", ",", "d,}a", "!~",
            "abc", "}", "def",
        ],
    );
    f(
        r#"_stream:{foo="bar",a=~"baz", b != 'cd',"d,}a"!~abc}"#,
        &[
            "_stream", ":", "{", "foo", "=", "bar", ",", "a", "=~", "baz", ",", "b", "!=", "cd",
            ",", "d,}a", "!~", "abc", "}",
        ],
    );
    f("foo:~*", &["foo", ":", "~", "*"]);
}

/// Round-trip helper (Go `TestParseQuery_Success` `f`): parse `s`, assert its
/// `String()` equals `expected`, then re-parse `expected` and assert stability.
fn ok(s: &str, expected: &str) {
    let q = ParseQuery(s).unwrap_or_else(|e| panic!("unexpected error for {s:?}: {e}"));
    let result = q.to_string();
    assert_eq!(result, expected, "round-trip mismatch for input {s:?}");
    let q2 =
        ParseQuery(&result).unwrap_or_else(|e| panic!("cannot parse marshaled {result:?}: {e}"));
    assert_eq!(
        q2.to_string(),
        result,
        "marshaled query not stable for {s:?}"
    );
}

/// Port of Go `TestParseQuery_Failure` `f`: `ParseQuery(s)` must error.
fn fail(s: &str) {
    assert!(ParseQuery(s).is_err(), "expecting error for {s:?}");
}

#[test]
fn test_parse_query_basic_filters() {
    ok("foo", "foo");
    ok(r#""":foo"#, "foo");
    ok("foo  :  bar", "foo:bar");
    ok("foo::bar", r#"foo:":bar""#);
    ok("foo :  :bar", r#"foo:":bar""#);
    ok("1 =2", "1 =2");
    ok("1 - 2", "1 !2");
    ok("1 ~2", "1 ~2");
    ok("1* 2", "1* 2");
    ok(r#""" bar"#, r#""" bar"#);
    ok(r#"!''"#, r#"!"""#);
    ok(r#"-''"#, r#"!"""#);
    ok(r#"foo:"""#, r#"foo:"""#);
    ok(r#"-foo:"""#, r#"!foo:"""#);
    ok(r#"!foo:"""#, r#"!foo:"""#);
    ok(r#"not foo:"""#, r#"!foo:"""#);
    ok("not(foo)", "!foo");
    ok("not (foo)", "!foo");
    ok("not ( foo or bar )", "!(foo or bar)");
    ok("!(foo or bar)", "!(foo or bar)");
    ok("-(foo or bar)", "!(foo or bar)");
    ok(r#"foo:!"""#, r#"!foo:"""#);
    ok("_msg:foo", "foo");
    ok("'foo:bar'", r#""foo:bar""#);
    ok("'!foo'", r#""!foo""#);
    ok("'-foo'", r#""-foo""#);
    ok(r#"'{a="b"}'"#, r#""{a=\"b\"}""#);
    ok("foo 'and' and bar", r#"foo "and" bar"#);
    ok("foo bar", "foo bar");
    ok("foo and bar", "foo bar");
    ok("foo AND bar", "foo bar");
    ok("foo or bar", "foo or bar");
    ok("foo OR bar", "foo or bar");
    ok("not foo", "!foo");
    ok("! foo", "!foo");
    ok("- foo", "!foo");
    ok("not !`foo bar`", r#""foo bar""#);
    ok("not -`foo bar`", r#""foo bar""#);
    ok("foo:!bar", "!foo:bar");
    ok("foo:-bar", "!foo:bar");
}

#[test]
fn test_parse_query_boolean_groups() {
    ok("foo or bar and not baz", "foo or bar !baz");
    ok("'foo bar' !baz", r#""foo bar" !baz"#);
    ok(
        "foo and bar and baz or x or y or z and zz",
        "foo bar baz or x or y or z zz",
    );
    ok(
        "foo and bar and (baz or x or y or z) and zz",
        "foo bar (baz or x or y or z) zz",
    );
    ok(
        "(foo or bar or baz) and x and y and (z or zz)",
        "(foo or bar or baz) x y (z or zz)",
    );
    ok(
        "(foo or bar or baz) and x and y and not (z or zz)",
        "(foo or bar or baz) x y !(z or zz)",
    );
    ok("NOT foo AND bar OR baz", "!foo bar or baz");
    ok("NOT (foo AND bar) OR baz", "!(foo bar) or baz");
    ok("foo OR bar AND baz", "foo or bar baz");
    ok("foo bar or baz xyz", "foo bar or baz xyz");
    ok("foo (bar or baz) xyz", "foo (bar or baz) xyz");
    ok("foo or bar baz or xyz", "foo or bar baz or xyz");
    ok("(foo or bar) (baz or xyz)", "(foo or bar) (baz or xyz)");
    ok("(foo OR bar) AND baz", "(foo or bar) baz");
    ok("'stats' foo", r#""stats" foo"#);
    ok("'stats_remote' abc", r#""stats_remote" abc"#);
    ok(
        r#""filter" bar copy fields avg baz"#,
        r#""filter" bar "copy" "fields" "avg" baz"#,
    );
    ok(
        "foo:(bar baz or not :xxx)",
        r#"foo:bar foo:baz or !foo:":xxx""#,
    );
    ok(
        "(foo:bar and (foo:baz or aa:bb) and xx) and y",
        "foo:bar (foo:baz or aa:bb) xx y",
    );
    ok("level:error and _msg:(a or b)", "level:error (a or b)");
    ok(
        "level: ( ((error or warn*) and re(foo))) (not (bar))",
        "(level:error or level:warn*) level:~foo !bar",
    );
    ok("!(foo bar or baz and not aa*)", "!(foo bar or baz !aa*)");
    // nested AND filters
    ok("(foo AND bar) AND (baz AND x:y)", "foo bar baz x:y");
    ok("(foo AND bar) OR (baz AND x:y)", "foo bar or baz x:y");
    // nested OR filters
    ok("(foo OR bar) OR (baz OR x:y)", "foo or bar or baz or x:y");
    ok("(foo OR bar) AND (baz OR x:y)", "(foo or bar) (baz or x:y)");
}

#[test]
fn test_parse_query_func_filters() {
    ok("contains_all()", "contains_all()");
    ok("contains_all(foo)", "contains_all(foo)");
    ok("contains_all(foo, bar)", "contains_all(foo,bar)");
    ok(
        r#"contains_all("foo bar", baz)"#,
        r#"contains_all("foo bar",baz)"#,
    );
    ok(
        "foo:contains_all(foo-bar/baz)",
        r#"foo:contains_all("foo-bar/baz")"#,
    );
    ok(
        "ipv4_range(1.2.3.4, \"5.6.7.8\")",
        "ipv4_range(1.2.3.4, 5.6.7.8)",
    );
    ok("ipv4_range(1.2.3.4)", "ipv4_range(1.2.3.4, 1.2.3.4)");
    ok("ipv4_range(1.2.3.4/20)", "ipv4_range(1.2.0.0, 1.2.15.255)");
    ok("ipv4_range(1.2.3.4,)", "ipv4_range(1.2.3.4, 1.2.3.4)");
    ok(r#"ipv6_range(::1, "::2")"#, "ipv6_range(::1, ::2)");
    ok("len_range(10, 20)", "len_range(10, 20)");
    ok(r#"foo:len_range("10", 20, )"#, "foo:len_range(10, 20)");
    ok("len_RANGe(10, inf)", "len_range(10, inf)");
    ok("len_range(10, 1_000_000)", "len_range(10, 1_000_000)");
    ok("len_range(0x10,0b100101)", "len_range(0x10, 0b100101)");
    ok(
        r#"pattern_match("<N> foo <DATE>, bar")"#,
        r#"pattern_match("<N> foo <DATE>, bar")"#,
    );
    ok(
        r#"pattern_match_full("<N> foo <DATE>, bar")"#,
        r#"pattern_match_full("<N> foo <DATE>, bar")"#,
    );
    ok("range(1.234, 5656.43454)", "range(1.234, 5656.43454)");
    ok(
        "foo:range(-2343.344, 2343.4343)",
        "foo:range(-2343.344, 2343.4343)",
    );
    ok("range[123, 456)", "range[123, 456)");
    ok("range(123, 445]", "range(123, 445]");
    ok("range(1_000, 0o7532)", "range(1_000, 0o7532)");
    ok("range(0x1ff, inf)", "range(0x1ff, inf)");
    ok("range(-INF,+inF)", "range(-INF, +inF)");
    ok("foo:range(5,inf)", "foo:range(5, inf)");
    ok("value_type(foo)", "value_type(foo)");
    ok(r#"x:value_type("dict")"#, "x:value_type(dict)");
    ok("x:value_type(dict:x)", r#"x:value_type("dict:x")"#);
    ok("seq()", "seq()");
    ok("seq(foo)", "seq(foo)");
    ok(r#"seq("foo, bar", baz, abc)"#, r#"seq("foo, bar",baz,abc)"#);
    ok("string_range(foo, bar)", "string_range(foo, bar)");
    ok(
        r#"foo:string_range("foo, bar", baz)"#,
        r#"foo:string_range("foo, bar", baz)"#,
    );
}

#[test]
fn test_parse_query_comparisons_regexp() {
    ok("foo: > 10.5M", "foo:>10.5M");
    ok("foo: >= 10.5M", "foo:>=10.5M");
    ok("foo: < 10.5M", "foo:<10.5M");
    ok("foo: <= 10.5M", "foo:<=10.5M");
    ok("foo:(>10 !<=20)", "foo:>10 !foo:<=20");
    ok(">=10 !<20", ">=10 !<20");
    ok("re('foo|ba(r.+)')", r#"~"foo|ba(r.+)""#);
    ok("re(foo)", "~foo");
    ok("foo:re(foo-bar/baz.)", r#"foo:~"foo-bar/baz.""#);
    ok(r#"foo:~"~foo~ba/ba>z""#, r#"foo:~"~foo~ba/ba>z""#);
    ok(r#"foo:~'.+'"#, "foo:*");
    ok(r#"x:~"a*""#, r#"x:~"a*""#);
    ok(r#"~'a*'"#, r#"~"a*""#);
    ok("foo:>bar", "foo:>bar");
    ok(r#"foo:>"1234""#, "foo:>1234");
    ok(r#">="abc""#, ">=abc");
    ok("foo:<bar", "foo:<bar");
    ok(r#"foo:<"-12.34""#, "foo:<-12.34");
}

#[test]
fn test_parse_query_special_fields() {
    ok(r#""_stream""#, "_stream");
    ok(r#""_time""#, "_time");
    ok(r#""_msg""#, "_msg");
    ok("_stream and _time or _msg", "_stream _time or _msg");
    ok("trace-id.foo.bar:baz", r#""trace-id.foo.bar":baz"#);
    ok("foo-bar+baz*", r#""foo-bar+baz"*"#);
    ok("foo- bar", r#""foo-" bar"#);
    ok("foo -bar", "foo !bar");
}

#[test]
fn test_parse_query_pipes_and_stats() {
    ok("* | fields a, b", "* | fields a, b");
    ok("* | keep a, b", "* | fields a, b");
    ok("* | delete a, b", "* | delete a, b");
    ok("* | rename a as b, c d", "* | rename a as b, c as d");
    ok("* | copy a b", "* | copy a as b");
    ok("* | limit 10", "* | limit 10");
    ok("* | head 5", "* | limit 5");
    ok("* | offset 20", "* | offset 20");
    ok("* | count() x", "* | stats count(*) as x");
    ok("* | stats count() rows", "* | stats count(*) as rows");
    ok(
        "* | stats by (host) count() n",
        "* | stats by (host) count(*) as n",
    );
    ok(
        "* | stats sum(x) s, avg(y) a",
        "* | stats sum(x) as s, avg(y) as a",
    );
    ok("* | uniq by (x)", "* | uniq by (x)");
    ok("* | uniq (x, y)", "* | uniq by (x, y)");
    ok("* | sort by (x)", "* | sort by (x)");
    ok("* | sort by (x desc)", "* | sort by (x desc)");
    ok("* | top 5 by (host)", "* | top 5 by (host)");
    ok("* | first 3 by (_time)", "* | first 3 by (_time)");
    ok("* | last 3 by (_time)", "* | last 3 by (_time)");
    ok("* | format \"foo\" as bar", "* | format foo as bar");
    ok("* | filter foo:bar", "foo:bar");
    ok(
        "foo | stats count() n | sort by (n desc)",
        "foo | stats count(*) as n | sort by (n desc)",
    );
    ok(
        "* | stats quantile(0.9, x) p90",
        "* | stats quantile(0.9, x) as p90",
    );
    ok(
        "* | stats count_uniq(a, b) c",
        "* | stats count_uniq(a, b) as c",
    );
    ok(
        "* | running_stats count() c",
        "* | running_stats count(*) as c",
    );
}

/// Port of Go `TestParsePipeStreamContextSuccess` / `...Failure`
/// (pipe_stream_context_test.go); the success strings are canonical, so they
/// round-trip unchanged.
#[test]
fn test_parse_pipe_stream_context() {
    fn p(pipe_str: &str) {
        let q_str = format!("* | {pipe_str}");
        ok(&q_str, &q_str);
    }

    p("stream_context before 5");
    p("stream_context after 10");
    p("stream_context after 0");
    p("stream_context before 10 after 20");
    p("stream_context after 1 time_window 2h30m");
    p("stream_context before 1 time_window 2h30m");
    p("stream_context before 1 after 3 time_window 2h30m");

    fn pf(pipe_str: &str) {
        fail(&format!("* | {pipe_str}"));
    }

    pf("stream_context");
    pf("stream_context before");
    pf("stream_context after");
    pf("stream_context before after");
    pf("stream_context after before");
    pf("stream_context before -4");
    pf("stream_context after -4");
    pf("stream_context time_window");
    pf("stream_context before 3 time_window");
    pf("stream_context before 3 time_window foobar");
}

/// Port of Go `TestParsePipeMathSuccess` (pipe_math_test.go); the pipe strings
/// are canonical, so they round-trip unchanged. The final case is the `math`
/// round-trip from Go `TestParseQuery_Success` (parser_test.go).
#[test]
fn test_parse_pipe_math_success() {
    fn p(pipe_str: &str) {
        let q_str = format!("* | {pipe_str}");
        ok(&q_str, &q_str);
    }

    p("math b as a");
    p("math -123 as a");
    p("math 12.345KB as a");
    p("math (-2 + 2) as a");
    p("math x as a, z as y");
    p("math (foo / bar + baz * abc % -45ms) as a");
    p("math (foo / (bar + baz) * abc ^ 2) as a");
    p("math (foo / ((bar + baz) * abc) ^ -2) as a");
    p("math (foo + bar / baz - abc) as a");
    p("math min(3, foo, (1 + bar) / baz) as a, max(a, b) as b, (abs(c) + 5) as d");
    p("math round(foo) as x");
    p("math rand() as y");
    p("math round(foo, 0.1) as y");
    p("math (a / b default 10) as z");
    p("math (ln(a) + exp(b)) as x");
    p("math (x / (24 * 3600)) as x");
    p("math (x / (1d / 1s)) as x");
    p("math (x / 1d * 1s) as x");
    p("math (x - y + z) as x");
    p("math (x - (y + z)) as x");
    p("math now() as current_time");
    p("math round((now() - max_time) / 1s) as duration_seconds");

    // TestParseQuery_Success: implicit result name + trailing ';'.
    ok("* | math a+b c;", "* | math (a + b) as c");
}

/// Port of Go `TestParsePipeMathFailure` (pipe_math_test.go) plus the `math`
/// case from Go `TestParseQuery_Failure` (parser_test.go).
#[test]
fn test_parse_pipe_math_failure() {
    fn p(pipe_str: &str) {
        fail(&format!("* | {pipe_str}"));
    }

    p("math");
    p("math * as y");
    p("math (foo*) as y");
    p("math foo as *");
    p("math foo as y*");
    p("math x as");
    p("math abs() as x");
    p("math abs(a, b) as x");
    p("math min() as x");
    p("math min(a) as x");
    p("math max() as x");
    p("math max(a) as x");
    p("math round() as x");
    p("math round(a, b, c) as x");
    p("math rand(123) as x");
    p("math now(123) as x");

    // TestParseQuery_Failure.
    fail("* | math.x + y");
}

#[test]
fn test_parse_query_options() {
    ok("options(concurrency=4) foo", "options(concurrency=4) foo");
    ok(
        "options(ignore_global_time_filter=true) foo",
        "options(ignore_global_time_filter=true) foo",
    );
}

/// Ported subquery round-trip cases from Go `TestParseQuery_Success`
/// (parser_test.go): the `in(<subquery>)` / `contains_any(<subquery>)` /
/// `contains_all(<subquery>)` / `_stream_id:in(<subquery>)` forms plus the
/// `join`/`union` subquery pipes (Go `TestParsePipeJoinSuccess` /
/// `TestParsePipeUnionSuccess`).
#[test]
fn test_parse_query_subqueries() {
    // in(<subquery>)
    ok("in(err|fields x)", "in(err | fields x)");
    ok(
        "ip:in(foo and user:in(admin, moderator)|fields ip)",
        "ip:in(foo user:in(admin,moderator) | fields ip)",
    );
    ok(
        "x:in(_time:5m y:in(*|fields z) | stats by (q) count() rows|fields q)",
        "x:in(_time:5m y:in(* | fields z) | stats by (q) count(*) as rows | fields q)",
    );
    ok(
        "in(bar:in(1,2,3) | uniq (x)) | stats count() rows",
        "in(bar:in(1,2,3) | uniq by (x)) | stats count(*) as rows",
    );
    ok(
        "in((1) | fields z) | stats count() rows",
        "in(1 | fields z) | stats count(*) as rows",
    );
    // in(*) with a star subquery collapses to `*` (Go parseInQuery returns a
    // nil query).
    ok("in(*)", "*");
    ok("foo:in(*)", "*");

    // contains_any(<subquery>) / contains_all(<subquery>) go through the same
    // parseInValues path (Go exercises them in TestQueryAddTimeFilter and the
    // parseFilterContains* unit tests).
    ok("contains_any(x|fields foo)", "contains_any(x | fields foo)");
    ok(
        "a:contains_any(* | fields bar)",
        "a:contains_any(* | fields bar)",
    );
    ok("contains_all(x|fields foo)", "contains_all(x | fields foo)");
    ok(
        "a:contains_all(* | fields bar)",
        "a:contains_all(* | fields bar)",
    );

    // trailing ';' inside subqueries
    ok("a:in(x | keep a;);", "a:in(x | fields a)");
    ok(
        "a:in(x | keep a;) | stats count() if (x;) y;",
        "a:in(x | fields a) | stats count(*) if (x) as y",
    );

    // subqueries inside `if (...)` filters of stats pipes
    ok(
        "* | stats count(x) if (error ip:in(_time:1d | fields ip)) rows",
        "* | stats count(x) if (error ip:in(_time:1d | fields ip)) as rows",
    );
    ok(
        "* | join by (x) (y) | count() if (a:in((a b) c (d e) | keep a)) z",
        "* | join by (x) (y) | stats count(*) if (a:in(a b c d e | fields a)) as z",
    );

    // `options(global_filter=...)` is applied (ANDed before the query filter)
    // and propagated into the subquery, then re-renders inlined rather than as
    // `options(global_filter=...)` (see `test_options_global_filter_applied`).
    ok(
        r#"options(global_filter=(_time:5m {host="abc"})) _time:1h foo:in(_time:3m | keep foo)"#,
        r#"{host="abc"} _time:5m _time:1h foo:in({host="abc"} _time:5m _time:3m | fields foo)"#,
    );
    ok(
        "options (concurrency=2) foo bar:in(a:b | uniq(bar)) | union (abc) | join on (x) (y)",
        "options(concurrency=2) foo bar:in(a:b | uniq by (bar)) | union (abc) | join by (x) (y)",
    );
    ok(
        "options (concurrency=2) foo bar:in(options (concurrency=10, ignore_global_time_filter=true) a:b | uniq(bar)) | union (abc) | join on(x) (y)",
        "options(concurrency=2) foo bar:in(options(concurrency=10, ignore_global_time_filter=true) a:b | uniq by (bar)) | union (abc) | join by (x) (y)",
    );

    // _stream_id:in(...) (Go TestParseFilterStreamID round-trips)
    ok("_stream_id:in()", "_stream_id:in()");
    ok(
        "_stream_id:in(0000007b000001c8302bc96e02e54e5524b3a68ec271e55e)",
        "_stream_id:0000007b000001c8302bc96e02e54e5524b3a68ec271e55e",
    );
    ok(
        r#"_stream_id:in(0000007b000001c8302bc96e02e54e5524b3a68ec271e55e, "0000007b000001c850d9950ea6196b1a4812081265faa1c7")"#,
        "_stream_id:in(0000007b000001c8302bc96e02e54e5524b3a68ec271e55e,0000007b000001c850d9950ea6196b1a4812081265faa1c7)",
    );
    ok(
        "_stream_id:in(_time:5m | fields _stream_id)",
        "_stream_id:in(_time:5m | fields _stream_id)",
    );
    ok("_stream_id:in(*)", "*");
    ok("'_stream_id':in(*)", "*");

    // join/union subquery pipes (Go TestParsePipeJoinSuccess /
    // TestParsePipeUnionSuccess, wrapped in `* | ...`)
    ok("* | join by (foo) (error)", "* | join by (foo) (error)");
    ok(
        "* | join by (foo, bar) (a:b | fields x, y)",
        "* | join by (foo, bar) (a:b | fields x, y)",
    );
    ok(
        "* | join by (foo) (a:b) prefix c",
        "* | join by (foo) (a:b) prefix c",
    );
    ok(
        "* | join by (foo) (bar | join by (x, z) (y))",
        "* | join by (foo) (bar | join by (x, z) (y))",
    );
    ok("* | join by (x) (y) inner", "* | join by (x) (y) inner");
    ok(
        r#"* | join by (x) ({foo="bar"})"#,
        r#"* | join by (x) ({foo="bar"})"#,
    );
    ok("* | union (*)", "* | union (*)");
    ok("* | union (foo)", "* | union (foo)");
    ok(
        "* | union (foo | union (bar | stats count(*) as x))",
        "* | union (foo | union (bar | stats count(*) as x))",
    );

    // failures (Go TestParseQuery_Failure / TestParsePipeJoinFailure /
    // TestParsePipeUnionFailure)
    fail("a:in(b;|keep a)");
    fail("in(foo|bar)");
    fail("in(err | count() x)"); // missing 'fields' or 'uniq' pipe at the end
    fail("* | join by (x) ()");
    fail("* | join by (x) (abc");
    fail("* | union");
    fail("* | union()");
}

#[test]
fn test_parse_query_failures() {
    fail("");
    fail("|");
    fail("foo|");
    fail("foo|bar(");
    fail("foo and");
    fail("foo OR ");
    fail("not");
    fail("NOT");
    fail("not (abc");
    fail("!");
    fail("a or;");
    fail("a and;");
    fail("not;");
    fail("(;");
    fail("a|;");
    fail(";");
    fail("a;b");
    fail(":foo");
    fail("::foo");
    fail("foo=bar");
    fail("==foo");
    fail("foo==bar");
    fail("foo != bar");
    fail("foo !~ bar");
    fail("foo > bar");
    fail("foo>=bar");
    fail("foo < bar");
    fail("* | nonexisting_pipe");
    fail("* | stats");
    fail("* | stats nonexisting_func()");
    fail("* | sort by (");
    fail("* | fields");
}

// ---------------------------------------------------------------------------
// Query stats/hits surface tests (port of parser_test.go GetStatsLabels* /
// AddExtraFilters / AddCountByTimePipe / GetFixedFields /
// IsFixedOutputFieldsOrder).
// ---------------------------------------------------------------------------

const NSECS_PER_MINUTE: i64 = 60 * 1_000_000_000;
const NSECS_PER_HOUR: i64 = 3600 * 1_000_000_000;
const NSECS_PER_DAY: i64 = 24 * 3600 * 1_000_000_000;

/// Port of Go `TestQueryGetStatsLabelsAddGroupingByTime_Success`.
#[test]
fn test_query_get_stats_labels_add_grouping_by_time_success() {
    #[track_caller]
    fn f(q_str: &str, step: i64, offset: i64, fields_expected: &[&str], q_expected: &str) {
        let mut q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        let fields = q
            .get_stats_labels_add_grouping_by_time(step, offset)
            .unwrap_or_else(|e| {
                panic!("unexpected error in get_stats_labels_add_grouping_by_time({q_str}): {e}")
            });
        let fields_expected: Vec<String> = fields_expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            fields, fields_expected,
            "unexpected labelFields for [{q_str}]"
        );

        // Verify the resulting query
        assert_eq!(q.to_string(), q_expected, "unexpected query for [{q_str}]");
    }

    f(
        "* | count()",
        NSECS_PER_HOUR,
        0,
        &["_time"],
        r#"* | stats by (_time:3600000000000) count(*) as "count(*)""#,
    );
    f(
        "* | count() x",
        NSECS_PER_HOUR,
        10 * NSECS_PER_MINUTE,
        &["_time"],
        "* | stats by (_time:3600000000000 offset 600000000000) count(*) as x",
    );
    f(
        "* | count() x",
        NSECS_PER_HOUR,
        -NSECS_PER_DAY,
        &["_time"],
        "* | stats by (_time:3600000000000 offset -86400000000000) count(*) as x",
    );
    f(
        "* | by (level) count() x",
        NSECS_PER_DAY,
        0,
        &["_time", "level"],
        "* | stats by (_time:86400000000000, level) count(*) as x",
    );
    f(
        "* | by (_time:1m) count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | by (_time:1m offset 30s,level) count() x, count_uniq(z) y",
        NSECS_PER_DAY,
        0,
        &["_time", "level"],
        "* | stats by (_time:86400000000000, level) count(*) as x, count_uniq(z) as y",
    );

    // Verify allowed pipes after the stats pipe
    f(
        "* | count() hits | x:y",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | filter x:y",
    );
    f(
        "* | by (path) rate() rps | first 3 by (rps)",
        NSECS_PER_DAY,
        0,
        &["_time", "path"],
        "* | stats by (_time:86400000000000, path) rate() as rps | first 3 by (rps) partition by (_time)",
    );
    f(
        "* | by (path) rate() rps | last 3 by (rps)",
        NSECS_PER_DAY,
        0,
        &["_time", "path"],
        "* | stats by (_time:86400000000000, path) rate() as rps | last 3 by (rps) partition by (_time)",
    );
    f(
        "* | by (path) rate() rps | sort (rps) limit 3",
        NSECS_PER_DAY,
        0,
        &["_time", "path"],
        "* | stats by (_time:86400000000000, path) rate() as rps | sort by (rps) partition by (_time) limit 3",
    );
    f(
        "* | count() hits | running_stats sum(hits) running_hits",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | running_stats sum(hits) as running_hits",
    );
    f(
        "* | count() hits | running_stats sum(hits) running_hits | rm hits",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | running_stats sum(hits) as running_hits | delete hits",
    );
    f(
        "* | count() hits | total_stats sum(hits) running_hits",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | total_stats sum(hits) as running_hits",
    );
    f(
        "* | count() hits | total_stats sum(hits) running_hits | rm hits",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | total_stats sum(hits) as running_hits | delete hits",
    );
    f(
        "* | by (x) count() hits | total_stats by (_time) sum(hits) total",
        NSECS_PER_DAY,
        0,
        &["_time", "x"],
        "* | stats by (_time:86400000000000, x) count(*) as hits | total_stats by (_time) sum(hits) as total",
    );
    f(
        "* | by (x,y) count() hits | total_stats by (x) sum(hits) total",
        NSECS_PER_DAY,
        0,
        &["_time", "x", "y"],
        "* | stats by (_time:86400000000000, x, y) count(*) as hits | total_stats by (x) sum(hits) as total",
    );
    f(
        "* | count() hits | math hits+bar as baz",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | math (hits + bar) as baz",
    );
    f(
        "* | count() hits | fields _time, hits, bar",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | fields _time, hits, bar",
    );
    f(
        "* | count() hits | delete foo, bar",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | delete foo, bar",
    );
    f(
        "* | count() hits | copy hits x, a b",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | copy hits as x, a as b",
    );
    f(
        "* | count() hits | mv hits x, a b",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | stats by (_time:86400000000000) count(*) as hits | rename hits as x, a as b",
    );
    f(
        r#"* | count() hits | format "foo<hits>" as bar"#,
        NSECS_PER_DAY,
        0,
        &["_time", "bar"],
        r#"* | stats by (_time:86400000000000) count(*) as hits | format "foo<hits>" as bar"#,
    );
    f(
        "* | count() hits, row_any(_msg) msg_sample",
        NSECS_PER_DAY,
        0,
        &["_time", "msg_sample"],
        "* | stats by (_time:86400000000000) count(*) as hits, row_any(_msg) as msg_sample",
    );
    f(
        "* | count() hits, row_any(_msg) msg_sample | unpack_json from msg_sample fields (_msg) | rm msg_sample",
        NSECS_PER_DAY,
        0,
        &["_time", "_msg"],
        "* | stats by (_time:86400000000000) count(*) as hits, row_any(_msg) as msg_sample | unpack_json from msg_sample fields (_msg) | delete msg_sample",
    );

    // limit and offset is allowed for instant queries
    f(
        "* | count() hits | limit 10",
        0,
        0,
        &[],
        "* | stats count(*) as hits | limit 10",
    );
    f(
        "* | count() hits | offset 10",
        0,
        0,
        &[],
        "* | stats count(*) as hits | offset 10",
    );

    // multiple stats pipes and sort pipes
    f(
        "* | by (path) count() requests | by (requests) count() hits | first (hits desc)",
        NSECS_PER_DAY,
        0,
        &["_time", "requests"],
        "* | stats by (_time:86400000000000, path) count(*) as requests | stats by (_time:86400000000000, requests) count(*) as hits | first by (hits desc) partition by (_time)",
    );

    // pipes, which do not drop or modify _time, are allowed in front of `stats` pipe
    f(
        "* | coalesce (x) | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | coalesce(x) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | collapse_nums | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | collapse_nums | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | copy foo bar | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | copy foo as bar | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "*|decolorize|count()x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | decolorize | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | delete foo, bar | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | delete foo, bar | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | drop_empty_fields | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | drop_empty_fields | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | extract '<foo>bar<baz>' | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | extract "<foo>bar<baz>" | stats by (_time:86400000000000) count(*) as x"#,
    );
    f(
        "* | extract_regexp 'foo(?P<bar>baz)' | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | extract_regexp "foo(?P<bar>baz)" | stats by (_time:86400000000000) count(*) as x"#,
    );
    f(
        "* | fields _time, x | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | fields _time, x | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | filter x:y | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "x:y | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | format 'x<y>' | count()x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | format "x<y>" | stats by (_time:86400000000000) count(*) as x"#,
    );
    f(
        "* | hash(x) | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | hash(x) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | json_array_len (x) | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | json_array_len(x) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | len(x) | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | len(x) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | math x+y as z | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | math (x + y) as z | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | pack_json | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | pack_json | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | pack_logfmt | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | pack_logfmt | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | rename foo bar | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | rename foo as bar | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | replace ('foo', 'bar') | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | replace (foo, bar) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | replace_regexp ('foo', 'bar') | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | replace_regexp (foo, bar) | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | split 'foo' | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | split foo | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | time_add 1h | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | time_add 1h | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | unpack_json x | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | unpack_json from x | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | unpack_logfmt x | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | unpack_logfmt from x | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | unpack_syslog x | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | unpack_syslog from x | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | unpack_words x | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | unpack_words from x | stats by (_time:86400000000000) count(*) as x",
    );
    f(
        "* | unroll by (x) | count() x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        "* | unroll by (x) | stats by (_time:86400000000000) count(*) as x",
    );

    // Unusual cases, which override the original stats labels

    f(
        "* | count() | running_stats sum(hits) _time",
        NSECS_PER_DAY,
        0,
        &[],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | running_stats sum(hits) as _time"#,
    );
    f(
        "* | by (x) count() | running_stats by (x) sum(hits) x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | running_stats by (x) sum(hits) as x"#,
    );

    f(
        "* | count() | total_stats sum(hits) _time",
        NSECS_PER_DAY,
        0,
        &[],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | total_stats sum(hits) as _time"#,
    );
    f(
        "* | by (x) count() | total_stats by (x) sum(hits) x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | total_stats by (x) sum(hits) as x"#,
    );

    f(
        "* | count() | math a+b _time",
        NSECS_PER_DAY,
        0,
        &[],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | math (a + b) as _time"#,
    );
    f(
        "* | by (x) count() | math a+b x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | math (a + b) as x"#,
    );

    f(
        "* | count() | rm _time",
        NSECS_PER_DAY,
        0,
        &[],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | delete _time"#,
    );
    f(
        "* | by (x) count() | rm x",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | delete x"#,
    );

    f(
        "* | count() | cp a _time",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | copy a as _time"#,
    );
    f(
        "* | by (x) count() | cp a x",
        NSECS_PER_DAY,
        0,
        &["_time", "x"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | copy a as x"#,
    );

    f(
        "* | count() | mv a _time",
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | rename a as _time"#,
    );
    f(
        "* | by (x) count() | mv a x",
        NSECS_PER_DAY,
        0,
        &["_time", "x"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | rename a as x"#,
    );

    f(
        r#"* | count() | format "a" as _time"#,
        NSECS_PER_DAY,
        0,
        &["_time"],
        r#"* | stats by (_time:86400000000000) count(*) as "count(*)" | format a as _time"#,
    );
    f(
        r#"* | by (x) count() | format "a" as x"#,
        NSECS_PER_DAY,
        0,
        &["_time", "x"],
        r#"* | stats by (_time:86400000000000, x) count(*) as "count(*)" | format a as x"#,
    );

    f(
        "* | stats by (host) count() total | rename host as server | fields host, total",
        NSECS_PER_DAY,
        0,
        &[],
        "* | stats by (_time:86400000000000, host) count(*) as total | rename host as server | fields host, total",
    );
}

/// Port of Go `TestQueryGetStatsLabelsAddGroupingByTime_Failure`.
#[test]
fn test_query_get_stats_labels_add_grouping_by_time_failure() {
    #[track_caller]
    fn f(q_str: &str) {
        let mut q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        let res = q.get_stats_labels_add_grouping_by_time(NSECS_PER_HOUR, 0);
        assert!(res.is_err(), "expecting non-nil error for [{q_str}]");
    }

    f("*");

    // verify invalid pipes after the stats pipe
    f("* | count() | running_stats by (x) sum(a) b");
    f("* | by (x) count() | running_stats sum(a) b");
    f("* | count() | total_stats by (x) sum(a) b");
    f("* | by (x) count() | total_stats by (y) sum(a) b");
    f("* | count() | fields a,b");
    f("* | by (x) count() y | unpack_json from y");
    f("* | by (x) count() y | unpack_json from y fields(z*)");

    f("* | by (x) count() | coalesce (x)");
    f("* | by (x) count() | collapse_nums at x");
    f("* | count() x | split ' '");

    // offset and limit pipes are disallowed, since they cannot be applied
    // individually per each step
    f("* | by (x) count() | offset 10");
    f("* | by (x) count() | limit 20");

    // pipes, which drop or modify _time field are disallowed in front of `stats` pipe
    f("* | blocks_count | count()");
    f("* | block_stats | count()");
    f("* | facets | count()");
    f("* | field_names | count()");
    f("* | fields foo, bar | count()");
    f("* | field_values x | count()");
    f("* | first 10 (x) | count()");
    f("* | format 'x<y>' as _time | count()");
    f("* | generate_sequence 10 | count()");
    f("* | hash(x) as _time | count()");
    f("* | join by (x) (foo) | count()");
    f("* | json_array_len (x) as _time | count()");
    f("* | last 10 (x) | count()");
    f("* | len(x) as _time | count()");
    f("* | limit 10 | count()");
    f("* | offset 10 | count()");
    f("* | pack_json as _time | count()");
    f("* | pack_logfmt as _time | count()");
    f("* | query_stats | count()");
    f("* | sample 10 | count()");
    f("* | sort by (x) | count()");
    f("* | stream_context before 10 after 20 | count()");
    f("* | top 5 by (x) | count()");
    f("* | union (x) | count()");
    f("* | uniq (x) | count()");
}

/// Port of Go `TestQueryGetStatsLabels_Success`.
#[test]
fn test_query_get_stats_labels_success() {
    #[track_caller]
    fn f(q_str: &str, fields_expected: &[&str]) {
        let mut q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        let fields = q
            .get_stats_labels()
            .unwrap_or_else(|e| panic!("unexpected error in get_stats_labels({q_str:?}): {e}"));
        let fields_expected: Vec<String> = fields_expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            fields, fields_expected,
            "unexpected labelFields for [{q_str}]"
        );
    }

    f("* | stats count()", &[]);
    f("* | count()", &[]);
    f("* | by (foo) count(), count_uniq(bar)", &["foo"]);
    f(
        "* | stats by (a, b, cd) min(foo), max(bar)",
        &["a", "b", "cd"],
    );

    // multiple pipes before stats is ok
    f(
        r#"foo | extract "ip=<ip>," | stats by (host) count_uniq(ip)"#,
        &["host"],
    );
    f("foo | decolorize | count()", &[]);

    // sort, offset and limit pipes are allowed after stats
    f(
        "foo | stats by (x, y) count() rows | sort by (rows) desc | offset 5 | limit 10",
        &["x", "y"],
    );

    // filter pipe is allowed after stats
    f(
        "foo | stats by (x, y) count() rows | filter rows:>100",
        &["x", "y"],
    );

    // math pipe is allowed after stats
    f(
        "foo | stats by (x) count() total, count() if (error) errors | math errors / total",
        &["x"],
    );
    f(
        "foo | stats by (x, y) count() hits | total_stats by (x) sum(hits) total | math hits / total",
        &["x", "y"],
    );

    // derive math results
    f("foo | stats count() x | math x / 10 as y | rm x", &[]);
    f(
        "foo | stats by (z) count() x | math x / 10 as y | rm x",
        &["z"],
    );

    // keep containing all the by(...) fields
    f(
        "foo | stats by (x) count() total | keep x, y, total",
        &["x"],
    );
    f(
        "foo | stats by (x) count() total | keep x*, y, total",
        &["x"],
    );

    // keep drops some metrics, but leaves others
    f(
        "foo | stats by (x) count() y, count_uniq(a) z | keep x, z, abc",
        &["x"],
    );
    f(
        "foo | stats by (x) count() y, count_uniq(a) z | keep x*, z, abc",
        &["x"],
    );

    // drop which doesn't contain by(...) fields
    f("foo | stats by (x) count() total | drop y", &["x"]);
    f("foo | stats by (x) count() total | drop y*", &["x"]);
    f(
        "foo | stats by (x) count() total, count_uniq(a) z | drop z",
        &["x"],
    );
    f(
        "foo | stats by (x) count() total, count_uniq(a) z | drop z*",
        &["x"],
    );

    // copy which doesn't contain by(...) fields
    f("foo | stats by (x) count() total | copy total abc", &["x"]);
    f(
        "foo | stats by (x) count() total | copy total* abc*",
        &["x"],
    );

    // copy by(...) fields
    f(
        "foo | stats by (x) count() | copy x y, y z",
        &["x", "y", "z"],
    );
    f(
        "foo | stats by (x) count() | copy x* y*, y* z*",
        &["x", "y", "z"],
    );

    // copy metrics
    f("foo | stats by (x) count() y | copy y z | drop y", &["x"]);
    f(
        "foo | stats by (x) count() y | copy y* z* | drop y*",
        &["x"],
    );

    // mv by(...) fields
    f("foo | stats by (x) count() total | mv x y", &["y"]);
    f("foo | stats by (x) count() total | mv x* y*", &["y"]);

    // mv metrics
    f("foo | stats by (x) count() y | mv y z", &["x"]);
    f("foo | stats by (x) count() y | mv y* z*", &["x"]);
    f("foo | stats by (x) count() y | mv y z | rm y", &["x"]);
    f("foo | stats by (x) count() y | mv y* z* | rm y*", &["x"]);

    // format result is treated as by(...) field
    f(r#"foo | count() | format "foo<bar>baz" as x"#, &["x"]);
    f(
        r#"foo | by (x) count() | format "foo<bar>baz" as y"#,
        &["x", "y"],
    );

    // check first and last pipes
    f("foo | stats by (x) count() y | first by (y)", &["x"]);
    f("foo | stats by (x) count() y | last by (y)", &["x"]);

    // unusual cases, which override the original labels

    f("foo | by (a, b) count() | copy a b", &["a", "b"]);
    f("foo | by (a, b) count() | copy a* b*", &["a", "b"]);
    f("foo | by (x) count() | cp a x", &["x"]);
    f("foo | by (x) count() | cp a* x*", &["x"]);

    f("foo | by (x) count() | mv a x", &["x"]);
    f("foo | by (x) count() | mv a* x*", &["x"]);
    f("foo | by (a, x) count() | mv a x", &["x"]);
    f("foo | by (a, x) count() | mv a* x*", &["x"]);

    f("foo | by (a, b) count() | delete a", &["b"]);
    f("foo | by (a, b) count() | delete a*", &["b"]);

    f("foo | by (x) count() y | math y*100 as x", &[]);

    f("* | by (x) count() | format 'foo' as x", &["x"]);
}

/// Port of Go `TestQueryGetStatsLabels_Failure`.
#[test]
fn test_query_get_stats_labels_failure() {
    #[track_caller]
    fn f(q_str: &str) {
        let mut q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        let res = q.get_stats_labels();
        assert!(res.is_err(), "expecting non-nil error for [{q_str}]");
    }

    f("*");
    f("foo bar");
    f("foo | by (a, b) count() | decolorize a");
    f("foo | count() | drop_empty_fields");
    f(r#"foo | count() | extract "foo<bar>baz""#);
    f(r#"foo | count() | extract_regexp "(?P<ip>([0-9]+[.]){3}[0-9]+)""#);
    f("foo | count() | block_stats");
    f("foo | count() | blocks_count");
    f("foo | count() | generate_sequence 123");
    f("foo | count() | coalesce (x, y)");
    f("foo | count() | collapse_nums");
    f("foo | count() | facets");
    f("foo | count() | field_names");
    f("foo | count() | field_values abc");
    f("foo | by (x) count() | fields a, b");
    f("foo | by (x) count() | fields a*, b");
    f("foo | by (x) count() hits | total_stats by (y) sum(hits) total");
    f("foo | count() | pack_json");
    f("foo | count() | pack_logfmt");
    f("foo | count() | query_stats");
    f("foo | rename x y");
    f(r#"foo | count() | replace ("foo", "bar")"#);
    f(r#"foo | count() | replace_regexp ("foo.+bar", "baz")"#);
    f("foo | count() | split ' '");
    f("foo | count() | stream_context after 10");
    f("foo | count() | top 5 by (x)");
    f("foo | count() | union (foo)");
    f("foo | count() | uniq by (x)");
    f("foo | count() | unpack_json");
    f("foo | count() | unpack_logfmt");
    f("foo | count() | unpack_syslog");
    f("foo | count() | unpack_words x");
    f("foo | count() | unroll by (x)");
    f("foo | count() | join by (x) (y)");
    f("foo | count() | json_array_len(a)");
    f("foo | count() | len(a)");
    f("foo | count() | hash(a)");

    // missing metric fields
    f("* | count() x | fields y");
    f("* | count() x | fields y*");
    f("* | by (x) count() y | fields x");
    f("* | by (x) count() y | fields x*");

    // copy to the remaining metric field
    f("* | by (x) count() y | cp a y");
    f("* | by (x) count() y | cp a* y*");

    // mv to the remaining metric fields
    f("* | by (x) count() y | mv x y");
    f("* | by (x) count() y | mv x* y*");

    // format to the remaining metric field
    f("* | by (x) count() y | format 'foo' as y");
}

/// Port of Go `TestQuery_AddTimeFilter` (subquery-propagation subset).
///
/// Uses occurrence counts rather than pinning the rendered timestamp text so
/// the assertions stay robust; the exact `_time:[...]` format is covered by the
/// storage-level tests.
#[test]
fn test_query_add_time_filter_propagates_to_subqueries() {
    let start = 1_700_000_000_000_000_000i64;
    let end = 1_700_000_001_000_000_000i64;

    // union subquery inherits the global time filter.
    let mut q = ParseQuery("foo | union (bar)").unwrap();
    q.add_time_filter(start, end);
    let s = q.to_string();
    assert_eq!(
        s.matches("_time:[").count(),
        2,
        "time filter must reach the union subquery: {s}"
    );

    // in(...) subquery inherits the global time filter.
    let mut q = ParseQuery("foo or bar:in(baz | fields bar)").unwrap();
    q.add_time_filter(start, end);
    let s = q.to_string();
    assert_eq!(
        s.matches("_time:[").count(),
        2,
        "time filter must reach the in() subquery: {s}"
    );

    // A subquery with options(ignore_global_time_filter=true) suppresses the
    // propagated filter for itself but not for the parent.
    let mut q =
        ParseQuery("foo or bar:in(options(ignore_global_time_filter=true) baz | fields bar)")
            .unwrap();
    q.add_time_filter(start, end);
    let s = q.to_string();
    assert_eq!(
        s.matches("_time:[").count(),
        1,
        "subquery opts must suppress the time filter there: {s}"
    );
}

/// `options(time_offset=...)` shifts the `_time` filter's matching bounds
/// (Go `updateFilterWithTimeOffset`).
#[test]
fn test_options_time_offset_shifts_time_filter() {
    use crate::rows::Field;

    // `_time:[12:00, 12:00]` with a 1h offset shifts the matching bounds back by
    // 1h, so a row at 11:00 matches and one at 12:00 does not.
    let q =
        ParseQuery("options(time_offset=1h) _time:[2024-06-01T12:00:00Z, 2024-06-01T12:00:00Z]")
            .unwrap();
    let f = q.get_final_filter();
    let row_11 = [Field {
        name: "_time".to_string(),
        value: "2024-06-01T11:00:00Z".to_string(),
    }];
    let row_12 = [Field {
        name: "_time".to_string(),
        value: "2024-06-01T12:00:00Z".to_string(),
    }];
    assert!(
        f.match_row(&row_11),
        "time_offset should shift the filter to match T-1h"
    );
    assert!(
        !f.match_row(&row_12),
        "time_offset-shifted filter should not match T"
    );
}

/// `stats ... switch(...)` expands to one `if`-guarded func per case; the
/// `default` case's filter is the negation of all case filters (Go
/// `parseStatsSwitch` + `getDefaultFilter`).
#[test]
fn test_parse_stats_switch() {
    #[track_caller]
    fn f(q_str: &str, expected: &str) {
        let q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        assert_eq!(q.to_string(), expected, "unexpected expansion of [{q_str}]");
    }

    // default => NOT(OR(case filters)).
    f(
        "* | stats count() switch(case (x:foo) as a, default as b)",
        "* | stats count(*) if (x:foo) as a, count(*) if (!(x:foo)) as b",
    );
    // multiple cases, no default => no negation func.
    f(
        "* | stats by (h) sum(n) switch(case (x:1) as lo, case (x:2) as hi)",
        "* | stats by (h) sum(n) if (x:1) as lo, sum(n) if (x:2) as hi",
    );
    // `if` is accepted as an alias for `case`.
    f(
        "* | stats count() switch(if (a:1) as x, default as y)",
        "* | stats count(*) if (a:1) as x, count(*) if (!(a:1)) as y",
    );

    // Error cases.
    for (q_str, want_err) in [
        ("* | stats count() switch()", "at least a single"),
        (
            "* | stats count() switch(default as x, default as y)",
            "more than one 'default'",
        ),
        (
            "* | stats count() switch(foo as x)",
            "want 'case' or 'default'",
        ),
    ] {
        let err = match ParseQuery(q_str) {
            Ok(_) => panic!("expected an error for [{q_str}]"),
            Err(e) => e,
        };
        assert!(err.contains(want_err), "for [{q_str}] got error: {err}");
    }
}

/// `options(global_filter=...)` is ANDed before the query filter (Go
/// `getFinalFilter`), so only rows matching both pass; the option is inlined
/// into the rendered filter rather than kept as `options(...)`.
#[test]
fn test_options_global_filter_applied() {
    use crate::rows::Field;

    let q = ParseQuery("options(global_filter=(host:web)) error").unwrap();
    let f = q.get_final_filter();

    let field = |n: &str, v: &str| Field {
        name: n.to_string(),
        value: v.to_string(),
    };
    let matching = [field("host", "web"), field("_msg", "error")];
    let wrong_host = [field("host", "db"), field("_msg", "error")];
    let no_error = [field("host", "web"), field("_msg", "info")];

    assert!(f.match_row(&matching), "host:web + error must match");
    assert!(
        !f.match_row(&wrong_host),
        "global_filter host:web must exclude host:db"
    );
    assert!(
        !f.match_row(&no_error),
        "query filter 'error' must exclude 'info'"
    );
    assert!(
        !q.to_string().contains("global_filter"),
        "global_filter should be inlined, not rendered as options: {q}"
    );
}

/// Port of Go `TestQuery_AddExtraFilters`.
#[test]
fn test_query_add_extra_filters() {
    #[track_caller]
    fn f(q_str: &str, extra_filters: &str, result_expected: &str) {
        let mut q = ParseQuery(q_str).unwrap_or_else(|e| panic!("cannot parse [{q_str}]: {e}"));
        if !extra_filters.is_empty() {
            let efs = crate::parser::ParseFilter(extra_filters)
                .unwrap_or_else(|e| panic!("unexpected error in ParseFilter: {e}"));
            q.add_extra_filters(efs);
        }

        assert_eq!(
            q.to_string(),
            result_expected,
            "unexpected result for [{q_str}]"
        );
    }

    f("*", "", "*");
    f("_time:5m", "", "_time:5m");
    f("foo _time:5m", "", "foo _time:5m");
    f("*", "foo:=bar", "foo:=bar");
    f(
        "_time:5m",
        r#""fo o":="=ba:r !""#,
        r#""fo o":="=ba:r !" _time:5m"#,
    );
    f(
        "_time:5m {a=b}",
        r#""fo o":="=ba:r !" and x:=y"#,
        r#"{a="b"} "fo o":="=ba:r !" x:=y _time:5m"#,
    );
    f("a or (b c)", "foo:=bar", "foo:=bar (a or b c)");

    // extra stream filters
    f("*", r#"{foo="bar",baz!="x"}"#, r#"{foo="bar",baz!="x"}"#);

    // mixed filters
    f(
        "c",
        r#"{foo="bar",baz!="x"} a:~b"#,
        r#"{foo="bar",baz!="x"} a:~b c"#,
    );

    // extra filters must be unconditionally propagated into subqueries
    // (`in(...)`, `union`, and `if(...)` filters) — the tenant/security filter
    // reaches every scanned subquery.
    f(
        "foo x:in(bar | keep x)",
        "tenant:=123",
        "tenant:=123 foo x:in(tenant:=123 bar | fields x)",
    );
    f(
        r#"foo x:in(bar | union (baz) | keep x) | count() if (a:in(b | keep a)) z"#,
        "tenant:=123",
        r#"tenant:=123 foo x:in(tenant:=123 bar | union (tenant:=123 baz) | fields x) | stats count(*) if (a:in(tenant:=123 b | fields a)) as z"#,
    );
}

/// Port of Go `TestQuery_AddCountByTimePipe`.
#[test]
fn test_query_add_count_by_time_pipe() {
    #[track_caller]
    fn f(q_str: &str, step: i64, offset: i64, fields: &[&str], result_expected: &str) {
        let mut q = ParseQuery(q_str)
            .unwrap_or_else(|e| panic!("unexpected error when parsing [{q_str}]: {e}"));
        let fields: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
        q.add_count_by_time_pipe(step, offset, &fields);

        assert_eq!(
            q.to_string(),
            result_expected,
            "unexpected result for [{q_str}]"
        );
    }

    // simple filter
    f(
        "*",
        NSECS_PER_MINUTE,
        0,
        &[],
        "* | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "*",
        NSECS_PER_MINUTE,
        2 * NSECS_PER_HOUR,
        &[],
        "* | stats by (_time:1m offset 2h) count(*) as hits | sort by (_time)",
    );
    f(
        "foo bar:baz",
        NSECS_PER_MINUTE,
        -2 * NSECS_PER_HOUR,
        &[],
        "foo bar:baz | stats by (_time:1m offset -2h) count(*) as hits | sort by (_time)",
    );

    // Avoid name collision for field=hits.
    // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1278
    f(
        "*",
        NSECS_PER_MINUTE,
        0,
        &["hits"],
        "* | stats by (_time:1m, hits) count(*) as hitss | sort by (_time, hits)",
    );

    // pipes, which do not change _time field
    f(
        "* | extract 'abc<de>fg' | filter de:='qwer'",
        NSECS_PER_MINUTE,
        0,
        &[],
        r#"* | extract "abc<de>fg" | filter de:=qwer | stats by (_time:1m) count(*) as hits | sort by (_time)"#,
    );

    // union pipe is allowed. See https://github.com/VictoriaMetrics/VictoriaLogs/issues/641
    f(
        "foo | union (bar)",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | union (bar) | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | union (bar) | stats count()",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | union (bar) | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | union (bar | stats count())",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | union (bar) | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );

    // union rows(...) isn't allowed.
    f(
        "foo | union rows()",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | union rows({})",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | union rows({_time=foo,x=bar})",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );

    // join pipe is allowed
    f(
        "foo | join by (x) (y)",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | join by (x) (y) | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );

    // join rows(...) is allowed
    f(
        "foo | join by (x) rows()",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | join by (x) rows() | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | join by (x) rows({})",
        NSECS_PER_MINUTE,
        0,
        &[],
        "foo | join by (x) rows({}) | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "foo | join by (x) rows({x=y})",
        NSECS_PER_MINUTE,
        0,
        &[],
        r#"foo | join by (x) rows({"x":"y"}) | stats by (_time:1m) count(*) as hits | sort by (_time)"#,
    );

    // pipes, which change _time field
    f(
        "* | extract 'abc<de>fg' | filter de:='qwer' | stats count()",
        NSECS_PER_MINUTE,
        0,
        &[],
        r#"* | extract "abc<de>fg" | filter de:=qwer | stats by (_time:1m) count(*) as hits | sort by (_time)"#,
    );
    f(
        "* | extract 'abc<de>fg' | sort by (x)",
        NSECS_PER_MINUTE,
        0,
        &[],
        r#"* | extract "abc<de>fg" | stats by (_time:1m) count(*) as hits | sort by (_time)"#,
    );
    f(
        "* | extract 'abc<de>fg' | sort by (_time)",
        NSECS_PER_MINUTE,
        0,
        &[],
        r#"* | extract "abc<de>fg" | stats by (_time:1m) count(*) as hits | sort by (_time)"#,
    );
    f(
        "* | count()",
        NSECS_PER_MINUTE,
        0,
        &[],
        "* | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
    f(
        "* | uniq (x)",
        NSECS_PER_MINUTE,
        0,
        &[],
        "* | stats by (_time:1m) count(*) as hits | sort by (_time)",
    );
}

/// Port of Go `TestQueryGetFixedFields_Success`.
#[test]
fn test_query_get_fixed_fields_success() {
    #[track_caller]
    fn f(q_str: &str, result_expected: &[&str]) {
        let q = ParseQuery(q_str).unwrap_or_else(|e| panic!("unexpected error: {e}"));

        let result = q
            .get_fixed_fields()
            .unwrap_or_else(|| panic!("unexpected error in get_fixed_fields() for [{q_str}]"));
        let result_expected: Vec<String> = result_expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(result, result_expected, "unexpected result for [{q_str}]");
    }

    f("* | fields foo", &["foo"]);
    f("* | fields a, b, cd", &["a", "b", "cd"]);
    f(
        "* | fields a, b, cd | sort by (x, a)",
        &["x", "a", "b", "cd"],
    );

    f("* | count(), sum(x) as y", &["count(*)", "y"]);
    f(
        "* | stats by (a, b) count(), sum(x) as y",
        &["a", "b", "count(*)", "y"],
    );
    f(
        "* | stats by (a, b) count(), sum(x) as y | sort by (c desc)",
        &["c", "a", "b", "count(*)", "y"],
    );
    f(
        "* | stats by (a, b) count(), sum(x) as y | offset 5",
        &["a", "b", "count(*)", "y"],
    );
    f(
        "* | stats by (a, b) count(), sum(x) as y | limit 10",
        &["a", "b", "count(*)", "y"],
    );
    f(
        "* | stats by (a, b) count(), sum(x) as y | limit 10 | offset 5",
        &["a", "b", "count(*)", "y"],
    );

    f("* | fields a, b | sort (a) | sort (c,b)", &["c", "b", "a"]);
}

/// Port of Go `TestQueryGetFixedFields_Failure`.
#[test]
fn test_query_get_fixed_fields_failure() {
    #[track_caller]
    fn f(q_str: &str) {
        let q = ParseQuery(q_str).unwrap_or_else(|e| panic!("unexpected error: {e}"));

        assert!(
            q.get_fixed_fields().is_none(),
            "expecting error for the query [{q_str}]"
        );
    }

    // missing fields or stats pipes
    f("*");
    f("* | limit 10");
    f("* | offset 10");
    f("* | sort by (_time desc)");
    f("* | block_stats");
}

/// Port of Go `TestQueryIsFixedOutputFieldsOrder`.
#[test]
fn test_query_is_fixed_output_fields_order() {
    #[track_caller]
    fn f(q_str: &str, result_expected: bool) {
        let q = ParseQuery(q_str).unwrap_or_else(|e| panic!("unexpected error: {e}"));

        let result = q.is_fixed_output_fields_order();
        assert_eq!(result, result_expected, "unexpected result for [{q_str}]");
    }

    f("*", false);
    f("* | sort by (_time)", false);
    f("* | fields x | union (*)", false);
    f("* | fields x | union (* | count())", true);
    f("* | fields x | union rows({'a':'b','c':'d'})", true);
    f("* | fields x | join by (a) (*)", false);
    f("* | fields x | join by (a) (* | count())", true);
    f("* | fields x | join by (a) rows({'a':'b','c':'d'})", true);

    f("* | fields x, y", true);
    f("* | fields x, y | sort by (a)", true);
    f("* | fields x, y | limit 10", true);
    f("* | count()", true);
    f("* | stats by (x,y) sum(y), count() a", true);
    f(
        "* | stats by (x,y) sum(y), count() a | sort (z,y desc)",
        true,
    );
    f("* | block_stats", true);
    f("* | query_stats", true);
    f("* | field_names", true);
    f("* | field_values x", true);
    f("* | top x", true);
}

/// Port of Go `TestParseQuery_OptimizeOffsetLimitPipes`.
#[test]
fn test_parse_query_optimize_uniq_limit_pipes() {
    // Go `optimizeUniqLimitPipes`: `uniq ... | limit N` merges into
    // `uniq ... limit N`, keeping the smaller of the two limits.
    ok("* | uniq by (x) | limit 5", "* | uniq by (x) limit 5");
    ok("* | uniq (x, y) | limit 5", "* | uniq by (x, y) limit 5");
    ok(
        "* | uniq by (x) limit 3 | limit 5",
        "* | uniq by (x) limit 3",
    );
    ok(
        "* | uniq by (x) limit 5 | limit 3",
        "* | uniq by (x) limit 3",
    );
    ok(
        "* | uniq by (x) hits | limit 5",
        "* | uniq by (x) with hits limit 5",
    );
    // Chained limits collapse first (optimizeOffsetLimitPipes), then merge.
    ok(
        "* | uniq by (x) | limit 5 | limit 3",
        "* | uniq by (x) limit 3",
    );
    // Go TestParseQuery_Success: the merge applies inside subqueries too.
    ok(
        "foo | union (bar | uniq(x) | limit 10)",
        "foo | union (bar | uniq by (x) limit 10)",
    );
}

#[test]
fn test_parse_query_optimize_offset_limit_pipes() {
    #[track_caller]
    fn f(s: &str, result_expected: &str) {
        let q = ParseQuery(s).unwrap_or_else(|e| panic!("cannot parse [{s}]: {e}"));
        assert_eq!(
            q.to_string(),
            result_expected,
            "unexpected result for [{s}]"
        );
    }

    f("* | sort by (x) | limit 30", "* | sort by (x) limit 30");
    f("* | sort by (x) | offset 10", "* | sort by (x) offset 10");
    f(
        "* | sort by (x) | offset 10 | limit 30",
        "* | sort by (x) offset 10 limit 30",
    );
    f("* | sort by (x) | offset 0", "* | sort by (x)");
    f(
        "* | sort by (x) | offset 0 | fields a, b",
        "* | sort by (x) | fields a, b",
    );
    f("* | sort by (x) | limit 0", "* | limit 0");
    f(
        "* | sort by (x) | limit 0 | keep a, b",
        "* | limit 0 | fields a, b",
    );

    f(
        "* | sort by (x) | limit 30 | limit 20",
        "* | sort by (x) limit 20",
    );
    f(
        "* | sort by (x) offset 5 | offset 10 | limit 30",
        "* | sort by (x) offset 15 limit 30",
    );
    f(
        "* | sort by (x) limit 12 | offset 10 | limit 30",
        "* | sort by (x) offset 10 limit 2",
    );
    f(
        "* | sort by (x) offset 3 limit 12 | offset 10 | limit 30 | fields x",
        "* | sort by (x) offset 13 limit 2 | fields x",
    );
    f(
        "* | sort by (x) offset 3 limit 10 | offset 10 | limit 30 | fields x",
        "* | limit 0 | fields x",
    );
    f(
        "* | sort by (x) | limit 30 | limit 20 | offset 4",
        "* | sort by (x) offset 4 limit 16",
    );
    f(
        "* | sort by (x) | limit 30 | limit 20 | offset 4 | offset 5",
        "* | sort by (x) offset 9 limit 11",
    );
    f(
        "* | sort by (x) | limit 30 | limit 20 | offset 4 | offset 5 | fields x",
        "* | sort by (x) offset 9 limit 11 | fields x",
    );

    // Verify the case without 'sort' pipe and with 'offset 0' pipes.
    // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/620#issuecomment-3276624504
    f("* | offset 0", "*");
    f("* | offset 0 | limit 10", "* | limit 10");

    // 'ofset N | limit M' without preceding 'sort' pipe
    f("* | offset 10 | limit 30", "* | limit 40 | offset 10");
    f(
        "* | offset 10 | limit 30 | fields x",
        "* | limit 40 | offset 10 | fields x",
    );

    // 'limit N | offset M' where M >= N
    f("* | limit 10 | offset 20", "* | limit 0");

    // Multiple offset pipes
    f("* | offset 10 | offset 30 | offset 50", "* | offset 90");

    // Multiple limit pipes
    f("* | limit 50 | limit 5 | limit 20", "* | limit 5");

    // Mix of limit and offset pipes
    f(
        "* | offset 3 | limit 10 | offset 5 | limit 30 | limit 5",
        "* | limit 13 | offset 8",
    );
    f(
        "* | offset 3 | limit 10 | offset 5 | limit 30 | limit 15",
        "* | limit 13 | offset 8",
    );
    f(
        "* | offset 3 | limit 10 | offset 5 | limit 30 | limit 4",
        "* | limit 12 | offset 8",
    );
    f(
        "* | offset 3 | limit 10 | offset 5 | limit 30 | limit 1",
        "* | limit 9 | offset 8",
    );
    f(
        "* | offset 3 | limit 10 | offset 5 | limit 30 | limit 0",
        "* | limit 0",
    );
}

/// Port of Go `TestParseQuery_OptimizeStarFilters`.
#[test]
fn test_parse_query_optimize_star_filters() {
    #[track_caller]
    fn f(s: &str, result_expected: &str) {
        let q = ParseQuery(s).unwrap_or_else(|e| panic!("cannot parse [{s}]: {e}"));
        assert_eq!(
            q.to_string(),
            result_expected,
            "unexpected result for [{s}]"
        );
    }

    f("*", "*");
    f("foo * bar", "foo bar");
    f("foo or * or bar", "*");
    f("foo and (bar or *)", "foo");
    f("foo or (* or (baz and (x and *))) x", "foo or x");
}

/// Port of Go `TestParseQuery_OptimizeStreamFilters`.
#[test]
fn test_parse_query_optimize_stream_filters() {
    #[track_caller]
    fn f(s: &str, result_expected: &str) {
        let q = ParseQuery(s).unwrap_or_else(|e| panic!("cannot parse [{s}]: {e}"));
        assert_eq!(
            q.to_string(),
            result_expected,
            "unexpected result for [{s}]"
        );
    }

    // Missing stream filters
    f("*", "*");
    f("foo", "foo");
    f("foo bar", "foo bar");

    // a single stream filter
    f("{foo=bar}", r#"{foo="bar"}"#);
    f(
        r#"{foo=bar,baz=~"x|y"} error"#,
        r#"{foo="bar",baz=~"x|y"} error"#,
    );
    f(
        r#"a {foo=bar,baz=~"x|y" OR a!=b} x"#,
        r#"{foo="bar",baz=~"x|y" or a!="b"} a x"#,
    );

    // multiple stream filters, which can be merged
    f(r#"{foo=bar} {baz="x"}"#, r#"{foo="bar",baz="x"}"#);
    f(
        r#"a {foo=~"bar|x"} (b:c or d) _stream:{x="y"} {foo!~"q.+"} c"#,
        r#"{foo=~"bar|x",x="y",foo!~"q.+"} a (b:c or d) c"#,
    );

    // multiple stream filters, which cannot be merged
    f(
        r#"{foo="bar" or baz="x"} {a="b"}"#,
        r#"{foo="bar" or baz="x"} {a="b"}"#,
    );
    f(
        r#"{x="y"} {foo="bar" or baz="x"} {a="b"}"#,
        r#"{x="y"} {foo="bar" or baz="x"} {a="b"}"#,
    );
    f(r#"{foo="bar"} or {baz="x"}"#, r#"{foo="bar"} or {baz="x"}"#);
}
