//! Port of `github.com/VictoriaMetrics/metrics/validator.go`.

use std::sync::LazyLock;

use regex::Regex;

/// Validates the provided string to be a valid Prometheus-compatible metric
/// with possible labels. For instance,
///
///   - `foo`
///   - `foo{bar="baz"}`
///   - `foo{bar="baz",aaa="b"}`
pub fn validate_metric(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("metric cannot be empty".to_string());
    }
    if s.contains('\n') {
        return Err("metric cannot contain line breaks".to_string());
    }
    let Some(n) = s.find('{') else {
        return validate_ident(s);
    };
    let ident = &s[..n];
    let s = &s[n + 1..];
    validate_ident(ident)?;
    if s.is_empty() || !s.ends_with('}') {
        return Err(format!(
            "missing closing curly brace at the end of {ident:?}"
        ));
    }
    validate_tags(&s[..s.len() - 1])
}

pub(super) fn validate_tags(mut s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Ok(());
    }
    loop {
        let Some(n) = s.find('=') else {
            return Err(format!("missing `=` after {s:?}"));
        };
        let ident = &s[..n];
        s = &s[n + 1..];
        validate_ident(ident)?;
        if s.is_empty() || !s.starts_with('"') {
            return Err(format!(
                "missing starting `\"` for {ident:?} value; tail={s:?}"
            ));
        }
        s = &s[1..];
        // Skip over escaped quotes inside the tag value (Go's `goto again`).
        loop {
            let Some(n) = s.find('"') else {
                return Err(format!(
                    "missing trailing `\"` for {ident:?} value; tail={s:?}"
                ));
            };
            let mut m = n;
            while m > 0 && s.as_bytes()[m - 1] == b'\\' {
                m -= 1;
            }
            if (n - m) % 2 == 1 {
                s = &s[n + 1..];
                continue;
            }
            s = &s[n + 1..];
            break;
        }
        if s.is_empty() {
            return Ok(());
        }
        let Some(rest) = s.strip_prefix(',') else {
            return Err(format!("missing `,` after {ident:?} value; tail={s:?}"));
        };
        s = skip_space(rest);
    }
}

fn skip_space(mut s: &str) -> &str {
    while let Some(rest) = s.strip_prefix(' ') {
        s = rest;
    }
    s
}

fn validate_ident(s: &str) -> Result<(), String> {
    if !IDENT_REGEXP.is_match(s) {
        return Err(format!("invalid identifier {s:?}"));
    }
    Ok(())
}

static IDENT_REGEXP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[a-zA-Z_:.][a-zA-Z0-9_:.]*$").expect("valid regexp"));

#[cfg(test)]
mod tests {
    use super::validate_metric;

    // Port of validator_test.go.
    #[test]
    fn test_validate_metric_success() {
        let f = |s: &str| {
            validate_metric(s).unwrap_or_else(|err| panic!("cannot validate {s:?}: {err}"));
        };
        f("a");
        f("_9:8");
        f("a{}");
        f(r#"a{foo="bar"}"#);
        f(r#"foo{bar="baz", x="y\"z"}"#);
        f(r#"foo{bar="b}az"}"#);
        f(r#":foo:bar{bar="a",baz="b"}"#);
        f(r#"some.foo{bar="baz"}"#);
    }

    #[test]
    fn test_validate_metric_error() {
        let f = |s: &str| {
            assert!(
                validate_metric(s).is_err(),
                "expecting non-nil error when validating {s:?}"
            );
        };
        f("");
        f("{}");

        // Superfluous space.
        f("a ");
        f(" a");
        f(" a ");
        f("a {}");
        f("a{} ");
        f("a{ }");
        f(r#"a{foo ="bar"}"#);
        f(r#"a{ foo="bar"}"#);
        f(r#"a{foo= "bar"}"#);
        f(r#"a{foo="bar" }"#);
        f(r#"a{foo="bar" ,baz="a"}"#);

        // Invalid tags.
        f("a{foo}");
        f("a{=}");
        f(r#"a{=""}"#);
        f("a{");
        f("a}");
        f("a{foo=}");
        f(r#"a{foo=""#);
        f(r#"a{foo="}"#);
        f(r#"a{foo="bar",}"#);
        f(r#"a{foo="bar", x"#);
        f(r#"a{foo="bar", x="#);
        f(r#"a{foo="bar", x=""#);
        f(r#"a{foo="bar", x="}"#);
    }
}
