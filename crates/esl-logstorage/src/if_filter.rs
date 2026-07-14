//! Port of EsLogs `lib/logstorage/if_filter.go`.
//!
//! `IfFilter` is the canonical `if (...)` / `case (...)` clause attached to
//! conditional pipes (`format if (...) ...`, `extract if (...) ...`, etc.).
//!
//! PORT NOTE: two placeholder copies of this type still live in
//! `pipe_unpack.rs` and `pipe_update.rs` (they were created before the parser
//! was ported, so the pipes could carry an already-parsed `if` clause without a
//! lexer). Those are left in place for a later dedup; the pipe parsers in
//! `parser::parse_pipe` parse a canonical [`IfFilter`] here and then rebuild the
//! pipe-local placeholder from its `f` via the pipe's own constructor. When the
//! dedup happens, point `pipe_unpack`/`pipe_update` at this module and delete
//! their copies.

use std::sync::Arc;

use crate::filter::Filter;
use crate::filter_noop::new_filter_noop;
use crate::parser::parse_filter::parse_filter;
use crate::prefix_filter;
use crate::stream_filter::Lexer;

/// Port of Go's `ifFilter`.
#[derive(Clone)]
pub struct IfFilter {
    /// The compiled filter (Go `f`).
    pub(crate) f: Arc<dyn Filter>,

    /// Fields the filter needs (Go `allowFilters`).
    ///
    /// PORT NOTE: computed for parity, but the pipe parsers rebuild their own
    /// pipe-local `IfFilter` placeholder (which recomputes this), so the
    /// canonical copy is currently only read in tests.
    #[allow(dead_code)]
    pub(crate) allow_filters: Vec<Vec<u8>>,
}

impl IfFilter {
    /// Port of Go `(*ifFilter).String`.
    #[allow(clippy::inherent_to_string, dead_code)]
    pub(crate) fn to_string(&self) -> String {
        format!("if ({})", self.f.to_string())
    }
}

/// Port of Go `parseIfFilter`.
pub(crate) fn parse_if_filter(lex: &mut Lexer) -> Result<IfFilter, String> {
    if !lex.is_keyword(&["if", "case"]) {
        return Err(format!(
            "unexpected keyword {}; expecting 'if' or 'case'",
            crate::parser::go_quote(&lex.token)
        ));
    }
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        return Err(format!(
            "unexpected token {} after 'if'; expecting '('",
            crate::parser::go_quote(&lex.token)
        ));
    }
    lex.next_token();

    if lex.is_keyword(&[")"]) {
        lex.next_token();
        return Ok(new_if_filter(Arc::new(new_filter_noop())));
    }

    let f = parse_filter(lex, true).map_err(|err| format!("cannot parse 'if' filter: {err}"))?;
    if lex.is_keyword(&[";"]) {
        lex.next_token();
    }
    if !lex.is_keyword(&[")"]) {
        return Err(format!(
            "unexpected token {} after 'if' filter; expecting ')'",
            crate::parser::go_quote(&lex.token)
        ));
    }
    lex.next_token();

    Ok(new_if_filter(Arc::from(f)))
}

/// Port of Go `newIfFilter`.
pub(crate) fn new_if_filter(f: Arc<dyn Filter>) -> IfFilter {
    let mut pf = prefix_filter::Filter::default();
    f.update_needed_fields(&mut pf);
    let allow_filters = pf.get_allow_filters();
    IfFilter { f, allow_filters }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<IfFilter, String> {
        let mut lex = Lexer::new(s);
        let iff = parse_if_filter(&mut lex)?;
        if !lex.token.is_empty() {
            return Err(format!("unexpected tail: {}", lex.token));
        }
        Ok(iff)
    }

    #[test]
    fn test_parse_if_filter_success() {
        let iff = parse("if (foo:bar)").unwrap();
        assert_eq!(iff.to_string(), "if (foo:bar)");
        assert_eq!(iff.allow_filters, vec![b"foo".to_vec()]);
    }

    #[test]
    fn test_parse_if_filter_case_keyword() {
        let iff = parse("case (x:y)").unwrap();
        assert_eq!(iff.to_string(), "if (x:y)");
    }

    #[test]
    fn test_parse_if_filter_empty_is_noop() {
        let iff = parse("if ()").unwrap();
        assert_eq!(iff.to_string(), "if (*)");
    }

    #[test]
    fn test_parse_if_filter_trailing_semicolon() {
        let iff = parse("if (a:b;)").unwrap();
        assert_eq!(iff.to_string(), "if (a:b)");
    }

    #[test]
    fn test_parse_if_filter_errors() {
        assert!(parse("foo (a:b)").is_err());
        assert!(parse("if a:b)").is_err());
        assert!(parse("if (a:b").is_err());
    }
}
