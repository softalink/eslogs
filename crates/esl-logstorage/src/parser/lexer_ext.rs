//! Extension methods that complete the LogsQL lexer for the parser.
//!
//! The base `Lexer` lives in [`crate::stream_filter`] (it is imported by many
//! filter/pipe modules, so it could not be relocated). The parser porter added
//! the missing `current_timestamp`/`s_orig` fields + accessors there; the
//! higher-level `*lexer` methods Go keeps in `parser.go` are provided here as an
//! extension trait so they did not have to be duplicated across modules.
//!
//! PORT NOTE: `next_compound_token_ext` re-implements Go's
//! `nextCompoundTokenExt` with stop-token support. The base
//! `Lexer::next_compound_token` in `stream_filter.rs` is the `stop_tokens == &[]`
//! case (kept there because other modules already call it); the two must stay in
//! sync.

use crate::stream_filter::Lexer;
use crate::tokenizer::is_token_rune;

/// deniedFirstCompoundTokens — disallowed starting tokens for compound tokens
/// without a whitespace in front.
const DENIED_FIRST_COMPOUND_TOKENS: &[&str] = &["/", ".", "$"];

/// glueCompoundTokens — tokens allowed inside unquoted compound tokens.
const GLUE_COMPOUND_TOKENS: &[&str] = &["+", "-", "/", ":", ".", "$"];

/// mathStopCompoundTokens — glue tokens disallowed in math compound tokens
/// (consumed by `next_compound_math_token` in the `math`/`eval` pipe parser).
const MATH_STOP_COMPOUND_TOKENS: &[&str] = &["+", "-", "/"];

/// queryPartTrailers — tokens that terminate a query part.
pub(crate) const QUERY_PART_TRAILERS: &[&str] = &["|", ")", ";", ""];

pub(crate) fn is_word(s: &str) -> bool {
    !s.is_empty() && s.chars().all(is_token_rune)
}

/// Extension trait providing the LogsQL `*lexer` methods used by the parser
/// (Go `parser.go`), implemented over [`crate::stream_filter::Lexer`].
pub(crate) trait LexerExt {
    fn is_end(&self) -> bool;
    fn is_query_part_trailer(&self) -> bool;
    fn context(&self) -> String;
    fn is_prev_raw_token(&self, tokens: &[&str]) -> bool;
    fn check_prev_adjacent_token(&self, tokens: &[&str]) -> Result<(), String>;
    fn next_compound_token_ext(&mut self, stop_tokens: &[&str]) -> Result<String, String>;
    fn next_compound_token_ext_pair(
        &mut self,
        stop_tokens: &[&str],
    ) -> Result<(String, Vec<u8>), String>;
    fn next_compound_math_token(&mut self) -> Result<String, String>;
    fn next_compound_math_token_bytes(&mut self) -> Result<Vec<u8>, String>;
    fn is_allowed_compound_token(&self, stop_tokens: &[&str]) -> bool;
}

impl LexerExt for Lexer<'_> {
    fn is_end(&self) -> bool {
        self.tail().is_empty() && self.token.is_empty() && self.raw_token().is_empty()
    }

    fn is_query_part_trailer(&self) -> bool {
        self.is_keyword(QUERY_PART_TRAILERS)
    }

    fn context(&self) -> String {
        let orig = self.s_orig();
        let mut tail = &orig[..orig.len() - self.tail().len()];
        if tail.len() > 50 {
            // mirror Go's byte-slice truncation, then snap to a char boundary
            let mut start = tail.len() - 50;
            while start < tail.len() && !tail.is_char_boundary(start) {
                start += 1;
            }
            tail = &tail[start..];
        }
        tail.to_string()
    }

    fn is_prev_raw_token(&self, tokens: &[&str]) -> bool {
        let prev_lower = self.prev_raw_token().to_lowercase();
        tokens.contains(&prev_lower.as_str())
    }

    fn check_prev_adjacent_token(&self, tokens: &[&str]) -> Result<(), String> {
        if self.is_skipped_space() || self.prev_raw_token().is_empty() {
            return Ok(());
        }
        if !self.is_prev_raw_token(tokens) {
            return Err(format!(
                "missing whitespace or ':' between {} and {}; probably, the whole string must be put into quotes",
                crate::parser::go_quote(self.prev_raw_token()),
                crate::parser::go_quote(&self.token)
            ));
        }
        Ok(())
    }

    fn is_allowed_compound_token(&self, stop_tokens: &[&str]) -> bool {
        if self.is_quoted_token() {
            return false;
        }
        if self.token.is_empty() {
            return false;
        }
        if self.is_keyword(stop_tokens) {
            return false;
        }
        if self.is_keyword(GLUE_COMPOUND_TOKENS) {
            return true;
        }
        is_word(&self.token)
    }

    fn next_compound_token_ext(&mut self, stop_tokens: &[&str]) -> Result<String, String> {
        if self.is_quoted_token() {
            let s = self.token.clone();
            self.next_token();
            return Ok(s);
        }

        if !self.is_skipped_space()
            && self.is_keyword(DENIED_FIRST_COMPOUND_TOKENS)
            && is_word(self.prev_raw_token())
        {
            return Err(format!(
                "missing whitespace between {} and {}",
                crate::parser::go_quote(self.prev_raw_token()),
                crate::parser::go_quote(&self.token)
            ));
        }

        if !self.is_allowed_compound_token(stop_tokens) {
            return Err(format!(
                "compound token cannot start with {}; put it into quotes if needed",
                crate::parser::go_quote(&self.token)
            ));
        }

        let mut s = self.token.clone();
        self.next_token();

        while !self.is_skipped_space() && self.is_allowed_compound_token(stop_tokens) {
            s += self.raw_token();
            self.next_token();
        }

        if GLUE_COMPOUND_TOKENS.contains(&s.as_str()) {
            return Err(format!(
                "compound token cannot be equal to {}; put it into quotes if needed",
                crate::parser::go_quote(&s)
            ));
        }

        Ok(s)
    }

    /// [`LexerExt::next_compound_token_ext`] returning both the legacy
    /// `String` form (scalar decoding of `\xNN >= 0x80` escapes; used for
    /// field names and keyword-shaped checks) and the Go-parity raw-byte
    /// payload (`Lexer::token_bytes`; used for phrase/value payloads). The
    /// two differ only for double/backtick-quoted tokens whose unquoted value
    /// is invalid UTF-8; unquoted compound tokens are slices of the query
    /// text and thus identical in both forms.
    fn next_compound_token_ext_pair(
        &mut self,
        stop_tokens: &[&str],
    ) -> Result<(String, Vec<u8>), String> {
        if self.is_quoted_token() {
            let s = self.token.clone();
            let b = self.token_bytes.clone();
            self.next_token();
            return Ok((s, b));
        }
        let s = self.next_compound_token_ext(stop_tokens)?;
        let b = s.clone().into_bytes();
        Ok((s, b))
    }

    fn next_compound_math_token(&mut self) -> Result<String, String> {
        self.next_compound_token_ext(MATH_STOP_COMPOUND_TOKENS)
    }

    /// Raw-byte form of [`LexerExt::next_compound_math_token`] for field
    /// names (quoted tokens carry Go-parity raw bytes).
    fn next_compound_math_token_bytes(&mut self) -> Result<Vec<u8>, String> {
        let (_, b) = self.next_compound_token_ext_pair(MATH_STOP_COMPOUND_TOKENS)?;
        Ok(b)
    }
}
