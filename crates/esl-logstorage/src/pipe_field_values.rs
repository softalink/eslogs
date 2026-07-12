//! Port of `pipe_field_values.go` from EsLogs v1.51.0.
//!
//! Implements the `| field_values field` pipe, which returns the distinct
//! values of `field` together with per-value hit counts. It is a thin wrapper
//! over the `| uniq` pipe.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_uniq::new_pipe_uniq;
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// Port of Go `getUniqueResultName` (parser.go).
///
/// PORT NOTE: duplicated here because `parser.rs` — where this helper
/// belongs — is deferred (matches the copy in `pipe_field_values_local`);
/// `pub(crate)` so `pipe_field_names`' split can reuse it.
pub(crate) fn get_unique_result_name(result_name: &str, by_fields: &[String]) -> String {
    let mut name = result_name.to_string();
    while by_fields.iter().any(|f| f == &name) {
        name.push('s');
    }
    name
}

/// `PipeFieldValues` implements the `| field_values ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#field_values-pipe>
pub(crate) struct PipeFieldValues {
    pub(crate) field: String,

    /// If non-empty, only values containing this substring are returned.
    pub(crate) filter: String,

    pub(crate) limit: u64,
}

/// Builds a `| field_values field` pipe.
///
/// PORT NOTE: `parsePipeFieldValues` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_field_values(field: String, filter: String, limit: u64) -> PipeFieldValues {
    PipeFieldValues {
        field,
        filter,
        limit,
    }
}

impl Pipe for PipeFieldValues {
    /// Port of Go `pipeFieldValues.splitToRemoteAndLocal`: per-node value hits
    /// are merged locally by `field_values_local`.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        let p_local = crate::pipe_field_values_local::new_pipe_field_values_local(
            self.field.clone(),
            self.limit,
        );
        (
            Some(crate::pipe::clone_pipe(self, timestamp)),
            vec![Box::new(p_local)],
        )
    }

    fn to_string(&self) -> String {
        let mut s = format!("field_values {}", quote_token_if_needed(&self.field));
        if !self.filter.is_empty() {
            s += &format!(" filter {}", quote_token_if_needed(&self.filter));
        }
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
        pf.add_allow_filter(&self.field);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        // Go builds an equivalent `pipeUniq` and returns its processor.
        let hits_field_name = self.get_hits_field_name();
        let pu = new_pipe_uniq(
            vec![self.field.clone()],
            self.filter.clone(),
            hits_field_name,
            self.limit,
        );
        pu.new_pipe_processor(concurrency, stop, pp_next)
    }
}

impl PipeFieldValues {
    fn get_hits_field_name(&self) -> String {
        get_unique_result_name("hits", std::slice::from_ref(&self.field))
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;

    fn pfv(field: &str, filter: &str, limit: u64) -> PipeFieldValues {
        new_pipe_field_values(field.to_string(), filter.to_string(), limit)
    }

    #[test]
    fn test_pipe_field_values_results() {
        let rows = vec![
            vec![field("a", "2"), field("b", "3")],
            vec![field("a", "2"), field("b", "3")],
            vec![field("a", "2"), field("b", "54"), field("c", "d")],
        ];

        // single distinct value for `a`
        let got = run_pipe(&pfv("a", "", 0), &rows);
        assert_rows_eq(got, &[vec![field("a", "2"), field("hits", "3")]]);

        // distinct values for `b` with hit counts
        let got = run_pipe(&pfv("b", "", 0), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("b", "3"), field("hits", "2")],
                vec![field("b", "54"), field("hits", "1")],
            ],
        );

        // missing field `d` -> single empty value across all rows
        let got = run_pipe(&pfv("d", "", 0), &rows);
        assert_rows_eq(got, &[vec![field("d", ""), field("hits", "3")]]);
    }

    #[test]
    fn test_pipe_field_values_string() {
        assert_eq!(pfv("x", "", 0).to_string(), "field_values x");
        assert_eq!(pfv("x", "abc", 0).to_string(), "field_values x filter abc");
        assert_eq!(pfv("x", "", 10).to_string(), "field_values x limit 10");
        assert_eq!(
            pfv("x", "abc", 10).to_string(),
            "field_values x filter abc limit 10"
        );
    }

    #[test]
    fn test_pipe_field_values_update_needed_fields() {
        expect_needed_fields(&pfv("x", "", 0), "*", "", "x", "");
        expect_needed_fields(&pfv("x", "", 0), "*", "f1,f2", "x", "");
        expect_needed_fields(&pfv("x", "", 0), "*", "f1,x", "x", "");
        expect_needed_fields(&pfv("x", "", 0), "f1,f2", "", "x", "");
        expect_needed_fields(&pfv("x", "", 0), "f1,x", "", "x", "");
    }
}
