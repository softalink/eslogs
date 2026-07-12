//! Port of `pipe_field_names.go` from EsLogs v1.51.0.
//!
//! Implements the `| field_names` pipe, which returns the set of field names
//! present in the input together with per-name hit counts.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::marshal_uint64_string;

/// `PipeFieldNames` implements the `| field_names` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#field_names-pipe>
pub(crate) struct PipeFieldNames {
    /// Name of the column results are written to (defaults to `name`).
    pub(crate) result_name: String,

    /// If non-empty, only field names containing this substring are returned.
    pub(crate) filter: String,

    /// If set, there is no need to load the columns header in `write_block`.
    ///
    /// PORT NOTE: the block-search fast path this flag enables (reading field
    /// names directly from `columnsHeaderIndex`) needs the unported
    /// `block_search`; the port always uses the `get_columns` path, which is
    /// correct for pipe-produced blocks.
    pub(crate) is_first_pipe: bool,
}

/// Builds a `| field_names` pipe.
///
/// PORT NOTE: `parsePipeFieldNames` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser. `is_first_pipe`
/// is set later by the query optimizer, matching Go's `parsePipeFieldNames`
/// which leaves it `false`.
pub(crate) fn new_pipe_field_names(result_name: String, filter: String) -> PipeFieldNames {
    PipeFieldNames {
        result_name,
        filter,
        is_first_pipe: false,
    }
}

impl Pipe for PipeFieldNames {
    fn to_string(&self) -> String {
        let mut s = "field_names".to_string();
        if !self.filter.is_empty() {
            s += &format!(" filter {}", quote_token_if_needed(&self.filter));
        }
        if self.result_name != "name" {
            s += &format!(" as {}", quote_token_if_needed(&self.result_name));
        }
        s
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if self.is_first_pipe {
            pf.reset();
        } else {
            pf.add_allow_filter("*");
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeFieldNamesProcessorShard::default()))
            .collect();
        Arc::new(PipeFieldNamesProcessor {
            result_name: self.result_name.clone(),
            filter: self.filter.clone(),
            stop,
            pp_next,
            shards,
        })
    }
}

struct PipeFieldNamesProcessor {
    result_name: String,
    filter: String,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeFieldNamesProcessorShard>>,
}

#[derive(Default)]
struct PipeFieldNamesProcessorShard {
    /// Hits per field name.
    m: HashMap<String, u64>,
}

impl PipeFieldNamesProcessorShard {
    fn update_column_hits(&mut self, column_name: &str, filter: &str, hits: u64) {
        let column_name = if column_name.is_empty() {
            "_msg"
        } else {
            column_name
        };
        if !filter.is_empty() && !column_name.contains(filter) {
            return;
        }
        *self.m.entry(column_name.to_string()).or_insert(0) += hits;
    }
}

impl PipeProcessor for PipeFieldNamesProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        // Assume that the column is set for all rows in the block. This is much
        // faster than reading all column values and counting non-empty rows.
        let hits = br.rows_len() as u64;

        let cols = br.get_columns();
        let names: Vec<String> = cols
            .iter()
            .map(|&c| br.column_name(c).to_string())
            .collect();

        let mut shard = self.shards[worker_id].lock().unwrap();
        for name in &names {
            shard.update_column_hits(name, &self.filter, hits);
        }
    }

    fn flush(&self) -> Result<(), String> {
        if self.stop.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Merge state across shards.
        let mut merged: HashMap<String, u64> = HashMap::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            for (name, hits) in &shard.m {
                *merged.entry(name.clone()).or_insert(0) += *hits;
            }
        }

        // Write result.
        let mut wctx = PipeFieldNamesWriteContext::new(&self.result_name, self.pp_next.as_ref());
        for (name, hits) in &merged {
            let mut buf = Vec::new();
            marshal_uint64_string(&mut buf, *hits);
            let hits_str = String::from_utf8(buf).unwrap();
            wctx.write_row(name, &hits_str);
        }
        wctx.flush();

        Ok(())
    }
}

struct PipeFieldNamesWriteContext<'a> {
    pp_next: &'a dyn PipeProcessor,
    rcs: [ResultColumn; 2],
    br: BlockResult,
    rows_count: usize,
    values_len: usize,
}

impl<'a> PipeFieldNamesWriteContext<'a> {
    fn new(result_name: &str, pp_next: &'a dyn PipeProcessor) -> Self {
        let mut rcs: [ResultColumn; 2] = Default::default();
        rcs[0].name = result_name.to_string();
        rcs[1].name = "hits".to_string();
        Self {
            pp_next,
            rcs,
            br: BlockResult::default(),
            rows_count: 0,
            values_len: 0,
        }
    }

    fn write_row(&mut self, name: &str, hits: &str) {
        self.rcs[0].add_value(name.as_bytes());
        self.rcs[1].add_value(hits.as_bytes());
        self.values_len += name.len() + hits.len();
        self.rows_count += 1;
        if self.values_len >= 1_000_000 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.values_len = 0;
        if self.rows_count == 0 {
            return;
        }

        // Flush rcs to pp_next. Move the values out (leaving the column names in
        // place, mirroring Go's `resetValues()` after `setResultColumns`).
        let rcs = vec![
            ResultColumn {
                name: self.rcs[0].name.clone(),
                values: std::mem::take(&mut self.rcs[0].values),
            },
            ResultColumn {
                name: self.rcs[1].name.clone(),
                values: std::mem::take(&mut self.rcs[1].values),
            },
        ];
        self.br.set_result_columns(rcs, self.rows_count);
        self.rows_count = 0;
        self.pp_next.write_block(0, &mut self.br);
        self.br.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;

    fn pfn(result_name: &str, filter: &str) -> PipeFieldNames {
        new_pipe_field_names(result_name.to_string(), filter.to_string())
    }

    #[test]
    fn test_pipe_field_names_string() {
        assert_eq!(pfn("name", "").to_string(), "field_names");
        assert_eq!(pfn("name", "foo").to_string(), "field_names filter foo");
        assert_eq!(pfn("x", "").to_string(), "field_names as x");
        assert_eq!(pfn("x", "foo").to_string(), "field_names filter foo as x");
    }

    #[test]
    fn test_pipe_field_names_single_row() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pfn("name", ""), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("name", "_msg"), field("hits", "1")],
                vec![field("name", "a"), field("hits", "1")],
            ],
        );
    }

    #[test]
    fn test_pipe_field_names_result_name() {
        let rows = vec![
            vec![field("a", "test"), field("b", "aaa")],
            vec![field("a", "bar")],
            vec![field("a", "bar"), field("c", "bar")],
        ];
        let got = run_pipe(&pfn("x", ""), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("x", "a"), field("hits", "3")],
                vec![field("x", "b"), field("hits", "1")],
                vec![field("x", "c"), field("hits", "1")],
            ],
        );
    }

    #[test]
    fn test_pipe_field_names_filter() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pfn("name", "a"), &rows);
        assert_rows_eq(got, &[vec![field("name", "a"), field("hits", "1")]]);
    }

    #[test]
    fn test_pipe_field_names_update_needed_fields() {
        // The pipe requires all fields (adds an allow-all filter).
        expect_needed_fields(&pfn("f1", ""), "*", "", "*", "");
        expect_needed_fields(&pfn("f3", ""), "*", "f1,f2", "*", "");
        expect_needed_fields(&pfn("f1", ""), "*", "s1,f1,f2", "*", "");
        expect_needed_fields(&pfn("f3", ""), "f1,f2", "", "*", "");
        expect_needed_fields(&pfn("f1", ""), "s1,f1,f2", "", "*", "");
    }
}
