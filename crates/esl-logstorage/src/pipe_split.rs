//! Port of `lib/logstorage/pipe_split.go` — the `| split ...` pipe.
//!
//! `split` breaks the value of `src_field` into parts using `separator`,
//! marshals the parts as a JSON array string, and stores the result into
//! `dst_field`.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#split-pipe>
//!
//! PORT NOTE — parser: Go's `parsePipeSplit(lex)` depends on the query lexer,
//! which is not ported yet. Only the lexer entry point is deferred; the pipe
//! is fully ported and constructed via [`PipeSplit::new`].
//!
//! PORT NOTE — write context: Go routes rows through the shared
//! `pipeUnpackWriteContext` (defined in `pipe_unpack.go`, not ported yet) with
//! `keepOriginalFields=false, skipEmptyResults=false`. That reduces to
//! "emit every source column unchanged plus one extra `dst_field` column", so
//! this port builds the output block directly instead of pulling in the whole
//! unpack write context. The intra-block 64KB re-flush is likewise omitted:
//! each input block produces one output block, which is behaviorally identical
//! (only block boundaries differ, and downstream collects rows regardless).
//!
//! PORT NOTE — sharding: Go keeps per-worker shards purely for buffer reuse.
//! `write_block` here is stateless across blocks, so no per-worker state is
//! required and the `Vec<Mutex<Shard>>` pattern is unnecessary.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stats_uniq_values::marshal_json_array;
use crate::stream_filter::quote_token_if_needed;

/// `pipeSplit` processes `| split ...` queries.
pub(crate) struct PipeSplit {
    /// Separator used for splitting the input field value.
    pub(crate) separator: String,

    /// Field to split.
    pub(crate) src_field: String,

    /// Field to store the split result.
    pub(crate) dst_field: String,
}

impl PipeSplit {
    /// Builds a `| split ...` pipe.
    ///
    /// PORT NOTE: replaces Go's lexer-driven `parsePipeSplit`; callers pass the
    /// already-parsed separator/source/destination.
    pub(crate) fn new(
        separator: impl Into<String>,
        src_field: impl Into<String>,
        dst_field: impl Into<String>,
    ) -> Self {
        Self {
            separator: separator.into(),
            src_field: src_field.into(),
            dst_field: dst_field.into(),
        }
    }
}

impl Pipe for PipeSplit {
    /// Port of Go `pipeSplit.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = format!("split {}", quote_token_if_needed(&self.separator));
        if self.src_field != "_msg" {
            s += &format!(" from {}", quote_token_if_needed(&self.src_field));
        }
        if self.dst_field != self.src_field {
            s += &format!(" as {}", quote_token_if_needed(&self.dst_field));
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.dst_field != "_time"
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_string(&self.dst_field) {
            pf.add_deny_filter(&self.dst_field);
            pf.add_allow_filter(&self.src_field);
        }
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeSplitProcessor {
            separator: self.separator.clone(),
            src_field: self.src_field.clone(),
            dst_field: self.dst_field.clone(),
            pp_next,
        })
    }
}

struct PipeSplitProcessor {
    separator: String,
    src_field: String,
    dst_field: String,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeSplitProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        let n = br.rows_len();
        if n == 0 {
            return;
        }

        // Materialize source-field values; a missing field resolves to an
        // empty const column, matching Go's getColumnByName behavior.
        let src_col = br.get_column_by_name(&self.src_field);
        let src_values: Vec<Vec<u8>> = br.column_get_values(src_col).to_vec();

        // Copy every source column through unchanged.
        let cols = br.get_columns();
        let mut rcs: Vec<ResultColumn> = Vec::with_capacity(cols.len() + 1);
        for &c in &cols {
            let name = br.column_name(c).to_vec();
            let values = br.column_get_values(c).to_vec();
            rcs.push(ResultColumn { name, values });
        }

        // Build the destination column: split each source value and marshal as
        // a JSON array. Values repeat often, so cache the previous result like
        // Go does.
        let mut dst_values: Vec<Vec<u8>> = Vec::with_capacity(n);
        let mut words: Vec<String> = Vec::new();
        let mut prev: Option<&[u8]> = None;
        let mut encoded: Vec<u8> = Vec::new();
        // Loop shape mirrors the Go source.
        #[allow(clippy::needless_range_loop)]
        for row in 0..n {
            let sv = src_values[row].as_slice();
            if prev != Some(sv) {
                let s = std::str::from_utf8(sv).unwrap_or("");
                words = split_string(std::mem::take(&mut words), s, &self.separator);
                let items: Vec<Vec<u8>> = words.iter().map(|w| w.as_bytes().to_vec()).collect();
                encoded.clear();
                marshal_json_array(&mut encoded, &items);
                prev = Some(sv);
            }
            dst_values.push(encoded.clone());
        }
        rcs.push(ResultColumn {
            name: self.dst_field.clone().into_bytes(),
            values: dst_values,
        });

        let mut out = BlockResult::default();
        out.set_result_columns(rcs, n);
        self.pp_next.write_block(worker_id, &mut out);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Splits `s` by `separator`, appending the parts to `dst`.
///
/// Port of Go's `splitString`. An empty separator splits into individual
/// characters (Unicode runes).
fn split_string(mut dst: Vec<String>, s: &str, separator: &str) -> Vec<String> {
    if separator.is_empty() {
        // special case for empty separator
        for r in s.chars() {
            dst.push(r.to_string());
        }
        return dst;
    }

    let mut s = s;
    while !s.is_empty() {
        match s.find(separator) {
            None => {
                dst.push(s.to_string());
                return dst;
            }
            Some(n) => {
                dst.push(s[..n].to_string());
                s = &s[n + separator.len()..];
            }
        }
    }
    dst.push(String::new());
    dst
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn f(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    // PORT NOTE: Go's `TestParsePipeSplitSuccess` / `TestParsePipeSplitFailure`
    // exercise the query lexer (`parsePipeSplit`), which is not ported; skipped.

    #[test]
    fn test_split_string() {
        fn check(s: &str, separator: &str, expected: &[&str]) {
            let result = split_string(Vec::new(), s, separator);
            let expected: Vec<String> = expected.iter().map(|x| x.to_string()).collect();
            assert_eq!(result, expected, "s={s:?} separator={separator:?}");
        }

        // empty input string
        check("", "", &[]);
        check("", "foobar", &[""]);

        // empty separator
        check("Шzч", "", &["Ш", "z", "ч"]);

        // single-char delimiter
        check(",foo,bar,,baz,", ",", &["", "foo", "bar", "", "baz", ""]);

        // multi-char delimiter with unicode chars
        check("йцуквенгшвевоы", "ве", &["йцук", "нгш", "воы"]);

        // missing separator
        check("foobar", "aaaa", &["foobar"]);
    }

    #[test]
    fn test_pipe_split() {
        // split by missing field
        check_pipe(
            PipeSplit::new(",", "x", "x"),
            &[vec![
                f("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                f("q", "w"),
            ]],
            &[vec![
                f("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                f("q", "w"),
                f("x", r#"[""]"#),
            ]],
        );

        // split by a field without separators
        check_pipe(
            PipeSplit::new(" ", "q", "q"),
            &[vec![
                f("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                f("q", "!#$%,"),
            ]],
            &[vec![
                f("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                f("q", r#"["!#$%,"]"#),
            ]],
        );

        // split by a field with separators
        check_pipe(
            PipeSplit::new(", ", "a", "a"),
            &[
                vec![f("a", "foo, bar baz"), f("q", "w")],
                vec![f("a", "b,c, d, ef"), f("c", "d")],
            ],
            &[
                vec![f("a", r#"["foo","bar baz"]"#), f("q", "w")],
                vec![f("a", r#"["b,c","d","ef"]"#), f("c", "d")],
            ],
        );

        // split by empty separator
        check_pipe(
            PipeSplit::new("", "a", "a"),
            &[
                vec![f("a", "foo,bar"), f("q", "w")],
                vec![f("a", "b,c"), f("c", "d")],
            ],
            &[
                vec![f("a", r#"["f","o","o",",","b","a","r"]"#), f("q", "w")],
                vec![f("a", r#"["b",",","c"]"#), f("c", "d")],
            ],
        );

        // split into another field
        check_pipe(
            PipeSplit::new(",", "a", "b"),
            &[
                vec![f("a", "foo,bar baz"), f("q", "w")],
                vec![f("a", "b"), f("c", "d")],
            ],
            &[
                vec![
                    f("a", "foo,bar baz"),
                    f("b", r#"["foo","bar baz"]"#),
                    f("q", "w"),
                ],
                vec![f("a", "b"), f("b", r#"["b"]"#), f("c", "d")],
            ],
        );

        // split from _msg inplace
        check_pipe(
            PipeSplit::new(",", "_msg", "_msg"),
            &[
                vec![f("_msg", "foo,bar baz"), f("q", "w")],
                vec![f("_msg", "b"), f("c", "d")],
            ],
            &[
                vec![f("_msg", r#"["foo","bar baz"]"#), f("q", "w")],
                vec![f("_msg", r#"["b"]"#), f("c", "d")],
            ],
        );

        // split from _msg into other field
        check_pipe(
            PipeSplit::new(",", "_msg", "b"),
            &[
                vec![f("_msg", "foo,bar foo"), f("q", "w")],
                vec![f("_msg", "b"), f("c", "d")],
            ],
            &[
                vec![
                    f("_msg", "foo,bar foo"),
                    f("b", r#"["foo","bar foo"]"#),
                    f("q", "w"),
                ],
                vec![f("_msg", "b"), f("b", r#"["b"]"#), f("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_split_update_needed_fields() {
        // all the needed fields
        check_needed_fields(PipeSplit::new(" ", "x", "x"), "*", "", "*", "");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "*", "", "*", "y");

        // all the needed fields, unneeded fields do not intersect with src
        check_needed_fields(PipeSplit::new(" ", "x", "x"), "*", "f1,f2", "*", "f1,f2");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "*", "f1,f2", "*", "f1,f2,y");

        // all the needed fields, unneeded fields intersect with src
        check_needed_fields(PipeSplit::new(" ", "x", "x"), "*", "f2,x", "*", "f2,x");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "*", "f2,x", "*", "f2,y");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "*", "f2,y", "*", "f2,y");

        // needed fields do not intersect with src
        check_needed_fields(PipeSplit::new(" ", "x", "x"), "f1,f2", "", "f1,f2", "");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "f1,f2", "", "f1,f2", "");

        // needed fields intersect with src
        check_needed_fields(PipeSplit::new(" ", "x", "x"), "f2,x", "", "f2,x", "");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "f2,x", "", "f2,x", "");
        check_needed_fields(PipeSplit::new(" ", "x", "y"), "f2,y", "", "f2,x", "");
    }

    // -- shared test harness (ports pipe_utils_test.go pieces) ----------------

    fn check_pipe(pipe: PipeSplit, rows: &[Vec<Field>], expected: &[Vec<Field>]) {
        let got = run_pipe(&pipe, rows);
        assert_rows_equal(got, expected.to_vec());
    }

    fn check_needed_fields(
        pipe: PipeSplit,
        allow: &str,
        deny: &str,
        allow_expected: &str,
        deny_expected: &str,
    ) {
        let mut pf = prefix_filter::Filter::default();
        if !allow.is_empty() {
            pf.add_allow_filters(&csv(allow));
        }
        if !deny.is_empty() {
            pf.add_deny_filters(&csv(deny));
        }
        pipe.update_needed_fields(&mut pf);

        let mut got_allow = pf.get_allow_filters();
        got_allow.sort();
        let mut got_deny = pf.get_deny_filters();
        got_deny.sort();

        let mut exp_allow = csv(allow_expected);
        exp_allow.sort();
        let mut exp_deny = csv(deny_expected);
        exp_deny.sort();

        assert_eq!(got_allow, exp_allow, "allow filters mismatch");
        assert_eq!(got_deny, exp_deny, "deny filters mismatch");
    }

    fn csv(s: &str) -> Vec<String> {
        if s.is_empty() {
            return Vec::new();
        }
        s.split(',').map(|x| x.to_string()).collect()
    }

    #[derive(Default)]
    struct CollectProcessor {
        rows: std::sync::Mutex<Vec<Vec<Field>>>,
    }

    impl PipeProcessor for CollectProcessor {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<Vec<u8>> = cols.iter().map(|&c| br.column_name(c).to_vec()).collect();
            let mut colvals: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cols.len());
            for &c in &cols {
                colvals.push(br.column_get_values(c).to_vec());
            }
            let n = br.rows_len();
            let mut out = self.rows.lock().unwrap();
            // Loop shape mirrors the Go source.
            #[allow(clippy::needless_range_loop)]
            for r in 0..n {
                let mut row = Vec::with_capacity(names.len());
                for (j, name) in names.iter().enumerate() {
                    let v = colvals[j][r].clone();
                    row.push(Field {
                        name: name.clone(),
                        value: v,
                    });
                }
                out.push(row);
            }
        }

        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn run_pipe(pipe: &dyn Pipe, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
        let stop = Arc::new(AtomicBool::new(false));
        let sink: Arc<CollectProcessor> = Arc::new(CollectProcessor::default());
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        // Feed rows in blocks of maximal contiguous same-field runs, mirroring
        // Go's testBlockResultWriter (which never mixes differing field sets in
        // one block). Randomized block splitting is unnecessary since the sink
        // collects and sorts all rows.
        let mut i = 0;
        while i < rows.len() {
            let mut j = i + 1;
            while j < rows.len() && same_fields(&rows[i], &rows[j]) {
                j += 1;
            }
            let mut br = BlockResult::default();
            br.must_init_from_rows(&rows[i..j]);
            pp.write_block(0, &mut br);
            i = j;
        }
        pp.flush().unwrap();
        // Drop the processor so its clone of the sink is released before unwrap.
        drop(pp);

        Arc::try_unwrap(sink)
            .ok()
            .expect("sink still shared")
            .rows
            .into_inner()
            .unwrap()
    }

    fn same_fields(a: &[Field], b: &[Field]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.name == y.name)
    }

    fn assert_rows_equal(got: Vec<Vec<Field>>, expected: Vec<Vec<Field>>) {
        let got = canon(got);
        let expected = canon(expected);
        assert_eq!(got, expected, "unexpected pipe output");
    }

    fn canon(mut rows: Vec<Vec<Field>>) -> Vec<Vec<Field>> {
        for row in &mut rows {
            row.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));
        }
        rows.sort_by(|a, b| {
            for (x, y) in a.iter().zip(b.iter()) {
                let c = x.name.cmp(&y.name).then(x.value.cmp(&y.value));
                if c != std::cmp::Ordering::Equal {
                    return c;
                }
            }
            a.len().cmp(&b.len())
        });
        rows
    }
}
