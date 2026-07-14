//! Port of `pipe_uniq_local.go` — the cluster-local merge stage of
//! `| uniq ... with hits`.
//!
//! Input blocks are pre-aggregated remote `uniq` results: the by-field columns
//! plus a hits column. Each row's by-field values are marshaled into a key, the
//! hits are parsed, and the resulting `(key, hits)` pairs are collected across
//! workers, merged (summing hits per identical key, applying the limit), and
//! written out as `by_fields ++ hits`.
//!
//! # PORT NOTES
//! * This is a cluster-only pipe (`splitToRemoteAndLocal` of `pipe_uniq`), so
//!   the lexer parser is out of scope; a `pub(crate)` constructor is exposed.
//! * Go's `ValueWithHits` / `MergeValuesWithHits` live in an unported helper
//!   package; a faithful self-contained merge is implemented here — sum hits per
//!   value, then (when the limit is exceeded) keep the top-`limit` entries by
//!   hits descending. The upstream reset-hits-on-overflow nuance is not modeled
//!   for the local stage (hits are already aggregated from the remote stage).
//! * `chunkedAllocator` / `stateSizeBudget` accounting is dropped; the parallel
//!   shard merge is done sequentially.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use esl_common::encoding::{marshal_bytes, unmarshal_bytes};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{marshal_uint64_string, try_parse_uint64};

/// A marshaled by-field key together with its hit count (Go `ValueWithHits`).
struct ValueWithHits {
    value: Vec<u8>,
    hits: u64,
}

/// The `| uniq_local ...` pipe.
pub struct PipeUniqLocal {
    by_fields: Vec<Vec<u8>>,
    hits_field_name: Vec<u8>,
    limit: u64,
}

/// Builds a [`PipeUniqLocal`] (Go `pipeUniqLocal` wrapping the parent
/// `uniq`); produced by `pipeUniq.splitToRemoteAndLocal` (the cluster split).
pub(crate) fn new_pipe_uniq_local(
    by_fields: Vec<Vec<u8>>,
    hits_field_name: Vec<u8>,
    limit: u64,
) -> PipeUniqLocal {
    PipeUniqLocal {
        by_fields,
        hits_field_name,
        limit,
    }
}

impl Pipe for PipeUniqLocal {
    /// Port of Go `pipeUniqLocal.splitToRemoteAndLocal`: this pipe is only
    /// ever produced by a split, so splitting it again is a bug.
    fn split_to_remote_and_local(&self, _timestamp: i64) -> crate::pipe::SplitPipesResult {
        esl_common::panicf!("BUG: unexpected call for pipeUniqLocal");
        unreachable!()
    }

    fn to_string(&self) -> String {
        let mut s = "uniq_local".to_string();
        if !self.by_fields.is_empty() {
            s += " by (";
            s += &crate::stats_count::field_names_string(&self.by_fields);
            s += ")";
        }
        s += &format!(" limit {}", self.limit);
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
        if self.by_fields.is_empty() {
            pf.add_allow_filter("*");
        } else {
            pf.add_allow_filters(&self.by_fields);
            pf.add_allow_filter(&self.hits_field_name);
        }
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let n = concurrency.max(1);
        Arc::new(PipeUniqLocalProcessor {
            by_fields: Arc::new(self.by_fields.clone()),
            hits_field_name: self.hits_field_name.clone(),
            limit: self.limit,
            pp_next,
            shards: (0..n).map(|_| Mutex::new(Vec::new())).collect(),
        })
    }
}

struct PipeUniqLocalProcessor {
    by_fields: Arc<Vec<Vec<u8>>>,
    hits_field_name: Vec<u8>,
    limit: u64,
    pp_next: Arc<dyn PipeProcessor>,
    // Per-worker collected (key, hits) pairs.
    shards: Vec<Mutex<Vec<ValueWithHits>>>,
}

impl PipeProcessor for PipeUniqLocalProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        if self.hits_field_name.is_empty() {
            panic!(
                "BUG: expecting non-empty hitsFieldName; by_fields={:?}",
                self.by_fields
            );
        }

        // Obtain the by-field column values (all columns when by_fields is empty,
        // mirroring Go's getColumnValuess).
        let mut column_valuess: Vec<Vec<Vec<u8>>> = Vec::new();
        if self.by_fields.is_empty() {
            let cols = br.get_columns();
            for c in cols {
                column_valuess.push(br.column_get_values(c).to_vec());
            }
        } else {
            for f in self.by_fields.iter() {
                let c = br.get_column_by_name(f);
                column_valuess.push(br.column_get_values(c).to_vec());
            }
        }

        let c_hits = br.get_column_by_name(&self.hits_field_name);
        let hits: Vec<Vec<u8>> = br.column_get_values(c_hits).to_vec();

        let rows_len = br.rows_len();
        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        for row_idx in 0..rows_len {
            let mut buf = Vec::new();
            for column_values in &column_valuess {
                marshal_bytes(&mut buf, &column_values[row_idx]);
            }
            let hits_str = std::str::from_utf8(&hits[row_idx]).unwrap_or("");
            let hits64 = match try_parse_uint64(hits_str) {
                Some(n) => n,
                None => panic!(
                    "BUG: unexpected hits received from the remote storage at the column {:?}: {:?}; it must be uint64",
                    self.hits_field_name, hits_str
                ),
            };
            shard.push(ValueWithHits {
                value: buf,
                hits: hits64,
            });
        }
    }

    fn flush(&self) -> Result<(), String> {
        let mut all: Vec<Vec<ValueWithHits>> = Vec::with_capacity(self.shards.len());
        for m in &self.shards {
            let mut shard = m.lock().unwrap();
            all.push(std::mem::take(&mut *shard));
        }
        let result = merge_values_with_hits(all, self.limit);

        // Output columns: by_fields ++ hits.
        let by_len = self.by_fields.len();
        let mut rcs: Vec<ResultColumn> = self
            .by_fields
            .iter()
            .map(|name| ResultColumn {
                name: name.clone(),
                values: Vec::new(),
            })
            .collect();
        rcs.push(ResultColumn {
            name: self.hits_field_name.clone(),
            values: Vec::new(),
        });

        let mut rows_count = 0usize;
        for vh in &result {
            let mut rest: &[u8] = &vh.value;
            let mut idx = 0;
            while idx < by_len {
                let (v, n) = unmarshal_bytes(rest);
                match v {
                    Some(b) if n > 0 => {
                        rcs[idx].add_value(b);
                        rest = &rest[n as usize..];
                        idx += 1;
                    }
                    _ => break,
                }
            }
            // Pad any missing by-field slots (defensive; keeps columns aligned).
            while idx < by_len {
                rcs[idx].add_value(b"");
                idx += 1;
            }
            let mut hb = Vec::new();
            marshal_uint64_string(&mut hb, vh.hits);
            rcs[by_len].add_value(&hb);
            rows_count += 1;
        }

        if rows_count == 0 {
            return Ok(());
        }
        let mut br = BlockResult::default();
        br.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(0, &mut br);
        Ok(())
    }
}

/// Sums hits per identical key across shards; when `limit > 0` and the number
/// of unique keys exceeds it, keeps the top-`limit` keys by hits descending.
fn merge_values_with_hits(shards: Vec<Vec<ValueWithHits>>, limit: u64) -> Vec<ValueWithHits> {
    let mut m: HashMap<Vec<u8>, u64> = HashMap::new();
    for shard in shards {
        for vh in shard {
            *m.entry(vh.value).or_default() += vh.hits;
        }
    }
    let mut result: Vec<ValueWithHits> = m
        .into_iter()
        .map(|(value, hits)| ValueWithHits { value, hits })
        .collect();

    if limit > 0 && result.len() as u64 > limit {
        result.sort_by_key(|x| std::cmp::Reverse(x.hits));
        result.truncate(limit as usize);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    struct Collector {
        blocks: Mutex<Vec<Vec<Field>>>,
    }
    impl Collector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                blocks: Mutex::new(Vec::new()),
            })
        }
    }
    impl PipeProcessor for Collector {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<Vec<u8>> = cols.iter().map(|&c| br.column_name(c).to_vec()).collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_vec(),
                    });
                }
                out.push(fields);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn run(pipe: &PipeUniqLocal, blocks: &[Vec<Vec<Field>>]) -> Vec<Vec<Field>> {
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());
        for rows in blocks {
            let mut br = BlockResult::default();
            br.must_init_from_rows(rows);
            pp.write_block(0, &mut br);
        }
        pp.flush().unwrap();
        let out = sink.blocks.lock().unwrap();
        out.clone()
    }

    fn find<'a>(row: &'a [Field], name: &str) -> &'a str {
        row.iter()
            .find(|f| f.name == name.as_bytes())
            .map(|f| std::str::from_utf8(&f.value).unwrap())
            .unwrap_or("")
    }

    #[test]
    fn test_uniq_local_single_field_sums_hits() {
        let pu = new_pipe_uniq_local(vec![b"x".to_vec()], b"hits".to_vec(), 0);
        // Two blocks emit the same key "a" with hits 2 and 3 → merged 5.
        let out = run(
            &pu,
            &[
                vec![vec![field("x", "a"), field("hits", "2")]],
                vec![
                    vec![field("x", "a"), field("hits", "3")],
                    vec![field("x", "b"), field("hits", "1")],
                ],
            ],
        );
        let mut got: Vec<(String, String)> = out
            .iter()
            .map(|r| (find(r, "x").to_string(), find(r, "hits").to_string()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), "5".to_string()),
                ("b".to_string(), "1".to_string())
            ]
        );
    }

    #[test]
    fn test_uniq_local_two_fields() {
        let pu = new_pipe_uniq_local(vec![b"a".to_vec(), b"b".to_vec()], b"hits".to_vec(), 0);
        let out = run(
            &pu,
            &[vec![
                vec![field("a", "1"), field("b", "x"), field("hits", "4")],
                vec![field("a", "1"), field("b", "x"), field("hits", "6")],
                vec![field("a", "2"), field("b", "y"), field("hits", "1")],
            ]],
        );
        let mut got: Vec<(String, String, String)> = out
            .iter()
            .map(|r| {
                (
                    find(r, "a").to_string(),
                    find(r, "b").to_string(),
                    find(r, "hits").to_string(),
                )
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("1".to_string(), "x".to_string(), "10".to_string()),
                ("2".to_string(), "y".to_string(), "1".to_string()),
            ]
        );
    }

    #[test]
    fn test_uniq_local_limit_keeps_top_hits() {
        let pu = new_pipe_uniq_local(vec![b"x".to_vec()], b"hits".to_vec(), 2);
        let out = run(
            &pu,
            &[vec![
                vec![field("x", "a"), field("hits", "10")],
                vec![field("x", "b"), field("hits", "5")],
                vec![field("x", "c"), field("hits", "1")],
            ]],
        );
        assert_eq!(out.len(), 2);
        let mut kept: Vec<String> = out.iter().map(|r| find(r, "x").to_string()).collect();
        kept.sort();
        assert_eq!(kept, vec!["a", "b"]);
    }
}
