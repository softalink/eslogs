//! Port of `pipe_field_values_local.go` — the cluster-local half of
//! `field_values`, which merges `(value, hits)` pairs received from the remote
//! storage nodes and emits the top values ordered by hit count. Also hosts the
//! shared [`merge_values_with_hits`] helper (Go `MergeValuesWithHits`).

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use esl_common::stringsutil::less_natural;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{marshal_uint64_string, try_parse_uint64_bytes};

/// Port of Go `ValueWithHits` — declared in `storage_search.go` upstream; the
/// canonical port lives in [`crate::storage_search`] and is re-used here.
pub use crate::storage_search::ValueWithHits;

/// `pipeFieldValuesLocal` processes the local part of `field_values`.
///
/// PORT NOTE: Go wraps `*pipeFieldValues`; since `pipe_field_values` is not yet
/// ported, this struct stores only the two fields it actually reads — `field`
/// and `limit`. `filter` is not used by the local processor.
pub struct PipeFieldValuesLocal {
    pub(crate) field: Vec<u8>,
    pub(crate) limit: u64,
}

/// Constructs a `field_values_local` pipe from already-parsed components.
///
/// This pipe is produced only by `pipeFieldValues.splitToRemoteAndLocal`
/// (the cluster split); the constructor takes the parsed `field` and `limit`
/// directly.
pub(crate) fn new_pipe_field_values_local(field: Vec<u8>, limit: u64) -> PipeFieldValuesLocal {
    PipeFieldValuesLocal { field, limit }
}

impl PipeFieldValuesLocal {
    /// Port of Go `(*pipeFieldValues).getHitsFieldName`.
    fn get_hits_field_name(&self) -> Vec<u8> {
        get_unique_result_name(b"hits", std::slice::from_ref(&self.field))
    }
}

impl Pipe for PipeFieldValuesLocal {
    /// Port of Go `pipeFieldValuesLocal.splitToRemoteAndLocal`: this pipe is only
    /// ever produced by a split, so splitting it again is a bug.
    fn split_to_remote_and_local(&self, _timestamp: i64) -> crate::pipe::SplitPipesResult {
        esl_common::panicf!("BUG: unexpected call for pipeFieldValuesLocal");
        unreachable!()
    }

    fn to_string(&self) -> String {
        let mut s = format!(
            "field_values_local {}",
            crate::parser::quote_token_bytes_if_needed(&self.field)
        );
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    // Go: canLiveTail() == false, canReturnLastNResults() == false — both match
    // the trait defaults, so they are not overridden here.

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
        pf.add_allow_filter(&self.field);
        let hits_field_name = self.get_hits_field_name();
        pf.add_allow_filter(&hits_field_name);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeFieldValuesLocalProcessorShard::default()))
            .collect();
        Arc::new(PipeFieldValuesLocalProcessor {
            field: self.field.clone(),
            limit: self.limit,
            hits_field_name: self.get_hits_field_name(),
            pp_next,
            shards,
        })
    }
}

struct PipeFieldValuesLocalProcessor {
    field: Vec<u8>,
    limit: u64,
    hits_field_name: Vec<u8>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeFieldValuesLocalProcessorShard>>,
}

#[derive(Default)]
struct PipeFieldValuesLocalProcessorShard {
    vhs: Vec<ValueWithHits>,
}

impl PipeProcessor for PipeFieldValuesLocalProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        let c_values = br.get_column_by_name(&self.field);
        let c_hits = br.get_column_by_name(&self.hits_field_name);

        // Release the mutable borrow on br before touching the shard.
        let values: Vec<Vec<u8>> = br.column_get_values(c_values).to_vec();
        let hits: Vec<Vec<u8>> = br.column_get_values(c_hits).to_vec();

        let mut shard = self.shards[worker_id].lock().unwrap();
        for (i, value) in values.iter().enumerate() {
            let hits64 = try_parse_uint64_bytes(&hits[i]).unwrap_or_else(|| {
                panic!(
                    "BUG: unexpected hits received from the remote storage for {:?}: {:?}; it must be uint64",
                    String::from_utf8_lossy(value),
                    String::from_utf8_lossy(&hits[i]),
                )
            });
            shard.vhs.push(ValueWithHits {
                value: value.clone(),
                hits: hits64,
            });
        }
    }

    fn flush(&self) -> Result<(), String> {
        let mut a: Vec<Vec<ValueWithHits>> = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            a.push(shard.vhs.clone());
        }
        let result = merge_values_with_hits(a, self.limit, true);

        let fields = vec![self.field.clone(), self.hits_field_name.clone()];
        let mut wctx = PipeFixedFieldsWriteContext::new(self.pp_next.as_ref(), fields);

        for vh in &result {
            let mut hits_buf = Vec::new();
            marshal_uint64_string(&mut hits_buf, vh.hits);
            wctx.write_row(&[vh.value.clone(), hits_buf]);
        }
        wctx.flush();

        Ok(())
    }
}

/// Port of Go `getUniqueResultName` (parser.go).
///
/// PORT NOTE: duplicated here (private) because `parser.rs` — where this helper
/// belongs — is deferred.
fn get_unique_result_name(result_name: &[u8], by_fields: &[Vec<u8>]) -> Vec<u8> {
    let mut name = result_name.to_vec();
    while by_fields.iter().any(|f| f == &name) {
        name.push(b's');
    }
    name
}

/// Merges `a` entries and applies `limit` to the number of returned entries.
///
/// If `reset_hits_on_limit_exceeded` is true and the number of merged entries
/// exceeds `limit`, then hits are zeroed in the returned response.
///
/// Port of Go `MergeValuesWithHits`.
pub fn merge_values_with_hits(
    a: Vec<Vec<ValueWithHits>>,
    limit: u64,
    reset_hits_on_limit_exceeded: bool,
) -> Vec<ValueWithHits> {
    let mut need_reset_hits = false;
    let mut m: HashMap<Vec<u8>, u64> = HashMap::new();
    for vhs in &a {
        if !need_reset_hits && has_zero_hits(vhs) {
            need_reset_hits = true;
        }
        for vh in vhs {
            *m.entry(vh.value.clone()).or_insert(0) += vh.hits;
        }
    }

    let mut result: Vec<ValueWithHits> = m
        .into_iter()
        .map(|(value, hits)| ValueWithHits { value, hits })
        .collect();

    if need_reset_hits {
        reset_hits(&mut result);
    }

    sort_values_with_hits(&mut result);

    if limit > 0 && result.len() as u64 > limit {
        if reset_hits_on_limit_exceeded {
            reset_hits(&mut result);
            sort_values_with_hits(&mut result);
        }
        result.truncate(limit as usize);
    }

    result
}

/// Sorts in descending order of hits and ascending natural order of values for
/// identical hits. Port of Go `sortValuesWithHits`.
pub(crate) fn sort_values_with_hits(vhs: &mut [ValueWithHits]) {
    vhs.sort_by(|a, b| {
        if a.hits == b.hits {
            // PORT NOTE: Go's lessNatural operates on raw bytes; the Rust
            // helper needs &str, so invalid-UTF-8 values fall back to plain
            // byte ordering (identical for valid UTF-8).
            match (std::str::from_utf8(&a.value), std::str::from_utf8(&b.value)) {
                (Ok(av), Ok(bv)) => {
                    if less_natural(av, bv) {
                        std::cmp::Ordering::Less
                    } else if less_natural(bv, av) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    }
                }
                _ => a.value.cmp(&b.value),
            }
        } else {
            b.hits.cmp(&a.hits)
        }
    });
}

/// Port of Go `resetHits`.
pub(crate) fn reset_hits(vhs: &mut [ValueWithHits]) {
    for vh in vhs.iter_mut() {
        vh.hits = 0;
    }
}

fn has_zero_hits(vhs: &[ValueWithHits]) -> bool {
    vhs.iter().any(|vh| vh.hits == 0)
}

/// Port of Go `pipeFixedFieldsWriteContext` (pipe_uniq_local.go).
///
/// PORT NOTE: reproduced locally (private) because `pipe_uniq_local` is not yet
/// ported. It should move to a shared location once that module lands.
struct PipeFixedFieldsWriteContext<'a> {
    pp_next: &'a dyn PipeProcessor,
    fields: Vec<Vec<u8>>,
    rcs: Vec<ResultColumn>,
    br: BlockResult,
    rows_count: usize,
    values_len: usize,
}

impl<'a> PipeFixedFieldsWriteContext<'a> {
    fn new(pp_next: &'a dyn PipeProcessor, fields: Vec<Vec<u8>>) -> Self {
        let rcs = fields
            .iter()
            .map(|name| ResultColumn {
                name: name.to_vec(),
                values: Vec::new(),
            })
            .collect();
        Self {
            pp_next,
            fields,
            rcs,
            br: BlockResult::default(),
            rows_count: 0,
            values_len: 0,
        }
    }

    fn write_row(&mut self, row_values: &[Vec<u8>]) {
        for (i, v) in row_values.iter().enumerate() {
            self.rcs[i].add_value(v);
            self.values_len += v.len();
        }
        self.rows_count += 1;
        // The 64_000 limit provides the best performance results.
        if self.values_len >= 64_000 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.rows_count == 0 {
            return;
        }
        // set_result_columns consumes the columns, so rebuild them (same names,
        // empty values) afterwards for any subsequent block — mirroring Go's
        // resetValues() while keeping the column names.
        let rcs = std::mem::take(&mut self.rcs);
        self.br.set_result_columns(rcs, self.rows_count);
        self.values_len = 0;
        self.rows_count = 0;
        self.pp_next.write_block(0, &mut self.br);
        self.br.reset();
        self.rcs = self
            .fields
            .iter()
            .map(|name| ResultColumn {
                name: name.to_vec(),
                values: Vec::new(),
            })
            .collect();
    }
}

// PORT NOTE: `pipe_field_values_local_test.go` contains only
// `TestMergeValuesWithHits` (no pipe behaviour or needed-fields test); it is
// ported below verbatim. The `test_..._update_needed_fields` test is
// port-added to cover the ported `update_needed_fields` / hits-field-name path.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::assert_needed_fields;

    fn vh(value: &str, hits: u64) -> ValueWithHits {
        ValueWithHits {
            value: value.as_bytes().to_vec(),
            hits,
        }
    }

    #[test]
    fn test_merge_values_with_hits() {
        // nil input
        let empty: Vec<ValueWithHits> = vec![];
        assert_eq!(merge_values_with_hits(vec![], 0, false), empty);

        // no limit
        let a = vec![vec![vh("foo", 123), vh("bar", 32)], vec![vh("bar", 456)]];
        let expected = vec![vh("bar", 488), vh("foo", 123)];
        assert_eq!(merge_values_with_hits(a.clone(), 0, false), expected);
        assert_eq!(merge_values_with_hits(a, 0, true), expected);

        // no limit, zero hits
        let a = vec![vec![vh("foo", 123), vh("bar", 0)], vec![vh("bar", 13)]];
        let expected = vec![vh("bar", 0), vh("foo", 0)];
        assert_eq!(merge_values_with_hits(a.clone(), 0, false), expected);
        assert_eq!(merge_values_with_hits(a, 0, true), expected);

        // limit exceeded, no hits reset
        let a = vec![vec![vh("bar", 123)], vec![vh("foo", 33), vh("bar", 365)]];
        assert_eq!(merge_values_with_hits(a, 1, false), vec![vh("bar", 488)]);

        // limit exceeded, hits reset
        let a = vec![vec![vh("bar", 123)], vec![vh("foo", 33), vh("bar", 365)]];
        assert_eq!(merge_values_with_hits(a, 1, true), vec![vh("bar", 0)]);
    }

    #[test]
    fn test_pipe_field_values_local_update_needed_fields() {
        // getHitsFieldName("foo") == "hits" (no collision with the field name).
        let p = new_pipe_field_values_local(b"foo".to_vec(), 0);
        assert_needed_fields(&p, "*", "", "foo,hits", "");
        assert_needed_fields(&p, "a,b", "", "foo,hits", "");
    }
}
