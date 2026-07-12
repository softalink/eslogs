//! Port of `pipe_uniq.go` — the `| uniq by (...) [filter ...] [with hits] [limit N]`
//! pipe: emits the unique combinations of the by-fields, optionally with a hits
//! count per combination.
//!
//! # Self-contained hits map
//! Go's `uniq`/`top`/`facets` share an unported `hitsMap` (three maps keyed by
//! u64 / negative-i64 / string, tracking hit counts). It is reimplemented here
//! as [`HitsMap`] — a faithful, non-sharded version (the "adaptive" concurrency
//! sharding is a perf optimization; correctness only needs one map per worker
//! merged in `flush`). `top`/`facets` embed their own copy of the same shape.
//!
//! # PORT NOTES
//! * `splitToRemoteAndLocal` (→ `pipe_uniq_local`) and the lexer parser are
//!   out of scope; a `pub(crate)` constructor is exposed instead.
//! * `valueTypeDict` uses `forEachDictValueWithHits`, which `block_result.rs`
//!   does not expose; the dict column falls back to the generic per-row path
//!   (identical totals, more map ops).
//! * `chunkedAllocator` / `stateSizeBudget` accounting is dropped; parallel
//!   shard merge is done sequentially.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use esl_common::encoding::{marshal_bytes, unmarshal_bytes};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{
    ValueType, marshal_int64_string, marshal_uint64_string, try_parse_int64, try_parse_uint64,
    unmarshal_int64, unmarshal_uint8, unmarshal_uint16, unmarshal_uint32, unmarshal_uint64,
};

/// A faithful reimplementation of Go's `hitsMap`: hit counts keyed by the
/// value's canonical form (u64 / negative-i64 / string bytes).
pub(crate) struct HitsMap {
    /// Optional substring filter; only values containing it are recorded.
    filter: String,
    pub u64s: HashMap<u64, u64>,
    pub negs: HashMap<i64, u64>,
    pub strings: HashMap<Vec<u8>, u64>,
}

impl HitsMap {
    pub(crate) fn new(filter: String) -> Self {
        Self {
            filter,
            u64s: HashMap::new(),
            negs: HashMap::new(),
            strings: HashMap::new(),
        }
    }

    pub(crate) fn entries_count(&self) -> u64 {
        (self.u64s.len() + self.negs.len() + self.strings.len()) as u64
    }

    fn passes_filter(&self, v: &str) -> bool {
        self.filter.is_empty() || v.contains(&self.filter)
    }

    pub(crate) fn update_state_generic(&mut self, v: &str, hits: u64) {
        if !self.passes_filter(v) {
            return;
        }
        if let Some(n) = try_parse_uint64(v) {
            *self.u64s.entry(n).or_default() += hits;
            return;
        }
        if v.as_bytes().first() == Some(&b'-')
            && let Some(n) = try_parse_int64(v)
        {
            *self.negs.entry(n).or_default() += hits;
            return;
        }
        *self.strings.entry(v.as_bytes().to_vec()).or_default() += hits;
    }

    pub(crate) fn update_state_string(&mut self, key: &[u8], hits: u64) {
        *self.strings.entry(key.to_vec()).or_default() += hits;
    }

    pub(crate) fn update_state_uint64(&mut self, n: u64, hits: u64) {
        if !self.filter.is_empty() && !n.to_string().contains(&self.filter) {
            return;
        }
        *self.u64s.entry(n).or_default() += hits;
    }

    pub(crate) fn update_state_int64(&mut self, n: i64, hits: u64) {
        if n >= 0 {
            self.update_state_uint64(n as u64, hits);
            return;
        }
        if !self.filter.is_empty() && !n.to_string().contains(&self.filter) {
            return;
        }
        *self.negs.entry(n).or_default() += hits;
    }

    pub(crate) fn merge(&mut self, other: &HitsMap) {
        for (k, v) in &other.u64s {
            *self.u64s.entry(*k).or_default() += *v;
        }
        for (k, v) in &other.negs {
            *self.negs.entry(*k).or_default() += *v;
        }
        for (k, v) in &other.strings {
            *self.strings.entry(k.clone()).or_default() += *v;
        }
    }
}

fn bytes_to_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// The `| uniq ...` pipe.
pub struct PipeUniq {
    by_fields: Vec<String>,
    filter: String,
    hits_field_name: String,
    limit: u64,
}

/// Builds a [`PipeUniq`] (Go `parsePipeUniq` result).
pub(crate) fn new_pipe_uniq(
    by_fields: Vec<String>,
    filter: String,
    hits_field_name: String,
    limit: u64,
) -> PipeUniq {
    PipeUniq {
        by_fields,
        filter,
        hits_field_name,
        limit,
    }
}

impl Pipe for PipeUniq {
    /// Port of Go `pipeUniq.splitToRemoteAndLocal`: without hits the same
    /// `uniq` runs on both sides; with hits the local side merges per-node
    /// hits via `uniq_local`.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        if self.hits_field_name.is_empty() {
            return (
                Some(crate::pipe::clone_pipe(self, timestamp)),
                vec![crate::pipe::clone_pipe(self, timestamp)],
            );
        }

        let p_local = crate::pipe_uniq_local::new_pipe_uniq_local(
            self.by_fields.clone(),
            self.hits_field_name.clone(),
            self.limit,
        );
        (
            Some(crate::pipe::clone_pipe(self, timestamp)),
            vec![Box::new(p_local)],
        )
    }

    fn to_string(&self) -> String {
        let mut s = format!("uniq by ({})", self.by_fields.join(", "));
        if !self.filter.is_empty() {
            s += " filter ";
            s += &self.filter;
        }
        if !self.hits_field_name.is_empty() {
            s += " with hits";
        }
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
        pf.add_allow_filters(&self.by_fields);
    }

    /// Go `isLastPipeUniq`'s `*pipeUniq` type-switch arm.
    fn is_uniq_pipe(&self) -> bool {
        true
    }

    /// Go `getFieldNameFromPipes`' `*pipeUniq` arm.
    fn in_query_field_name(&self) -> Option<Result<String, String>> {
        if self.by_fields.len() != 1 {
            return Some(Err(format!(
                "'{}' pipe must contain only a single non-star field name",
                Pipe::to_string(self)
            )));
        }
        Some(Ok(self.by_fields[0].clone()))
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let n = concurrency.max(1);
        Arc::new(PipeUniqProcessor {
            by_fields: Arc::new(self.by_fields.clone()),
            hits_field_name: self.hits_field_name.clone(),
            limit: self.limit,
            stop,
            pp_next,
            shards: (0..n)
                .map(|_| Mutex::new(Shard::new(self.filter.clone())))
                .collect(),
        })
    }
}

struct Shard {
    m: HitsMap,
    key_buf: Vec<u8>,
}

impl Shard {
    fn new(filter: String) -> Self {
        Self {
            m: HitsMap::new(filter),
            key_buf: Vec::new(),
        }
    }
}

struct PipeUniqProcessor {
    by_fields: Arc<Vec<String>>,
    hits_field_name: String,
    limit: u64,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<Shard>>,
}

impl PipeUniqProcessor {
    fn write_block_into(&self, shard: &mut Shard, br: &mut BlockResult) -> bool {
        if self.limit > 0 && shard.m.entries_count() > self.limit {
            return false;
        }
        let need_hits = !self.hits_field_name.is_empty();

        if self.by_fields.len() == 1 {
            self.update_single_column(shard, br, &self.by_fields[0].clone(), need_hits);
            return true;
        }

        let mut col_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(self.by_fields.len());
        for f in self.by_fields.iter() {
            let c = br.get_column_by_name(f);
            col_values.push(br.column_get_values(c).to_vec());
        }
        let rows_len = br.rows_len();
        for i in 0..rows_len {
            // Skip rows whose by-field values equal the previous row's, unless
            // hits are requested (Go's seenValue dedup).
            let mut seen = i > 0;
            for vals in &col_values {
                if need_hits || i == 0 || vals[i - 1] != vals[i] {
                    seen = false;
                    break;
                }
            }
            if seen {
                continue;
            }
            shard.key_buf.clear();
            for vals in &col_values {
                marshal_bytes(&mut shard.key_buf, &vals[i]);
            }
            let key = std::mem::take(&mut shard.key_buf);
            shard.m.update_state_string(&key, 1);
            shard.key_buf = key;
        }
        true
    }

    fn update_single_column(
        &self,
        shard: &mut Shard,
        br: &mut BlockResult,
        column_name: &str,
        need_hits: bool,
    ) {
        let c = br.get_column_by_name(column_name);
        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_string();
            shard.m.update_state_generic(&v, br.rows_len() as u64);
            return;
        }
        let vt = br.column_value_type(c);
        match vt {
            ValueType::UINT8 | ValueType::UINT16 | ValueType::UINT32 | ValueType::UINT64 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    let n = match vt {
                        ValueType::UINT8 => unmarshal_uint8(v) as u64,
                        ValueType::UINT16 => unmarshal_uint16(v) as u64,
                        ValueType::UINT32 => unmarshal_uint32(v) as u64,
                        _ => unmarshal_uint64(v),
                    };
                    shard.m.update_state_uint64(n, 1);
                }
            }
            ValueType::INT64 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    shard.m.update_state_int64(unmarshal_int64(v), 1);
                }
            }
            _ => {
                // DICT and STRING (and everything else) go through the generic
                // per-row path.
                let values = br.column_get_values(c).to_vec();
                for (i, v) in values.iter().enumerate() {
                    if need_hits || i == 0 || values[i - 1] != *v {
                        shard.m.update_state_generic(bytes_to_str(v), 1);
                    }
                }
            }
        }
    }
}

impl PipeProcessor for PipeUniqProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        if !self.write_block_into(&mut shard, br) {
            self.stop.store(true, Ordering::SeqCst);
        }
    }

    fn flush(&self) -> Result<(), String> {
        // Merge all shard maps into one.
        let mut merged = HitsMap::new(String::new());
        for m in &self.shards {
            let shard = m.lock().unwrap();
            merged.merge(&shard.m);
        }
        if self.stop.load(Ordering::SeqCst) {
            return Ok(());
        }

        let mut reset_hits = false;
        if self.limit > 0 && merged.entries_count() > self.limit {
            // On exceeding the limit, hits become meaningless (arbitrary
            // entries dropped), so they are zeroed. Trim to the limit.
            reset_hits = true;
            trim_to_limit(&mut merged, self.limit);
        }

        self.write_output(&merged, reset_hits);
        Ok(())
    }
}

fn trim_to_limit(hm: &mut HitsMap, limit: u64) {
    let mut count = hm.entries_count();
    let drop_from = |m: &mut HashMap<u64, u64>, count: &mut u64| {
        let keys: Vec<u64> = m.keys().copied().collect();
        for k in keys {
            if *count <= limit {
                break;
            }
            m.remove(&k);
            *count -= 1;
        }
    };
    drop_from(&mut hm.u64s, &mut count);
    if count > limit {
        let keys: Vec<i64> = hm.negs.keys().copied().collect();
        for k in keys {
            if count <= limit {
                break;
            }
            hm.negs.remove(&k);
            count -= 1;
        }
    }
    if count > limit {
        let keys: Vec<Vec<u8>> = hm.strings.keys().cloned().collect();
        for k in keys {
            if count <= limit {
                break;
            }
            hm.strings.remove(&k);
            count -= 1;
        }
    }
}

impl PipeUniqProcessor {
    fn write_output(&self, hm: &HitsMap, reset_hits: bool) {
        let need_hits = !self.hits_field_name.is_empty();
        let by_len = self.by_fields.len();

        let mut rcs: Vec<ResultColumn> = self
            .by_fields
            .iter()
            .map(|name| ResultColumn {
                name: name.clone(),
                values: Vec::new(),
            })
            .collect();
        if need_hits {
            rcs.push(ResultColumn {
                name: self.hits_field_name.clone(),
                values: Vec::new(),
            });
        }

        let mut rows_count = 0usize;
        let push_hits = |rcs: &mut [ResultColumn], hits: u64| {
            if !need_hits {
                return;
            }
            let mut b = Vec::new();
            marshal_uint64_string(&mut b, if reset_hits { 0 } else { hits });
            rcs[by_len].add_value(&b);
        };

        if by_len == 1 {
            for (n, hits) in &hm.u64s {
                let mut b = Vec::new();
                marshal_uint64_string(&mut b, *n);
                rcs[0].add_value(&b);
                push_hits(&mut rcs, *hits);
                rows_count += 1;
            }
            for (n, hits) in &hm.negs {
                let mut b = Vec::new();
                marshal_int64_string(&mut b, *n);
                rcs[0].add_value(&b);
                push_hits(&mut rcs, *hits);
                rows_count += 1;
            }
            for (k, hits) in &hm.strings {
                rcs[0].add_value(k);
                push_hits(&mut rcs, *hits);
                rows_count += 1;
            }
        } else {
            for (k, hits) in &hm.strings {
                let mut rest: &[u8] = k;
                let mut idx = 0;
                while !rest.is_empty() && idx < by_len {
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
                push_hits(&mut rcs, *hits);
                rows_count += 1;
            }
        }

        if rows_count == 0 {
            return;
        }
        let mut br = BlockResult::default();
        br.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(0, &mut br);
    }
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
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_string(),
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
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn run(pipe: &PipeUniq, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());
        let mut br = BlockResult::default();
        br.must_init_from_rows(rows);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();
        let out = sink.blocks.lock().unwrap();
        out.clone()
    }

    #[test]
    fn test_uniq_single_field() {
        let pu = new_pipe_uniq(vec!["x".to_string()], String::new(), String::new(), 0);
        let out = run(
            &pu,
            &[
                vec![field("x", "a")],
                vec![field("x", "b")],
                vec![field("x", "a")],
            ],
        );
        let mut vals: Vec<String> = out.iter().map(|r| r[0].value.clone()).collect();
        vals.sort();
        assert_eq!(vals, vec!["a", "b"]);
    }

    #[test]
    fn test_uniq_with_hits() {
        let pu = new_pipe_uniq(vec!["x".to_string()], String::new(), "hits".to_string(), 0);
        let out = run(
            &pu,
            &[
                vec![field("x", "a")],
                vec![field("x", "a")],
                vec![field("x", "b")],
            ],
        );
        let mut got: Vec<(String, String)> = out
            .iter()
            .map(|r| {
                let x = r.iter().find(|f| f.name == "x").unwrap().value.clone();
                let h = r.iter().find(|f| f.name == "hits").unwrap().value.clone();
                (x, h)
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("a".to_string(), "2".to_string()),
                ("b".to_string(), "1".to_string())
            ]
        );
    }

    #[test]
    fn test_uniq_two_fields() {
        let pu = new_pipe_uniq(
            vec!["a".to_string(), "b".to_string()],
            String::new(),
            String::new(),
            0,
        );
        let out = run(
            &pu,
            &[
                vec![field("a", "1"), field("b", "x")],
                vec![field("a", "1"), field("b", "x")],
                vec![field("a", "2"), field("b", "y")],
            ],
        );
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn test_uniq_filter_substring() {
        let pu = new_pipe_uniq(vec!["x".to_string()], "foo".to_string(), String::new(), 0);
        let out = run(
            &pu,
            &[
                vec![field("x", "foobar")],
                vec![field("x", "baz")],
                vec![field("x", "afoo")],
            ],
        );
        let mut vals: Vec<String> = out.iter().map(|r| r[0].value.clone()).collect();
        vals.sort();
        assert_eq!(vals, vec!["afoo", "foobar"]);
    }
}
