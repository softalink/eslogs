//! Port of `pipe_top.go` — the `| top [N] by (...) [hits as ...] [rank ...]`
//! pipe: emits the N most frequent by-field combinations with their hit counts
//! (and optional rank).
//!
//! # Self-contained hits map
//! `top` shares Go's unported `hitsMap` with `uniq`/`facets`; it is
//! reimplemented here as [`HitsMap`], copied from `pipe_uniq.rs` (the filter is
//! always empty for `top`). See that file for the shape rationale.
//!
//! # PORT NOTES
//! * Go selects the top entries with a bounded binary min-heap
//!   (`getTopEntries`); this port collects all entries and does a full sort +
//!   truncate — identical result, `O(n log n)` instead of `O(n log limit)`.
//! * `valueTypeDict` (`forEachDictValueWithHits`) is not exposed by
//!   `block_result.rs`; the dict column falls back to the generic run-length
//!   path (identical totals).
//! * `chunkedAllocator` / `stateSizeBudget` accounting is dropped; shard merge
//!   is sequential.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use esl_common::encoding::{marshal_bytes, unmarshal_bytes};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{
    ValueType, marshal_int64_string, marshal_uint64_string, try_parse_int64_bytes,
    try_parse_uint64_bytes, unmarshal_int64, unmarshal_uint8, unmarshal_uint16, unmarshal_uint32,
    unmarshal_uint64,
};

const PIPE_TOP_DEFAULT_LIMIT: u64 = 10;

/// A faithful reimplementation of Go's `hitsMap` (see `pipe_uniq.rs`).
pub(crate) struct HitsMap {
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

    fn passes_filter(&self, v: &[u8]) -> bool {
        // Byte-native strings.Contains (Go operates on raw bytes).
        self.filter.is_empty()
            || v.windows(self.filter.len())
                .any(|w| w == self.filter.as_bytes())
    }

    pub(crate) fn update_state_generic(&mut self, v: &[u8], hits: u64) {
        if !self.passes_filter(v) {
            return;
        }
        if let Some(n) = try_parse_uint64_bytes(v) {
            *self.u64s.entry(n).or_default() += hits;
            return;
        }
        if v.first() == Some(&b'-')
            && let Some(n) = try_parse_int64_bytes(v)
        {
            *self.negs.entry(n).or_default() += hits;
            return;
        }
        *self.strings.entry(v.to_vec()).or_default() += hits;
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

/// One top entry: the value key and its hit count (Go `pipeTopEntry`).
struct TopEntry {
    k: Vec<u8>,
    hits: u64,
}

impl TopEntry {
    fn less(&self, r: &TopEntry) -> bool {
        if self.hits == r.hits {
            self.k > r.k
        } else {
            self.hits < r.hits
        }
    }
}

fn is_equal_prev_row(col_values: &[Vec<Vec<u8>>], row_idx: usize) -> bool {
    if row_idx == 0 {
        return false;
    }
    for vals in col_values {
        if vals[row_idx - 1] != vals[row_idx] {
            return false;
        }
    }
    true
}

fn rank_field_name_string(name: &str) -> String {
    let mut s = " rank".to_string();
    if name != "rank" {
        s += " as ";
        s += name;
    }
    s
}

/// The `| top ...` pipe.
pub struct PipeTop {
    by_fields: Vec<String>,
    limit: u64,
    limit_str: String,
    hits_field_name: String,
    rank_field_name: String,
}

/// Builds a [`PipeTop`] (Go `parsePipeTop` result).
pub(crate) fn new_pipe_top(
    by_fields: Vec<String>,
    limit: u64,
    limit_str: String,
    hits_field_name: String,
    rank_field_name: String,
) -> PipeTop {
    PipeTop {
        by_fields,
        limit,
        limit_str,
        hits_field_name,
        rank_field_name,
    }
}

impl Pipe for PipeTop {
    /// Port of Go `pipeTop.splitToRemoteAndLocal`: every node counts hits per
    /// group; the local side sums them and selects the top groups.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        let hits_quoted = crate::stream_filter::quote_token_if_needed(&self.hits_field_name);
        let fields_quoted = crate::stats_count::field_names_string(&self.by_fields);

        let mut p_local_str = format!(
            "stats by ({fields_quoted}) sum({hits_quoted}) as {hits_quoted} | first {} by ({hits_quoted} desc, {fields_quoted})",
            self.limit
        );
        if !self.rank_field_name.is_empty() {
            p_local_str += &rank_field_name_string(&self.rank_field_name);
        }
        p_local_str += &format!(" | fields {fields_quoted}, {hits_quoted}");
        if !self.rank_field_name.is_empty() {
            p_local_str += ", ";
            p_local_str += &crate::stream_filter::quote_token_if_needed(&self.rank_field_name);
        }

        let ps_local = crate::pipe::must_parse_pipes(&p_local_str, timestamp);

        let p_remote_str = format!("stats by ({fields_quoted}) count() as {hits_quoted}");
        let p_remote = crate::pipe::must_parse_pipe(&p_remote_str, timestamp);

        (Some(p_remote), ps_local)
    }

    fn to_string(&self) -> String {
        let mut s = "top".to_string();
        if self.limit != PIPE_TOP_DEFAULT_LIMIT {
            s += " ";
            s += &self.limit_str;
        }
        s += &format!(" by ({})", self.by_fields.join(", "));
        if self.hits_field_name != "hits" {
            s += " hits as ";
            s += &self.hits_field_name;
        }
        if !self.rank_field_name.is_empty() {
            s += &rank_field_name_string(&self.rank_field_name);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
        pf.add_allow_filters(&self.by_fields);
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
        Arc::new(PipeTopProcessor {
            by_fields: Arc::new(self.by_fields.clone()),
            limit: self.limit,
            hits_field_name: self.hits_field_name.clone(),
            rank_field_name: self.rank_field_name.clone(),
            stop,
            pp_next,
            shards: (0..n).map(|_| Mutex::new(Shard::default())).collect(),
        })
    }
}

#[derive(Default)]
struct Shard {
    m: Option<HitsMap>,
    key_buf: Vec<u8>,
}

impl Shard {
    fn hits_map(&mut self) -> &mut HitsMap {
        self.m.get_or_insert_with(|| HitsMap::new(String::new()))
    }
}

struct PipeTopProcessor {
    by_fields: Arc<Vec<String>>,
    limit: u64,
    hits_field_name: String,
    rank_field_name: String,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<Shard>>,
}

impl PipeTopProcessor {
    fn write_block_into(&self, shard: &mut Shard, br: &mut BlockResult) {
        if self.by_fields.len() == 1 {
            let name = self.by_fields[0].clone();
            self.update_single_column(shard, br, &name);
            return;
        }

        let mut col_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(self.by_fields.len());
        for f in self.by_fields.iter() {
            let c = br.get_column_by_name(f);
            col_values.push(br.column_get_values(c).to_vec());
        }
        let rows_len = br.rows_len();
        if rows_len == 0 {
            return;
        }

        let mut key_buf = std::mem::take(&mut shard.key_buf);
        let mut hits: u64 = 1;
        for row_idx in 1..rows_len {
            if is_equal_prev_row(&col_values, row_idx) {
                hits += 1;
                continue;
            }
            key_buf.clear();
            for vals in &col_values {
                marshal_bytes(&mut key_buf, &vals[row_idx - 1]);
            }
            shard.hits_map().update_state_string(&key_buf, hits);
            hits = 1;
        }
        key_buf.clear();
        for vals in &col_values {
            marshal_bytes(&mut key_buf, &vals[rows_len - 1]);
        }
        shard.hits_map().update_state_string(&key_buf, hits);
        shard.key_buf = key_buf;
    }

    fn update_single_column(&self, shard: &mut Shard, br: &mut BlockResult, field_name: &str) {
        let c = br.get_column_by_name(field_name);
        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_vec();
            let rows_len = br.rows_len() as u64;
            shard.hits_map().update_state_generic(&v, rows_len);
            return;
        }
        let vt = br.column_value_type(c);
        match vt {
            ValueType::UINT8 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                if values.is_empty() {
                    return;
                }
                // Run-length hit accumulation, matching Go's uint8 fast path.
                let mut hits: u64 = 1;
                for row_idx in 1..values.len() {
                    if values[row_idx - 1] == values[row_idx] {
                        hits += 1;
                    } else {
                        let n = unmarshal_uint8(&values[row_idx - 1]) as u64;
                        shard.hits_map().update_state_uint64(n, hits);
                        hits = 1;
                    }
                }
                let n = unmarshal_uint8(&values[values.len() - 1]) as u64;
                shard.hits_map().update_state_uint64(n, hits);
            }
            ValueType::UINT16 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    shard
                        .hits_map()
                        .update_state_uint64(unmarshal_uint16(v) as u64, 1);
                }
            }
            ValueType::UINT32 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    shard
                        .hits_map()
                        .update_state_uint64(unmarshal_uint32(v) as u64, 1);
                }
            }
            ValueType::UINT64 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    shard.hits_map().update_state_uint64(unmarshal_uint64(v), 1);
                }
            }
            ValueType::INT64 => {
                let values = br.column_get_values_encoded(c).unwrap_or(&[]).to_vec();
                for v in &values {
                    shard.hits_map().update_state_int64(unmarshal_int64(v), 1);
                }
            }
            _ => {
                // DICT / STRING / everything else: generic run-length path.
                let values = br.column_get_values(c).to_vec();
                if values.is_empty() {
                    return;
                }
                let mut hits: u64 = 1;
                for row_idx in 1..values.len() {
                    if values[row_idx - 1] == values[row_idx] {
                        hits += 1;
                    } else {
                        let v = values[row_idx - 1].clone();
                        shard.hits_map().update_state_generic(&v, hits);
                        hits = 1;
                    }
                }
                let v = values[values.len() - 1].clone();
                shard.hits_map().update_state_generic(&v, hits);
            }
        }
    }

    fn write_output(&self, entries: &[TopEntry]) {
        let by_len = self.by_fields.len();
        let has_rank = !self.rank_field_name.is_empty();

        let mut rcs: Vec<ResultColumn> = self
            .by_fields
            .iter()
            .map(|name| ResultColumn {
                name: name.clone().into_bytes(),
                values: Vec::new(),
            })
            .collect();
        rcs.push(ResultColumn {
            name: self.hits_field_name.clone().into_bytes(),
            values: Vec::new(),
        });
        if has_rank {
            rcs.push(ResultColumn {
                name: self.rank_field_name.clone().into_bytes(),
                values: Vec::new(),
            });
        }

        let mut rows_count = 0usize;
        for (i, e) in entries.iter().enumerate() {
            if by_len == 1 {
                rcs[0].add_value(&e.k);
            } else {
                let mut rest: &[u8] = &e.k;
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
            }
            let mut hb = Vec::new();
            marshal_uint64_string(&mut hb, e.hits);
            rcs[by_len].add_value(&hb);
            if has_rank {
                let rank = format!("{}", i + 1);
                rcs[by_len + 1].add_value(rank.as_bytes());
            }
            rows_count += 1;
        }

        if rows_count == 0 {
            return;
        }
        let mut br = BlockResult::default();
        br.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(0, &mut br);
    }
}

impl PipeProcessor for PipeTopProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        self.write_block_into(&mut shard, br);
    }

    fn flush(&self) -> Result<(), String> {
        if self.limit == 0 {
            return Ok(());
        }

        let mut merged = HitsMap::new(String::new());
        for m in &self.shards {
            let shard = m.lock().unwrap();
            if let Some(hm) = &shard.m {
                merged.merge(hm);
            }
        }
        if self.stop.load(AtomicOrdering::SeqCst) {
            return Ok(());
        }

        let mut entries: Vec<TopEntry> = Vec::new();
        for (n, hits) in &merged.u64s {
            let mut b = Vec::new();
            marshal_uint64_string(&mut b, *n);
            entries.push(TopEntry { k: b, hits: *hits });
        }
        for (n, hits) in &merged.negs {
            let mut b = Vec::new();
            marshal_int64_string(&mut b, *n);
            entries.push(TopEntry { k: b, hits: *hits });
        }
        for (k, hits) in &merged.strings {
            entries.push(TopEntry {
                k: k.clone(),
                hits: *hits,
            });
        }

        // Sort so the largest (most hits, then smallest key) comes first,
        // matching Go's `sort.Slice(entries, |i,j| entries[j].less(entries[i]))`.
        entries.sort_by(|a, b| {
            if b.less(a) {
                Ordering::Less
            } else if a.less(b) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });
        if entries.len() as u64 > self.limit {
            entries.truncate(self.limit as usize);
        }

        self.write_output(&entries);
        Ok(())
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

    fn run(pipe: &PipeTop, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
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

    fn get<'a>(row: &'a [Field], name: &str) -> &'a str {
        row.iter()
            .find(|f| f.name == name.as_bytes())
            .map(|f| std::str::from_utf8(&f.value).unwrap())
            .unwrap_or("")
    }

    #[test]
    fn test_top_single_field_ordered_by_hits() {
        let pt = new_pipe_top(
            vec!["x".to_string()],
            10,
            String::new(),
            "hits".to_string(),
            String::new(),
        );
        let out = run(
            &pt,
            &[
                vec![field("x", "a")],
                vec![field("x", "a")],
                vec![field("x", "b")],
                vec![field("x", "c")],
            ],
        );
        // a=2 first; b and c tie at 1, smaller key first → b before c.
        assert_eq!(out.len(), 3);
        assert_eq!(get(&out[0], "x"), "a");
        assert_eq!(get(&out[0], "hits"), "2");
        assert_eq!(get(&out[1], "x"), "b");
        assert_eq!(get(&out[1], "hits"), "1");
        assert_eq!(get(&out[2], "x"), "c");
    }

    #[test]
    fn test_top_limit_truncates() {
        let pt = new_pipe_top(
            vec!["x".to_string()],
            1,
            "1".to_string(),
            "hits".to_string(),
            String::new(),
        );
        let out = run(
            &pt,
            &[
                vec![field("x", "a")],
                vec![field("x", "a")],
                vec![field("x", "b")],
            ],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], "x"), "a");
        assert_eq!(get(&out[0], "hits"), "2");
    }

    #[test]
    fn test_top_rank_field() {
        let pt = new_pipe_top(
            vec!["x".to_string()],
            10,
            String::new(),
            "hits".to_string(),
            "rank".to_string(),
        );
        let out = run(
            &pt,
            &[
                vec![field("x", "a")],
                vec![field("x", "a")],
                vec![field("x", "b")],
            ],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(get(&out[0], "rank"), "1");
        assert_eq!(get(&out[1], "rank"), "2");
    }

    #[test]
    fn test_top_two_fields() {
        let pt = new_pipe_top(
            vec!["a".to_string(), "b".to_string()],
            10,
            String::new(),
            "hits".to_string(),
            String::new(),
        );
        let out = run(
            &pt,
            &[
                vec![field("a", "1"), field("b", "x")],
                vec![field("a", "1"), field("b", "x")],
                vec![field("a", "2"), field("b", "y")],
            ],
        );
        assert_eq!(out.len(), 2);
        // most frequent combo is (1, x) with hits 2.
        assert_eq!(get(&out[0], "a"), "1");
        assert_eq!(get(&out[0], "b"), "x");
        assert_eq!(get(&out[0], "hits"), "2");
    }

    #[test]
    fn test_top_to_string() {
        let pt = new_pipe_top(
            vec!["x".to_string()],
            5,
            "5".to_string(),
            "hits".to_string(),
            String::new(),
        );
        assert_eq!(pt.to_string(), "top 5 by (x)");
    }
}
