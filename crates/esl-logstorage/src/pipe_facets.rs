//! Port of `pipe_facets.go` — the `| facets [N] [max_values_per_field N]
//! [max_value_len N] [keep_const_fields]` pipe: builds faceted-search stats
//! (top values per log field with hit counts).
//!
//! # PORT NOTES
//! * `valueTypeDict` uses `forEachDictValueWithHits`, which `block_result.rs`
//!   does not expose; dict columns fall back to the generic per-row path
//!   (identical hit totals, more map ops).
//! * `chunkedAllocator` / `stateSizeBudget` accounting is dropped; shard merge
//!   and the 64_000-byte chunked writer flush are collapsed into a single
//!   output block. The self-contained [`HitsMap`] mirrors the one in
//!   `pipe_uniq.rs` (Go's unported shared `hitsMap`).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{
    ValueType, marshal_int64_string, marshal_uint64_string, try_parse_int64, try_parse_uint64,
    unmarshal_int64, unmarshal_uint8, unmarshal_uint16, unmarshal_uint32, unmarshal_uint64,
};

const PIPE_FACETS_DEFAULT_LIMIT: u64 = 10;
const PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD: u64 = 1000;
const PIPE_FACETS_DEFAULT_MAX_VALUE_LEN: u64 = 128;

/// Hit counts keyed by canonical value form (u64 / negative-i64 / string).
/// Mirrors the self-contained map in `pipe_uniq.rs`.
struct HitsMap {
    u64s: HashMap<u64, u64>,
    negs: HashMap<i64, u64>,
    strings: HashMap<Vec<u8>, u64>,
}

impl HitsMap {
    fn new() -> Self {
        Self {
            u64s: HashMap::new(),
            negs: HashMap::new(),
            strings: HashMap::new(),
        }
    }

    fn entries_count(&self) -> u64 {
        (self.u64s.len() + self.negs.len() + self.strings.len()) as u64
    }

    fn update_state_generic(&mut self, v: &str, hits: u64) {
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

    fn update_state_uint64(&mut self, n: u64, hits: u64) {
        *self.u64s.entry(n).or_default() += hits;
    }

    fn update_state_int64(&mut self, n: i64, hits: u64) {
        if n >= 0 {
            *self.u64s.entry(n as u64).or_default() += hits;
            return;
        }
        *self.negs.entry(n).or_default() += hits;
    }

    fn merge(&mut self, other: &HitsMap) {
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

fn uint64_string_len(n: u64) -> usize {
    if n < 10 {
        1
    } else if n < 100 {
        2
    } else if n < 1_000 {
        3
    } else if n < 10_000 {
        4
    } else if n < 100_000 {
        5
    } else if n < 1_000_000 {
        6
    } else if n < 10_000_000 {
        7
    } else if n < 100_000_000 {
        8
    } else if n < 1_000_000_000 {
        9
    } else if n < 10_000_000_000 {
        10
    } else {
        20
    }
}

fn int64_string_len(n: i64) -> usize {
    if n >= 0 {
        uint64_string_len(n as u64)
    } else if n == i64::MIN {
        21
    } else {
        1 + uint64_string_len((-n) as u64)
    }
}

/// Per-field hit tracking. Fields with too many unique values or too-long
/// values are ignored.
struct FieldHits {
    m: HitsMap,
    must_ignore: bool,
}

impl FieldHits {
    fn new() -> Self {
        Self {
            m: HitsMap::new(),
            must_ignore: false,
        }
    }

    fn enable_ignore_field(&mut self) {
        self.m = HitsMap::new();
        self.must_ignore = true;
    }
}

fn update_state_generic(fhs: &mut FieldHits, v: &str, hits: u64, max_value_len: u64) {
    if v.is_empty() {
        // Empty per-field values cannot be counted meaningfully (blocks without
        // the field are not included), so they are ignored.
        return;
    }
    if v.len() as u64 > max_value_len {
        fhs.enable_ignore_field();
        return;
    }
    fhs.m.update_state_generic(v, hits);
}

fn update_state_uint64(fhs: &mut FieldHits, n: u64, max_value_len: u64) {
    if max_value_len <= 20 && uint64_string_len(n) as u64 > max_value_len {
        fhs.enable_ignore_field();
        return;
    }
    fhs.m.update_state_uint64(n, 1);
}

fn update_state_int64(fhs: &mut FieldHits, n: i64, max_value_len: u64) {
    if max_value_len <= 21 && int64_string_len(n) as u64 > max_value_len {
        fhs.enable_ignore_field();
        return;
    }
    fhs.m.update_state_int64(n, 1);
}

/// The `| facets ...` pipe.
pub struct PipeFacets {
    limit: u64,
    max_values_per_field: u64,
    max_value_len: u64,
    keep_const_fields: bool,
}

/// Builds a [`PipeFacets`] (Go `parsePipeFacets` result).
pub(crate) fn new_pipe_facets(
    limit: u64,
    max_values_per_field: u64,
    max_value_len: u64,
    keep_const_fields: bool,
) -> PipeFacets {
    PipeFacets {
        limit,
        max_values_per_field,
        max_value_len,
        keep_const_fields,
    }
}

impl Pipe for PipeFacets {
    /// Port of Go `pipeFacets.splitToRemoteAndLocal`: every node returns all
    /// its facet hits (no limit); the local side sums, filters and re-limits
    /// them.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        let p_remote = PipeFacets {
            limit: u64::MAX,
            max_values_per_field: self.max_values_per_field,
            max_value_len: self.max_value_len,
            keep_const_fields: self.keep_const_fields,
        };

        let ps_local_str = format!(
            "stats by (field_name, field_value) sum(hits) as hits
	        | total_stats by (field_name) count() as field_values_count
		| filter field_values_count:<={}
		| delete field_values_count
		| sort by (hits desc) limit {} partition by (field_name)
		| sort by (field_name, hits desc, field_value)
		| fields field_name, field_value, hits",
            self.max_values_per_field, self.limit
        );
        let ps_local = crate::pipe::must_parse_pipes(&ps_local_str, timestamp);

        (Some(Box::new(p_remote)), ps_local)
    }

    fn to_string(&self) -> String {
        let mut s = "facets".to_string();
        if self.limit != PIPE_FACETS_DEFAULT_LIMIT {
            s += &format!(" {}", self.limit);
        }
        if self.max_values_per_field != PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD {
            s += &format!(" max_values_per_field {}", self.max_values_per_field);
        }
        if self.max_value_len != PIPE_FACETS_DEFAULT_MAX_VALUE_LEN {
            s += &format!(" max_value_len {}", self.max_value_len);
        }
        if self.keep_const_fields {
            s += " keep_const_fields";
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("*");
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
        Arc::new(PipeFacetsProcessor {
            limit: self.limit,
            max_values_per_field: self.max_values_per_field,
            max_value_len: self.max_value_len,
            keep_const_fields: self.keep_const_fields,
            stop,
            pp_next,
            shards: (0..n).map(|_| Mutex::new(Shard::default())).collect(),
        })
    }
}

#[derive(Default)]
struct Shard {
    m: HashMap<String, FieldHits>,
    rows_total: u64,
}

impl Shard {
    fn get_field_hits(&mut self, name: &str) -> &mut FieldHits {
        self.m
            .entry(name.to_string())
            .or_insert_with(FieldHits::new)
    }
}

struct PipeFacetsProcessor {
    limit: u64,
    max_values_per_field: u64,
    max_value_len: u64,
    keep_const_fields: bool,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<Shard>>,
}

impl PipeFacetsProcessor {
    fn update_facets_for_column(
        &self,
        shard: &mut Shard,
        br: &mut BlockResult,
        c: crate::block_result::ColRef,
    ) {
        let name = br.column_name(c).to_string();
        let max_values_per_field = self.max_values_per_field;
        let max_value_len = self.max_value_len;

        {
            let fhs = shard.get_field_hits(&name);
            if fhs.must_ignore {
                return;
            }
            if fhs.m.entries_count() > max_values_per_field {
                fhs.enable_ignore_field();
                return;
            }
        }

        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_string();
            let rows_len = br.rows_len() as u64;
            let fhs = shard.get_field_hits(&name);
            update_state_generic(fhs, &v, rows_len, max_value_len);
            return;
        }

        let vt = br.column_value_type(c);
        match vt {
            ValueType::UINT8 | ValueType::UINT16 | ValueType::UINT32 | ValueType::UINT64 => {
                let values = br
                    .column_get_values_encoded(c)
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                let fhs = shard.get_field_hits(&name);
                for v in &values {
                    let n = match vt {
                        ValueType::UINT8 => unmarshal_uint8(v) as u64,
                        ValueType::UINT16 => unmarshal_uint16(v) as u64,
                        ValueType::UINT32 => unmarshal_uint32(v) as u64,
                        _ => unmarshal_uint64(v),
                    };
                    update_state_uint64(fhs, n, max_value_len);
                }
            }
            ValueType::INT64 => {
                let values = br
                    .column_get_values_encoded(c)
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                let fhs = shard.get_field_hits(&name);
                for v in &values {
                    update_state_int64(fhs, unmarshal_int64(v), max_value_len);
                }
            }
            _ => {
                // DICT and STRING (and everything else) go per-row.
                let rows_len = br.rows_len();
                for i in 0..rows_len {
                    let v = br.column_get_value_at_row(c, i).to_string();
                    let fhs = shard.get_field_hits(&name);
                    update_state_generic(fhs, &v, 1, max_value_len);
                }
            }
        }
    }
}

impl PipeProcessor for PipeFacetsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        let cols = br.get_columns();
        for c in cols {
            self.update_facets_for_column(&mut shard, br, c);
        }
        shard.rows_total += br.rows_len() as u64;
    }

    fn flush(&self) -> Result<(), String> {
        // Merge shard state per field, honoring must_ignore.
        let mut ignore: HashSet<String> = HashSet::new();
        let mut merged: HashMap<String, HitsMap> = HashMap::new();
        let mut rows_total: u64 = 0;

        for m in &self.shards {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let shard = m.lock().unwrap();
            for (name, fhs) in &shard.m {
                if ignore.contains(name) {
                    continue;
                }
                if fhs.must_ignore {
                    ignore.insert(name.clone());
                    merged.remove(name);
                    continue;
                }
                merged
                    .entry(name.clone())
                    .or_insert_with(HitsMap::new)
                    .merge(&fhs.m);
            }
            rows_total += shard.rows_total;
        }

        let mut field_names: Vec<String> = merged.keys().cloned().collect();
        field_names.sort();

        let mut rcs = vec![
            ResultColumn {
                name: "field_name".to_string(),
                values: Vec::new(),
            },
            ResultColumn {
                name: "field_value".to_string(),
                values: Vec::new(),
            },
            ResultColumn {
                name: "hits".to_string(),
                values: Vec::new(),
            },
        ];
        let mut rows_count = 0usize;

        for field_name in &field_names {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let hm = &merged[field_name];
            if hm.entries_count() > self.max_values_per_field {
                continue;
            }

            let mut vs: Vec<(Vec<u8>, u64)> = Vec::with_capacity(hm.entries_count() as usize);
            for (n, hits) in &hm.u64s {
                let mut b = Vec::new();
                marshal_uint64_string(&mut b, *n);
                vs.push((b, *hits));
            }
            for (n, hits) in &hm.negs {
                let mut b = Vec::new();
                marshal_int64_string(&mut b, *n);
                vs.push((b, *hits));
            }
            for (k, hits) in &hm.strings {
                vs.push((k.clone(), *hits));
            }

            if vs.len() == 1 && vs[0].1 == rows_total && !self.keep_const_fields {
                // Skip fields with a constant value over all selected logs.
                continue;
            }

            vs.sort_by_key(|x| std::cmp::Reverse(x.1));
            if vs.len() as u64 > self.limit {
                vs.truncate(self.limit as usize);
            }

            for (value, hits) in vs {
                rcs[0].add_value(field_name.as_bytes());
                rcs[1].add_value(&value);
                let mut hb = Vec::new();
                marshal_uint64_string(&mut hb, hits);
                rcs[2].add_value(&hb);
                rows_count += 1;
            }
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

    fn run(pipe: &PipeFacets, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
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

    fn triple(r: &[Field]) -> (String, String, String) {
        (
            r.iter()
                .find(|f| f.name == "field_name")
                .unwrap()
                .value
                .clone(),
            r.iter()
                .find(|f| f.name == "field_value")
                .unwrap()
                .value
                .clone(),
            r.iter().find(|f| f.name == "hits").unwrap().value.clone(),
        )
    }

    #[test]
    fn test_facets_basic() {
        let pf = new_pipe_facets(
            PIPE_FACETS_DEFAULT_LIMIT,
            PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD,
            PIPE_FACETS_DEFAULT_MAX_VALUE_LEN,
            false,
        );
        let out = run(
            &pf,
            &[
                vec![field("level", "info"), field("host", "a")],
                vec![field("level", "info"), field("host", "b")],
                vec![field("level", "error"), field("host", "a")],
            ],
        );
        // "level": info=2, error=1 ; "host": a=2, b=1. None is const over all
        // rows, so all appear.
        let mut got: Vec<(String, String, String)> = out.iter().map(|r| triple(r)).collect();
        got.sort();
        assert!(got.contains(&("level".to_string(), "info".to_string(), "2".to_string())));
        assert!(got.contains(&("level".to_string(), "error".to_string(), "1".to_string())));
        assert!(got.contains(&("host".to_string(), "a".to_string(), "2".to_string())));
    }

    #[test]
    fn test_facets_skips_const_field() {
        let pf = new_pipe_facets(
            PIPE_FACETS_DEFAULT_LIMIT,
            PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD,
            PIPE_FACETS_DEFAULT_MAX_VALUE_LEN,
            false,
        );
        let out = run(
            &pf,
            &[
                vec![field("k", "same"), field("v", "1")],
                vec![field("k", "same"), field("v", "2")],
            ],
        );
        // "k" is constant over all rows -> skipped. Only "v" facets remain.
        for r in &out {
            assert_ne!(
                r.iter().find(|f| f.name == "field_name").unwrap().value,
                "k"
            );
        }
    }

    #[test]
    fn test_facets_keep_const_fields() {
        let pf = new_pipe_facets(
            PIPE_FACETS_DEFAULT_LIMIT,
            PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD,
            PIPE_FACETS_DEFAULT_MAX_VALUE_LEN,
            true,
        );
        let out = run(&pf, &[vec![field("k", "same")], vec![field("k", "same")]]);
        let got: Vec<(String, String, String)> = out.iter().map(|r| triple(r)).collect();
        assert!(got.contains(&("k".to_string(), "same".to_string(), "2".to_string())));
    }

    #[test]
    fn test_facets_max_value_len_ignores_field() {
        // max_value_len = 3 -> "toolong" (len 7) triggers ignore of field "x".
        let pf = new_pipe_facets(
            PIPE_FACETS_DEFAULT_LIMIT,
            PIPE_FACETS_DEFAULT_MAX_VALUES_PER_FIELD,
            3,
            true,
        );
        let out = run(
            &pf,
            &[
                vec![field("x", "toolong"), field("y", "ok")],
                vec![field("x", "alsolong"), field("y", "ok")],
            ],
        );
        for r in &out {
            assert_ne!(
                r.iter().find(|f| f.name == "field_name").unwrap().value,
                "x"
            );
        }
    }
}
