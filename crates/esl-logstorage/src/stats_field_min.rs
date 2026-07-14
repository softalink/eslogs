//! Port of `stats_field_min.go`: the `field_min(src, field)` stats function,
//! which tracks the value of `field` in the row holding the minimum `src`.
//!
//! See [`crate::stats_min`] for the shared helpers and the config-capture PORT
//! NOTE.
//!
//! Go's `_time`-source fast path is ported, including its quirk: the block's
//! minimum timestamp comes from `getMinTimestamp` and the companion field is
//! read at row 0 of the winning block — not at the row that actually holds the
//! minimum timestamp.
//!
//! PORT NOTE: Go additionally gates the per-value scan on the column
//! `valueType` (dict/uint/int/float/ipv4/iso8601 min-value pre-checks); those
//! are perf-only — the plain scan below selects the same winner.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::less_bytes;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, try_parse_timestamp_rfc3339_nano,
};

/// Port of `statsFieldMin`.
pub(crate) struct StatsFieldMin {
    src_field: Vec<u8>,
    field_name: Vec<u8>,
}

/// Port of `parseStatsFieldMin`; expects exactly two args (src, field).
pub(crate) fn new_stats_field_min(args: Vec<Vec<u8>>) -> Result<StatsFieldMin, String> {
    if args.len() != 2 {
        return Err(format!(
            "unexpected number of arguments for 'field_min' func; got {} args; want 2; args={:?}",
            args.len(),
            args
        ));
    }
    Ok(StatsFieldMin {
        src_field: args[0].clone(),
        field_name: args[1].clone(),
    })
}

impl StatsFunc for StatsFieldMin {
    fn to_string(&self) -> String {
        format!(
            "field_min({}, {})",
            crate::parser::quote_token_bytes_if_needed(&self.src_field),
            crate::parser::quote_token_bytes_if_needed(&self.field_name)
        )
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filter(&self.field_name);
        pf.add_allow_filter(&self.src_field);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsFieldMinProcessor {
            src_field: self.src_field.clone(),
            field_name: self.field_name.clone(),
            min: Vec::new(),
            value: Vec::new(),
        })
    }
}

/// Port of `statsFieldMinProcessor`.
pub(crate) struct StatsFieldMinProcessor {
    src_field: Vec<u8>,
    field_name: Vec<u8>,
    min: Vec<u8>,
    value: Vec<u8>,
}

impl StatsFieldMinProcessor {
    fn need_update_state_string(&self, v: &[u8]) -> bool {
        if v.is_empty() {
            return false;
        }
        self.min.is_empty() || less_bytes(v, &self.min)
    }

    fn update_state(
        &mut self,
        v: &[u8],
        br: &mut BlockResult,
        field_name: &[u8],
        row_idx: usize,
    ) -> i64 {
        if !self.need_update_state_string(v) {
            return 0;
        }
        let mut delta = 0i64;
        delta -= self.min.len() as i64;
        delta += v.len() as i64;
        self.min = v.to_vec();

        let c = br.get_column_by_name(field_name);
        let value = br.column_get_value_at_row(c, row_idx).to_vec();
        delta -= self.value.len() as i64;
        delta += value.len() as i64;
        self.value = value;

        delta
    }
}

impl StatsProcessor for StatsFieldMinProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let field_name = self.field_name.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        if br.column_is_const(c_src) {
            let v = br.column_get_value_at_row(c_src, 0).to_owned();
            return self.update_state(&v, br, &field_name, 0);
        }
        if br.column_is_time(c_src) {
            // Go fast path: take the block minimum from `getMinTimestamp` and
            // read the companion field at row 0 (Go's quirk — not the row
            // holding the minimum).
            let timestamp = std::str::from_utf8(&self.min)
                .ok()
                .and_then(try_parse_timestamp_rfc3339_nano)
                .unwrap_or(i64::MAX);
            let min_timestamp = br.get_min_timestamp(timestamp);
            if min_timestamp >= timestamp {
                return 0;
            }
            let mut b = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut b, min_timestamp);
            return self.update_state(&b, br, &field_name, 0);
        }

        let src_vals: Vec<Vec<u8>> = br.column_get_values(c_src).to_vec();
        let mut inc = 0i64;
        for (i, v) in src_vals.iter().enumerate() {
            inc += self.update_state(v, br, &field_name, i);
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let field_name = self.field_name.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        let v = br.column_get_value_at_row(c_src, row_index).to_owned();
        self.update_state(&v, br, &field_name, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsFieldMinProcessor>()
            .expect("merge_state: other must be StatsFieldMinProcessor");
        if self.need_update_state_string(&src.min) {
            self.min = src.min.clone();
            self.value = src.value.clone();
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_bytes(dst, &self.min);
        encoding::marshal_bytes(dst, &self.value);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (min_value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal minValue".to_string());
        }
        let mut src = &src[n as usize..];
        self.min = min_value.unwrap_or_default().to_vec();

        let (value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal value".to_string());
        }
        src = &src[n as usize..];
        self.value = value.unwrap_or_default().to_vec();

        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail; len(tail)={}",
                src.len()
            ));
        }

        Ok((self.min.len() + self.value.len()) as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(&self.value);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsFieldMin*` (lexer) and
// `TestStatsFieldMin` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn run(src: &str, fname: &str, blocks: &[Vec<Vec<Field>>]) -> String {
        let sf =
            new_stats_field_min(vec![src.as_bytes().to_vec(), fname.as_bytes().to_vec()]).unwrap();
        let mut sp = sf.new_stats_processor();
        for block in blocks {
            let mut br = BlockResult::default();
            br.must_init_from_rows(block);
            sp.update_stats_for_all_rows(&sf, &mut br);
        }
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    #[test]
    fn test_field_min_picks_companion() {
        // field_min(a, b): min a is 1 in the {a:1, b:xx} row.
        let blocks = vec![
            vec![vec![field("a", "2"), field("b", "two")]],
            vec![vec![field("a", "1"), field("b", "one")]],
            vec![vec![field("a", "3"), field("b", "three")]],
        ];
        assert_eq!(run("a", "b", &blocks), "one");
    }

    #[test]
    fn test_field_min_requires_two_args() {
        assert!(new_stats_field_min(vec![b"a".to_vec()]).is_err());
        assert!(new_stats_field_min(vec!["a".into(), "b".into(), "c".into()]).is_err());
    }

    /// Go `TestStatsFieldMin` case `stats field_min(foo, a)`: a missing source
    /// field yields an empty result.
    #[test]
    fn test_field_min_missing_src_field() {
        let blocks = vec![vec![
            vec![field("_msg", "abc"), field("a", "2"), field("b", "3")],
            vec![field("_msg", "def"), field("a", "1")],
            vec![field("a", "3"), field("b", "54")],
        ]];
        assert_eq!(run("foo", "a", &blocks), "");
    }

    /// `_time`-source fast path over storage-backed blocks: `field_min(_time,
    /// b)` / `field_max(_time, b)` must take the block min/max timestamp from
    /// the timestamps header and read the companion at row 0 / the last row
    /// (Go's quirk; rows within a block are timestamp-sorted, so these are
    /// also the true min/max rows).
    #[test]
    fn test_field_min_max_time_source_end_to_end() {
        use std::sync::{Arc, Mutex};

        use crate::log_rows::get_log_rows;
        use crate::parser::ParseQuery;
        use crate::storage::{Storage, StorageConfig};
        use crate::storage_search::{DataBlock, WriteDataBlockFn};
        use crate::tenant_id::TenantID;

        let path = std::env::temp_dir().join(format!(
            "esl-logstorage-fieldminmax-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let tenant = TenantID {
            account_id: 0,
            project_id: 0,
        };

        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        for i in 0..10 {
            let mut fields = vec![
                field("_msg", &format!("message {i}")),
                field("host", "node-1"),
                field("b", &format!("b{i}")),
            ];
            lr.must_add(tenant, base + i as i64, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();

        let q = ParseQuery("* | stats field_min(_time, b) as xmin, field_max(_time, b) as xmax")
            .expect("parse query");
        let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
            let n = db.rows_count();
            let columns = db.get_columns(false).to_vec();
            let mut out = cap.lock().unwrap();
            for i in 0..n {
                for c in &columns {
                    out.push((
                        String::from_utf8(c.name.clone()).unwrap(),
                        String::from_utf8_lossy(&c.values[i]).into_owned(),
                    ));
                }
            }
        });
        s.run_query(&[tenant], &q, write).expect("run_query");

        let rows = captured.lock().unwrap();
        let get = |name: &str| -> String {
            rows.iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing stats column {name}"))
        };
        assert_eq!(get("xmin"), "b0");
        assert_eq!(get("xmax"), "b9");
        drop(rows);

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_field_min_roundtrip_and_merge() {
        let sf = new_stats_field_min(vec!["a".into(), "b".into()]).unwrap();
        let mut a = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(&[vec![field("a", "5"), field("b", "five")]]);
        a.update_stats_for_all_rows(&sf, &mut br);

        let mut buf = Vec::new();
        a.export_state(&mut buf, None);
        let mut a2 = sf.new_stats_processor();
        a2.import_state(&buf, None).unwrap();

        let mut b = sf.new_stats_processor();
        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "2"), field("b", "two")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a2.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "two");
    }
}
