//! Port of `stats_row_max.go`: the `row_max(src[, fields...])` stats function,
//! which captures a whole row (the requested fields) at the maximum `src`.
//!
//! Mirror of [`crate::stats_row_min`]; see it for the shared-helper and
//! decoded-scan PORT NOTEs.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::{self, Filter};
use crate::rows::{Field, marshal_fields_to_json};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::{field_names_string, get_matching_columns, less_bytes};
use crate::stats_row_any::{fields_state_size, marshal_fields, unmarshal_fields};
use crate::stream_filter::quote_token_if_needed;

/// Port of `statsRowMax`.
pub(crate) struct StatsRowMax {
    src_field: String,
    field_filters: Vec<String>,
}

/// Port of `parseStatsRowMax`. The first parsed filter is the (non-wildcard)
/// source field; the remainder default to `["*"]` when empty.
pub(crate) fn new_stats_row_max(mut field_filters: Vec<String>) -> Result<StatsRowMax, String> {
    if field_filters.is_empty() {
        return Err("missing source field for 'row_max' func".to_string());
    }
    let src_field = field_filters.remove(0);
    if prefix_filter::is_wildcard_filter(&src_field) {
        return Err(format!("the source field {src_field:?} cannot be wildcard"));
    }
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    Ok(StatsRowMax {
        src_field,
        field_filters,
    })
}

impl StatsFunc for StatsRowMax {
    fn is_row_label(&self) -> bool {
        true
    }

    fn to_string(&self) -> String {
        let mut s = format!("row_max({}", quote_token_if_needed(&self.src_field));
        if !prefix_filter::match_all(&self.field_filters) {
            s.push_str(", ");
            s.push_str(&field_names_string(&self.field_filters));
        }
        s.push(')');
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
        pf.add_allow_filter(&self.src_field);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsRowMaxProcessor {
            src_field: self.src_field.clone(),
            field_filters: self.field_filters.clone(),
            max: Vec::new(),
            fields: Vec::new(),
        })
    }
}

/// Port of `statsRowMaxProcessor`.
pub(crate) struct StatsRowMaxProcessor {
    src_field: String,
    field_filters: Vec<String>,
    max: Vec<u8>,
    fields: Vec<Field>,
}

impl StatsRowMaxProcessor {
    fn need_update_state_string(&self, v: &[u8]) -> bool {
        if v.is_empty() {
            return false;
        }
        self.max.is_empty() || less_bytes(&self.max, v)
    }

    fn update_state(
        &mut self,
        v: &[u8],
        br: &mut BlockResult,
        field_filters: &[String],
        row_idx: usize,
    ) -> i64 {
        if !self.need_update_state_string(v) {
            return 0;
        }
        let mut delta = 0i64;
        delta -= self.max.len() as i64;
        delta += v.len() as i64;
        self.max = v.to_vec();

        for f in &self.fields {
            delta -= (f.name.len() + f.value.len()) as i64;
        }
        self.fields.clear();

        let cols = get_matching_columns(br, field_filters);
        for c in cols {
            let name = br.column_name(c).to_owned();
            let value = br.column_get_value_at_row(c, row_idx).to_owned();
            delta += (name.len() + value.len()) as i64;
            self.fields.push(Field { name, value });
        }

        delta
    }
}

impl StatsProcessor for StatsRowMaxProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let filters = self.field_filters.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        let src_vals: Vec<Vec<u8>> = br.column_get_values(c_src).to_vec();
        let mut inc = 0i64;
        for (i, v) in src_vals.iter().enumerate() {
            inc += self.update_state(v, br, &filters, i);
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let filters = self.field_filters.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        let v = br.column_get_value_at_row(c_src, row_index).to_owned();
        self.update_state(&v, br, &filters, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsRowMaxProcessor>()
            .expect("merge_state: other must be StatsRowMaxProcessor");
        if self.need_update_state_string(&src.max) {
            self.max = src.max.clone();
            self.fields = src.fields.clone();
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_bytes(dst, &self.max);
        marshal_fields(dst, &self.fields);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (max_value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot read maxValue".to_string());
        }
        let src = &src[n as usize..];
        self.max = max_value.unwrap_or_default().to_vec();

        let (fields, tail) =
            unmarshal_fields(src).map_err(|e| format!("cannot unmarshal fields: {e}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                tail.len()
            ));
        }
        self.fields = fields;

        Ok((self.max.len() + fields_state_size(&self.fields)) as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_fields_to_json(dst, &self.fields);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsRowMax*` (lexer) and
// `TestStatsRowMax` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn run(filters: Vec<&str>, blocks: &[Vec<Vec<Field>>]) -> String {
        let sf = new_stats_row_max(filters.iter().map(|s| s.to_string()).collect()).unwrap();
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
    fn test_row_max_captures_row() {
        let blocks = vec![
            vec![vec![field("a", "2"), field("b", "two")]],
            vec![vec![field("a", "1"), field("b", "one")]],
            vec![vec![field("a", "3"), field("b", "three")]],
        ];
        assert_eq!(run(vec!["a", "b"], &blocks), r#"{"b":"three"}"#);
    }

    #[test]
    fn test_row_max_rejects_missing_and_wildcard_src() {
        assert!(new_stats_row_max(vec![]).is_err());
        assert!(new_stats_row_max(vec!["*".to_string()]).is_err());
    }

    #[test]
    fn test_row_max_roundtrip() {
        let sf = new_stats_row_max(vec!["a".into(), "b".into()]).unwrap();
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(&[vec![field("a", "4"), field("b", "four")]]);
        sp.update_stats_for_all_rows(&sf, &mut br);

        let mut buf = Vec::new();
        sp.export_state(&mut buf, None);
        let mut sp2 = sf.new_stats_processor();
        sp2.import_state(&buf, None).unwrap();
        let mut dst = Vec::new();
        sp2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), r#"{"b":"four"}"#);
    }
}
