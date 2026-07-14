//! Port of `stats_max.go`: the `max(...)` stats function.
//!
//! Mirrors [`crate::stats_min`]; see that module for the shared helpers and the
//! PORT NOTEs on config capture and the dropped column fast paths.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::{field_names_string, get_matching_columns, less_bytes};

/// Port of `statsMax`.
pub(crate) struct StatsMax {
    field_filters: Vec<String>,
}

/// Port of `parseStatsMax`. Empty filters default to `["*"]`.
pub(crate) fn new_stats_max(mut field_filters: Vec<String>) -> StatsMax {
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    StatsMax { field_filters }
}

impl StatsFunc for StatsMax {
    fn to_string(&self) -> String {
        format!("max({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsMaxProcessor {
            field_filters: self.field_filters.clone(),
            max: Vec::new(),
            has_items: false,
        })
    }
}

/// Port of `statsMaxProcessor`.
pub(crate) struct StatsMaxProcessor {
    field_filters: Vec<String>,
    max: Vec<u8>,
    has_items: bool,
}

impl StatsMaxProcessor {
    fn needs_update_state(&self, v: &[u8]) -> bool {
        !self.has_items || less_bytes(&self.max, v)
    }

    fn set_state(&mut self, v: &[u8]) {
        self.max = v.to_vec();
        self.has_items = true;
    }

    fn update_state_string(&mut self, v: &[u8]) {
        if self.needs_update_state(v) {
            self.set_state(v);
        }
    }
}

impl StatsProcessor for StatsMaxProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let max_len = self.max.len();
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            let values = br.column_get_values(c);
            for v in values {
                if self.needs_update_state(v) {
                    self.set_state(v);
                }
            }
        }
        self.max.len() as i64 - max_len as i64
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let max_len = self.max.len();
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            let v = br.column_get_value_at_row(c, row_index);
            if self.needs_update_state(v) {
                self.set_state(v);
            }
        }
        // PORT NOTE: Go returns `maxLen - len(smp.max)` here; kept verbatim.
        max_len as i64 - self.max.len() as i64
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsMaxProcessor>()
            .expect("merge_state: other must be StatsMaxProcessor");
        if src.has_items {
            self.update_state_string(&src.max);
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        if !self.has_items {
            dst.push(0);
            return;
        }
        dst.push(1);
        encoding::marshal_bytes(dst, &self.max);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        if src.is_empty() {
            return Err("missing `hasItems`".to_string());
        }
        self.has_items = src[0] == 1;
        let mut src = &src[1..];

        if self.has_items {
            let (max_value, n) = encoding::unmarshal_bytes(src);
            if n <= 0 {
                return Err("cannot unmarshal max value".to_string());
            }
            self.max = max_value.unwrap_or_default().to_vec();
            src = &src[n as usize..];
        } else {
            self.max = Vec::new();
        }

        if !src.is_empty() {
            return Err(format!(
                "unexpected tail left after decoding max value; len(tail)={}",
                src.len()
            ));
        }

        Ok(self.max.len() as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(&self.max);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsMax*` (lexer) and `TestStatsMax`
// (`expectPipeResults`). Pure computation covered below.
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

    fn run_max(filters: &[&str], blocks: &[Vec<Vec<Field>>]) -> String {
        let sf = new_stats_max(filters.iter().map(|s| s.to_string()).collect());
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

    fn sample_blocks() -> Vec<Vec<Vec<Field>>> {
        vec![
            vec![vec![field("_msg", "abc"), field("a", "2"), field("b", "3")]],
            vec![vec![field("_msg", "def"), field("a", "1")]],
            vec![vec![field("a", "3"), field("b", "54")]],
        ]
    }

    #[test]
    fn test_stats_max_single_field() {
        assert_eq!(run_max(&["a"], &sample_blocks()), "3");
        assert_eq!(run_max(&["b"], &sample_blocks()), "54");
    }

    #[test]
    fn test_stats_max_wildcard() {
        // max(*): among abc, def, 2, 3, 1, 54 -> "def" (non-numeric sorts above numbers)
        assert_eq!(run_max(&["*"], &sample_blocks()), "def");
    }

    #[test]
    fn test_stats_max_export_import_roundtrip() {
        let sf = new_stats_max(vec!["a".to_string()]);
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(&[vec![field("a", "5")], vec![field("a", "9")]]);
        sp.update_stats_for_all_rows(&sf, &mut br);

        let mut buf = Vec::new();
        sp.export_state(&mut buf, None);

        let mut sp2 = sf.new_stats_processor();
        sp2.import_state(&buf, None).unwrap();
        let mut dst = Vec::new();
        sp2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "9");
    }

    #[test]
    fn test_stats_max_merge() {
        let sf = new_stats_max(vec!["a".to_string()]);
        let mut a = sf.new_stats_processor();
        let mut b = sf.new_stats_processor();

        let mut br1 = BlockResult::default();
        br1.must_init_from_rows(&[vec![field("a", "7")], vec![field("a", "5")]]);
        a.update_stats_for_all_rows(&sf, &mut br1);

        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "3")], vec![field("a", "9")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "9");
    }
}
