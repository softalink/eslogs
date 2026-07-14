//! Port of `stats_any.go` — the `any(field)` stats function.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stream_filter::quote_token_if_needed;

/// `any(field)` stats function.
pub struct StatsAny {
    field_name: String,
}

/// Builds a [`StatsAny`] for the given single field (Go `parseStatsAny`'s tail;
/// the "exactly one arg" check lives in the not-yet-ported parser).
pub(crate) fn new_stats_any(field_name: String) -> StatsAny {
    StatsAny { field_name }
}

impl StatsFunc for StatsAny {
    fn to_string(&self) -> String {
        format!("any({})", quote_token_if_needed(&self.field_name))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filter(&self.field_name);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsAnyProcessor {
            value: Vec::new(),
            field_name: self.field_name.clone(),
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsAnyProcessor {
    value: Vec<u8>,
    field_name: String,
}

impl StatsAnyProcessor {
    fn update_state(&mut self, br: &mut BlockResult, row_index: usize) -> i64 {
        let c = br.get_column_by_name(&self.field_name);
        let v = br.column_get_value_at_row(c, row_index);
        if v.is_empty() {
            return 0;
        }
        let n = v.len();
        self.value = v.to_vec();
        n as i64
    }
}

impl StatsProcessor for StatsAnyProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if !self.value.is_empty() {
            return 0;
        }
        self.update_state(br, 0)
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        if !self.value.is_empty() {
            return 0;
        }
        self.update_state(br, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsAnyProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        if self.value.is_empty() {
            self.value = src.value.clone();
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_bytes(dst, &self.value);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal value".to_string());
        }
        let src = &src[n as usize..];
        self.value = value.unwrap_or_default().to_vec();
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }
        Ok(self.value.len() as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(&self.value);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: parse/pipe tests deferred (need parser + pipe_stats).
    #[test]
    fn test_stats_any_export_import_state() {
        fn f(sap: &StatsAnyProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            sap.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut sap2 = StatsAnyProcessor::default();
            sap2.import_state(&data, None).unwrap();
            assert_eq!(sap, &sap2);
        }

        // zero state
        let sap = StatsAnyProcessor::default();
        f(&sap, 1);

        // non-zero state
        let sap = StatsAnyProcessor {
            value: b"foobar".to_vec(),
            ..Default::default()
        };
        f(&sap, 7);
    }
}
