//! Port of `stats_row_any.go` — the `row_any(...)` stats function.
//!
//! Also hosts [`marshal_fields`] / [`unmarshal_fields`] / [`fields_state_size`],
//! the field (de)serialization helpers defined alongside `statsRowAny` in Go.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::rows::{Field, marshal_fields_to_json, sort_fields_by_name};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count::field_names_string;
use crate::stats_sum::get_matching_columns;

/// Marshals `fields` (including field names) into `dst` (Go `marshalFields`).
pub(crate) fn marshal_fields(dst: &mut Vec<u8>, fields: &[Field]) {
    encoding::marshal_var_uint64(dst, fields.len() as u64);
    for f in fields {
        f.marshal(dst, true);
    }
}

/// Unmarshals a length-prefixed slice of fields, returning them and the tail
/// (Go `unmarshalFields`).
///
/// PORT NOTE: Go appends into a caller-supplied `dst`; this returns a fresh
/// `Vec<Field>` since the only caller (`importState`) passes `nil`. `Field`
/// already owns its strings, so no explicit clone is needed after unmarshaling.
pub(crate) fn unmarshal_fields(src: &[u8]) -> Result<(Vec<Field>, &[u8]), String> {
    let (fields_len, n) = encoding::unmarshal_var_uint64(src);
    if n <= 0 {
        return Err("cannot unmarshal fieldsLen".to_string());
    }
    if fields_len > src.len() as u64 {
        return Err(format!(
            "too big fieldsLen={}; it mustn't exceed {}",
            fields_len,
            src.len()
        ));
    }
    let mut src = &src[n as usize..];
    let mut fields = Vec::with_capacity(fields_len as usize);
    for _ in 0..fields_len {
        let mut f = Field::default();
        let tail = f
            .unmarshal_inplace(src, true)
            .map_err(|e| format!("cannot unmarshal field: {e}"))?;
        src = tail;
        fields.push(f);
    }
    Ok((fields, src))
}

/// Approximate in-memory size of `fields` (Go `fieldsStateSize`).
///
/// PORT NOTE: Go uses `unsafe.Sizeof(Field)`; Rust's `size_of::<Field>()`
/// differs numerically but the value is only used for allocator accounting,
/// which is not asserted by the ported test.
pub(crate) fn fields_state_size(fields: &[Field]) -> usize {
    let mut state_size = std::mem::size_of_val(fields);
    for f in fields {
        state_size += f.name.len() + f.value.len();
    }
    state_size
}

/// `row_any(...)` stats function.
pub struct StatsRowAny {
    field_filters: Vec<String>,
}

/// Builds a [`StatsRowAny`] from already-parsed field filters
/// (Go `parseStatsRowAny`).
pub(crate) fn new_stats_row_any(field_filters: Vec<String>) -> StatsRowAny {
    StatsRowAny { field_filters }
}

impl StatsFunc for StatsRowAny {
    fn to_string(&self) -> String {
        format!("row_any({})", field_names_string(&self.field_filters))
    }

    fn is_row_label(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsRowAnyProcessor {
            fields: Vec::new(),
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsRowAnyProcessor {
    fields: Vec<Field>,
    field_filters: Vec<String>,
}

impl StatsRowAnyProcessor {
    fn update_state(&mut self, br: &mut BlockResult, row_index: usize) -> i64 {
        let cs = get_matching_columns(br, &self.field_filters);

        let mut empty_row = true;
        for &c in &cs {
            if !br.column_get_value_at_row(c, row_index).is_empty() {
                empty_row = false;
                break;
            }
        }
        if empty_row {
            return 0;
        }

        let mut state_size_increase = 0i64;
        self.fields.clear();
        for &c in &cs {
            let name = br.column_name(c).to_vec();
            let value = br.column_get_value_at_row(c, row_index).to_vec();
            state_size_increase += (name.len() + value.len()) as i64;
            self.fields.push(Field { name, value });
        }
        state_size_increase
    }
}

impl StatsProcessor for StatsRowAnyProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if !self.fields.is_empty() {
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
        if !self.fields.is_empty() {
            return 0;
        }
        self.update_state(br, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsRowAnyProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        if self.fields.is_empty() {
            self.fields = src.fields.clone();
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_fields(dst, &self.fields);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (fields, tail) = unmarshal_fields(src)?;
        self.fields = fields;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                tail.len()
            ));
        }
        Ok(fields_state_size(&self.fields) as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        // PORT NOTE: Go's finalizeStats has a pointer receiver and sorts in
        // place; the trait method is `&self`, so sort a clone.
        let mut fields = self.fields.clone();
        sort_fields_by_name(&mut fields);
        marshal_fields_to_json(dst, &fields);
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
    fn test_stats_row_any_export_import_state() {
        fn f(sap: &StatsRowAnyProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            sap.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut sap2 = StatsRowAnyProcessor::default();
            sap2.import_state(&data, None).unwrap();
            assert_eq!(sap, &sap2);
        }

        // zero state
        let sap = StatsRowAnyProcessor::default();
        f(&sap, 1);

        // non-zero state
        let sap = StatsRowAnyProcessor {
            fields: vec![
                Field {
                    name: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                },
                Field {
                    name: b"abc".to_vec(),
                    value: b"de".to_vec(),
                },
            ],
            ..Default::default()
        };
        f(&sap, 16);
    }
}
