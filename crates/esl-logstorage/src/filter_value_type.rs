//! Port of EsLogs `lib/logstorage/filter_value_type.go`.
//!
//! `FilterValueType` filters field entries by their storage value type.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, new_filter_generic};
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::quote_token_if_needed;

// ---------------------------------------------------------------------------
// FilterValueType
// ---------------------------------------------------------------------------

/// `FilterValueType` filters field entries by value type.
///
/// For example `fieldName:value_type("uint64")` returns logs where `fieldName`
/// is stored as a uint64 column.
pub(crate) struct FilterValueType {
    pub(crate) value_type: String,
}

/// Builds a value-type filter for `field_name`.
pub(crate) fn new_filter_value_type(field_name: &[u8], value_type: &str) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterValueType {
            value_type: value_type.to_string(),
        }),
    )
}

impl FieldFilter for FilterValueType {
    fn to_string(&self) -> String {
        format!("value_type({})", quote_token_if_needed(&self.value_type))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &[u8]) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        if v.is_empty() {
            // empty values have no any type
            return false;
        }
        // Assume all the fields have string type, since we cannot determine the
        // real type of the value at the given field.
        self.value_type == "string"
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        let r = br.get_column_by_name(field_name);
        // PORT NOTE: Go returns "inmemory" when `br.bs == nil` (an in-memory,
        // pipe-built block result). `BlockResult` exposes no public indicator for
        // that state, and the in-memory path is not reachable via the filter
        // search flow, so the port reports the concrete `valueType` string here.
        // Wire the "inmemory" case once `BlockResult` exposes the indicator.
        let typ = if br.column_is_const(r) {
            "const".to_string()
        } else if br.column_is_time(r) {
            "time".to_string()
        } else {
            br.column_value_type(r).to_string()
        };
        if self.value_type != typ {
            bm.reset_bits();
        }
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        // Verify whether the filter matches a const column.
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if self.value_type != "const" {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether the filter matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => ch,
            None => {
                bm.reset_bits();
                return;
            }
        };

        let typ = ch.value_type.to_string();
        if self.value_type != typ {
            bm.reset_bits();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let f = FilterValueType {
            value_type: "uint64".to_string(),
        };
        assert_eq!(f.to_string(), "value_type(uint64)");
    }

    #[test]
    fn test_match_row_by_field() {
        let fields = vec![Field {
            name: b"foo".to_vec(),
            value: b"bar".to_vec(),
        }];
        let f = FilterValueType {
            value_type: "string".to_string(),
        };
        assert!(f.match_row_by_field(&fields, b"foo"));
        // empty value has no type
        assert!(!f.match_row_by_field(&fields, b"missing"));
        let f2 = FilterValueType {
            value_type: "uint64".to_string(),
        };
        assert!(!f2.match_row_by_field(&fields, b"foo"));
    }
}
