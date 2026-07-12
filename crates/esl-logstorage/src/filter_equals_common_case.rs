//! Port of EsLogs `lib/logstorage/filter_equals_common_case.go`.
//!
//! `FilterEqualsCommonCase` matches words and phrases where every capital
//! letter can be replaced with a small letter, plus all-capital words.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_contains_common_case::get_common_case_phrases;
use crate::filter_generic::{FilterGeneric, new_filter_generic};
use crate::filter_in::FilterIn;
use crate::in_values::InValues;
use crate::rows::Field;
use crate::stream_filter::quote_token_if_needed;

/// `FilterEqualsCommonCase` matches words and phrases where every capital letter
/// can be replaced with a small letter, plus all capital words.
///
/// Example LogsQL: `equals_common_case("Error")` is equivalent to
/// `in("Error", "error", "ERROR")`.
pub(crate) struct FilterEqualsCommonCase {
    phrases: Vec<String>,

    equals_any: FilterIn,
}

pub(crate) fn new_filter_equals_common_case(
    field_name: &str,
    phrases: Vec<String>,
) -> Result<FilterGeneric, String> {
    let common_case_phrases = get_common_case_phrases(&phrases)?;

    let fi = FilterEqualsCommonCase {
        phrases,
        equals_any: FilterIn {
            values: InValues::new(common_case_phrases),
        },
    };

    Ok(new_filter_generic(field_name, Box::new(fi)))
}

impl FieldFilter for FilterEqualsCommonCase {
    fn to_string(&self) -> String {
        let phrases = self
            .phrases
            .iter()
            .map(|p| quote_token_if_needed(p))
            .collect::<Vec<_>>()
            .join(",");
        format!("equals_common_case({phrases})")
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        self.equals_any.match_row_by_field(fields, field_name)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        self.equals_any
            .apply_to_block_result_by_field(br, bm, field_name);
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        self.equals_any
            .apply_to_block_search_by_field(bs, bm, field_name);
    }
}
