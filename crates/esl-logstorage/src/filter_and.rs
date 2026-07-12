//! Port of EsLogs `lib/logstorage/filter_and.go`.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::{Filter, visit_filters_recursive};
use crate::prefix_filter;
use crate::rows::Field;

/// `FilterAnd` contains filters joined by the AND operator (`f1 AND ... AND fN`).
pub(crate) struct FilterAnd {
    pub(crate) filters: Vec<Box<dyn Filter>>,
}

/// Joins `filters` with AND.
pub(crate) fn new_filter_and(filters: Vec<Box<dyn Filter>>) -> FilterAnd {
    FilterAnd { filters }
}

impl Filter for FilterAnd {
    fn to_string(&self) -> String {
        // Go wraps a child `filterOr` in `(...)` so the string form round-trips
        // with the original grouping.
        self.filters
            .iter()
            .map(|f| {
                let s = f.to_string();
                if f.is_filter_or() {
                    format!("({s})")
                } else {
                    s
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        for f in &self.filters {
            f.update_needed_fields(pf);
        }
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        self.filters.iter().all(|f| f.match_row(fields))
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        for f in &self.filters {
            f.apply_to_block_result(br, bm);
            if bm.is_zero() {
                // Shortcut - the result is zero anyway.
                return;
            }
        }
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        // PORT NOTE: Go first checks `matchBloomFilters` (a fast-path that
        // extracts common tokens from the child filters via `filter`-interface
        // type introspection). That introspection needs a downcast hook on the
        // frozen `Filter` trait and its `getCommonTokensForAndFilters` consumer
        // (parser.go) is unported, so the fast-path is deferred. The slow path
        // below yields the same result without it.
        for f in &self.filters {
            f.apply_to_block_search(bs, bm);
            if bm.is_zero() {
                // Shortcut - the result is zero anyway.
                return;
            }
        }
    }

    fn visit_subfilters(&self, visit_func: &mut dyn FnMut(&dyn Filter) -> bool) -> bool {
        visit_filters_recursive(&self.filters, visit_func)
    }

    fn take_and_children(&mut self) -> Option<Vec<Box<dyn Filter>>> {
        Some(std::mem::take(&mut self.filters))
    }

    fn and_children(&self) -> Option<&[Box<dyn Filter>]> {
        Some(&self.filters)
    }
}
