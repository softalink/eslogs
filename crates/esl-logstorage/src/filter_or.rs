//! Port of EsLogs `lib/logstorage/filter_or.go`.

use crate::bitmap::{Bitmap, get_bitmap, put_bitmap};
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::{Filter, visit_filters_recursive};
use crate::prefix_filter;
use crate::rows::Field;

/// `FilterOr` contains filters joined by the OR operator (`f1 OR ... OR fN`).
pub(crate) struct FilterOr {
    pub(crate) filters: Vec<Box<dyn Filter>>,
}

/// Joins `filters` with OR.
pub(crate) fn new_filter_or(filters: Vec<Box<dyn Filter>>) -> FilterOr {
    FilterOr { filters }
}

impl Filter for FilterOr {
    fn to_string(&self) -> String {
        self.filters
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(" or ")
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        for f in &self.filters {
            f.update_needed_fields(pf);
        }
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        self.filters.iter().any(|f| f.match_row(fields))
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        let mut bm_result = get_bitmap(bm.bits_len);
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_result.copy_from(bm);
        for f in &self.filters {
            bm_tmp.copy_from(&bm_result);
            f.apply_to_block_result(br, &mut bm_tmp);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }
        bm.and_not(&bm_result);
        put_bitmap(bm_tmp);
        put_bitmap(bm_result);
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        // PORT NOTE: Go first checks `matchBloomFilters` (common-token fast-path
        // via `filter`-interface type introspection). That introspection needs a
        // downcast hook on the frozen `Filter` trait and its
        // `getCommonTokensForOrFilters` consumer (parser.go) is unported, so the
        // fast-path is deferred. The slow path below yields the same result.
        let mut bm_result = get_bitmap(bm.bits_len);
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_result.copy_from(bm);
        for f in &self.filters {
            bm_tmp.copy_from(&bm_result);
            f.apply_to_block_search(bs, &mut bm_tmp);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }
        bm.and_not(&bm_result);
        put_bitmap(bm_tmp);
        put_bitmap(bm_result);
    }

    fn visit_subfilters(&self, visit_func: &mut dyn FnMut(&dyn Filter) -> bool) -> bool {
        visit_filters_recursive(&self.filters, visit_func)
    }

    fn take_or_children(&mut self) -> Option<Vec<Box<dyn Filter>>> {
        Some(std::mem::take(&mut self.filters))
    }

    fn or_children(&self) -> Option<&[Box<dyn Filter>]> {
        Some(&self.filters)
    }

    fn is_filter_or(&self) -> bool {
        true
    }

    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        for f in &mut self.filters {
            f.visit_subqueries_mut(timestamp, visit);
        }
    }

    fn update_with_time_offset(&mut self, offset: i64) {
        for f in &mut self.filters {
            f.update_with_time_offset(offset);
        }
    }
}
