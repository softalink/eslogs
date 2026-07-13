//! Port of EsLogs `lib/logstorage/filter_not.go`.

use crate::bitmap::{Bitmap, get_bitmap, put_bitmap};
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::{Filter, visit_filter_recursive};
use crate::prefix_filter;
use crate::rows::Field;

/// `FilterNot` negates the wrapped filter. Expressed as `NOT f` / `!f` in LogsQL.
pub(crate) struct FilterNot {
    pub(crate) f: Box<dyn Filter>,
}

/// Wraps `f` in a negation filter.
pub(crate) fn new_filter_not(f: Box<dyn Filter>) -> FilterNot {
    FilterNot { f }
}

impl Filter for FilterNot {
    fn to_string(&self) -> String {
        // Go renders `!(...)` when the child is a `filterAnd`/`filterOr` and
        // `!...` otherwise.
        //
        // PORT NOTE: Go type-switches on `*filterAnd` / `*filterOr`; the port
        // classifies through the `and_children` / `is_filter_or` trait hooks
        // (only `FilterAnd` / `FilterOr` implement them).
        let s = self.f.to_string();
        if self.f.and_children().is_some() || self.f.is_filter_or() {
            return format!("!({s})");
        }
        format!("!{s}")
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        self.f.update_needed_fields(pf);
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        !self.f.match_row(fields)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        // Minimize the number of rows to check by applying the filter only to
        // the rows which match bm.
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_tmp.copy_from(bm);
        self.f.apply_to_block_result(br, &mut bm_tmp);
        bm.and_not(&bm_tmp);
        put_bitmap(bm_tmp);
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        // Minimize the number of rows to check by applying the filter only to
        // the rows which match bm.
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_tmp.copy_from(bm);
        self.f.apply_to_block_search(bs, &mut bm_tmp);
        bm.and_not(&bm_tmp);
        put_bitmap(bm_tmp);
    }

    fn visit_subfilters(&self, visit_func: &mut dyn FnMut(&dyn Filter) -> bool) -> bool {
        visit_filter_recursive(self.f.as_ref(), visit_func)
    }

    fn take_not_child(&mut self) -> Option<Box<dyn Filter>> {
        Some(std::mem::replace(
            &mut self.f,
            Box::new(crate::filter_noop::new_filter_noop()),
        ))
    }

    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        self.f.visit_subqueries_mut(timestamp, visit);
    }

    fn update_with_time_offset(&mut self, offset: i64) {
        self.f.update_with_time_offset(offset);
    }
}
