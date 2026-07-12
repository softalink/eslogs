//! Port of EsLogs `lib/logstorage/filter_noop.go`.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::prefix_filter;
use crate::rows::Field;

/// `FilterNoop` does nothing (matches everything). Renders as `*`.
///
/// PORT NOTE: Go exposes a package-level `noopFilter` singleton returned by
/// `newFilterNoop`; the port returns a fresh zero-sized value instead.
pub(crate) struct FilterNoop;

/// Returns a no-op filter.
pub(crate) fn new_filter_noop() -> FilterNoop {
    FilterNoop
}

impl Filter for FilterNoop {
    fn to_string(&self) -> String {
        "*".to_string()
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // nothing to do
    }

    fn match_row(&self, _fields: &[Field]) -> bool {
        true
    }

    fn is_match_all(&self) -> bool {
        true
    }

    fn apply_to_block_search(&self, _bs: &mut BlockSearch<'_>, _bm: &mut Bitmap) {
        // nothing to do
    }

    fn apply_to_block_result(&self, _br: &mut BlockResult, _bm: &mut Bitmap) {
        // nothing to do
    }
}
