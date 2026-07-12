//! Port of EsLogs `lib/logstorage/filter.go`.
//!
//! # Filter dispatch contract (READ THIS BEFORE PORTING A `filter_*.go` FILE)
//!
//! Go stores filters as values of the unexported `filter` interface and
//! dispatches through the interface vtable. The Rust port keeps that model:
//! every concrete filter is a struct that `impl Filter for FilterFoo`, and
//! filters are held as **`Box<dyn Filter>`** trait objects (see
//! `PartitionSearchOptions.filter`, `FilterAnd.filters`, etc.).
//!
//! Rationale for `Box<dyn Filter>` (NOT a single giant `enum`):
//!   * It mirrors Go's interface dispatch one-to-one, so each `filter_*.go`
//!     file ports into its own sibling module with an `impl Filter` block and
//!     no central match arm to keep in sync across ~34 parallel ports.
//!   * Filters are applied once per data block and operate over a whole block
//!     via [`Bitmap`], so the per-call virtual dispatch is amortised across
//!     thousands of rows — the vtable cost is irrelevant.
//!   * An enum would force every filter variant into one file and one type,
//!     defeating the 1:1 file mapping the port relies on.
//!
//! ## What every `impl Filter` must provide
//! The five methods below map 1:1 to the Go interface methods
//! (`String`, `updateNeededFields`, `matchRow`, `applyToBlockSearch`,
//! `applyToBlockResult`). Composite filters (`filterAnd`, `filterOr`,
//! `filterNot`) additionally override [`Filter::visit_subfilters`] so the
//! recursive visitor helpers can walk into their children — see that method.
//!
//! ## Thread-safety
//! A single filter value is shared by reference across the parallel block
//! search workers (Go passes the same `*partitionSearchOptions` — and thus the
//! same `filter` — to every worker goroutine). The trait therefore requires
//! `Send + Sync`; any interior caches a filter keeps (regex state, `sync.Once`
//! equivalents, atomic pointers) must be thread-safe.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::prefix_filter;
use crate::rows::Field;

/// `Filter` implements filtering for log entries.
///
/// Port of Go's unexported `filter` interface. See the module docs for the
/// `Box<dyn Filter>` dispatch decision.
pub trait Filter: Send + Sync {
    /// Returns the string representation of the filter (Go `String()`).
    fn to_string(&self) -> String;

    /// Updates `pf` with the fields needed for the filter
    /// (Go `updateNeededFields`).
    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter);

    /// Returns true if the filter matches a row with the given fields
    /// (Go `matchRow`).
    fn match_row(&self, fields: &[Field]) -> bool;

    /// Updates `bm` according to the filter applied to the given `bs` block
    /// (Go `applyToBlockSearch`).
    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap);

    /// Updates `bm` according to the filter applied to the given `br` block
    /// (Go `applyToBlockResult`).
    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap);

    /// Recurses into the sub-filters of `self`, calling `visit_func` on each,
    /// and returns as soon as one call returns true.
    ///
    /// This is the trait-method replacement for Go's `visitFilterInternal`
    /// type switch. Leaf filters keep the default (no sub-filters, returns
    /// false, matching Go's `default:` arm). The composite filters override it:
    ///   * `filterAnd` / `filterOr` →
    ///     `visit_filters_recursive(&self.filters, visit_func)`
    ///   * `filterNot` →
    ///     `visit_filter_recursive(self.f.as_ref(), visit_func)`
    fn visit_subfilters(&self, _visit_func: &mut dyn FnMut(&dyn Filter) -> bool) -> bool {
        false
    }

    /// `Query::optimize` support (Go `removeStarFilters`): returns true when
    /// the filter trivially matches every row (`*` prefix filter on the `_msg`
    /// field, or an already-noop filter).
    ///
    /// PORT NOTE: Go type-switches on concrete filter types inside
    /// `removeStarFilters`; `Box<dyn Filter>` has no downcasting, so the
    /// classification lives on the trait with a conservative default.
    fn is_match_all(&self) -> bool {
        false
    }

    /// `Query::optimize` support: takes ownership of the sub-filters of a
    /// `FilterOr`, leaving it empty. Returns `None` for every other filter.
    fn take_or_children(&mut self) -> Option<Vec<Box<dyn Filter>>> {
        None
    }

    /// Display support (Go `filterAnd.String`'s `*filterOr` type switch):
    /// true only for `FilterOr`, whose string form needs parenthesizing when
    /// nested inside an AND.
    fn is_filter_or(&self) -> bool {
        false
    }

    /// `Query::optimize` support: takes ownership of the sub-filters of a
    /// `FilterAnd`, leaving it empty. Returns `None` for every other filter.
    fn take_and_children(&mut self) -> Option<Vec<Box<dyn Filter>>> {
        None
    }

    /// `storage_search::init_filter_in_values_for_filter` support: takes
    /// ownership of the sub-filter of a `FilterNot`, replacing it with a noop.
    /// Returns `None` for every other filter.
    ///
    /// PORT NOTE: Go's `copyFilterInternal` type-switches on `*filterNot`; the
    /// accessor lives on the trait with a `None` default (see
    /// `take_and_children`).
    fn take_not_child(&mut self) -> Option<Box<dyn Filter>> {
        None
    }

    /// Whether this filter is an `in`/`contains_any`/`contains_all`/
    /// `_stream_id:in` filter with a subquery whose values must be resolved
    /// before execution (Go `hasFilterInWithQueryForFilter`'s visit callback,
    /// which type-switches on `*filterGeneric` / `*filterStreamID`). Combined
    /// with [`visit_filter_recursive`] by
    /// `storage_search::has_filter_in_with_query_for_filter`.
    fn has_filter_in_with_query(&self) -> bool {
        false
    }

    /// `storage_search::init_filter_in_values_for_filter` support (the leaf
    /// arm of Go `initFilterInValuesForFilter`'s copyFunc): when this filter
    /// embeds an `in(<subquery>)`, executes the subquery via
    /// `get_values(q_text, q_field_name)` and returns the literal-values
    /// replacement filter. `Ok(None)` (the default) keeps the filter as is.
    fn init_filter_in_values(
        &self,
        _get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
    ) -> Result<Option<Box<dyn Filter>>, String> {
        Ok(None)
    }

    /// `Query::get_filter_time_range` support (Go `getFilterTimeRange`): a
    /// read-only view of the sub-filters of a `FilterAnd`.
    ///
    /// PORT NOTE: Go type-switches on `*filterAnd` inside `getFilterTimeRange`;
    /// `Box<dyn Filter>` has no downcasting, so the accessor lives on the trait
    /// with a `None` default.
    fn and_children(&self) -> Option<&[Box<dyn Filter>]> {
        None
    }

    /// `Query::get_filter_time_range` support (Go `getFilterTimeRange`):
    /// returns `(min_timestamp, max_timestamp)` for a `FilterTime`.
    ///
    /// PORT NOTE: Go type-switches on `*filterTime`; the classification lives
    /// on the trait with a `None` default (see `and_children`).
    fn filter_time_range(&self) -> Option<(i64, i64)> {
        None
    }

    /// `storage_search::get_common_stream_filter` support (Go
    /// `getCommonStreamFilter` type-switches on `*filterStream`): a read-only
    /// view of the [`crate::stream_filter::StreamFilter`] wrapped by a
    /// `FilterStream`. Returns `None` for every other filter.
    fn as_stream_filter(&self) -> Option<&crate::stream_filter::StreamFilter> {
        None
    }

    /// `Query::optimize` support (Go `mergeFiltersStream` type-switches on
    /// `*filterStream`): takes ownership of the
    /// [`crate::stream_filter::StreamFilter`] wrapped by a `FilterStream`,
    /// leaving it empty. Returns `None` for every other filter.
    fn take_stream_filter(&mut self) -> Option<crate::stream_filter::StreamFilter> {
        None
    }

    /// `Query::get_stream_ids` support (Go `getStreamIDsFromFilterOr`
    /// type-switches on `*filterStreamID`): a read-only view of the streamIDs
    /// of a `FilterStreamID`. Returns `None` for every other filter.
    fn stream_ids(&self) -> Option<&[crate::stream_id::StreamID]> {
        None
    }

    /// `Query::get_stream_ids` support (Go `getStreamIDsFromFilterOr`
    /// type-switches on `*filterOr`): a read-only view of the sub-filters of a
    /// `FilterOr` (see `and_children`).
    fn or_children(&self) -> Option<&[Box<dyn Filter>]> {
        None
    }
}

/// `FieldFilter` implements filtering for log entries by a given `field_name`.
///
/// Port of Go's unexported `fieldFilter` interface. It is implemented by the
/// small helper filters (e.g. the field-scoped variants) that other filters
/// dispatch to for a specific column.
pub trait FieldFilter: Send + Sync {
    /// Returns the string representation of the filter (Go `String()`).
    fn to_string(&self) -> String;

    /// Returns true if the filter for `field_name` matches a row with the
    /// given fields (Go `matchRowByField`).
    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool;

    /// Updates `bm` according to the filter for `field_name` applied to the
    /// given `bs` block (Go `applyToBlockSearchByField`).
    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    );

    /// Updates `bm` according to the filter for `field_name` applied to the
    /// given `br` block (Go `applyToBlockResultByField`).
    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    );

    /// `Query::optimize` support (Go `removeStarFilters`): returns true when
    /// this is a `FilterPrefix` with an empty prefix (the `*` filter).
    fn is_empty_prefix(&self) -> bool {
        false
    }

    /// `FilterGeneric::has_filter_in_with_query`/`init_filter_in_values`
    /// support: the [`crate::in_values::InValues`] of an
    /// `in`/`contains_any`/`contains_all` field filter; `None` for every other
    /// field filter.
    ///
    /// PORT NOTE: Go's `filterGeneric.hasFilterInWithQuery`/`initFilterInValues`
    /// type-switch on `*filterIn`/`*filterContainsAny`/`*filterContainsAll`;
    /// the accessor lives on the trait with a `None` default.
    fn in_values(&self) -> Option<&crate::in_values::InValues> {
        None
    }

    /// `FilterGeneric::init_filter_in_values` support: builds the
    /// literal-values replacement for this field filter kind (Go
    /// `newFilterInValues` / `newFilterContainsAnyValues` /
    /// `newFilterContainsAllValues`). Only implemented by the field filters
    /// whose [`FieldFilter::in_values`] returns `Some`.
    fn new_with_values(&self, _field_name: &str, _values: Vec<String>) -> Option<Box<dyn Filter>> {
        None
    }
}

/// Recursively calls `visit_func` for filters inside `f`.
///
/// It stops calling `visit_func` on the remaining filters as soon as
/// `visit_func` returns true. It returns the result of the last `visit_func`
/// call. Port of Go `visitFilterRecursive`.
///
/// PORT NOTE: Go's `visitFilterRecursive(f)` is
/// `visitFilterInternal(f) || visitFunc(f)`. `visitFilterInternal`'s type
/// switch is expressed here as the [`Filter::visit_subfilters`] trait method,
/// so this free function does not need to know about the concrete composite
/// filter types.
pub fn visit_filter_recursive(
    f: &dyn Filter,
    visit_func: &mut dyn FnMut(&dyn Filter) -> bool,
) -> bool {
    f.visit_subfilters(visit_func) || visit_func(f)
}

/// Recursively calls `visit_func` per each filter in `filters`.
///
/// It stops calling `visit_func` on the remaining filters as soon as
/// `visit_func` returns true. It returns the result of the last `visit_func`
/// call. Port of Go `visitFiltersRecursive`.
pub fn visit_filters_recursive(
    filters: &[Box<dyn Filter>],
    visit_func: &mut dyn FnMut(&dyn Filter) -> bool,
) -> bool {
    for f in filters {
        if visit_filter_recursive(f.as_ref(), visit_func) {
            return true;
        }
    }
    false
}

// PORT NOTE: Go's generic `copyFilter` / `copyFilterInternal` / `copyFilters`
// are not ported as such. Their only consumer with subquery support landed —
// `initFilterInValuesForFilter` (storage_search.go) — and it is expressed as
// the ownership-based tree rewrite `storage_search::init_filter_in_values_for_filter`,
// which recurses through the `take_or_children`/`take_and_children`/
// `take_not_child` hooks (the `copyFilterInternal` composite arms) and
// substitutes leaves via [`Filter::init_filter_in_values`] (the copyFunc leaf
// arm). The `flattenFiltersAnd/Or` and `mergeFiltersStream` optimize passes
// use the same hooks (see `parser::query`); the remaining copyFilter consumer
// (time-offset shifting) stays deferred.

// PORT NOTE: upstream `filter_test.go` (TestComplexFilters) is a full
// filter-subsystem integration test (it builds `filterAnd`/`filterOr`/
// `filterNot`/`filterPhrase`, opens a `Storage`, and drives `searchParallel`),
// none of which is ported yet. It belongs with the filter ports once assembled.
// The tests below cover the visitor helpers this module actually defines, and
// double as a reference `visit_subfilters` implementation for the composite
// filter ports.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;

    /// A leaf filter that only records its name; the block-search/result
    /// methods are never exercised by the visitor tests.
    struct Leaf(&'static str);

    impl Filter for Leaf {
        fn to_string(&self) -> String {
            self.0.to_string()
        }
        fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {}
        fn match_row(&self, _fields: &[Field]) -> bool {
            false
        }
        fn apply_to_block_search(&self, _bs: &mut BlockSearch<'_>, _bm: &mut Bitmap) {
            unimplemented!()
        }
        fn apply_to_block_result(&self, _br: &mut BlockResult, _bm: &mut Bitmap) {
            unimplemented!()
        }
    }

    /// A composite filter mirroring how `filterAnd`/`filterOr` override
    /// `visit_subfilters`.
    struct Composite {
        filters: Vec<Box<dyn Filter>>,
    }

    impl Filter for Composite {
        fn to_string(&self) -> String {
            "composite".to_string()
        }
        fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {}
        fn match_row(&self, _fields: &[Field]) -> bool {
            false
        }
        fn apply_to_block_search(&self, _bs: &mut BlockSearch<'_>, _bm: &mut Bitmap) {
            unimplemented!()
        }
        fn apply_to_block_result(&self, _br: &mut BlockResult, _bm: &mut Bitmap) {
            unimplemented!()
        }
        fn visit_subfilters(&self, visit_func: &mut dyn FnMut(&dyn Filter) -> bool) -> bool {
            visit_filters_recursive(&self.filters, visit_func)
        }
    }

    fn leaf(name: &'static str) -> Box<dyn Filter> {
        Box::new(Leaf(name))
    }

    #[test]
    fn test_visit_filter_recursive_visits_leaf_once() {
        let f = Leaf("a");
        let mut names = Vec::new();
        let matched = visit_filter_recursive(&f, &mut |x| {
            names.push(x.to_string());
            false
        });
        assert!(!matched);
        assert_eq!(names, vec!["a".to_string()]);
    }

    #[test]
    fn test_visit_filter_recursive_walks_children_then_self() {
        // Children are visited before the composite itself (Go visits
        // visitFilterInternal first, then visitFunc(f)).
        let f = Composite {
            filters: vec![leaf("a"), leaf("b")],
        };
        let mut names = Vec::new();
        let matched = visit_filter_recursive(&f, &mut |x| {
            names.push(x.to_string());
            false
        });
        assert!(!matched);
        assert_eq!(
            names,
            vec!["a".to_string(), "b".to_string(), "composite".to_string()]
        );
    }

    #[test]
    fn test_visit_filter_recursive_short_circuits_on_true() {
        let f = Composite {
            filters: vec![leaf("a"), leaf("b")],
        };
        let mut names = Vec::new();
        let matched = visit_filter_recursive(&f, &mut |x| {
            names.push(x.to_string());
            x.to_string() == "a"
        });
        assert!(matched);
        // Stops as soon as visit_func returns true, before reaching "b" or self.
        assert_eq!(names, vec!["a".to_string()]);
    }

    #[test]
    fn test_visit_filters_recursive_empty_returns_false() {
        let filters: Vec<Box<dyn Filter>> = Vec::new();
        let mut count = 0;
        let matched = visit_filters_recursive(&filters, &mut |_| {
            count += 1;
            true
        });
        assert!(!matched);
        assert_eq!(count, 0);
    }
}
