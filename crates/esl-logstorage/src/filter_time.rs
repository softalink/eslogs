//! Port of EsLogs `lib/logstorage/filter_time.go`.
//!
//! `FilterTime` filters the `_time` field by an inclusive nanosecond range.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::filter_phrase::match_column_by_generic;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::try_parse_timestamp_rfc3339_nano;

/// `FilterTime` filters by time; expressed as `_time:[start, end]` in LogsQL.
pub(crate) struct FilterTime {
    /// Minimum timestamp in nanoseconds to match.
    pub(crate) min_timestamp: i64,
    /// Maximum timestamp in nanoseconds to match.
    pub(crate) max_timestamp: i64,
    /// String representation of the filter.
    pub(crate) string_repr: String,
}

/// Builds a time filter.
pub(crate) fn new_filter_time(
    min_timestamp: i64,
    max_timestamp: i64,
    string_repr: &str,
) -> FilterTime {
    FilterTime {
        min_timestamp,
        max_timestamp,
        string_repr: string_repr.to_string(),
    }
}

impl FilterTime {
    fn match_timestamp_string(&self, v: &[u8]) -> bool {
        // Invalid UTF-8 fails the parse = no match, same as Go's parse failure.
        match std::str::from_utf8(v)
            .ok()
            .and_then(try_parse_timestamp_rfc3339_nano)
        {
            Some(timestamp) => self.match_timestamp_value(timestamp),
            None => false,
        }
    }

    fn match_timestamp_value(&self, timestamp: i64) -> bool {
        timestamp >= self.min_timestamp && timestamp <= self.max_timestamp
    }
}

impl Filter for FilterTime {
    fn to_string(&self) -> String {
        format!("_time:{}", self.string_repr)
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_time");
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let v = get_field_value_by_name(fields, b"_time");
        self.match_timestamp_string(v)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.min_timestamp > self.max_timestamp {
            bm.reset_bits();
            return;
        }

        let r = br.get_column_by_name(b"_time");
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !self.match_timestamp_string(v) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            let timestamps = br.get_timestamps().to_vec();
            bm.for_each_set_bit(|idx| self.match_timestamp_value(timestamps[idx]));
            return;
        }

        // PORT NOTE: Go's per-`valueType` path uses raw ISO8601 nanos and resets
        // numeric/ipv4 columns. The port matches the decoded per-row values via
        // `match_timestamp_string` (which parses RFC3339Nano). ISO8601 columns
        // round-trip losslessly and unparseable values return false, so the result
        // is identical to Go's explicit resets.
        match_column_by_generic(br, bm, r, "", &|v, _| self.match_timestamp_string(v));
    }

    fn filter_time_range(&self) -> Option<(i64, i64)> {
        Some((self.min_timestamp, self.max_timestamp))
    }

    fn update_with_time_offset(&mut self, offset: i64) {
        // Go `updateFilterWithTimeOffset`'s `*filterTime` arm: shift the
        // matching bounds, keep `string_repr` as written.
        self.min_timestamp =
            crate::values_encoder::sub_int64_no_overflow(self.min_timestamp, offset);
        self.max_timestamp =
            crate::values_encoder::sub_int64_no_overflow(self.max_timestamp, offset);
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        let min_timestamp = self.min_timestamp;
        let max_timestamp = self.max_timestamp;

        if min_timestamp > max_timestamp {
            bm.reset_bits();
            return;
        }

        let (th_min, th_max) = {
            let th = &bs.block_header().timestamps_header;
            (th.min_timestamp, th.max_timestamp)
        };
        if min_timestamp > th_max || max_timestamp < th_min {
            bm.reset_bits();
            return;
        }
        if min_timestamp <= th_min && max_timestamp >= th_max {
            return;
        }

        let timestamps = bs.get_timestamps().to_vec();
        bm.for_each_set_bit(|idx| {
            let ts = timestamps[idx];
            ts >= min_timestamp && ts <= max_timestamp
        });
    }
}

// PORT NOTE: TestFilterTime uses `testFilterMatchForColumns`, which needs the
// `Storage`/`searchParallel` query pipeline (not yet ported). It is deferred with
// the rest of the filter search-integration tests. `to_string` is unit-tested
// via the whole-filter tests once that harness lands; the timestamp match logic
// is shared with `filter_day_range`/`filter_week_range`.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let f = new_filter_time(0, 100, "[2024-01-01, 2024-01-02]");
        assert_eq!(f.to_string(), "_time:[2024-01-01, 2024-01-02]");
    }

    #[test]
    fn test_match_timestamp_value() {
        let f = new_filter_time(10, 20, "");
        assert!(f.match_timestamp_value(10));
        assert!(f.match_timestamp_value(15));
        assert!(f.match_timestamp_value(20));
        assert!(!f.match_timestamp_value(9));
        assert!(!f.match_timestamp_value(21));
    }
}
