//! Port of EsLogs `lib/logstorage/filter_day_range.go`.
//!
//! `FilterDayRange` filters `_time` by an intra-day nanosecond offset range.

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::filter_phrase::match_column_by_generic;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{
    NSECS_PER_DAY, sub_int64_no_overflow, try_parse_timestamp_rfc3339_nano,
};

/// `FilterDayRange` filters by a day range; expressed as
/// `_time:day_range[start, end] offset d` in LogsQL.
pub(crate) struct FilterDayRange {
    /// Offset in nanoseconds from the start of the day for the range start.
    pub(crate) start: i64,
    /// Offset in nanoseconds from the start of the day for the range end.
    pub(crate) end: i64,
    /// Offset applied to `_time` before applying `[start, end]`.
    pub(crate) offset: i64,
    /// String representation of the filter.
    pub(crate) string_repr: String,
}

/// Builds a day-range filter.
pub(crate) fn new_filter_day_range(
    start: i64,
    end: i64,
    offset: i64,
    string_repr: &str,
) -> FilterDayRange {
    FilterDayRange {
        start,
        end,
        offset,
        string_repr: string_repr.to_string(),
    }
}

impl FilterDayRange {
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
        let day_offset = self.day_range_offset(timestamp);
        day_offset >= self.start && day_offset <= self.end
    }

    fn day_range_offset(&self, timestamp: i64) -> i64 {
        let timestamp = sub_int64_no_overflow(timestamp, self.offset.wrapping_neg());
        timestamp % NSECS_PER_DAY
    }
}

impl Filter for FilterDayRange {
    fn to_string(&self) -> String {
        format!("_time:day_range{}", self.string_repr)
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_time");
    }

    fn update_with_time_offset(&mut self, offset: i64) {
        // Go `updateFilterWithTimeOffset`'s `*filterDayRange` arm:
        // `offset = SubInt64NoOverflow(offset, -timeOffset)`.
        self.offset = crate::values_encoder::sub_int64_no_overflow(self.offset, -offset);
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let v = get_field_value_by_name(fields, b"_time");
        self.match_timestamp_string(v)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.start > self.end {
            bm.reset_bits();
            return;
        }
        if self.start == 0 && self.end == NSECS_PER_DAY - 1 {
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

        // PORT NOTE: see filter_time.rs — the decoded per-row match via
        // `match_timestamp_string` reproduces Go's per-`valueType` results.
        match_column_by_generic(br, bm, r, "", &|v, _| self.match_timestamp_string(v));
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        if self.start > self.end {
            bm.reset_bits();
            return;
        }
        if self.start == 0 && self.end == NSECS_PER_DAY - 1 {
            return;
        }

        let timestamps = bs.get_timestamps().to_vec();
        bm.for_each_set_bit(|idx| self.match_timestamp_value(timestamps[idx]));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let f = new_filter_day_range(0, 100, 0, "[00:00, 08:00]");
        assert_eq!(f.to_string(), "_time:day_range[00:00, 08:00]");
    }

    #[test]
    fn test_match_timestamp_value() {
        // start of day .. 1 hour into the day, no offset.
        let one_hour = 3600 * 1_000_000_000;
        let f = new_filter_day_range(0, one_hour, 0, "");
        assert!(f.match_timestamp_value(0)); // 1970-01-01T00:00:00
        assert!(f.match_timestamp_value(one_hour)); // exactly 01:00
        assert!(!f.match_timestamp_value(one_hour + 1)); // just past 01:00
        assert!(f.match_timestamp_value(NSECS_PER_DAY)); // next day 00:00
        assert!(!f.match_timestamp_value(NSECS_PER_DAY + one_hour + 1));
    }
}
