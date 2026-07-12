//! Port of EsLogs `lib/logstorage/filter_week_range.go`.
//!
//! `FilterWeekRange` filters `_time` by the UTC weekday.

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

// Port of Go `time.Weekday` constants (Sunday == 0 .. Saturday == 6).
const SUNDAY: i32 = 0;
const MONDAY: i32 = 1;
const SATURDAY: i32 = 6;

/// `FilterWeekRange` filters by a week range; expressed as
/// `_time:week_range[start, end] offset d` in LogsQL.
pub(crate) struct FilterWeekRange {
    /// Starting day of the week (0 == Sunday).
    pub(crate) start_day: i32,
    /// Ending day of the week (6 == Saturday).
    pub(crate) end_day: i32,
    /// Offset applied to `_time` before applying `[start, end]`.
    pub(crate) offset: i64,
    /// String representation of the filter.
    pub(crate) string_repr: String,
}

/// Builds a week-range filter.
pub(crate) fn new_filter_week_range(
    start_day: i32,
    end_day: i32,
    offset: i64,
    string_repr: &str,
) -> FilterWeekRange {
    FilterWeekRange {
        start_day,
        end_day,
        offset,
        string_repr: string_repr.to_string(),
    }
}

impl FilterWeekRange {
    fn match_timestamp_string(&self, v: &str) -> bool {
        match try_parse_timestamp_rfc3339_nano(v) {
            Some(timestamp) => self.match_timestamp_value(timestamp),
            None => false,
        }
    }

    fn match_timestamp_value(&self, timestamp: i64) -> bool {
        let d = self.weekday(timestamp);
        d >= self.start_day && d <= self.end_day
    }

    /// Port of Go `filterWeekRange.weekday`: `time.Unix(0, ts).UTC().Weekday()`.
    ///
    /// PORT NOTE: computed directly from the Unix epoch (1970-01-01 is a
    /// Thursday, weekday 4) instead of pulling in a calendar library.
    fn weekday(&self, timestamp: i64) -> i32 {
        let timestamp = sub_int64_no_overflow(timestamp, self.offset.wrapping_neg());
        let days = timestamp.div_euclid(NSECS_PER_DAY);
        ((days.rem_euclid(7) + 4) % 7) as i32
    }
}

impl Filter for FilterWeekRange {
    fn to_string(&self) -> String {
        format!("_time:week_range{}", self.string_repr)
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_time");
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let v = get_field_value_by_name(fields, "_time");
        self.match_timestamp_string(v)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.start_day > self.end_day || self.start_day > SATURDAY || self.end_day < MONDAY {
            bm.reset_bits();
            return;
        }
        if self.start_day <= SUNDAY && self.end_day >= SATURDAY {
            return;
        }

        let r = br.get_column_by_name("_time");
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
        if self.start_day > self.end_day {
            bm.reset_bits();
            return;
        }
        if self.start_day <= SUNDAY && self.end_day >= SATURDAY {
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
        let f = new_filter_week_range(MONDAY, SATURDAY, 0, "[Mon, Sat]");
        assert_eq!(f.to_string(), "_time:week_range[Mon, Sat]");
    }

    #[test]
    fn test_weekday() {
        let f = new_filter_week_range(0, 6, 0, "");
        // 1970-01-01T00:00:00Z is a Thursday (4).
        assert_eq!(f.weekday(0), 4);
        // 1970-01-04 is a Sunday (0).
        assert_eq!(f.weekday(3 * NSECS_PER_DAY), 0);
        // 1970-01-05 is a Monday (1).
        assert_eq!(f.weekday(4 * NSECS_PER_DAY), 1);
        // 1969-12-31 is a Wednesday (3).
        assert_eq!(f.weekday(-NSECS_PER_DAY), 3);
    }

    #[test]
    fn test_match_timestamp_value() {
        // Match Monday..Friday.
        let f = new_filter_week_range(MONDAY, 5, 0, "");
        assert!(!f.match_timestamp_value(3 * NSECS_PER_DAY)); // Sunday
        assert!(f.match_timestamp_value(4 * NSECS_PER_DAY)); // Monday
        assert!(f.match_timestamp_value(0)); // Thursday
        assert!(!f.match_timestamp_value(2 * NSECS_PER_DAY)); // Saturday
    }
}
