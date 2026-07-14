//! Port of `stats_median.go`: `median(fields...)`, which is `quantile(0.5, ...)`.
//!
//! Delegates to [`crate::stats_quantile`]; see [`crate::stats_min`] for the
//! config-capture PORT NOTE.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::field_names_string;
use crate::stats_quantile::{StatsQuantileProcessor, new_stats_quantile_processor};

/// Port of `statsMedian` (a thin wrapper over `statsQuantile` with phi=0.5).
pub(crate) struct StatsMedian {
    field_filters: Vec<Vec<u8>>,
}

/// Port of `parseStatsMedian`. Empty filters default to `["*"]`.
pub(crate) fn new_stats_median(mut field_filters: Vec<Vec<u8>>) -> StatsMedian {
    if field_filters.is_empty() {
        field_filters.push(b"*".to_vec());
    }
    StatsMedian { field_filters }
}

impl StatsFunc for StatsMedian {
    fn to_string(&self) -> String {
        format!("median({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsMedianProcessor {
            sqp: new_stats_quantile_processor(self.field_filters.clone(), 0.5),
        })
    }
}

/// Port of `statsMedianProcessor`.
pub(crate) struct StatsMedianProcessor {
    sqp: StatsQuantileProcessor,
}

impl StatsProcessor for StatsMedianProcessor {
    fn update_stats_for_all_rows(&mut self, sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        self.sqp.update_stats_for_all_rows(sf, br)
    }

    fn update_stats_for_row(
        &mut self,
        sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        self.sqp.update_stats_for_row(sf, br, row_index)
    }

    fn merge_state(&mut self, sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsMedianProcessor>()
            .expect("merge_state: other must be StatsMedianProcessor");
        self.sqp.merge_state(sf, &src.sqp);
    }

    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        self.sqp.export_state(dst, stop);
    }

    fn import_state(&mut self, src: &[u8], stop: Option<&AtomicBool>) -> Result<i64, String> {
        self.sqp.import_state(src, stop)
    }

    fn finalize_stats(&self, sf: &dyn StatsFunc, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        self.sqp.finalize_stats(sf, dst, stop);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsMedian*` (lexer) and
// `TestStatsMedian` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_median_is_quantile_half() {
        let sf = new_stats_median(vec![b"a".to_vec()]);
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        let rows: Vec<Vec<Field>> = (1..=5).map(|i| vec![field("a", &i.to_string())]).collect();
        br.must_init_from_rows(&rows);
        sp.update_stats_for_all_rows(&sf, &mut br);
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        // sorted [1,2,3,4,5], idx = int(0.5*5)=2 -> "3"
        assert_eq!(String::from_utf8(dst).unwrap(), "3");
    }

    #[test]
    fn test_median_to_string() {
        let sf = new_stats_median(vec![b"a".to_vec()]);
        assert_eq!(sf.to_string(), "median(a)");
    }
}
