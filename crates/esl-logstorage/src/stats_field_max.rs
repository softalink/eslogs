//! Port of `stats_field_max.go`: the `field_max(src, field)` stats function,
//! which tracks the value of `field` in the row holding the maximum `src`.
//!
//! See [`crate::stats_field_min`] for the mirror image and the `_time`-source
//! PORT NOTE (Go reads the companion at the last row; this port reads it at the
//! row holding the max timestamp).

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::less_string;
use crate::stream_filter::quote_token_if_needed;

/// Port of `statsFieldMax`.
pub(crate) struct StatsFieldMax {
    src_field: String,
    field_name: String,
}

/// Port of `parseStatsFieldMax`; expects exactly two args (src, field).
pub(crate) fn new_stats_field_max(args: Vec<String>) -> Result<StatsFieldMax, String> {
    if args.len() != 2 {
        return Err(format!(
            "unexpected number of arguments for 'field_max' func; got {} args; want 2; args={:?}",
            args.len(),
            args
        ));
    }
    Ok(StatsFieldMax {
        src_field: args[0].clone(),
        field_name: args[1].clone(),
    })
}

impl StatsFunc for StatsFieldMax {
    fn to_string(&self) -> String {
        format!(
            "field_max({}, {})",
            quote_token_if_needed(&self.src_field),
            quote_token_if_needed(&self.field_name)
        )
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filter(&self.field_name);
        pf.add_allow_filter(&self.src_field);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsFieldMaxProcessor {
            src_field: self.src_field.clone(),
            field_name: self.field_name.clone(),
            max: String::new(),
            value: String::new(),
        })
    }
}

/// Port of `statsFieldMaxProcessor`.
pub(crate) struct StatsFieldMaxProcessor {
    src_field: String,
    field_name: String,
    max: String,
    value: String,
}

impl StatsFieldMaxProcessor {
    fn need_update_state_string(&self, v: &str) -> bool {
        if v.is_empty() {
            return false;
        }
        self.max.is_empty() || less_string(&self.max, v)
    }

    fn update_state(
        &mut self,
        v: &str,
        br: &mut BlockResult,
        field_name: &str,
        row_idx: usize,
    ) -> i64 {
        if !self.need_update_state_string(v) {
            return 0;
        }
        let mut delta = 0i64;
        delta -= self.max.len() as i64;
        delta += v.len() as i64;
        self.max = v.to_owned();

        let c = br.get_column_by_name(field_name);
        let value = br.column_get_value_at_row(c, row_idx).to_owned();
        delta -= self.value.len() as i64;
        delta += value.len() as i64;
        self.value = value;

        delta
    }
}

impl StatsProcessor for StatsFieldMaxProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let field_name = self.field_name.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        let src_vals: Vec<Vec<u8>> = br.column_get_values(c_src).to_vec();
        let mut inc = 0i64;
        for (i, v) in src_vals.iter().enumerate() {
            inc += self.update_state(to_unsafe_string(v), br, &field_name, i);
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let field_name = self.field_name.clone();
        let c_src = br.get_column_by_name(&self.src_field);
        let v = br.column_get_value_at_row(c_src, row_index).to_owned();
        self.update_state(&v, br, &field_name, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsFieldMaxProcessor>()
            .expect("merge_state: other must be StatsFieldMaxProcessor");
        if self.need_update_state_string(&src.max) {
            self.max = src.max.clone();
            self.value = src.value.clone();
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_bytes(dst, self.max.as_bytes());
        encoding::marshal_bytes(dst, self.value.as_bytes());
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (max_value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal maxValue".to_string());
        }
        let mut src = &src[n as usize..];
        self.max = to_unsafe_string(max_value.unwrap_or_default()).to_owned();

        let (value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal value".to_string());
        }
        src = &src[n as usize..];
        self.value = to_unsafe_string(value.unwrap_or_default()).to_owned();

        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail; len(tail)={}",
                src.len()
            ));
        }

        Ok((self.max.len() + self.value.len()) as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(self.value.as_bytes());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsFieldMax*` (lexer) and
// `TestStatsFieldMax` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn run(src: &str, fname: &str, blocks: &[Vec<Vec<Field>>]) -> String {
        let sf = new_stats_field_max(vec![src.to_string(), fname.to_string()]).unwrap();
        let mut sp = sf.new_stats_processor();
        for block in blocks {
            let mut br = BlockResult::default();
            br.must_init_from_rows(block);
            sp.update_stats_for_all_rows(&sf, &mut br);
        }
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    #[test]
    fn test_field_max_picks_companion() {
        let blocks = vec![
            vec![vec![field("a", "2"), field("b", "two")]],
            vec![vec![field("a", "1"), field("b", "one")]],
            vec![vec![field("a", "3"), field("b", "three")]],
        ];
        assert_eq!(run("a", "b", &blocks), "three");
    }

    #[test]
    fn test_field_max_requires_two_args() {
        assert!(new_stats_field_max(vec!["a".to_string()]).is_err());
    }

    #[test]
    fn test_field_max_roundtrip_and_merge() {
        let sf = new_stats_field_max(vec!["a".into(), "b".into()]).unwrap();
        let mut a = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(&[vec![field("a", "5"), field("b", "five")]]);
        a.update_stats_for_all_rows(&sf, &mut br);

        let mut buf = Vec::new();
        a.export_state(&mut buf, None);
        let mut a2 = sf.new_stats_processor();
        a2.import_state(&buf, None).unwrap();

        let mut b = sf.new_stats_processor();
        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "9"), field("b", "nine")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a2.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "nine");
    }
}
