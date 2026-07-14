//! Port of `stats_quantile.go`: the `quantile(phi[, fields...])` stats function
//! and the shared [`Histogram`] reservoir used by both quantile and
//! [`crate::stats_median`].
//!
//! See [`crate::stats_min`] for the config-capture and decoded-scan PORT NOTEs.
//!
//! PORT NOTE — RNG. Go's histogram uses `valyala/fastrand.RNG` for reservoir
//! sampling once more than `MAX_HISTOGRAM_SAMPLES` (10_000) values are seen.
//! [`Rng`] is an exact port of that generator (xorshift32 with shifts 13/17/5
//! plus the Lemire `Uint32n` reduction), including its lazy seeding from the
//! wall clock. Which samples survive the reservoir is therefore
//! nondeterministic across runs — in Go too (its zero-value RNG seeds from
//! `time.Now().UnixNano()` on first use), so runs differ from each other by
//! the seed only, never by the algorithm.
//!
//! PORT NOTE — `finalize_stats` is `&self`, but Go's `histogram.quantile` sorts
//! `h.a` in place via a pointer receiver. This port sorts a clone so the
//! finalize path stays immutable.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::{self, Filter};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::{field_names_string, get_matching_columns, less_bytes};
use crate::values_encoder::try_parse_float64;

const MAX_HISTOGRAM_SAMPLES: usize = 10_000;

// ---------------------------------------------------------------------------
// Histogram (shared with stats_median)
// ---------------------------------------------------------------------------

/// Port of `valyala/fastrand.RNG` — the reservoir RNG used by Go's
/// `histogram` (see the module PORT NOTE on seeding).
#[derive(Default)]
struct Rng {
    x: u32,
}

impl Rng {
    /// Go `RNG.Uint32`: xorshift32, lazily seeded from the wall clock.
    fn uint32(&mut self) -> u32 {
        while self.x == 0 {
            self.x = get_random_uint32();
        }
        let mut x = self.x;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.x = x;
        x
    }

    /// Go `RNG.Uint32n`: the Lemire multiply-shift reduction to `[0..max_n)`.
    fn uint32n(&mut self, max_n: u32) -> u32 {
        let x = self.uint32();
        ((u64::from(x) * u64::from(max_n)) >> 32) as u32
    }

    /// Go `RNG.Seed`.
    #[cfg(test)]
    fn seed(&mut self, n: u32) {
        self.x = n;
    }
}

/// Go `fastrand.getRandomUint32`: folds the current UnixNano timestamp.
fn get_random_uint32() -> u32 {
    let x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default();
    ((x >> 32) ^ x) as u32
}

/// Port of Go's `histogram`.
#[derive(Default)]
pub(crate) struct Histogram {
    a: Vec<Vec<u8>>,
    min: Vec<u8>,
    max: Vec<u8>,
    count: u64,
    rng: Rng,
}

impl Histogram {
    /// Port of `histogram.update`.
    pub(crate) fn update(&mut self, v: &[u8]) -> i64 {
        if self.count == 0 || less_bytes(v, &self.min) {
            self.min = v.to_vec();
        }
        if self.count == 0 || less_bytes(&self.max, v) {
            self.max = v.to_vec();
        }

        self.count += 1;
        if self.a.len() < MAX_HISTOGRAM_SAMPLES {
            if !self.a.is_empty() && self.a[self.a.len() - 1] == v {
                self.a.push(v.to_vec());
                // PORT NOTE: Go returns unsafe.Sizeof(string)=16; this uses
                // Rust's size_of::<Vec<u8>>() (allocator accounting only).
                return std::mem::size_of::<Vec<u8>>() as i64;
            }
            let v_copy = v.to_vec();
            let n = v_copy.len();
            self.a.push(v_copy);
            return (n + std::mem::size_of::<Vec<u8>>()) as i64;
        }

        let n = self.rng.uint32n(self.count as u32) as usize;
        if n < self.a.len() && self.a[n] != v {
            let prev_len = self.a[n].len();
            let v_copy = v.to_vec();
            let new_len = v_copy.len();
            self.a[n] = v_copy;
            return new_len as i64 - prev_len as i64;
        }
        0
    }

    /// Port of `histogram.mergeState`.
    pub(crate) fn merge_state(&mut self, src: &Histogram) {
        if src.count == 0 {
            return;
        }
        if self.count == 0 {
            self.a.extend_from_slice(&src.a);
            self.min = src.min.clone();
            self.max = src.max.clone();
            self.count = src.count;
            return;
        }

        self.a.extend_from_slice(&src.a);
        if less_bytes(&src.min, &self.min) {
            self.min = src.min.clone();
        }
        if less_bytes(&self.max, &src.max) {
            self.max = src.max.clone();
        }
        self.count += src.count;
    }

    /// Port of `histogram.exportState`.
    pub(crate) fn export_state(&self, dst: &mut Vec<u8>) {
        encoding::marshal_var_uint64(dst, self.a.len() as u64);
        for v in &self.a {
            encoding::marshal_bytes(dst, v);
        }
        encoding::marshal_bytes(dst, &self.min);
        encoding::marshal_bytes(dst, &self.max);
        encoding::marshal_var_uint64(dst, self.count);
    }

    /// Port of `histogram.importState`.
    pub(crate) fn import_state(&mut self, src: &[u8]) -> Result<i64, String> {
        let (items_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot read itemsLen".to_string());
        }
        let mut src = &src[n as usize..];

        let mut a = Vec::with_capacity(items_len as usize);
        let mut state_size = std::mem::size_of::<Vec<u8>>() * items_len as usize;
        for _ in 0..items_len {
            let (value, n) = encoding::unmarshal_bytes(src);
            if n <= 0 {
                return Err("cannot read value".to_string());
            }
            src = &src[n as usize..];
            let value = value.unwrap_or_default().to_vec();
            state_size += value.len();
            a.push(value);
        }
        self.a = a;

        let (min_value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot read min value".to_string());
        }
        src = &src[n as usize..];
        self.min = min_value.unwrap_or_default().to_vec();
        state_size += self.min.len();

        let (max_value, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot read max value".to_string());
        }
        src = &src[n as usize..];
        self.max = max_value.unwrap_or_default().to_vec();
        state_size += self.max.len();

        let (count, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot read count".to_string());
        }
        src = &src[n as usize..];
        self.count = count;

        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }

        Ok(state_size as i64)
    }

    /// Port of `histogram.quantile`.
    pub(crate) fn quantile(&self, phi: f64) -> Vec<u8> {
        if self.a.is_empty() {
            return Vec::new();
        }
        if self.a.len() == 1 {
            return self.a[0].clone();
        }
        if phi <= 0.0 {
            return self.min.clone();
        }
        if phi >= 1.0 {
            return self.max.clone();
        }

        let mut sorted = self.a.clone();
        sorted.sort_by(|x, y| {
            if less_bytes(x, y) {
                std::cmp::Ordering::Less
            } else if less_bytes(y, x) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let idx = (phi * sorted.len() as f64) as usize;
        if idx == sorted.len() {
            return self.max.clone();
        }
        sorted[idx].clone()
    }
}

// ---------------------------------------------------------------------------
// statsQuantile
// ---------------------------------------------------------------------------

/// Port of `statsQuantile`.
pub(crate) struct StatsQuantile {
    field_filters: Vec<String>,
    phi: f64,
    phi_str: String,
}

/// Port of `parseStatsQuantile`. The first parsed filter is `phi`; the
/// remainder default to `["*"]` when empty.
pub(crate) fn new_stats_quantile(mut field_filters: Vec<String>) -> Result<StatsQuantile, String> {
    if field_filters.is_empty() {
        return Err("missing phi arg at 'quantile'".to_string());
    }
    let phi_str = field_filters.remove(0);
    let phi = try_parse_float64(&phi_str).ok_or_else(|| {
        format!("phi arg in 'quantile' must be floating point number; got {phi_str:?}")
    })?;
    if !(0.0..=1.0).contains(&phi) {
        return Err(format!(
            "phi arg in 'quantile' must be in the range [0..1]; got {phi_str:?}"
        ));
    }
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    Ok(StatsQuantile {
        field_filters,
        phi,
        phi_str,
    })
}

impl StatsFunc for StatsQuantile {
    fn to_string(&self) -> String {
        let mut s = format!("quantile({}", self.phi_str);
        if !prefix_filter::match_all(&self.field_filters) {
            s.push_str(", ");
            s.push_str(&field_names_string(&self.field_filters));
        }
        s.push(')');
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(new_stats_quantile_processor(
            self.field_filters.clone(),
            self.phi,
        ))
    }
}

/// Builds a bare [`StatsQuantileProcessor`] (used by `stats_median` too).
pub(crate) fn new_stats_quantile_processor(
    field_filters: Vec<String>,
    phi: f64,
) -> StatsQuantileProcessor {
    StatsQuantileProcessor {
        field_filters,
        phi,
        h: Histogram::default(),
    }
}

/// Port of `statsQuantileProcessor`.
pub(crate) struct StatsQuantileProcessor {
    field_filters: Vec<String>,
    phi: f64,
    h: Histogram,
}

impl StatsProcessor for StatsQuantileProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let cols = get_matching_columns(br, &self.field_filters);
        let mut inc = 0i64;
        for c in cols {
            let values = br.column_get_values(c);
            for v in values {
                inc += self.h.update(v);
            }
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let cols = get_matching_columns(br, &self.field_filters);
        let mut inc = 0i64;
        for c in cols {
            let v = br.column_get_value_at_row(c, row_index).to_owned();
            inc += self.h.update(&v);
        }
        inc
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsQuantileProcessor>()
            .expect("merge_state: other must be StatsQuantileProcessor");
        self.h.merge_state(&src.h);
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        self.h.export_state(dst);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        self.h.import_state(src)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let q = self.h.quantile(self.phi);
        dst.extend_from_slice(&q);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsQuantile*` (lexer) and
// `TestStatsQuantile` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn run_quantile(phi: &str, filters: &[&str], block: &[Vec<Field>]) -> String {
        let mut all: Vec<String> = vec![phi.to_string()];
        all.extend(filters.iter().map(|s| s.to_string()));
        let sf = new_stats_quantile(all).unwrap();
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(block);
        sp.update_stats_for_all_rows(&sf, &mut br);
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    #[test]
    fn test_quantile_parse_validation() {
        assert!(new_stats_quantile(vec![]).is_err());
        assert!(new_stats_quantile(vec!["abc".into(), "a".into()]).is_err());
        assert!(new_stats_quantile(vec!["1.5".into(), "a".into()]).is_err());
        assert!(new_stats_quantile(vec!["0.5".into(), "a".into()]).is_ok());
    }

    #[test]
    fn test_quantile_values() {
        // values 1..=5 in column a
        let block: Vec<Vec<Field>> = (1..=5).map(|i| vec![field("a", &i.to_string())]).collect();
        // phi<=0 -> min "1"; phi>=1 -> max "5"; phi=0.5 -> sorted[2]="3"
        assert_eq!(run_quantile("0", &["a"], &block), "1");
        assert_eq!(run_quantile("1", &["a"], &block), "5");
        assert_eq!(run_quantile("0.5", &["a"], &block), "3");
    }

    #[test]
    fn test_quantile_single_value() {
        let block = vec![vec![field("a", "42")]];
        assert_eq!(run_quantile("0.9", &["a"], &block), "42");
    }

    /// Pins [`Rng`] to `valyala/fastrand.RNG`'s exact xorshift32 sequence
    /// (shifts 13/17/5) and Lemire reduction, seeded like Go `RNG.Seed(1)`.
    #[test]
    fn test_rng_matches_go_fastrand() {
        let mut rng = Rng::default();
        rng.seed(1);
        let seq: Vec<u32> = (0..5).map(|_| rng.uint32()).collect();
        assert_eq!(
            seq,
            vec![270369, 67634689, 2647435461, 307599695, 2398689233]
        );

        let mut rng = Rng::default();
        rng.seed(1);
        let seq_n: Vec<u32> = (0..5).map(|_| rng.uint32n(10)).collect();
        assert_eq!(seq_n, vec![0, 0, 6, 0, 5]);
    }

    /// The reservoir stops growing at `MAX_HISTOGRAM_SAMPLES` while `count`
    /// keeps increasing (Go `histogram.update`).
    #[test]
    fn test_histogram_reservoir_caps_at_max_samples() {
        let mut h = Histogram::default();
        for i in 0..(MAX_HISTOGRAM_SAMPLES + 100) {
            h.update(format!("v{i:06}").as_bytes());
        }
        assert_eq!(h.a.len(), MAX_HISTOGRAM_SAMPLES);
        assert_eq!(h.count, (MAX_HISTOGRAM_SAMPLES + 100) as u64);
        assert_eq!(h.min, b"v000000");
        assert_eq!(
            h.max,
            format!("v{:06}", MAX_HISTOGRAM_SAMPLES + 99).into_bytes()
        );
    }

    #[test]
    fn test_quantile_export_import_merge() {
        let sf = new_stats_quantile(vec!["0.5".into(), "a".into()]).unwrap();
        let mut a = sf.new_stats_processor();
        let mut br1 = BlockResult::default();
        br1.must_init_from_rows(&[vec![field("a", "1")], vec![field("a", "2")]]);
        a.update_stats_for_all_rows(&sf, &mut br1);

        let mut buf = Vec::new();
        a.export_state(&mut buf, None);
        let mut a2 = sf.new_stats_processor();
        a2.import_state(&buf, None).unwrap();

        let mut b = sf.new_stats_processor();
        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "3")], vec![field("a", "4")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a2.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a2.finalize_stats(&sf, &mut dst, None);
        // sorted [1,2,3,4], idx = int(0.5*4)=2 -> "3"
        assert_eq!(String::from_utf8(dst).unwrap(), "3");
    }
}
