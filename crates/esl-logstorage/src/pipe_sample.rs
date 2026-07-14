//! Port of `lib/logstorage/pipe_sample.go` — the `| sample N` pipe.
//!
//! `sample N` keeps on average one out of every `N` rows, skipping the rest.
//! The gap between kept rows follows an exponential distribution with mean
//! `N - 1`, so the effective sampling rate is `1/N`.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#limit-sample>
//!
//! PORT NOTE — parser: Go's `parsePipeSample(lex)` depends on the query lexer,
//! which is not ported yet. The pipe is fully ported and constructed via
//! [`PipeSample::new`].
//!
//! PORT NOTE — RNG: Go seeds a per-worker `math/rand` source with
//! `time.Now().UnixNano()` and draws gaps with `rng.ExpFloat64()` (ziggurat).
//! Since the seed is wall-clock based, sampling is inherently non-deterministic
//! and no test asserts exact selected rows. This port uses a std-only
//! SplitMix64 generator plus inverse-transform sampling (`-ln(1-u)`), which
//! yields the same `Exp(1)` distribution without pulling in the `rand` crate.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;

/// `pipeSample` implements `| sample ...` pipe.
pub(crate) struct PipeSample {
    /// How many rows on average must be skipped during sampling.
    pub(crate) sample: u64,
}

impl PipeSample {
    /// Builds a `| sample N` pipe.
    ///
    /// PORT NOTE: replaces Go's lexer-driven `parsePipeSample`; the caller has
    /// already validated `sample > 0`.
    pub(crate) fn new(sample: u64) -> Self {
        Self { sample }
    }
}

impl Pipe for PipeSample {
    /// Port of Go `pipeSample.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        format!("sample {}", self.sample)
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        false
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // nothing to do
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let base_seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15);

        let n = concurrency.max(1);
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            // Distinct seed per worker so shards don't share a sequence.
            let seed = base_seed ^ (i as u64).wrapping_mul(0x9E3779B97F4A7C15);
            let d = self.sample as f64 - 1.0;
            let mut shard = PipeSampleProcessorShard {
                rng: Rng::new(seed),
                d,
                pp_next: pp_next.clone(),
                rows_processed: 0,
                row_next: 0,
            };
            // Go: rowNext = nextStep() - 1.
            shard.row_next = shard.next_step() - 1;
            shards.push(Mutex::new(shard));
        }

        Arc::new(PipeSampleProcessor { shards })
    }
}

struct PipeSampleProcessor {
    // PORT NOTE: Go's atomicutil.Slice[shard] -> per-worker Vec<Mutex<Shard>>.
    shards: Vec<Mutex<PipeSampleProcessorShard>>,
}

struct PipeSampleProcessorShard {
    rng: Rng,
    d: f64,
    pp_next: Arc<dyn PipeProcessor>,

    rows_processed: u64,
    row_next: u64,
}

impl PipeSampleProcessorShard {
    fn next_step(&mut self) -> u64 {
        1 + (self.d * self.rng.exp_float64()).round() as u64
    }

    fn write_row(&mut self, worker_id: usize, br: &mut BlockResult, row_idx: usize) {
        let cols = br.get_columns();
        let mut rcs: Vec<ResultColumn> = Vec::with_capacity(cols.len());
        for &c in &cols {
            let name = br.column_name(c).to_string();
            let v = br.column_get_value_at_row(c, row_idx).to_vec();
            rcs.push(ResultColumn {
                name,
                values: vec![v],
            });
        }
        let mut out = BlockResult::default();
        out.set_result_columns(rcs, 1);
        self.pp_next.write_block(worker_id, &mut out);
    }
}

impl PipeProcessor for PipeSampleProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        let rows_len = br.rows_len();
        if rows_len == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        loop {
            if shard.row_next < shard.rows_processed {
                esl_common::panicf!(
                    "BUG: rowNext={} cannot be smaller than rowsProcessed={}",
                    shard.row_next,
                    shard.rows_processed
                );
            }

            let row_idx = shard.row_next - shard.rows_processed;
            if row_idx >= rows_len as u64 {
                shard.rows_processed += rows_len as u64;
                return;
            }

            shard.write_row(worker_id, br, row_idx as usize);
            let step = shard.next_step();
            shard.row_next += step;
        }
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Minimal std-only PRNG (SplitMix64) with an exponential draw.
///
/// PORT NOTE: stands in for Go's `math/rand` source; see module docs.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x2545F4914F6CDD1D,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform float in `[0, 1)` with 53-bit precision.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// Draws from the `Exp(1)` distribution via inverse transform.
    fn exp_float64(&mut self) -> f64 {
        let u = self.next_f64();
        -(1.0 - u).ln()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: Go's `TestParsePipeSampleSuccess` / `TestParsePipeSampleFailure`
    // exercise the query lexer (`parsePipeSample`), which is not ported; skipped.
    //
    // PORT NOTE: Go has no data-flow test for `sample` because selection is
    // random; only String() and updateNeededFields are behavior-checked here.

    #[test]
    fn test_pipe_sample_string() {
        assert_eq!(PipeSample::new(10).to_string(), "sample 10");
        assert_eq!(PipeSample::new(10000).to_string(), "sample 10000");
    }

    #[test]
    fn test_pipe_sample_update_needed_fields() {
        // all the needed fields
        check_needed_fields("*", "", "*", "");
        // all the needed fields, plus unneeded fields
        check_needed_fields("*", "f1,f2", "*", "f1,f2");
        // needed fields
        check_needed_fields("f1,f2", "", "f1,f2", "");
    }

    fn check_needed_fields(allow: &str, deny: &str, allow_expected: &str, deny_expected: &str) {
        let pipe = PipeSample::new(10);
        let mut pf = prefix_filter::Filter::default();
        if !allow.is_empty() {
            pf.add_allow_filters(&csv(allow));
        }
        if !deny.is_empty() {
            pf.add_deny_filters(&csv(deny));
        }
        pipe.update_needed_fields(&mut pf);

        let mut got_allow = pf.get_allow_filters();
        got_allow.sort();
        let mut got_deny = pf.get_deny_filters();
        got_deny.sort();

        let mut exp_allow = csv(allow_expected);
        exp_allow.sort();
        let mut exp_deny = csv(deny_expected);
        exp_deny.sort();

        assert_eq!(got_allow, exp_allow, "allow filters mismatch");
        assert_eq!(got_deny, exp_deny, "deny filters mismatch");
    }

    fn csv(s: &str) -> Vec<String> {
        if s.is_empty() {
            return Vec::new();
        }
        s.split(',').map(|x| x.to_string()).collect()
    }
}
