//! Stats-function dispatch contract for the `| stats ...` pipe.
//!
//! Port of the `statsFunc` / `statsProcessor` interfaces defined in Go's
//! `pipe_stats.go`. Extracted into their own module so the ~26 `stats_*.go`
//! ports (and `running_stats_*.go`) share one fixed contract, the same way
//! `filter.rs` fixes the `Filter` trait for the filter ports.
//!
//! # Dispatch (READ BEFORE PORTING A `stats_*.go` FILE)
//!
//! Go stores stats functions as values of the unexported `statsFunc` interface
//! and creates a `statsProcessor` per stats function per group. The Rust port
//! keeps that model with trait objects: each concrete stats function is a
//! struct that `impl StatsFunc`, and its `new_stats_processor` returns a
//! `Box<dyn StatsProcessor>`.
//!
//! # `&mut BlockResult`
//! The `BlockResult` value accessors are `&mut self` (they lazily decode and
//! cache column values, mirroring Go's pointer-receiver methods). Therefore the
//! `update_stats_for_*` methods take `&mut BlockResult` even though Go's
//! signatures read `br *blockResult` — a stats worker owns the block
//! exclusively while processing it, so this is sound. Do NOT try to take
//! `&BlockResult`; it cannot call the accessors.
//!
//! # PORT NOTE — allocator
//! Go threads a `*chunkedAllocator` into `newStatsProcessor`/`mergeState` for
//! arena allocation of processor state. The Rust processors own their state
//! (`Vec`, `HashMap`, ...), so the allocator parameter is dropped. If stats
//! allocation churn shows up as a hot spot in the benchmark, reintroduce a
//! `&mut ChunkedAllocator` parameter (tracked in docs/PARITY.md Layer-7
//! backlog).
//!
//! # Thread-safety
//! A single `StatsFunc` is shared across parallel group-by workers, so it
//! requires `Send + Sync`. Each `StatsProcessor` is owned by one worker at a
//! time and only needs `Send`.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::prefix_filter;

/// A stats function such as `count()`, `sum(x)`, `quantile(0.5, y)`.
///
/// Port of Go's unexported `statsFunc` interface.
pub trait StatsFunc: Send + Sync {
    /// String representation of the stats function (Go `String()`).
    fn to_string(&self) -> String;

    /// Updates `pf` with the fields needed to compute this stats function
    /// (Go `updateNeededFields`).
    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter);

    /// Creates a fresh processor for accumulating this function's stats over
    /// one group (Go `newStatsProcessor`).
    fn new_stats_processor(&self) -> Box<dyn StatsProcessor>;

    /// `Query::get_stats_labels*` support: true for the row-selector functions
    /// (`row_any` / `row_min` / `row_max`), whose results are treated as
    /// labels rather than metrics (Go type-switches on `*statsRowAny` /
    /// `*statsRowMin` / `*statsRowMax` in `GetStatsLabelsAddGroupingByTime`).
    fn is_row_label(&self) -> bool {
        false
    }

    /// Sets the per-second step used to normalize `rate()`/`rate_sum()`
    /// (Go `pipeStats.initRateFuncs`'s `case *statsRate/*statsRateSum:
    /// t.stepSeconds = ...`). Default: no-op for all other stats functions.
    fn set_rate_step_seconds(&mut self, _step_seconds: f64) {}
}

/// Accumulates the running state for one [`StatsFunc`] over one group of rows.
///
/// Port of Go's unexported `statsProcessor` interface. Each `update_*` and
/// `import_state` returns the change in internal state size in bytes (Go's
/// `int`); the Rust port uses `i64` so the negative deltas Go can return are
/// representable.
pub trait StatsProcessor: Send {
    /// Updates stats for every row in `br`. Returns the state-size delta in
    /// bytes. `br` is guaranteed to contain at least one row.
    fn update_stats_for_all_rows(&mut self, sf: &dyn StatsFunc, br: &mut BlockResult) -> i64;

    /// Updates stats for the single row at `row_index` in `br`. Returns the
    /// state-size delta in bytes.
    fn update_stats_for_row(
        &mut self,
        sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64;

    /// Merges `other`'s state into `self` (Go `mergeState`). `other` is always
    /// a processor produced by the same `StatsFunc`; recover its concrete type
    /// via `other.as_any().downcast_ref::<Self>()`.
    fn merge_state(&mut self, sf: &dyn StatsFunc, other: &dyn StatsProcessor);

    /// Appends this processor's serialized state to `dst` (Go `exportState`).
    /// Must return promptly if `stop` is set.
    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>);

    /// Imports state previously produced by [`Self::export_state`]. Returns the
    /// state-size increase in bytes, or an error on malformed input
    /// (Go `importState`).
    fn import_state(&mut self, src: &[u8], stop: Option<&AtomicBool>) -> Result<i64, String>;

    /// Appends the string representation of the collected result to `dst`
    /// (Go `finalizeStats`). Must return promptly if `stop` is set.
    fn finalize_stats(&self, sf: &dyn StatsFunc, dst: &mut Vec<u8>, stop: Option<&AtomicBool>);

    /// Returns `self` as `&dyn Any` so `merge_state` can downcast `other` to the
    /// concrete processor type. Every impl is the one-liner
    /// `fn as_any(&self) -> &dyn std::any::Any { self }`.
    fn as_any(&self) -> &dyn Any;
}
