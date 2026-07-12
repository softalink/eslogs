//! Port of `pipe_total_stats.go`.
//!
//! PORT NOTE: Upstream `pipe_total_stats.go` contains **no pipe type of its
//! own** — its entire body is the lexer-based parser `parsePipeTotalStats`,
//! which simply consumes the `total_stats` keyword and delegates to
//! `parsePipeRunningStatsExt(lex, "total_stats")`. That parser builds a
//! `pipeRunningStats` value with `isTotal = true`; the `total_stats` pipe is
//! therefore just `running_stats` in "total" mode.
//!
//! Consequences for this port:
//!
//! * `parsePipeTotalStats` is lexer-dependent and deferred (see the parser
//!   PORT NOTES), so there is nothing lexer-free to expose here.
//! * The backing struct, its `String` / `updateNeededFields` / processor logic
//!   and the running-stats functions (`count`, `first`, `last`, `max`, `min`,
//!   `sum`) all live in the separate `pipe_running_stats` module (and the
//!   `running_stats_*` modules), which are not part of this port and are still
//!   stubs. Duplicating `pipeRunningStats` here would create a conflicting
//!   second definition, so no type is defined in this file.
//! * The upstream behaviour tests `TestPipeTotalStats` and
//!   `TestPipeTotalStatsUpdateNeededFields` (in `pipe_total_stats_test.go`)
//!   exercise `pipeRunningStats` via the parser. They must be ported alongside
//!   the `pipe_running_stats` module — once `PipeRunningStats` gains an
//!   `is_total` flag and a lexer-free constructor — rather than here.
//! * `TestParsePipeTotalStatsSuccess` / `TestParsePipeTotalStatsFailure`
//!   exercise the deferred lexer and are omitted, per the parse-test policy.
