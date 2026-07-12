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
//! * The parser lives in `parser/parse_stats.rs` (`parse_pipe_total_stats`,
//!   delegating to the shared running-stats parser like Go).
//! * The backing struct (`PipeRunningStats` with its `is_total` flag), its
//!   `String` / `update_needed_fields` / processor logic and the
//!   running-stats functions (`count`, `first`, `last`, `max`, `min`, `sum`)
//!   live in `pipe_running_stats.rs` and the `running_stats_*` modules.
//!   Duplicating the type here would create a conflicting second definition,
//!   so no type is defined in this file.
//! * The upstream behaviour tests (`TestPipeTotalStats*`,
//!   `TestParsePipeTotalStats*`) are ported alongside `pipe_running_stats.rs`
//!   and the parser tests rather than here.
