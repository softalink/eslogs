# lib/logstorage Porting Plan

Source: upstream `lib/logstorage` (see `UPSTREAM.lock`) — 176 non-test Go files,
~59k LOC + ~47k test LOC. Ported into `crates/esl-logstorage/src/`, one module
per Go file (same snake_case name), tests alongside per `docs/CONVENTIONS.md`.

Port bottom-up in dependency layers; each layer is a fan-out batch of parallel
agents, verified (`cargo test -p esl-logstorage`) and committed before the next.

## Layer 0 — primitives (no intra-package deps)
consts, arena, bitmap, u128, hash, hash128, tokenizer, stringbucket, cache,
bloomfilter, color, filenames, chunked(?), plus `lib/prefixfilter` (module
`prefix_filter`).

## Layer 1 — value encoding & parsing helpers
values_encoder (1440), encoding (449), json_parser, logfmt_parser,
pattern_matcher (412), in_values (371), rows (452), tenant, syslog_parser (745),
stream_tags (329), fields-related helpers.

## Layer 2 — block format
block (691), block_data (394), block_header (1021), column_names(?),
timestamps encoding, inmemory_part, block_stream_reader (564),
block_stream_writer (513), block_stream_merger (397).

## Layer 3 — storage engine
part, partition (298), datadb (1538), indexdb (1027), index files,
storage (1365), storage_search (1935), delete, stream_filter (317),
stream_id / streams handling, hits_map (415).

## Layer 4 — LogsQL: parser + filters
parser.go (4095 — split into Rust submodules), query, if-clause,
35 `filter_*.go` files (~15k LOC non-test): phrase, prefix, range, regexp,
exact, in, sequence, and/or/not, contains_*, le_field, len_range, etc.
`block_search` (620) + `block_result` (2900) sit between filters and pipes.

## Layer 5 — pipes
pipe.go + 56 `pipe_*.go` files (~20k LOC non-test): stats (1872), math (1071),
sort (961+735), stream_context (963), top, uniq, facets, format, join, unpack
family, extract family, running_stats, etc.

## Layer 6 — stats functions
26 `stats_*.go` files (~9k LOC non-test): count, count_uniq (890),
count_uniq_hash (729), quantile, histogram, uniq_values, avg/min/max/sum, etc.

## Cross-cutting notes
- External deps used by logstorage: xxhash (cespare/xxhash/v2 → `twox-hash` or
  `xxhash-rust`), zstd (via esl-common), `valyala/fastjson` (port the needed
  subset as json_parser), `valyala/quicktemplate` (only for JSON rendering in
  app layer, not logstorage), snappy(?) — verify per file.
- Concurrency: datadb/partition background merges use worker goroutines; port
  with std::thread + condvars, keep shard counts = available CPUs like Go.
- Memory pooling: LogRows/blocks are pooled aggressively upstream (sync.Pool);
  mirror with explicit pools — this drives the CPU/RSS benchmark win.
- `getBloomFilterMaybe` / bloom sizing constants must match exactly (on-disk
  compat).

## Status
Layer batches tracked in docs/PARITY.md. Foundation (esl-common) fan-out was
launched 2026-07-06; logstorage layers start after it lands.
