# Parity Tracker

Status of each upstream package's port. Status values:
`todo` → `in-progress` → `ported` (code + tests pass) → `verified`
(cross-checked against Go behavior on real data).

## Foundation (`crates/esl-common`)

| Upstream package | Status | Notes |
|------------------|--------|-------|
| lib/bytesutil | ported | |
| lib/encoding | ported | varint/zigzag + block encodings |
| lib/encoding/zstd | ported | |
| lib/slicesutil | ported | |
| lib/stringsutil | ported | |
| lib/fasttime | ported | |
| lib/timeutil | ported | |
| lib/atomicutil | ported | |
| lib/memory | ported | |
| lib/cgroup | ported | Linux-only behavior; no-op on Windows |
| lib/logger | ported | |
| lib/flagutil | ported | incl. flag registry for /flags + esm_flag gauges (registration at first Flag::get; PORT NOTE on coverage) |
| lib/envflag | ported | |
| lib/buildinfo | ported | |
| lib/fs | ported | cross-platform file ops |
| lib/fs/fsutil | ported | |
| lib/filestream | ported | |
| lib/regexutil | ported | |
| lib/httpserver | ported | threaded server (worker pool sized to available_cpus); server-side TLS via -tls/-tlsCertFile/-tlsKeyFile/-tlsMinVersion/-tlsCipherSuites (rustls session shared behind a mutex on TLS conns; plain-TCP path keeps its lock-free tri-handle plumbing — see the module docs); no auth/pprof (PORT NOTE); /metrics serves the full registry via lib/appmetrics + esm_http_* request metrics; /flags serves the Go-format flag dump (secrets redacted) |
| lib/httputil | ported | GetRequestValue/GetArray/GetBool/GetInt/CheckURL over Request |
| github.com/VictoriaMetrics/metrics | ported | esl-common/src/metrics.rs: Set + default set, Counter, FloatCounter, Gauge, Histogram (vmrange buckets), Summary (incl. inline valyala/histogram+fastrand), validator, WritePrometheus, process metrics (linux /proc + windows, incl. PSI process_pressure_*), push.go (metrics/push.rs: periodic push, extra labels, gzip, metrics_push_* self-metrics); prometheus_histogram.go / go_metrics.go unported (ignore list) |
| lib/appmetrics | ported | esl-common/src/appmetrics.rs: /metrics payload (registry + process + esm_app_*/esm_os_info); esm_flag{name,value,is_set} gauges + -metrics.exposeMetadata flag wired |
| lib/pushmetrics | ported | esl-common/src/pushmetrics.rs: -pushmetrics.url/.interval/.extraLabel/.header/.disableCompression, Init/Stop wired in es-logs + esl-agent mains; InitWith/StopAndPush unported (vmctl/vmbackup-only, PORT NOTE) |
| lib/netutil | todo | |
| lib/protoparser/protoparserutil | partial | request-body decompression (gzip/deflate/zstd/snappy) + ReadLinesBlock in httpserver; rest todo |
| lib/writeconcurrencylimiter | todo | |
| lib/chunkedbuffer | ported | |
| lib/bufferedwriter | todo | |
| lib/timerpool | ported | |
| lib/contextutil | ported | |
| lib/procutil | ported | |
| lib/fastnum | ported | |
| lib/decimal | ported | only if actually needed by used code paths |

## Storage engine (`crates/esl-logstorage`)

Tracked at file/subsystem granularity once porting starts:

| Subsystem | Status | Notes |
|-----------|--------|-------|
| primitives (consts, filenames, u128, hash128, arena, stringbucket, color_sequence, cache, chunked_allocator) | ported | |
| parsers (json, json_scanner, logfmt, pattern, pattern_matcher, syslog) | ported | fastjson subset ported inside json_parser |
| rows / in_values / tenant_id / stream_tags | ported | Field = owned Strings (crate-wide decision) |
| lib/prefixfilter | ported | |
| hash / tokenizer / bloom filter | ported | bitmap, tokenizer, hash_tokenizer, bloomfilter; golden bytes pinned |
| value encoding (values_encoder etc.) | ported | + encoding.rs strings/uint64/bytes blocks |
| block format (block, block_data, block_header) | ported | + column_names, log_rows, stream_id, inmemory_part |
| part / merge | ported | part, part_header, block_stream_{reader,writer,merger}, index_block_header |
| datadb / partition | ported | |
| indexdb / streams | ported | see mergeset note below |
| Storage API (storage.go et al.) | ported | search/RunQuery deferred to Layer 4 |
| LogsQL lexer/parser | ported | parser/ dir module, ParseQuery, all constructors wired |
| filters (filter_*.go) | ported | all 34 files, Box<dyn Filter>, &mut block ctx |
| pipes (pipe_*.go) | ported | all 56 files, Pipe/PipeProcessor trait, single-node |
| stats (stats_*.go) | ported | all 26 stats_*.go + 6 running_stats_*.go, StatsFunc/StatsProcessor trait |

## App layer — COMPLETE (all app/ packages ported, 2026-07-11)

| Upstream | Rust crate | Status | Notes |
|----------|-----------|--------|-------|
| app/es-logs | es-logs | ported | binary, flags, lifecycle, syslog-listener hooks |
| app/eslstorage | esl-storage | ported | full main (auth keys, snapshots, metrics writer), query_stats, lastnoptimization (ported+tested; deliberately NOT on the query path — engine block pruning is faster, see esl-select PORT NOTE), netinsert, netselect |
| app/eslinsert (all) | esl-insert | ported | jsonline, elasticsearch, loki json+protobuf(+easyproto in esl-common), opentelemetry, datadog, journald, splunk, native/multitenant/internal insert, syslog TCP/UDP/unix listeners incl. TLS, insertutil incl. flags/testutils |
| app/eslselect (all) | esl-select | ported | all 13 /select/logsql/* endpoints incl. hits/facets/stats_query(_range)/streams/stream_*/tail (chunked streaming)/query_time_range, format=csv, internalselect (server side of netselect, round-trip tested), esmui embedded byte-identical |
| app/eslagent | esl-agent | ported | tail (rotation/fingerprints), filecollector (internal doublestar), kubernetescollector (CRI/klog parsing, kubelet watch), remotewrite + full lib/persistentqueue port; e2e wire-compat verified against Go v1.51 binary |
| app/eslogscli | eslogscli | ported | REPL, history, output modes, pager; minimal line editor PORT NOTE; https datasource + -tls* flags supported |
| app/eslogsgenerator | eslogsgenerator | ported | all 20 flags, e2e-verified generation |
| app/esmui | esl-select assets | ported | prebuilt upstream assets embedded, completeness-tested |

Cross-cutting deferrals (PORT-NOTEd at each site):
net_query_runner (cluster query splitting) is PORTED (2026-07-12):
`Pipe::split_to_remote_and_local` across all 51 pipe impls,
`PipeStatsMode` remote/local/proxy (export_state/import_state wire),
`NetQueryRunner` + eager subquery init, wired into netselect
`Storage::run_query` (2-node stats-merge e2e in esl-select internalselect
tests). Context
cancellation is ported (2026-07-12): a global disconnect-watcher thread
(esl_common::disconnect_watcher, peek-based socket probing) stands in for
Go's request ctx; `Storage::run_query_with_cancel` and the `Get*` query
surface take the cancel token and return "context canceled"
(storage_search::QUERY_CANCELED_ERROR) on abort, wired into all buffered
/select/logsql/* handlers and /internal/select/*; /select/logsql/tail keeps
its flush_chunk-based per-window disconnect detection (PORT NOTE at the
site). Per-query stats accumulation from Go's QueryContext remains dropped. The metrics registry is ported (esl_common::metrics) and wired
across the crates: /metrics serves the registry (esl_/esm_/eslagent_ series),
the storage writer set, per-query-stats vmrange histograms and process
metrics; remaining unwired families (vm_filestream_*, vm_fs_*, vm_gorutines
and other Go-runtime series) are PORT-NOTEd at their sites. TLS is supported via `esl_common::tlsutil` (rustls/ring, MSVC
cross-compile-clean): client side (-storageNode.tls*, -remoteWrite.tls*,
kubernetes collector, eslogscli -tls*) and server side (-syslog.tls* and
httpserver's -tls/-tlsCertFile/-tlsKeyFile/-tlsMinVersion/-tlsCipherSuites
serving flags for both es-logs and eslagent).
rustls-vs-Go gaps (PORT-NOTEd in tlsutil): no TLS 1.0/1.1, AEAD-only cipher
suites, webpki-roots bundle instead of the system cert pool.
`_stream:{...}` execution is fully wired (lazy per-partition streamID
resolution in filter_stream.rs); Go's getCommonStreamFilter block-scheduling
pre-filter remains an unported optimization.

## Benchmark gate

| Metric | Linux | Windows (MSVC) |
|--------|-------|----------------|
| CPU usage during ingest | — | — |
| Memory (RSS) during ingest | — | — |
| Disk space used | — | — |
| Ingestion throughput | — | — |
| Query latency | — | — |

## Architectural decisions

- **indexdb / mergeset (2026-07-07, superseded 2026-07-12):** EsLogs'
  `indexdb` sits on Softalink LLC `lib/mergeset` (~4500 LOC LSM
  engine). The port initially used an API-compatible internal sorted-items
  store instead (single length-prefixed `items.bin`), which preserved query
  semantics but not the on-disk format. That store has been replaced by a
  faithful port of `lib/mergeset` (`indexdb/mergeset/`): part layout
  (`metaindex.bin`/`index.bin`/`items.bin`/`lens.bin` + `metadata.json` per
  part, `parts.json` listing), block encodings (commonPrefix block headers,
  plain + zstd items/lens encodings), rawItems shards → in-memory parts →
  file parts with background merges, and the `PrepareBlockCallback` merge
  hook. **Implication:** the on-disk `indexdb/` directory IS now
  byte-compatible with upstream — an existing Go `-storageDataPath` opens in
  place, and both cross directions are verified live against the Go reference
  binary (Go-written indexdb read by Rust; Rust-written parts read back by
  Go; see the `#[ignore]`d `test_go_indexdb_cross_compat` in
  `indexdb/mod.rs`). Deliberate divergences, PORT-NOTEd in
  `indexdb/mergeset/`: no global block caches (Storage-level caches cover
  the hot paths), `Arc<PartWrapper>` instead of the manual refCount, no
  read-only mode, object pools omitted. All other formats (streamID,
  stream-tags canonical, tag encoding, cache keys) remain byte-identical to
  upstream.

## Layer 3 integration seams (to wire before/with Layer 4)

- partition.rs currently creates the indexdb dir via bare mkdir and defers the
  stream-registration half of `mustAddRows` — partition must open an
  `indexdb::Indexdb` and call `must_register_stream` on new streams so queries
  can resolve stream filters.
- indexdb holds a narrow `indexdb::Storage` placeholder (3 fields it needs);
  replace with `crate::storage::Storage` when wiring partition↔indexdb↔storage.
- storage.rs `must_add_rows` slow path recomputes streamID via
  `must_add_insert_row` instead of a shared internal — reconcile when wiring.

## Layer 7 optimization backlog (deferred fast-paths — correct but slower than Go)

These paths were ported behavior-identical but skip Go's block/row pruning
micro-optimizations (which need types not yet ported). Restore during the
optimization loop if the benchmark shows the relevant filter/path is hot:

- **And/Or bloom fast-path** (filter_and/or): Go's `matchBloomFilters` +
  `getCommonTokensForAnd/OrFilters` prune whole blocks before per-row work.
  Deferred — needs filter downcast introspection (parser consumer unported).
- **filter_pattern_match bloom-token pre-filter**: omitted because
  `PatternMatcher.separators/pmo` are private. Expose `pub(crate)` accessors to
  restore `initTokens` block-skipping.
- **Dict block-result fast-path** (many filters): `BlockResult` exposes no
  `dictValues`, so `valueTypeDict` routes through decoded per-row values instead
  of Go's dict match-table. Expose the dict on BlockResult to restore.
- **Range/eq/le min-max pruning**: BlockResult ColRef has no min/max accessor;
  slow per-row path used. Expose to restore Go's range pruning.
- **filter_regexp**: takes an extra `re_str` param; expose a source accessor on
  `regexutil::Regex` (it already stores `expr_str`) to drop it (cosmetic).

## Cleanup backlog (non-blocking, do during Layer-7 or a dedicated pass)

- **Duplicated stats helpers**: `get_matching_columns` (stats_sum/stats_min/
  stats_uniq_values), `marshal_json_values` (stats_uniq_values/stats_json_values),
  `less_string` (stats_min/stats_uniq_values/stats_json_values_sorted),
  `field_names_string`/`is_single_field` — each ported independently by
  parallel agents at distinct module paths (compiles fine). Consolidate into
  one shared location (e.g. a stats_util module or their true Go home once
  pipe_sort/parser land). The `try_parse_number`/`parse_math_number` copies
  are consolidated into `pipe_math.rs` (the Go home) as of the parity sweep.
- **Provisional homes**: `BySortField` (Go pipe_sort.go), `marshal_json_values`
  (Go stats_uniq_values.go) — relocate when the owning file is ported.
- **~10 residual clippy style lints** in the pipe layer (items-after-test-module in
  running_stats_{first,last,min,max}.rs from appended trait impls; loop-index/let-binding
  style in pipe_split/pipe_replace_regexp/pipe_unpack; pipe.rs doc-list indent). Cosmetic,
  from parallel-agent integration; clean in a dedicated clippy pass.
- **Duplicate IfFilter** in pipe_unpack.rs + pipe_update.rs (and if_filter.go not yet
  ported as its own module) — consolidate into one when if_filter.go is ported in the
  Layer-4 finalize.
- **running_stats_* as inherent methods**: running_stats_{count,sum,min,max,
  first,last} expose update/get as inherent methods pending the
  runningStatsFunc/runningStatsProcessor trait from pipe_running_stats.go (L5).

## RunQuery orchestration — DONE (all 3 benchmark queries pass end-to-end)

storage_search.rs ports the full RunQuery spine: `Storage::run_query` /
`run_query_with_stats`, `searchParallel` block scheduling (with stream/tenant
pre-filters and time-range pruning), `run_pipes`, `init_subqueries`
(`in(...)` values, `join` maps, `union` wiring, `stream_context` seam) and the
`GetFieldNames`/`GetFieldValues`/`GetStreams`/`GetStreamIDs`/`GetStreamField*`
value-with-hits surfaces. `net_query_runner.rs` ports the cluster query
splitter (`NewNetQueryRunner`/`splitToRemoteAndLocal`), wired through
`esl-storage/netselect` + `esl-select/internalselect`.

## Parser status (was: deferrals)
- optimize() is ported: `removeStarFilters`, and/or flattening, stream-filter
  merging, `optimizeOffsetLimitPipes`, `optimizeUniqLimitPipes`,
  `optimizeFilterPipes` + leading-`filter` merge into `q.f`, and the
  first-pipe `field_names` marking — via object-safe `Filter`/`Pipe` trait
  hooks in place of Go's `copyFilter` + type switches. Join/union subqueries
  are optimized before their text is rendered (Go reaches them via
  `visitSubqueries`). Still deferred: the `!!x` double-negation collapse and
  `updateFilterWithTimeOffset`'s `time_offset != 0` filter rewrite.
- The full lexer + parse grammar is ported, including the `math`/`eval`
  expression sub-grammar (`parsePipeMath` + operator-priority rebalancing).
- NOT-IN-GO query fast paths (perf, benchmark-driven): desc-time top-N block
  skipping (`PipeProcessor::block_skip_check` + newest-first scheduling +
  global full-heap threshold in pipe_sort_topk), monotone-timestamps early
  break in topk, zero-copy bloom probes (`ReaderAt::mmap_slice` +
  `BloomFilter::bytes_contain_all`).
- Subqueries ARE ported: `in(<subquery>)` / `contains_any(<subquery>)` /
  `contains_all(<subquery>)` / `_stream_id:in(<subquery>)` parse into
  rendered-text subqueries (`InValues::q_text`, `FilterStreamID::q_text`,
  `PipeJoin`/`PipeUnion::query_text`) and are resolved before the search by
  `storage_search::init_subqueries` (Go `initSubqueries`/`initFilterInValues`/
  `initJoinMaps`/`initUnionQueries`/`initStreamContextPipes`). The eager
  cluster mode (Go `initSubqueries(..., eagerExecute=true)`) is ported in
  `net_query_runner::init_subqueries_net`, with a PORT NOTE divergence:
  local-half `union` subqueries are also resolved eagerly (inlined as
  `union rows(...)`) instead of Go's lazy flush-time wiring, because the
  runner borrows the caller's run-net-query callback. Still deferred:
  `visitSubqueries`-based propagation (`AddTimeFilter`/`AddExtraFilters` do
  not descend into subqueries) and `stream_context` runQuery wiring on the
  NetQueryRunner local-pipes path (the local `Storage::run_query` path IS
  wired).
- filter_and/not Display omit parens; filter_phrase Display incomplete quoter.

## Parity ledger (v1.51.0)

Definitive audit of every `PORT NOTE` in the workspace (1,326 notes reviewed
file-by-file on 2026-07-12, after the final parity sweep closed
`optimizeUniqLimitPipes`, the `pipeFieldNames` first-pipe fast mode, the
`math`/`eval` parse grammar, `initStreamContextPipes` wiring on the local
run-query path, and the shared `tryParseNumber` "inf" handling). "Full
parity" is measured against section (a): every entry there is a way the Rust
port can behave observably differently from upstream Go v1.51.0. Items marked
*(verify)* are classified conservatively — they could not be proven identical
and belong here until proven otherwise.

### (a) Observable behavioral divergences

**Query semantics (esl-logstorage)**

- `parser/query.rs:205` — `options(global_filter=...)` parses but is never
  ANDed into the search filter (`get_final_filter` returns `q.f` only);
  queries that set it get unfiltered results.
- `parser/query.rs:367`, `parser/query.rs:386`, `pipe.rs:29` — Go's
  `visitSubqueries` propagation is not ported: `AddTimeFilter` (the HTTP
  start/end range) and `AddExtraFilters` are applied to the top-level query
  only, not to `in(...)`/`join`/`union` subqueries. Subqueries scan their own
  (unbounded) time range and ignore extra/security filters.
- `parser/query.rs:982`, `parser/query.rs:1033` — query options are not
  inherited into nested subqueries (Go pushes the options stack); subqueries
  run with default options.
- `parser/query.rs:1011` — `options(time_offset=...)` does not shift
  `filterTime`/`filterDayRange`/`filterWeekRange` bounds
  (`update_filter_with_time_offset` is a no-op).
- `parser/query.rs:194` + `parser/query_stats.rs:127` —
  `initStatsRateFuncSteps` is a stub; `rate()`/`rate_sum()` step
  initialization is skipped, changing their computed values.
- `parser/parse_pipe.rs:4` (+ `pipe_replace.rs:6`, `pipe_replace_regexp.rs:7`)
  — `replace`/`replace_regexp` parse a leading `if (...)` and drop it: the
  replacement runs unconditionally on every row and the `if` is omitted from
  the rendered query.
- `parser/parse_stats.rs:5` — `stats switch(...)` is rejected; Go accepts it.
- `parser/parse_filter.rs:290` — `!!foo` is not collapsed to `foo` by
  optimize (rendering + re-parse grouping differ).
- `storage_search.rs:458` / `:619` — the `hiddenFieldsFilter` pass is unwired
  (always empty): fields Go hides via `HiddenFieldsFilters` are returned.
  Mirrored at the HTTP layer (`esl-select/src/logsql.rs:595/:760/:967`,
  where `hidden_fields_filters` is parsed but unused).
- `pipe_stream_context.rs` / `net_query_runner.rs` — `stream_context` is
  wired only on the local `Storage::run_query` path; on the `NetQueryRunner`
  local-pipes path (cluster seam, unreachable in the shipped single-node
  binary) it errors instead of fetching surrounding logs.
- `pipe_stream_context.rs:190` — the surrounding-log fetch has no
  `stateSizeBudget`; Go errors when the fetch exceeds ~20% of memory, the
  port never does.
- `pipe_stats.rs:28` — stats `stateSizeBudget`/cancel is dropped: at extreme
  group cardinality Go stops with a state-size error while the port keeps
  accumulating (fuller results or OOM).
- `pipe_sort.rs:26/:536` *(verify)* — the sort OOM budget is charged per
  block, not per value; borderline-memory sorts may error in one
  implementation and not the other.
- `pipe_union.rs:204` *(verify)* — a union subquery is not cancelled when the
  downstream pipeline stops early (Go cancels via context).
- `pipe_block_stats.rs:7/:101` — for persisted (file) parts, `block_stats`
  reports `part_path`/`stream`/`column_type` as `"inmemory"` and zero
  values/bloom/dict sizes; Go reports the real per-column on-disk data.
- `stats_field_min.rs:7`, `stats_field_max.rs:4` — for a `_time` source
  column the companion field is read at the true min/max row; Go reads it at
  row 0 / the last row (Go's fast-path quirk). Companion values can differ.
- `stats_quantile.rs:7` — reservoir sampling uses a different RNG than Go's
  `fastrand`; approximate quantiles over >10,000 samples select different
  survivors.
- `stats_stddev.rs:6` — stddev renders via Rust shortest-round-trip `Display`
  and may use exponent form at extreme magnitudes where Go's `'f'` format
  never does.
- `pipe_format.rs:333/:345` — `uc`/`lc` use full Unicode case mapping vs
  Go's per-rune simple mapping (e.g. `ß` → `SS`).

**LogsQL text rendering / round-trip (esl-logstorage)**

- `filter_not.rs:22` (+ `parser/tests.rs:104/:128`) — `filterNot`/`filterAnd`/
  `filterOr` `String()` omit Go's disambiguating parens; `!(foo or bar)` and
  `a (b or c)` render without grouping. Because `Query::clone` and the pipe
  clones round-trip through rendered text, this is not only cosmetic: e.g.
  the facets pipeline re-parses the ungrouped text and can produce wrong
  results (`esl-select/src/logsql_facets.rs:131`).
- `parser/mod.rs:51` (+ `parser/tests.rs:142`) — the Display quoter omits
  Go's `isPipeName`/`isStatsFuncName` checks; a phrase equal to a pipe or
  stats-func name renders unquoted where Go quotes it.
- `prefix_filter.rs:155` *(verify)* — quoted-list rendering uses Rust `{:?}`
  instead of Go `strconv.Quote` (differs for non-ASCII/control chars).
- `json_parser.rs:1147` *(verify)* — re-quoted object keys keep non-printable
  non-ASCII runes raw where Go `\uXXXX`-escapes them.
- `parser/mod.rs:194` *(verify)* — `string_range` upper-bound sentinel is
  `U+10FFFF`×4 vs Go's `0xFF`×4 (edge-case inclusion difference).
- `stream_filter.rs:339` *(verify)* — stream tag names equal to pipe/stats
  names are not quoted in Display.
- `stream_filter.rs:762/:843` *(verify)* — `is_go_print` approximates
  `unicode.IsPrint`, and `\xNN` (≥0x80) decodes to a Unicode scalar instead
  of Go's raw byte.

**Input handling edge cases (esl-logstorage)**

- `json_parser.rs:322` *(verify)* — invalid UTF-8 in ingested JSON becomes
  `U+FFFD` instead of Go's raw bytes. The same raw-byte-vs-lossy divergence
  recurs across the ingestion surface: `arena.rs:67` (non-UTF-8 field value
  panics — verify), `esl-insert/src/journald.rs:394`,
  `esl-insert/src/loki_protobuf.rs:412`,
  `esl-agent/src/filecollector.rs:918`,
  `esl-agent/src/kubernetescollector.rs:2336/:2434/:2654`,
  `esl-common/src/easyproto.rs:314/:588` (protobuf strings with invalid
  UTF-8 are rejected where Go accepts them).
- `pattern.rs:373` — `extract` pattern `\x`/octal escapes ≥0x80 are
  UTF-8-encoded instead of emitting the raw byte.
- `pattern.rs:501` — HTML-entity unescaping knows 6 named entities vs Go's
  full (~2200-entry) table.
- `syslog_parser.rs:24/:571` — RFC3164 timestamps use a fixed UTC offset;
  DST/IANA zone rules are not applied. Related:
  `esl-insert/src/syslog_listeners.rs:368/:1658` — named IANA timezones are
  a fatal startup error (only `UTC`/`Local`/fixed offsets are supported).

**Storage engine (esl-logstorage)**

- `datadb.rs:24` (+ `:633`, `block_stream_merger.rs:3`, `rows.rs:329`,
  `storage.rs:523/:742/:179`) — **log deletion is not executed**: delete
  tasks register and persist, but the `runDeleteTasksWatcher`/
  `processDeleteTask` merge path that drops matching rows is deferred, so
  deleted rows remain searchable.
- `indexdb/mergeset/table.rs:23` *(verify)* — read-only mode / low-disk
  parking (`isReadOnly`) is not ported for the mergeset table.
- `indexdb/mergeset/table.rs:1438` — `DataBlocksCache*`/`IndexBlocksCache*`
  metrics are absent/zero (the global mergeset block caches are omitted).
- `indexdb/mergeset/table.rs:440` — "skipping too long item" is logged
  unthrottled vs Go's 5s throttle.
- `query_stats.rs:109` *(verify)* — `QueryStats.writeToPipeProcessor`
  surface pending (per-query stats emission differences).
- `encoding.rs:405` *(verify)* — zstd frames are produced by libzstd, not
  klauspost/gozstd: fully interoperable, but stored bytes/sizes differ from
  a Go-written directory (also `esl-common/src/encoding/zstd.rs:10/:53`).
- `delete_task.rs:101` *(verify)* — an empty delete-task list serializes as
  `[]` where Go writes `null` (on-disk JSON bytes differ; both readable).
- `part_header.rs:139` *(verify)* — `metadata.json` parsing rejects unknown
  members containing nested arrays/objects that Go skips.
- `rows.rs:337` *(verify)* — duplicate field names sorted with a different
  stability guarantee; tie order can differ.

**HTTP server, TLS, flags, logging (esl-common)**

- `httpserver.rs:41/:1375` — **HTTP auth is not enforced**: basic auth,
  `-*AuthKey` (including `-metricsAuthKey`/`-flagsAuthKey`) and
  `/debug/pprof` are omitted; all requests are allowed. gzip response
  compression and connection-deadline jitter are also absent. Mirrored at
  `esl-storage/src/lib.rs:639` (httpAuth fallback unported).
- `httpserver.rs:479-620` *(verify)* — request-body gzip/zstd/deflate is
  decompressed into memory without a cap before handler-level size checks
  (Go caps during the read) — decompression-bomb exposure on insert paths.
- `httpserver.rs:128` (+ `es-logs/src/main.rs:34`,
  `esl-agent/src/main.rs:8`) — `-httpListenAddr` accepts a single address;
  Go supports multiple listeners (+ `useProxyProtocol` per listener).
- `httpserver.rs:644` *(verify)* — responses are buffered and sent with
  Content-Length instead of Go's streaming writer (except `/tail`); no
  mid-response abort. Same pattern at `esl-select/src/internalselect.rs:31`
  and `esl-select/src/logsql.rs:998`.
- `httpserver.rs:109` — extra 10s TLS-handshake timeout Go does not have.
- `tlsutil.rs:9/:65` — TLS 1.0/1.1 unsupported (min version silently clamps
  to 1.2); CBC/static-RSA cipher-suite names rejected; trust roots come from
  bundled webpki-roots rather than the OS store.
- `cgroup.rs:23`, `memory.rs:29`, `filestream.rs:4`, `fs/mod.rs:4` — several
  Go-emitted metric series are missing (`process_cpu_cores_available`,
  `process_memory_limit_bytes`, `*_filestream_*`, fs/nfs/mmapped series);
  `flagutil.rs:112` — flag gauges cover only read/set flags, not all
  declared flags. The whole namespace is intentionally rebranded
  `vm_*`→`esm_*`.
- `memory.rs:13` — `-memory.allowedBytes` rejects KB/MB/GiB suffixes.
- `flagutil.rs:16` — repeated scalar flags keep the last value where Go
  array flags append; `flagutil/array.rs:204/:376` — `\xHH` decode and
  negative-duration clamping differ; `flagutil/duration.rs:233` — Grafana
  `$__interval` pseudo-durations unsupported.
- `flagutil/password.rs:19/:150/:162` — `http(s)://` password sources are
  not fetched; Windows fallback password uses a non-crypto RNG *(verify)*.
- `logger.rs:32/:147` — only UTC `-loggerTimezone` supported (others panic);
  `:311/:500/:640` *(verify)* — arg-length truncation, multi-byte
  truncation and error-writer caller location differ.
- `stringsutil.rs:171` + `regexutil/gofold.rs:3` *(verify)* — Unicode
  case-insensitive matching diverges from Go's simple folding for a handful
  of code points (e.g. `İ`).
- `regexutil.rs:10` (+ `goparse.rs:7`, `gosyntax.rs:123`) — `\p{...}`
  classes are rejected (Go accepts); `\b` is Unicode-aware vs Go's ASCII;
  `regexutil.rs:461/:597` — `MustCompile` returns an error instead of
  panicking on those gaps; `gosyntax.rs:607` *(verify)* — simplified-regex
  `String()` may differ for exotic code points.
- `metrics/push.rs:444` *(verify)* — push-URL userinfo is not
  percent-decoded before basic auth.
- `fs/mod.rs:11` + `filestream.rs:152/:321` *(verify)* — file-close errors
  are swallowed where Go panics; `fs/mod.rs:175` *(verify)* — directory
  modes 0777&umask vs Go's 0755&umask.
- `buildinfo.rs:40` *(verify)* — version line not prepended to `-help`
  (also `es-logs/src/main.rs:199`, `eslogscli/src/main.rs:902`).
- `appmetrics.rs:12`, `metrics/process_metrics_linux.rs:319` *(verify)* —
  `vm_os_info`-equivalent lacks the Windows release label;
  `process_start_time_seconds` derivation differs slightly.

**Ingestion protections (esl-insert)**

- `common_params.rs:5` (+ `datadog.rs:13`, `journald.rs:13`, `otel.rs:77`,
  `syslog_listeners.rs:11/:1083`) — Go's `CanWriteData()` gate is never
  called: ingestion does not reject when storage is read-only/out of disk.
- `loki.rs:127`, `datadog.rs:7`, `otel.rs:93`, `loki_protobuf.rs:28/:104` —
  `-*.maxRequestSize` caps are missing (Loki JSON, DataDog, OTLP) or only
  enforced on the snappy path (Loki protobuf).
- `journald.rs:13`, `syslog_listeners.rs:30/:1112` — the
  `writeconcurrencylimiter` backpressure/queueing layer is not ported.
- `syslog_listeners.rs:1083` — no periodic background flush for long-lived
  syslog connections; rows become searchable only on buffer-full/close.
- `syslog_listeners.rs:25/:445/:539/:806` *(verify)* — unix-socket listeners
  are `cfg(unix)`-only, UDP4/TCP6 network selection flags are not honored,
  and unrecoverable accept errors back off instead of `Fatalf`.

**Query serving, agent, tools (esl-select / esl-storage / esl-agent / CLIs)**

- `esl-select/src/internalselect.rs:5` —
  `-internalselect.maxConcurrentRequests` limiter and its wait-summary
  metric are dropped.
- `esl-select/src/internalselect.rs:197` *(verify)* — multipart form parsing
  lacks Go's 10%-of-memory bound.
- `esl-select/src/logsql.rs:673/:760` *(verify)* —
  `-search.maxQueryTimeRange` cannot be enabled (flag unported).
- `esl-select/src/esmui_assets.rs:10/:117` *(verify, low)* — esmui static
  serving lacks ETag/Last-Modified/ranges; redirect keeps the raw query
  string.
- `esl-storage/src/lib.rs:783` *(verify)* — snapshots created for a
  disconnected client are kept (Go deletes them).
- `esl-agent/src/remotewrite.rs:225/:750/:147/:22` — `-remoteWrite.oauth2.*`
  and `-remoteWrite.proxyURL` are unsupported (fatal when set);
  `tlsHandshakeTimeout` folds into `sendTimeout` *(verify)*; shutdown grace
  differs *(verify)*.
- `esl-agent/src/filecollector.rs:330` *(verify)* — stricter glob validation
  than doublestar.
- `esl-agent/src/kubernetescollector.rs:18` *(verify)* — kubeconfig parsed
  with a minimal YAML subset.
- `eslogscli/src/main.rs:263/:169/:829`, `less_wrapper.rs:12/:97` — Ctrl+C
  kills the process instead of cancelling the in-flight query / returning to
  the prompt; no raw-mode line editing; Windows prints without paging.
- `eslogsgenerator/src/main.rs:600` *(verify)* — generator pushes over
  http:// only; query params not canonically encoded.

### (b) Mechanism divergences (identical observable behavior)

~1,000 of the 1,326 notes fall here; by category (examples, not exhaustive):

1. **Go type switches → object-safe trait hooks** on `Filter`/`Pipe`/stats
   (~35): the optimize passes, `initSubqueries` dispatch, stats label
   transforms (e.g. `pipe.rs`, `parser/query.rs:237`).
2. **Shallow `*Query`/filter sharing → render + re-parse clones** (~25):
   `Query::clone`, `clone_pipe`, rendered-text subqueries
   (`storage_search.rs:1009`, `filter_stream_id.rs:27`).
3. **`sync.Pool`/arena/`chunkedAllocator`/`atomicutil.Slice` dropped or
   replaced by `Mutex`-shards / thread-locals / owned values** (~80):
   results identical, only allocation behavior differs (`stats.rs:24`,
   `values_encoder.rs:178`, `esl-insert/src/journald.rs:248`).
4. **Goroutines/channels/context → threads, mpsc, condvars, stop tokens,
   upfront scheduling** (~55): `storage_search.rs:42/:857`,
   `esl-common/src/procutil.rs`, syslog/agent shutdown paths.
5. **Per-`valueType`/dict block fast paths folded into decoded per-row
   paths** (perf-only; identical results) (~25): filters and stats
   (`filter_range.rs:79`, `stats_count_uniq.rs:691`); tracked in the
   Layer-7 backlog above.
6. **Vendored Go libraries reimplemented with byte-identical output** (~40):
   fastjson subsets, quicktemplate JSON, `regexp/syntax` parser, itoa,
   civil-time math, xxhash streams.
7. **Byte/string ownership shims** — `[]byte` aliasing → owned
   `Vec<u8>`/`String`, unsafe string views → safe (~55).
8. **`(n, err)`/EOF/nil-receiver idioms → `Result`/`Option`** and
   error-message wording-only differences (~45).
9. **Helper homing** (a Go helper hosted in a different module until its Go
   home file exists) and module-layout merges (~50).
10. **Test provenance/adaptation** — upstream ships no `_test.go`, or the Go
    test drives the lexer/harness differently; port-only tests added (~140).
11. **Numeric/format parity notes** asserting bit/byte-identical output
    (`strconv` float formats, wrapping arithmetic, FMA) (~25).
12. **Raw-pointer single-thread contracts, `Send` impls, cache-line
    padding** documenting safety of the Rust translation (~15).

### (c) Deliberately N/A in Rust

- Go runtime surfaces: `go_*` metrics, `prometheus_histogram.go`,
  `debug.SetGCPercent`/GOGC (no-op), GOMAXPROCS beyond CPU-count parity,
  `fasttime` synctest variant (`esl-common/src/metrics.rs:11/:16`,
  `cgroup.rs:68`).
- Go-only unsafe string/byte aliasing tricks made unnecessary by ownership
  (`values_encoder.rs:1291`, `pattern.rs:339`).
- Upstream entry points with no single-node consumer: `pushmetrics.InitWith`
  / `StopAndPush` (vmctl/vmbackup-only), Splunk HEC raw mode (absent in
  v1.51.0 upstream too).
- **Cluster mode is explicitly gated off** (`esl-storage/src/lib.rs:22/:313`:
  `-storageNode` fails fast), so divergences confined to the ported-but-
  unreachable `netinsert`/`netselect` client modules (dropped inter-node
  cancellation, buffered node responses, 30s request cap, storageNode-side
  hidden-fields) are latent in the shipped binary. `allow_partial_response`
  is parsed for protocol compatibility and dropped (meaningless without
  storage nodes).
- Windows-target notes where Go has the same platform behavior (cgroups
  no-op, unix sockets absent).
