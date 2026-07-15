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
| lib/flagutil | ported | incl. flag registry for /flags + esm_flag gauges (full VisitAll coverage via linkme register_flag! at each Flag static) |
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
  runner borrows the caller's run-net-query callback. `visitSubqueries`-based
  propagation is wired: `AddTimeFilter`/`AddExtraFilters` descend into
  subqueries and subqueries inherit the parent's scalar query options. Still
  deferred: `stream_context` runQuery wiring on the NetQueryRunner local-pipes
  path (the local `Storage::run_query` path IS wired).
- filter_and/not Display omit parens; filter_phrase Display incomplete quoter.

## Parity ledger (v1.51.0)

Definitive audit of every `PORT NOTE` in the workspace (1,326 notes reviewed
file-by-file on 2026-07-12, after the final parity sweep closed
`optimizeUniqLimitPipes`, the `pipeFieldNames` first-pipe fast mode, the
`math`/`eval` parse grammar, `initStreamContextPipes` wiring on the local
run-query path, and the shared `tryParseNumber` "inf" handling). "Full
parity" is measured against section (a): every entry there is a way the Rust
port can behave observably differently from upstream Go v1.51.0. As of the
2026-07-13 verification audit (below) every entry has been confirmed against
the pinned Go checkout, so the earlier conservative *(verify)* tags are gone —
what remains in section (a) is confirmed-present divergence.

> **Verification audit (2026-07-13).** Every remaining section-(a) bullet —
> the 34 `*(verify)*` entries plus the untagged ones — was re-checked
> file-by-file against the pinned Go v1.51.0 checkout. Section (a) below now
> reflects only divergences confirmed to still exist in the current code; each
> retained `*(verify)*` tag was resolved (kept as a confirmed divergence or
> struck as at-parity), so the tag no longer appears.
>
> **Genuinely closed and struck** (code now matches Go): basic-auth/`-*AuthKey`
> enforcement; `replace`/`replace_regexp` `if (...)`; `hiddenFieldsFilter`;
> `!!foo` collapse; And/Or/Not paren rendering + pipe/stats-name token quoting
> (fixing the facets re-parse case); the full HTML-entity table; `stats switch`
> was *not* closed (see below); `block_stats` on-disk reporting; `min`/`max`
> companion-field fast path; `quantile` fastrand port; `stddev` `Display`;
> `uc`/`lc` simple case mapping; regexutil `\p{}`/`\b`; repeated-flag/negative-
> duration/`$__interval` semantics; `-loggerTimezone` UTC/Local; `-memory.allowedBytes`
> suffixes; the previously-missing `process_*`/`filestream`/`fs` metric series;
> the internalselect concurrency limiter + its 10%-memory multipart bound;
> storage-side httpAuth fallback; mergeset `isReadOnly` (Go never sets it);
> `query_stats.writeToPipeProcessor`; and eslogscli Ctrl+C query cancellation.
>
> **Corrections to the prior (over-optimistic) closure note** — these were
> claimed closed but the audit found them still OPEN, so they remain in
> section (a): `stats switch(...)`
> (still rejected); `pattern.rs` `\x`≥0x80 (still UTF-8-encoded).
>
> **Items closed this session** (with tests / gate-verified): (1) log deletion
> including stream-filtered rows (drop path used the wrong `_msg` canonicalizer;
> full Go delete test passes); (2) syslog idle-connection periodic flush
> (`syslog_listeners::process_stream` now drives `flush_if_idle` from a
> `thread::scope`d `AddJitterToDuration(1s)` flusher over an `mpsc` stopCh,
> mirroring Go's `initPeriodicFlush` for `isStreamMode=true`); (3) iff-nested
> `in(subquery)` propagation via `visit_subqueries_in_shared_filter`; (4)
> `CanWriteData()` now gates every ingest path — the six that were missing it
> (splunk, native-insert, OTLP, journald, syslog, internal-insert) now call it
> like Go; (5) the OTLP ingest path now enforces `-opentelemetry.maxRequestSize`
> (64MiB) like its Loki/DataDog siblings; (6) every bulk insert handler
> (Loki/DataDog/OTLP/Splunk/native/internal) now reads through
> `read_full_body_limited`, which caps the decompressed size *during* the read
> like Go's `ReadUncompressedData` — closing the decompression-bomb exposure
> where the body was fully materialized before the size check; (7)
> `rate()`/`rate_sum()` now receive their per-second step (Go
> `initStatsRateFuncSteps` → `pipeStats.initRateFuncs`, wired through
> object-safe `Pipe`/`StatsFunc` hooks and applied for both parse-time `_time`
> filters and HTTP `AddTimeFilter`), so their computed values are normalized
> like Go instead of skipping the divide; (8) `AddTimeFilter` and
> `AddExtraFilters` now propagate into `in(...)`/`join`/`union` subqueries via
> `visit_subqueries` (Go `AddTimeFilter`/`AddExtraFilters`), so subqueries no
> longer scan an unbounded time range or bypass extra/security filters — the
> extra filter is rendered once and re-parsed per subquery (single-owner
> filters), honoring each subquery's own `ignore_global_time_filter`/`time_offset`.

### (a) Observable behavioral divergences

**Query semantics (esl-logstorage)**

- `parser/query.rs` — `options(global_filter=...)` is now ANDed before the
  query filter (and propagated into subqueries) at parse time, matching Go's
  `getFinalFilter`, so results are correct. Two residual differences: it
  re-renders with the filter inlined rather than as `options(global_filter=...)`
  (Go keeps the option and ANDs it per-search), and a subquery that sets its
  *own* `global_filter` is not composed (only the top-level option is).
- `parser/query.rs` `visit_subqueries` — scalar query options ARE now inherited
  into nested subqueries (Go copies the parent `queryOptions` via the lexer
  options stack): `visit_subqueries` wraps the visit closure so each subquery
  takes the parent's `concurrency`/`parallel_readers`/`ignore_global_time_filter`/
  `allow_partial_response`/`time_offset` for the fields it did not set itself,
  cascading through the recursion. So a subquery whose *parent* set
  `options(ignore_global_time_filter=true)` now suppresses the propagated
  `_time` filter like Go. One ultra-narrow residual: the value is inherited
  *after* the subquery is re-parsed, so a literal `_time`/`day_range`/
  `week_range` filter already inside a subquery is not shifted by an inherited
  parent `time_offset` (re-applying it would double-shift across the parse-time
  and `add_time_filter` visits); the inherited offset value still shifts the
  added global `_time` filter and rate-step normalization.
- `pipe_sort.rs:26/:536` — the sort state-size budget charges the copied
  per-value byte lengths where Go charges the cloned block's buffer
  capacities (and shares value bytes in the `byFields` path), so the port
  accounts more memory per block and can cross the identical 20% threshold on
  smaller input — a borderline sort errors in the port but not in Go.

**LogsQL text rendering / round-trip (esl-logstorage)**

- `stream_filter.rs` — the LogsQL lexer carries a Go-exact raw-byte token payload
  (`Lexer.token_bytes`, `strconv.Unquote` semantics: double-quoted `\xNN`≥0x80
  IS the raw byte), consumed by the phrase-filter family
  (`phrase`/`exact`/`prefix`/`exact_prefix`/`seq`/`i()`) end-to-end with
  lossless render→re-parse — as are `in()`/`contains_*` literal values,
  `string_range` bounds (incl. the `>`/`>=`/`<`/`<=` string forms), `*substr*`,
  `*_common_case` (Go `strings.ToUpper`-exact case expansion),
  `json_array_contains_any`, stream-filter tag values (`{label="value"}` —
  byte-exact `=`/`!=`, Go byte-wise `QuoteMeta` for `in`), and the `replace`
  pipe's from/to. Quoted field NAMES from query text are raw bytes too (the
  `FieldFilter` trait, `FilterGeneric.field_name`, the grammar's name threading,
  `prefix_filter`, and every pipe/stats by-field config are byte-native, with
  the redundant `_bytes` lookup twins consolidated into single byte APIs) — a
  quoted `"\xff"` name matches an ingested raw-byte name end-to-end, and name
  rendering quotes invalid bytes as `\xNN` like Go's `needQuoteToken`
  (`RuneError` ⇒ quoted). The `extract`-pipe PATTERN is byte-native too
  (`parse_pattern(&[u8])`, token read via `next_compound_token_bytes`), so a
  `\xNN` escape in an extract pattern literal denotes a raw byte and matches raw
  value bytes. Residuals (each PORT-NOTEd): the cluster netselect form channel
  rejects non-UTF-8 field-name args (String-typed multipart seam; single-node is
  fully raw-byte); `re()`/`pattern_match*` pattern *text* stays scalar/`&str`
  (str-native engines — the matched *values* are byte-native).

**Input handling edge cases (esl-logstorage)**

- Field **values** are now raw bytes end-to-end (`rows::Field.value: Vec<u8>`,
  `block::Column.values: Vec<Vec<u8>>`), matching Go's arbitrary-byte strings:
  invalid UTF-8 survives ingest→storage→filter→query-result byte-identically
  (round-trip tests at every layer + a jsonline e2e). The block-level filter
  matchers, tokenizer/bloom hashing, and phrase-boundary checks are byte-native
  ports of the Go code (incl. Go's `RuneError` boundary special-case); the
  jsonline/elasticsearch/splunk/journald/loki-protobuf/OTLP/datadog/native/
  internal ingest paths and the tail/stats/facets/lastn output paths preserve
  bytes; OTLP now reads value paths via byte accessors where it previously
  **rejected** invalid UTF-8. The syslog parse chain is byte-native too
  (`syslog_parser::parse(&[u8])`, listeners pass raw framed bytes,
  `unpack_syslog` unpacks stored bytes directly), so invalid UTF-8 in syslog
  message content is preserved verbatim. Field **names** are raw bytes too
  (`Field.name: Vec<u8>` plus the column-name chain — `ColumnHeader`,
  `BlockResultColumn`, the interned part column-names table — so an
  invalid-UTF-8 name round-trips ingest→disk→query byte-identically; syslog
  SD-IDs and Loki protobuf label names came along for free). The `logfmt`
  parser is byte-native too (`LogfmtParser::parse(&[u8])`), so RFC5424 SD field
  values and `unpack_logfmt` values with invalid UTF-8 are preserved verbatim.
  Remaining lossy (each PORT-NOTEd in place, none on a stored/returned name or
  value): `_stream`/`_stream_id` rendering (validated printable text);
  `any_case` filters lossy-lowercase — which IS Go (`strings.ToLower` maps
  invalid bytes to `U+FFFD`); display/error text. Regex (`re()`, stream-tag
  `=~`/`!~`) and `pattern_match*` matching are now byte-native
  (`regex::bytes::Regex` / `PatternMatcher::matches_bytes` on raw value bytes,
  no lossy view) — see the regex invalid-haystack note below.
- `pattern.rs` — the `extract` pattern path is byte-native: double-quoted
  `\x`/octal escapes ≥0x80 emit the raw byte (Go `strconv.Unquote` exactly;
  single-quoted keeps `AppendRune` UTF-8 encoding, also Go-exact) and values
  with invalid UTF-8 are matched/extracted verbatim. The retained `String`
  unquote forms (used by stream-tag/logfmt/storage_search token paths) keep the
  scalar-encoding for `\x`≥0x80 — that residual moves to the lexer/token-layer
  follow-up (`Lexer.token` is a `String`, so a raw-byte query token is not yet
  representable there).
- `syslog_parser.rs`, `esl-insert/src/syslog_listeners.rs` — a named IANA
  `-syslog.timezone` (e.g. `America/New_York`) is now supported on Unix: it is
  loaded DST-aware from the system zoneinfo database (`crate::tzdata`) and the
  RFC3164 timestamp's offset is resolved per timestamp via
  `Location::offset_for_wall_secs` (Go `time.Date`), so it is no longer a fatal
  startup error. Fixed forms (UTC/`Etc/GMT±N`/`±HH:MM`/`Local`) keep the cheap
  fixed-offset path. Residual: Windows named zones remain unsupported (no system
  zoneinfo; the port does not bundle tzdata), and `Local` still samples a single
  offset at startup.

**Storage engine (esl-logstorage)**

- `indexdb/mergeset/table.rs:1438` — `DataBlocksCache*`/`IndexBlocksCache*`
  metrics are absent/zero (the global mergeset block caches are omitted).
- `encoding.rs:405` — zstd frames are produced by libzstd, not klauspost's
  pure-Go encoder (used by `CGO_ENABLED=0` release binaries), with different
  level bucketing: fully interoperable (both emit standard zstd frames and
  decode each other), but stored compressed bytes and part
  `CompressedSizeBytes` differ from a Go-written directory (also
  `esl-common/src/encoding/zstd.rs:10/:53`).
- `delete_task.rs:101` — a genuinely nil delete-task list serializes as `[]`
  where Go's `json.Marshal` writes `null` (on-disk bytes differ, both
  readable; an empty-but-non-nil list is `[]` on both sides).
**HTTP server, TLS, flags, logging (esl-common)**

- `httpserver.rs:1375` — basic auth and the `-*AuthKey` flags
  (`-metricsAuthKey`/`-flagsAuthKey`) ARE enforced (`check_basic_auth`/
  `check_auth_flag`, with the storage-side fallback at
  `esl-storage/src/lib.rs:639`); what remains unported is `/debug/pprof` +
  `-pprofAuthKey` (Go's runtime pprof has no clean Rust equivalent). gzip
  **response** compression now matches Go's gzhttp wrapper (1024-byte min,
  content-type filter, `Vary`/`Content-Encoding`), and the per-connection
  timeout + jitter (`CONN_TIMEOUT`, `esm_http_conn_timeout_closed_conns_total`)
  is ported.
- `es-logs/src/main.rs`, `esl-agent/src/main.rs` — `-httpListenAddr` accepts
  multiple addresses (an `ArrayString`), each started via
  `httpserver::serve_listener` with its own indexed `-tls*` config and
  `-httpListenAddr.useProxyProtocol` (PROXY protocol v2 is read and stripped
  before the TLS/HTTP bytes, recovering the real client address; v1 is rejected,
  matching Go's v2-only `netutil` implementation).
- `httpserver.rs:644` — responses are buffered and sent with Content-Length
  instead of Go's streaming/flushing writer (except `/tail`'s `flush_chunk`);
  no mid-response abort. Same pattern at `esl-select/src/internalselect.rs:31`
  and `esl-select/src/logsql.rs:998`.
- `httpserver.rs:109` — extra 10s TLS-handshake timeout Go does not have
  (imposed by the fixed worker pool).
- `flagutil.rs` — the metric namespace is intentionally rebranded `vm_*`→`esm_*`.
  (The `esm_flag` gauges now enumerate **every** declared flag like Go's
  `flag.VisitAll`, via a `linkme` distributed-slice registry populated by the
  `register_flag!` macro at each `Flag` static — no longer a read/set subset.
  The previously-missing `process_cpu_cores_available`,
  `process_memory_limit_bytes`, `*_filestream_*`, and fs/nfs/mmapped series
  are also registered.)
- `flagutil/password.rs:19` — `http(s)://` password sources are not fetched
  (the flag layer has no HTTP client dependency); such a source falls back to
  the stored random value. (The generated random password now uses a
  cryptographically secure RNG on both platforms — `/dev/urandom` on Unix,
  `BCryptGenRandom` on Windows — and panics on read failure like Go's
  `crypto/rand`, instead of the previous non-crypto Windows fallback.)
- `logger.rs:311/:500/:640` — per-arg length truncation, multi-byte truncation
  (lossy `U+FFFD` vs Go's raw byte slice), and the error-writer caller location
  differ (part of the raw-byte `String`-vs-`[]byte` family). (Named IANA
  `-loggerTimezone` values are now supported on Unix — they load from the system
  zoneinfo database via `crate::tzdata` and the offset is looked up per log
  timestamp, so DST is honored; only Windows named zones remain unsupported, as
  the port does not bundle tzdata.)
- `regexutil.rs:461/:597` — `MustCompile` returns an error instead of panicking
  (deliberate: no `expect`-panic API). (`\p{...}` classes are now accepted and
  `\b` is ASCII like Go. The simplified-regex `String()` printable-rune escaping
  now matches Go exactly — see the `strconv.IsPrint` note below.)
- `regexutil` (invalid-UTF-8 haystacks) — regex matching runs on raw value
  bytes via `regex::bytes::Regex` (Go `regexp` matches byte payloads too), so
  valid-UTF-8 haystacks and literal/positive-class matching over invalid bytes
  are byte-exact. One narrow residual: Go decodes each invalid byte as `U+FFFD`
  (`utf8.DecodeRune`), so rune-oriented constructs (`.`, negated classes,
  `\p{...}`) match such bytes; `regex::bytes` in its default Unicode mode only
  matches well-formed UTF-8 there, so they don't. Irreducible without a custom
  rune-stepping engine; pinned by `test_bytes_regex_invalid_utf8_probe`.
- `fs/mod.rs:11` + `filestream.rs:215/:389` — file-close errors are swallowed
  (file closed on `Drop`) where Go's `MustClose` panics. (The `must_mkdir`
  0777-vs-0755 divergence is closed: `must_mkdir` now sets mode `0o755`
  explicitly via `DirBuilderExt`, matching Go's `os.MkdirAll(path, 0755)`
  under any umask.)
- `tlsutil.rs:9/:65` *(deliberate — rustls-imposed)* — TLS 1.0/1.1
  unsupported (min version clamps to 1.2); CBC/static-RSA cipher-suite names
  rejected; trust roots come from bundled webpki-roots rather than the OS
  store.

**Ingestion protections (esl-insert)**

- The syslog stream path omits the `writeconcurrencylimiter` backpressure
  layer Go applies, but is at parity there (ingestion is bounded by the
  listener/reader thread pool instead). (The journald HTTP ingest now wraps its
  body with `writeconcurrencylimiter::get_reader` like its
  jsonline/elasticsearch siblings.)
- `syslog_listeners.rs:25/:445/:539/:806` — unix-socket listeners are
  `cfg(unix)`-only, UDP4/TCP6 network-selection flags (`-enableTCP6`) are not
  honored (the stack is derived from the bind address), and unrecoverable
  accept errors back off instead of `Fatalf`.

**Query serving, agent, tools (esl-select / esl-storage / esl-agent / CLIs)**

- `esl-select/src/esmui_assets.rs` — esmui static serving honors a single-range
  `Range:` request with `206 Partial Content` + `Content-Range`/`Accept-Ranges`
  (Go's `http.ServeContent`), and answers an unsatisfiable range with `416`.
  Residual: a *multi-range* request (comma-separated) falls back to the full
  `200` body where Go emits `multipart/byteranges` — browsers never send this
  for the small esmui JS/CSS/HTML/image assets. The `/select/esmui` redirect
  re-encodes the query like Go's `Form.Encode()` (`Request::form_encoded`: keys
  sorted, keys/values percent-escaped).
- `esl-agent/src/remotewrite.rs` — `-remoteWrite.oauth2.*` IS supported: a
  faithful `client_credentials` token source (`crate::oauth2`) fetches, caches
  (with x/oauth2's 10s refresh margin), and applies a bearer token, sending the
  `base64(url.QueryEscape(id):url.QueryEscape(secret))` Basic header like
  x/oauth2's `AuthStyleInHeader`. `-remoteWrite.proxyURL` IS supported for
  `http://` (HTTP CONNECT, RFC 9110) and `socks5://` (RFC 1928 + RFC 1929
  user/pass) proxies to https/http targets (`esl-storage/src/proxy.rs`,
  `connect_via_proxy` tunnels through the existing TCP+rustls path). Residuals:
  an `https://` **proxy** (TLS-to-the-proxy, i.e. TLS-over-TLS) is rejected —
  the shared connect path returns a concrete `TcpStream` and `tlsutil` consumes
  one, so nesting can't be represented without boxing the whole request path;
  `-remoteWrite.tlsHandshakeTimeout` folds into `sendTimeout` (one connection
  per request, no separate handshake timeout); and shutdown abandons an
  in-flight request after the full `sendTimeout` rather than Go's fixed 5s
  grace.
- `eslogscli/src/main.rs` (`less_wrapper.rs:103`) — raw-mode line editing (in-line
  arrow-key/Ctrl-A/E/K/U/W editing, history recall, Ctrl+C clears the line and
  returns to the prompt) now works via `rustyline`, matching Go's
  `ergochat/readline`; piped/non-interactive input keeps the byte-identical
  plain-line path, and the on-disk history file keeps Go's `strconv.Quote`
  format. Residual: Windows prints without a `less` pager (no ubiquitous `less`).

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
   civil-time math, xxhash streams, and `strconv.IsPrint`/`unicode.IsPrint`
   (`strconv_isprint.rs` — compact `isprint.go` tables cross-checked over all
   `0x0..=0x10FFFF`, so JSON key re-quoting, `go_quote`, and simplified-regex
   `String()` escape printable runes exactly like Go).
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
13. **Port is stricter or more precise where Go is unspecified, lax, or
    approximate** — output stays within Go's contract (or strictly improves on
    it), so these are not defects against upstream behavior (4):
    - `rows.rs:442` — duplicate field names sort *stably* (`sort_by`) where Go's
      `sort.Slice` leaves tie order **unspecified**; the port's order is one
      valid realization of Go's contract (differs only under a byte-exact
      differential over duplicate-name input).
    - `metrics/process_metrics_linux.rs:319` — `process_start_time_seconds` is
      derived from the exact kernel start (`/proc` btime+starttime), *more*
      accurate than Go's package-init `time.Now()` approximation;
      semantically the same metric. (`esm_os_info`'s Windows release label
      also matches Go — `major.minor.build` from `RtlGetVersion`.)
    - `parser/parse_stats.rs` — `stats switch(...)` computes **identical
      results** to Go; because `Box<dyn StatsFunc>` is not `Clone` it is
      expanded at parse time into equivalent `if`-guarded funcs, so only the
      re-rendered query text differs (`count(*) if (x) as a, count(*) if
      (!(x)) as b` vs `switch(...)`).
    - `esl-agent/src/filecollector.rs:330` — glob validation scans the whole
      pattern, so malformed patterns Go's `doublestar.PathMatch` would accept
      via early-segment short-circuit are rejected as `ErrBadPattern`; affects
      only malformed configs (fail-fast, arguably safer).

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
