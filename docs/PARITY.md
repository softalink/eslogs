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
| lib/flagutil | ported | |
| lib/envflag | ported | |
| lib/buildinfo | ported | |
| lib/fs | ported | cross-platform file ops |
| lib/fs/fsutil | ported | |
| lib/filestream | ported | |
| lib/regexutil | ported | |
| lib/httpserver | ported | threaded server (worker pool sized to available_cpus); no server-side TLS (single-owner rustls session vs the tri-handle conn plumbing, PORT NOTE), no auth/pprof (PORT NOTE); /metrics serves the full registry via lib/appmetrics + esm_http_* request metrics |
| lib/httputil | ported | GetRequestValue/GetArray/GetBool/GetInt/CheckURL over Request |
| github.com/VictoriaMetrics/metrics | ported | esl-common/src/metrics.rs: Set + default set, Counter, FloatCounter, Gauge, Histogram (vmrange buckets), Summary (incl. inline valyala/histogram+fastrand), validator, WritePrometheus, process metrics (linux /proc + windows); push.go / prometheus_histogram.go / go_metrics.go unported (ignore list) |
| lib/appmetrics | ported | esl-common/src/appmetrics.rs: /metrics payload (registry + process + esm_app_*/esm_os_info); flag export + -metrics.exposeMetadata flag PORT-NOTEd |
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
net_query_runner (cluster query splitting) stubbed for single-node. Context
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
kubernetes collector, eslogscli -tls*) and server side (-syslog.tls*); the
one exception is httpserver's -tls serving flags, omitted with a PORT NOTE
(single-owner rustls session vs the server's tri-handle connection plumbing).
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

- **indexdb / mergeset (2026-07-07):** EsLogs' `indexdb` sits on
  Softalink LLC `lib/mergeset` (~4500 LOC LSM engine). We did NOT port
  mergeset; instead `indexdb/mergeset.rs` is an API-compatible internal
  sorted-items store (in-memory sorted store persisted to a single
  length-prefixed `items.bin`, with lower-bound seek cursor matching
  mergeset's Seek/NextItem/FirstItemWithPrefix). The indexdb *item byte
  encoding* (nsPrefix+tenantID+tag/streamID) is defined in indexdb.go and is
  preserved exactly, so query semantics match upstream. **Implication:** the
  on-disk `indexdb/` directory is NOT byte-compatible with upstream mergeset
  parts. This is acceptable because the logs-benchmark runs each server on a
  fresh data dir. RISK to revisit in Layer 7 optimization: the store keeps the
  index in RAM and flushes as one blob — fine while stream cardinality is low
  vs log volume, but if the disk-usage or RSS metric proves sensitive, port
  real mergeset. All other formats (streamID, stream-tags canonical, tag
  encoding, cache keys) remain byte-identical to upstream.

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
  `field_names_string`/`is_single_field`, private `try_parse_number` copies —
  each ported independently by parallel agents at distinct module paths (compiles
  fine). Consolidate into one shared location (e.g. a stats_util module or their
  true Go home once pipe_sort/parser land).
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

storage_search.rs ported DataBlock/ValueWithHits + block_search/block_result
block-read wiring + hits_map, but the end-to-end RunQuery spine is DEFERRED:
- Query accessors needed (parser deferred as part of optimize()): get_final_filter,
  get_needed_columns (walk pipes' update_needed_fields), get_filter_time_range
  (default full range = correct, no partition time pruning), get_common_stream_filter/
  get_stream_ids (stream pre-filter; scan-all fallback ok).
- Search plumbing not yet on types: getPartsForTimeRange (datadb), part.searchBy
  {Tenant,Stream}IDs, partition.search, datadb.search, worker-thread block iteration.
- storage.rs Storage::run_query is a PORT-NOTE stub.
INGESTION fully wired → 4/5 benchmark metrics don't need this; only query latency does.

## Parser deferrals (Layer-7 / correctness backlog)
- optimize(): `removeStarFilters` IS ported (perf-critical: `*` → FilterNoop,
  or-with-noop collapse, and-noop drop) via object-safe hooks on `Filter`
  (`is_match_all` / `take_or_children` / `take_and_children`). Still deferred:
  and/or flattening, stream-filter merging, pipe merges (offset/limit/uniq/
  filter, `q | filter` into q.f) — esl-select composes the merged
  sort-offset-limit form textually instead. Queries run correctly either way.
- NOT-IN-GO query fast paths (perf, benchmark-driven): desc-time top-N block
  skipping (`PipeProcessor::block_skip_check` + newest-first scheduling +
  global full-heap threshold in pipe_sort_topk), monotone-timestamps early
  break in topk, zero-copy bloom probes (`ReaderAt::mmap_slice` +
  `BloomFilter::bytes_contain_all`).
- math/eval pipe parser errors; stats switch errors.
- Subqueries ARE ported: `in(<subquery>)` / `contains_any(<subquery>)` /
  `contains_all(<subquery>)` / `_stream_id:in(<subquery>)` parse into
  rendered-text subqueries (`InValues::q_text`, `FilterStreamID::q_text`,
  `PipeJoin`/`PipeUnion::query_text`) and are resolved before the search by
  `storage_search::init_subqueries` (Go `initSubqueries`/`initFilterInValues`/
  `initJoinMaps`/`initUnionQueries`). Still deferred: `visitSubqueries`-based
  propagation (`AddTimeFilter`/`AddExtraFilters`/`optimize` do not descend into
  subqueries), `stream_context` runQuery wiring (`initStreamContextPipes`) and
  the eager cluster mode (`initSubqueries(..., eagerExecute=true)` with
  `net_query_runner`).
- filter_and/not Display omit parens; filter_phrase Display incomplete quoter.
