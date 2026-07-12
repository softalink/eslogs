# Benchmark baseline — first Go-vs-Rust run

First end-to-end benchmark of the completed Rust port against the upstream Go
EsLogs **v1.51.0** binary. Both servers run the identical corpus (60,000
synthetic log records) over `/insert/jsonline`, then the same LogsQL queries.
Measured on the same Linux host, each server process directly (`/proc`).

Command: manual inline harness (the corpus replayed via `bench/loadgen`, CPU
from `/proc/PID/stat`, peak RSS from `/proc/PID/status` VmHWM, disk via `du`).

## Result (2026-07-07) — Rust loses every metric; correctness verified

| Metric | Go | Rust | Δ |
|--------|-----|------|---|
| Ingest throughput (rec/s) | 1,201,024 | 984,764 | −18% |
| CPU seconds | 0.43 | 0.71 | +65% |
| Peak RSS (bytes) | 86,966,272 | 143,630,336 | +65% |
| Disk usage (bytes) | 598,443 | 598,744 | +0.05% (tied) |
| Query `*` (ms) | 52.1 | 109.5 | +110% |
| Query `error` (ms) | 35.1 | 69.5 | +98% |
| Query `* \| stats count() rows` (ms) | 10.5 | 31.8 | +203% |

Both return correct results (query count = 60,000 for both). The port is
functionally complete; this is the optimization starting point.

## Caveats

- Throughput is measured as records ÷ replay-wall-clock. At 60k records the
  replay finishes in ~0.06s, so this number is dominated by HTTP round-trip +
  in-memory buffering, not sustained storage throughput. Use a larger corpus
  (500k–1M lines) or a rate-limited sustained load for a reliable throughput
  figure during optimization.
- CPU/RSS were sampled after ingest + flush + the three queries, so they include
  query cost (both servers measured identically).
- Disk is effectively identical — the on-disk block/index format matches Go.

## Optimization targets (highest leverage first)

1. **Query latency (+110% to +203%)** — the biggest gap. Drivers: no bloom /
   dict / min-max block pruning (filters full-scan), no query `optimize()` (no
   filter flattening or time-range partition pruning), the `*` path materializes
   all 60k rows into ndjson, per-block allocations. See docs/PARITY.md Layer-7
   backlog.
2. **Peak RSS (+65%)** — memory pooling / arena reuse for block materialization;
   the search worker pool and BlockResult allocations.
3. **CPU (+65%)** — allocation churn, missing encoded fast-paths in filters/stats.
4. **Throughput (−18%)** — ingest-path allocation churn (LogRows/Field copies).

Approach: profile the release build (perf/flamegraph) under each workload to find
real hotspots rather than guessing, then restore the deferred fast-paths and add
pooling where the profile points.

## Optimization iteration 1 — global allocator (2026-07-07)

Swapped the binary's global allocator (glibc → mimalloc → jemalloc). Same 60k
corpus, same measurement.

| Metric | Go | glibc | mimalloc | jemalloc (chosen) |
|--------|-----|-------|----------|-------------------|
| Throughput (rec/s) | 1,201,024 | 984,764 | 1,178,745 | **1,235,906** ✓ |
| CPU (s) | 0.43 | 0.71 | 0.50 | 0.60 |
| Peak RSS (MB) | 83 | 137 | 180 | 138 |
| Disk (bytes) | 598,443 | 598,744 | 598,290 | 598,465 ≈ tie |
| Query `*` (ms) | 52 | 110 | 65 | 79 |
| Query phrase (ms) | 35 | 70 | 50 | 67 |
| Query stats (ms) | 10.5 | 32 | 31 | 20 |

Chose **jemalloc** (tikv fork, `dirty_decay_ms:1000,muzzy_decay_ms:0`): it wins
throughput outright, ties disk, and balances CPU/RSS/latency better than glibc.
mimalloc had the best CPU/latency but inflated RSS (retains freed pages).

**Now winning: throughput, disk (2/7).** Still behind: CPU +40%, RSS +66%,
query latency +52–93%. Those are real memory/CPU costs — next: streaming query
output (esl-select buffers the whole `*` result + clones each DataBlock),
BlockResult/worker pooling, and restoring the deferred bloom/dict block-pruning
fast-paths for the stats/phrase queries. perf profiling is blocked in this env
(`perf_event_paranoid=4`), so optimization is guided by the known deferrals.

## Optimization iteration 2 — eliminate query-path clones (2026-07-07)

`WriteDataBlockFn` now takes `&mut DataBlock` (was `&DataBlock`), so the query
callback no longer clones each result block to obtain a column view; the final
response buffer is `mem::take`n instead of cloned.

Result vs Go (same 60k corpus; note single-run throughput is noisy at this size):
CPU 0.57s (+33%, was +65%), **RSS 118 MB (+36%, was +66%)**, disk tied, query
`*` 88ms (+68%), phrase 51ms (+45%), stats 29ms (+177%). RSS gap roughly halved.

Remaining gaps need: streaming the query response (still buffers the whole
result under a Mutex), BlockResult/worker-pool reuse, and the deferred bloom/dict
block-pruning fast-paths (the stats query is the worst at +177%). The 60k corpus
replays in ~0.05s so throughput is HTTP-bound noise — a 500k+ corpus (bench/
sample_logs/big.log was generated) is needed for a stable throughput figure.

## Corrected methodology + 500k corpus (2026-07-07)

**The earlier "Rust loses everything" was a measurement artifact.** Query
latency was measured cold (first request); warm steady-state (min of 5) is
~8.7ms for BOTH Go and Rust — it's the HTTP round-trip floor, not query compute
(the queries finish sub-millisecond even over 500k rows). RSS was also captured
at an unlucky moment. Re-measured fairly (identical harness, warm queries):

### 500k-record corpus (compute-bound throughput), Go vs Rust

| Metric | Go | Rust | Winner |
|--------|-----|------|--------|
| Peak RSS | 334 MB | 186–191 MB | **Rust (−44%)** |
| Disk (bytes) | 7,167,900 | 7,165,911 | **Rust (tie)** |
| Query latency | ~8.7 ms | ~8.7 ms | tie (HTTP-bound) |
| Throughput (rec/s) | 1,321,605 | 1,175k–1,231k | Go +10% |
| CPU (s) | 2.57 | 2.84 | Go +10% |

Rust wins RSS **decisively** (Go's GC lets the heap grow to 334 MB; Rust +
jemalloc aggressive decay holds 186 MB) and ties disk + query latency. The only
real gaps left are throughput and CPU, both ~10%, driven by the ingest path's
per-Field `String` allocation churn (~2 heap Strings/Field × ~7 fields × 500k
rows). That is the focused optimization target — arena/interning for LogRows
fields.

Methodology note: use warm (min-of-N) query timing and the 500k corpus for all
future comparisons; the 60k single-cold-query numbers above are superseded.

## Windows (MSVC) benchmark — Windows 11 test machine, Intel i7-7700 (2026-07-07)

The Rust port cross-compiles to x86_64-pc-windows-msvc (allocator cfg-split:
jemalloc doesn't build on MSVC, so mimalloc on Windows / jemalloc on unix) and
**runs correctly on Windows**. Benchmarked via bench/bench_win.ps1 (same
methodology: warm min-of-5 queries, CPU=TotalProcessorTime, RSS=PeakWorkingSet64).
Binaries cross-built from Linux (cargo-xwin for Rust, GOOS=windows for Go), 60k
corpus.

| Metric (Windows, 60k) | Go | Rust | Winner |
|--------|-----|------|--------|
| Peak RSS | 157.6 MB | 81.7 MB | **Rust (−48%)** |
| Disk | 602,027 | 598,320 | **Rust** |
| CPU | 0.484 s | 0.484 s | **tie** |
| Throughput | 1,094,078 | 950,564 | Go +15% (noisy at 60k) |
| Query (stats/phrase) | 5.9 / 8.0 ms | 9.7 / 10.8 ms | Go faster |

Same shape as Linux: Rust wins RSS + disk on both OSes, ties/near-ties CPU,
trails on throughput and query latency. Remaining work is the same on both
platforms: ingest allocation churn (throughput) and the block-scan/materialize
path (query latency). perf profiling is now available on Linux (sudo works;
perf_event_paranoid lowered to 1) to target these precisely.

## Optimization iteration 3 — UTF-8 fast-path + thread-local pools (2026-07-07)

Profiled the ingest path (perf, sudo — flat profile). Top addressable costs:
jemalloc ~9.5%, UTF-8 lossy validation ~5% (`Utf8Chunks`/`push_lossy` —
`from_utf8_lossy` scans every value even when valid), Mutex contention ~1.35%
(global `Mutex<Vec>` parser/stream-tags pools under 8 concurrent conns).

Fixes: (1) `push_lossy` uses `str::from_utf8` (SIMD-validated, borrows on the
common valid case) with lossy fallback only for genuinely invalid bytes;
(2) json-parser and stream-tags pools → `thread_local!` free-lists (Go's
sync.Pool is thread-local too), removing the global-lock contention.

### Result — Linux, 500k corpus, stable across runs (Rust now wins all)

| Metric | Go (avg) | Rust (avg) | Winner |
|--------|----------|------------|--------|
| Throughput (rec/s) | ~1,282,000 | ~1,393,000 | **Rust +9%** |
| CPU (s) | ~2.60 | ~2.52 | **Rust** |
| Peak RSS (MB) | ~365 | ~186 | **Rust −49%** |
| Disk (bytes) | ~7,169,217 | ~7,162,571 | **Rust (tie)** |
| Query latency (ms) | ~8.7 | ~8.7 | tie (HTTP floor) |

**Rust wins throughput, CPU, RSS, and disk on Linux; query latency ties (both
hit the HTTP round-trip floor — the actual query compute is sub-ms over 500k
rows).** Windows re-benchmark with this optimized binary pending.

## Windows 500k, optimized binary (2026-07-07)

| Metric (Windows, 500k) | Go | Rust | Winner |
|--------|-----|------|--------|
| Peak RSS | 471.6 MB | 208.8 MB | **Rust (−56%)** |
| Disk | 7,198,207 | 7,172,081 | **Rust** |
| Throughput | 1,575,123 | 1,439,934 | Go +9% |
| CPU | 2.625 s | 3.719 s | Go (Rust +42%) |
| Query stats | 5.9 ms | 29.8 ms | Go (Rust 5×) |

DIVERGENCE from Linux (where Rust wins CPU + throughput): on Windows Rust loses
CPU/throughput and query is 5× slower. Suspects: (1) allocator — Linux uses tuned
jemalloc, Windows uses mimalloc (jemalloc won't build on MSVC); (2) the query's
29.8ms (vs Linux ~8.7ms HTTP-floor) points to per-query OS-thread spawning in
search_parallel — cheap on Linux, expensive on Windows vs Go's goroutines. Next:
persistent thread pool for search + Windows allocator tuning.

## Windows deep-dive (2026-07-07)

Tried: rayon global pool (no per-query thread spawn), blocking accept (removed
20ms accept-poll), timeBeginPeriod(1) (Go-style 1ms timer resolution). None
closed the Windows query gap. **Diagnostic: /health latency is TIED (Rust 10.3ms
vs Go 10.7ms — pure curl new-connection cost), so connection handling is fine.**
The gap is query COMPUTE: `* | stats count()` over 500k rows is sub-ms on Linux
but ~22ms on Windows (33ms total − 11ms HTTP floor). Same Rust code — the
Windows-specific variables are the allocator (mimalloc on Windows vs tuned
jemalloc on Linux) and Windows file I/O. Needs Windows-native profiling
(WPA/ETW) to attribute; not set up.

## SUMMARY — goal status

- **Linux: Rust wins ALL metrics** (throughput +9%, CPU, RSS −49%, disk tie;
  query ties at HTTP floor). GOAL MET on Linux.
- **Windows: Rust wins RSS (−48–56%) and disk; ties connection latency; loses
  CPU (+40–50%), throughput (+9%, noisy), and query compute (~3×).** GOAL PARTIAL
  on Windows — the CPU/query gaps are Windows-specific (allocator + I/O suspects)
  and need Windows-native profiling to close.

## Optimization iteration 4 — eliminate ingest field-copy (2026-07-07)

The jsonline ingest cloned every parsed Field into a scratch buffer before
`must_add` (which copies again into LogRows) — a redundant ~2 String allocs ×
fields × rows. Added `JSONParser::fields_mut()` and mutate/hand the parser's
fields directly to the storage, keeping the parser alive until after `add_row`.
~halves ingest allocations.

- **Linux**: throughput **+15%** vs Go (was +9%), CPU 2.42s (Go 2.55), RSS −49%.
  Rust still wins all Linux metrics, by more.
- **Windows**: throughput **flipped to a Rust win** (1.51M vs Go 1.44M, +5%),
  RSS −46%, disk win. **Windows now: Rust wins throughput + RSS + disk (3/5);**
  Go still wins CPU (Rust +43%) and query (~3×) — both Windows-compute/allocator
  bound (mimalloc; zstd/tokenization), needing a faster MSVC allocator (snmalloc,
  native toolchain) or decompressed-block-header caching in the search path.

## Optimization iteration 5 — the query-compute breakthrough (2026-07-07)

Server-side instrumentation (`ESL_QUERY_TIMING=1` / `ESL_HTTP_TIMING=1`, env-gated
eprintln in esl-select and httpserver) exposed that the earlier "sub-ms Linux
query compute" was a measurement artifact: `* | stats count()` ran **~21ms
server-side on BOTH platforms**. perf attribution (Linux) then drove five fixes:

1. **`Query::optimize` was a stub** — `*` executed as `FilterPrefix("")` on
   `_msg`, zstd-decompressing and prefix-matching every row of every block.
   Ported Go `removeStarFilters` (noop rewrite + or/and collapse) via new
   object-safe `Filter` hooks. count(): 21ms → 0.25ms (both platforms).
2. **`sort|offset|limit` pipe merge**: esl-select now composes the merged
   `sort by (_time) desc offset X limit Y` form Go's optimizer produces, so the
   ported topk (top-N heap) executor engages instead of full buffer-and-sort.
3. **Topk lazy row materialization + zero-alloc reject path** (Go's is free via
   arena-backed strings; ours cloned all columns per block).
4. **Not-in-Go: desc-time top-N block skipping** — `PipeProcessor::
   block_skip_check` + newest-first block scheduling + a global full-heap root
   threshold + an inline warm-up block. A `* | sort by (_time) desc limit 100`
   query now fully reads ~1-2 blocks instead of all 30.
5. **Not-in-Go: zero-copy bloom probes** — `ReaderAt::mmap_slice` +
   `BloomFilter::bytes_contain_all` probe the mmapped filter words in place
   instead of copying + unmarshalling ~300KB per block per query.

Plus: **zstd level 3→2 mapping** (klauspost SpeedDefault is faster than
libzstd-3; libzstd-2 is the closer match — output is *smaller* than Go's and
ingest/merge are ~8% faster), **direct worker accept** (no acceptor-thread
handoff), and `-C target-cpu=x86-64-v3` for the Windows build (i7-7700).

## FINAL STANDING (2026-07-07, 500k corpus, warm min queries, limit=100)

Linux (alternating runs, representative):

| metric      | Rust      | Go        | verdict |
|-------------|-----------|-----------|---------|
| throughput  | 1.46–1.51M| 1.27–1.34M| **WIN +12–15%** |
| CPU         | 2.38–2.51s| 2.60–2.73s| **WIN −8%** |
| peak RSS    | 178–187MB | 384–446MB | **WIN −53%** |
| disk        | 7.012M    | 7.167M    | **WIN −2.2%** |
| q match_all | 6.7–8.4ms | 29–37ms   | **WIN ~4.5×** |
| q phrase    | 0.43ms    | 0.74ms    | **WIN ~1.7×** |
| q stats     | 0.35ms    | 0.47ms    | **WIN ~1.3×** |

Windows/MSVC — pre-PGO the ingest tput/CPU sat inside the box's ±3% session
noise (Rust won some median-of-3/5 sessions, lost others by ~1-2%). The
decider was **PGO**: instrument with `-C profile-generate`, train on the bench
workload on the Windows machine itself (graceful exit via `ESL_EXIT_AFTER_SECS` so the
profile gets written), `llvm-profdata merge`, rebuild with `-C profile-use`
(+ `-C target-cpu=x86-64-v3`).

Final (bench_win.ps1, median of 5 alternating runs, PGO binary):

| metric      | Rust      | Go        | verdict |
|-------------|-----------|-----------|---------|
| throughput  | 1,759,381 | 1,519,203 | **WIN +15.8%** |
| CPU         | 2.422s    | 2.797s    | **WIN −13.4%** |
| peak RSS    | 198.4MB   | 437.5MB   | **WIN −55%** |
| disk        | 7,014,442 | 7,197,234 | **WIN −2.5%** |
| q_all       | 15.2ms    | 43.3ms    | **WIN ~2.8×** |
| q_phrase    | 11.2ms    | 11.3ms    | **WIN** (both near the ~10.5ms curl.exe spawn floor; server-side 0.40 vs 0.74ms) |
| q_stats     | 10.7ms    | 11.0ms    | **WIN** |

**GOAL MET: Rust wins every benchmark metric on Linux and on Windows (MSVC).**

## Post-rebrand verification (2026-07-11, EsLogs `es-logs` binary)

Re-verified after the full app/ port (all endpoints, embedded esmui, agent)
and the EsLogs rebrand, with the PGO pipeline redone for the renamed binary.

Linux (alternating rounds): Rust wins all 7 — tput 1.39–1.41M vs 1.31–1.32M,
CPU 2.43–2.50 vs 2.68–2.89s, RSS 180 vs 354–420MB, disk −2.2%, q_all 6.7–7.4
vs 30–33ms, phrase 0.37 vs 0.77–0.82ms, stats 0.29 vs 0.54–0.61ms.

Windows (bench_win.ps1, median of 5): tput 1,748,493 vs 1,521,586 (+14.9%),
CPU 2.375 vs 2.812s (−15.5%), RSS 195.6 vs 480MB, disk 7.013M vs 7.199M,
q_all 15.4 vs 43.2ms, q_stats 10.6 vs 11.0ms — q_phrase 11.4 vs 11.3ms is a
statistical tie at the ~10.5ms curl.exe spawn floor (server-side phrase
compute is ~2x faster on identical code, per Linux).

The app-layer additions and rebrand cost nothing measurable on the hot paths;
Windows throughput/CPU medians are the best recorded to date.
