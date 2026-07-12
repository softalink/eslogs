# Syncing with upstream releases

EsLogs is a Rust port of VictoriaLogs; the exact upstream release the code
matches is pinned in [`UPSTREAM.lock`](../UPSTREAM.lock) at the repo root.
`scripts/upstream_sync.py` turns "upstream released vX.Y.Z" into a file-level
work plan.

## How it works

Every ported Rust file starts with a module doc header naming its upstream
source (`//! Port of ... `lib/logstorage/foo.go``). The sync tool extracts
those headers into an upstream-path → Rust-file map, so an upstream diff can
be translated mechanically into "these Rust files need updating". The
rebranded path tokens (`eslinsert`, `esmui`, ...) are mapped back to upstream
spellings automatically.

Two gotchas the tool handles for you:

- **Pre-split tags.** The upstream repo inherits old VictoriaMetrics tag
  history, so the *highest version number* is not the newest logs release.
  The tool only considers tags that descend from the pinned commit and
  refuses non-descendant `--to` targets.
- **Coverage drift.** `check` verifies every in-scope upstream Go file is
  either mapped or listed (with a reason) in
  `scripts/upstream_sync_ignore.txt`, so new upstream files can't be missed
  silently.

The upstream checkout defaults to `../VictoriaLogs` (a sibling of this repo); override
with `--upstream <path>` or `ESLOGS_UPSTREAM=<path>`.

## The workflow

```sh
# 0. Is there anything to do? (fetches tags; picks the newest real release)
python3 scripts/upstream_sync.py report

# 1. Generate the work plan for a specific release (or preview master):
python3 scripts/upstream_sync.py report --to v1.52.0 --diffs
#    -> docs/sync/REPORT-v1.52.0.md      (checklist: upstream file -> Rust files)
#    -> target/upstream-sync/v1.52.0/*.patch  (per-file upstream diffs)

# 2. Port, subsystem by subsystem. For each checklist line:
#    - read the .patch, apply the change to the mapped Rust file(s)
#    - port any *_test.go changes alongside, same file
#    - keep the house conventions: mirror Go names, `// PORT NOTE:` for
#      deliberate divergences, new files get a `//! Port of ...` header
#      (that header is what keeps the sync map complete)

# 3. Gate (same as any change):
cargo test --workspace && cargo clippy --workspace
XWIN_ACCEPT_LICENSE=1 cargo xwin check --target x86_64-pc-windows-msvc --workspace

# 4. Pin the new tag, then verify. `bump` also mirrors the tag into the
#    workspace version — the port's semver follows upstream, and `check`
#    (run in CI) fails on any drift between Cargo.toml and UPSTREAM.lock:
python3 scripts/upstream_sync.py bump --to v1.52.0
python3 scripts/upstream_sync.py check

# 5. Re-run the benchmark against the new Go release binary and record the
#    result in bench/BASELINE.md (docs in bench/README.md; Windows needs the
#    PGO redeploy described in the README).
```

Tick the checklist boxes in the report as you go; the report is meant to be
committed with the sync so the review shows exactly what upstream changed and
where it landed.

## Keeping the map healthy

- New Rust file ⇒ give it a `//! Port of ... `path`` header. `check` fails
  when an upstream file has no owner.
- Deliberately skipping an upstream file/package ⇒ add it to
  `scripts/upstream_sync_ignore.txt` **with a reason comment**.
- `python3 scripts/upstream_sync.py map` prints the full extracted mapping if
  you need to debug a mismatch.

## Current status

`docs/sync/` holds generated reports. A report against upstream `master`
(unreleased work) can be regenerated any time with
`report --to FETCH_HEAD` after `git -C $ESLOGS_UPSTREAM fetch origin master`.
