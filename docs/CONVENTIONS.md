# Porting Conventions

Rules for porting Go packages from EsLogs v1.51.0
(the upstream checkout — see `UPSTREAM.lock`; vendored Softalink LLC libs under
`vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/`) into this workspace.

## Fidelity

- Semantics first: the Go behavior (including edge cases, error messages, limits
  and on-disk/wire formats) is the spec. When Go and Rust idioms conflict, keep
  the behavior, adapt the idiom.
- Port the package's `*_test.go` tests as `#[cfg(test)] mod tests` (or
  `tests/` integration tests when they need the filesystem). Test names:
  `TestFooBar` → `test_foo_bar`. Keep the same cases and expected values.
- Divergences from the Go source must carry a `// PORT NOTE:` comment saying
  what changed and why.
- Do NOT invent features, config options or error handling the Go code doesn't
  have (no speculative generality).

## Mapping Go → Rust

- One Go package → one `esl-common` module (`lib/bytesutil` →
  `esl_common::bytesutil`). `lib/logstorage` files map 1:1 into
  `crates/esl-logstorage/src/` modules.
- Naming: `MustReadData` → `must_read_data`, types stay `PascalCase`.
  `Must*` functions keep the `must_` prefix and panic/fatal like Go.
- `logger.Infof/Warnf/Errorf/Fatalf/Panicf` → `esl_common::{infof!, warnf!,
  errorf!, fatalf!, panicf!}` macros. `logger.Panicf("BUG: ...")` assertions
  keep their exact message text.
- Errors: Go `(T, error)` → `Result<T, String>` unless a richer error type is
  already established for that subsystem. Preserve error message wording —
  tests assert on it.
- `sync.Pool` → the pooling helpers in `esl-common` (or a `thread_local!`/
  `Mutex<Vec<T>>` pool matching usage). Buffer reuse patterns must be
  preserved — allocator churn is a benchmark-relevant behavior.
- Goroutines → `std::thread` (named via `Builder::new().name(...)`);
  `sync.WaitGroup` → `std::thread::Scope` or explicit joins; channels →
  `std::sync::mpsc` unless a package clearly needs more.
- `atomic.Int64` etc. → `std::sync::atomic` with the same orderings Go
  guarantees (Go atomics are sequentially consistent: use `SeqCst` unless a
  comment justifies weaker).
- Build tags `_linux.go` / `_windows.go` / `_unix.go` → `#[cfg(...)]` blocks or
  `mod foo_unix; mod foo_windows;` split. Windows (MSVC) must always compile:
  check with `cargo check --target x86_64-pc-windows-msvc`.

## Dependencies

Prefer std. Already available in `esl-common`: `zstd`, `regex`, `memmap2`,
`libc` (unix), `windows-sys` (windows). Adding a new dependency requires a
recorded reason in the crate's Cargo.toml comment and it must build on both
Linux and Windows MSVC.

## Quality gates (every ported package)

1. `cargo test -p <crate> <module>::` passes.
2. `cargo clippy -p <crate>` introduces no new warnings.
3. `cargo check --target x86_64-pc-windows-msvc -p <crate>` passes.
4. Update the package's row in `docs/PARITY.md` (`todo` → `ported`).

## Working around the GateGuard hook

The first Write to any NEW file path is denied by a `[Fact-Forcing Gate]`
PreToolUse hook with a request to state facts. State the facts in one short
sentence (callers, duplicate check, schema, instruction) and re-send the exact
same Write — the retry always passes. Do not skip files because of this.
