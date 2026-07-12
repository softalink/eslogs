#!/usr/bin/env python3
"""Upstream sync tool for the EsLogs Rust port.

The port tracks upstream VictoriaLogs (see UPSTREAM.lock for the pinned tag).
This tool turns "upstream released vX.Y.Z" into an actionable, file-level work
plan by exploiting the port's own conventions: every ported Rust file starts
with a `//! Port of ... `path/to/source.go`` doc header, which gives a
machine-extractable mapping from upstream Go paths to Rust files.

Subcommands
-----------
  map                     Print the upstream-path -> Rust-file map extracted
                          from the module doc headers.
  check                   Coverage gate: every Go file in the ported scopes at
                          the pinned tag must be mapped or ignored
                          (scripts/upstream_sync_ignore.txt). Exits non-zero
                          and lists the gaps otherwise. Run this in CI.
  report [--to TAG]       Fetch upstream, diff pinned..TAG (default: newest
                          tag) over the ported scopes, and write
                          docs/sync/REPORT-<TAG>.md: a checklist of changed
                          upstream files with their Rust counterparts,
                          diffstats, and new/deleted files. With --diffs also
                          dumps per-file patches under target/upstream-sync/.
  bump --to TAG           After the sync work lands, update UPSTREAM.lock to
                          the new tag.

The upstream checkout defaults to ../VictoriaLogs (a sibling of this repo)
and can be overridden with --upstream or the ESLOGS_UPSTREAM env var.
"""

import argparse
import fnmatch
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOCK_FILE = REPO / "UPSTREAM.lock"
IGNORE_FILE = REPO / "scripts" / "upstream_sync_ignore.txt"
DEFAULT_UPSTREAM = os.environ.get("ESLOGS_UPSTREAM", str(REPO.parent / "VictoriaLogs"))

# The rebrand (VictoriaLogs -> EsLogs) also renamed upstream path tokens
# inside doc headers; map them back to the upstream spelling for matching.
DEBRAND = [
    ("eslogscli", "vlogscli"),
    ("eslogsgenerator", "vlogsgenerator"),
    ("eslagent", "vlagent"),
    ("eslinsert", "vlinsert"),
    ("eslselect", "vlselect"),
    ("eslstorage", "vlstorage"),
    ("esmui", "vmui"),
    ("es-logs", "victoria-logs"),
    ("EsLogs", "VictoriaLogs"),
    ("eslogs", "vlogs"),
]

# Upstream directories the port covers. Files changing outside these never
# affect the port.
SCOPES = ("app/", "lib/", "vendor/github.com/VictoriaMetrics/")

# A backticked token in a doc header is treated as an upstream source
# reference when it looks like a path into the Go tree.
PATH_TOKEN = re.compile(r"`([^`]+)`")


def debrand(s: str) -> str:
    for ours, theirs in DEBRAND:
        s = s.replace(ours, theirs)
    return s


def run_git(upstream: str, *args: str) -> str:
    return subprocess.check_output(["git", "-C", upstream, *args], text=True)


def read_lock() -> dict:
    d = {}
    for line in LOCK_FILE.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            d[k.strip()] = v.strip()
    return d


def workspace_version() -> str:
    """The [workspace.package] version in the root Cargo.toml."""
    in_section = False
    for line in (REPO / "Cargo.toml").read_text().splitlines():
        if line.strip() == "[workspace.package]":
            in_section = True
        elif in_section and line.startswith("["):
            break
        elif in_section and line.startswith("version"):
            return line.split('"')[1]
    raise SystemExit("cannot find [workspace.package] version in Cargo.toml")


def set_workspace_version(v: str) -> None:
    path = REPO / "Cargo.toml"
    out, in_section, done = [], False, False
    for line in path.read_text().splitlines(keepends=True):
        if line.strip() == "[workspace.package]":
            in_section = True
        elif in_section and line.startswith("["):
            in_section = False
        if in_section and line.startswith("version") and not done:
            line = f'version = "{v}"\n'
            done = True
        out.append(line)
    assert done, "version line not found"
    path.write_text("".join(out))


def looks_like_source_ref(tok: str) -> bool:
    if tok.startswith(("lib/", "app/", "vendor/", "github.com/")):
        return True
    return tok.endswith(".go")


def upstream_go_files(upstream: str, ref: str) -> list[str]:
    out = run_git(upstream, "ls-tree", "-r", "--name-only", ref)
    return [
        f
        for f in out.splitlines()
        if f.endswith(".go") and f.startswith(SCOPES)
    ]


def load_ignores() -> list[str]:
    pats = []
    if IGNORE_FILE.exists():
        for line in IGNORE_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                pats.append(line)
    return pats


def is_ignored(path: str, pats: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, p) for p in pats)


def rust_files() -> list[Path]:
    files = []
    for crate in sorted((REPO / "crates").iterdir()):
        files.extend(sorted(crate.rglob("*.rs")))
    return files


def extract_map() -> dict[str, list[str]]:
    """upstream Go path (file or package dir) -> [Rust files]."""
    mapping: dict[str, list[str]] = defaultdict(list)
    for rs in rust_files():
        rel = str(rs.relative_to(REPO))
        header = []
        with open(rs, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("//!"):
                    header.append(line)
                elif header:
                    break
                if len(header) > 60:
                    break
        text = debrand("".join(header))
        for tok in PATH_TOKEN.findall(text):
            for part in re.split(r"[+,]| and ", tok):
                part = part.strip().strip("`")
                if not part or not looks_like_source_ref(part):
                    continue
                # `github.com/VictoriaMetrics/easyproto` style -> vendor path.
                if part.startswith("github.com/"):
                    part = "vendor/" + part
                mapping[part].append(rel)
    return dict(mapping)


def match_go_file(go_path: str, mapping: dict[str, list[str]]) -> list[str]:
    """Resolve one concrete upstream .go file against the extracted map:
    exact file match first, then the longest mapped package-directory
    prefix, then a basename match within the same package."""
    if go_path in mapping:
        return mapping[go_path]
    # The VictoriaMetrics helper libs are vendored upstream but referenced as
    # bare `lib/...` in esl-common's doc headers; try the unvendored alias.
    vm_vendor = "vendor/github.com/VictoriaMetrics/VictoriaMetrics/"
    if go_path.startswith(vm_vendor):
        alias = go_path[len(vm_vendor):]
        hit = match_go_file(alias, mapping) if alias != go_path else []
        if hit:
            return hit
    best: list[str] = []
    best_len = -1
    d = os.path.dirname(go_path)
    base = os.path.basename(go_path)
    for key, targets in mapping.items():
        if key.endswith(".go"):
            # Same package + same basename family (foo.go vs foo_test.go).
            if os.path.dirname(key) == d and base.replace("_test.go", ".go") == os.path.basename(key):
                return targets
            continue
        key_dir = key.rstrip("/")
        if (go_path.startswith(key_dir + "/")) and len(key_dir) > best_len:
            best, best_len = targets, len(key_dir)
    return best


def cmd_map(args) -> int:
    mapping = extract_map()
    for k in sorted(mapping):
        print(f"{k}\t{';'.join(sorted(set(mapping[k])))}")
    print(f"# {len(mapping)} upstream refs from {len(rust_files())} Rust files", file=sys.stderr)
    return 0


def cmd_check(args) -> int:
    lock = read_lock()
    mapping = extract_map()
    ignores = load_ignores()
    missing = []
    for go in upstream_go_files(args.upstream, lock["commit"]):
        if is_ignored(go, ignores):
            continue
        if not match_go_file(go, mapping):
            missing.append(go)
    # The port's semver follows the pinned upstream release.
    want = lock["tag"].lstrip("v")
    have = workspace_version()
    if have != want:
        print(f"VERSION MISMATCH: Cargo.toml workspace version is {have}, but")
        print(f"UPSTREAM.lock pins {lock['tag']} — the port's semver follows")
        print("upstream. Run `scripts/upstream_sync.py bump --to <tag>`.")
        return 1
    if missing:
        print(f"UNMAPPED upstream files at {lock['tag']} ({len(missing)}):")
        for m in missing:
            print(f"  {m}")
        print("\nEither add a `//! Port of ...` header to the owning Rust file,")
        print("or add the path to scripts/upstream_sync_ignore.txt with a reason.")
        return 1
    print(f"OK: all in-scope upstream files at {lock['tag']} are mapped or ignored.")
    return 0


def cmd_report(args) -> int:
    lock = read_lock()
    upstream = args.upstream
    if not args.no_fetch:
        try:
            subprocess.run(
                ["git", "-C", upstream, "fetch", "--tags", "origin"],
                check=True, capture_output=True, timeout=120,
            )
        except Exception as e:  # offline is fine; use local tags
            print(f"(fetch skipped: {e})", file=sys.stderr)
    frm = lock["tag"]

    def is_descendant(ref: str) -> bool:
        return subprocess.run(
            ["git", "-C", upstream, "merge-base", "--is-ancestor", lock["commit"], ref],
            capture_output=True).returncode == 0

    to = args.to
    if not to:
        # Newest tag that actually descends from the pinned release. The
        # upstream repo carries pre-split VictoriaMetrics tag history, so
        # "highest version number" is NOT the newest logs release.
        tags = run_git(upstream, "tag", "--sort=creatordate").split()
        successors = [t for t in reversed(tags) if t != frm and is_descendant(t)]
        if not successors:
            print(f"No release tag newer than the pinned {frm} exists upstream yet.")
            print("Tip: preview unreleased work with `report --to origin/master`.")
            return 0
        to = successors[0]
    elif not is_descendant(to):
        print(f"error: {to} does not descend from the pinned {frm} "
              f"({lock['commit'][:12]}).")
        print("The upstream repo carries pre-split VictoriaMetrics tags; pick a")
        print("VictoriaLogs release tag (or a branch like origin/master) that")
        print(f"contains {frm}.")
        return 1
    mapping = extract_map()
    ignores = load_ignores()

    raw = run_git(upstream, "diff", "--name-status", f"{frm}..{to}")
    stat = {}
    for line in run_git(upstream, "diff", "--numstat", f"{frm}..{to}").splitlines():
        add, rm, path = line.split("\t", 2)
        stat[path] = (add, rm)

    changed, added, deleted, ignored, out_of_scope = [], [], [], [], []
    for line in raw.splitlines():
        parts = line.split("\t")
        status, path = parts[0], parts[-1]
        if not path.endswith(".go"):
            continue
        if not path.startswith(SCOPES):
            out_of_scope.append(path)
            continue
        if is_ignored(path, ignores):
            ignored.append(path)
            continue
        entry = (path, match_go_file(path, mapping), stat.get(path, ("?", "?")))
        if status.startswith("A"):
            added.append(entry)
        elif status.startswith("D"):
            deleted.append(entry)
        else:
            changed.append(entry)

    outdir = REPO / "docs" / "sync"
    outdir.mkdir(parents=True, exist_ok=True)
    report = outdir / f"REPORT-{to}.md"
    with open(report, "w", encoding="utf-8") as f:
        f.write(f"# Upstream sync report: {frm} -> {to}\n\n")
        f.write(f"Generated by `scripts/upstream_sync.py report --to {to}`.\n")
        f.write(f"Upstream: {run_git(upstream, 'rev-parse', f'{to}^{{commit}}').strip()} ")
        f.write(f"({run_git(upstream, 'log', '-1', '--format=%cs', to).strip()})\n\n")
        f.write(f"In-scope Go changes: {len(changed)} modified, {len(added)} added, "
                f"{len(deleted)} deleted ({len(ignored)} ignored by policy).\n\n")

        def section(title, entries, note=""):
            f.write(f"## {title}\n\n")
            if note:
                f.write(note + "\n\n")
            if not entries:
                f.write("(none)\n\n")
                return
            for path, targets, (add, rm) in sorted(entries):
                f.write(f"- [ ] `{path}` (+{add}/-{rm})")
                if targets:
                    f.write(" -> " + ", ".join(f"`{t}`" for t in sorted(set(targets))))
                else:
                    f.write(" -> **UNMAPPED (new port work or missing header)**")
                f.write("\n")
            f.write("\n")

        section("Modified upstream files", changed,
                "Update the mapped Rust file(s) to match, porting test changes alongside.")
        section("New upstream files", added,
                "Port as new modules following docs/UPSTREAM_SYNC.md conventions.")
        section("Deleted upstream files", deleted,
                "Remove or PORT-NOTE the Rust counterpart.")
        if ignored:
            f.write("## Ignored by policy\n\n")
            for p in sorted(ignored):
                f.write(f"- `{p}`\n")
            f.write("\n")
        f.write("## Completion checklist\n\n")
        f.write("- [ ] all boxes above ticked; `cargo test --workspace` green\n")
        f.write("- [ ] `cargo clippy --workspace` clean; MSVC `cargo xwin check` green\n")
        f.write("- [ ] `scripts/upstream_sync.py check` passes against the new tag\n")
        f.write(f"- [ ] benchmark re-run vs the {to} Go binary (bench/BASELINE.md)\n")
        f.write(f"- [ ] `scripts/upstream_sync.py bump --to {to}`\n")

    print(f"wrote {report.relative_to(REPO)}")

    if args.diffs:
        ddir = REPO / "target" / "upstream-sync" / to
        ddir.mkdir(parents=True, exist_ok=True)
        for path, _, _ in changed + added:
            patch = subprocess.check_output(
                ["git", "-C", upstream, "diff", f"{frm}..{to}", "--", path], text=True)
            out = ddir / (path.replace("/", "__") + ".patch")
            out.write_text(patch, encoding="utf-8")
        print(f"wrote {len(changed) + len(added)} patches under {ddir.relative_to(REPO)}/")
    return 0


def cmd_bump(args) -> int:
    # ^{commit} dereferences annotated tag objects to the tagged commit.
    commit = run_git(args.upstream, "rev-parse", f"{args.to}^{{commit}}").strip()
    date = run_git(args.upstream, "log", "-1", "--format=%cs", args.to).strip()
    LOCK_FILE.write_text(
        f"tag = {args.to}\ncommit = {commit}\ndate = {date}\n", encoding="utf-8")
    print(f"UPSTREAM.lock -> {args.to} ({commit[:12]}, {date})")
    # The port's semver follows upstream: mirror the tag into the workspace
    # version (all crates inherit it).
    v = args.to.lstrip("v")
    set_workspace_version(v)
    print(f"Cargo.toml [workspace.package] version -> {v} "
          "(run a cargo build to refresh Cargo.lock)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--upstream", default=DEFAULT_UPSTREAM,
                    help="path to the upstream VictoriaLogs git checkout")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("map")
    sub.add_parser("check")
    rp = sub.add_parser("report")
    rp.add_argument("--to", help="target upstream tag (default: newest)")
    rp.add_argument("--diffs", action="store_true",
                    help="also dump per-file patches under target/upstream-sync/")
    rp.add_argument("--no-fetch", action="store_true")
    bp = sub.add_parser("bump")
    bp.add_argument("--to", required=True)
    args = ap.parse_args()
    return {"map": cmd_map, "check": cmd_check,
            "report": cmd_report, "bump": cmd_bump}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
