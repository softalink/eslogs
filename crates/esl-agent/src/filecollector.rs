//! Port of EsLogs `app/eslagent/filecollector` — glob-based discovery of
//! log files, per-file tailers and the processor that turns log lines into
//! log rows for the remotewrite layer.
//!
//! Sections below mirror the Go files: `file_collector.go` and `processor.go`.
//!
//! PORT NOTE: Go matches globs with github.com/bmatcuk/doublestar/v4. Adding
//! external dependencies is off-limits, so the needed subset of doublestar
//! semantics (`**`, `*`, `?`, `[...]` classes, `{...}` alternates, escaping,
//! `WithNoHidden`) is implemented in the internal [`doublestar`] module below.
//!
//! PORT NOTE: Go registers `esl_rows_ingested_total{type="file_logs"}`,
//! `esl_bytes_ingested_total{type="file_logs"}` and
//! `esl_rows_dropped_total{reason="too_many_fields"}` via
//! `Softalink LLC/metrics`. The metrics registry isn't ported yet, so the
//! counters are kept as process-local `AtomicU64`s.

use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use esl_common::flagutil::{ArrayString, Flag, FlagValue};
use esl_common::timeutil;
use esl_common::{errorf, fatalf, panicf, warnf};

use esl_insert::common_params::DEFAULT_MSG_VALUE;
use esl_insert::common_params::extract_timestamp_from_fields;
// PORT NOTE: Go's processor receives `insertutil.LogRowsStorage` (implemented
// by `remotewrite.Storage`, see `var storage = &remotewrite.Storage{}`); the
// port uses the equivalent trait from esl-insert. The Go interface also carries
// `CanWriteData() error`, which the esl-insert port dropped (see the PORT NOTE
// in esl_insert::common_params); the file collector never calls it.
pub use esl_insert::common_params::LogRowsStorage;

use esl_logstorage::json_parser::{get_json_parser, put_json_parser};
use esl_logstorage::log_rows::{LogRows, estimated_json_row_len, get_log_rows, put_log_rows};
use esl_logstorage::rows::{Field, marshal_fields_to_json, rename_field};
use esl_logstorage::tenant_id::{TenantID, parse_tenant_id};

use crate::tail;
use crate::tail::Processor as TailProcessor;

// ---------------------------------------------------------------------------
// file_collector.go — flags
// ---------------------------------------------------------------------------

static GLOB: Flag<ArrayString> = Flag::new(
    "fileCollector.glob",
    "Glob pattern for log files to collect. Can be specified multiple times. \
     The pattern must match files, not directories. \
     Example: -fileCollector.glob=\"/var/log/my_app/*.log\"",
    ArrayString::default,
);
static EXCLUDE_GLOB: Flag<ArrayString> = Flag::new(
    "fileCollector.excludeGlob",
    "Glob pattern for log files to exclude from collection. Can be specified multiple times. \
     Example: -fileCollector.excludeGlob=\"/var/log/my_app/*.gz\"",
    ArrayString::default,
);
static CHECKPOINTS_PATH: Flag<String> = Flag::new(
    "fileCollector.checkpointsPath",
    "Path to the file where eslagent stores its read position for each collected file. \
     By default, stored in the directory specified by -tmpDataPath. \
     Example: -fileCollector.checkpointsPath=/var/lib/eslagent/file-checkpoints.json",
    String::new,
);

static REFRESH_INTERVAL: Flag<DurationFlag> = Flag::new(
    "fileCollector.refreshInterval",
    "How often eslagent checks for new files matching the glob pattern",
    || DurationFlag {
        nanos: 10_000_000_000,
    },
);

/// Port of Go `flag.Duration`, stored as nanoseconds.
struct DurationFlag {
    nanos: i64,
}

impl FlagValue for DurationFlag {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let nanos = timeutil::parse_duration(s)?;
        Ok(DurationFlag { nanos })
    }
}

// ---------------------------------------------------------------------------
// file_collector.go — Init/Stop and glob processing
// ---------------------------------------------------------------------------

static STOP_TX: Mutex<Option<Sender<()>>> = Mutex::new(None);
static REFRESH_THREAD: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

static TAILER: OnceLock<tail::Tailer> = OnceLock::new();

// PORT NOTE: Go's Init(tmpDataPath) uses a package-global
// `var storage = &remotewrite.Storage{}`; the port receives the storage handle
// from the caller (main.rs wiring) so this module doesn't depend on the
// sibling-owned remotewrite module directly. Generic (not `dyn`) because the
// esl-insert trait uses a `self: &Arc<Self>` receiver.
pub fn init<S: LogRowsStorage + 'static>(tmp_data_path: &str, storage: Arc<S>) {
    let glob = &GLOB.get().0;
    if glob.is_empty() {
        return;
    }

    // PORT NOTE: Go mutates *checkpointsPath in place; the ported flags are
    // immutable, so the effective value is computed here.
    let mut checkpoints_path = CHECKPOINTS_PATH.get().clone();
    if checkpoints_path.is_empty() {
        checkpoints_path = Path::new(tmp_data_path)
            .join("eslagent-file-checkpoints.json")
            .to_string_lossy()
            .into_owned();
    }

    // Ensure glob patterns are valid.
    for pattern in glob {
        if let Err(err) = doublestar::path_match(pattern, ".") {
            panicf!("FATAL: cannot start fileCollector: invalid glob pattern {pattern:?}: {err}");
        }
    }
    for pattern in &EXCLUDE_GLOB.get().0 {
        if let Err(err) = doublestar::path_match(pattern, ".") {
            panicf!(
                "FATAL: cannot start fileCollector: invalid exclude glob pattern {pattern:?}: {err}"
            );
        }
    }

    let mut refresh_interval = REFRESH_INTERVAL.get().nanos;
    if refresh_interval < 1_000_000_000 {
        // PORT NOTE: Go prints the duration with %q ("500ms"); std Duration's
        // Debug format is close enough (esl-common's format_go_duration is
        // private).
        warnf!(
            "fileCollector.refreshInterval=\"{:?}\" too small, setting to 1 second",
            Duration::from_nanos(refresh_interval.max(0) as u64)
        );
        refresh_interval = 1_000_000_000;
    }
    let refresh_interval = Duration::from_nanos(refresh_interval.max(0) as u64);

    init_tenant_ids();
    init_extra_fields();
    init_hostname();

    let _ = TAILER.set(tail::start(&checkpoints_path));

    let (tx, rx) = channel::<()>();
    *STOP_TX.lock().unwrap() = Some(tx);

    let handle = std::thread::spawn(move || {
        for (i, pattern) in glob.iter().enumerate() {
            process_glob(i, pattern, &storage);
        }

        loop {
            match rx.recv_timeout(refresh_interval) {
                Err(RecvTimeoutError::Timeout) => {
                    for (i, pattern) in glob.iter().enumerate() {
                        process_glob(i, pattern, &storage);
                    }
                }
                _ => return,
            }
        }
    });
    *REFRESH_THREAD.lock().unwrap() = Some(handle);
}

fn process_glob<S: LogRowsStorage + 'static>(arg_idx: usize, pattern: &str, storage: &Arc<S>) {
    if pattern.is_empty() {
        return;
    }

    // Handle regular paths as a special case with verbose logging for better UX,
    // as glob matching ignores I/O errors.
    if !is_glob(pattern) {
        start_read(arg_idx, pattern, storage);
        return;
    }

    // Follow traditional shell glob behavior where `*` or a `?` at the start will
    // not match dotfiles by default (doublestar.WithNoHidden). Users can explicitly
    // use `.*` or `.?` syntax to collect logs from the hidden files.
    let matches = match doublestar::filepath_glob(pattern, true) {
        Ok(matches) => matches,
        Err(err) => {
            // Pattern must be valid since we validate it in the init function.
            panicf!("BUG: pattern {pattern:?} should be valid; got: {err}");
            unreachable!()
        }
    };
    for f in &matches {
        start_read(arg_idx, f, storage);
    }
}

fn start_read<S: LogRowsStorage + 'static>(arg_idx: usize, file_path: &str, storage: &Arc<S>) {
    let tailer = TAILER.get().unwrap();
    if tailer.is_tailing(file_path) {
        return;
    }

    let exclude_pattern = EXCLUDE_GLOB.get().get_optional_arg(arg_idx);
    if !exclude_pattern.is_empty()
        && doublestar::path_match(exclude_pattern, file_path).unwrap_or(false)
    {
        return;
    }

    if Path::new(file_path)
        .extension()
        .is_some_and(|ext| ext == "gz")
    {
        warnf!(
            "skipping gzipped file {file_path:?}; eslagent does not support reading archived files"
        );
        return;
    }

    let f = match File::open(file_path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            warnf!("cannot start reading logs from file {file_path:?}: file does not exist");
            return;
        }
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            warnf!("cannot start reading logs from file {file_path:?}: permission denied");
            return;
        }
        Err(err) => {
            errorf!("cannot open file {file_path:?}: {err}");
            return;
        }
    };

    let fi = match f.metadata() {
        Ok(fi) => fi,
        Err(err) => {
            warnf!("cannot stat file: {err}");
            return;
        }
    };
    if fi.is_dir() {
        let suggested_path = Path::new(file_path).join("*.log");
        warnf!(
            "cannot start reading logs from file {file_path:?}: is a directory; probably you meant {:?}",
            suggested_path.to_string_lossy()
        );
        return;
    }
    drop(f);

    let proc = new_processor(arg_idx, file_path, Arc::clone(storage));
    tailer.start_read(file_path, Box::new(proc));
}

pub fn stop() {
    if GLOB.get().0.is_empty() {
        return;
    }
    drop(STOP_TX.lock().unwrap().take());
    if let Some(handle) = REFRESH_THREAD.lock().unwrap().take() {
        let _ = handle.join();
    }
    TAILER.get().unwrap().stop();
}

/// is_glob reports whether s contains any unescaped glob meta character.
/// See <https://github.com/bmatcuk/doublestar/blob/a9ad9e0ef4d6b7e4443090e9a7201d847a881711/glob.go#L334>
fn is_glob(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'*' || c == b'?' || c == b'[' || c == b'{' {
            return true;
        }
        if c == b'\\' && std::path::MAIN_SEPARATOR != '\\' {
            // skip next byte
            i += 1;
        }
        i += 1;
    }
    false
}

// ---------------------------------------------------------------------------
// doublestar — internal port of the needed github.com/bmatcuk/doublestar/v4
// matching semantics
// ---------------------------------------------------------------------------

pub(crate) mod doublestar {
    use std::fs;

    /// doublestar.ErrBadPattern text.
    const ERR_BAD_PATTERN: &str = "syntax error in pattern";

    /// Escaping with `\` is disabled when the OS path separator is `\`
    /// (mirrors doublestar's PathMatch/FilepathGlob behavior on Windows).
    const ESCAPING_ENABLED: bool = std::path::MAIN_SEPARATOR != '\\';

    fn bad_pattern() -> String {
        ERR_BAD_PATTERN.to_string()
    }

    /// Converts OS path separators to `/` for matching.
    fn normalize_separators(s: &str) -> String {
        if std::path::MAIN_SEPARATOR == '/' {
            s.to_string()
        } else {
            s.replace(std::path::MAIN_SEPARATOR, "/")
        }
    }

    /// Port of doublestar.PathMatch: matches `name` against `pattern`, using
    /// the OS path separator. Returns [`ERR_BAD_PATTERN`] for malformed
    /// patterns.
    ///
    /// PORT NOTE: unlike doublestar (which, like Go's path.Match, may miss a
    /// malformed suffix after a mismatch), the pattern is always validated in
    /// full so that Init's `PathMatch(pattern, ".")` check reliably rejects
    /// bad patterns.
    pub(crate) fn path_match(pattern: &str, name: &str) -> Result<bool, String> {
        match_internal(pattern, name, false)
    }

    fn match_internal(pattern: &str, name: &str, no_hidden: bool) -> Result<bool, String> {
        validate_pattern(pattern)?;
        let pattern = normalize_separators(pattern);
        let name = normalize_separators(name);
        let name_segs: Vec<&str> = name.split('/').collect();
        for pat in expand_braces(&pattern)? {
            let pat_segs: Vec<&str> = pat.split('/').collect();
            if match_segments(&pat_segs, &name_segs, no_hidden)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Validates the whole pattern: balanced `{}`, closed `[]`, no dangling `\`.
    fn validate_pattern(pattern: &str) -> Result<(), String> {
        let chars: Vec<char> = pattern.chars().collect();
        let mut brace_depth = 0i32;
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '\\' if ESCAPING_ENABLED => {
                    if i + 1 >= chars.len() {
                        return Err(bad_pattern());
                    }
                    i += 1;
                }
                '{' => brace_depth += 1,
                '}' => {
                    brace_depth -= 1;
                    if brace_depth < 0 {
                        return Err(bad_pattern());
                    }
                }
                '[' => {
                    let mut j = i + 1;
                    if j < chars.len() && (chars[j] == '!' || chars[j] == '^') {
                        j += 1;
                    }
                    if j < chars.len() && chars[j] == ']' {
                        // A `]` right after the (possibly negated) opening
                        // bracket is a literal.
                        j += 1;
                    }
                    let mut closed = false;
                    while j < chars.len() {
                        if chars[j] == '\\' && ESCAPING_ENABLED {
                            j += 2;
                            continue;
                        }
                        if chars[j] == ']' {
                            closed = true;
                            break;
                        }
                        j += 1;
                    }
                    if !closed {
                        return Err(bad_pattern());
                    }
                    i = j;
                }
                _ => {}
            }
            i += 1;
        }
        if brace_depth != 0 {
            return Err(bad_pattern());
        }
        Ok(())
    }

    /// Expands `{a,b}` alternates (recursively) into plain patterns.
    /// Alternates may contain path separators and nested braces.
    fn expand_braces(pattern: &str) -> Result<Vec<String>, String> {
        let chars: Vec<char> = pattern.chars().collect();

        // Find the first unescaped '{'.
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '\\' if ESCAPING_ENABLED => i += 2,
                '{' => break,
                _ => i += 1,
            }
        }
        if i >= chars.len() {
            return Ok(vec![pattern.to_string()]);
        }

        // Find the matching '}' and split the top-level comma alternates.
        let mut depth = 1;
        let mut j = i + 1;
        let mut alts: Vec<String> = Vec::new();
        let mut cur = String::new();
        while j < chars.len() && depth > 0 {
            match chars[j] {
                '\\' if ESCAPING_ENABLED => {
                    cur.push('\\');
                    if j + 1 < chars.len() {
                        cur.push(chars[j + 1]);
                    }
                    j += 2;
                    continue;
                }
                '{' => {
                    depth += 1;
                    cur.push('{');
                }
                '}' => {
                    depth -= 1;
                    if depth > 0 {
                        cur.push('}');
                    }
                }
                ',' if depth == 1 => alts.push(std::mem::take(&mut cur)),
                c => cur.push(c),
            }
            j += 1;
        }
        if depth > 0 {
            return Err(bad_pattern());
        }
        alts.push(cur);

        let prefix: String = chars[..i].iter().collect();
        let suffix: String = chars[j..].iter().collect();
        let mut out = Vec::new();
        for alt in alts {
            let combined = format!("{prefix}{alt}{suffix}");
            out.extend(expand_braces(&combined)?);
        }
        Ok(out)
    }

    /// Matches path segments; `**` matches zero or more segments.
    fn match_segments(pat: &[&str], name: &[&str], no_hidden: bool) -> Result<bool, String> {
        if pat.is_empty() {
            return Ok(name.is_empty());
        }
        if pat[0] == "**" {
            // Zero segments.
            if match_segments(&pat[1..], name, no_hidden)? {
                return Ok(true);
            }
            // One or more segments; with no_hidden, `**` (a leading `*`) does
            // not cross hidden components.
            if !name.is_empty() {
                if no_hidden && name[0].starts_with('.') {
                    return Ok(false);
                }
                return match_segments(pat, &name[1..], no_hidden);
            }
            return Ok(false);
        }
        if name.is_empty() {
            return Ok(false);
        }
        if !match_component(pat[0], name[0], no_hidden)? {
            return Ok(false);
        }
        match_segments(&pat[1..], &name[1..], no_hidden)
    }

    /// Matches a single path component (no separators on either side).
    fn match_component(pat: &str, name: &str, no_hidden: bool) -> Result<bool, String> {
        if no_hidden && name.starts_with('.') && matches!(pat.chars().next(), Some('*') | Some('?'))
        {
            return Ok(false);
        }
        let p: Vec<char> = pat.chars().collect();
        let n: Vec<char> = name.chars().collect();
        match_chars(&p, &n)
    }

    fn match_chars(pat: &[char], name: &[char]) -> Result<bool, String> {
        let Some(&pc) = pat.first() else {
            return Ok(name.is_empty());
        };
        match pc {
            '*' => {
                for k in 0..=name.len() {
                    if match_chars(&pat[1..], &name[k..])? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            '?' => {
                if name.is_empty() {
                    return Ok(false);
                }
                match_chars(&pat[1..], &name[1..])
            }
            '[' => {
                let (consumed, negated, ranges) = parse_class(pat)?;
                let Some(&nc) = name.first() else {
                    return Ok(false);
                };
                let mut matched = ranges.iter().any(|&(lo, hi)| lo <= nc && nc <= hi);
                if negated {
                    matched = !matched;
                }
                if !matched {
                    return Ok(false);
                }
                match_chars(&pat[consumed..], &name[1..])
            }
            '\\' if ESCAPING_ENABLED => {
                let Some(&escaped) = pat.get(1) else {
                    return Err(bad_pattern());
                };
                if name.first() == Some(&escaped) {
                    return match_chars(&pat[2..], &name[1..]);
                }
                Ok(false)
            }
            c => {
                if name.first() == Some(&c) {
                    return match_chars(&pat[1..], &name[1..]);
                }
                Ok(false)
            }
        }
    }

    /// Parses a `[...]` character class starting at `pat[0] == '['`.
    /// Returns (chars consumed, negated, ranges).
    #[allow(clippy::type_complexity)]
    fn parse_class(pat: &[char]) -> Result<(usize, bool, Vec<(char, char)>), String> {
        let mut i = 1;
        let mut negated = false;
        if i < pat.len() && (pat[i] == '!' || pat[i] == '^') {
            negated = true;
            i += 1;
        }
        let mut ranges: Vec<(char, char)> = Vec::new();
        let mut first = true;
        let mut closed = false;
        while i < pat.len() {
            let mut c = pat[i];
            if c == ']' && !first {
                closed = true;
                i += 1;
                break;
            }
            first = false;
            if c == '\\' && ESCAPING_ENABLED {
                i += 1;
                if i >= pat.len() {
                    return Err(bad_pattern());
                }
                c = pat[i];
            }
            // Range?
            if i + 2 < pat.len() && pat[i + 1] == '-' && pat[i + 2] != ']' {
                let mut hi = pat[i + 2];
                let mut extra = 3;
                if hi == '\\' && ESCAPING_ENABLED {
                    if i + 3 >= pat.len() {
                        return Err(bad_pattern());
                    }
                    hi = pat[i + 3];
                    extra = 4;
                }
                if hi < c {
                    return Err(bad_pattern());
                }
                ranges.push((c, hi));
                i += extra;
            } else {
                ranges.push((c, c));
                i += 1;
            }
        }
        if !closed || ranges.is_empty() {
            return Err(bad_pattern());
        }
        Ok((i, negated, ranges))
    }

    /// Port of doublestar.FilepathGlob: returns filesystem entries (files and
    /// directories) matching `pattern`. `no_hidden` corresponds to
    /// doublestar.WithNoHidden. Missing base directories are not an error.
    pub(crate) fn filepath_glob(pattern: &str, no_hidden: bool) -> Result<Vec<String>, String> {
        validate_pattern(pattern)?;
        let norm = normalize_separators(pattern);

        // Split off the longest literal (meta-free, escape-free) prefix of
        // path segments; the walk starts there.
        let segs: Vec<&str> = norm.split('/').collect();
        let mut lit = 0;
        for seg in &segs {
            if segment_has_meta(seg) {
                break;
            }
            lit += 1;
        }

        if lit == segs.len() {
            // No meta characters at all: return the path itself if it exists.
            return Ok(if fs::symlink_metadata(&norm).is_ok() {
                vec![norm]
            } else {
                Vec::new()
            });
        }

        let base = segs[..lit].join("/");
        let remaining = segs.len() - lit;
        let has_doublestar = segs.contains(&"**");

        let expanded: Vec<String> = expand_braces(&norm)?;
        let expanded_segs: Vec<Vec<&str>> =
            expanded.iter().map(|p| p.split('/').collect()).collect();

        let mut results = Vec::new();

        // The base itself can match (e.g. `dir/**` matches `dir`).
        if !base.is_empty() && fs::symlink_metadata(&base).is_ok() {
            let name_segs: Vec<&str> = base.split('/').collect();
            for pat in &expanded_segs {
                if match_segments(pat, &name_segs, no_hidden)? {
                    results.push(base.clone());
                    break;
                }
            }
        }

        let walk_root = if base.is_empty() {
            if norm.starts_with('/') { "/" } else { "." }.to_string()
        } else {
            base.clone()
        };
        if fs::metadata(&walk_root)
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            let prefix = if base.is_empty() && norm.starts_with('/') {
                "".to_string() // absolute pattern: children are "/name"
            } else {
                base
            };
            walk_glob(
                &walk_root,
                &prefix,
                1,
                remaining,
                has_doublestar,
                &expanded_segs,
                no_hidden,
                &mut results,
            )?;
        }

        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn walk_glob(
        dir: &str,
        prefix: &str,
        depth: usize,
        max_depth: usize,
        has_doublestar: bool,
        pats: &[Vec<&str>],
        no_hidden: bool,
        results: &mut Vec<String>,
    ) -> Result<(), String> {
        let Ok(entries) = fs::read_dir(dir) else {
            // Glob matching ignores I/O errors (unreadable dirs are skipped).
            return Ok(());
        };
        let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            let full = if prefix.is_empty() {
                name.clone()
            } else if prefix == "/" {
                format!("/{name}")
            } else {
                format!("{prefix}/{name}")
            };

            let name_segs: Vec<&str> = full.split('/').collect();
            for pat in pats {
                if match_segments(pat, &name_segs, no_hidden)? {
                    results.push(full.clone());
                    break;
                }
            }

            let may_descend = has_doublestar || depth < max_depth;
            if may_descend {
                // Follow symlinked directories, like doublestar's walker.
                let full_dir = if prefix.is_empty() { &name } else { &full };
                if fs::metadata(full_dir).map(|m| m.is_dir()).unwrap_or(false) {
                    walk_glob(
                        full_dir,
                        &full,
                        depth + 1,
                        max_depth,
                        has_doublestar,
                        pats,
                        no_hidden,
                        results,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Reports whether the path segment contains unescaped glob meta chars
    /// (or escapes, which the literal walk root cannot resolve).
    fn segment_has_meta(seg: &str) -> bool {
        seg.contains(['*', '?', '[', '{']) || (ESCAPING_ENABLED && seg.contains('\\'))
    }

    #[cfg(test)]
    pub(crate) fn path_match_no_hidden(pattern: &str, name: &str) -> Result<bool, String> {
        match_internal(pattern, name, true)
    }
}

// ---------------------------------------------------------------------------
// processor.go — flags
// ---------------------------------------------------------------------------

static TENANT_IDS: Flag<ArrayString> = Flag::new(
    "fileCollector.tenantID",
    "Default tenant ID to use for logs collected from files in format: <accountID>:<projectID>. \
     See https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
    ArrayString::default,
);
static IGNORE_FIELDS: Flag<ArrayString> = Flag::new(
    "fileCollector.ignoreFields",
    "Fields to ignore across logs ingested from files",
    ArrayString::default,
);
static DECOLORIZE_FIELDS: Flag<ArrayString> = Flag::new(
    "fileCollector.decolorizeFields",
    "Fields to remove ANSI color codes across logs ingested from files",
    ArrayString::default,
);
static MSG_FIELD: Flag<ArrayString> = Flag::new(
    "fileCollector.msgField",
    "Fields that may contain the _msg field. \
     Default: message, msg, log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field",
    ArrayString::default,
);
static TIME_FIELD: Flag<ArrayString> = Flag::new(
    "fileCollector.timeField",
    "Fields that may contain the _time field. \
     Default: time, timestamp, ts. If none of the specified fields is found in the log line, then the read time will be used. \
     See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field",
    ArrayString::default,
);
static EXTRA_FIELDS: Flag<ArrayString> = Flag::new(
    "fileCollector.extraFields",
    "Extra fields in JSON format to add to each log line collected from files. \
     For example, -fileCollector.extraFields='{\"app\":\"nginx\", \"hostname\":\"%{HOST}\"}'. \
     The \"hostname\" and \"file\" fields are injected automatically; \
     see -fileCollector.hostnameField and -fileCollector.fileField for details",
    ArrayString::default,
);
static FILE_FIELD: Flag<String> = Flag::new(
    "fileCollector.fileField",
    "Field name used to store the source file path in collected log entries. Set to empty string to disable",
    || "file".to_string(),
);
static HOSTNAME_FIELD: Flag<String> = Flag::new(
    "fileCollector.hostnameField",
    "Field name used to store the hostname in collected log entries. Set to empty string to disable",
    || "hostname".to_string(),
);
static STREAM_FIELDS: Flag<ArrayString> = Flag::new(
    "fileCollector.streamFields",
    "Comma-separated list of fields to use as log stream fields for logs ingested from files. \
     Default: -fileCollector.fileField and -fileCollector.hostnameField. \
     See: https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields",
    ArrayString::default,
);

// ---------------------------------------------------------------------------
// processor.go — processor
// ---------------------------------------------------------------------------

pub struct Processor<S: LogRowsStorage> {
    storage: Arc<S>,
    extra_fields_json_len: usize,
    tenant_id: TenantID,

    log_rows: Option<LogRows>,

    rows_ingested_local: usize,
    bytes_ingested_local: usize,
}

pub fn new_processor<S: LogRowsStorage>(
    arg_idx: usize,
    file_path: &str,
    storage: Arc<S>,
) -> Processor<S> {
    let mut efs: Vec<Field> = get_extra_fields(arg_idx).to_vec();
    let mut default_stream_fields: Vec<&str> = Vec::new();

    let file_field = FILE_FIELD.get();
    if !file_field.is_empty() {
        efs.push(Field {
            name: file_field.clone(),
            value: file_path.to_string(),
        });
        default_stream_fields.push(file_field);
    }

    let hostname_field = HOSTNAME_FIELD.get();
    if !hostname_field.is_empty() {
        efs.push(Field {
            name: hostname_field.clone(),
            value: hostname(),
        });
        default_stream_fields.push(hostname_field);
    }

    let stream_fields = &STREAM_FIELDS.get().0;
    let sfs: Vec<&str> = if stream_fields.is_empty() {
        default_stream_fields
    } else {
        stream_fields.iter().map(String::as_str).collect()
    };

    let ignore_fields: Vec<&str> = IGNORE_FIELDS.get().0.iter().map(String::as_str).collect();
    let decolorize_fields: Vec<&str> = DECOLORIZE_FIELDS
        .get()
        .0
        .iter()
        .map(String::as_str)
        .collect();

    let log_rows = get_log_rows(
        &sfs,
        &ignore_fields,
        &decolorize_fields,
        &efs,
        DEFAULT_MSG_VALUE.get(),
    );

    Processor {
        storage,
        extra_fields_json_len: estimated_json_row_len(&efs),
        tenant_id: get_tenant_id(arg_idx),
        log_rows: Some(log_rows),
        rows_ingested_local: 0,
        bytes_ingested_local: 0,
    }
}

impl<S: LogRowsStorage + 'static> TailProcessor for Processor<S> {
    fn try_add_line(&mut self, line: &[u8]) -> bool {
        if line.is_empty() {
            // Skip empty lines to avoid zero-value logs with the content
            // "missing _msg field".
            return true;
        }

        let mut parser = get_json_parser();

        let mut ok = false;
        if line[0] == b'{' {
            // Automatically parse JSON, since there's no sense to use an
            // unstructured log format.
            ok = parser.parse_log_message(line, &[], "").is_ok();
            // Rename the message field to _msg.
            let msg_fields = get_msg_fields();
            rename_field(parser.fields_mut(), &msg_fields, "_msg");
        }
        if !ok {
            // PORT NOTE: Go aliases the line bytes via
            // bytesutil.ToUnsafeString; the owned Field requires a copy
            // (lossy for non-UTF-8 input).
            parser.fields_mut().push(Field {
                name: "_msg".to_string(),
                value: String::from_utf8_lossy(line).into_owned(),
            });
        }

        // Try to parse timestamp from the time fields.
        let time_fields = get_time_fields();
        let timestamp = extract_timestamp_from_fields(&time_fields, parser.fields_mut())
            .unwrap_or_else(|_| now_unix_nanos());

        if parser.fields().len() > 1000 {
            let mut line_json = Vec::new();
            marshal_fields_to_json(&mut line_json, parser.fields());
            warnf!(
                "dropping log line with {} fields; {}",
                parser.fields().len(),
                String::from_utf8_lossy(&line_json)
            );
            ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS.fetch_add(1, Ordering::Relaxed);
            put_json_parser(parser);
            return true;
        }

        let log_rows = self.log_rows.as_mut().unwrap();
        log_rows.must_add(self.tenant_id, timestamp, parser.fields_mut(), -1);
        self.storage.must_add_rows(log_rows);
        log_rows.reset_keep_settings();

        self.rows_ingested_local += 1;
        self.bytes_ingested_local += self.extra_fields_json_len + line.len();
        if self.rows_ingested_local > 128 {
            self.flush_metrics();
        }

        put_json_parser(parser);
        true
    }

    fn flush(&mut self) {
        self.flush_metrics();
    }

    fn must_close(&mut self) {
        self.flush_metrics();
        if let Some(log_rows) = self.log_rows.take() {
            put_log_rows(log_rows);
        }
    }
}

impl<S: LogRowsStorage> Processor<S> {
    fn flush_metrics(&mut self) {
        if self.rows_ingested_local == 0 {
            return;
        }
        ROWS_INGESTED_TOTAL.fetch_add(self.rows_ingested_local as u64, Ordering::Relaxed);
        BYTES_INGESTED_TOTAL.fetch_add(self.bytes_ingested_local as u64, Ordering::Relaxed);
        self.rows_ingested_local = 0;
        self.bytes_ingested_local = 0;
    }
}

// esl_rows_dropped_total{reason="too_many_fields"}
static ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS: AtomicU64 = AtomicU64::new(0);
// esl_rows_ingested_total{type="file_logs"}
static ROWS_INGESTED_TOTAL: AtomicU64 = AtomicU64::new(0);
// esl_bytes_ingested_total{type="file_logs"}
static BYTES_INGESTED_TOTAL: AtomicU64 = AtomicU64::new(0);

static PARSED_TENANT_IDS: OnceLock<Vec<TenantID>> = OnceLock::new();

fn get_tenant_id(arg_idx: usize) -> TenantID {
    let Some(parsed) = PARSED_TENANT_IDS.get() else {
        return TenantID::default();
    };
    if arg_idx >= parsed.len() {
        return TenantID::default();
    }
    parsed[arg_idx]
}

fn init_tenant_ids() {
    let glob_len = GLOB.get().0.len();
    let tenant_ids = TENANT_IDS.get();
    let mut parsed = Vec::with_capacity(glob_len);
    for i in 0..glob_len {
        let s = tenant_ids.get_optional_arg(i);
        if s.is_empty() {
            parsed.push(TenantID::default());
            continue;
        }
        match parse_tenant_id(s) {
            Ok(v) => parsed.push(v),
            Err(err) => {
                fatalf!("cannot parse -fileCollector.tenantID={s:?}: {err}");
                unreachable!()
            }
        }
    }
    let _ = PARSED_TENANT_IDS.set(parsed);
}

static PARSED_EXTRA_FIELDS: OnceLock<Vec<Vec<Field>>> = OnceLock::new();

fn get_extra_fields(arg_idx: usize) -> &'static [Field] {
    let Some(parsed) = PARSED_EXTRA_FIELDS.get() else {
        return &[];
    };
    if arg_idx >= parsed.len() {
        return &[];
    }
    &parsed[arg_idx]
}

fn init_extra_fields() {
    let glob_len = GLOB.get().0.len();
    let extra_fields = EXTRA_FIELDS.get();
    let mut parsed: Vec<Vec<Field>> = Vec::with_capacity(glob_len);
    for i in 0..glob_len {
        let s = extra_fields.get_optional_arg(i);
        if s.is_empty() {
            parsed.push(Vec::new());
            continue;
        }

        let mut p = get_json_parser();
        if let Err(err) = p.parse_log_message(s.as_bytes(), &[], "") {
            fatalf!("cannot parse -fileCollector.extraFields={s:?}: {err}");
            unreachable!()
        }

        let mut fields = p.fields().to_vec();
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        put_json_parser(p);

        parsed.push(fields);
    }
    let _ = PARSED_EXTRA_FIELDS.set(parsed);
}

const DEFAULT_MSG_FIELDS: [&str; 3] = ["message", "msg", "log"];

fn get_msg_fields() -> Vec<&'static str> {
    let msg_field = &MSG_FIELD.get().0;
    if msg_field.is_empty() {
        DEFAULT_MSG_FIELDS.to_vec()
    } else {
        msg_field.iter().map(String::as_str).collect()
    }
}

const DEFAULT_TIME_FIELDS: [&str; 3] = ["time", "timestamp", "ts"];

fn get_time_fields() -> Vec<&'static str> {
    let time_field = &TIME_FIELD.get().0;
    if time_field.is_empty() {
        DEFAULT_TIME_FIELDS.to_vec()
    } else {
        time_field.iter().map(String::as_str).collect()
    }
}

static HOSTNAME: OnceLock<String> = OnceLock::new();

/// Returns the hostname captured by [`init_hostname`], or "" when it wasn't
/// initialized (matching Go's zero-value package var in tests).
fn hostname() -> String {
    HOSTNAME.get().cloned().unwrap_or_default()
}

fn init_hostname() {
    // PORT NOTE: Go uses os.Hostname(). std Rust has no hostname API and new
    // dependencies are off-limits, so the kernel hostname is read on Linux
    // with an environment-variable fallback (COMPUTERNAME is always set on
    // Windows).
    match read_hostname() {
        Some(s) => {
            let _ = HOSTNAME.set(s);
        }
        None => {
            fatalf!("cannot get hostname: no hostname source available");
            unreachable!()
        }
    }
}

fn read_hostname() -> Option<String> {
    #[cfg(target_os = "linux")]
    if let Ok(s) = fs::read_to_string("/proc/sys/kernel/hostname") {
        let s = s.trim().to_string();
        if !s.is_empty() {
            return Some(s);
        }
    }
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        match std::env::var(var) {
            Ok(s) if !s.is_empty() => return Some(s),
            _ => {}
        }
    }
    None
}

/// Returns the current unix time in nanoseconds (Go `time.Now().UnixNano()`).
fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests: processor_test.go + internal doublestar matching cases
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use esl_logstorage::values_encoder::marshal_timestamp_rfc3339_nano_string;

    // testLogRowsStorage implements the LogRowsStorage trait.
    #[derive(Default)]
    struct TestLogRowsStorage {
        log_rows: Mutex<Vec<String>>,
        timestamps: Mutex<Vec<i64>>,
    }

    impl LogRowsStorage for TestLogRowsStorage {
        // MustAddRows implements the LogRowsStorage trait.
        fn must_add_rows(self: &Arc<Self>, lr: &LogRows) {
            lr.for_each_row(|_, r| {
                let mut row = Vec::new();
                marshal_fields_to_json(&mut row, &r.fields);
                self.log_rows
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(row).unwrap());
                self.timestamps.lock().unwrap().push(r.timestamp);
            });
        }

        // PORT NOTE: Go's testLogRowsStorage also implements CanWriteData();
        // the ported trait doesn't carry it (see the module-level PORT NOTE).
    }

    // Port of Go TestProcessorParseContent.
    #[test]
    fn test_processor_parse_content() {
        let f = |input: &[&str], results_expected: &[&str]| {
            let storage = Arc::new(TestLogRowsStorage::default());
            let mut proc = new_processor(0, "test.log", storage.clone());
            for s in input {
                proc.try_add_line(s.as_bytes());
            }

            let expected = results_expected.join("\n");
            let got = storage.log_rows.lock().unwrap().join("\n");
            assert_eq!(expected, got, "expected:\n{expected}\ngot:\n{got}");
        };

        // Empty content
        f(&[""], &[]);

        // Spaces
        f(
            &["", " ", "  "],
            &[
                r#"{"_msg":" ","file":"test.log"}"#,
                r#"{"_msg":"  ","file":"test.log"}"#,
            ],
        );

        // JSON content
        f(
            &[r#"{"_msg":"foo bar","file":"test.log"}"#],
            &[r#"{"_msg":"foo bar","file":"test.log"}"#],
        );

        // Started like JSON object, but it is a regular log line
        f(&["{foobar}"], &[r#"{"_msg":"{foobar}","file":"test.log"}"#]);

        // Non-JSON content
        f(
            &["foo", "bar", "buz"],
            &[
                r#"{"_msg":"foo","file":"test.log"}"#,
                r#"{"_msg":"bar","file":"test.log"}"#,
                r#"{"_msg":"buz","file":"test.log"}"#,
            ],
        );
    }

    // Port of Go TestProcessorSetTimestamp.
    #[test]
    fn test_processor_set_timestamp() {
        let f = |input: &str, timestamps_expected: &[i64]| {
            let storage = Arc::new(TestLogRowsStorage::default());
            let mut proc = new_processor(0, "test.log", storage.clone());
            proc.try_add_line(input.as_bytes());
            proc.must_close();

            let timestamps = storage.timestamps.lock().unwrap();
            assert_eq!(
                timestamps.as_slice(),
                timestamps_expected,
                "unexpected timestamps; expected:\n{timestamps_expected:?}\ngot:\n{timestamps:?}"
            );
        };

        let fmt_rfc3339 = |nsecs: i64| -> String {
            let mut buf = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut buf, nsecs);
            String::from_utf8(buf).unwrap()
        };

        let current_nanos = now_unix_nanos();
        let current_secs = current_nanos / 1_000_000_000;
        let current_millis = current_nanos / 1_000_000;
        let current_micros = current_nanos / 1_000;

        // RFC3339
        let input = format!(
            r#"{{"_msg":"foo","time":"{}"}}"#,
            fmt_rfc3339(current_secs * 1_000_000_000)
        );
        f(&input, &[current_secs * 1_000_000_000]);

        // RFC3339 nano
        let input = format!(
            r#"{{"_msg":"foo","time":"{}"}}"#,
            fmt_rfc3339(current_nanos)
        );
        f(&input, &[current_nanos]);

        // Unix timestamp
        let input = format!(r#"{{"_msg":"foo","time":{current_secs}}}"#);
        f(&input, &[current_secs * 1_000_000_000]);

        // Unix timestamp with milliseconds precision
        let input = format!(r#"{{"_msg":"foo","time":{current_millis}}}"#);
        f(&input, &[current_millis * 1_000_000]);

        // Unix timestamp with microseconds precision
        let input = format!(r#"{{"_msg":"foo","time":{current_micros}}}"#);
        f(&input, &[current_micros * 1_000]);

        // Unix timestamp with nanosecond precision
        let input = format!(r#"{{"_msg":"foo","time":{current_nanos}}}"#);
        f(&input, &[current_nanos]);
    }

    #[test]
    fn test_is_glob() {
        let f = |s: &str, expected: bool| {
            assert_eq!(is_glob(s), expected, "is_glob({s:?})");
        };

        f("", false);
        f("/var/log/messages", false);
        f("/var/log/*.log", true);
        f("/var/log/file?.log", true);
        f("/var/log/[ab].log", true);
        f("/var/log/{a,b}.log", true);
        #[cfg(unix)]
        f("/var/log/\\*.log", false); // escaped meta char
    }

    // Matching cases following doublestar/v4 semantics.
    #[test]
    fn test_path_match() {
        let f = |pattern: &str, name: &str, expected: bool| {
            let got = doublestar::path_match(pattern, name)
                .unwrap_or_else(|err| panic!("unexpected error for {pattern:?}: {err}"));
            assert_eq!(got, expected, "path_match({pattern:?}, {name:?})");
        };

        // Literal and single-segment wildcards.
        f("abc", "abc", true);
        f("abc", "abd", false);
        f("*", "abc", true);
        f("*", "", true);
        f("*", "a/b", false);
        f("a*", "abc", true);
        f("a*b", "ab", true);
        f("a*b", "axxb", true);
        f("a*b", "axxbc", false);
        f("?at", "cat", true);
        f("?at", "at", false);
        f("*c", "abc", true);
        f("a*/b", "abc/b", true);
        f("a*/b", "a/c/b", false);

        // Character classes.
        f("[abc]at", "bat", true);
        f("[abc]at", "dat", false);
        f("[!abc]at", "dat", true);
        f("[!abc]at", "bat", false);
        f("[^abc]at", "dat", true);
        f("[a-c]at", "bat", true);
        f("[a-c]at", "dat", false);
        f("[]]", "]", true);

        // Doublestar.
        f("a/**/b", "a/b", true);
        f("a/**/b", "a/x/b", true);
        f("a/**/b", "a/x/y/b", true);
        f("a/**", "a", true);
        f("a/**", "a/b", true);
        f("a/**", "a/b/c", true);
        f("**/c", "c", true);
        f("**/c", "a/b/c", true);
        f("**/c", "a/b/d", false);
        f("**", "a/b/c", true);

        // Alternates.
        f("{cat,bat}", "bat", true);
        f("{cat,bat}", "rat", false);
        f("{cat,[fb]at}", "fat", true);
        f("a/{b,c/d}/e", "a/c/d/e", true);
        f("a/{b,c/d}/e", "a/b/e", true);
        f("a/{b,c/d}/e", "a/c/e", false);

        // Escaping (unix only; escaping is disabled on Windows).
        #[cfg(unix)]
        {
            f("\\*", "*", true);
            f("\\*", "x", false);
            f("a\\?b", "a?b", true);
            f("a\\?b", "axb", false);
        }

        // Bad patterns.
        for pattern in ["[", "a[", "[a-", "{a", "a}b{"] {
            assert!(
                doublestar::path_match(pattern, "x").is_err(),
                "expected error for pattern {pattern:?}"
            );
        }

        // Init-style validation call.
        assert!(doublestar::path_match("/var/log/*.log", ".").is_ok());
    }

    #[test]
    fn test_path_match_no_hidden() {
        let f = |pattern: &str, name: &str, expected: bool| {
            let got = doublestar::path_match_no_hidden(pattern, name)
                .unwrap_or_else(|err| panic!("unexpected error for {pattern:?}: {err}"));
            assert_eq!(got, expected, "path_match_no_hidden({pattern:?}, {name:?})");
        };

        f("*", ".hidden", false);
        f(".*", ".hidden", true);
        f("?at", ".at", false);
        f("logs/*.log", "logs/.secret.log", false);
        f("logs/.*.log", "logs/.secret.log", true);
        f("**/x", ".hidden/x", false);
        f("a/**", "a/.hidden", false);
    }

    #[cfg(unix)]
    #[test]
    fn test_filepath_glob() {
        use std::path::PathBuf;

        struct TempDir {
            path: PathBuf,
        }
        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.path);
            }
        }

        let td = TempDir {
            path: std::env::temp_dir().join(format!(
                "esl-agent-filecollector-glob-test-{}",
                std::process::id()
            )),
        };
        let root = &td.path;
        let _ = fs::remove_dir_all(root);
        fs::create_dir_all(root.join("app/sub")).unwrap();
        for p in [
            "app/a.log",
            "app/b.log",
            "app/c.txt",
            "app/.hidden.log",
            "app/sub/d.log",
        ] {
            fs::write(root.join(p), b"x\n").unwrap();
        }

        let rootstr = root.to_str().unwrap();
        let f = |pattern: &str, expected: &[&str]| {
            let got = doublestar::filepath_glob(pattern, true)
                .unwrap_or_else(|err| panic!("unexpected error for {pattern:?}: {err}"));
            let expected: Vec<String> = expected.iter().map(|p| format!("{rootstr}/{p}")).collect();
            assert_eq!(got, expected, "filepath_glob({pattern:?})");
        };

        f(&format!("{rootstr}/app/*.log"), &["app/a.log", "app/b.log"]);
        f(
            &format!("{rootstr}/app/**/*.log"),
            &["app/a.log", "app/b.log", "app/sub/d.log"],
        );
        f(&format!("{rootstr}/app/.*.log"), &["app/.hidden.log"]);
        f(&format!("{rootstr}/app/*.gz"), &[]);
        // `dir/**` matches the dir itself plus everything visible below it.
        f(
            &format!("{rootstr}/app/sub/**"),
            &["app/sub", "app/sub/d.log"],
        );
        // Missing base directory is not an error.
        f(&format!("{rootstr}/missing/*.log"), &[]);
    }
}
