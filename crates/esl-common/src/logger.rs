//! Port of Softalink LLC `lib/logger`.
//!
//! The macro API below (`infof!`, `warnf!`, `errorf!`, `fatalf!`, `panicf!`)
//! is the stable surface every other ported package logs through; the
//! internals (flag-controlled level/format/output, throttling, suppression)
//! mirror the Go package.

use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::panic::Location;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Once, OnceLock, mpsc};

use crate::flagutil::{Flag, go_quote};

static LOGGER_LEVEL: Flag<String> = Flag::new(
    "loggerLevel",
    "Minimum level of errors to log. Possible values: INFO, WARN, ERROR, FATAL, PANIC",
    || "INFO".to_string(),
);
static LOGGER_FORMAT: Flag<String> = Flag::new(
    "loggerFormat",
    "Format for logs. Possible values: default, json",
    || "default".to_string(),
);
static LOGGER_OUTPUT: Flag<String> = Flag::new(
    "loggerOutput",
    "Output for the logs. Supported values: stderr, stdout",
    || "stderr".to_string(),
);
// PORT NOTE: Go loads any IANA timezone via time.LoadLocation (with embedded
// tzdata); the port has no IANA tzdb dependency, so only "UTC" (also ""),
// and "Local" are supported — named zones are a fatal init error like Go's
// unknown-zone Fatalf. See init_timezone().
static LOGGER_TIMEZONE: Flag<String> = Flag::new(
    "loggerTimezone",
    "Timezone to use for timestamps in logs. Timezone must be a valid IANA Time Zone. \
     For example: America/New_York, Europe/Berlin, Etc/GMT+3 or Local",
    || "UTC".to_string(),
);
static DISABLE_TIMESTAMPS: Flag<bool> = Flag::new(
    "loggerDisableTimestamps",
    "Whether to disable writing timestamps in logs",
    || false,
);
static MAX_LOG_ARG_LEN: Flag<i64> = Flag::new(
    "loggerMaxArgLen",
    "The maximum length of a single logged argument. Longer arguments are replaced with 'arg_start..arg_end', \
     where 'arg_start' and 'arg_end' is prefix and suffix of the arg with the length not exceeding -loggerMaxArgLen / 2",
    || 5000,
);
static ERRORS_PER_SECOND_LIMIT: Flag<i64> = Flag::new(
    "loggerErrorsPerSecondLimit",
    "Per-second limit on the number of ERROR messages. If more than the given number of errors are emitted per second, \
     the remaining errors are suppressed. Zero values disable the rate limit",
    || 0,
);
static WARNS_PER_SECOND_LIMIT: Flag<i64> = Flag::new(
    "loggerWarnsPerSecondLimit",
    "Per-second limit on the number of WARN messages. If more than the given number of warns are emitted per second, \
     then the remaining warns are suppressed. Zero values disable the rate limit",
    || 0,
);
static LOGGER_JSON_FIELDS: Flag<String> = Flag::new(
    "loggerJSONFields",
    "Allows renaming fields in JSON formatted logs. \
     Example: \"ts:timestamp,msg:message\" renames \"ts\" to \"timestamp\" and \"msg\" to \"message\". \
     Supported fields: ts, level, caller, msg",
    String::new,
);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Info = 0,
    Warn = 1,
    Error = 2,
    Fatal = 3,
    Panic = 4,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
            Level::Panic => "panic",
        }
    }

    fn from_flag(s: &str) -> Level {
        match s {
            "INFO" => Level::Info,
            "WARN" => Level::Warn,
            "ERROR" => Level::Error,
            "FATAL" => Level::Fatal,
            "PANIC" => Level::Panic,
            _ => panic!(
                "FATAL: unsupported `-loggerLevel` value: {s:?}; supported values are: INFO, WARN, ERROR, FATAL, PANIC"
            ),
        }
    }
}

static MIN_LEVEL: AtomicU8 = AtomicU8::new(0);

/// Initializes the logger from command-line flags and logs the build version
/// together with all the explicitly set flags.
///
/// Mirrors Go `logger.Init`. Must be called after flag parsing.
/// There is no need in calling init from tests.
pub fn init() {
    init_internal(true);
}

/// Initializes the logger without logging the flags, like Go
/// `logger.InitNoLogFlags`.
pub fn init_no_log_flags() {
    init_internal(false);
}

fn init_internal(log_flags: bool) {
    set_logger_json_fields();
    set_logger_output();
    validate_logger_level();
    validate_logger_format();
    init_timezone();

    static CLEANER_ONCE: Once = Once::new();
    CLEANER_ONCE.call_once(|| {
        std::thread::Builder::new()
            .name("logLimiterCleaner".to_string())
            .spawn(|| {
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    LOG_LIMITER.reset();
                }
            })
            .expect("cannot spawn logLimiterCleaner thread");
    });

    if log_flags {
        log_all_flags();
    }
}

/// UTC offset of the `-loggerTimezone` zone in seconds, applied to log
/// timestamps. Zero (UTC) until init_timezone() runs, like Go's
/// `var timezone = time.UTC`.
static TIMEZONE_OFFSET_SECS: AtomicI64 = AtomicI64::new(0);

// PORT NOTE: Go loads the -loggerTimezone IANA timezone via time.LoadLocation
// (with embedded tzdata). The port has no IANA tzdb dependency, so it
// supports "UTC" (and "", which LoadLocation also treats as UTC) and "Local".
// "Local" uses the OS zone offset sampled at startup, so unlike Go a DST
// transition during the process lifetime does not shift later timestamps.
// Any other zone name is a fatal init error (Go fatals only on UNKNOWN
// zones).
fn init_timezone() {
    let tz = LOGGER_TIMEZONE.get().as_str();
    let offset_secs = match tz {
        "UTC" | "" => 0,
        "Local" => crate::timeutil::get_local_timezone_offset_nsecs() / 1_000_000_000,
        _ => panic!(
            "cannot load timezone {tz:?}: named IANA timezones need a tzdb, which this port does not bundle; supported values: UTC, Local"
        ),
    };
    TIMEZONE_OFFSET_SECS.store(offset_secs, Ordering::Relaxed);
}

fn validate_logger_level() {
    let level = Level::from_flag(LOGGER_LEVEL.get());
    set_min_level(level);
}

fn validate_logger_format() {
    match LOGGER_FORMAT.get().as_str() {
        "default" | "json" => {}
        v => panic!(
            "FATAL: unsupported `-loggerFormat` value: {v:?}; supported values are: default, json"
        ),
    }
}

enum Output {
    Stderr,
    Stdout,
    Test(Arc<Mutex<dyn Write + Send>>),
}

static OUTPUT: Mutex<Output> = Mutex::new(Output::Stderr);

fn set_logger_output() {
    let o = match LOGGER_OUTPUT.get().as_str() {
        "stderr" => Output::Stderr,
        "stdout" => Output::Stdout,
        v => panic!(
            "FATAL: unsupported `loggerOutput` value: {v:?}; supported values are: stderr, stdout"
        ),
    };
    *OUTPUT.lock().unwrap() = o;
}

/// Redefines the logger output. Use for tests only. Call
/// [`reset_output_for_test`] to return the output state to default.
pub fn set_output_for_tests(writer: Arc<Mutex<dyn Write + Send>>) {
    *OUTPUT.lock().unwrap() = Output::Test(writer);
}

/// Sets the logger output to the default value.
pub fn reset_output_for_test() {
    *OUTPUT.lock().unwrap() = Output::Stderr;
}

fn write_log(msg: &str) {
    // Serialize writes to the log via the OUTPUT lock, like Go's `mu`.
    let output = OUTPUT.lock().unwrap();
    match &*output {
        Output::Stderr => {
            let _ = std::io::stderr().write_all(msg.as_bytes());
        }
        Output::Stdout => {
            let _ = std::io::stdout().write_all(msg.as_bytes());
        }
        Output::Test(w) => {
            let _ = w.lock().unwrap().write_all(msg.as_bytes());
        }
    }
}

/// JSON log field names, adjustable via -loggerJSONFields
/// (port of `lib/logger/json_fields.go`).
struct JsonFields {
    ts: String,
    level: String,
    caller: String,
    msg: String,
}

impl Default for JsonFields {
    fn default() -> Self {
        JsonFields {
            ts: "ts".to_string(),
            level: "level".to_string(),
            caller: "caller".to_string(),
            msg: "msg".to_string(),
        }
    }
}

static JSON_FIELDS: OnceLock<JsonFields> = OnceLock::new();

fn json_fields() -> &'static JsonFields {
    static DEFAULT: OnceLock<JsonFields> = OnceLock::new();
    JSON_FIELDS
        .get()
        .unwrap_or_else(|| DEFAULT.get_or_init(JsonFields::default))
}

fn set_logger_json_fields() {
    let _ = JSON_FIELDS.set(parse_json_fields(LOGGER_JSON_FIELDS.get()));
}

// PORT NOTE: Go uses log.Fatalf on invalid -loggerJSONFields values; this
// port panics, since the logger isn't initialized yet either way.
fn parse_json_fields(s: &str) -> JsonFields {
    let mut fields = JsonFields::default();
    if s.is_empty() {
        return fields;
    }
    for f in s.split(',') {
        let f = f.trim();
        let v: Vec<&str> = f.split(':').collect();
        if v.len() != 2 {
            panic!("missing ':' delimiter in -loggerJSONFields={s:?} for {f:?} item");
        }
        let (name, value) = (v[0], v[1]);
        match name {
            "ts" => fields.ts = value.to_string(),
            "level" => fields.level = value.to_string(),
            "caller" => fields.caller = value.to_string(),
            "msg" => fields.msg = value.to_string(),
            _ => panic!(
                "unexpected json field name in -loggerJSONFields={s:?}: {name:?}; supported values: ts, level, caller, msg"
            ),
        }
    }
    fields
}

fn log_all_flags() {
    crate::infof!("build version: {}", crate::buildinfo::version());
    crate::infof!("command-line flags");
    crate::flagutil::visit_set_flags(|name, value| {
        let lname = name.to_lowercase();
        let value = if crate::flagutil::is_secret_flag(&lname) {
            "secret"
        } else {
            value
        };
        crate::infof!("  -{}={}", name, go_quote(value));
    });
}

/// Sets the minimum log level directly. Levels below it are suppressed
/// (but still counted in the log-message counters).
pub fn set_min_level(level: Level) {
    MIN_LEVEL.store(level as u8, Ordering::Relaxed);
}

fn should_skip(level: Level) -> bool {
    (level as u8) < MIN_LEVEL.load(Ordering::Relaxed)
}

/// Core log entrypoint used by the level macros. Not called directly.
pub fn log_at(level: Level, location: &Location<'_>, args: fmt::Arguments<'_>) {
    let location = format!("{}:{}", short_file(location.file()), location.line());

    if should_skip(level) {
        // Increment the log-message counter even if the log is suppressed.
        // This simplifies troubleshooting when logs are suppressed.
        count_log_message(level, &location, false);
        return;
    }

    // PORT NOTE: Go's formatLogMessage truncates each %s/%q argument with
    // -loggerMaxArgLen before rendering. Rust's format_args! pre-renders the
    // message, so the limit is applied to the whole formatted message here;
    // format_log_message() keeps the per-argument algorithm for parity.
    let msg = limit_string_len(&format!("{args}"), *MAX_LOG_ARG_LEN.get());
    let is_printed = log_message_internal(level, &msg, &location);
    count_log_message(level, &location, is_printed);
}

// Go increments `vm_log_messages_total{...}` counters in lib/metrics; the
// port registers the same series (rebranded `esm_`) in the metrics registry.
fn count_log_message(level: Level, location: &str, is_printed: bool) {
    let counter_name = format!(
        "esm_log_messages_total{{app_version={}, level={}, location={}, is_printed=\"{}\"}}",
        go_quote(crate::buildinfo::version()),
        go_quote(level.as_str()),
        go_quote(location),
        is_printed
    );
    crate::metrics::get_or_create_counter(&counter_name).inc();
}

fn log_message_internal(level: Level, msg: &str, location: &str) -> bool {
    let disable_timestamps = *DISABLE_TIMESTAMPS.get();
    let timestamp = if disable_timestamps {
        String::new()
    } else {
        now_timestamp()
    };

    // Rate limit ERROR and WARN log messages with the given limit.
    let mut msg = msg.to_string();
    if level == Level::Error || level == Level::Warn {
        let limit = if level == Level::Warn {
            *WARNS_PER_SECOND_LIMIT.get()
        } else {
            *ERRORS_PER_SECOND_LIMIT.get()
        };
        let (suppress, suppress_message) = LOG_LIMITER.need_suppress(location, limit.max(0) as u64);
        if suppress {
            return false;
        }
        if !suppress_message.is_empty() {
            msg = format!("{suppress_message}{msg}");
        }
    }

    while msg.ends_with('\n') {
        msg.pop();
    }

    let format = LOGGER_FORMAT.get().as_str();
    let log_msg = compose_log_msg(
        format,
        disable_timestamps,
        &timestamp,
        level.as_str(),
        location,
        &msg,
        json_fields(),
    );
    write_log(&log_msg);

    match level {
        Level::Panic => {
            if format == "json" {
                // Do not clutter `json` output with the panic stack trace.
                std::process::exit(-1);
            }
            panic!("{msg}");
        }
        Level::Fatal => std::process::exit(-1),
        _ => {}
    }

    true
}

fn compose_log_msg(
    format: &str,
    disable_timestamps: bool,
    timestamp: &str,
    level_lowercase: &str,
    location: &str,
    msg: &str,
    fields: &JsonFields,
) -> String {
    match format {
        "json" => {
            if disable_timestamps {
                format!(
                    "{{{}:{},{}:{},{}:{}}}\n",
                    go_quote(&fields.level),
                    go_quote(level_lowercase),
                    go_quote(&fields.caller),
                    go_quote(location),
                    go_quote(&fields.msg),
                    go_quote(msg),
                )
            } else {
                format!(
                    "{{{}:{},{}:{},{}:{},{}:{}}}\n",
                    go_quote(&fields.ts),
                    go_quote(timestamp),
                    go_quote(&fields.level),
                    go_quote(level_lowercase),
                    go_quote(&fields.caller),
                    go_quote(location),
                    go_quote(&fields.msg),
                    go_quote(msg),
                )
            }
        }
        _ => {
            if disable_timestamps {
                format!("{level_lowercase}\t{location}\t{msg}\n")
            } else {
                format!("{timestamp}\t{level_lowercase}\t{location}\t{msg}\n")
            }
        }
    }
}

/// Formats the current time like Go's "2006-01-02T15:04:05.000Z0700" layout
/// in the `-loggerTimezone` zone (see init_timezone()).
fn now_timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    timestamp_with_offset(
        now.as_secs(),
        now.subsec_millis(),
        TIMEZONE_OFFSET_SECS.load(Ordering::Relaxed),
    )
}

/// Renders `secs`/`millis` since the epoch in Go's
/// "2006-01-02T15:04:05.000Z0700" layout at the given UTC offset: `Z` for a
/// zero offset, `+hhmm`/`-hhmm` otherwise, like Go's `Z0700` verb.
fn timestamp_with_offset(secs: u64, millis: u32, offset_secs: i64) -> String {
    let local_secs = (secs as i64 + offset_secs).max(0) as u64;
    let mut out = format!("{}.{millis:03}", format_utc_timestamp(local_secs));
    if offset_secs == 0 {
        out.push('Z');
    } else {
        let mins = offset_secs / 60;
        out.push(if mins < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}{:02}", mins.abs() / 60, mins.abs() % 60));
    }
    out
}

/// Port of Go `logger.formatLogMessage`: renders `format` with `args`,
/// truncating every `%s`/`%q` argument to `max_arg_len` via
/// `stringsutil.LimitStringLen`.
///
/// PORT NOTE: Rust has no printf; only the `%s`, `%q`, `%d`, `%v` and `%%`
/// verbs are interpreted, which covers the logger usage in this codebase.
pub fn format_log_message(max_arg_len: i64, format: &str, args: &[&dyn fmt::Display]) -> String {
    let mut out = String::with_capacity(format.len());
    let mut rest = format;
    let mut arg_idx = 0usize;
    while let Some(n) = rest.find('%') {
        out.push_str(&rest[..n]);
        rest = &rest[n + 1..];
        match rest.chars().next() {
            None => {
                out.push('%');
            }
            Some('%') => {
                out.push('%');
                rest = &rest[1..];
            }
            Some(verb @ ('s' | 'q' | 'd' | 'v')) => {
                rest = &rest[1..];
                if arg_idx < args.len() {
                    let mut s = args[arg_idx].to_string();
                    arg_idx += 1;
                    if verb == 's' || verb == 'q' {
                        s = limit_string_len(&s, max_arg_len);
                    }
                    if verb == 'q' {
                        out.push_str(&go_quote(&s));
                    } else {
                        out.push_str(&s);
                    }
                } else {
                    out.push('%');
                    out.push(verb);
                }
            }
            Some(other) => {
                out.push('%');
                out.push(other);
                rest = &rest[other.len_utf8()..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Port of `stringsutil.LimitStringLen`: if `s` is longer than `max_len`,
/// it is replaced with `prefix..suffix`.
///
/// PORT NOTE: duplicated here until `lib/stringsutil` is ported; Go slices
/// bytes and may split multibyte chars, this port replaces broken chars
/// lossily.
fn limit_string_len(s: &str, max_len: i64) -> String {
    let max_len = if max_len < 4 { 4 } else { max_len as usize };
    if s.len() <= max_len {
        return s.to_string();
    }
    let n = (max_len / 2) - 1;
    let b = s.as_bytes();
    format!(
        "{}..{}",
        String::from_utf8_lossy(&b[..n]),
        String::from_utf8_lossy(&b[b.len() - n..])
    )
}

static LOG_LIMITER: LazyLock<LogLimit> = LazyLock::new(LogLimit::new);

struct LogLimit {
    m: Mutex<HashMap<String, u64>>,
}

impl LogLimit {
    fn new() -> Self {
        LogLimit {
            m: Mutex::new(HashMap::new()),
        }
    }

    fn reset(&self) {
        self.m.lock().unwrap().clear();
    }

    /// Checks whether the number of calls for the given location exceeds the
    /// given limit.
    ///
    /// When the number of calls equals the limit, a log message prefix is
    /// returned.
    fn need_suppress(&self, location: &str, limit: u64) -> (bool, String) {
        // fast path
        let mut msg = String::new();
        if limit == 0 {
            return (false, msg);
        }
        let mut m = self.m.lock().unwrap();

        if let Some(&n) = m.get(location) {
            if n >= limit {
                if n == limit {
                    // report only once
                    msg = format!("suppressing log message with rate limit={limit}: ");
                } else {
                    return (true, msg);
                }
            }
            m.insert(location.to_string(), n + 1);
        } else {
            m.insert(location.to_string(), 1);
        }
        (false, msg)
    }
}

static LOG_THROTTLER_REGISTRY: LazyLock<Mutex<HashMap<String, &'static LogThrottler>>> =
    LazyLock::new(Default::default);

/// Returns a logger throttled by time - only one message in the throttle
/// duration will be logged.
///
/// A new logger is created only once for each unique name passed.
/// The function is thread-safe.
pub fn with_throttler(name: &str, throttle: std::time::Duration) -> &'static LogThrottler {
    let mut registry = LOG_THROTTLER_REGISTRY.lock().unwrap();
    if let Some(lt) = registry.get(name) {
        return lt;
    }
    let lt: &'static LogThrottler = Box::leak(Box::new(LogThrottler::new(throttle, name)));
    registry.insert(name.to_string(), lt);
    lt
}

/// A logger which throttles log messages passed to
/// [`LogThrottler::warnf`] and [`LogThrottler::errorf`].
///
/// LogThrottler must be created via [`with_throttler`].
pub struct LogThrottler {
    ch: mpsc::SyncSender<()>,
    dropped: AtomicU64,
}

impl LogThrottler {
    fn new(throttle: std::time::Duration, name: &str) -> Self {
        let (tx, rx) = mpsc::sync_channel::<()>(1);
        std::thread::Builder::new()
            .name(format!("logThrottler-{name}"))
            .spawn(move || {
                while rx.recv().is_ok() {
                    std::thread::sleep(throttle);
                }
            })
            .expect("cannot spawn logThrottler thread");
        LogThrottler {
            ch: tx,
            dropped: AtomicU64::new(0),
        }
    }

    /// Logs an error message, unless throttled.
    #[track_caller]
    pub fn errorf(&self, args: fmt::Arguments<'_>) {
        self.log_throttled(Level::Error, args);
    }

    /// Logs a warn message, unless throttled.
    #[track_caller]
    pub fn warnf(&self, args: fmt::Arguments<'_>) {
        self.log_throttled(Level::Warn, args);
    }

    #[track_caller]
    fn log_throttled(&self, level: Level, args: fmt::Arguments<'_>) {
        match self.ch.try_send(()) {
            Ok(()) => {
                let dropped = self.dropped.swap(0, Ordering::SeqCst);
                let msg = format!(
                    "{args}. (Similar {dropped} log messages were dropped due to throttling)"
                );
                log_at(level, Location::caller(), format_args!("{msg}"));
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

/// Port of the writer behind Go `logger.StdErrorLogger()`: every written
/// buffer is logged as an ERROR message.
///
/// PORT NOTE: Go returns a `*log.Logger` that reports the caller's frame;
/// this writer reports its own location instead.
pub struct StdErrorLogWriter;

impl Write for StdErrorLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = String::from_utf8_lossy(buf);
        log_at(Level::Error, Location::caller(), format_args!("{s}"));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Formats seconds since the epoch as `YYYY-MM-DDTHH:MM:SS` in UTC.
fn format_utc_timestamp(secs: u64) -> String {
    // Civil-time conversion (Howard Hinnant's algorithm); avoids a chrono dep.
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mth <= 2 { y + 1 } else { y };
    format!("{y:04}-{mth:02}-{d:02}T{h:02}:{m:02}:{s:02}")
}

/// Trims a source path down to its last two components, like Go's
/// `getLogLocation`.
///
/// PORT NOTE: `file!()` bakes backslash separators when the crate is
/// compiled natively on Windows (cross-compiled builds keep `/`), so both
/// separators are handled and the result is normalized to `/` like Go.
fn short_file(file: &str) -> std::borrow::Cow<'_, str> {
    let sep = |c: char| c == '/' || c == '\\';
    let short = match file.rfind(sep) {
        Some(pos) => match file[..pos].rfind(sep) {
            Some(pos2) => &file[pos2 + 1..],
            None => file,
        },
        None => file,
    };
    if short.contains('\\') {
        std::borrow::Cow::Owned(short.replace('\\', "/"))
    } else {
        std::borrow::Cow::Borrowed(short)
    }
}

#[macro_export]
macro_rules! infof {
    ($($arg:tt)*) => {
        $crate::logger::log_at($crate::logger::Level::Info, ::std::panic::Location::caller(), format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! warnf {
    ($($arg:tt)*) => {
        $crate::logger::log_at($crate::logger::Level::Warn, ::std::panic::Location::caller(), format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! errorf {
    ($($arg:tt)*) => {
        $crate::logger::log_at($crate::logger::Level::Error, ::std::panic::Location::caller(), format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! fatalf {
    ($($arg:tt)*) => {
        $crate::logger::log_at($crate::logger::Level::Fatal, ::std::panic::Location::caller(), format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! panicf {
    ($($arg:tt)*) => {
        $crate::logger::log_at($crate::logger::Level::Panic, ::std::panic::Location::caller(), format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests which redirect the global logger output must not run
    // concurrently with each other.
    static OUTPUT_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_format_log_message() {
        fn f(format: &str, args: &[&dyn fmt::Display], max_arg_len: i64, expected_result: &str) {
            let result = format_log_message(max_arg_len, format, args);
            assert_eq!(
                result, expected_result,
                "unexpected result; got\n{result:?}\nwant\n{expected_result:?}"
            );
        }

        // Zero format args
        f("foobar", &[], 1, "foobar");

        // Format args not exceeding the maxArgLen
        f(
            "foo: %d, %s, %s, %s",
            &[&123, &"bar", &"baz", &"abc"],
            3,
            "foo: 123, bar, baz, abc",
        );

        // Format args exceeding the maxArgLen
        f(
            "foo: %s, %q, %s",
            &[&"abcde", &"foo bar baz", &"xx"],
            4,
            "foo: a..e, \"f..z\", xx",
        );
    }

    #[test]
    fn test_timestamp_with_offset() {
        // Go's "2006-01-02T15:04:05.000Z0700" layout: "Z" at zero offset,
        // ±hhmm otherwise (2021-01-02T03:04:05 UTC = 1609556645).
        assert_eq!(
            timestamp_with_offset(1_609_556_645, 7, 0),
            "2021-01-02T03:04:05.007Z"
        );
        assert_eq!(
            timestamp_with_offset(1_609_556_645, 7, 5 * 3600 + 30 * 60),
            "2021-01-02T08:34:05.007+0530"
        );
        assert_eq!(
            timestamp_with_offset(1_609_556_645, 7, -8 * 3600),
            "2021-01-01T19:04:05.007-0800"
        );
    }

    #[test]
    fn test_limit_string_len() {
        assert_eq!(limit_string_len("bar", 3), "bar");
        assert_eq!(limit_string_len("abcd", 3), "abcd");
        assert_eq!(limit_string_len("abcde", 4), "a..e");
        assert_eq!(limit_string_len("foo bar baz", 4), "f..z");
        assert_eq!(limit_string_len("foo bar baz", 100), "foo bar baz");
        assert_eq!(limit_string_len("abcdefgh", 6), "ab..gh");
    }

    #[test]
    fn test_log_limit_need_suppress() {
        let ll = LogLimit::new();

        // Zero limit disables the rate limit.
        for _ in 0..10 {
            let (suppress, msg) = ll.need_suppress("foo.rs:1", 0);
            assert!(!suppress);
            assert!(msg.is_empty());
        }

        // Limit of 2: two messages logged, third gets the suppression
        // prefix, fourth and later are suppressed.
        let (suppress, msg) = ll.need_suppress("bar.rs:2", 2);
        assert!(!suppress);
        assert!(msg.is_empty());
        let (suppress, msg) = ll.need_suppress("bar.rs:2", 2);
        assert!(!suppress);
        assert!(msg.is_empty());
        let (suppress, msg) = ll.need_suppress("bar.rs:2", 2);
        assert!(!suppress);
        assert_eq!(msg, "suppressing log message with rate limit=2: ");
        let (suppress, msg) = ll.need_suppress("bar.rs:2", 2);
        assert!(suppress);
        assert!(msg.is_empty());

        // Other locations are unaffected.
        let (suppress, msg) = ll.need_suppress("baz.rs:3", 2);
        assert!(!suppress);
        assert!(msg.is_empty());

        // reset() clears the counters.
        ll.reset();
        let (suppress, msg) = ll.need_suppress("bar.rs:2", 2);
        assert!(!suppress);
        assert!(msg.is_empty());
    }

    #[test]
    fn test_compose_log_msg() {
        let fields = JsonFields::default();
        assert_eq!(
            compose_log_msg(
                "default",
                false,
                "2025-07-06T00:01:02.003Z",
                "info",
                "src/foo.rs:42",
                "hello",
                &fields
            ),
            "2025-07-06T00:01:02.003Z\tinfo\tsrc/foo.rs:42\thello\n"
        );
        assert_eq!(
            compose_log_msg(
                "default",
                true,
                "",
                "warn",
                "src/foo.rs:42",
                "hello",
                &fields
            ),
            "warn\tsrc/foo.rs:42\thello\n"
        );
        assert_eq!(
            compose_log_msg(
                "json",
                false,
                "2025-07-06T00:01:02.003Z",
                "info",
                "src/foo.rs:42",
                "hel\"lo",
                &fields
            ),
            "{\"ts\":\"2025-07-06T00:01:02.003Z\",\"level\":\"info\",\"caller\":\"src/foo.rs:42\",\"msg\":\"hel\\\"lo\"}\n"
        );
        assert_eq!(
            compose_log_msg("json", true, "", "error", "src/foo.rs:42", "hello", &fields),
            "{\"level\":\"error\",\"caller\":\"src/foo.rs:42\",\"msg\":\"hello\"}\n"
        );

        // Renamed fields, like -loggerJSONFields=ts:timestamp,msg:message
        let fields = parse_json_fields("ts:timestamp,msg:message");
        assert_eq!(
            compose_log_msg(
                "json",
                false,
                "2025-07-06T00:01:02.003Z",
                "info",
                "src/foo.rs:42",
                "hello",
                &fields
            ),
            "{\"timestamp\":\"2025-07-06T00:01:02.003Z\",\"level\":\"info\",\"caller\":\"src/foo.rs:42\",\"message\":\"hello\"}\n"
        );
    }

    #[test]
    fn test_parse_json_fields() {
        let f = parse_json_fields("");
        assert_eq!(
            (
                f.ts.as_str(),
                f.level.as_str(),
                f.caller.as_str(),
                f.msg.as_str()
            ),
            ("ts", "level", "caller", "msg")
        );
        let f = parse_json_fields("ts:timestamp, level:severity ,caller:source,msg:message");
        assert_eq!(
            (
                f.ts.as_str(),
                f.level.as_str(),
                f.caller.as_str(),
                f.msg.as_str()
            ),
            ("timestamp", "severity", "source", "message")
        );
    }

    #[test]
    #[should_panic(expected = "missing ':' delimiter")]
    fn test_parse_json_fields_missing_delimiter() {
        parse_json_fields("tstimestamp");
    }

    #[test]
    #[should_panic(expected = "unexpected json field name")]
    fn test_parse_json_fields_unknown_name() {
        parse_json_fields("foo:bar");
    }

    #[test]
    fn test_level_from_flag() {
        assert_eq!(Level::from_flag("INFO"), Level::Info);
        assert_eq!(Level::from_flag("WARN"), Level::Warn);
        assert_eq!(Level::from_flag("ERROR"), Level::Error);
        assert_eq!(Level::from_flag("FATAL"), Level::Fatal);
        assert_eq!(Level::from_flag("PANIC"), Level::Panic);
    }

    #[test]
    #[should_panic(expected = "unsupported `-loggerLevel` value")]
    fn test_level_from_flag_invalid() {
        Level::from_flag("info");
    }

    #[test]
    fn test_format_utc_timestamp() {
        assert_eq!(format_utc_timestamp(0), "1970-01-01T00:00:00");
        assert_eq!(format_utc_timestamp(1_751_760_000), "2025-07-06T00:00:00");
    }

    #[test]
    fn test_short_file() {
        assert_eq!(short_file("a/b/c/foo.rs"), "c/foo.rs");
        assert_eq!(short_file("b/foo.rs"), "b/foo.rs");
        assert_eq!(short_file("foo.rs"), "foo.rs");
    }

    #[test]
    fn test_count_log_message() {
        count_log_message(Level::Warn, "counter_test.rs:7", true);
        count_log_message(Level::Warn, "counter_test.rs:7", true);
        count_log_message(Level::Warn, "counter_test.rs:7", false);
        let names = crate::metrics::list_metric_names();
        let printed: Vec<_> = names
            .iter()
            .filter(|name| {
                name.contains("level=\"warn\"")
                    && name.contains("location=\"counter_test.rs:7\"")
                    && name.contains("is_printed=\"true\"")
            })
            .collect();
        assert_eq!(printed.len(), 1);
        assert_eq!(crate::metrics::get_or_create_counter(printed[0]).get(), 2);
        let suppressed: Vec<_> = names
            .iter()
            .filter(|name| {
                name.contains("location=\"counter_test.rs:7\"")
                    && name.contains("is_printed=\"false\"")
            })
            .collect();
        assert_eq!(suppressed.len(), 1);
        assert_eq!(
            crate::metrics::get_or_create_counter(suppressed[0]).get(),
            1
        );
    }

    #[test]
    fn test_output_capture() {
        let _guard = OUTPUT_TEST_MUTEX.lock().unwrap();
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        set_output_for_tests(buf.clone());

        crate::infof!("test message {}", 42);

        let contents = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        reset_output_for_test();

        let line = contents.lines().next().expect("no log line captured");
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 4, "unexpected log line: {line:?}");
        // Timestamp like 2025-07-06T00:01:02.003Z
        assert_eq!(parts[0].len(), 24, "unexpected timestamp: {:?}", parts[0]);
        assert!(parts[0].ends_with('Z'));
        assert_eq!(parts[1], "info");
        assert!(
            parts[2].starts_with("src/logger.rs:"),
            "unexpected location: {:?}",
            parts[2]
        );
        assert_eq!(parts[3], "test message 42");
    }

    #[test]
    fn test_short_file_windows_separators() {
        // file!() bakes backslashes when compiled natively on Windows.
        assert_eq!(
            short_file(r"crates\esl-common\src\logger.rs"),
            "src/logger.rs"
        );
        assert_eq!(
            short_file("crates/esl-common/src/logger.rs"),
            "src/logger.rs"
        );
        assert_eq!(short_file("logger.rs"), "logger.rs");
    }

    #[test]
    fn test_with_throttler() {
        let _guard = OUTPUT_TEST_MUTEX.lock().unwrap();
        let d = std::time::Duration::from_secs(3600);
        let lt = with_throttler("test-throttler", d);
        let lt2 = with_throttler("test-throttler", d);
        assert!(
            std::ptr::eq(lt, lt2),
            "expected the same throttler instance"
        );

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        set_output_for_tests(buf.clone());

        // First message is logged; the background thread then drains the
        // token and goes to sleep for `d`.
        lt.warnf(format_args!("first"));
        std::thread::sleep(std::time::Duration::from_millis(100));
        // Channel is drained: this one is logged too, and its token stays in
        // the channel while the thread sleeps.
        lt.warnf(format_args!("second"));
        // These are dropped.
        lt.warnf(format_args!("third"));
        lt.warnf(format_args!("fourth"));

        let contents = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        reset_output_for_test();

        assert!(
            contents.contains("first. (Similar 0 log messages were dropped due to throttling)"),
            "missing first message in {contents:?}"
        );
        assert!(
            contents.contains("second. (Similar 0 log messages were dropped due to throttling)"),
            "missing second message in {contents:?}"
        );
        assert!(!contents.contains("third"), "third message must be dropped");
        assert!(
            !contents.contains("fourth"),
            "fourth message must be dropped"
        );
        assert_eq!(lt.dropped.load(Ordering::SeqCst), 2);
    }
}
