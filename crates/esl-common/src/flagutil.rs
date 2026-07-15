//! Go-style command-line flag handling.
//!
//! Covers the std `flag` package semantics that ported code relies on, plus
//! the Softalink LLC `lib/flagutil` extras (`ArrayString`/`ArrayBool`/
//! `ArrayInt`/`ArrayDuration`/`ArrayBytes`, `Bytes`, `DictInt`,
//! `RetentionDuration`/`ExtendedDuration`, `Password` and the secret-flag
//! registry) in the `array`, `bytes`, `dict`, `duration` and `password`
//! submodules.
//!
//! PORT NOTE: Go registers flags in package `init()` before `flag.Parse()`.
//! Rust has no pre-main init, so `parse_args` stores all raw `-name=value`
//! occurrences, and each `Flag<T>` static lazily resolves its value from that
//! map on first access. Unknown-flag rejection is deferred to the app-layer
//! port, where the full flag set is known.
//!
//! Repeated flags (`-foo=a -foo=b`) follow Go `flag.Parse` semantics: every
//! occurrence is applied in order via [`FlagValue::set_flag_occurrences`]
//! (Go calls `flag.Value.Set` once per occurrence), so scalar flags keep the
//! last value while array-valued flags append.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

pub mod array;
pub mod bytes;
pub mod dict;
pub mod duration;
pub mod password;

pub use array::{ArrayBool, ArrayBytes, ArrayDuration, ArrayInt, ArrayString};
pub use bytes::{Bytes, parse_bytes};
pub use dict::{DictInt, parse_json_map};
pub use duration::{ExtendedDuration, RetentionDuration};
pub use password::Password;

static RAW_FLAGS: OnceLock<HashMap<String, Vec<String>>> = OnceLock::new();

/// Parses Go-style flags from `args` (without the program name):
/// `-name=value`, `--name=value`, `-name value`, and bare `-name` for
/// booleans. Parsing stops at `--` or the first non-flag argument, like Go.
/// Every occurrence of a repeated flag is kept, in command-line order.
pub fn parse_args(args: &[String]) {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" || !arg.starts_with('-') {
            break;
        }
        let name = arg.trim_start_matches('-');
        if let Some((k, v)) = name.split_once('=') {
            map.entry(k.to_string()).or_default().push(v.to_string());
        } else if i + 1 < args.len() && !args[i + 1].starts_with('-') {
            map.entry(name.to_string())
                .or_default()
                .push(args[i + 1].clone());
            i += 1;
        } else {
            // Bare flag: boolean `true`, like Go.
            map.entry(name.to_string())
                .or_default()
                .push("true".to_string());
        }
        i += 1;
    }
    let _ = RAW_FLAGS.set(map);
}

/// Initializes flags from `std::env::args`. Called once from `main`.
pub fn parse() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    parse_args(&args);
}

/// Returns true if `-h` or `-help` (any number of leading dashes) was passed.
/// Go's `flag` package prints the usage banner and exits for these; the port
/// mirrors that from each binary's `main` (see the binaries' `usage`).
pub fn help_requested() -> bool {
    raw_occurrences("h").is_some() || raw_occurrences("help").is_some()
}

/// Returns the last raw command-line value of `name`, i.e. Go's observable
/// value for scalar flags.
pub(crate) fn raw(name: &str) -> Option<&'static str> {
    raw_occurrences(name).map(|v| v.last().unwrap().as_str())
}

/// Returns every raw command-line occurrence of `name`, in order. The
/// returned slice is never empty.
pub(crate) fn raw_occurrences(name: &str) -> Option<&'static [String]> {
    RAW_FLAGS
        .get()
        .and_then(|m| m.get(name))
        .map(|v| v.as_slice())
}

/// Calls `f(name, value)` for every explicitly set command-line flag, in
/// lexicographical order of flag names, like Go's `flag.Visit`.
///
/// Like Go, the value is the canonical `flag.Value.String()` form whenever
/// the flag has been resolved via [`Flag::get`].
///
/// PORT NOTE: for set-but-never-resolved flags Go still knows the canonical
/// form (flags register before `flag.Parse()`); the port falls back to the
/// raw command-line occurrences joined with `,` for those.
pub fn visit_set_flags(mut f: impl FnMut(&str, &str)) {
    let Some(m) = RAW_FLAGS.get() else { return };
    // Snapshot the canonical values before invoking `f`, so `f` may resolve
    // further flags (which locks the registry) without deadlocking.
    let registry = flag_registry().lock().unwrap().clone();
    let mut names: Vec<&String> = m.keys().collect();
    names.sort();
    for name in names {
        match registry.get(name.as_str()) {
            Some(v) => f(name, v),
            None => f(name, &m[name].join(",")),
        }
    }
}

/// Registry of resolved flags: name -> canonical value string
/// (`flag.Value.String()` in Go), populated by `Flag::get`.
///
/// PORT NOTE: Go's `flag` package knows every registered flag before
/// `flag.Parse()` because registration happens in package `init()`. Rust
/// statics are lazy and there is no life-before-main, so a `Flag` enters the
/// registry when it is first resolved via `Flag::get`. See
/// [`visit_all_flags`] for the resulting coverage.
static FLAG_REGISTRY: OnceLock<Mutex<BTreeMap<&'static str, String>>> = OnceLock::new();

fn flag_registry() -> &'static Mutex<BTreeMap<&'static str, String>> {
    FLAG_REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn register_flag_value(name: &'static str, value: String) {
    flag_registry().lock().unwrap().insert(name, value);
}

/// Re-export so the [`register_flag!`] macro can reach `linkme` from crates that
/// do not depend on it directly.
pub use ::linkme;

/// A compile-time registration of one declared flag, collected into
/// [`ALL_FLAGS`]. The function pointers resolve the flag lazily at
/// metrics-render time — matching Go's `flag.VisitAll`, which reports each
/// flag's current value (its default when unset).
pub struct FlagReg {
    pub name: fn() -> &'static str,
    pub value: fn() -> String,
    pub is_set: fn() -> bool,
}

/// Every declared flag, populated at link time by [`register_flag!`] across all
/// crates linked into the binary. Go registers flags in package `init()`;
/// linkme's link-section collection is the life-before-main-free equivalent, so
/// `esm_flag` can enumerate declared-but-never-resolved flags like
/// `flag.VisitAll`.
#[linkme::distributed_slice]
pub static ALL_FLAGS: [FlagReg];

/// Returns true if `name` was passed on the command line.
pub fn raw_is_set(name: &str) -> bool {
    raw_occurrences(name).is_some()
}

/// Registers a declared `Flag<T>` static into [`ALL_FLAGS`] so it appears in the
/// `esm_flag` gauge even when never resolved or set at runtime (Go's
/// `flag.VisitAll`). Place immediately after the `static` declaration.
#[macro_export]
macro_rules! register_flag {
    ($flag:path) => {
        const _: () = {
            #[$crate::flagutil::linkme::distributed_slice($crate::flagutil::ALL_FLAGS)]
            #[linkme(crate = $crate::flagutil::linkme)]
            static REG: $crate::flagutil::FlagReg = $crate::flagutil::FlagReg {
                name: || $flag.name(),
                value: || $flag.get().to_string(),
                is_set: || $crate::flagutil::raw_is_set($flag.name()),
            };
        };
    };
}

/// Calls `f(name, value, is_set)` for every known flag in lexicographical
/// order of flag names, like Go's `flag.VisitAll` (used by the `esm_flag`
/// gauges on the `/metrics` page).
///
/// PORT NOTE: Go visits every registered flag, because flags register in
/// package `init()` before `flag.Parse()`. Rust statics are lazy with no
/// life-before-main, so the port uses a `linkme` distributed slice
/// ([`ALL_FLAGS`], populated by [`register_flag!`]) as the equivalent
/// link-time registry: every declared flag reports its current value (its
/// default when unset), exactly like `flag.VisitAll`. The resolved-value
/// registry and raw command-line occurrences are unioned on top so that a
/// flag's canonical string (including any command-line override) wins over the
/// bare `.to_string()` of its default.
pub fn visit_all_flags(mut f: impl FnMut(&str, &str, bool)) {
    // Snapshot the union before invoking `f`, so `f` may resolve further
    // flags (which locks the registry) without deadlocking.
    let mut all: BTreeMap<String, (String, bool)> = BTreeMap::new();
    // (a) Every declared flag, via the link-time registry — this is what gives
    // parity with Go's flag.VisitAll for flags never read or set at runtime.
    for reg in ALL_FLAGS {
        let name = (reg.name)();
        all.insert(name.to_string(), ((reg.value)(), (reg.is_set)()));
    }
    // (b) Flags resolved via `Flag::get` carry the canonical value string
    // (identical to (a) for registered flags, but also covers any flag that
    // registers a value without a `register_flag!` site).
    for (name, value) in flag_registry().lock().unwrap().iter() {
        all.insert(name.to_string(), (value.clone(), raw(name).is_some()));
    }
    // (c) Flags set on the command line but never resolved.
    if let Some(m) = RAW_FLAGS.get() {
        for (name, occurrences) in m {
            all.entry(name.clone())
                .or_insert_with(|| (occurrences.join(","), true));
        }
    }
    for (name, (value, is_set)) in &all {
        f(name, value, *is_set);
    }
}

/// Writes all the explicitly set flags to `w`.
///
/// Port of Go `flagutil.WriteFlags`.
pub fn write_flags(w: &mut dyn std::io::Write) {
    visit_set_flags(|name, value| {
        let lname = name.to_lowercase();
        let value = if is_secret_flag(&lname) {
            "secret"
        } else {
            value
        };
        let _ = writeln!(w, "-{}={}", name, go_quote(value));
    });
}

static SECRET_FLAGS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn secret_flags() -> &'static Mutex<HashSet<String>> {
    SECRET_FLAGS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Registers `flag_name` as secret.
///
/// This function must be called before starting logging.
/// Secret flags aren't exported at `/metrics` page.
pub fn register_secret_flag(flag_name: &str) {
    let lname = flag_name.to_lowercase();
    secret_flags().lock().unwrap().insert(lname);
}

/// Unregisters all secret flags.
///
/// This function must be used in tests only.
pub fn unregister_all_secret_flags() {
    secret_flags().lock().unwrap().clear();
}

/// Returns true if `s` contains a flag name with secret value, which shouldn't
/// be exposed.
pub fn is_secret_flag(s: &str) -> bool {
    if s.contains("pass") || s.contains("key") || s.contains("secret") || s.contains("token") {
        return true;
    }
    secret_flags().lock().unwrap().contains(s)
}

/// Quotes `s` like Go's `%q` / `strconv.Quote`.
///
/// PORT NOTE: printable non-ASCII characters are kept as-is instead of being
/// `\u`-escaped like Go does for non-printable runes; the logger and flag
/// output only rely on ASCII escaping behavior.
pub(crate) fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\x7f' => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A typed command-line flag, resolved lazily on first access.
///
/// Declare as a static:
/// `static HTTP_ADDR: Flag<String> = Flag::new("httpListenAddr", "...", || ":9428".to_string());`
pub struct Flag<T: FlagValue> {
    name: &'static str,
    pub usage: &'static str,
    default: fn() -> T,
    cell: OnceLock<T>,
}

impl<T: FlagValue> Flag<T> {
    pub const fn new(name: &'static str, usage: &'static str, default: fn() -> T) -> Self {
        Flag {
            name,
            usage,
            default,
            cell: OnceLock::new(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the parsed flag value (or the default when unset). Panics with
    /// the same wording as Go's flag package on an unparsable value.
    ///
    /// Every command-line occurrence of the flag is applied in order, like
    /// Go's `flag.Parse` calling `flag.Value.Set` once per occurrence:
    /// scalar flags keep the last value, array-valued flags append.
    ///
    /// The resolved value also enters the global flag registry used by
    /// [`visit_all_flags`] (Go registers flags in package `init()` instead;
    /// see the registry PORT NOTE).
    pub fn get(&self) -> &T {
        self.cell.get_or_init(|| {
            let v = match raw_occurrences(self.name) {
                Some(occurrences) => {
                    let mut v = (self.default)();
                    if let Err(e) = v.set_flag_occurrences(occurrences) {
                        crate::panicf!(
                            "invalid value \"{}\" for flag -{}: {}",
                            e.value,
                            self.name,
                            e.err
                        );
                        unreachable!()
                    }
                    v
                }
                // Port of `lib/envflag`: when `-envflag.enable` is set via
                // `envflag::parse`, flags not set on the command line are read
                // from the corresponding environment variable.
                None => match crate::envflag::lookup_flag_env(self.name) {
                    Some((value, env_name)) => {
                        let mut v = (self.default)();
                        if let Err(e) = v.set_flag_occurrences(std::slice::from_ref(&value)) {
                            crate::panicf!(
                                "cannot set flag {} to {value:?}, which is read from env var {env_name:?}: {}",
                                self.name,
                                e.err
                            );
                            unreachable!()
                        }
                        v
                    }
                    None => (self.default)(),
                },
            };
            register_flag_value(self.name, v.to_string());
            v
        })
    }
}

/// Error from applying a raw command-line value to a flag: the offending raw
/// `value` and the parse error, so [`Flag::get`] can panic with Go's
/// `invalid value "..." for flag -...` wording.
#[derive(Debug)]
pub struct FlagParseError {
    pub value: String,
    pub err: String,
}

/// Conversion from a raw flag string, mirroring Go's `strconv`/`flag` parsing.
///
/// The `Display` bound mirrors Go's `flag.Value.String()`: it produces the
/// canonical value string used by the flag registry ([`visit_all_flags`]).
pub trait FlagValue: Sized + std::fmt::Display + 'static {
    fn parse_flag(s: &str) -> Result<Self, String>;

    /// Applies every command-line occurrence of the flag to `self`, like Go's
    /// `flag.Parse` calling `flag.Value.Set` once per occurrence.
    ///
    /// The default implementation mirrors Go's scalar flags: each occurrence
    /// is parsed (so an invalid earlier occurrence still errors, like Go) and
    /// overwrites the previous one, keeping the last value. Array-valued
    /// flags override this to append (see `flagutil::array`).
    fn set_flag_occurrences(&mut self, occurrences: &[String]) -> Result<(), FlagParseError> {
        for s in occurrences {
            match Self::parse_flag(s) {
                Ok(v) => *self = v,
                Err(err) => {
                    return Err(FlagParseError {
                        value: s.clone(),
                        err,
                    });
                }
            }
        }
        Ok(())
    }
}

impl FlagValue for bool {
    fn parse_flag(s: &str) -> Result<Self, String> {
        match s {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
            _ => Err("parse error".to_string()),
        }
    }
}

impl FlagValue for String {
    fn parse_flag(s: &str) -> Result<Self, String> {
        Ok(s.to_string())
    }
}

macro_rules! impl_flagvalue_num {
    ($($t:ty),*) => {$(
        impl FlagValue for $t {
            fn parse_flag(s: &str) -> Result<Self, String> {
                s.parse::<$t>().map_err(|_| "parse error".to_string())
            }
        }
    )*};
}
impl_flagvalue_num!(i32, i64, u32, u64, usize, f64);

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_parse_args_forms() {
        // RAW_FLAGS is process-global; exercise every form in one parse.
        parse_args(&args(&[
            "-a=1",
            "--b=two",
            "-boolFlag",
            "-c",
            "3",
            "-repeatedScalar=x",
            "-repeatedScalar=y",
            "-repeatedArray=a,b",
            "-repeatedArray=c",
            "--",
            "-ignored=x",
        ]));
        assert_eq!(raw("a"), Some("1"));
        assert_eq!(raw("b"), Some("two"));
        assert_eq!(raw("boolFlag"), Some("true"));
        assert_eq!(raw("c"), Some("3"));
        assert_eq!(raw("ignored"), None);

        // Repeated flags: every occurrence is kept in order; `raw` (Go's
        // scalar view) returns the last one.
        assert_eq!(raw("repeatedScalar"), Some("y"));
        assert_eq!(
            raw_occurrences("repeatedScalar"),
            Some(&["x".to_string(), "y".to_string()][..])
        );

        // Scalar flags keep the last occurrence, like Go.
        static REPEATED_SCALAR: Flag<String> =
            Flag::new("repeatedScalar", "", || "unset".to_string());
        assert_eq!(REPEATED_SCALAR.get(), "y");

        // Array-valued flags append across occurrences, like Go.
        static REPEATED_ARRAY: Flag<crate::flagutil::ArrayString> =
            Flag::new("repeatedArray", "", crate::flagutil::ArrayString::default);
        assert_eq!(
            REPEATED_ARRAY.get().0,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );

        // visit_all_flags covers explicitly set flags even when they were
        // never resolved via `Flag::get` (raw values, is_set=true).
        let mut seen = None;
        visit_all_flags(|name, value, is_set| {
            if name == "b" {
                seen = Some((value.to_string(), is_set));
            }
        });
        assert_eq!(seen, Some(("two".to_string(), true)));

        // visit_set_flags reports the canonical (resolved) value like Go's
        // flag.Visit, and joins raw occurrences for unresolved flags.
        let mut set_flags = std::collections::HashMap::new();
        visit_set_flags(|name, value| {
            set_flags.insert(name.to_string(), value.to_string());
        });
        assert_eq!(
            set_flags.get("repeatedArray").map(String::as_str),
            Some("a,b,c")
        );
        assert_eq!(
            set_flags.get("repeatedScalar").map(String::as_str),
            Some("y")
        );
    }

    #[test]
    fn test_visit_all_flags_registered_default() {
        static REG_DEFAULT: Flag<i64> = Flag::new("visitAllRegisteredFlag", "", || 7);
        assert_eq!(*REG_DEFAULT.get(), 7);
        // A resolved flag shows up with its canonical value and is_set=false
        // (it was not passed on the command line).
        let mut seen = None;
        visit_all_flags(|name, value, is_set| {
            if name == "visitAllRegisteredFlag" {
                seen = Some((value.to_string(), is_set));
            }
        });
        assert_eq!(seen, Some(("7".to_string(), false)));
    }

    // A flag registered via `register_flag!` but NEVER resolved via `.get()`
    // must still appear in `visit_all_flags` (its default value), unlike a bare
    // `Flag` static — this is the link-time registry parity with Go's
    // `flag.VisitAll`.
    static UNRESOLVED_REG: Flag<i64> = Flag::new("visitAllUnresolvedRegistered", "", || 99);
    crate::register_flag!(UNRESOLVED_REG);

    #[test]
    fn test_visit_all_flags_unresolved_registered() {
        let mut seen = None;
        visit_all_flags(|name, value, is_set| {
            if name == "visitAllUnresolvedRegistered" {
                seen = Some((value.to_string(), is_set));
            }
        });
        assert_eq!(seen, Some(("99".to_string(), false)));
    }

    #[test]
    fn test_flag_default_and_bool_parse() {
        static MISSING: Flag<i64> = Flag::new("noSuchFlag", "", || 42);
        assert_eq!(*MISSING.get(), 42);
        assert_eq!(bool::parse_flag("t"), Ok(true));
        assert_eq!(bool::parse_flag("False"), Ok(false));
        assert!(bool::parse_flag("yes").is_err());
    }
}
