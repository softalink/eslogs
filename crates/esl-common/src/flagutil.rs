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
//! pairs, and each `Flag<T>` static lazily resolves its value from that map on
//! first access. Unknown-flag rejection is deferred to the app-layer port,
//! where the full flag set is known.
//!
//! PORT NOTE: repeated flags (`-foo=a -foo=b`) collapse to the last value in
//! the raw map, unlike Go where array-valued flags append. Use comma-separated
//! values (`-foo=a,b`) instead.

use std::collections::{HashMap, HashSet};
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

static RAW_FLAGS: OnceLock<HashMap<String, String>> = OnceLock::new();

/// Parses Go-style flags from `args` (without the program name):
/// `-name=value`, `--name=value`, `-name value`, and bare `-name` for
/// booleans. Parsing stops at `--` or the first non-flag argument, like Go.
pub fn parse_args(args: &[String]) {
    let mut map = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--" || !arg.starts_with('-') {
            break;
        }
        let name = arg.trim_start_matches('-');
        if let Some((k, v)) = name.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        } else if i + 1 < args.len() && !args[i + 1].starts_with('-') {
            map.insert(name.to_string(), args[i + 1].clone());
            i += 1;
        } else {
            // Bare flag: boolean `true`, like Go.
            map.insert(name.to_string(), "true".to_string());
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

pub(crate) fn raw(name: &str) -> Option<&'static str> {
    RAW_FLAGS
        .get()
        .and_then(|m| m.get(name))
        .map(|s| s.as_str())
}

/// Calls `f(name, raw_value)` for every explicitly set command-line flag, in
/// lexicographical order of flag names, like Go's `flag.Visit`.
///
/// PORT NOTE: values are the raw command-line strings, not the canonical
/// `flag.Value.String()` form used by Go.
pub fn visit_set_flags(mut f: impl FnMut(&str, &str)) {
    let Some(m) = RAW_FLAGS.get() else { return };
    let mut names: Vec<&String> = m.keys().collect();
    names.sort();
    for name in names {
        f(name, &m[name]);
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
    pub fn get(&self) -> &T {
        self.cell.get_or_init(|| match raw(self.name) {
            Some(s) => match T::parse_flag(s) {
                Ok(v) => v,
                Err(err) => {
                    crate::panicf!("invalid value \"{s}\" for flag -{}: {err}", self.name);
                    unreachable!()
                }
            },
            // Port of `lib/envflag`: when `-envflag.enable` is set via
            // `envflag::parse`, flags not set on the command line are read
            // from the corresponding environment variable.
            None => match crate::envflag::lookup_flag_env(self.name) {
                Some((value, env_name)) => match T::parse_flag(&value) {
                    Ok(v) => v,
                    Err(err) => {
                        crate::panicf!(
                            "cannot set flag {} to {value:?}, which is read from env var {env_name:?}: {err}",
                            self.name
                        );
                        unreachable!()
                    }
                },
                None => (self.default)(),
            },
        })
    }
}

/// Conversion from a raw flag string, mirroring Go's `strconv`/`flag` parsing.
pub trait FlagValue: Sized + 'static {
    fn parse_flag(s: &str) -> Result<Self, String>;
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
            "--",
            "-ignored=x",
        ]));
        assert_eq!(raw("a"), Some("1"));
        assert_eq!(raw("b"), Some("two"));
        assert_eq!(raw("boolFlag"), Some("true"));
        assert_eq!(raw("c"), Some("3"));
        assert_eq!(raw("ignored"), None);
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
