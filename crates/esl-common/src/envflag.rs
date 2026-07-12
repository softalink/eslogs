//! Port of Softalink LLC `lib/envflag`.
//!
//! When `-envflag.enable` is set, flags that aren't set via the command line
//! are read from the corresponding environment variables (with dots in flag
//! names replaced by underscores, optionally prefixed with `-envflag.prefix`).
//!
//! PORT NOTE: Go reads env values through `lib/envtemplate`, which expands
//! `%{ENV_VAR}` placeholders recursively over the whole environment map at
//! process start. This port expands placeholders on demand with the same
//! semantics (unknown placeholders are left as-is).
//!
//! PORT NOTE: Go's `envflag.Parse` fatals on unprocessed positional args and
//! unknown flags via `flag.FlagSet.Parse`. Like `flagutil::parse_args`,
//! unknown-flag rejection is deferred to the app-layer port, where the full
//! flag set is known.

use std::sync::OnceLock;

use crate::flagutil::{self, ArrayString, FlagValue};

struct State {
    enable: bool,
    prefix: String,
}

static STATE: OnceLock<State> = OnceLock::new();

/// Parses environment vars and command-line flags.
///
/// Flags set via the command line override flags set via environment vars.
///
/// This function must be called instead of `flagutil::parse()` before using
/// any flags in the program.
pub fn parse() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    parse_args(&args);
}

/// Parses the given `args` (without the program name), like Go
/// `envflag.ParseFlagSet`.
pub fn parse_args(args: &[String]) {
    let args = expand_args(args);
    flagutil::parse_args(&args);

    let enable = match flagutil::raw("envflag.enable") {
        Some(s) => match bool::parse_flag(s) {
            Ok(v) => v,
            Err(err) => {
                // Do not use the logger here, since it is uninitialized yet.
                panic!("cannot parse -envflag.enable={s:?}: {err}");
            }
        },
        None => false,
    };
    let prefix = flagutil::raw("envflag.prefix").unwrap_or("").to_string();
    let _ = STATE.set(State { enable, prefix });

    apply_secret_flags();
}

/// Returns `(value, env_var_name)` for the given flag name when
/// `-envflag.enable` is set and the corresponding env var exists.
///
/// Used by `flagutil::Flag::get` for flags not set on the command line.
pub(crate) fn lookup_flag_env(name: &str) -> Option<(String, String)> {
    let st = STATE.get()?;
    if !st.enable {
        return None;
    }
    let fname = get_env_flag_name(&st.prefix, name);
    let v = std::env::var(&fname).ok()?;
    Some((expand_string(&v), fname))
}

/// Substitutes `%{ENV_VAR}` placeholders inside `args` with the corresponding
/// environment variable values. Empty results are dropped, like in Go.
fn expand_args(args: &[String]) -> Vec<String> {
    let mut dst = Vec::with_capacity(args.len());
    for arg in args {
        let s = expand_string(arg);
        if !s.is_empty() {
            dst.push(s);
        }
    }
    dst
}

fn expand_string(s: &str) -> String {
    expand_with(s, &|name| std::env::var(name).ok(), 0)
}

fn expand_with(s: &str, lookup: &dyn Fn(&str) -> Option<String>, depth: usize) -> String {
    if depth > 100 {
        // Guard against recursive %{...} definitions.
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("%{") {
        out.push_str(&rest[..i]);
        match rest[i + 2..].find('}') {
            None => {
                out.push_str(&rest[i..]);
                return out;
            }
            Some(j) => {
                let tag = &rest[i + 2..i + 2 + j];
                match lookup(tag) {
                    Some(v) => out.push_str(&expand_with(&v, lookup, depth + 1)),
                    None => {
                        // Cannot find the tag. Leave it as is.
                        out.push_str("%{");
                        out.push_str(tag);
                        out.push('}');
                    }
                }
                rest = &rest[i + 2 + j + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn get_env_flag_name(prefix: &str, s: &str) -> String {
    // Substitute dots with underscores, since env var names cannot contain
    // dots.
    format!("{}{}", prefix, s.replace('.', "_"))
}

/// Registers flags from `-secret.flags` after they are parsed.
///
/// Port of `lib/envflag/secret.go`: comma-separated list of flag names with
/// secret values, which are hidden in logs and on the /metrics page.
fn apply_secret_flags() {
    let mut secret_flags_list = ArrayString::default();
    if let Some(v) = flagutil::raw("secret.flags") {
        // ArrayString::set never fails.
        let _ = secret_flags_list.set(v);
    }
    apply_secret_flags_list(&secret_flags_list);
}

fn apply_secret_flags_list(secret_flags_list: &ArrayString) {
    for f in secret_flags_list.iter() {
        flagutil::register_secret_flag(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_secret_flags() {
        let mut secret_flags_list = ArrayString::default();
        secret_flags_list
            .set("foo,bar")
            .unwrap_or_else(|err| panic!("unexpected error: {err}"));

        assert!(
            !flagutil::is_secret_flag("foo") && !flagutil::is_secret_flag("bar"),
            "foo and bar shouldn't be secret before apply_secret_flags"
        );

        apply_secret_flags_list(&secret_flags_list);

        assert!(
            flagutil::is_secret_flag("foo") && flagutil::is_secret_flag("bar"),
            "foo and bar should be secret after apply_secret_flags"
        );

        flagutil::unregister_all_secret_flags();
    }

    #[test]
    fn test_get_env_flag_name() {
        assert_eq!(get_env_flag_name("", "httpListenAddr"), "httpListenAddr");
        assert_eq!(get_env_flag_name("", "envflag.enable"), "envflag_enable");
        assert_eq!(get_env_flag_name("VM_", "foo.bar.baz"), "VM_foo_bar_baz");
    }

    #[test]
    fn test_expand_with() {
        let lookup = |name: &str| -> Option<String> {
            match name {
                "FOO" => Some("foo-value".to_string()),
                "NESTED" => Some("x%{FOO}y".to_string()),
                "SELF" => Some("%{SELF}".to_string()),
                _ => None,
            }
        };
        let f = |s: &str| expand_with(s, &lookup, 0);
        assert_eq!(f(""), "");
        assert_eq!(f("plain"), "plain");
        assert_eq!(f("%{FOO}"), "foo-value");
        assert_eq!(f("a=%{FOO},b"), "a=foo-value,b");
        assert_eq!(f("%{MISSING}"), "%{MISSING}");
        assert_eq!(f("%{unclosed"), "%{unclosed");
        assert_eq!(f("%{NESTED}"), "xfoo-valuey");
        // Recursive definitions terminate.
        assert_eq!(f("%{SELF}"), "%{SELF}");
    }
}
