//! Port of Softalink LLC `lib/buildinfo`.

use std::sync::OnceLock;

use crate::flagutil::Flag;

static SHOW_VERSION: Flag<bool> = Flag::new("version", "Show Softalink LLC version", || false);

/// PORT NOTE: Go sets `buildinfo.Version` via `-ldflags '-X'`; the Rust port
/// sets it once from `main` via [`set_version`].
static VERSION: OnceLock<String> = OnceLock::new();

/// Sets the build version string. Only the first call takes effect.
pub fn set_version(version: &str) {
    let _ = VERSION.set(version.to_string());
}

/// Returns the build version string ("" if unset, like Go's zero value).
pub fn version() -> &'static str {
    VERSION.get().map(String::as_str).unwrap_or("")
}

/// Returns a shortened version, like Go `buildinfo.ShortVersion`.
pub fn short_version() -> String {
    short_version_of(version())
}

fn short_version_of(version: &str) -> String {
    static SHORT_VERSION_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = SHORT_VERSION_RE
        .get_or_init(|| regex::Regex::new(r"v\d+\.\d+\.\d+(?:-enterprise)?(?:-cluster)?").unwrap());
    re.find(version)
        .map(|m| m.as_str().to_string())
        .unwrap_or_default()
}

/// Must be called after flag parsing. Prints the version and exits when the
/// `-version` flag is set.
///
/// Go's `buildinfo` also wraps `flag.Usage` to print the version first; the
/// port has no central usage hook, so each binary prepends `version()` in its
/// own `usage()` (invoked on `-h`/`-help` via `flagutil::help_requested`).
pub fn init() {
    init_with_default(env!("CARGO_PKG_VERSION"));
}

/// Like [`init`], but lets the binary brand its default version string.
///
/// PORT NOTE: Go injects the version via -ldflags at build time; the port
/// derives it from the crate version, which follows the pinned upstream
/// release (see UPSTREAM.lock).
pub fn init_with_default(pkg_version: &str) {
    if version().is_empty() {
        set_version(&format!("eslogs-v{pkg_version}"));
    }
    if *SHOW_VERSION.get() {
        print_version();
        std::process::exit(0);
    }
}

fn print_version() {
    // Go writes to flag.CommandLine.Output(), which defaults to stderr.
    eprintln!("{}", version());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_version_of() {
        fn f(version: &str, expected: &str) {
            assert_eq!(
                short_version_of(version),
                expected,
                "unexpected short version for {version:?}"
            );
        }
        f("", "");
        f("es-logs", "");
        f("es-logs-20250101-abcdef-v1.51.0", "v1.51.0");
        f("es-metrics-v1.99.12-enterprise", "v1.99.12-enterprise");
        f(
            "es-metrics-v1.99.12-enterprise-cluster",
            "v1.99.12-enterprise-cluster",
        );
        f("v10.20.30-cluster-something", "v10.20.30-cluster");
    }

    #[test]
    fn test_version_roundtrip() {
        // VERSION is process-global and can only be set once; exercise both
        // accessors in a single test.
        set_version("es-logs-v1.51.0");
        assert_eq!(version(), "es-logs-v1.51.0");
        assert_eq!(short_version(), "v1.51.0");
        // Subsequent set_version calls are ignored.
        set_version("other-v9.9.9");
        assert_eq!(version(), "es-logs-v1.51.0");
    }
}
