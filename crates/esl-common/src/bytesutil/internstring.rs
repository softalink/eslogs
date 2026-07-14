//! Port of `lib/bytesutil/internstring.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::flagutil::Flag;

use super::unix_timestamp;

static INTERN_STRING_MAX_LEN: Flag<i64> = Flag::new(
    "internStringMaxLen",
    "The maximum length for strings to intern. A lower limit may save memory at the cost of higher CPU usage. \
     See https://en.wikipedia.org/wiki/String_interning . See also -internStringDisableCache and -internStringCacheExpireDuration",
    || 500,
);
static DISABLE_CACHE: Flag<bool> = Flag::new(
    "internStringDisableCache",
    "Whether to disable caches for interned strings. This may reduce memory usage at the cost of higher CPU usage. \
     See https://en.wikipedia.org/wiki/String_interning . See also -internStringCacheExpireDuration and -internStringMaxLen",
    || false,
);
// PORT NOTE: `flagutil` has no Duration flag type yet (ported in parallel);
// the flag holds the Go duration string and is parsed by the private
// `parse_go_duration` helper below.
static CACHE_EXPIRE_DURATION: Flag<String> = Flag::new(
    "internStringCacheExpireDuration",
    "The expiry duration for caches for interned strings. \
     See https://en.wikipedia.org/wiki/String_interning . See also -internStringMaxLen and -internStringDisableCache",
    || "6m".to_string(),
);

pub(super) fn cache_expire_duration() -> Duration {
    static D: OnceLock<Duration> = OnceLock::new();
    *D.get_or_init(|| {
        let s = CACHE_EXPIRE_DURATION.get();
        match parse_go_duration(s) {
            Ok(d) => d,
            Err(err) => {
                crate::panicf!(
                    "invalid value \"{s}\" for flag -internStringCacheExpireDuration: {err}"
                );
                unreachable!()
            }
        }
    })
}

// PORT NOTE: minimal port of Go's `time.ParseDuration` covering the forms the
// flag accepts ("6m", "300ms", "1h30m", ...). Negative durations are rejected
// since a negative cache expiry is meaningless here.
fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let orig = s;
    let mut s = s.strip_prefix('+').unwrap_or(s);
    if s.starts_with('-') {
        return Err(format!(
            "time: negative duration \"{orig}\" is not supported"
        ));
    }
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.is_empty() {
        return Err(format!("time: invalid duration \"{orig}\""));
    }
    let mut total = 0f64;
    while !s.is_empty() {
        let num_end = s
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(s.len());
        if num_end == 0 {
            return Err(format!("time: invalid duration \"{orig}\""));
        }
        let v: f64 = s[..num_end]
            .parse()
            .map_err(|_| format!("time: invalid duration \"{orig}\""))?;
        s = &s[num_end..];
        let (mult, rest) = if let Some(rest) = s.strip_prefix("ns") {
            (1e-9, rest)
        } else if let Some(rest) = s.strip_prefix("us").or_else(|| s.strip_prefix("µs")) {
            (1e-6, rest)
        } else if let Some(rest) = s.strip_prefix("ms") {
            (1e-3, rest)
        } else if let Some(rest) = s.strip_prefix('s') {
            (1.0, rest)
        } else if let Some(rest) = s.strip_prefix('m') {
            (60.0, rest)
        } else if let Some(rest) = s.strip_prefix('h') {
            (3600.0, rest)
        } else {
            return Err(format!("time: missing unit in duration \"{orig}\""));
        };
        total += v * mult;
        s = rest;
    }
    Ok(Duration::from_secs_f64(total))
}

// PORT NOTE: `lib/timeutil` is being ported in parallel; this private helper
// mirrors `timeutil.AddJitterToDuration` (adds up to 10% of jitter, capped at
// 10 seconds) using SystemTime nanos as the randomness source.
fn add_jitter_to_duration(d: Duration) -> Duration {
    let mut dv = d / 10;
    if dv > Duration::from_secs(10) {
        dv = Duration::from_secs(10);
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let p = f64::from(nanos % 1000) / 1000.0;
    d + Duration::from_secs_f64(p * dv.as_secs_f64())
}

/// PORT NOTE: the map is generic over `str`/`[u8]` so column names (raw
/// bytes, possibly invalid UTF-8) can be interned alongside plain strings;
/// Go has a single string map because Go strings are arbitrary bytes.
trait Internable: Eq + std::hash::Hash {
    fn byte_len(&self) -> usize;
    fn to_arc(&self) -> Arc<Self>;
}

impl Internable for str {
    fn byte_len(&self) -> usize {
        self.len()
    }
    fn to_arc(&self) -> Arc<str> {
        Arc::from(self)
    }
}

impl Internable for [u8] {
    fn byte_len(&self) -> usize {
        self.len()
    }
    fn to_arc(&self) -> Arc<[u8]> {
        Arc::from(self)
    }
}

struct InternStringMapEntry<T: ?Sized> {
    deadline: u64,
    s: Arc<T>,
}

impl<T: ?Sized> Clone for InternStringMapEntry<T> {
    fn clone(&self) -> Self {
        InternStringMapEntry {
            deadline: self.deadline,
            s: self.s.clone(),
        }
    }
}

struct MutableState<T: ?Sized + Internable> {
    m: HashMap<Arc<T>, Arc<T>>,
    reads: u64,
}

// PORT NOTE: Go stores the readonly map in an `atomic.Pointer` for lock-free
// reads; std Rust has no atomic Arc swap, so the port uses
// `RwLock<Arc<HashMap<..>>>` (readers only clone the Arc under the read lock).
struct InternStringMap<T: ?Sized + Internable> {
    mutable: Mutex<MutableState<T>>,
    readonly: RwLock<Arc<HashMap<Arc<T>, InternStringMapEntry<T>>>>,
}

impl<T: ?Sized + Internable> InternStringMap<T> {
    fn new() -> Self {
        InternStringMap {
            mutable: Mutex::new(MutableState {
                m: HashMap::new(),
                reads: 0,
            }),
            readonly: RwLock::new(Arc::new(HashMap::new())),
        }
    }

    fn get_readonly(&self) -> Arc<HashMap<Arc<T>, InternStringMapEntry<T>>> {
        self.readonly.read().unwrap().clone()
    }

    fn intern(&self, s: &T) -> Arc<T> {
        if is_skip_cache(s.byte_len()) {
            return s.to_arc();
        }

        let mut readonly = self.get_readonly();
        if let Some(e) = readonly.get(s) {
            // Fast path - the string has been found in readonly map.
            return e.s.clone();
        }

        // Slower path - search for the string in mutable map under the lock.
        let mut mutable = self.mutable.lock().unwrap();
        let s_interned = match mutable.m.get(s) {
            Some(v) => v.clone(),
            None => {
                // Verify whether s has been already registered by concurrent
                // threads in the readonly map.
                readonly = self.get_readonly();
                match readonly.get(s) {
                    Some(e) => e.s.clone(),
                    None => {
                        // Slowest path - register the string in mutable map.
                        // to_arc(s) makes a fresh copy, removing references
                        // to a possible bigger string s refers to.
                        let s_interned: Arc<T> = s.to_arc();
                        mutable.m.insert(s_interned.clone(), s_interned.clone());
                        s_interned
                    }
                }
            }
        };
        mutable.reads += 1;
        if mutable.reads > readonly.len() as u64 {
            self.migrate_mutable_to_readonly_locked(&mut mutable);
            mutable.reads = 0;
        }
        drop(mutable);

        s_interned
    }

    fn migrate_mutable_to_readonly_locked(&self, mutable: &mut MutableState<T>) {
        let readonly = self.get_readonly();
        let mut readonly_copy: HashMap<Arc<T>, InternStringMapEntry<T>> =
            HashMap::with_capacity(readonly.len() + mutable.m.len());
        for (k, e) in readonly.iter() {
            readonly_copy.insert(k.clone(), e.clone());
        }
        let deadline = unix_timestamp() + (cache_expire_duration().as_secs_f64() + 0.5) as u64;
        for s in mutable.m.values() {
            readonly_copy.insert(
                s.clone(),
                InternStringMapEntry {
                    s: s.clone(),
                    deadline,
                },
            );
        }
        mutable.m = HashMap::new();
        *self.readonly.write().unwrap() = Arc::new(readonly_copy);
    }

    fn cleanup(&self) {
        let readonly = self.get_readonly();
        let current_time = unix_timestamp();
        let need_cleanup = readonly.values().any(|e| e.deadline <= current_time);
        if !need_cleanup {
            return;
        }

        let readonly_copy: HashMap<Arc<T>, InternStringMapEntry<T>> = readonly
            .iter()
            .filter(|(_, e)| e.deadline > current_time)
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect();
        *self.readonly.write().unwrap() = Arc::new(readonly_copy);
    }
}

pub(super) fn is_skip_cache(len: usize) -> bool {
    *DISABLE_CACHE.get() || len as i64 > *INTERN_STRING_MAX_LEN.get()
}

/// Returns interned `b`.
///
/// PORT NOTE: Go interns column names as strings (arbitrary bytes); the
/// byte-native map keeps invalid UTF-8 intact.
pub fn intern_bytes(b: &[u8]) -> Arc<[u8]> {
    ibm().intern(b)
}

/// Returns interned `s`.
///
/// This may be needed for reducing the amounts of allocated memory.
///
/// PORT NOTE: Go returns the canonical `string` (shared header); the Rust
/// equivalent of a shared immutable string is `Arc<str>`.
pub fn intern_string(s: &str) -> Arc<str> {
    ism().intern(s)
}

fn ism() -> &'static InternStringMap<str> {
    static ISM: OnceLock<InternStringMap<str>> = OnceLock::new();
    let m = ISM.get_or_init(InternStringMap::new);
    start_cleaner();
    m
}

fn ibm() -> &'static InternStringMap<[u8]> {
    static IBM: OnceLock<InternStringMap<[u8]>> = OnceLock::new();
    let m = IBM.get_or_init(InternStringMap::new);
    start_cleaner();
    m
}

// PORT NOTE: Go starts the cleanup goroutine in newInternStringMap();
// the port spawns one named thread sweeping both maps on first use.
fn start_cleaner() {
    static CLEANER: Once = Once::new();
    CLEANER.call_once(|| {
        std::thread::Builder::new()
            .name("internstring-cleanup".to_string())
            .spawn(move || {
                loop {
                    let cleanup_interval = add_jitter_to_duration(cache_expire_duration()) / 2;
                    std::thread::sleep(cleanup_interval);
                    ism().cleanup();
                    ibm().cleanup();
                }
            })
            .expect("FATAL: cannot spawn internstring-cleanup thread");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn test_intern_string_serial() {
        if let Err(err) = test_intern_string_helper() {
            panic!("unexpected error: {err}");
        }
    }

    #[test]
    fn test_intern_string_concurrent() {
        let concurrency = 5;
        let (tx, rx) = mpsc::channel();
        for _ in 0..concurrency {
            let tx = tx.clone();
            std::thread::spawn(move || {
                tx.send(test_intern_string_helper()).unwrap();
            });
        }
        for _ in 0..concurrency {
            match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => {}
                Ok(Err(err)) => panic!("unexpected error: {err}"),
                Err(_) => panic!("timeout"),
            }
        }
    }

    fn test_intern_string_helper() -> Result<(), String> {
        for i in 0..1000 {
            let s = format!("foo_{i}");
            let s1 = intern_string(&s);
            if s != *s1 {
                return Err(format!(
                    "unexpected string returned from intern_string; got {s1:?}; want {s:?}"
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn test_parse_go_duration() {
        assert_eq!(parse_go_duration("6m"), Ok(Duration::from_secs(360)));
        assert_eq!(parse_go_duration("300ms"), Ok(Duration::from_millis(300)));
        assert_eq!(parse_go_duration("1h30m"), Ok(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_go_duration("0"), Ok(Duration::ZERO));
        assert!(parse_go_duration("").is_err());
        assert!(parse_go_duration("123").is_err());
        assert!(parse_go_duration("foo").is_err());
    }
}
