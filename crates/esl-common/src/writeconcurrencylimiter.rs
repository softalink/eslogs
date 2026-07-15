//! Port of Softalink LLC `lib/writeconcurrencylimiter`.
//!
//! Limits the number of concurrent insert requests via the
//! `-maxConcurrentInserts` flag; callers exceeding the limit queue for up to
//! `-insert.maxQueueDuration` before receiving a 503 error.
//!
//! PORT NOTE: Go implements the limiter with a buffered channel
//! (`concurrencyLimitCh`); the port uses a `Mutex<usize>` + `Condvar`
//! semaphore with identical semantics (fast non-blocking attempt, then a
//! timed wait).
//!
//! PORT NOTE: [`ErrorWithStatusCode`] is homed here until the Rust
//! `httpserver` module gains it (Go: `lib/httpserver.ErrorWithStatusCode`).
//! It carries the message plus the HTTP status code the handler must write.

use std::io::Read;
use std::sync::{Condvar, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::flagutil::{Flag, FlagValue};
use crate::metrics::Counter;
use crate::{cgroup, timeutil};

/// Port of Go `httpserver.ErrorWithStatusCode`: an error message carrying the
/// HTTP status code to respond with.
#[derive(Debug)]
pub struct ErrorWithStatusCode {
    pub err: String,
    pub status_code: u16,
}

static MAX_CONCURRENT_INSERTS: Flag<i64> = Flag::new(
    "maxConcurrentInserts",
    "The maximum number of concurrent insert requests. \
     Set higher value when clients send data over slow networks. \
     Default value depends on the number of available CPU cores. It should work fine in most cases since it minimizes resource usage. \
     See also -insert.maxQueueDuration",
    || 2 * cgroup::available_cpus() as i64,
);
crate::register_flag!(MAX_CONCURRENT_INSERTS);

static MAX_QUEUE_DURATION: Flag<DurationFlag> = Flag::new(
    "insert.maxQueueDuration",
    "The maximum duration to wait in the queue when -maxConcurrentInserts \
     concurrent insert requests are executed",
    || DurationFlag {
        nanos: 60_000_000_000,
    },
);
crate::register_flag!(MAX_QUEUE_DURATION);

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

// `FlagValue` requires the canonical value string for the flag registry
// (Go `flag.Value.String()`, i.e. `time.Duration.String()` here).
impl std::fmt::Display for DurationFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&timeutil::format_go_duration(self.nanos))
    }
}

/// Reader decreases the concurrency before every `read()` call and increases
/// the concurrency after the `read()` call, so slow network reads don't hold
/// concurrency tokens.
///
/// It must be obtained via [`get_reader`]; the token is released when the
/// Reader is dropped (Go `PutReader`).
///
/// PORT NOTE: Go pools Reader structs via `sync.Pool`; the port allocates the
/// (two-word) wrapper on the stack instead.
pub struct Reader<'a> {
    r: &'a mut dyn Read,
    increased_concurrency: bool,
}

/// Returns the [`Reader`] for r, obtaining a concurrency token
/// (Go `GetReader`; dropping the Reader is Go `PutReader`).
pub fn get_reader(r: &mut dyn Read) -> Result<Reader<'_>, ErrorWithStatusCode> {
    inc_concurrency()?;
    Ok(Reader {
        r,
        increased_concurrency: true,
    })
}

impl Read for Reader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        dec_concurrency();
        self.increased_concurrency = false;

        let res = self.r.read(buf);

        if let Err(err_c) = inc_concurrency() {
            // PORT NOTE: Go returns `(n, errC)`; io::Read cannot return both
            // bytes and an error, so the read bytes are dropped on this
            // (timeout-after-a-minute) path.
            return Err(std::io::Error::other(err_c.err));
        }
        self.increased_concurrency = true;

        match res {
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/8704
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "{err}: while reading the request body. This might be caused by a timeout on the client side. \
                         Possible solutions: to lower -insert.maxQueueDuration below the client\u{2019}s timeout; to increase the client-side timeout; \
                         to increase compute resources at the server; to increase -maxConcurrentInserts"
                    ),
                ))
            }
            other => other,
        }
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        if self.increased_concurrency {
            dec_concurrency();
            self.increased_concurrency = false;
        }
    }
}

/// RAII guard for a concurrency token; dropping it returns the token
/// (stands in for Go's `defer writeconcurrencylimiter.DecConcurrency()`).
pub struct ConcurrencyGuard(());

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        dec_concurrency();
    }
}

/// Obtains a concurrency token from `-maxConcurrentInserts` and returns a
/// guard which releases it on drop (Go `IncConcurrency` + deferred
/// `DecConcurrency`).
pub fn inc_concurrency_guard() -> Result<ConcurrencyGuard, ErrorWithStatusCode> {
    inc_concurrency()?;
    Ok(ConcurrencyGuard(()))
}

struct Semaphore {
    current: Mutex<usize>,
    cv: Condvar,
}

static SEMAPHORE: Semaphore = Semaphore {
    current: Mutex::new(0),
    cv: Condvar::new(),
};

// Go initializes concurrencyLimitCh once on first use
// (concurrencyLimitChOnce); the flag values are likewise latched here.
static CONCURRENCY_LIMIT: OnceLock<usize> = OnceLock::new();

fn concurrency_limit() -> usize {
    *CONCURRENCY_LIMIT.get_or_init(|| (*MAX_CONCURRENT_INSERTS.get()).max(0) as usize)
}

/// Obtains a concurrency token from -maxConcurrentInserts
/// (Go `IncConcurrency`).
///
/// The obtained token must be returned back via [`dec_concurrency`].
pub fn inc_concurrency() -> Result<(), ErrorWithStatusCode> {
    let limit = concurrency_limit();
    let mut current = SEMAPHORE.current.lock().unwrap();
    if *current < limit {
        *current += 1;
        return Ok(());
    }

    CONCURRENCY_LIMIT_REACHED.inc();
    let max_queue_duration = Duration::from_nanos(MAX_QUEUE_DURATION.get().nanos.max(0) as u64);
    let deadline = Instant::now() + max_queue_duration;
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let (guard, _timeout) = SEMAPHORE.cv.wait_timeout(current, deadline - now).unwrap();
        current = guard;
        if *current < limit {
            *current += 1;
            return Ok(());
        }
    }

    CONCURRENCY_LIMIT_TIMEOUT.inc();
    Err(ErrorWithStatusCode {
        err: format!(
            "cannot process insert request for {:.3} seconds because {limit} concurrent insert requests are executed. \
             Possible solutions: to reduce workload; to increase compute resources at the server; \
             to increase -insert.maxQueueDuration; to increase -maxConcurrentInserts",
            max_queue_duration.as_secs_f64()
        ),
        status_code: 503,
    })
}

/// Returns the token obtained via [`inc_concurrency`], so other threads could
/// obtain it (Go `DecConcurrency`).
pub fn dec_concurrency() {
    let mut current = SEMAPHORE.current.lock().unwrap();
    *current = current.saturating_sub(1);
    drop(current);
    SEMAPHORE.cv.notify_one();
}

static CONCURRENCY_LIMIT_REACHED: LazyLock<std::sync::Arc<Counter>> =
    LazyLock::new(|| crate::metrics::new_counter("esm_concurrent_insert_limit_reached_total"));
static CONCURRENCY_LIMIT_TIMEOUT: LazyLock<std::sync::Arc<Counter>> =
    LazyLock::new(|| crate::metrics::new_counter("esm_concurrent_insert_limit_timeout_total"));

/// Registers the `esm_concurrent_insert_capacity` / `esm_concurrent_insert_current`
/// gauges (Go registers them at package init; the Rust metrics registry needs
/// an explicit call, done by [`crate::appmetrics`]-style init in the apps).
pub fn init_metrics() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        crate::metrics::new_gauge(
            "esm_concurrent_insert_capacity",
            Some(Box::new(|| concurrency_limit() as f64)),
        );
        crate::metrics::new_gauge(
            "esm_concurrent_insert_current",
            Some(Box::new(|| *SEMAPHORE.current.lock().unwrap() as f64)),
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT-only tests: Go's lib/writeconcurrencylimiter ships without a test
    // file; these pin the semaphore and Reader token accounting. The flag
    // defaults cannot be overridden in tests (flags latch from process args),
    // so the timeout path is not exercised here.

    /// The tests share the global semaphore, so they must not run
    /// concurrently with each other.
    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn test_inc_dec_concurrency_up_to_limit() {
        let _l = test_lock();
        let limit = concurrency_limit();
        assert!(limit > 0, "limit must be positive");

        for _ in 0..limit {
            inc_concurrency().unwrap();
        }
        assert_eq!(*SEMAPHORE.current.lock().unwrap(), limit);
        for _ in 0..limit {
            dec_concurrency();
        }
        assert_eq!(*SEMAPHORE.current.lock().unwrap(), 0);
    }

    #[test]
    fn test_reader_releases_token_during_read_and_on_drop() {
        let _l = test_lock();
        let data = b"hello".to_vec();
        let mut src: &[u8] = &data;
        {
            let mut r = get_reader(&mut src).unwrap();
            assert_eq!(*SEMAPHORE.current.lock().unwrap(), 1);
            let mut buf = [0u8; 16];
            let n = r.read(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"hello");
            // The token is re-acquired after the read.
            assert_eq!(*SEMAPHORE.current.lock().unwrap(), 1);
        }
        // Dropping the Reader releases the token (Go PutReader).
        assert_eq!(*SEMAPHORE.current.lock().unwrap(), 0);
    }

    #[test]
    fn test_concurrency_guard() {
        let _l = test_lock();
        {
            let _g = inc_concurrency_guard().unwrap();
            assert_eq!(*SEMAPHORE.current.lock().unwrap(), 1);
        }
        assert_eq!(*SEMAPHORE.current.lock().unwrap(), 0);
    }
}
