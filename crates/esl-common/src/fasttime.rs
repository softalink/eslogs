//! Port of Softalink LLC `lib/fasttime`.

use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::atomicutil::Uint64;

fn system_unix_timestamp() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

// PORT NOTE: Go starts the once-per-second updater goroutine in the package
// `init()`. Rust has no package init, so the timestamp cell and the updater
// thread are created lazily on the first call to `unix_timestamp()`.
// The synctest-only variant (`fasttime_synctest.go`) is not ported.
static CURRENT_TIMESTAMP: OnceLock<Uint64> = OnceLock::new();

fn current_timestamp() -> &'static Uint64 {
    CURRENT_TIMESTAMP.get_or_init(|| {
        thread::Builder::new()
            .name("fasttime".to_string())
            .spawn(|| {
                loop {
                    thread::sleep(Duration::from_secs(1));
                    if let Some(ts) = CURRENT_TIMESTAMP.get() {
                        ts.store(system_unix_timestamp());
                    }
                }
            })
            .expect("FATAL: cannot spawn fasttime updater thread");
        Uint64::new(system_unix_timestamp())
    })
}

/// UnixTimestamp returns the current unix timestamp in seconds.
///
/// It is faster than obtaining the timestamp from the OS on every call.
pub fn unix_timestamp() -> u64 {
    current_timestamp().load()
}

/// UnixDate returns date from the current unix timestamp.
///
/// The date is calculated by dividing unix timestamp by (24*3600)
pub fn unix_date() -> u64 {
    unix_timestamp() / (24 * 3600)
}

/// UnixHour returns hour from the current unix timestamp.
///
/// The hour is calculated by dividing unix timestamp by 3600
pub fn unix_hour() -> u64 {
    unix_timestamp() / 3600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_timestamp() {
        let ts_expected = system_unix_timestamp();
        let ts = unix_timestamp();
        assert!(
            ts.wrapping_sub(ts_expected) <= 1,
            "unexpected UnixTimestamp; got {ts}; want {ts_expected}"
        );
    }

    #[test]
    fn test_unix_date() {
        let date_expected = system_unix_timestamp() / (24 * 3600);
        let date = unix_date();
        assert!(
            date.wrapping_sub(date_expected) <= 1,
            "unexpected UnixDate; got {date}; want {date_expected}"
        );
    }

    #[test]
    fn test_unix_hour() {
        let hour_expected = system_unix_timestamp() / 3600;
        let hour = unix_hour();
        assert!(
            hour.wrapping_sub(hour_expected) <= 1,
            "unexpected UnixHour; got {hour}; want {hour_expected}"
        );
    }
}
