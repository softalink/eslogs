//! Port of `github.com/VictoriaMetrics/metrics/counter.go`.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter is a counter.
///
/// It may be used as a gauge if [`Counter::dec`] and [`Counter::set`] are
/// called.
#[derive(Default)]
pub struct Counter {
    n: AtomicU64,
}

impl Counter {
    /// Increments the counter.
    #[inline]
    pub fn inc(&self) {
        self.n.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the counter.
    #[inline]
    pub fn dec(&self) {
        self.n.fetch_sub(1, Ordering::Relaxed);
    }

    /// Adds `n` to the counter.
    ///
    /// PORT NOTE: Go's `Add(n int)` accepts negative values via two's
    /// complement wrap-around; use [`Counter::add_i64`] for that.
    #[inline]
    pub fn add(&self, n: u64) {
        self.n.fetch_add(n, Ordering::Relaxed);
    }

    /// Adds `n` to the counter (Go `AddInt64`).
    #[inline]
    pub fn add_i64(&self, n: i64) {
        self.n.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Returns the current value of the counter.
    #[inline]
    pub fn get(&self) -> u64 {
        self.n.load(Ordering::Relaxed)
    }

    /// Sets the counter value to `n`.
    #[inline]
    pub fn set(&self, n: u64) {
        self.n.store(n, Ordering::Relaxed);
    }

    /// Marshals the counter with the given prefix to `w`.
    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        let v = self.get();
        let _ = writeln!(w, "{prefix} {v}");
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::metrics::testutil::{test_concurrent, test_marshal_to};
    use crate::metrics::{MetricValue, get_or_create_counter, new_counter};
    use std::sync::Arc;

    // Port of counter_test.go.
    #[test]
    fn test_counter_serial() {
        let name = "CounterSerial";
        let c = new_counter(name);
        c.inc();
        assert_eq!(c.get(), 1, "unexpected counter value");
        c.set(123);
        assert_eq!(c.get(), 123, "unexpected counter value");
        c.dec();
        assert_eq!(c.get(), 122, "unexpected counter value");
        c.add(3);
        assert_eq!(c.get(), 125, "unexpected counter value");

        // Verify marshal_to.
        test_marshal_to(&MetricValue::Counter(c), "foobar", "foobar 125\n");
    }

    #[test]
    fn test_counter_concurrent() {
        let name = "CounterConcurrent";
        let c = new_counter(name);
        test_concurrent(|| {
            let n_prev = c.get();
            for _ in 0..10 {
                c.inc();
                let n = c.get();
                if n <= n_prev {
                    return Err(format!(
                        "counter value must be greater than {n_prev}; got {n}"
                    ));
                }
            }
            Ok(())
        });
    }

    #[test]
    fn test_get_or_create_counter_serial() {
        let name = "GetOrCreateCounterSerial";
        test_get_or_create_counter(name).unwrap();
    }

    #[test]
    fn test_get_or_create_counter_concurrent() {
        let name = "GetOrCreateCounterConcurrent";
        test_concurrent(|| test_get_or_create_counter(name));
    }

    pub(crate) fn test_get_or_create_counter(name: &str) -> Result<(), String> {
        let c1 = get_or_create_counter(name);
        for _ in 0..10 {
            let c2 = get_or_create_counter(name);
            if !Arc::ptr_eq(&c1, &c2) {
                return Err("unexpected counter returned".to_string());
            }
        }
        Ok(())
    }
}
