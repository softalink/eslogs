//! Port of `github.com/VictoriaMetrics/metrics/floatcounter.go`.

use std::sync::Mutex;

use super::{lock_ignore_poison, write_g};

/// FloatCounter is an f64 counter guarded by a mutex.
///
/// It may be used as a gauge if [`FloatCounter::add`] and
/// [`FloatCounter::sub`] are called.
#[derive(Default)]
pub struct FloatCounter {
    n: Mutex<f64>,
}

impl FloatCounter {
    /// Adds `n` to the counter.
    pub fn add(&self, n: f64) {
        *lock_ignore_poison(&self.n) += n;
    }

    /// Subtracts `n` from the counter.
    pub fn sub(&self, n: f64) {
        *lock_ignore_poison(&self.n) -= n;
    }

    /// Returns the current value of the counter.
    pub fn get(&self) -> f64 {
        *lock_ignore_poison(&self.n)
    }

    /// Sets the counter value to `n`.
    pub fn set(&self, n: f64) {
        *lock_ignore_poison(&self.n) = n;
    }

    /// Marshals the counter with the given prefix to `w`.
    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        let v = self.get();
        w.push_str(prefix);
        w.push(' ');
        write_g(w, v);
        w.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::counter::tests::test_get_or_create_counter;
    use crate::metrics::testutil::{test_concurrent, test_marshal_to};
    use crate::metrics::{MetricValue, get_or_create_float_counter, new_float_counter};
    use std::sync::Arc;

    // Port of floatcounter_test.go.
    #[test]
    fn test_float_counter_serial() {
        let name = "FloatCounterSerial";
        let c = new_float_counter(name);
        c.add(0.1);
        assert_eq!(c.get(), 0.1, "unexpected counter value");
        c.set(123.00001);
        assert_eq!(c.get(), 123.00001, "unexpected counter value");
        c.sub(0.00001);
        assert_eq!(c.get(), 123.0, "unexpected counter value");
        c.add(2.002);
        assert_eq!(c.get(), 125.002, "unexpected counter value");

        // Verify marshal_to.
        test_marshal_to(&MetricValue::FloatCounter(c), "foobar", "foobar 125.002\n");
    }

    #[test]
    fn test_float_counter_concurrent() {
        let name = "FloatCounterConcurrent";
        let c = new_float_counter(name);
        test_concurrent(|| {
            let n_prev = c.get();
            for _ in 0..10 {
                c.add(1.001);
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
    fn test_get_or_create_float_counter_serial() {
        // Go's TestGetOrCreateFloatCounterSerial calls testGetOrCreateCounter,
        // which registers a *Counter under this name; port that verbatim.
        let name = "GetOrCreateFloatCounterSerial";
        test_get_or_create_counter(name).unwrap();
    }

    #[test]
    fn test_get_or_create_float_counter_concurrent() {
        let name = "GetOrCreateFloatCounterConcurrent";
        test_concurrent(|| test_get_or_create_float_counter(name));
    }

    fn test_get_or_create_float_counter(name: &str) -> Result<(), String> {
        let c1 = get_or_create_float_counter(name);
        for _ in 0..10 {
            let c2 = get_or_create_float_counter(name);
            if !Arc::ptr_eq(&c1, &c2) {
                return Err("unexpected counter returned".to_string());
            }
        }
        Ok(())
    }
}
