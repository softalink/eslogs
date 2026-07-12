//! Port of `github.com/VictoriaMetrics/metrics/gauge.go`.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{GaugeFn, write_g};

/// Gauge is an f64 gauge.
///
/// It either wraps a callback returning the gauge value or stores the value
/// set via [`Gauge::set`] / [`Gauge::inc`] / [`Gauge::dec`] / [`Gauge::add`].
#[derive(Default)]
pub struct Gauge {
    /// The u64 representation of the f64 passed to [`Gauge::set`].
    value_bits: AtomicU64,

    /// An optional callback, which is called for returning the gauge value.
    pub(crate) f: Option<GaugeFn>,
}

impl Gauge {
    pub(crate) fn with_callback(f: Option<GaugeFn>) -> Gauge {
        Gauge {
            value_bits: AtomicU64::new(0),
            f,
        }
    }

    /// Returns the current value of the gauge.
    pub fn get(&self) -> f64 {
        if let Some(f) = &self.f {
            return f();
        }
        let n = self.value_bits.load(Ordering::Relaxed);
        f64::from_bits(n)
    }

    /// Sets the gauge value to `v`.
    ///
    /// The gauge must be created with a `None` callback in order to be able
    /// to call this function.
    pub fn set(&self, v: f64) {
        if self.f.is_some() {
            panic!("cannot call set on gauge created with non-nil callback");
        }
        self.value_bits.store(v.to_bits(), Ordering::Relaxed);
    }

    /// Increments the gauge by 1.
    ///
    /// The gauge must be created with a `None` callback in order to be able
    /// to call this function.
    pub fn inc(&self) {
        self.add(1.0);
    }

    /// Decrements the gauge by 1.
    ///
    /// The gauge must be created with a `None` callback in order to be able
    /// to call this function.
    pub fn dec(&self) {
        self.add(-1.0);
    }

    /// Adds `f_add` to the gauge. `f_add` may be positive or negative.
    ///
    /// The gauge must be created with a `None` callback in order to be able
    /// to call this function.
    pub fn add(&self, f_add: f64) {
        if self.f.is_some() {
            panic!("cannot call set on gauge created with non-nil callback");
        }
        loop {
            let n = self.value_bits.load(Ordering::Relaxed);
            let f = f64::from_bits(n);
            let n_new = (f + f_add).to_bits();
            if self
                .value_bits
                .compare_exchange(n, n_new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        let v = self.get();
        if v as i64 as f64 == v {
            // Marshal integer values without scientific notation.
            let _ = writeln!(w, "{prefix} {}", v as i64);
        } else {
            w.push_str(prefix);
            w.push(' ');
            write_g(w, v);
            w.push('\n');
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::testutil::{expect_panic, test_concurrent, test_marshal_to};
    use crate::metrics::{MetricValue, Set, get_or_create_gauge, new_gauge};
    use std::sync::Mutex;

    // Port of gauge_test.go.
    #[test]
    fn test_gauge_error() {
        expect_panic("NewGauge_Set_non-nil-callback", || {
            let g = new_gauge("NewGauge_non_nil_callback", Some(Box::new(|| 123.0)));
            g.set(12.35);
        });
        expect_panic("GetOrCreateGauge_Set_non-nil-callback", || {
            let g = get_or_create_gauge("GetOrCreateGauge_nil_callback", Some(Box::new(|| 123.0)));
            g.set(42.0);
        });
        expect_panic("GetOrCreateGauge_Add_non-nil-callback", || {
            let g = get_or_create_gauge("GetOrCreateGauge_nil_callback", Some(Box::new(|| 123.0)));
            g.add(42.0);
        });
        expect_panic("GetOrCreateGauge_Inc_non-nil-callback", || {
            let g = get_or_create_gauge("GetOrCreateGauge_nil_callback", Some(Box::new(|| 123.0)));
            g.inc();
        });
        expect_panic("GetOrCreateGauge_Dec_non-nil-callback", || {
            let g = get_or_create_gauge("GetOrCreateGauge_nil_callback", Some(Box::new(|| 123.0)));
            g.dec();
        });
    }

    #[test]
    fn test_gauge_set() {
        let s = Set::new();
        let g = s.new_gauge("foo", None);
        assert_eq!(g.get(), 0.0, "unexpected gauge value; expecting 0");
        g.set(1.234);
        assert_eq!(g.get(), 1.234, "unexpected gauge value; expecting 1.234");
    }

    #[test]
    fn test_gauge_inc_dec() {
        let s = Set::new();
        let g = s.new_gauge("foo", None);
        assert_eq!(g.get(), 0.0, "unexpected gauge value; expecting 0");
        for i in 1..=100 {
            g.inc();
            assert_eq!(
                g.get(),
                f64::from(i),
                "unexpected gauge value; expecting {i}"
            );
        }
        for i in (0..=99).rev() {
            g.dec();
            assert_eq!(
                g.get(),
                f64::from(i),
                "unexpected gauge value; expecting {i}"
            );
        }
    }

    #[test]
    fn test_gauge_inc_dec_concurrent() {
        let s = Set::new();
        let g = s.new_gauge("foo", None);

        const WORKERS: usize = 5;
        std::thread::scope(|scope| {
            for _ in 0..WORKERS {
                scope.spawn(|| {
                    for _ in 0..100 {
                        g.inc();
                        g.dec();
                    }
                });
            }
        });

        assert_eq!(g.get(), 0.0, "unexpected gauge value; want 0");
    }

    #[test]
    fn test_gauge_serial() {
        let name = "GaugeSerial";
        let n = Mutex::new(1.23);
        // PORT NOTE: the Go test mutates the captured variable from the
        // callback and reads it afterwards; the port shares it via a leaked
        // Mutex so the 'static gauge callback can reference it.
        let n: &'static Mutex<f64> = Box::leak(Box::new(n));
        let g = new_gauge(
            name,
            Some(Box::new(|| {
                let mut n = n.lock().unwrap();
                *n += 1.0;
                *n
            })),
        );
        for _ in 0..10 {
            let nn = g.get();
            let want = *n.lock().unwrap();
            assert_eq!(nn, want, "unexpected gauge value");
        }

        // Verify marshal_to.
        let g = MetricValue::Gauge(g);
        test_marshal_to(&g, "foobar", "foobar 12.23\n");

        // Verify big numbers marshaling.
        *n.lock().unwrap() = 1234567899.0;
        test_marshal_to(&g, "prefix", "prefix 1234567900\n");
    }

    #[test]
    fn test_gauge_concurrent() {
        let name = "GaugeConcurrent";
        let n: &'static Mutex<i64> = Box::leak(Box::new(Mutex::new(0)));
        let g = new_gauge(
            name,
            Some(Box::new(|| {
                let mut n = n.lock().unwrap();
                *n += 1;
                *n as f64
            })),
        );
        test_concurrent(|| {
            let n_prev = g.get();
            for _ in 0..10 {
                let n = g.get();
                if n <= n_prev {
                    return Err(format!(
                        "gauge value must be greater than {n_prev}; got {n}"
                    ));
                }
            }
            Ok(())
        });
    }
}
