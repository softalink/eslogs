//! Port of Softalink LLC `lib/timerpool`.
//!
//! Timer pool reduces the load on the allocator by reusing timers.

// PORT NOTE: Go pools `*time.Timer` objects in a `sync.Pool`. Rust std has no
// timer type, so each pooled `Timer` owns a dedicated worker thread that is
// armed on demand and fires on the `c` channel; the pool itself is a
// `Mutex<Vec<Timer>>`. Semantics of Get/Reset/Stop/Put match Go: after
// `reset()` returns, no value corresponding to the previous timer settings
// can be received from `c`.

use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

enum Ctl {
    Arm(Duration),
    Stop(Sender<()>),
}

/// Timer fires on the `c` channel once the configured duration elapses.
pub struct Timer {
    /// The channel where the fire time is sent when the timer expires (Go `t.C`).
    pub c: Receiver<Instant>,

    ctl: Sender<Ctl>,
}

impl Timer {
    fn new(d: Duration) -> Timer {
        let (ctl_tx, ctl_rx) = mpsc::channel();
        // Capacity 1 matches Go's timer channel: extra fires are dropped.
        let (c_tx, c_rx) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("timerpool".to_string())
            .spawn(move || timer_worker(ctl_rx, c_tx))
            .expect("FATAL: cannot spawn timerpool worker thread");
        let t = Timer {
            c: c_rx,
            ctl: ctl_tx,
        };
        t.arm(d);
        t
    }

    fn arm(&self, d: Duration) {
        let _ = self.ctl.send(Ctl::Arm(d));
    }

    fn stop(&self) {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.ctl.send(Ctl::Stop(ack_tx)).is_ok() {
            // Wait until the worker is disarmed, so it cannot fire afterwards.
            let _ = ack_rx.recv();
        }
    }

    fn reset(&self, d: Duration) {
        self.stop();
        // Drop a possible stale fire from the previous settings, so any
        // receive from c after reset() has returned is guaranteed not to
        // receive a value corresponding to the previous timer settings.
        while self.c.try_recv().is_ok() {}
        self.arm(d);
    }
}

fn timer_worker(ctl: Receiver<Ctl>, c: SyncSender<Instant>) {
    let mut pending: Option<Duration> = None;
    loop {
        let cmd = match pending.take() {
            Some(d) => match ctl.recv_timeout(d) {
                Ok(cmd) => cmd,
                Err(RecvTimeoutError::Timeout) => {
                    let _ = c.try_send(Instant::now());
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => return,
            },
            None => match ctl.recv() {
                Ok(cmd) => cmd,
                Err(_) => return,
            },
        };
        match cmd {
            Ctl::Arm(d) => pending = Some(d),
            Ctl::Stop(ack) => {
                let _ = ack.send(());
            }
        }
    }
}

static TIMER_POOL: Mutex<Vec<Timer>> = Mutex::new(Vec::new());

/// Get returns a timer for the given duration d from the pool.
///
/// Return back the timer to the pool with `put`.
pub fn get(d: Duration) -> Timer {
    if let Some(t) = TIMER_POOL.lock().unwrap().pop() {
        // any receive from t.c after reset has returned is guaranteed not
        // to receive a time value corresponding to the previous timer settings
        t.reset(d);
        return t;
    }
    Timer::new(d)
}

/// Put returns t to the pool.
///
/// t cannot be accessed after returning to the pool.
pub fn put(t: Timer) {
    t.stop();
    TIMER_POOL.lock().unwrap().push(t);
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: the Go package has no tests; these smoke tests cover the
    // Rust worker-thread emulation of time.Timer.

    #[test]
    fn test_get_put_reuse() {
        let t = get(Duration::from_millis(10));
        t.c.recv().expect("timer must fire");
        put(t);

        let t = get(Duration::from_millis(10));
        t.c.recv().expect("reused timer must fire");
        put(t);
    }

    #[test]
    fn test_reset_drops_stale_fire() {
        let t = get(Duration::from_millis(1));
        // Let the timer fire and leave the value in the channel.
        thread::sleep(Duration::from_millis(50));
        put(t);

        let t = get(Duration::from_secs(3600));
        assert!(
            t.c.try_recv().is_err(),
            "no value from the previous timer settings must be received after reset"
        );
        put(t);
    }
}
