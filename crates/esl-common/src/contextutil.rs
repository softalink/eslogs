//! Port of Softalink LLC `lib/contextutil`.
//!
//! PORT NOTE: Go's `context.Context` and `chan struct{}` have no direct Rust
//! equivalents, so the port maps them to simple cancellation tokens:
//! `StopChan` stands in for a close-only `chan struct{}` stop channel, and
//! `StopChanContext` mirrors the `context.Context` returned by
//! `NewStopChanContext` with the same method semantics — `is_done()` is a
//! non-blocking `select` on `ctx.Done()`, `wait()` is `<-ctx.Done()` and
//! `err()` is `ctx.Err()` (Go's `context.Canceled` message is preserved).
//! `Deadline()` is not ported: it always returns "no deadline" in Go.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Rust stand-in for a Go `chan struct{}` used as a close-only stop signal.
#[derive(Clone)]
pub struct StopChan {
    inner: Arc<StopState>,
}

struct StopState {
    state: Mutex<StateInner>,
    cv: Condvar,
}

struct StateInner {
    closed: bool,
    children: Vec<StopChan>,
}

impl StopChan {
    /// Creates an open stop channel, like `make(chan struct{})`.
    pub fn new() -> Self {
        StopChan {
            inner: Arc::new(StopState {
                state: Mutex::new(StateInner {
                    closed: false,
                    children: Vec::new(),
                }),
                cv: Condvar::new(),
            }),
        }
    }

    /// Closes the channel, like `close(stopCh)`. Contexts derived from it are
    /// canceled. Closing an already closed channel is a no-op (unlike Go,
    /// where a double close panics).
    pub fn close(&self) {
        let children = {
            let mut st = self.inner.state.lock().unwrap();
            if st.closed {
                return;
            }
            st.closed = true;
            self.inner.cv.notify_all();
            std::mem::take(&mut st.children)
        };
        for child in children {
            child.close();
        }
    }

    /// Non-blocking closed check, like `select { case <-stopCh: ... default: }`.
    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().unwrap().closed
    }

    /// Blocks until the channel is closed, like `<-stopCh`.
    pub fn wait(&self) {
        let mut st = self.inner.state.lock().unwrap();
        while !st.closed {
            st = self.inner.cv.wait(st).unwrap();
        }
    }

    /// Waits for the channel to be closed for up to `timeout`.
    /// Returns true when the channel is closed.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let st = self.inner.state.lock().unwrap();
        let (st, _) = self
            .inner
            .cv
            .wait_timeout_while(st, timeout, |st| !st.closed)
            .unwrap();
        st.closed
    }

    /// Registers a child closed together with self.
    /// Returns false when self is already closed (the child isn't registered).
    fn add_child(&self, child: StopChan) -> bool {
        let mut st = self.inner.state.lock().unwrap();
        if st.closed {
            return false;
        }
        st.children.push(child);
        true
    }

    /// Unregisters a previously added child.
    fn remove_child(&self, child: &StopChan) {
        let mut st = self.inner.state.lock().unwrap();
        st.children.retain(|c| !Arc::ptr_eq(&c.inner, &child.inner));
    }
}

impl Default for StopChan {
    fn default() -> Self {
        StopChan::new()
    }
}

/// Returns new context for the given `stop_ch`, together with cancel function.
///
/// The returned context is canceled on the following events:
///
///   - when `stop_ch` is closed
///   - when the returned [`CancelFunc`] is called
///
/// The caller must call the returned CancelFunc when the context is no longer needed.
pub fn new_stop_chan_context(stop_ch: &StopChan) -> (StopChanContext, CancelFunc) {
    let done = StopChan::new();
    if !stop_ch.add_child(done.clone()) {
        // stop_ch is already closed.
        done.close();
    }
    (
        StopChanContext { done: done.clone() },
        CancelFunc {
            done,
            parent: stop_ch.clone(),
        },
    )
}

/// Cancellation-token mapping of the `context.Context` returned by Go's
/// `NewStopChanContext`.
#[derive(Clone)]
pub struct StopChanContext {
    done: StopChan,
}

impl StopChanContext {
    /// Non-blocking done check, like `select { case <-ctx.Done(): ... default: }`.
    pub fn is_done(&self) -> bool {
        self.done.is_closed()
    }

    /// Blocks until the context is canceled, like `<-ctx.Done()`.
    pub fn wait(&self) {
        self.done.wait();
    }

    /// Waits for cancellation for up to `timeout`; returns true when canceled.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.done.wait_timeout(timeout)
    }

    /// Like `ctx.Err()`: `Some("context canceled")` after cancellation, `None` before.
    pub fn err(&self) -> Option<&'static str> {
        if self.done.is_closed() {
            Some("context canceled")
        } else {
            None
        }
    }
}

/// Mapping of Go's `context.CancelFunc`; cancels the paired [`StopChanContext`].
///
/// PORT NOTE: Go's CancelFunc is a closure; the Rust port uses a struct with a
/// `cancel` method so it can also unregister the context from the stop channel
/// (Go's context.WithCancel does the same on cancel), preventing unbounded
/// growth on a long-lived stop channel. Calling `cancel` multiple times is a
/// no-op, like in Go.
pub struct CancelFunc {
    done: StopChan,
    parent: StopChan,
}

impl CancelFunc {
    /// Cancels the paired context.
    pub fn cancel(&self) {
        self.parent.remove_child(&self.done);
        self.done.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The Go package has no tests; these cover the documented semantics of
    // NewStopChanContext.
    #[test]
    fn test_cancel_func_cancels_context() {
        let stop_ch = StopChan::new();
        let (ctx, cancel) = new_stop_chan_context(&stop_ch);
        assert!(!ctx.is_done());
        assert_eq!(ctx.err(), None);
        cancel.cancel();
        assert!(ctx.is_done());
        assert_eq!(ctx.err(), Some("context canceled"));
        // wait() must return immediately once canceled.
        ctx.wait();
        assert!(ctx.wait_timeout(Duration::from_millis(1)));
        // Canceling again is a no-op.
        cancel.cancel();
        // Canceling the context must not close the stop channel.
        assert!(!stop_ch.is_closed());
    }

    #[test]
    fn test_stop_chan_close_cancels_context() {
        let stop_ch = StopChan::new();
        let (ctx, _cancel) = new_stop_chan_context(&stop_ch);
        assert!(!ctx.is_done());
        assert!(!ctx.wait_timeout(Duration::from_millis(1)));
        stop_ch.close();
        assert!(ctx.is_done());
        assert_eq!(ctx.err(), Some("context canceled"));
    }

    #[test]
    fn test_already_closed_stop_chan() {
        let stop_ch = StopChan::new();
        stop_ch.close();
        // Double close is a no-op.
        stop_ch.close();
        let (ctx, _cancel) = new_stop_chan_context(&stop_ch);
        assert!(ctx.is_done());
        assert_eq!(ctx.err(), Some("context canceled"));
    }

    #[test]
    fn test_wait_unblocks_across_threads() {
        let stop_ch = StopChan::new();
        let (ctx, _cancel) = new_stop_chan_context(&stop_ch);
        let waiter = {
            let ctx = ctx.clone();
            std::thread::spawn(move || ctx.wait())
        };
        stop_ch.close();
        waiter.join().unwrap();
        assert!(ctx.is_done());
    }
}
