//! Port of Softalink LLC `lib/procutil`.
//!
//! PORT NOTE: Go returns `os.Signal` values and `<-chan os.Signal` channels.
//! The Rust port represents signals as their raw `i32` numbers (see the
//! `SIGHUP`/`SIGINT`/`SIGTERM` constants) and signal channels as
//! `std::sync::mpsc::Receiver<i32>` with a buffer of 1 and drop-on-full
//! notification, matching Go's `make(chan os.Signal, 1)` + `select`/`default`.

use std::sync::Mutex;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

/// SIGHUP signal number.
pub const SIGHUP: i32 = 1;
/// SIGINT signal number.
pub const SIGINT: i32 = 2;
/// SIGTERM signal number.
pub const SIGTERM: i32 = 15;

struct Subscribers {
    sighup: Vec<SyncSender<i32>>,
    term: Vec<SyncSender<i32>>,
}

static SUBSCRIBERS: Mutex<Subscribers> = Mutex::new(Subscribers {
    sighup: Vec::new(),
    term: Vec::new(),
});

fn notify(subs: &mut Vec<SyncSender<i32>>, sig: i32) {
    // A full buffer drops the signal, like Go's `select { case sub <- sig: default: }`.
    // Senders whose receiver is gone are pruned.
    subs.retain(|tx| !matches!(tx.try_send(sig), Err(TrySendError::Disconnected(_))));
}

fn notify_sighup(sig: i32) {
    notify(&mut SUBSCRIBERS.lock().unwrap().sighup, sig);
}

fn notify_term(sig: i32) {
    notify(&mut SUBSCRIBERS.lock().unwrap().term, sig);
}

/// Waits for either SIGTERM or SIGINT.
///
/// Returns the caught signal.
///
/// It also prevent from program termination on SIGHUP signal,
/// since this signal is frequently used for config reloading.
///
/// On Windows there is no SIGHUP/SIGTERM: Ctrl+C and Ctrl+Break are reported
/// as `SIGINT`, like Go's `os.Interrupt`.
pub fn wait_for_sigterm() -> i32 {
    imp::init();
    let rx = {
        let (tx, rx) = sync_channel(1);
        SUBSCRIBERS.lock().unwrap().term.push(tx);
        rx
    };
    let sig = rx
        .recv()
        .expect("BUG: procutil signal dispatcher terminated unexpectedly");
    // Stop listening for SIGINT and SIGTERM signals,
    // so the app could be interrupted by sending these signals again
    // in the case if the caller doesn't finish the app gracefully.
    imp::stop_term_signals();
    sig
}

/// Installs the SIGHUP/SIGINT/SIGTERM handlers up front.
///
/// Call this once at the very start of `main`, before any long-running setup
/// (opening storage, starting the HTTP server). Otherwise a SIGHUP delivered
/// during startup — e.g. when the launching shell exits and the server has been
/// backgrounded — hits the default "terminate" disposition and kills the
/// process before [`wait_for_sigterm`] gets a chance to install the handlers.
/// Idempotent (guarded by a `Once`).
pub fn init() {
    imp::init();
}

/// Sends SIGHUP signal to the current process.
///
/// On Windows (which has no SIGHUP) the subscribed listeners are notified
/// directly, like in the Go windows build.
pub fn self_sighup() {
    imp::self_sighup();
}

/// Delivers a synthetic SIGTERM to the [`wait_for_sigterm`] listeners so the
/// process shuts down gracefully (used by the `ESL_EXIT_AFTER_SECS` test/PGO
/// aid, where a forced kill would skip atexit hooks and lose profile data).
pub fn self_sigterm() {
    notify_term(15);
}

/// Returns a channel, which is triggered on every SIGHUP
/// (on Windows: on every [`self_sighup`] call).
pub fn new_sighup_chan() -> Receiver<i32> {
    imp::init();
    let (tx, rx) = sync_channel(1);
    SUBSCRIBERS.lock().unwrap().sighup.push(tx);
    rx
}

/// Returns a channel, which is triggered on every SIGINT/SIGTERM (on Windows:
/// Ctrl+C / Ctrl+Break) without terminating the process — the Rust equivalent
/// of Go's `signal.Notify(ch, os.Interrupt, ...)` for interactive tools that
/// must intercept Ctrl+C (see eslogscli's query cancellation).
///
/// Subscribing installs the process-wide signal handlers, so the default
/// kill-on-signal disposition is replaced for the process lifetime; the
/// subscriber decides whether to exit. Note the handlers cover SIGHUP too
/// (swallowed unless a [`new_sighup_chan`] subscriber exists).
pub fn new_term_chan() -> Receiver<i32> {
    imp::init();
    let (tx, rx) = sync_channel(1);
    SUBSCRIBERS.lock().unwrap().term.push(tx);
    rx
}

#[cfg(unix)]
mod imp {
    use std::sync::Once;
    use std::sync::atomic::{AtomicI32, Ordering};

    static PIPE_WR: AtomicI32 = AtomicI32::new(-1);
    static INIT: Once = Once::new();

    extern "C" fn on_signal(sig: libc::c_int) {
        let fd = PIPE_WR.load(Ordering::SeqCst);
        if fd >= 0 {
            let b = sig as u8;
            // SAFETY: write(2) is async-signal-safe; the buffer is a valid
            // single byte on the handler's stack.
            unsafe {
                libc::write(fd, std::ptr::addr_of!(b).cast(), 1);
            }
        }
    }

    // PORT NOTE: Go's os/signal delivers signals through runtime hooks. The
    // Rust port uses the classic self-pipe trick: signal handlers write the
    // signal number to a pipe, and a dedicated dispatcher thread forwards it
    // to the subscribed channels.
    pub(super) fn init() {
        INIT.call_once(|| {
            let mut fds = [0 as libc::c_int; 2];
            // SAFETY: pipe(2) with a valid 2-element out array.
            if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
                crate::panicf!(
                    "FATAL: cannot create signal pipe: {}",
                    std::io::Error::last_os_error()
                );
            }
            let (rd, wr) = (fds[0], fds[1]);
            PIPE_WR.store(wr, Ordering::SeqCst);
            install_handler(libc::SIGHUP);
            install_handler(libc::SIGINT);
            install_handler(libc::SIGTERM);
            std::thread::Builder::new()
                .name("procutil-signal-dispatcher".to_string())
                .spawn(move || dispatch_loop(rd))
                .expect("FATAL: cannot spawn the signal dispatcher thread");
        });
    }

    fn install_handler(sig: libc::c_int) {
        // SAFETY: sigaction is initialized to a zeroed struct with a valid
        // handler function pointer and an empty signal mask.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = on_signal as *const () as libc::sighandler_t;
            sa.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(sig, &sa, std::ptr::null_mut());
        }
    }

    fn dispatch_loop(rd: libc::c_int) {
        loop {
            let mut b = 0u8;
            // SAFETY: reading a single byte into a valid buffer.
            let n = unsafe { libc::read(rd, std::ptr::addr_of_mut!(b).cast(), 1) };
            if n <= 0 {
                if n < 0
                    && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                {
                    continue;
                }
                return;
            }
            match i32::from(b) {
                s if s == libc::SIGHUP => super::notify_sighup(super::SIGHUP),
                s if s == libc::SIGINT => super::notify_term(super::SIGINT),
                s if s == libc::SIGTERM => super::notify_term(super::SIGTERM),
                _ => {}
            }
        }
    }

    // PORT NOTE: Go's signal.Stop(ch) also stops SIGHUP delivery for the
    // channel used by WaitForSigterm. The Rust port restores the default
    // disposition for SIGINT/SIGTERM only and keeps the SIGHUP handler
    // installed, so new_sighup_chan subscribers keep working and SIGHUP
    // still doesn't terminate the process.
    pub(super) fn stop_term_signals() {
        // SAFETY: resetting the disposition of SIGINT/SIGTERM to SIG_DFL.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = libc::SIG_DFL;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
            libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        }
    }

    pub(super) fn self_sighup() {
        // PORT NOTE: handlers are installed first so that the self-signal
        // cannot terminate the process before any listener is registered.
        init();
        // SAFETY: kill(2) with the pid of the current process.
        unsafe {
            if libc::kill(libc::getpid(), libc::SIGHUP) != 0 {
                crate::panicf!(
                    "FATAL: cannot send SIGHUP to itself: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
    }
}

#[cfg(windows)]
mod imp {
    use std::sync::Once;

    // PORT NOTE: SetConsoleCtrlHandler is bound manually instead of enabling
    // the extra `Win32_System_Console` windows-sys feature for one function.
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetConsoleCtrlHandler(
            handler_routine: Option<unsafe extern "system" fn(u32) -> i32>,
            add: i32,
        ) -> i32;
    }

    const CTRL_C_EVENT: u32 = 0;
    const CTRL_BREAK_EVENT: u32 = 1;

    // https://golang.org/pkg/os/signal/#hdr-Windows — Go reports Ctrl+C and
    // Ctrl+Break as os.Interrupt; the port maps them to SIGINT.
    unsafe extern "system" fn ctrl_handler(ctrl_type: u32) -> i32 {
        match ctrl_type {
            CTRL_C_EVENT | CTRL_BREAK_EVENT => {
                super::notify_term(super::SIGINT);
                1
            }
            _ => 0,
        }
    }

    static INIT: Once = Once::new();

    pub(super) fn init() {
        INIT.call_once(|| {
            // SAFETY: registering a valid console ctrl handler function.
            if unsafe { SetConsoleCtrlHandler(Some(ctrl_handler), 1) } == 0 {
                crate::panicf!(
                    "FATAL: cannot set console ctrl handler: {}",
                    std::io::Error::last_os_error()
                );
            }
        });
    }

    pub(super) fn stop_term_signals() {
        // SAFETY: removing the previously registered handler; Ctrl+C then
        // terminates the process again, like the default disposition.
        unsafe {
            SetConsoleCtrlHandler(Some(ctrl_handler), 0);
        }
    }

    // https://github.com/golang/go/issues/6948 — Windows has no SIGHUP;
    // notify the subscribed listeners directly, like the Go windows build.
    pub(super) fn self_sighup() {
        super::notify_sighup(super::SIGHUP);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // The Go package has no tests; this verifies the SIGHUP delivery path
    // end to end (a real signal on unix, direct notification on windows).
    #[test]
    fn test_self_sighup_notifies_sighup_chan() {
        let rx = new_sighup_chan();
        self_sighup();
        let sig = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("SIGHUP wasn't delivered to the sighup chan");
        assert_eq!(sig, SIGHUP);
    }

    #[test]
    fn test_sighup_chan_does_not_block_on_full_buffer() {
        let rx = new_sighup_chan();
        // Two notifications without reading: the second is dropped, like
        // Go's buffered channel + select/default notify.
        self_sighup();
        self_sighup();
        let sig = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("SIGHUP wasn't delivered to the sighup chan");
        assert_eq!(sig, SIGHUP);
    }
}
