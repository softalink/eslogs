//! Client-disconnect watcher for long-running (buffered) HTTP query handlers.
//!
//! Go cancels an in-flight query through the request context: `net/http`
//! closes `r.Context().Done()` when the client goes away, and
//! `Storage.RunQuery` aborts with `context.Canceled`. This threaded/blocking
//! server has no per-request context, so the mechanism is adapted: a single
//! global watcher thread polls the sockets of registered in-flight requests
//! every [`POLL_INTERVAL`] with a non-blocking 1-byte `TcpStream::peek`. A
//! peek returning `Ok(0)` (EOF) or a hard error means the client is gone and
//! the entry's cancel flag is flipped; `WouldBlock` (nothing buffered) and
//! `Ok(1)` (a pipelined follow-up request, which `peek` leaves queued) mean
//! the client is still connected.
//!
//! Registration is one mutex push; connections that never register cost
//! nothing. Deregistration ([`CancelToken::drop`]) removes the entry under
//! the same lock the watcher holds while probing, so once `drop` returns the
//! watcher can never touch that socket again and the connection worker may
//! freely resume blocking reads/writes on it.

use std::io::ErrorKind;
use std::net::TcpStream;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

/// How often the watcher probes the registered connections. A disconnect is
/// observed within roughly this interval; matches the coarse granularity of
/// Go's per-connection read-loop detection closely enough for query abort.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

struct WatchEntry {
    id: u64,
    stream: Arc<TcpStream>,
    cancel: Arc<AtomicBool>,
}

struct Watcher {
    entries: Mutex<Vec<WatchEntry>>,
    next_id: AtomicU64,
}

/// The global watcher. The polling thread is spawned once, on first use.
static WATCHER: LazyLock<Watcher> = LazyLock::new(|| {
    std::thread::Builder::new()
        .name("disconnect-watcher".to_string())
        .spawn(watch_loop)
        .expect("FATAL: cannot spawn the disconnect-watcher thread");
    Watcher {
        entries: Mutex::new(Vec::new()),
        next_id: AtomicU64::new(1),
    }
});

fn watch_loop() {
    loop {
        std::thread::sleep(POLL_INTERVAL);
        // Probing happens under the entries lock: CancelToken::drop removes
        // its entry under the same lock, so after a drop returns the watcher
        // is guaranteed to no longer probe (or have in-flight probes on) that
        // socket. Probes are non-blocking and take microseconds, so holding
        // the lock across the sweep does not stall registrations meaningfully.
        let mut entries = WATCHER.entries.lock().unwrap();
        entries.retain(|e| {
            if !probe_disconnected(&e.stream) {
                return true;
            }
            e.cancel.store(true, Ordering::SeqCst);
            false
        });
    }
}

/// One-shot synchronous client-disconnect probe (Go `r.Context().Err()` at a
/// single point): a non-blocking `peek` that returns `true` when the client has
/// gone (EOF or hard error) and `false` while it is still connected (idle or a
/// pipelined follow-up request queued). Does not consume any buffered bytes.
pub fn probe_disconnected_once(stream: &TcpStream) -> bool {
    probe_disconnected(stream)
}

/// Probes `stream` for a client disconnect without consuming any buffered
/// bytes (`peek`, not `read`: a pipelined keep-alive request must stay queued
/// for the connection worker).
fn probe_disconnected(stream: &TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return true;
    }
    let mut probe = [0u8; 1];
    let res = stream.peek(&mut probe);
    // If blocking mode cannot be restored, the connection worker's blocking
    // reads/writes would misbehave; treat the socket as dead.
    if stream.set_nonblocking(false).is_err() {
        return true;
    }
    match res {
        Ok(0) => true,                                        // EOF: client gone
        Ok(_) => false,                                       // pipelined bytes queued
        Err(e) if e.kind() == ErrorKind::WouldBlock => false, // idle: still connected
        Err(_) => true,                                       // reset or other hard error
    }
}

/// A registered disconnect watch for one in-flight request.
///
/// Derefs to the cancel flag (`Arc<AtomicBool>`), which the watcher sets to
/// `true` once the client disconnects. Dropping the token deregisters the
/// socket and quiesces the watcher (see the module docs) — drop it before the
/// connection worker resumes socket I/O (in the HTTP server this holds
/// naturally: the response is written only after the handler returns).
pub struct CancelToken {
    id: u64,
    cancel: Arc<AtomicBool>,
}

impl Deref for CancelToken {
    type Target = Arc<AtomicBool>;

    fn deref(&self) -> &Arc<AtomicBool> {
        &self.cancel
    }
}

impl Drop for CancelToken {
    fn drop(&mut self) {
        let mut entries = WATCHER.entries.lock().unwrap();
        entries.retain(|e| e.id != self.id);
    }
}

/// Registers `stream` with the global watcher and returns the token whose
/// flag flips when the peer disconnects.
pub(crate) fn watch(stream: Arc<TcpStream>) -> CancelToken {
    let cancel = Arc::new(AtomicBool::new(false));
    let id = WATCHER.next_id.fetch_add(1, Ordering::SeqCst);
    WATCHER.entries.lock().unwrap().push(WatchEntry {
        id,
        stream,
        cancel: Arc::clone(&cancel),
    });
    CancelToken { id, cancel }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Instant;

    /// Returns a connected (server_side, client_side) loopback socket pair.
    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (server, client)
    }

    fn wait_until(deadline: Duration, mut f: impl FnMut() -> bool) -> bool {
        let t0 = Instant::now();
        while t0.elapsed() < deadline {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        f()
    }

    #[test]
    fn test_probe_disconnected_once() {
        // Still connected (idle) -> false.
        let (server, client) = tcp_pair();
        assert!(!probe_disconnected_once(&server));
        // Client closes -> the one-shot probe reports the disconnect, and the
        // socket is left in blocking mode for the connection worker.
        drop(client);
        assert!(wait_until(Duration::from_secs(5), || {
            probe_disconnected_once(&server)
        }));
        // A pipelined follow-up byte is NOT consumed and reads as "connected".
        let (server, mut client) = tcp_pair();
        client.write_all(b"x").unwrap();
        assert!(!probe_disconnected_once(&server));
        let mut buf = [0u8; 1];
        server.set_nonblocking(false).unwrap();
        assert_eq!((&server).read(&mut buf).unwrap(), 1);
        assert_eq!(buf[0], b'x'); // peek left the byte queued
    }

    #[test]
    fn test_watcher_flips_flag_on_client_close() {
        let (server, client) = tcp_pair();
        let token = watch(Arc::new(server));
        assert!(!token.load(Ordering::SeqCst));
        drop(client);
        assert!(
            wait_until(Duration::from_secs(5), || token.load(Ordering::SeqCst)),
            "cancel flag must flip after the client closes the connection"
        );
    }

    #[test]
    fn test_watcher_does_not_flip_while_connected() {
        let (server, mut client) = tcp_pair();
        let token = watch(Arc::new(server));
        // Idle connection: several poll intervals pass without a flip.
        std::thread::sleep(POLL_INTERVAL * 3);
        assert!(
            !token.load(Ordering::SeqCst),
            "idle connection is not a disconnect"
        );
        // Pipelined bytes must not flip the flag either (peek leaves them queued).
        client.write_all(b"GET /next HTTP/1.1\r\n").unwrap();
        std::thread::sleep(POLL_INTERVAL * 3);
        assert!(
            !token.load(Ordering::SeqCst),
            "buffered pipelined bytes are not a disconnect"
        );
    }

    #[test]
    fn test_drop_quiesces_and_leaves_socket_usable() {
        let (server, mut client) = tcp_pair();
        let server = Arc::new(server);
        let token = watch(Arc::clone(&server));
        // Let the watcher probe at least once, then deregister.
        std::thread::sleep(POLL_INTERVAL * 2);
        let flag = Arc::clone(&token);
        drop(token);

        // After drop the watcher must never touch the socket again: bytes sent
        // now stay queued (peek would have left them anyway, but the socket
        // must also be back in blocking mode) and are read back cleanly.
        client.write_all(b"hello").unwrap();
        let mut buf = [0u8; 5];
        server
            .as_ref()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        (&*server).read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");

        // Even a client close after deregistration must not flip the flag.
        drop(client);
        std::thread::sleep(POLL_INTERVAL * 3);
        assert!(
            !flag.load(Ordering::SeqCst),
            "deregistered sockets must not be probed"
        );
    }
}
