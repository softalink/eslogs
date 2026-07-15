//! Port of the listen-address handling in Softalink LLC `lib/netutil`.
//!
//! Go's `netutil.GetTCPNetwork`/`GetUDPNetwork` pick `tcp4`/`udp4` (IPv4 only)
//! unless `-enableTCP6` is set, in which case they pick `tcp`/`udp` (dual
//! stack). The port maps that onto the bind address: a "lone port" form
//! (`:9428` or empty, meaning "all interfaces") binds `0.0.0.0:port` by
//! default, or `[::]:port` when `-enableTCP6` is set.
//!
//! PORT NOTE: on Linux a `[::]` listener is dual-stack (the kernel default is
//! `IPV6_V6ONLY=0`), so `-enableTCP6` accepts both IPv4 and IPv6 exactly like
//! Go. On Windows the default is `IPV6_V6ONLY=1`, so a `[::]` listener is
//! IPv6-only there; matching Go's dual stack would require setting the socket
//! option before bind, which `std::net` does not expose (a `socket2`-style
//! dependency). This is the one residual — documented in docs/PARITY.md.

use crate::flagutil::Flag;

static ENABLE_TCP6: Flag<bool> = Flag::new(
    "enableTCP6",
    "Whether to enable IPv6 for listening and dialing. By default, only IPv4 TCP and UDP are used",
    || false,
);
crate::register_flag!(ENABLE_TCP6);

/// Returns true if `-enableTCP6` is set (Go `netutil.TCP6Enabled`).
pub fn tcp6_enabled() -> bool {
    *ENABLE_TCP6.get()
}

/// Normalizes a Go-style listen address for `std::net::{TcpListener,UdpSocket}`,
/// honoring `-enableTCP6`. A lone port (`:port`) or empty address binds all
/// interfaces: `0.0.0.0:port` (IPv4 only) by default, or `[::]:port` (dual
/// stack on Linux) when `-enableTCP6` is set. An explicit address is returned
/// unchanged.
pub fn normalize_listen_addr(addr: &str) -> String {
    normalize_listen_addr_with(addr, tcp6_enabled())
}

/// [`normalize_listen_addr`] with the `-enableTCP6` value passed explicitly, so
/// the mapping is unit-testable without touching global flag state.
pub fn normalize_listen_addr_with(addr: &str, enable_v6: bool) -> String {
    let all = if enable_v6 { "[::]" } else { "0.0.0.0" };
    if addr.is_empty() {
        format!("{all}:0")
    } else if let Some(port) = addr.strip_prefix(':') {
        format!("{all}:{port}")
    } else {
        addr.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_listen_addr_ipv4_default() {
        assert_eq!(normalize_listen_addr_with("", false), "0.0.0.0:0");
        assert_eq!(normalize_listen_addr_with(":9428", false), "0.0.0.0:9428");
        // Explicit addresses pass through unchanged.
        assert_eq!(
            normalize_listen_addr_with("127.0.0.1:9428", false),
            "127.0.0.1:9428"
        );
        assert_eq!(
            normalize_listen_addr_with("[::1]:9428", false),
            "[::1]:9428"
        );
    }

    #[test]
    fn test_normalize_listen_addr_tcp6() {
        assert_eq!(normalize_listen_addr_with("", true), "[::]:0");
        assert_eq!(normalize_listen_addr_with(":9428", true), "[::]:9428");
        // Explicit addresses still pass through unchanged.
        assert_eq!(
            normalize_listen_addr_with("127.0.0.1:9428", true),
            "127.0.0.1:9428"
        );
    }

    // On Linux a `[::]` listener is dual-stack, so it accepts an IPv4 loopback
    // connection — proving `-enableTCP6` gives Go-equivalent dual-stack there.
    #[cfg(unix)]
    #[test]
    fn test_tcp6_listener_is_dual_stack() {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        let addr = normalize_listen_addr_with(":0", true);
        let ln = TcpListener::bind(&addr).expect("bind [::]:0");
        let port = ln.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut conn, _) = ln.accept().expect("accept");
            let mut buf = [0u8; 4];
            conn.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"ping");
        });
        // Connect over IPv4 loopback to the [::] listener.
        let mut c = TcpStream::connect(("127.0.0.1", port)).expect("v4 connect to [::] listener");
        c.write_all(b"ping").unwrap();
        server.join().unwrap();
    }
}
