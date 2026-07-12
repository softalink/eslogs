//! Tests for `esl_common::tlsutil`.
//!
//! PORT NOTE: Go has no direct test-file counterpart for the promauth/netutil
//! TLS helpers used here (upstream covers them indirectly); these tests cover
//! the ported surface end to end with self-signed certificates generated
//! in-test via `rcgen` (dev-dependency only — it does not affect the
//! cross-compile target closure).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use esl_common::tlsutil::{
    self, TLSConfig, client_connect, get_server_tls_config, new_tls_client_config,
    parse_tls_version, server_accept,
};

/// A self-signed cert/key pair for `localhost`/`127.0.0.1`, written to temp
/// PEM files (the server-side API takes file paths, like Go).
struct TestCert {
    cert_path: String,
    key_path: String,
    cert_pem: String,
}

impl TestCert {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let dir = std::env::temp_dir().join(format!(
            "esl-tlsutil-test-{}-{tag}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem").to_string_lossy().into_owned();
        let key_path = dir.join("key.pem").to_string_lossy().into_owned();
        let cert_pem = ck.cert.pem();
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, ck.key_pair.serialize_pem()).unwrap();
        TestCert {
            cert_path,
            key_path,
            cert_pem,
        }
    }
}

/// Starts a TLS echo server: accepts one connection, completes the handshake,
/// reads until EOF and echoes the payload back, then closes.
fn spawn_tls_echo_server(
    cfg: Arc<tlsutil::rustls::ServerConfig>,
) -> (
    std::net::SocketAddr,
    std::thread::JoinHandle<Result<(), String>>,
) {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = ln.local_addr().unwrap();
    let handle = std::thread::spawn(move || -> Result<(), String> {
        let (tcp, _) = ln.accept().map_err(|e| e.to_string())?;
        let mut stream = server_accept(&cfg, tcp)?;
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).map_err(|e| e.to_string())?;
        stream.write_all(&buf[..n]).map_err(|e| e.to_string())?;
        stream.conn.send_close_notify();
        let _ = stream.flush();
        Ok(())
    });
    (addr, handle)
}

fn connect_and_echo(
    client_cfg: &tlsutil::TlsClientConfig,
    addr: std::net::SocketAddr,
) -> Result<Vec<u8>, String> {
    // Deterministic echo protocol: write one message, read the exact echo
    // back. No client close_notify/shutdown before reading — the one-shot
    // server may close before consuming them, and unread bytes at close would
    // turn the server's FIN into a RST.
    let tcp = TcpStream::connect(addr).map_err(|e| e.to_string())?;
    let mut stream = client_connect(client_cfg, "localhost", tcp)?;
    stream
        .write_all(b"hello over tls")
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    let mut out = vec![0u8; b"hello over tls".len()];
    stream.read_exact(&mut out).map_err(|e| e.to_string())?;
    Ok(out)
}

#[test]
fn test_round_trip_with_custom_ca() {
    let cert = TestCert::new("custom-ca");
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);

    // The self-signed server cert doubles as the custom CA (ca_file path).
    let client_cfg = new_tls_client_config(&TLSConfig {
        ca_file: cert.cert_path.clone(),
        ..Default::default()
    })
    .unwrap();
    let echoed = connect_and_echo(&client_cfg, addr).unwrap();
    assert_eq!(echoed, b"hello over tls");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_round_trip_with_inline_ca() {
    let cert = TestCert::new("inline-ca");
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);

    let client_cfg = new_tls_client_config(&TLSConfig {
        ca: cert.cert_pem.clone(),
        ..Default::default()
    })
    .unwrap();
    let echoed = connect_and_echo(&client_cfg, addr).unwrap();
    assert_eq!(echoed, b"hello over tls");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_verification_fails_against_untrusted_ca_and_insecure_skips_it() {
    let cert = TestCert::new("untrusted");

    // Attempt 1: default (webpki) roots do not trust the self-signed cert.
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg.clone());
    let strict_cfg = new_tls_client_config(&TLSConfig::default()).unwrap();
    let err = connect_and_echo(&strict_cfg, addr).unwrap_err();
    assert!(
        err.contains("handshake") || err.contains("certificate"),
        "unexpected error: {err}"
    );
    let _ = handle.join().unwrap(); // server side sees the aborted handshake

    // Attempt 2: insecure_skip_verify accepts it.
    let (addr, handle) = spawn_tls_echo_server(server_cfg);
    let insecure_cfg = new_tls_client_config(&TLSConfig {
        insecure_skip_verify: true,
        ..Default::default()
    })
    .unwrap();
    let echoed = connect_and_echo(&insecure_cfg, addr).unwrap();
    assert_eq!(echoed, b"hello over tls");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_server_name_override() {
    let cert = TestCert::new("server-name");
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);

    // Connect "to" a mismatching host but override the verified name via
    // server_name (Go tls.Config.ServerName).
    let client_cfg = new_tls_client_config(&TLSConfig {
        ca_file: cert.cert_path.clone(),
        server_name: "localhost".to_string(),
        ..Default::default()
    })
    .unwrap();
    let tcp = TcpStream::connect(addr).unwrap();
    let mut stream = client_connect(&client_cfg, "host-that-does-not-match", tcp).unwrap();
    stream.write_all(b"hi").unwrap();
    stream.flush().unwrap();
    let mut out = [0u8; 2];
    stream.read_exact(&mut out).unwrap();
    assert_eq!(&out, b"hi");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_min_version_rejection() {
    let cert = TestCert::new("min-version");
    // Server requires TLS 1.3 (the -syslog.tlsMinVersion default).
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "TLS13", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);

    // A TLS 1.2-only client must be rejected.
    let provider = Arc::new(tlsutil::rustls::crypto::ring::default_provider());
    let mut roots = tlsutil::rustls::RootCertStore::empty();
    let mut reader = std::io::BufReader::new(cert.cert_pem.as_bytes());
    for c in rustls_pemfile::certs(&mut reader) {
        roots.add(c.unwrap()).unwrap();
    }
    let tls12_only = tlsutil::rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&tlsutil::rustls::version::TLS12])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client_cfg = tlsutil::TlsClientConfig {
        config: Arc::new(tls12_only),
        server_name: String::new(),
    };
    let err = connect_and_echo(&client_cfg, addr).unwrap_err();
    assert!(err.contains("handshake"), "unexpected error: {err}");
    let _ = handle.join().unwrap();
}

#[test]
fn test_round_trip_with_client_certificate() {
    let cert = TestCert::new("client-cert");
    let server_cfg = get_server_tls_config(&cert.cert_path, &cert.key_path, "", &[]).unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);

    // The server does not require client auth, but the config path exercising
    // cert_file+key_file loading must still handshake fine.
    let client_cfg = new_tls_client_config(&TLSConfig {
        ca_file: cert.cert_path.clone(),
        cert_file: cert.cert_path.clone(),
        key_file: cert.key_path.clone(),
        ..Default::default()
    })
    .unwrap();
    let echoed = connect_and_echo(&client_cfg, addr).unwrap();
    assert_eq!(echoed, b"hello over tls");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_cert_and_key_must_both_be_set() {
    let cert = TestCert::new("half-pair");
    let err = new_tls_client_config(&TLSConfig {
        cert_file: cert.cert_path.clone(),
        ..Default::default()
    })
    .unwrap_err();
    assert!(err.contains("both TLS certificate and key"), "{err}");
}

#[test]
fn test_parse_tls_version() {
    // Ported from Go netutil TestParseTLSVersionSuccess/Failure semantics.
    for ok in ["", "TLS13", "TLS12", "TLS11", "TLS10", "tls13", "Tls12"] {
        assert!(parse_tls_version(ok).is_ok(), "must parse {ok:?}");
    }
    for bad in ["invalid", "TLS14", "SSL3", "TLS1.2"] {
        assert!(parse_tls_version(bad).is_err(), "must reject {bad:?}");
    }
    // TLS13 restricts to a single version; TLS12 keeps 1.2 and 1.3.
    assert_eq!(parse_tls_version("TLS13").unwrap().len(), 1);
    assert_eq!(parse_tls_version("TLS12").unwrap().len(), 2);
}

#[test]
fn test_server_config_cipher_suites() {
    let cert = TestCert::new("ciphers");

    // Named TLS 1.2 suite (Go crypto/tls spelling) is accepted.
    get_server_tls_config(
        &cert.cert_path,
        &cert.key_path,
        "",
        &["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()],
    )
    .unwrap();
    // Case-insensitive, like Go's strings.ToLower matching.
    get_server_tls_config(
        &cert.cert_path,
        &cert.key_path,
        "",
        &["tls_ecdhe_rsa_with_aes_128_gcm_sha256".to_string()],
    )
    .unwrap();
    // Numeric ID (Go: strconv.ParseUint(name, 0, 16)).
    get_server_tls_config(&cert.cert_path, &cert.key_path, "", &["0xc02f".to_string()]).unwrap();
    // Unknown name is rejected with Go's error text.
    let err = get_server_tls_config(
        &cert.cert_path,
        &cert.key_path,
        "",
        &["TLS_NOT_A_SUITE".to_string()],
    )
    .unwrap_err();
    assert!(err.contains("unsupported TLS cipher suite name"), "{err}");
    // A CBC suite Go would accept is not implemented by rustls (PORT NOTE).
    let err = get_server_tls_config(
        &cert.cert_path,
        &cert.key_path,
        "",
        &["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA".to_string()],
    )
    .unwrap_err();
    assert!(err.contains("unsupported TLS cipher suite name"), "{err}");

    // End-to-end with a restricted TLS 1.2 suite list and TLS 1.2 minimum.
    let server_cfg = get_server_tls_config(
        &cert.cert_path,
        &cert.key_path,
        "TLS12",
        &["TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".to_string()],
    )
    .unwrap();
    let (addr, handle) = spawn_tls_echo_server(server_cfg);
    let client_cfg = new_tls_client_config(&TLSConfig {
        ca_file: cert.cert_path.clone(),
        ..Default::default()
    })
    .unwrap();
    let echoed = connect_and_echo(&client_cfg, addr).unwrap();
    assert_eq!(echoed, b"hello over tls");
    handle.join().unwrap().unwrap();
}

#[test]
fn test_server_config_requires_valid_cert_files() {
    let err = get_server_tls_config("/nonexistent/cert.pem", "/nonexistent/key.pem", "", &[])
        .unwrap_err();
    assert!(
        err.contains("cannot load TLS certificate and key files"),
        "{err}"
    );

    let cert = TestCert::new("bad-min-version");
    let err = get_server_tls_config(&cert.cert_path, &cert.key_path, "TLS14", &[]).unwrap_err();
    assert!(err.contains("cannot use TLS min version"), "{err}");
}

#[test]
fn test_client_config_bad_min_version() {
    let err = new_tls_client_config(&TLSConfig {
        min_version: "SSL3".to_string(),
        ..Default::default()
    })
    .unwrap_err();
    assert!(err.contains("cannot use TLS min version"), "{err}");
}
