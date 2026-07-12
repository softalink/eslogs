//! Shared TLS helpers for the workspace's std-TCP (blocking, threaded) code.
//!
//! Ports the TLS surface of two Go packages onto rustls:
//!   * `lib/promauth` client-side config (`TLSConfig` + `new_tls_client_config`
//!     porting `tlsContext.initFromTLSConfig` / `Config.GetTLSConfig`);
//!   * `lib/netutil` server-side config (`get_server_tls_config`,
//!     `parse_tls_version`, `cipher_suites_from_names`).
//!
//! PORT NOTE: divergences from Go's `crypto/tls`, shared by all users:
//!   * TLS 1.0/1.1 are not implemented by rustls. `parse_tls_version` accepts
//!     the Go names `TLS10`/`TLS11` but treats them as "no additional floor"
//!     (TLS 1.2+): a *minimum* version below what the library supports adds no
//!     restriction. `TLS13` restricts to TLS 1.3 exactly like Go.
//!   * cipher-suite granularity: rustls implements only the AEAD suites
//!     (ECDHE + AES-GCM/CHACHA20). Go additionally accepts CBC and static-RSA
//!     key-exchange suite names in `-*.tlsCipherSuites`; those names now fail
//!     with `unsupported TLS cipher suite name`, matching Go's error text for
//!     unknown names. Like Go, TLS 1.3 suites are not configurable — they stay
//!     enabled regardless of the configured list, which only filters TLS 1.2
//!     suites.
//!   * default roots come from the bundled Mozilla CA list (`webpki-roots`)
//!     instead of Go's system cert pool; `ca`/`ca_file` overrides replace the
//!     bundle, mirroring `tls.Config.RootCAs`.
//!   * client certs and roots are loaded once when the config is built. Go
//!     re-reads the files with a 1-second cache (`newGetTLSCertCached`); the
//!     server side keeps that reload behavior (see `reloading_cert_resolver`),
//!     the client side does not (config lifetimes here are process-long and
//!     Go only rebuilds its transport when the *config values* change).
//!
//! The crypto provider is rustls' `ring` backend: pure-Rust + prebuilt-asm, so
//! the workspace keeps cross-compiling to x86_64-pc-windows-msvc via clang-cl
//! (aws-lc-rs would drag in cmake/NASM).

use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub use rustls;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, ServerConfig,
    ServerConnection, SignatureScheme, StreamOwned, SupportedCipherSuite, SupportedProtocolVersion,
};

// ---------------------------------------------------------------------------
// version / cipher-suite parsing (Go lib/netutil)
// ---------------------------------------------------------------------------

static VERSIONS_TLS12_PLUS: [&SupportedProtocolVersion; 2] =
    [&rustls::version::TLS12, &rustls::version::TLS13];
static VERSIONS_TLS13_ONLY: [&SupportedProtocolVersion; 1] = [&rustls::version::TLS13];

/// Returns the protocol versions allowed by the given minimum-version name
/// (Go `netutil.ParseTLSVersion`; see the module PORT NOTE about TLS10/TLS11).
pub fn parse_tls_version(s: &str) -> Result<&'static [&'static SupportedProtocolVersion], String> {
    match s.to_uppercase().as_str() {
        // Special case - use the default TLS versions provided by rustls.
        "" => Ok(&VERSIONS_TLS12_PLUS),
        "TLS13" => Ok(&VERSIONS_TLS13_ONLY),
        "TLS12" => Ok(&VERSIONS_TLS12_PLUS),
        // PORT NOTE: rustls does not implement TLS 1.0/1.1; a minimum version
        // below the library floor imposes no extra restriction.
        "TLS11" | "TLS10" => Ok(&VERSIONS_TLS12_PLUS),
        _ => Err(format!("unsupported TLS version {s:?}")),
    }
}

/// Go crypto/tls cipher-suite names for every suite the ring provider
/// implements. TLS 1.3 suites carry the IANA name Go uses plus the rustls
/// constant name as an alias.
static CIPHER_SUITE_NAMES: &[(&str, u16)] = &[
    ("TLS_AES_128_GCM_SHA256", 0x1301),
    ("TLS13_AES_128_GCM_SHA256", 0x1301),
    ("TLS_AES_256_GCM_SHA384", 0x1302),
    ("TLS13_AES_256_GCM_SHA384", 0x1302),
    ("TLS_CHACHA20_POLY1305_SHA256", 0x1303),
    ("TLS13_CHACHA20_POLY1305_SHA256", 0x1303),
    ("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", 0xc02b),
    ("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", 0xc02f),
    ("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", 0xc02c),
    ("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", 0xc030),
    ("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", 0xcca9),
    ("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", 0xcca8),
];

/// Maps cipher-suite names (or numeric IDs, Go-style `strconv.ParseUint(name,
/// 0, 16)`) to suite IDs (Go `netutil.cipherSuitesFromNames`).
fn cipher_suite_ids_from_names(cipher_suite_names: &[String]) -> Result<Vec<u16>, String> {
    let supported: Vec<u16> = default_provider()
        .cipher_suites
        .iter()
        .map(|cs| u16::from(cs.suite()))
        .collect();
    let mut ids = Vec::with_capacity(cipher_suite_names.len());
    for name in cipher_suite_names {
        let by_name = CIPHER_SUITE_NAMES
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|&(_, id)| id);
        let id = match by_name {
            Some(id) => id,
            None => {
                // Try searching by ID (base-0 integer like Go: 0x-hex or decimal).
                let parsed = if let Some(hex) =
                    name.strip_prefix("0x").or_else(|| name.strip_prefix("0X"))
                {
                    u16::from_str_radix(hex, 16).ok()
                } else {
                    name.parse::<u16>().ok()
                };
                match parsed {
                    Some(id) if supported.contains(&id) => id,
                    _ => return Err(format!("unsupported TLS cipher suite name: {name}")),
                }
            }
        };
        ids.push(id);
    }
    Ok(ids)
}

fn default_provider() -> CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// Returns the ring provider with its TLS 1.2 suites filtered down to `names`
/// (empty `names` keeps every suite). TLS 1.3 suites always stay enabled —
/// same semantics as Go, where `tls.Config.CipherSuites` is ignored for 1.3.
fn provider_with_cipher_suites(names: &[String]) -> Result<CryptoProvider, String> {
    let mut provider = default_provider();
    if names.is_empty() {
        return Ok(provider);
    }
    let ids = cipher_suite_ids_from_names(names)?;
    provider.cipher_suites.retain(|cs| {
        matches!(cs, SupportedCipherSuite::Tls13(_)) || ids.contains(&u16::from(cs.suite()))
    });
    Ok(provider)
}

// ---------------------------------------------------------------------------
// PEM loading helpers
// ---------------------------------------------------------------------------

fn parse_pem_certs(pem: &[u8], source: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let mut reader = std::io::BufReader::new(pem);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|err| format!("cannot parse PEM certificates from {source}: {err}"))?;
    if certs.is_empty() {
        return Err(format!("no PEM certificates found in {source}"));
    }
    Ok(certs)
}

fn parse_pem_key(pem: &[u8], source: &str) -> Result<PrivateKeyDer<'static>, String> {
    let mut reader = std::io::BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| format!("cannot parse PEM private key from {source}: {err}"))?
        .ok_or_else(|| format!("no PEM private key found in {source}"))
}

fn read_file(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| format!("cannot read {path:?}: {err}"))
}

// ---------------------------------------------------------------------------
// client-side config (Go lib/promauth TLS surface)
// ---------------------------------------------------------------------------

/// TLS client options (Go `promauth.TLSConfig`, snake_case fields).
///
/// Inline PEM data (`ca`/`cert`/`key`) takes precedence over the corresponding
/// `*_file` path, matching Go's `tlsContext.initFromTLSConfig`.
#[derive(Debug, Default, Clone)]
pub struct TLSConfig {
    pub ca: String,
    pub ca_file: String,
    pub cert: String,
    pub cert_file: String,
    pub key: String,
    pub key_file: String,
    pub server_name: String,
    pub insecure_skip_verify: bool,
    pub min_version: String,
}

/// A built client config plus the SNI/Host override from `server_name`
/// (Go keeps the latter inside `tls.Config.ServerName`; rustls takes the name
/// at connect time instead).
#[derive(Debug, Clone)]
pub struct TlsClientConfig {
    pub config: Arc<ClientConfig>,
    /// Non-empty when the user overrode the server name; use it for SNI and
    /// as the https `Host` header (Go sets `req.Host = ac.tlsServerName`).
    pub server_name: String,
}

/// Builds a rustls client config from the given options (Go
/// `promauth.Config.GetTLSConfig` for a config built from `TLSConfig`).
pub fn new_tls_client_config(tc: &TLSConfig) -> Result<TlsClientConfig, String> {
    let provider = Arc::new(default_provider());
    let versions = parse_tls_version(&tc.min_version).map_err(|err| {
        format!(
            "cannot use TLS min version from min_version={:?}. Supported TLS versions (TLS10, TLS11, TLS12, TLS13): {err}",
            tc.min_version
        )
    })?;
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .map_err(|err| format!("cannot set TLS protocol versions: {err}"))?;

    let builder = if tc.insecure_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {
                algs: provider.signature_verification_algorithms,
            }))
    } else {
        let mut roots = RootCertStore::empty();
        if !tc.ca.is_empty() || !tc.ca_file.is_empty() {
            let (pem, source) = if !tc.ca.is_empty() {
                (tc.ca.clone().into_bytes(), "inline `ca` data".to_string())
            } else {
                (read_file(&tc.ca_file)?, format!("ca_file={:?}", tc.ca_file))
            };
            for cert in parse_pem_certs(&pem, &source)? {
                roots
                    .add(cert)
                    .map_err(|err| format!("cannot load root CA from {source}: {err}"))?;
            }
        } else {
            // PORT NOTE: Go uses the system cert pool by default; the port
            // ships the Mozilla CA bundle via webpki-roots.
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        builder.with_root_certificates(roots)
    };

    let has_cert = !tc.cert.is_empty() || !tc.cert_file.is_empty();
    let has_key = !tc.key.is_empty() || !tc.key_file.is_empty();
    let config = if has_cert || has_key {
        if !has_cert || !has_key {
            return Err(
                "both TLS certificate and key must be set; only one of them is set".to_string(),
            );
        }
        let (cert_pem, cert_source) = if !tc.cert.is_empty() {
            (
                tc.cert.clone().into_bytes(),
                "inline `cert` data".to_string(),
            )
        } else {
            (
                read_file(&tc.cert_file)?,
                format!("cert_file={:?}", tc.cert_file),
            )
        };
        let (key_pem, key_source) = if !tc.key.is_empty() {
            (tc.key.clone().into_bytes(), "inline `key` data".to_string())
        } else {
            (
                read_file(&tc.key_file)?,
                format!("key_file={:?}", tc.key_file),
            )
        };
        let certs = parse_pem_certs(&cert_pem, &cert_source)?;
        let key = parse_pem_key(&key_pem, &key_source)?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|err| format!("cannot load client certificate: {err}"))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(TlsClientConfig {
        config: Arc::new(config),
        server_name: tc.server_name.clone(),
    })
}

/// Certificate verifier that accepts anything
/// (Go `tls.Config.InsecureSkipVerify=true`).
#[derive(Debug)]
struct NoCertificateVerification {
    algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

/// A blocking client-side TLS stream over std TCP.
pub type TlsClientStream = StreamOwned<ClientConnection, TcpStream>;

/// Wraps an established TCP connection in TLS and completes the handshake
/// (blocking). `host` is the peer's hostname or IP (no port); it is used for
/// SNI/verification unless `cfg.server_name` overrides it.
pub fn client_connect(
    cfg: &TlsClientConfig,
    host: &str,
    tcp: TcpStream,
) -> Result<TlsClientStream, String> {
    let name = if cfg.server_name.is_empty() {
        host
    } else {
        &cfg.server_name
    };
    let server_name = ServerName::try_from(name.to_string())
        .map_err(|err| format!("invalid TLS server name {name:?}: {err}"))?;
    let conn = ClientConnection::new(Arc::clone(&cfg.config), server_name)
        .map_err(|err| format!("cannot create TLS client connection to {name:?}: {err}"))?;
    let mut stream = StreamOwned::new(conn, tcp);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|err| format!("TLS handshake with {name:?} failed: {err}"))?;
    }
    Ok(stream)
}

// ---------------------------------------------------------------------------
// server-side config (Go lib/netutil.GetServerTLSConfig)
// ---------------------------------------------------------------------------

/// Certificate resolver that re-loads the cert/key pair from disk with a
/// 1-second cache (Go `netutil.newGetCertificateFunc`), so rotated certs are
/// picked up without a restart.
#[derive(Debug)]
struct ReloadingCertResolver {
    cert_file: String,
    key_file: String,
    cached: Mutex<(u64, Option<Arc<CertifiedKey>>)>,
}

impl ReloadingCertResolver {
    fn load(&self) -> Result<Arc<CertifiedKey>, String> {
        let certs = parse_pem_certs(
            &read_file(&self.cert_file)?,
            &format!("certFile={:?}", self.cert_file),
        )?;
        let key = parse_pem_key(
            &read_file(&self.key_file)?,
            &format!("keyFile={:?}", self.key_file),
        )?;
        let signing_key = default_provider()
            .key_provider
            .load_private_key(key)
            .map_err(|err| format!("cannot load private key from {:?}: {err}", self.key_file))?;
        Ok(Arc::new(CertifiedKey::new(certs, signing_key)))
    }
}

impl ResolvesServerCert for ReloadingCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let mut cached = self.cached.lock().unwrap();
        let now = crate::fasttime::unix_timestamp();
        if now > cached.0 {
            match self.load() {
                Ok(ck) => *cached = (now + 1, Some(ck)),
                Err(err) => {
                    crate::errorf!(
                        "cannot load TLS cert from certFile={:?}, keyFile={:?}: {}",
                        self.cert_file,
                        self.key_file,
                        err
                    );
                    return cached.1.clone();
                }
            }
        }
        cached.1.clone()
    }
}

/// Builds a rustls server config from cert/key files, a minimum TLS version
/// name and optional cipher-suite names (Go `netutil.GetServerTLSConfig`).
pub fn get_server_tls_config(
    tls_cert_file: &str,
    tls_key_file: &str,
    tls_min_version: &str,
    tls_cipher_suites: &[String],
) -> Result<Arc<ServerConfig>, String> {
    let resolver = ReloadingCertResolver {
        cert_file: tls_cert_file.to_string(),
        key_file: tls_key_file.to_string(),
        cached: Mutex::new((0, None)),
    };
    // Go validates the pair upfront with tls.LoadX509KeyPair.
    resolver
        .load()
        .map_err(|err| format!("cannot load TLS certificate and key files: {err}"))?;

    let versions = parse_tls_version(tls_min_version).map_err(|err| {
        format!(
            "cannot use TLS min version from tlsMinVersion={tls_min_version:?}. Supported TLS versions (TLS10, TLS11, TLS12, TLS13): {err}"
        )
    })?;
    let provider = provider_with_cipher_suites(tls_cipher_suites).map_err(|err| {
        format!("cannot use TLS cipher suites from tlsCipherSuites={tls_cipher_suites:?}: {err}")
    })?;
    let config = ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(versions)
        .map_err(|err| format!("cannot set TLS protocol versions: {err}"))?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));
    Ok(Arc::new(config))
}

/// A blocking server-side TLS stream over std TCP.
pub type TlsServerStream = StreamOwned<ServerConnection, TcpStream>;

/// Wraps an accepted TCP connection in server-side TLS and completes the
/// handshake (blocking), like Go's `tls.Server(conn, cfg)` + `Handshake()`.
pub fn server_accept(cfg: &Arc<ServerConfig>, tcp: TcpStream) -> Result<TlsServerStream, String> {
    let conn = ServerConnection::new(Arc::clone(cfg))
        .map_err(|err| format!("cannot create TLS server connection: {err}"))?;
    let mut stream = StreamOwned::new(conn, tcp);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|err| format!("TLS handshake failed: {err}"))?;
    }
    Ok(stream)
}
