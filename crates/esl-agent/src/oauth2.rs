//! OAuth2 `client_credentials` token source for the eslagent remote-write
//! client (`-remoteWrite.oauth2.*` flags).
//!
//! This is a faithful port of what `golang.org/x/oauth2/clientcredentials`
//! does inside VictoriaMetrics `lib/promauth` when a `-remoteWrite.url` is
//! configured with OAuth2 credentials (see
//! `app/vlagent/remotewrite/client.go` `getAuthConfig` and
//! `lib/promauth/config.go` `newOAuth2ConfigInternal` / `getTokenSource`).
//!
//! Flow:
//!   1. POST `grant_type=client_credentials` (plus optional `scope` and any
//!      `endpointParams`) to the token URL, form-encoded, with the client
//!      credentials in an HTTP Basic `Authorization` header.
//!   2. Parse `access_token` / `token_type` / `expires_in` from the JSON
//!      response body.
//!   3. Cache the resulting bearer header until it is about to expire, then
//!      re-fetch on demand.
//!
//! PORT NOTES / approximations vs Go:
//!   * Credentials are sent in the Basic `Authorization` header as
//!     `base64(url.QueryEscape(clientID):url.QueryEscape(clientSecret))`,
//!     exactly like x/oauth2's `AuthStyleInHeader` (see `basic_auth_header`).
//!     x/oauth2 defaults to `AuthStyleAutoDetect`, which probes the header
//!     style first, so this is the style used in the common case; the
//!     body-credentials fallback (`AuthStyleInParams`) is not implemented.
//!   * `expires_in` is honored as x/oauth2 does: absent/zero means "no
//!     expiry" (re-fetch only on demand). We refresh `EXPIRY_DELTA` (10s,
//!     x/oauth2's `defaultExpiryDelta`) before the real expiry.
//!   * The token endpoint uses default TLS (system CA), matching Go, which
//!     does not apply the `-remoteWrite.tls*` flags to the OAuth2 endpoint.
//!   * The clientSecretFile is re-read on every token fetch, matching Go
//!     re-reading the secret before each request so a rotated file is used.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use esl_storage::http_client::{AuthConfig, do_request_with_timeout};

/// Refresh margin before a token's real expiry (x/oauth2 `defaultExpiryDelta`).
const EXPIRY_DELTA: Duration = Duration::from_secs(10);

/// A cached bearer `Authorization` header value plus its expiry deadline.
///
/// `expiry == None` means the token never expires on its own (the server did
/// not return `expires_in`), matching x/oauth2's treatment of a zero expiry.
struct CachedToken {
    /// Ready-to-send `Authorization` value, e.g. `"Bearer abc123"`.
    auth_header: String,
    expiry: Option<Instant>,
}

/// A thread-safe OAuth2 `client_credentials` token source shared by all
/// remote-write workers of a single `-remoteWrite.url`.
pub struct Oauth2TokenSource {
    /// `host:port` of the token URL.
    token_addr: String,
    /// Path plus query of the token URL, as sent on the request line.
    token_path_and_query: String,
    /// Carries the token endpoint TLS config (`tls()`); the Basic credentials
    /// are built directly from the raw client id/secret below so they can be
    /// `url.QueryEscape`d before base64, matching x/oauth2's header auth.
    auth_cfg: AuthConfig,
    /// OAuth2 client id.
    client_id: String,
    /// OAuth2 client secret value (empty when `client_secret_file` is used).
    client_secret: String,
    /// Path to a file holding the client secret (re-read per token fetch, so a
    /// rotated file is picked up — matches Go).
    client_secret_file: String,
    scopes: Vec<String>,
    endpoint_params: BTreeMap<String, String>,
    send_timeout: Duration,
    cache: Mutex<Option<CachedToken>>,
}

impl Oauth2TokenSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        token_addr: String,
        token_path_and_query: String,
        auth_cfg: AuthConfig,
        client_id: String,
        client_secret: String,
        client_secret_file: String,
        scopes: Vec<String>,
        endpoint_params: BTreeMap<String, String>,
        send_timeout: Duration,
    ) -> Self {
        Self {
            token_addr,
            token_path_and_query,
            auth_cfg,
            client_id,
            client_secret,
            client_secret_file,
            scopes,
            endpoint_params,
            send_timeout,
            cache: Mutex::new(None),
        }
    }

    /// Builds the token-request `Authorization: Basic` header, escaping the
    /// credentials with `url.QueryEscape` before base64 exactly like x/oauth2's
    /// `AuthStyleInHeader`. The client secret is re-read from its file (if any)
    /// on every call so a rotated secret is used.
    fn basic_auth_header(&self) -> Result<String, String> {
        let secret = if !self.client_secret.is_empty() {
            self.client_secret.clone()
        } else if !self.client_secret_file.is_empty() {
            std::fs::read_to_string(&self.client_secret_file)
                .map_err(|err| {
                    format!(
                        "cannot read -remoteWrite.oauth2.clientSecretFile={:?}: {err}",
                        self.client_secret_file
                    )
                })?
                .trim_end()
                .to_string()
        } else {
            String::new()
        };
        let creds = format!(
            "{}:{}",
            query_escape(&self.client_id),
            query_escape(&secret)
        );
        Ok(format!(
            "Basic {}",
            esl_storage::http_client::base64_std_encode(creds.as_bytes())
        ))
    }

    /// Returns a ready-to-send `Authorization` header value (e.g.
    /// `"Bearer <access_token>"`), fetching and caching a fresh token when the
    /// cached one is missing or about to expire.
    ///
    /// The cache mutex is held across the (rare) HTTP fetch; token fetches are
    /// infrequent enough that this is simpler than releasing and re-checking.
    pub fn get_token(&self) -> Result<String, String> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(ct) = cache.as_ref()
            && token_is_valid(ct.expiry, Instant::now(), EXPIRY_DELTA)
        {
            return Ok(ct.auth_header.clone());
        }
        let fresh = self.fetch_token()?;
        let header = fresh.auth_header.clone();
        *cache = Some(fresh);
        Ok(header)
    }

    /// Performs the token endpoint round trip and parses the response.
    fn fetch_token(&self) -> Result<CachedToken, String> {
        // Basic credentials (QueryEscape'd, re-reading the clientSecretFile).
        let basic_header = self.basic_auth_header()?;
        let body = build_token_request_body(&self.scopes, &self.endpoint_params);
        let headers = vec![
            (
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            ),
            ("Authorization".to_string(), basic_header),
        ];
        let resp = do_request_with_timeout(
            &self.token_addr,
            self.auth_cfg.tls(),
            "POST",
            &self.token_path_and_query,
            &headers,
            Some(body.as_bytes()),
            self.send_timeout,
            // PORT NOTE: the OAuth2 token request is sent directly, not through
            // `-remoteWrite.proxyURL`. Go routes it through the same transport
            // (so the proxy applies), but the token endpoint is typically the
            // provider's public IdP rather than the proxied remote-write target;
            // keeping it direct avoids threading the proxy through the token
            // source for a rarely-combined configuration.
            None,
        )?;
        if resp.status_code / 100 != 2 {
            return Err(format!(
                "unexpected status code {} from OAuth2 token endpoint; response body: {}",
                resp.status_code,
                String::from_utf8_lossy(&resp.body)
            ));
        }
        let parsed = parse_token_response(&resp.body)?;
        let token_type = if parsed.token_type.is_empty() {
            "Bearer".to_string()
        } else {
            parsed.token_type
        };
        let auth_header = format!("{token_type} {}", parsed.access_token);
        let expiry = parsed
            .expires_in
            .filter(|&s| s > 0)
            .map(|s| Instant::now() + Duration::from_secs(s as u64));
        Ok(CachedToken {
            auth_header,
            expiry,
        })
    }
}

/// Returns true when a cached token with the given expiry is still usable at
/// `now`, keeping a `delta` refresh margin (x/oauth2 `Token.expired`).
fn token_is_valid(expiry: Option<Instant>, now: Instant, delta: Duration) -> bool {
    match expiry {
        None => true,
        Some(exp) => now + delta < exp,
    }
}

/// Builds the `application/x-www-form-urlencoded` token request body.
///
/// Mirrors `clientcredentials.Config.tokenSource` + `url.Values.Encode`:
/// `grant_type=client_credentials`, an optional `scope` (space-joined), plus
/// any endpoint params; keys are sorted and both keys and values are
/// query-escaped. Endpoint params may override `grant_type`/`scope`, matching
/// Go's ordering where they are applied last.
fn build_token_request_body(
    scopes: &[String],
    endpoint_params: &BTreeMap<String, String>,
) -> String {
    let mut params: BTreeMap<String, String> = BTreeMap::new();
    params.insert("grant_type".to_string(), "client_credentials".to_string());
    // Go: `if len(c.Scopes) > 0 { v.Set("scope", strings.Join(c.Scopes, " ")) }`.
    // Note strings.Split("", ";") yields [""], so an unset scopes flag still
    // adds an empty `scope=`; we replicate that exactly.
    if !scopes.is_empty() {
        params.insert("scope".to_string(), scopes.join(" "));
    }
    for (k, v) in endpoint_params {
        params.insert(k.clone(), v.clone());
    }
    params
        .iter()
        .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-escapes `s` like Go `url.QueryEscape` (space becomes `+`).
fn query_escape(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// The fields extracted from an OAuth2 token endpoint JSON response.
#[derive(Debug, PartialEq)]
pub struct ParsedToken {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<i64>,
}

/// Parses the token endpoint JSON response body, extracting `access_token`
/// (required), `token_type` (optional) and `expires_in` (optional; accepts a
/// JSON number or a numeric string, as some servers return it as a string).
pub fn parse_token_response(body: &[u8]) -> Result<ParsedToken, String> {
    let s = std::str::from_utf8(body)
        .map_err(|_| "OAuth2 token response is not valid UTF-8".to_string())?;
    let mut p = Json {
        b: s.as_bytes(),
        i: 0,
    };
    let obj = match p.parse_value()? {
        JsonVal::Obj(o) => o,
        _ => return Err("OAuth2 token response is not a JSON object".to_string()),
    };
    p.skip_ws();
    if p.i != p.b.len() {
        return Err("unexpected trailing data in OAuth2 token response".to_string());
    }

    let mut access_token: Option<String> = None;
    let mut token_type = String::new();
    let mut expires_in: Option<i64> = None;
    for (k, v) in obj {
        match k.as_str() {
            "access_token" => {
                if let JsonVal::Str(s) = v {
                    access_token = Some(s);
                }
            }
            "token_type" => {
                if let JsonVal::Str(s) = v {
                    token_type = s;
                }
            }
            "expires_in" => match v {
                JsonVal::Num(n) => expires_in = Some(n as i64),
                JsonVal::Str(s) => expires_in = s.trim().parse::<i64>().ok(),
                _ => {}
            },
            _ => {}
        }
    }

    let access_token = access_token
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "OAuth2 token response is missing the access_token field".to_string())?;
    Ok(ParsedToken {
        access_token,
        token_type,
        expires_in,
    })
}

// ---------------------------------------------------------------------------
// Minimal JSON value parser
//
// A small recursive-descent parser, enough to read the token response object
// robustly (skipping any nested/unknown fields). Kept local to avoid pulling
// in a serde dependency, matching the hand-rolled parser in
// `esl_common::flagutil::parse_json_map`.
// ---------------------------------------------------------------------------

enum JsonVal {
    Str(String),
    Num(f64),
    Obj(Vec<(String, JsonVal)>),
    // Bool/Null/Arr are parsed only to advance past unread fields, so they
    // carry no payload (the token response fields we read are strings/numbers).
    Bool,
    Null,
    Arr,
}

struct Json<'a> {
    b: &'a [u8],
    i: usize,
}

impl Json<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn parse_value(&mut self) -> Result<JsonVal, String> {
        self.skip_ws();
        let c = *self
            .b
            .get(self.i)
            .ok_or_else(|| "unexpected end of JSON".to_string())?;
        match c {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => Ok(JsonVal::Str(self.parse_string()?)),
            b't' | b'f' => self.parse_bool(),
            b'n' => self.parse_null(),
            b'-' | b'0'..=b'9' => self.parse_number(),
            other => Err(format!(
                "unexpected byte {:?} at position {} in JSON",
                other as char, self.i
            )),
        }
    }

    fn parse_object(&mut self) -> Result<JsonVal, String> {
        self.i += 1; // consume '{'
        let mut out = Vec::new();
        self.skip_ws();
        if self.b.get(self.i) == Some(&b'}') {
            self.i += 1;
            return Ok(JsonVal::Obj(out));
        }
        loop {
            self.skip_ws();
            if self.b.get(self.i) != Some(&b'"') {
                return Err(format!(
                    "expecting string key at position {} in JSON",
                    self.i
                ));
            }
            let key = self.parse_string()?;
            self.skip_ws();
            if self.b.get(self.i) != Some(&b':') {
                return Err(format!("expecting ':' at position {} in JSON", self.i));
            }
            self.i += 1;
            let val = self.parse_value()?;
            out.push((key, val));
            self.skip_ws();
            match self.b.get(self.i) {
                Some(&b',') => {
                    self.i += 1;
                }
                Some(&b'}') => {
                    self.i += 1;
                    return Ok(JsonVal::Obj(out));
                }
                _ => {
                    return Err(format!(
                        "expecting ',' or '}}' at position {} in JSON",
                        self.i
                    ));
                }
            }
        }
    }

    fn parse_array(&mut self) -> Result<JsonVal, String> {
        self.i += 1; // consume '['
        self.skip_ws();
        if self.b.get(self.i) == Some(&b']') {
            self.i += 1;
            return Ok(JsonVal::Arr);
        }
        loop {
            // Parse (and discard) each element to advance the cursor.
            self.parse_value()?;
            self.skip_ws();
            match self.b.get(self.i) {
                Some(&b',') => {
                    self.i += 1;
                }
                Some(&b']') => {
                    self.i += 1;
                    return Ok(JsonVal::Arr);
                }
                _ => {
                    return Err(format!(
                        "expecting ',' or ']' at position {} in JSON",
                        self.i
                    ));
                }
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.i += 1; // consume opening '"'
        let mut out = String::new();
        while let Some(&c) = self.b.get(self.i) {
            match c {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    let e = *self
                        .b
                        .get(self.i)
                        .ok_or_else(|| "unterminated escape in JSON string".to_string())?;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let cp = self.parse_hex4()?;
                            // Note: lone surrogates are passed through as the
                            // replacement char; token fields are plain ASCII in
                            // practice, so this is sufficient.
                            out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                        }
                        _ => return Err("invalid escape in JSON string".to_string()),
                    }
                    self.i += 1;
                }
                _ => {
                    // Copy a UTF-8 continuation run verbatim.
                    let start = self.i;
                    while let Some(&b) = self.b.get(self.i) {
                        if b == b'"' || b == b'\\' {
                            break;
                        }
                        self.i += 1;
                    }
                    out.push_str(
                        std::str::from_utf8(&self.b[start..self.i])
                            .map_err(|_| "invalid UTF-8 in JSON string".to_string())?,
                    );
                }
            }
        }
        Err("unterminated JSON string".to_string())
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut cp = 0u32;
        for _ in 0..4 {
            self.i += 1;
            let h = *self
                .b
                .get(self.i)
                .ok_or_else(|| "truncated \\u escape in JSON string".to_string())?;
            let d = (h as char)
                .to_digit(16)
                .ok_or_else(|| "invalid \\u escape in JSON string".to_string())?;
            cp = cp * 16 + d;
        }
        Ok(cp)
    }

    fn parse_number(&mut self) -> Result<JsonVal, String> {
        let start = self.i;
        while let Some(&c) = self.b.get(self.i) {
            match c {
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => self.i += 1,
                _ => break,
            }
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).unwrap_or("");
        s.parse::<f64>()
            .map(JsonVal::Num)
            .map_err(|_| format!("invalid JSON number {s:?}"))
    }

    fn parse_bool(&mut self) -> Result<JsonVal, String> {
        if self.b[self.i..].starts_with(b"true") {
            self.i += 4;
            Ok(JsonVal::Bool)
        } else if self.b[self.i..].starts_with(b"false") {
            self.i += 5;
            Ok(JsonVal::Bool)
        } else {
            Err(format!("invalid JSON literal at position {}", self.i))
        }
    }

    fn parse_null(&mut self) -> Result<JsonVal, String> {
        if self.b[self.i..].starts_with(b"null") {
            self.i += 4;
            Ok(JsonVal::Null)
        } else {
            Err(format!("invalid JSON literal at position {}", self.i))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use esl_storage::http_client::Options;

    #[test]
    fn parses_access_token_with_numeric_expires_in() {
        let body = br#"{"access_token":"abc123","token_type":"Bearer","expires_in":3600}"#;
        let t = parse_token_response(body).unwrap();
        assert_eq!(
            t,
            ParsedToken {
                access_token: "abc123".to_string(),
                token_type: "Bearer".to_string(),
                expires_in: Some(3600),
            }
        );
    }

    #[test]
    fn parses_expires_in_given_as_string() {
        let body = br#"{"access_token":"tok","expires_in":"7200"}"#;
        let t = parse_token_response(body).unwrap();
        assert_eq!(t.access_token, "tok");
        assert_eq!(t.expires_in, Some(7200));
        // token_type absent -> empty here; defaulted to "Bearer" at fetch time.
        assert_eq!(t.token_type, "");
    }

    #[test]
    fn parses_token_without_expires_in() {
        let body = br#"{"access_token":"tok","token_type":"bearer"}"#;
        let t = parse_token_response(body).unwrap();
        assert_eq!(t.access_token, "tok");
        assert_eq!(t.token_type, "bearer");
        assert_eq!(t.expires_in, None);
    }

    #[test]
    fn ignores_unknown_and_nested_fields() {
        let body =
            br#"{"scope":"a b","access_token":"tok","extra":{"nested":[1,2,3]},"expires_in":60}"#;
        let t = parse_token_response(body).unwrap();
        assert_eq!(t.access_token, "tok");
        assert_eq!(t.expires_in, Some(60));
    }

    #[test]
    fn rejects_missing_access_token() {
        let body = br#"{"token_type":"Bearer","expires_in":60}"#;
        let err = parse_token_response(body).unwrap_err();
        assert!(err.contains("missing the access_token"), "{err}");
    }

    #[test]
    fn rejects_non_object_body() {
        assert!(parse_token_response(b"[1,2,3]").is_err());
        assert!(parse_token_response(b"not json").is_err());
    }

    #[test]
    fn decodes_string_escapes() {
        let body = br#"{"access_token":"a\/bA"}"#;
        let t = parse_token_response(body).unwrap();
        assert_eq!(t.access_token, "a/bA");
    }

    #[test]
    fn builds_form_body_with_scopes_and_endpoint_params() {
        let scopes = vec!["read".to_string(), "write".to_string()];
        let mut params = BTreeMap::new();
        params.insert(
            "audience".to_string(),
            "https://api.example.com".to_string(),
        );
        params.insert("resource".to_string(), "a b".to_string());
        let body = build_token_request_body(&scopes, &params);
        // Keys sorted; values query-escaped; scope is space-joined.
        assert_eq!(
            body,
            "audience=https%3A%2F%2Fapi.example.com&grant_type=client_credentials&resource=a+b&scope=read+write"
        );
    }

    #[test]
    fn builds_form_body_replicates_go_empty_scope_quirk() {
        // strings.Split("", ";") => [""], so an unset scopes flag still adds an
        // empty scope= param (Go-faithful behavior).
        let scopes = vec!["".to_string()];
        let body = build_token_request_body(&scopes, &BTreeMap::new());
        assert_eq!(body, "grant_type=client_credentials&scope=");
    }

    #[test]
    fn builds_form_body_without_scopes() {
        let body = build_token_request_body(&[], &BTreeMap::new());
        assert_eq!(body, "grant_type=client_credentials");
    }

    fn test_source(client_id: &str, client_secret: &str) -> Oauth2TokenSource {
        let ac = Options::default().new_config().unwrap();
        Oauth2TokenSource::new(
            "203.0.113.1:1".to_string(),
            "/token".to_string(),
            ac,
            client_id.to_string(),
            client_secret.to_string(),
            String::new(),
            Vec::new(),
            BTreeMap::new(),
            Duration::from_millis(50),
        )
    }

    #[test]
    fn basic_auth_header_matches_expected_base64() {
        // Plain URL-safe credentials: base64("my-client:s3cr3t").
        let src = test_source("my-client", "s3cr3t");
        assert_eq!(
            src.basic_auth_header().unwrap(),
            "Basic bXktY2xpZW50OnMzY3IzdA=="
        );
        // Credentials with RFC3986-reserved bytes are url.QueryEscape'd before
        // base64, exactly like x/oauth2's AuthStyleInHeader. QueryEscape maps
        // ' ' -> '+', ':' -> '%3A', so "a b" / "p:q" -> base64("a+b:p%3Aq").
        let src = test_source("a b", "p:q");
        let creds = format!("{}:{}", "a+b", "p%3Aq");
        let want = format!(
            "Basic {}",
            esl_storage::http_client::base64_std_encode(creds.as_bytes())
        );
        assert_eq!(src.basic_auth_header().unwrap(), want);
    }

    #[test]
    fn cached_valid_token_is_returned_without_fetching() {
        // A source pointing at an unroutable addr; a live (non-expiring)
        // cached token must be returned without any HTTP fetch.
        let src = test_source("id", "sec");
        *src.cache.lock().unwrap() = Some(CachedToken {
            auth_header: "Bearer cached".to_string(),
            expiry: Some(Instant::now() + Duration::from_secs(3600)),
        });
        assert_eq!(src.get_token().unwrap(), "Bearer cached");
    }

    #[test]
    fn expired_token_triggers_a_fetch() {
        // With an already-expired cached token, get_token must attempt a fetch,
        // which fails against the unroutable/blackhole token endpoint.
        let src = test_source("id", "sec");
        *src.cache.lock().unwrap() = Some(CachedToken {
            auth_header: "Bearer stale".to_string(),
            expiry: Some(Instant::now() - Duration::from_secs(1)),
        });
        assert!(src.get_token().is_err(), "expired token must force a fetch");
    }

    #[test]
    fn token_validity_respects_refresh_margin() {
        let now = Instant::now();
        let delta = Duration::from_secs(10);
        // No expiry => always valid.
        assert!(token_is_valid(None, now, delta));
        // Expires well beyond the margin => valid.
        assert!(token_is_valid(
            Some(now + Duration::from_secs(60)),
            now,
            delta
        ));
        // Within the refresh margin => invalid (should refresh).
        assert!(!token_is_valid(
            Some(now + Duration::from_secs(5)),
            now,
            delta
        ));
        // Already past => invalid.
        assert!(!token_is_valid(
            Some(now - Duration::from_secs(1)),
            now,
            delta
        ));
    }
}
