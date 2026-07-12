//! Port of `lib/logstorage/tenant_id.go`.

use std::fmt;

use esl_common::encoding;

use crate::u128;

/// TenantID is an id of a tenant for log streams.
///
/// Each log stream is associated with a single TenantID.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TenantID {
    /// AccountID is the id of the account for the log stream.
    pub account_id: u32,

    /// ProjectID is the id of the project for the log stream.
    pub project_id: u32,
}

impl TenantID {
    /// Resets tid.
    pub fn reset(&mut self) {
        self.account_id = 0;
        self.project_id = 0;
    }

    /// Returns true if tid equals to a.
    pub fn equal(&self, a: &TenantID) -> bool {
        self == a
    }

    /// Returns true if tid is less than a.
    pub fn less(&self, a: &TenantID) -> bool {
        if self.account_id != a.account_id {
            return self.account_id < a.account_id;
        }
        self.project_id < a.project_id
    }

    pub fn marshal_string(&self, dst: &mut Vec<u8>) {
        let n = (self.account_id as u64) << 32 | (self.project_id as u64);
        u128::marshal_uint64_hex(dst, n);
    }

    /// Appends the marshaled tid to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_uint32(dst, self.account_id);
        encoding::marshal_uint32(dst, self.project_id);
    }

    /// Unmarshals tid from src and returns the remaining tail.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal tenantID from {} bytes; need at least 8 bytes",
                src.len()
            ));
        }
        self.account_id = encoding::unmarshal_uint32(&src[..4]);
        self.project_id = encoding::unmarshal_uint32(&src[4..]);
        Ok(&src[8..])
    }
}

impl fmt::Display for TenantID {
    /// Returns human-readable representation of tid.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{accountID={},projectID={}}}",
            self.account_id, self.project_id
        )
    }
}

/// Returns tenantID from the AccountID / ProjectID HTTP header values.
///
/// PORT NOTE: Go's GetTenantIDFromRequest takes *http.Request and reads the
/// "AccountID" and "ProjectID" headers; the port takes the raw header values
/// instead (pass "" when a header is missing, which maps to 0 like in Go).
pub fn get_tenant_id_from_request(account_id: &str, project_id: &str) -> Result<TenantID, String> {
    let mut tenant_id = TenantID::default();

    let account_id = get_uint32_from_header(account_id)?;
    let project_id = get_uint32_from_header(project_id)?;

    tenant_id.account_id = account_id;
    tenant_id.project_id = project_id;
    Ok(tenant_id)
}

/// Returns tenantID from s.
///
/// s is expected in the form of accountID:projectID. If s is empty, then zero
/// tenantID is returned.
pub fn parse_tenant_id(s: &str) -> Result<TenantID, String> {
    let mut tenant_id = TenantID::default();
    if s.is_empty() {
        return Ok(tenant_id);
    }

    let (before, after) = match s.split_once(':') {
        None => {
            let account = get_uint32_from_string(s)
                .map_err(|err| format!("cannot parse accountID from {s:?}: {err}"))?;
            tenant_id.account_id = account;

            return Ok(tenant_id);
        }
        Some((before, after)) => (before, after),
    };

    let account = get_uint32_from_string(before)
        .map_err(|err| format!("cannot parse accountID part from {s:?}: {err}"))?;
    tenant_id.account_id = account;

    let project = get_uint32_from_string(after)
        .map_err(|err| format!("cannot parse projectID part from {s:?}: {err}"))?;
    tenant_id.project_id = project;

    Ok(tenant_id)
}

/// Returns JSON representation of the given tenantIDs.
///
/// PORT NOTE: Go uses encoding/json; the port renders the same output by hand
/// (serde is not a dependency). An empty slice marshals to "[]".
pub fn marshal_tenant_ids_to_json(tenant_ids: &[TenantID]) -> Vec<u8> {
    let mut data = Vec::with_capacity(tenant_ids.len() * 40 + 2);
    data.push(b'[');
    for (i, tid) in tenant_ids.iter().enumerate() {
        if i > 0 {
            data.push(b',');
        }
        data.extend_from_slice(
            format!(
                "{{\"account_id\":{},\"project_id\":{}}}",
                tid.account_id, tid.project_id
            )
            .as_bytes(),
        );
    }
    data.push(b']');
    data
}

/// Unmarshals tenantIDs from JSON array at src.
///
/// PORT NOTE: Go uses encoding/json; the port implements a minimal parser for
/// the subset produced by marshal_tenant_ids_to_json (plus whitespace and
/// "null", which unmarshals to an empty list like in Go).
pub fn unmarshal_tenant_ids_from_json(src: &[u8]) -> Result<Vec<TenantID>, String> {
    parse_tenant_ids_json(src)
        .map_err(|err| format!("cannot unmarshal tenantIDs from JSON array: {err}"))
}

fn parse_tenant_ids_json(src: &[u8]) -> Result<Vec<TenantID>, String> {
    let mut p = JsonParser { src, pos: 0 };
    p.skip_ws();
    if p.consume_literal(b"null") {
        p.skip_ws();
        p.expect_eof()?;
        return Ok(Vec::new());
    }
    p.expect(b'[')?;
    let mut tenant_ids = Vec::new();
    p.skip_ws();
    if p.peek() == Some(b']') {
        p.pos += 1;
        p.skip_ws();
        p.expect_eof()?;
        return Ok(tenant_ids);
    }
    loop {
        tenant_ids.push(p.parse_tenant_id()?);
        p.skip_ws();
        match p.next() {
            Some(b',') => p.skip_ws(),
            Some(b']') => break,
            _ => return Err("missing ',' or ']' after object".to_string()),
        }
    }
    p.skip_ws();
    p.expect_eof()?;
    Ok(tenant_ids)
}

struct JsonParser<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.src.len()
            && matches!(self.src[self.pos], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.next() != Some(c) {
            return Err(format!("missing {:?}", c as char));
        }
        Ok(())
    }

    fn expect_eof(&self) -> Result<(), String> {
        if self.pos != self.src.len() {
            return Err("unexpected trailing data".to_string());
        }
        Ok(())
    }

    fn consume_literal(&mut self, lit: &[u8]) -> bool {
        if self.src[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            return true;
        }
        false
    }

    fn parse_tenant_id(&mut self) -> Result<TenantID, String> {
        self.skip_ws();
        self.expect(b'{')?;
        let mut tid = TenantID::default();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(tid);
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.parse_uint32()?;
            match key {
                "account_id" => tid.account_id = value,
                "project_id" => tid.project_id = value,
                _ => return Err(format!("unknown field {key:?}")),
            }
            self.skip_ws();
            match self.next() {
                Some(b',') => continue,
                Some(b'}') => return Ok(tid),
                _ => return Err("missing ',' or '}' after object member".to_string()),
            }
        }
    }

    fn parse_string(&mut self) -> Result<&'a str, String> {
        self.expect(b'"')?;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'"' {
                let s = std::str::from_utf8(&self.src[start..self.pos])
                    .map_err(|_| "invalid UTF-8 in string".to_string())?;
                self.pos += 1;
                return Ok(s);
            }
            if c == b'\\' {
                return Err("escape sequences in strings are not supported".to_string());
            }
            self.pos += 1;
        }
        Err("unterminated string".to_string())
    }

    fn parse_uint32(&mut self) -> Result<u32, String> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err("missing number".to_string());
        }
        let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        s.parse::<u32>()
            .map_err(|err| format!("cannot parse {s:?} as uint32: {err}"))
    }
}

fn get_uint32_from_header(s: &str) -> Result<u32, String> {
    if s.is_empty() {
        return Ok(0);
    }
    get_uint32_from_string(s)
}

fn get_uint32_from_string(s: &str) -> Result<u32, String> {
    if s.is_empty() {
        return Ok(0);
    }
    // PORT NOTE: Go rejects a leading sign in strconv.ParseUint; Rust's
    // u32::from_str accepts '+', so it is rejected explicitly. The error
    // detail wording differs from Go's strconv errors.
    if s.starts_with('+') {
        return Err(format!("cannot parse {s:?} as uint32: invalid syntax"));
    }
    s.parse::<u32>()
        .map_err(|err| format!("cannot parse {s:?} as uint32: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_marshal_unmarshal() {
        fn f(tid: &TenantID) {
            let mut data = Vec::new();
            tid.marshal(&mut data);
            let mut tid2 = TenantID::default();
            let tail = tid2
                .unmarshal(&data)
                .unwrap_or_else(|err| panic!("unexpected error at unmarshal({tid}): {err}"));
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail after unmarshal({tid}): {tail:X?}"
            );
            assert_eq!(
                tid, &tid2,
                "unexpected value after unmarshal; got {tid2}; want {tid}"
            );
            let s1 = tid.to_string();
            let s2 = tid2.to_string();
            assert_eq!(
                s1, s2,
                "unexpected string value after unmarshal; got {s2}; want {s1}"
            );
        }
        f(&TenantID::default());
        f(&TenantID {
            account_id: 123,
            project_id: 456,
        });
    }

    #[test]
    fn test_tenant_id_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut tid = TenantID::default();
            let result = tid.unmarshal(data);
            assert!(result.is_err(), "expecting non-nil error");
        }
        f(&[]);
        f(b"abc");
    }

    #[test]
    fn test_tenant_id_less_equal() {
        // compare equal values
        let tid1 = TenantID::default();
        let tid2 = TenantID::default();
        assert!(!tid1.less(&tid2), "less for equal values must return false");
        assert!(!tid2.less(&tid1), "less for equal values must return false");
        assert!(
            tid1.equal(&tid2),
            "unexpected equal({tid1}, {tid2}) result; got false; want true"
        );
        assert!(
            tid2.equal(&tid1),
            "unexpected equal({tid2}, {tid1}) result; got false; want true"
        );

        let tid1 = TenantID {
            account_id: 123,
            project_id: 456,
        };
        let tid2 = TenantID {
            account_id: 123,
            project_id: 456,
        };
        assert!(!tid1.less(&tid2), "less for equal values must return false");
        assert!(!tid2.less(&tid1), "less for equal values must return false");
        assert!(
            tid1.equal(&tid2),
            "unexpected equal({tid1}, {tid2}) result; got false; want true"
        );
        assert!(
            tid2.equal(&tid1),
            "unexpected equal({tid2}, {tid1}) result; got false; want true"
        );

        // compare unequal values
        let tid1 = TenantID {
            account_id: 0,
            project_id: 456,
        };
        let tid2 = TenantID {
            account_id: 123,
            project_id: 0,
        };
        assert!(
            tid1.less(&tid2),
            "unexpected result for less({tid1}, {tid2}); got false; want true"
        );
        assert!(
            !tid2.less(&tid1),
            "unexpected result for less({tid2}, {tid1}); got true; want false"
        );
        assert!(
            !tid1.equal(&tid2),
            "unexpected result for equal({tid1}, {tid2}); got true; want false"
        );

        let tid1 = TenantID {
            account_id: 123,
            project_id: 0,
        };
        let tid2 = TenantID {
            account_id: 123,
            project_id: 456,
        };
        assert!(
            tid1.less(&tid2),
            "unexpected result for less({tid1}, {tid2}); got false; want true"
        );
        assert!(
            !tid2.less(&tid1),
            "unexpected result for less({tid2}, {tid1}); got true; want false"
        );
        assert!(
            !tid1.equal(&tid2),
            "unexpected result for equal({tid1}, {tid2}); got true; want false"
        );
    }

    #[test]
    fn test_parse_tenant_id() {
        fn f(tenant: &str, expected: TenantID) {
            let got =
                parse_tenant_id(tenant).unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(
                got.to_string(),
                expected.to_string(),
                "expected {expected}, got {got}"
            );
        }

        f("", TenantID::default());
        f(
            "123",
            TenantID {
                account_id: 123,
                project_id: 0,
            },
        );
        f(
            "123:456",
            TenantID {
                account_id: 123,
                project_id: 456,
            },
        );
        f(
            "123:",
            TenantID {
                account_id: 123,
                project_id: 0,
            },
        );
        f(
            ":456",
            TenantID {
                account_id: 0,
                project_id: 456,
            },
        );
    }

    #[test]
    fn test_marshal_unmarshal_tenant_ids_as_json() {
        let tenant_ids = vec![
            TenantID {
                account_id: 0,
                project_id: 0,
            },
            TenantID {
                account_id: 123,
                project_id: 456,
            },
            TenantID {
                account_id: 73249834,
                project_id: 34242123,
            },
        ];
        let data = marshal_tenant_ids_to_json(&tenant_ids);

        let result = unmarshal_tenant_ids_from_json(&data)
            .unwrap_or_else(|err| panic!("unexpected error when unmarshaling tenants: {err}"));
        assert_eq!(
            tenant_ids, result,
            "unexpected tenantIDs after unmarshaling"
        );
    }
}
