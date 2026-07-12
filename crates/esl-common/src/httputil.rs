//! Port of Softalink LLC `lib/httputil` request helper functions
//! (`array.go`, `bool.go`, `int.go`, `url.go`).
//!
//! These read values from an incoming HTTP request, mirroring the Go helpers
//! used by the EsLogs ingestion and query paths (`_msg_field`,
//! `_time_field`, `_stream_fields`, `debug`, ... with header fallbacks such as
//! `ESL-Msg-Field`). They operate on [`crate::httpserver::Request`].

use crate::httpserver::Request;

/// Returns the request value for the given `arg_key` query arg, falling back to
/// the `header_key` header when the query arg is empty.
///
/// Mirrors Go `httputil.GetRequestValue`.
pub fn get_request_value(r: &Request, arg_key: &str, header_key: &str) -> String {
    let v = r.form_value(arg_key);
    if !v.is_empty() {
        return v.to_string();
    }
    r.header(header_key).to_string()
}

/// Returns an array of comma-separated values from the `arg_key` query arg or
/// the `header_key` header.
///
/// Mirrors Go `httputil.GetArray`. Returns an empty vector when the value is
/// empty (Go returns `nil`).
pub fn get_array(r: &Request, arg_key: &str, header_key: &str) -> Vec<String> {
    let v = get_request_value(r, arg_key, header_key);
    if v.is_empty() {
        return Vec::new();
    }
    v.split(',').map(|s| s.to_string()).collect()
}

/// Returns a boolean value from the given `arg_key` query arg.
///
/// Mirrors Go `httputil.GetBool`: empty, `0`, `f`, `false`, `no`
/// (case-insensitive) are false; everything else is true.
pub fn get_bool(r: &Request, arg_key: &str) -> bool {
    let arg_value = r.form_value(arg_key).to_lowercase();
    !matches!(arg_value.as_str(), "" | "0" | "f" | "false" | "no")
}

/// Returns an integer value from the given `arg_key`.
///
/// Mirrors Go `httputil.GetInt`: an empty value yields `0`; a non-parseable
/// value yields an error with the same wording as the Go source.
pub fn get_int(r: &Request, arg_key: &str) -> Result<i64, String> {
    let arg_value = r.form_value(arg_key);
    if arg_value.is_empty() {
        return Ok(0);
    }
    arg_value
        .parse::<i64>()
        .map_err(|e| format!("cannot parse integer {arg_key:?}={arg_value:?}: {e}"))
}

/// Checks whether `url_str` contains a valid URL.
///
/// Mirrors Go `httputil.CheckURL`. Go delegates to `net/url.Parse`, which is
/// extremely permissive; the only hard failures are an empty string and control
/// characters, so this port checks those two conditions.
pub fn check_url(url_str: &str) -> Result<(), String> {
    if url_str.is_empty() {
        return Err("url cannot be empty".to_string());
    }
    if url_str.chars().any(|c| c.is_control()) {
        return Err(format!(
            "failed to parse url {url_str:?}: contains control characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::httpserver::Request;

    #[test]
    fn test_get_request_value_prefers_query_over_header() {
        let r = Request::new_test(
            "POST",
            "/insert?_time_field=@timestamp",
            "1.2.3.4:5",
            &[("ESL-Time-Field", "ignored")],
        );
        assert_eq!(
            get_request_value(&r, "_time_field", "ESL-Time-Field"),
            "@timestamp"
        );
    }

    #[test]
    fn test_get_request_value_falls_back_to_header() {
        let r = Request::new_test("POST", "/insert", "1.2.3.4:5", &[("ESL-Time-Field", "ts")]);
        assert_eq!(get_request_value(&r, "_time_field", "ESL-Time-Field"), "ts");
        assert_eq!(get_request_value(&r, "_time_field", "ESL-Missing"), "");
    }

    #[test]
    fn test_get_array() {
        let r = Request::new_test("POST", "/insert", "h:1", &[("ESL-Stream-Fields", "a,b,c")]);
        assert_eq!(
            get_array(&r, "_stream_fields", "ESL-Stream-Fields"),
            vec!["a", "b", "c"]
        );

        let empty = Request::new_test("POST", "/insert", "h:1", &[]);
        assert!(get_array(&empty, "_stream_fields", "ESL-Stream-Fields").is_empty());
    }

    #[test]
    fn test_get_bool_debug_flag() {
        let on = Request::new_test("GET", "/q?debug=1", "h:1", &[]);
        assert!(get_bool(&on, "debug"));
        let off = Request::new_test("GET", "/q?debug=false", "h:1", &[]);
        assert!(!get_bool(&off, "debug"));
        let unset = Request::new_test("GET", "/q", "h:1", &[]);
        assert!(!get_bool(&unset, "debug"));
    }

    #[test]
    fn test_get_int() {
        let r = Request::new_test("GET", "/q?limit=100", "h:1", &[]);
        assert_eq!(get_int(&r, "limit").unwrap(), 100);
        let empty = Request::new_test("GET", "/q", "h:1", &[]);
        assert_eq!(get_int(&empty, "limit").unwrap(), 0);
        let bad = Request::new_test("GET", "/q?limit=xx", "h:1", &[]);
        assert!(
            get_int(&bad, "limit")
                .unwrap_err()
                .contains("cannot parse integer")
        );
    }

    #[test]
    fn test_check_url() {
        assert!(check_url("http://localhost:8428/insert").is_ok());
        assert!(check_url("").unwrap_err().contains("empty"));
        assert!(check_url("http://x/\u{0007}").is_err());
    }
}
