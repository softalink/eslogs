//! Embedded esmui web UI assets and their `/select/esmui*` handler.
//!
//! Go embeds the prebuilt UI via `//go:embed esmui` in `app/eslselect/main.go`
//! and serves it with `http.FileServer`; the port embeds the same files from
//! `crates/esl-select/assets/esmui/` (a byte-identical copy of Go
//! `app/eslselect/esmui/`) via `include_bytes!` in a hand-listed table. The
//! asset list is stable per upstream release; `test_asset_table_completeness`
//! walks the assets directory and fails if the table ever goes stale.
//!
//! PORT NOTE — `http.FileServer` semantics are reduced to what the esmui asset
//! tree needs: directory URLs (`/select/esmui/`) serve `index.html`, known
//! files are served with the Content-Type Go's `mime.TypeByExtension` would
//! produce, and unknown paths get Go's `http.NotFound` response. The
//! FileServer niceties that never trigger for these assets (301 canonical
//! redirects for `.../index.html` and subdirectories, Last-Modified/ETag and
//! range requests) are not ported.

use esl_common::httpserver::{Request, ResponseWriter};

/// The embedded esmui assets: (path relative to the esmui root, Content-Type,
/// contents). The Content-Type values mirror Go `mime.TypeByExtension`.
static VMUI_ASSETS: &[(&str, &str, &[u8])] = &[
    (
        "index.html",
        "text/html; charset=utf-8",
        include_bytes!("../assets/esmui/index.html"),
    ),
    (
        "config.json",
        "application/json",
        include_bytes!("../assets/esmui/config.json"),
    ),
    (
        "manifest.json",
        "application/json",
        include_bytes!("../assets/esmui/manifest.json"),
    ),
    (
        "favicon.svg",
        "image/svg+xml",
        include_bytes!("../assets/esmui/favicon.svg"),
    ),
    (
        "preview.jpg",
        "image/jpeg",
        include_bytes!("../assets/esmui/preview.jpg"),
    ),
    (
        "robots.txt",
        "text/plain; charset=utf-8",
        include_bytes!("../assets/esmui/robots.txt"),
    ),
    (
        "assets/config-Mbx-WYSj.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/config-Mbx-WYSj.js"),
    ),
    (
        "assets/downloader-CXDt9uMb.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/downloader-CXDt9uMb.js"),
    ),
    (
        "assets/FileSystemFileHandle-DXAnvdud.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/FileSystemFileHandle-DXAnvdud.js"),
    ),
    (
        "assets/index-BdZaN6k3.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/esmui/assets/index-BdZaN6k3.css"),
    ),
    (
        "assets/index-DiHn3JKq.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/index-DiHn3JKq.js"),
    ),
    (
        "assets/rolldown-runtime-Cyuzqnbw.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/rolldown-runtime-Cyuzqnbw.js"),
    ),
    (
        "assets/vendor-CnsZ1jie.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/esmui/assets/vendor-CnsZ1jie.css"),
    ),
    (
        "assets/vendor-CWtYmzdT.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/esmui/assets/vendor-CWtYmzdT.js"),
    ),
];

/// Looks up an embedded asset by its path relative to the esmui root.
fn get_asset(rel_path: &str) -> Option<(&'static str, &'static [u8])> {
    VMUI_ASSETS
        .iter()
        .find(|(path, _, _)| *path == rel_path)
        .map(|&(_, content_type, data)| (content_type, data))
}

/// Handles `/select/esmui` and `/select/esmui/*` requests (the esmui branches of
/// Go `eslselect.selectHandler`).
///
/// Returns `true` if `req.path()` was a esmui route this handler served, and
/// `false` otherwise.
pub fn request_handler(req: &Request, w: &mut ResponseWriter) -> bool {
    let path = req.path().replace("//", "/");

    if path == "/select/esmui" {
        // VMUI access via incomplete url without `/` in the end.
        // Redirect to complete url. Use relative redirect, since the hostname
        // and path prefix may be incorrect if EsLogs is hidden behind
        // vmauth or similar proxy.
        //
        // Go redirects to `"esmui/?" + r.Form.Encode()` (key-sorted, re-encoded)
        // via `httpserver.Redirect` (relative Location, 302 Found).
        let new_url = format!("esmui/?{}", req.form_encoded());
        w.set_header("Location", &new_url);
        w.set_status(302);
        return true;
    }

    let Some(rel_path) = path.strip_prefix("/select/esmui/") else {
        return false;
    };

    if rel_path.starts_with("static/") {
        // Allow clients caching static contents for long period of time,
        // since it shouldn't change over time. Path to static contents (such
        // as js and css) must be changed whenever its contents is changed.
        // See https://developer.chrome.com/docs/lighthouse/performance/uses-long-cache-ttl/
        //
        // PORT NOTE: kept from Go even though this esmui build ships its
        // hashed bundles under `assets/` rather than `static/`.
        w.set_header("Cache-Control", "max-age=31536000");
    }

    // `http.FileServer` serves the directory index for `/select/esmui/`.
    let rel_path = if rel_path.is_empty() {
        "index.html"
    } else {
        rel_path
    };

    match get_asset(rel_path) {
        Some((content_type, data)) => {
            w.set_header("Content-Type", content_type);
            serve_asset(req, w, data);
        }
        None => {
            // Go `http.NotFound` (via `http.FileServer` on a missing file).
            w.error("404 page not found", 404);
        }
    }
    true
}

/// Serves `data`, honoring a single-range `Range: bytes=...` request with a
/// `206 Partial Content` reply (Go's `http.ServeContent`). `Accept-Ranges:
/// bytes` is always advertised; an unsatisfiable range yields `416`. A
/// multi-range request (comma-separated) falls back to the full `200` body —
/// Go emits `multipart/byteranges`, which browsers never request for these
/// small assets (the single residual).
fn serve_asset(req: &Request, w: &mut ResponseWriter, data: &[u8]) {
    w.set_header("Accept-Ranges", "bytes");
    let range = req.header("Range");
    if range.is_empty() {
        w.write_bytes(data);
        return;
    }
    match parse_single_byte_range(range, data.len()) {
        RangeResult::Full => w.write_bytes(data),
        RangeResult::Partial(start, end) => {
            // end is inclusive (RFC 9110 §14.1.2 byte-range).
            w.set_status(206);
            w.set_header(
                "Content-Range",
                &format!("bytes {start}-{end}/{}", data.len()),
            );
            w.write_bytes(&data[start..=end]);
        }
        RangeResult::Unsatisfiable => {
            w.set_status(416);
            w.set_header("Content-Range", &format!("bytes */{}", data.len()));
        }
    }
}

enum RangeResult {
    /// Serve the whole body with `200` (no range, malformed, or multi-range).
    Full,
    /// Serve `data[start..=end]` with `206`.
    Partial(usize, usize),
    /// `416 Range Not Satisfiable`.
    Unsatisfiable,
}

/// Parses a single-byte-range `Range` header (`bytes=start-end`, `bytes=start-`,
/// or `bytes=-suffix`) against `size`, like Go's `http.parseRange` for one
/// range. Anything else (wrong unit, multiple ranges, malformed) → `Full`.
fn parse_single_byte_range(header: &str, size: usize) -> RangeResult {
    let Some(spec) = header.trim().strip_prefix("bytes=") else {
        return RangeResult::Full;
    };
    let spec = spec.trim();
    if spec.contains(',') {
        // Multi-range: fall back to the full body (see serve_asset docs).
        return RangeResult::Full;
    }
    let Some((start_s, end_s)) = spec.split_once('-') else {
        return RangeResult::Full;
    };
    let (start_s, end_s) = (start_s.trim(), end_s.trim());
    if start_s.is_empty() {
        // Suffix range `-N`: the last N bytes.
        let Ok(suffix) = end_s.parse::<usize>() else {
            return RangeResult::Full;
        };
        if suffix == 0 {
            return RangeResult::Unsatisfiable;
        }
        if size == 0 {
            return RangeResult::Unsatisfiable;
        }
        let start = size.saturating_sub(suffix);
        return RangeResult::Partial(start, size - 1);
    }
    let Ok(start) = start_s.parse::<usize>() else {
        return RangeResult::Full;
    };
    if start >= size {
        return RangeResult::Unsatisfiable;
    }
    let end = if end_s.is_empty() {
        size - 1
    } else {
        match end_s.parse::<usize>() {
            Ok(e) => e.min(size - 1),
            Err(_) => return RangeResult::Full,
        }
    };
    if end < start {
        return RangeResult::Full;
    }
    RangeResult::Partial(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_parse_single_byte_range() {
        use RangeResult::*;
        let m = |h: &str, size: usize| parse_single_byte_range(h, size);
        // Closed range (end inclusive).
        assert!(matches!(m("bytes=0-4", 10), Partial(0, 4)));
        assert!(matches!(m("bytes=2-5", 10), Partial(2, 5)));
        // Open-ended -> to last byte; end clamped to size-1.
        assert!(matches!(m("bytes=3-", 10), Partial(3, 9)));
        assert!(matches!(m("bytes=3-100", 10), Partial(3, 9)));
        // Suffix range -> last N bytes.
        assert!(matches!(m("bytes=-3", 10), Partial(7, 9)));
        assert!(matches!(m("bytes=-100", 10), Partial(0, 9)));
        // Unsatisfiable: start past the end / empty suffix.
        assert!(matches!(m("bytes=10-12", 10), Unsatisfiable));
        assert!(matches!(m("bytes=-0", 10), Unsatisfiable));
        // Not a byte range / multi-range / malformed -> full body.
        assert!(matches!(m("items=0-4", 10), Full));
        assert!(matches!(m("bytes=0-4,6-8", 10), Full));
        assert!(matches!(m("bytes=abc", 10), Full));
        assert!(matches!(m("bytes=5-2", 10), Full));
    }

    /// Walks `assets/esmui` on disk and asserts the embedded table matches it
    /// exactly (every file present, byte-identical, and nothing extra), so a
    /// esmui refresh cannot silently desync the hand-listed table.
    #[test]
    fn test_asset_table_completeness() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/esmui");

        let mut on_disk = Vec::new();
        collect_files(&root, &root, &mut on_disk);
        on_disk.sort();
        assert!(!on_disk.is_empty(), "no files under {}", root.display());

        let mut in_table: Vec<String> = VMUI_ASSETS.iter().map(|(p, _, _)| p.to_string()).collect();
        in_table.sort();

        assert_eq!(
            on_disk, in_table,
            "the VMUI_ASSETS table is out of sync with assets/esmui"
        );

        for (path, content_type, data) in VMUI_ASSETS {
            let disk = std::fs::read(root.join(path)).expect("read asset");
            assert_eq!(&disk, data, "embedded bytes differ for {path}");
            assert!(!content_type.is_empty(), "missing content type for {path}");
        }
    }

    fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("read_dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                collect_files(root, &path, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .expect("strip prefix")
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push(rel);
            }
        }
    }

    /// One raw HTTP GET against a server running [`request_handler`];
    /// returns (status, headers, body).
    fn http_get(addr: std::net::SocketAddr, target: &str) -> (u16, Vec<(String, String)>, Vec<u8>) {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(addr).expect("connect");
        write!(
            stream,
            "GET {target} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
        )
        .expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        let sep = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("headers/body separator");
        let head = String::from_utf8_lossy(&raw[..sep]).into_owned();
        let body = raw[sep + 4..].to_vec();
        let mut lines = head.lines();
        let status: u16 = lines
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("status code");
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        (status, headers, body)
    }

    fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }

    #[test]
    fn test_esmui_serve_semantics() {
        let handle = esl_common::httpserver::serve("127.0.0.1:0", |req, w| {
            if !request_handler(req, w) {
                w.error("not routed", 404);
            }
        })
        .expect("serve");
        let addr = handle.local_addr();

        // `/select/esmui` redirects to the trailing-slash URL, carrying the
        // query string.
        let (status, headers, _) = http_get(addr, "/select/esmui?foo=bar");
        assert_eq!(status, 302);
        assert_eq!(header(&headers, "location"), "esmui/?foo=bar");

        // Directory index → index.html.
        let (status, headers, body) = http_get(addr, "/select/esmui/");
        assert_eq!(status, 200);
        assert_eq!(header(&headers, "content-type"), "text/html; charset=utf-8");
        assert!(!body.is_empty());

        // A hashed bundle with the right Content-Type and exact bytes.
        let (status, headers, body) = http_get(addr, "/select/esmui/assets/index-BdZaN6k3.css");
        assert_eq!(status, 200);
        assert_eq!(header(&headers, "content-type"), "text/css; charset=utf-8");
        assert_eq!(
            body,
            get_asset("assets/index-BdZaN6k3.css").expect("asset").1
        );

        // config.json is a plain asset (no special handling in Go).
        let (status, headers, body) = http_get(addr, "/select/esmui/config.json");
        assert_eq!(status, 200);
        assert_eq!(header(&headers, "content-type"), "application/json");
        assert!(body.starts_with(b"{"));

        // Unknown asset → Go http.NotFound.
        let (status, _, body) = http_get(addr, "/select/esmui/no-such-file.js");
        assert_eq!(status, 404);
        assert!(String::from_utf8_lossy(&body).contains("404 page not found"));

        // The Go static/ branch sets Cache-Control even on a miss.
        let (status, headers, _) = http_get(addr, "/select/esmui/static/missing.js");
        assert_eq!(status, 404);
        assert_eq!(header(&headers, "cache-control"), "max-age=31536000");

        // Non-esmui paths are not routed here.
        let (status, _, body) = http_get(addr, "/select/logsql/query");
        assert_eq!(status, 404);
        assert!(String::from_utf8_lossy(&body).contains("not routed"));

        handle.stop();
    }
}
