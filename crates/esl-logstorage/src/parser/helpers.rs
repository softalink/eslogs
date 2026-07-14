//! Shared parse helpers for the LogsQL grammar (numbers, durations, time, IP
//! ranges, field lists) — ports of the free helper functions in `parser.go`,
//! `pipe_stats.go`, `pipe_fields.go`, `pipe_sort.go`, `pipe_len.go`.

use esl_common::timeutil;

use crate::filter_range::parse_math_number;
use crate::log_rows::get_canonical_column_name;
use crate::parser::go_quote;
use crate::parser::lexer_ext::LexerExt;
use crate::prefix_filter;
use crate::stream_filter::Lexer;
use crate::values_encoder::{
    try_parse_bytes, try_parse_duration, try_parse_ipv4, try_parse_uint64,
};

const NSECS_PER_DAY: i64 = 24 * 3600 * 1_000_000_000;

/// Port of Go `SubInt64NoOverflow`.
pub(crate) fn sub_int64_no_overflow(a: i64, b: i64) -> i64 {
    if b >= 0 {
        if a == i64::MAX {
            return a;
        }
        if a < i64::MIN.wrapping_add(b) {
            return i64::MIN;
        }
        return a - b;
    }
    if a == i64::MIN {
        return a;
    }
    if a > i64::MAX.wrapping_add(b) {
        return i64::MAX;
    }
    a - b
}

/// Port of Go `nextafter(f, xInf)` — steps `f` one ULP toward `x_inf`.
pub(crate) fn nextafter(f: f64, x_inf: f64) -> f64 {
    if f.is_infinite() {
        return f;
    }
    // Rust std lacks nextafter; replicate the IEEE-754 next-representable step.
    if f == x_inf {
        return f;
    }
    if f.is_nan() || x_inf.is_nan() {
        return f64::NAN;
    }
    let bits = f.to_bits();
    let next = if f == 0.0 {
        // smallest subnormal toward x_inf
        if x_inf > 0.0 { 1 } else { (1u64 << 63) | 1 }
    } else if (f < x_inf) == (f > 0.0) {
        bits + 1
    } else {
        bits - 1
    };
    f64::from_bits(next)
}

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

/// Reads a compound token and tries to interpret it as a float.
/// Returns `Ok((maybe_value, token))`; `maybe_value` is `None` when the token
/// is not a number. Returns `Err` only when the token can't be read.
pub(crate) fn read_number(lex: &mut Lexer) -> Result<(Option<f64>, String), String> {
    let s = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read number: {e}"))?;
    let f = parse_math_number(&s);
    if !f.is_nan() || s.eq_ignore_ascii_case("nan") {
        return Ok((Some(f), s));
    }
    Ok((None, s))
}

/// Port of Go `parseNumber`.
pub(crate) fn parse_number(lex: &mut Lexer) -> Result<(f64, String), String> {
    match read_number(lex)? {
        (Some(f), s) => Ok((f, s)),
        (None, s) => Err(format!("cannot parse {} as float64", go_quote(&s))),
    }
}

/// Port of Go `parseUint`.
pub(crate) fn parse_uint(s: &str) -> Result<u64, String> {
    if s.eq_ignore_ascii_case("inf") || s.eq_ignore_ascii_case("+inf") {
        return Ok(u64::MAX);
    }
    if let Some(n) = parse_uint64_base0(s) {
        return Ok(n);
    }
    let nn = try_parse_bytes(s)
        .or_else(|| try_parse_duration(s))
        .ok_or_else(|| format!("cannot parse {} as unsigned integer", go_quote(s)))?;
    if nn < 0 {
        return Err(format!(
            "cannot parse negative value {} as unsigned integer",
            go_quote(s)
        ));
    }
    Ok(nn as u64)
}

/// Port of Go `strconv.ParseUint(s, 0, 64)` (base auto-detection).
fn parse_uint64_base0(s: &str) -> Option<u64> {
    let (radix, digits) = if let Some(r) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        (16, r)
    } else if let Some(r) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        (8, r)
    } else if let Some(r) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        (2, r)
    } else {
        (10, s)
    };
    let digits = digits.replace('_', "");
    if digits.is_empty() {
        return None;
    }
    u64::from_str_radix(&digits, radix).ok()
}

// ---------------------------------------------------------------------------
// Duration & time
// ---------------------------------------------------------------------------

/// Port of Go `parseDuration`.
pub(crate) fn parse_duration(lex: &mut Lexer) -> Result<(i64, String), String> {
    let s = lex.next_compound_token()?;
    match try_parse_duration(&s) {
        Some(d) => Ok((d, s)),
        None => Err(format!("cannot parse duration {}", go_quote(&s))),
    }
}

/// Port of Go `parseTimeOffset`.
pub(crate) fn parse_time_offset(lex: &mut Lexer) -> Result<(i64, String), String> {
    if !lex.is_keyword(&["offset"]) {
        return Err(format!(
            "unexpected token {}; want 'offset'",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let (d, s) = parse_duration(lex).map_err(|e| format!("cannot parse duration: {e}"))?;
    Ok((d, format!("offset {s}")))
}

/// Port of Go `parseTime`.
pub(crate) fn parse_time(lex: &mut Lexer) -> Result<(i64, String), String> {
    let s = lex.next_compound_token()?;
    let nsecs = timeutil::parse_time_at(&s, lex.current_timestamp())?;
    Ok((nsecs, s))
}

/// Port of Go `isLikelyTimestamp`.
pub(crate) fn is_likely_timestamp(lex: &Lexer) -> bool {
    lex.is_keyword(&["now"]) || starts_with_year(&lex.token)
}

/// Port of Go `startsWithYear`.
pub(crate) fn starts_with_year(s: &str) -> bool {
    if s.len() < 4 {
        return false;
    }
    let b = s.as_bytes();
    if !b[..4].iter().all(|c| c.is_ascii_digit()) {
        return false;
    }
    if s.len() == 4 {
        return true;
    }
    let c = b[4];
    c == b'-' || c == b'+' || c == b'Z' || c == b'z'
}

pub(crate) fn is_all_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit())
}

fn strip_timezone_suffix(s: &str) -> &str {
    if let Some(stripped) = s.strip_suffix('Z') {
        return stripped;
    }
    if s.len() < 6 {
        return s;
    }
    let tz = &s.as_bytes()[s.len() - 6..];
    if tz[0] != b'-' && tz[0] != b'+' {
        return s;
    }
    if tz[3] != b':' {
        return s;
    }
    &s[..s.len() - 6]
}

// Civil-date helpers (Howard Hinnant's algorithms), std-only, for the YYYY /
// YYYY-MM end-of-range calendar arithmetic in `adjust_end_timestamp`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Port of Go `adjustEndTimestamp`.
pub(crate) fn adjust_end_timestamp(t: i64, t_str: &str) -> i64 {
    let t_str = strip_timezone_suffix(t_str);

    let add = |nanos: i64| -> i64 { t + nanos - 1 };

    if t_str.len() == 4 {
        // Year only, e.g. "2025": add a full year.
        let days = t.div_euclid(NSECS_PER_DAY);
        let nsec = t.rem_euclid(NSECS_PER_DAY);
        let (y, m, d) = civil_from_days(days);
        let end_days = days_from_civil(y + 1, m, d);
        return end_days * NSECS_PER_DAY + nsec - 1;
    }

    if is_all_digits(t_str) {
        return match t_str.len() {
            0..=10 => add(1_000_000_000),
            11..=13 => add(1_000_000),
            14..=16 => add(1_000),
            _ => add(1),
        };
    }

    let ymd_dash = t_str.len() > 4 && t_str.as_bytes()[4] == b'-';
    if !ymd_dash {
        // fractional-seconds unix timestamp: <int>.<frac>
        if let Some((before, after)) = t_str.split_once('.')
            && is_all_digits(before)
            && is_all_digits(after)
        {
            return match after.len() {
                3 => add(1_000_000),
                6 => add(1_000),
                _ => add(1),
            };
        }
        // Unknown format: no adjustment (Go returns tEnd.UnixNano() == t).
        return t;
    }

    match t_str.len() {
        7 => {
            // YYYY-MM: add a full month.
            let days = t.div_euclid(NSECS_PER_DAY);
            let nsec = t.rem_euclid(NSECS_PER_DAY);
            let (y, mut m, d) = civil_from_days(days);
            // Go: if d != 1 { d = 0; m++ } then time.Date(y, m+1, d, ...)
            let (mut yy, mut mm, dd) = if d != 1 { (y, m + 1, 0) } else { (y, m, d) };
            mm += 1;
            while mm > 12 {
                mm -= 12;
                yy += 1;
            }
            // d==0 means "last day of previous month"; days_from_civil handles
            // d as offset, so d=0 → day before the 1st.
            let end_days = days_from_civil(yy, mm, dd);
            let _ = &mut m;
            end_days * NSECS_PER_DAY + nsec - 1
        }
        10 => add(24 * 3600 * 1_000_000_000), // YYYY-MM-DD
        13 => add(3600 * 1_000_000_000),      // YYYY-MM-DDThh
        16 => add(60 * 1_000_000_000),        // YYYY-MM-DDThh:mm
        19 => add(1_000_000_000),             // YYYY-MM-DDThh:mm:ss
        23 => add(1_000_000),                 // .SSS
        26 => add(1_000),                     // .SSSSSS
        _ => add(1),
    }
}

// ---------------------------------------------------------------------------
// IP ranges
// ---------------------------------------------------------------------------

/// Port of Go `tryParseIPv4CIDR`.
pub(crate) fn try_parse_ipv4_cidr(s: &str) -> Option<(u32, u32)> {
    match s.split_once('/') {
        None => {
            let n = try_parse_ipv4(s)?;
            Some((n, n))
        }
        Some((before, after)) => {
            let ip = try_parse_ipv4(before)?;
            let mask_bits = try_parse_uint64(after)?;
            if mask_bits > 32 {
                return None;
            }
            let mask: u32 = ((1u64 << (32 - mask_bits)) - 1) as u32;
            Some((ip & !mask, ip | mask))
        }
    }
}

/// Port of Go `tryParseIPv6` (accepts IPv4 mapped to IPv6 too).
pub(crate) fn try_parse_ipv6(s: &str) -> Option<[u8; 16]> {
    if s.len() < 2 || s.len() > 45 {
        return None;
    }
    let addr: std::net::IpAddr = s.parse().ok()?;
    match addr {
        std::net::IpAddr::V6(a) => Some(a.octets()),
        std::net::IpAddr::V4(a) => Some(a.to_ipv6_mapped().octets()),
    }
}

/// Port of Go `tryParseIPv6CIDR`.
pub(crate) fn try_parse_ipv6_cidr(s: &str) -> Option<([u8; 16], [u8; 16])> {
    match s.split_once('/') {
        None => {
            let ip = try_parse_ipv6(s)?;
            Some((ip, ip))
        }
        Some((before, after)) => {
            let ip = try_parse_ipv6(before)?;
            let mask_bits = try_parse_uint64(after)?;
            if mask_bits > 128 {
                return None;
            }
            let mut min_v = ip;
            let mut max_v = ip;
            let mut byte_idx = (mask_bits / 8) as usize;
            let bit_idx = mask_bits % 8;
            if bit_idx > 0 {
                let mask: u8 = 0xffu8 << (8 - bit_idx);
                min_v[byte_idx] &= mask;
                max_v[byte_idx] |= !mask;
                byte_idx += 1;
            }
            while byte_idx < 16 {
                min_v[byte_idx] = 0;
                max_v[byte_idx] = 0xff;
                byte_idx += 1;
            }
            Some((min_v, max_v))
        }
    }
}

/// Port of Go `tryParseHHMM` (private in `values_encoder.rs`; reimplemented for
/// the `day_range` filter). Returns nanosecond offset within a day.
pub(crate) fn try_parse_hhmm(s: &str) -> Option<i64> {
    let (hh, mm) = s.split_once(':')?;
    if hh.len() != 2 || mm.len() != 2 {
        return None;
    }
    let h: i64 = hh.parse().ok()?;
    let m: i64 = mm.parse().ok()?;
    if h > 23 || m > 59 {
        return None;
    }
    Some((h * 3600 + m * 60) * 1_000_000_000)
}

// ---------------------------------------------------------------------------
// Field-name / field-filter helpers (pipe_stats.go / pipe_fields.go / etc.)
// ---------------------------------------------------------------------------

/// Port of Go `parseFieldName`.
pub(crate) fn parse_field_name(lex: &mut Lexer) -> Result<String, String> {
    let field_name = lex.next_compound_token()?;
    Ok(get_canonical_column_name(&field_name).to_string())
}

/// Port of Go `parseFieldFilter`.
pub(crate) fn parse_field_filter(lex: &mut Lexer) -> Result<String, String> {
    if lex.is_keyword(&["*"]) {
        lex.next_token();
        return Ok("*".to_string());
    }
    let mut field_name = lex.next_compound_token()?;
    field_name = get_canonical_column_name(&field_name).to_string();
    if !lex.is_skipped_space() && lex.is_keyword(&["*"]) {
        lex.next_token();
        field_name.push('*');
    }
    Ok(field_name)
}

/// Port of Go `parseCommaSeparatedFields`.
pub(crate) fn parse_comma_separated_fields(lex: &mut Lexer) -> Result<Vec<String>, String> {
    let mut fields = Vec::new();
    loop {
        let field = parse_field_filter(lex).map_err(|e| format!("cannot parse field name: {e}"))?;
        fields.push(field);
        if !lex.is_keyword(&[","]) {
            return Ok(fields);
        }
        lex.next_token();
    }
}

/// Port of Go `parseFieldFiltersInParens`.
pub(crate) fn parse_field_filters_in_parens(lex: &mut Lexer) -> Result<Vec<String>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing `(`".to_string());
    }
    let mut fields = Vec::new();
    loop {
        lex.next_token();
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(fields);
        }
        if lex.is_keyword(&[","]) {
            return Err("unexpected `,`".to_string());
        }
        let field = parse_field_filter(lex)?;
        fields.push(field);
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(fields);
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token: {}; expecting ',' or ')'",
                go_quote(&lex.token)
            ));
        }
    }
}

/// Port of Go `parseFieldNamesInParens`.
pub(crate) fn parse_field_names_in_parens(lex: &mut Lexer) -> Result<Vec<String>, String> {
    let field_names = parse_field_filters_in_parens(lex)?;
    for field_name in &field_names {
        if prefix_filter::is_wildcard_filter(field_name) {
            return Err(format!(
                "the field name {} cannot end with '*'",
                go_quote(field_name)
            ));
        }
    }
    Ok(field_names)
}

/// Port of Go `parseFieldNameWithOptionalParens`.
pub(crate) fn parse_field_name_with_optional_parens(lex: &mut Lexer) -> Result<String, String> {
    let has_parens = lex.is_keyword(&["("]);
    if has_parens {
        lex.next_token();
    }
    let field_name = parse_field_name(lex)?;
    if has_parens {
        if !lex.is_keyword(&[")"]) {
            return Err(format!(
                "missing ')' after '{}'",
                crate::parser::quote_token_if_needed(&field_name)
            ));
        }
        lex.next_token();
    }
    Ok(field_name)
}

/// Port of Go `parseArgsInParens`.
pub(crate) fn parse_args_in_parens(lex: &mut Lexer) -> Result<Vec<String>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();
    let mut args = Vec::new();
    while !lex.is_keyword(&[")"]) {
        if lex.is_keyword(&[","]) {
            return Err("unexpected ','".to_string());
        }
        if lex.is_keyword(&["("]) {
            return Err("unexpected '('".to_string());
        }
        let arg = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse arg: {e}"))?;
        args.push(arg);
        if lex.is_keyword(&[")"]) {
            break;
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "missing ',' after {}; got {} instead",
                go_quote(args.last().unwrap()),
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
    lex.next_token();
    Ok(args)
}

/// Byte form of [`parse_args_in_parens`] for raw-byte phrase payloads:
/// quoted args carry Go-parity raw bytes (`Lexer::token_bytes`); unquoted
/// compound args are slices of the query text (valid UTF-8) in both forms.
pub(crate) fn parse_args_in_parens_bytes(lex: &mut Lexer) -> Result<Vec<Vec<u8>>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();
    let mut args: Vec<Vec<u8>> = Vec::new();
    while !lex.is_keyword(&[")"]) {
        if lex.is_keyword(&[","]) {
            return Err("unexpected ','".to_string());
        }
        if lex.is_keyword(&["("]) {
            return Err("unexpected '('".to_string());
        }
        let arg = lex
            .next_compound_token_bytes()
            .map_err(|e| format!("cannot parse arg: {e}"))?;
        args.push(arg);
        if lex.is_keyword(&[")"]) {
            break;
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "missing ',' after {}; got {} instead",
                // go_quote_bytes: display-only quoting of a raw-byte arg in
                // the error message (Go %q over raw bytes).
                crate::stream_filter::go_quote_bytes(args.last().unwrap()),
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
    lex.next_token();
    Ok(args)
}

/// Port of Go `parseLimit` (pipe_sort.go).
pub(crate) fn parse_limit(lex: &mut Lexer) -> Result<u64, String> {
    if !lex.is_keyword(&["limit"]) {
        return Err(format!("expecting 'limit'; got {}", go_quote(&lex.token)));
    }
    lex.next_token();
    let limit_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot parse 'limit': {e}"))?;
    try_parse_uint64(&limit_str).ok_or_else(|| {
        format!(
            "cannot parse {} as number in the 'limit'",
            go_quote(&limit_str)
        )
    })
}

/// Port of Go `parseOffset` (pipe_sort.go).
pub(crate) fn parse_offset(lex: &mut Lexer) -> Result<u64, String> {
    if !lex.is_keyword(&["offset"]) {
        return Err(format!("expecting 'offset'; got {}", go_quote(&lex.token)));
    }
    lex.next_token();
    let s = lex
        .next_compound_token()
        .map_err(|e| format!("cannot parse 'offset': {e}"))?;
    try_parse_uint64(&s)
        .ok_or_else(|| format!("cannot parse {} as number in the 'offset'", go_quote(&s)))
}

/// Port of Go `getUniqueResultName`.
pub(crate) fn get_unique_result_name(result_name: &str, by_fields: &[String]) -> String {
    let mut result_name = result_name.to_string();
    while by_fields.iter().any(|f| f == &result_name) {
        result_name.push('s');
    }
    result_name
}

/// Port of Go `parseBySortFields`, returning `(name, is_desc)` pairs. Callers
/// map these to their own `bySortField` type (`pipe_sort` vs `stats_json_values`
/// have distinct types).
pub(crate) fn parse_by_sort_fields_raw(lex: &mut Lexer) -> Result<Vec<(String, bool)>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing `(`".to_string());
    }
    let mut bfs = Vec::new();
    loop {
        lex.next_token();
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(bfs);
        }
        let name = parse_field_name(lex).map_err(|e| format!("cannot parse field name: {e}"))?;
        let mut is_desc = false;
        if lex.is_keyword(&["desc"]) {
            lex.next_token();
            is_desc = true;
        } else if lex.is_keyword(&["asc"]) {
            lex.next_token();
        }
        bfs.push((name, is_desc));
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(bfs);
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token: {}; expecting ',' or ')'",
                go_quote(&lex.token)
            ));
        }
    }
}

/// Port of Go `parseRankFieldName`.
pub(crate) fn parse_rank_field_name(lex: &mut Lexer) -> Result<String, String> {
    if !lex.is_keyword(&["rank"]) {
        return Err(format!(
            "unexpected token: {}; want 'rank'",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let mut rank_field_name = "rank".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
        if lex.is_keyword(&["("]) || lex.is_query_part_trailer() {
            return Err("missing rank name".to_string());
        }
    }
    if !lex.is_keyword(&["limit"]) && !lex.is_query_part_trailer() {
        rank_field_name = parse_field_name(lex)?;
    }
    Ok(rank_field_name)
}
