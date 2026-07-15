//! Minimal IANA timezone support by parsing TZif files (RFC 8536), the same
//! format Go's `time.LoadLocation` reads. On Unix the bytes come from the
//! system zoneinfo database (`/usr/share/zoneinfo`), matching Go with no extra
//! dependency; on Windows, which ships no system zoneinfo, they come from the
//! bundled `tzdb_data` crate and go through the very same parser, so offsets
//! are computed identically on both platforms.
//!
//! Only what the log/syslog timestamp paths need is implemented: loading a
//! named zone and computing its UTC offset at a given instant (so DST is
//! honored per timestamp). The trailing POSIX-TZ footer string IS parsed (Go
//! `tzset`) and applied to instants at or after the last explicit transition,
//! so "slim" TZif files — whose explicit transitions stop early and delegate
//! the recurring rule to that footer — and far-future instants on any file are
//! resolved correctly. Leap seconds are ignored (as in Go's wall-clock path).
//!
//! PORT NOTE: Go resolves named zones on Windows from its embedded `time/tzdata`
//! (or `$GOROOT/.../zoneinfo.zip`); the port mirrors that with the bundled
//! `tzdb_data` (a pinned IANA release). On Unix the system database wins, so
//! Unix behavior is byte-for-byte the system tzdata like Go. Residual: the
//! Windows lookup is case-insensitive (`tzdb_data::find_raw`) where a Unix file
//! lookup is case-sensitive.

/// Directories searched for zoneinfo files, mirroring Go's `zoneSources`
/// (minus the Android/embedded ones).
#[cfg(unix)]
const ZONE_DIRS: &[&str] = &[
    "/usr/share/zoneinfo/",
    "/usr/share/lib/zoneinfo/",
    "/usr/lib/locale/TZ/",
    "/etc/zoneinfo/",
];

/// One local-time type (Go `zone`): its offset east of UTC and DST flag.
#[derive(Debug, Clone, Copy)]
struct Ttinfo {
    utoff: i32,
    isdst: bool,
}

/// A loaded IANA timezone.
#[derive(Debug, Clone)]
pub struct Location {
    name: String,
    /// Transition instants (Unix seconds), strictly increasing.
    trans: Vec<i64>,
    /// Index into `types` for the interval starting at `trans[i]`.
    trans_type: Vec<u8>,
    types: Vec<Ttinfo>,
    /// Type used for instants before the first transition (first non-DST type).
    default_type: usize,
    /// Parsed trailing POSIX-TZ footer (v2/v3 files), applied to instants at or
    /// after the last explicit transition — Go's `Location.extend`/`tzset`.
    /// Required for correctness on "slim" TZif files, whose explicit
    /// transitions stop early and rely on this recurring rule.
    extend: Option<TzExtend>,
}

/// A parsed POSIX-TZ footer rule (Go `tzset`). Offsets are seconds east of UTC.
#[derive(Debug, Clone)]
struct TzExtend {
    std_offset: i32,
    /// `None` => the footer has no DST rule (constant `std_offset`).
    dst: Option<TzDst>,
}

#[derive(Debug, Clone)]
struct TzDst {
    dst_offset: i32,
    start: TzRule,
    end: TzRule,
}

/// A DST start/end rule from a POSIX-TZ footer (Go `rule`).
#[derive(Debug, Clone)]
struct TzRule {
    kind: TzRuleKind,
    /// Transition wall-clock time, seconds after midnight (default 02:00:00).
    time: i32,
}

#[derive(Debug, Clone)]
enum TzRuleKind {
    /// `Jn`: day 1..365, Feb 29 never counted.
    Julian(i32),
    /// `n`: day 0..365, Feb 29 counted.
    DayOfYear(i32),
    /// `Mm.w.d`: month 1..12, week 1..5, weekday 0..6 (Sunday=0).
    MonthWeekDay { mon: i32, week: i32, day: i32 },
}

impl Location {
    /// Returns the zone name (as requested).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// UTC offset in seconds (east positive) in effect at Unix instant `t`.
    pub fn offset_at_secs(&self, t: i64) -> i32 {
        if self.trans.is_empty() {
            // No transitions: a bare footer rule (if any) governs all instants,
            // else the default type.
            if let Some(ext) = &self.extend {
                return ext.offset_at(t);
            }
            return self.types.get(self.default_type).map_or(0, |ti| ti.utoff);
        }
        let idx = match self.trans.binary_search(&t) {
            Ok(i) => self.trans_type[i] as usize,
            // Before the first transition: use the default (first standard) type.
            Err(0) => self.default_type,
            // Interval [trans[i-1], trans[i]).
            Err(i) => self.trans_type[i - 1] as usize,
        };
        // At or after the last explicit transition, the POSIX-TZ footer takes
        // over (Go `lookup`: `lo == len(tx)-1 && extend != ""`). This is what
        // makes "slim" TZif files — whose explicit transitions stop early —
        // resolve correctly for present-day and future instants.
        if t >= *self.trans.last().unwrap()
            && let Some(ext) = &self.extend
        {
            return ext.offset_at(t);
        }
        self.types.get(idx).map_or(0, |ti| ti.utoff)
    }

    /// UTC offset (seconds, east positive) for a *wall-clock* instant expressed
    /// as if it were UTC (`wall_naive_secs`), resolving DST like Go's
    /// `time.Date(..., loc)`: look up the offset at the naive instant, correct
    /// the instant by it, and re-look-up — one correction converges for every
    /// real transition. During a DST gap/overlap this picks the post-correction
    /// offset, matching Go's normalization.
    pub fn offset_for_wall_secs(&self, wall_naive_secs: i64) -> i32 {
        let off1 = self.offset_at_secs(wall_naive_secs);
        self.offset_at_secs(wall_naive_secs - off1 as i64)
    }

    /// Loads the named IANA zone, like Go `time.LoadLocation`.
    ///
    /// `UTC`/`""` resolve to a fixed-zero location without touching the disk;
    /// `Local` is not handled here (callers sample the OS offset separately).
    pub fn load(name: &str) -> Result<Location, String> {
        if name.is_empty() || name == "UTC" {
            return Ok(Location {
                name: "UTC".to_string(),
                trans: Vec::new(),
                trans_type: Vec::new(),
                types: vec![Ttinfo {
                    utoff: 0,
                    isdst: false,
                }],
                default_type: 0,
                extend: None,
            });
        }
        if !is_valid_zone_name(name) {
            return Err(format!("invalid timezone name {name:?}"));
        }

        #[cfg(unix)]
        {
            for dir in ZONE_DIRS {
                let path = format!("{dir}{name}");
                if let Ok(data) = std::fs::read(&path) {
                    return parse_tzif(&data, name)
                        .map_err(|err| format!("cannot parse timezone file {path:?}: {err}"));
                }
            }
            Err(format!(
                "unknown time zone {name}: no zoneinfo file found under {ZONE_DIRS:?}"
            ))
        }
        #[cfg(windows)]
        {
            // Windows has no system zoneinfo database, so named IANA zones are
            // resolved from the bundled `tzdb_data` and parsed with the same
            // `parse_tzif` as the Unix path — offsets are computed identically.
            // Go's `time.LoadLocation` likewise reads a platform tzdata source
            // (the embedded `time/tzdata` or `$GOROOT/.../zoneinfo.zip`).
            match tzdb_data::find_raw(name.as_bytes()) {
                Some(data) => parse_tzif(data, name)
                    .map_err(|err| format!("cannot parse bundled timezone {name:?}: {err}")),
                None => Err(format!(
                    "unknown time zone {name}: not found in the bundled tzdb ({} release)",
                    tzdb_data::VERSION
                )),
            }
        }
        #[cfg(all(not(unix), not(windows)))]
        {
            Err(format!(
                "unknown time zone {name}: named IANA zones need the system zoneinfo database, \
                 which is unavailable on this platform (only UTC and Local are supported)"
            ))
        }
    }
}

/// Rejects names that could escape the zoneinfo directory or aren't plausible
/// IANA zone identifiers (Go's `LoadLocation` similarly rejects these).
fn is_valid_zone_name(name: &str) -> bool {
    if name.starts_with('/') || name.contains("..") {
        return false;
    }
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-' | b'+' | b'.'))
        && !name.is_empty()
}

/// Parses a TZif (RFC 8536) file into a [`Location`]. Uses the 64-bit v2/v3
/// data block when present (it covers instants past 2038), else the 32-bit v1
/// block.
fn parse_tzif(data: &[u8], name: &str) -> Result<Location, String> {
    if data.len() < 44 || &data[0..4] != b"TZif" {
        return Err("not a TZif file".to_string());
    }
    let version = data[4];

    // v1 header counts live at bytes 20..44 (six big-endian u32).
    let v1 = Header::parse(&data[20..44])?;

    if version == b'2' || version == b'3' {
        // Skip the v1 data block, then the second (v2) header, then parse the
        // 64-bit v2 data block.
        let v1_block_len = v1.block_len(4);
        let v2_header_off = 44 + v1_block_len;
        let counts_off = v2_header_off.checked_add(20).ok_or("truncated v2 header")?;
        if counts_off + 24 > data.len() {
            return Err("truncated v2 header".to_string());
        }
        let v2 = Header::parse(&data[counts_off..counts_off + 24])?;
        let block_off = v2_header_off + 44;
        let mut loc = parse_block(&data[block_off..], &v2, 8, name)?;
        // The v2/v3 data block is followed by a `\n<POSIX-TZ>\n` footer.
        loc.extend = parse_footer(&data[block_off + v2.block_len(8)..]);
        Ok(loc)
    } else {
        parse_block(&data[44..], &v1, 4, name)
    }
}

/// Extracts and parses the trailing `\n<POSIX-TZ>\n` footer of a v2/v3 TZif file
/// (Go's `Location.extend`). Returns `None` if absent or unparseable — callers
/// then fall back to the last explicit transition, as before.
fn parse_footer(rest: &[u8]) -> Option<TzExtend> {
    // The footer is framed by newlines: skip the leading '\n', take up to the
    // trailing '\n'.
    let start = rest.iter().position(|&b| b == b'\n')? + 1;
    let end = start + rest[start..].iter().position(|&b| b == b'\n')?;
    let s = core::str::from_utf8(&rest[start..end]).ok()?;
    if s.is_empty() {
        return None;
    }
    parse_extend(s)
}

/// The six counts in a TZif header.
struct Header {
    isutcnt: usize,
    isstdcnt: usize,
    leapcnt: usize,
    timecnt: usize,
    typecnt: usize,
    charcnt: usize,
}

impl Header {
    fn parse(b: &[u8]) -> Result<Header, String> {
        if b.len() < 24 {
            return Err("truncated header".to_string());
        }
        let u = |i: usize| u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]) as usize;
        Ok(Header {
            isutcnt: u(0),
            isstdcnt: u(4),
            leapcnt: u(8),
            timecnt: u(12),
            typecnt: u(16),
            charcnt: u(20),
        })
    }

    /// Byte length of the data block following this header, where `time_size`
    /// is 4 (v1) or 8 (v2/v3) for transition and leap-second times.
    fn block_len(&self, time_size: usize) -> usize {
        self.timecnt * time_size            // transition times
            + self.timecnt                  // transition type indices
            + self.typecnt * 6              // ttinfo records
            + self.charcnt                  // abbreviation chars
            + self.leapcnt * (time_size + 4) // leap-second records
            + self.isstdcnt                 // standard/wall indicators
            + self.isutcnt // UT/local indicators
    }
}

/// Parses a TZif data block (v1 with `time_size==4` or v2/v3 with `8`).
fn parse_block(b: &[u8], h: &Header, time_size: usize, name: &str) -> Result<Location, String> {
    if b.len() < h.block_len(time_size) {
        return Err("truncated data block".to_string());
    }
    let mut off = 0;

    // Transition times.
    let mut trans = Vec::with_capacity(h.timecnt);
    for _ in 0..h.timecnt {
        let t = if time_size == 8 {
            i64::from_be_bytes(b[off..off + 8].try_into().unwrap())
        } else {
            i32::from_be_bytes(b[off..off + 4].try_into().unwrap()) as i64
        };
        trans.push(t);
        off += time_size;
    }

    // Transition type indices.
    let trans_type = b[off..off + h.timecnt].to_vec();
    off += h.timecnt;

    // ttinfo records: int32 utoff, uint8 isdst, uint8 abbrev index.
    let mut types = Vec::with_capacity(h.typecnt);
    for _ in 0..h.typecnt {
        let utoff = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
        let isdst = b[off + 4] != 0;
        types.push(Ttinfo { utoff, isdst });
        off += 6;
    }

    if types.is_empty() {
        return Err("no local-time types".to_string());
    }
    // Validate transition type indices point at a real ttinfo.
    if trans_type.iter().any(|&i| i as usize >= types.len()) {
        return Err("transition references unknown type".to_string());
    }

    // Type for instants before the first transition: first non-DST type,
    // else the first type (Go `tzset`/`LoadLocation` convention).
    let default_type = types.iter().position(|t| !t.isdst).unwrap_or(0);

    Ok(Location {
        name: name.to_string(),
        trans,
        trans_type,
        types,
        default_type,
        extend: None,
    })
}

const SECONDS_PER_HOUR: i32 = 3600;
const SECONDS_PER_DAY: i32 = 86_400;
/// Cumulative days before month `m` (1-based) in a non-leap year.
const DAYS_BEFORE: [i32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];

impl TzExtend {
    /// UTC offset (seconds east) at Unix instant `sec`, from the POSIX-TZ
    /// footer rule — Go `tzset`, reduced to the offset the callers need.
    fn offset_at(&self, sec: i64) -> i32 {
        let Some(dst) = &self.dst else {
            return self.std_offset;
        };
        let (year, yday) = year_yday(sec);
        let ysec =
            (yday - 1) as i64 * SECONDS_PER_DAY as i64 + sec.rem_euclid(SECONDS_PER_DAY as i64);
        let mut std_off = self.std_offset;
        let mut dst_off = dst.dst_offset;
        // start rule uses the pre-transition (std) offset; end rule uses dst.
        let mut start_sec = tzrule_time(year, &dst.start, std_off) as i64;
        let mut end_sec = tzrule_time(year, &dst.end, dst_off) as i64;
        // Southern hemisphere: DST spans the year boundary, so end < start.
        if end_sec < start_sec {
            core::mem::swap(&mut start_sec, &mut end_sec);
            core::mem::swap(&mut std_off, &mut dst_off);
        }
        if ysec < start_sec || ysec >= end_sec {
            std_off
        } else {
            dst_off
        }
    }
}

/// Parses a POSIX-TZ footer string (Go `tzset`, parse half). Returns `None` on
/// any malformed input, so callers fall back to explicit transitions.
fn parse_extend(s: &str) -> Option<TzExtend> {
    let b = s.as_bytes();
    let (_std_name, rest) = tzset_name(b)?;
    let (std_offset, rest) = tzset_offset(rest)?;
    // POSIX offsets are added to local time to get UTC; ours are east-of-UTC,
    // so negate (Go does the same).
    let std_offset = -std_offset;
    if rest.is_empty() || rest[0] == b',' {
        return Some(TzExtend {
            std_offset,
            dst: None,
        });
    }
    let (_dst_name, rest) = tzset_name(rest)?;
    let (dst_offset, rest) = if rest.is_empty() || rest[0] == b',' {
        (std_offset + SECONDS_PER_HOUR, rest)
    } else {
        let (o, r) = tzset_offset(rest)?;
        (-o, r)
    };
    // Default DST rules per tzcode when the string omits them.
    let rest: &[u8] = if rest.is_empty() {
        b",M3.2.0,M11.1.0"
    } else {
        rest
    };
    // tzcode also accepts ';' as the separator.
    if rest[0] != b',' && rest[0] != b';' {
        return None;
    }
    let (start, rest) = tzset_rule(&rest[1..])?;
    if rest.is_empty() || rest[0] != b',' {
        return None;
    }
    let (end, rest) = tzset_rule(&rest[1..])?;
    if !rest.is_empty() {
        return None;
    }
    Some(TzExtend {
        std_offset,
        dst: Some(TzDst {
            dst_offset,
            start,
            end,
        }),
    })
}

/// Go `tzsetName`: zone name (bare, ≥3 chars, or `<...>` quoted) + remainder.
fn tzset_name(s: &[u8]) -> Option<(&[u8], &[u8])> {
    if s.is_empty() {
        return None;
    }
    if s[0] != b'<' {
        for (i, &c) in s.iter().enumerate() {
            if matches!(c, b'0'..=b'9' | b',' | b'-' | b'+') {
                if i < 3 {
                    return None;
                }
                return Some((&s[..i], &s[i..]));
            }
        }
        if s.len() < 3 {
            return None;
        }
        Some((s, &s[s.len()..]))
    } else {
        let close = s.iter().position(|&c| c == b'>')?;
        Some((&s[1..close], &s[close + 1..]))
    }
}

/// Go `tzsetOffset`: `[+|-]hh[:mm[:ss]]` in seconds (POSIX sign) + remainder.
fn tzset_offset(s: &[u8]) -> Option<(i32, &[u8])> {
    if s.is_empty() {
        return None;
    }
    let (neg, mut s) = match s[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    // tzcode permits up to 24*7 hours (POSIX allows only 24).
    let (hours, r) = tzset_num(s, 0, 24 * 7)?;
    s = r;
    let mut off = hours * SECONDS_PER_HOUR;
    if !s.is_empty() && s[0] == b':' {
        let (mins, r) = tzset_num(&s[1..], 0, 59)?;
        s = r;
        off += mins * 60;
        if !s.is_empty() && s[0] == b':' {
            let (secs, r) = tzset_num(&s[1..], 0, 59)?;
            s = r;
            off += secs;
        }
    }
    Some((if neg { -off } else { off }, s))
}

/// Go `tzsetRule`: a `Jn` / `n` / `Mm.w.d` rule with optional `/time`.
fn tzset_rule(s: &[u8]) -> Option<(TzRule, &[u8])> {
    if s.is_empty() {
        return None;
    }
    let (kind, s) = if s[0] == b'J' {
        let (jday, s) = tzset_num(&s[1..], 1, 365)?;
        (TzRuleKind::Julian(jday), s)
    } else if s[0] == b'M' {
        let (mon, s) = tzset_num(&s[1..], 1, 12)?;
        if s.is_empty() || s[0] != b'.' {
            return None;
        }
        let (week, s) = tzset_num(&s[1..], 1, 5)?;
        if s.is_empty() || s[0] != b'.' {
            return None;
        }
        let (day, s) = tzset_num(&s[1..], 0, 6)?;
        (TzRuleKind::MonthWeekDay { mon, week, day }, s)
    } else {
        let (day, s) = tzset_num(s, 0, 365)?;
        (TzRuleKind::DayOfYear(day), s)
    };
    if s.is_empty() || s[0] != b'/' {
        return Some((
            TzRule {
                kind,
                time: 2 * SECONDS_PER_HOUR,
            },
            s,
        ));
    }
    let (time, s) = tzset_offset(&s[1..])?;
    Some((TzRule { kind, time }, s))
}

/// Go `tzsetNum`: a decimal in `[min, max]` + remainder.
fn tzset_num(s: &[u8], min: i32, max: i32) -> Option<(i32, &[u8])> {
    if s.is_empty() {
        return None;
    }
    let mut num = 0i32;
    for (i, &c) in s.iter().enumerate() {
        if !c.is_ascii_digit() {
            if i == 0 || num < min {
                return None;
            }
            return Some((num, &s[i..]));
        }
        num = num * 10 + (c - b'0') as i32;
        if num > max {
            return None;
        }
    }
    if num < min {
        return None;
    }
    Some((num, &s[s.len()..]))
}

/// Go `tzruleTime`: seconds after the start of `year` at which rule `r` fires,
/// given the offset `off` in effect just before it.
fn tzrule_time(year: i32, r: &TzRule, off: i32) -> i32 {
    let s = match r.kind {
        TzRuleKind::Julian(day) => {
            let mut s = (day - 1) * SECONDS_PER_DAY;
            if is_leap(year) && day >= 60 {
                s += SECONDS_PER_DAY;
            }
            s
        }
        TzRuleKind::DayOfYear(day) => day * SECONDS_PER_DAY,
        TzRuleKind::MonthWeekDay { mon, week, day } => {
            // Zeller's congruence: weekday (Sun=0) of the first day of `mon`.
            let m1 = (mon + 9) % 12 + 1;
            let mut yy0 = year;
            if mon <= 2 {
                yy0 -= 1;
            }
            let yy1 = yy0 / 100;
            let yy2 = yy0 % 100;
            let mut dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if dow < 0 {
                dow += 7;
            }
            // 0-based day-of-month of the first `day`-of-week, advanced to week.
            let mut d = day - dow;
            if d < 0 {
                d += 7;
            }
            let mut i = 1;
            while i < week {
                if d + 7 >= days_in_month(mon, year) {
                    break;
                }
                d += 7;
                i += 1;
            }
            d += DAYS_BEFORE[(mon - 1) as usize];
            if is_leap(year) && mon > 2 {
                d += 1;
            }
            d * SECONDS_PER_DAY
        }
    };
    s + r.time - off
}

fn is_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(mon: i32, year: i32) -> i32 {
    if mon == 2 && is_leap(year) {
        29
    } else {
        DAYS_BEFORE[mon as usize] - DAYS_BEFORE[(mon - 1) as usize]
    }
}

/// `(year, day-of-year 1-based)` for a Unix instant. Uses Hinnant's
/// days→civil-date algorithm (proleptic Gregorian, no external dependency).
fn year_yday(sec: i64) -> (i32, i32) {
    let days = sec.div_euclid(SECONDS_PER_DAY as i64);
    let (y, m, d) = civil_from_days(days);
    let mut yday = DAYS_BEFORE[(m - 1) as usize] + d;
    if is_leap(y) && m > 2 {
        yday += 1;
    }
    (y, yday)
}

/// Days since 1970-01-01 → `(year, month 1-12, day 1-31)` (Howard Hinnant).
fn civil_from_days(z: i64) -> (i32, i32, i32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as i32; // [1, 12]
    (if m <= 2 { y + 1 } else { y } as i32, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_no_disk() {
        let utc = Location::load("UTC").unwrap();
        assert_eq!(utc.offset_at_secs(1_700_000_000), 0);
        assert_eq!(Location::load("").unwrap().name(), "UTC");
    }

    #[test]
    fn test_invalid_names_rejected() {
        assert!(Location::load("../etc/passwd").is_err());
        assert!(Location::load("/etc/passwd").is_err());
        assert!(!is_valid_zone_name("a/../b"));
        assert!(is_valid_zone_name("America/New_York"));
        assert!(is_valid_zone_name("Etc/GMT+3"));
    }

    #[cfg(unix)]
    #[test]
    fn test_new_york_dst_offsets() {
        // Skip gracefully if the system lacks the zone (minimal containers).
        let Ok(ny) = Location::load("America/New_York") else {
            return;
        };
        // 2021-01-15T12:00:00Z -> EST (UTC-5).
        assert_eq!(ny.offset_at_secs(1_610_712_000), -5 * 3600);
        // 2021-07-15T12:00:00Z -> EDT (UTC-4).
        assert_eq!(ny.offset_at_secs(1_626_350_400), -4 * 3600);
    }

    #[cfg(unix)]
    #[test]
    fn test_offset_for_wall_dst() {
        let Ok(ny) = Location::load("America/New_York") else {
            return;
        };
        // 2021-01-15 12:00:00 wall time in New York -> EST (UTC-5).
        let jan_wall = 1_610_712_000; // 2021-01-15T12:00:00 treated as UTC seconds
        assert_eq!(ny.offset_for_wall_secs(jan_wall), -5 * 3600);
        // 2021-07-15 12:00:00 wall time in New York -> EDT (UTC-4).
        let jul_wall = 1_626_350_400;
        assert_eq!(ny.offset_for_wall_secs(jul_wall), -4 * 3600);
    }

    // Exercises the bundled-tzdata path used in production on Windows: resolve
    // raw TZif bytes from `tzdb_data` and parse them with the same `parse_tzif`
    // as the Unix path. `tzdb_data` is a Windows dependency and a Unix
    // dev-dependency, so this runs on both CI platforms. Uses historically
    // fixed offsets, so it is independent of the bundled IANA release.
    #[cfg(any(unix, windows))]
    #[test]
    fn test_bundled_tzdata_offsets() {
        let load = |z: &str| {
            let raw = tzdb_data::find_raw(z.as_bytes()).expect("zone in bundled tzdb");
            parse_tzif(raw, z).expect("parse bundled TZif")
        };
        let ny = load("America/New_York");
        assert_eq!(ny.offset_at_secs(1_610_712_000), -5 * 3600); // 2021-01 EST
        assert_eq!(ny.offset_at_secs(1_626_350_400), -4 * 3600); // 2021-07 EDT
        assert_eq!(ny.offset_for_wall_secs(1_626_350_400), -4 * 3600);
        assert!(tzdb_data::find_raw(b"No/Such_Zone").is_none());

        // Far-future instants (beyond every explicit transition) must come from
        // the POSIX-TZ footer — the whole point of parsing it.
        let jan40 = 2_210_241_600; // 2040-01-15 12:00 UTC
        let jul40 = 2_225_966_400; // 2040-07-15 12:00 UTC
        assert_eq!(ny.offset_at_secs(jan40), -5 * 3600); // EST
        assert_eq!(ny.offset_at_secs(jul40), -4 * 3600); // EDT

        // Southern hemisphere: DST wraps the year boundary, so the footer's end
        // rule fires before the start rule (exercises the swap). Sydney is on
        // AEDT (+11) in austral summer (January) and AEST (+10) in winter.
        let syd = load("Australia/Sydney");
        assert_eq!(syd.offset_at_secs(jan40), 11 * 3600); // AEDT
        assert_eq!(syd.offset_at_secs(jul40), 10 * 3600); // AEST

        // A zone with no DST: the footer is a bare offset (constant far-future).
        let kolkata = load("Asia/Kolkata");
        assert_eq!(kolkata.offset_at_secs(jan40), 5 * 3600 + 30 * 60); // +05:30
        assert_eq!(kolkata.offset_at_secs(jul40), 5 * 3600 + 30 * 60);

        // Unit-check the footer parser directly.
        let ext = parse_extend("EST5EDT,M3.2.0,M11.1.0").expect("parse footer");
        assert_eq!(ext.std_offset, -5 * 3600);
        assert_eq!(ext.dst.as_ref().unwrap().dst_offset, -4 * 3600);
        assert!(parse_extend("IST-5:30").unwrap().dst.is_none());

        // On Windows this bundled path IS what `Location::load` runs, so assert
        // the named zone now loads end-to-end (the closed gap).
        #[cfg(windows)]
        {
            let via_load = Location::load("America/New_York").expect("named zone on windows");
            assert_eq!(via_load.offset_at_secs(1_610_712_000), -5 * 3600);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_fixed_offset_zone() {
        let Ok(z) = Location::load("Etc/GMT+3") else {
            return;
        };
        // Etc/GMT+3 is UTC-3 (POSIX sign convention), with no DST.
        assert_eq!(z.offset_at_secs(1_610_712_000), -3 * 3600);
        assert_eq!(z.offset_at_secs(1_626_350_400), -3 * 3600);
    }
}
