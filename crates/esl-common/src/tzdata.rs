//! Minimal IANA timezone support by reading the system zoneinfo database
//! (TZif files, RFC 8536), the same source Go's `time.LoadLocation` uses on
//! Unix. No external crate dependency — the OS ships `/usr/share/zoneinfo`.
//!
//! Only what the log/syslog timestamp paths need is implemented: loading a
//! named zone and computing its UTC offset at a given instant (so DST is
//! honored per timestamp). The trailing POSIX-TZ string (used for instants
//! beyond the last explicit transition — typically past ~2037) is not parsed;
//! such far-future instants use the last transition's offset. That matches Go
//! closely for present-day timestamps, which every explicit transition table
//! covers.
//!
//! PORT NOTE: Windows has no `/usr/share/zoneinfo`; [`Location::load`] there
//! only resolves `UTC`. Go embeds tzdata on Windows via `time/tzdata`; the port
//! does not bundle it.

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
}

impl Location {
    /// Returns the zone name (as requested).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// UTC offset in seconds (east positive) in effect at Unix instant `t`.
    pub fn offset_at_secs(&self, t: i64) -> i32 {
        if self.trans.is_empty() {
            return self.types.get(self.default_type).map_or(0, |ti| ti.utoff);
        }
        let idx = match self.trans.binary_search(&t) {
            Ok(i) => self.trans_type[i] as usize,
            // Before the first transition: use the default (first standard) type.
            Err(0) => self.default_type,
            // Interval [trans[i-1], trans[i]).
            Err(i) => self.trans_type[i - 1] as usize,
        };
        self.types.get(idx).map_or(0, |ti| ti.utoff)
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
        #[cfg(not(unix))]
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
        parse_block(&data[block_off..], &v2, 8, name)
    } else {
        parse_block(&data[44..], &v1, 4, name)
    }
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
    })
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
    fn test_fixed_offset_zone() {
        let Ok(z) = Location::load("Etc/GMT+3") else {
            return;
        };
        // Etc/GMT+3 is UTC-3 (POSIX sign convention), with no DST.
        assert_eq!(z.offset_at_secs(1_610_712_000), -3 * 3600);
        assert_eq!(z.offset_at_secs(1_626_350_400), -3 * 3600);
    }
}
