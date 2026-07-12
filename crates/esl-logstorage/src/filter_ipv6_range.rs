//! Port of EsLogs `lib/logstorage/filter_ipv6_range.go`.
//!
//! `FilterIPv6Range` matches values in the IPv6 range `[min_value..max_value]`.

use std::net::{IpAddr, Ipv6Addr};
use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{match_column_by_generic, match_encoded_values_dict, visit_values};
use crate::filter_range::match_ipv4_by_range;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

// ---------------------------------------------------------------------------
// FilterIPv6Range
// ---------------------------------------------------------------------------

/// `FilterIPv6Range` matches the given ipv6 range `[min_value..max_value]`.
///
/// Example LogsQL: `ipv6_range(::1, ::2)`.
pub(crate) struct FilterIPv6Range {
    pub(crate) min_value: [u8; 16],
    pub(crate) max_value: [u8; 16],

    /// Cached ipv4 projection of the range, used to match ipv4-typed columns.
    ///
    /// PORT NOTE: Go uses `sync.Once` + three fields; the port caches the
    /// `Option<(min4, max4)>` in a `OnceLock` (`None` mirrors Go's `isIPv4=false`).
    min_max_ipv4: OnceLock<Option<(u32, u32)>>,
}

/// Builds an ipv6-range filter for `field_name`.
pub(crate) fn new_filter_ipv6_range(
    field_name: &str,
    min_value: [u8; 16],
    max_value: [u8; 16],
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterIPv6Range {
            min_value,
            max_value,
            min_max_ipv4: OnceLock::new(),
        }),
    )
}

impl FilterIPv6Range {
    fn get_min_max_ipv4_values(&self) -> Option<(u32, u32)> {
        *self
            .min_max_ipv4
            .get_or_init(|| self.init_min_max_ipv4_values())
    }

    fn init_min_max_ipv4_values(&self) -> Option<(u32, u32)> {
        let mut min_value6 = self.min_value;
        if ipv6_less(min_value6, MIN_IPV6_FOR_IPV4_VALUE) {
            min_value6 = MIN_IPV6_FOR_IPV4_VALUE;
        }
        let min_value4 = get_ipv4_value_from16(min_value6);

        let mut max_value6 = self.max_value;
        if ipv6_less(MAX_IPV6_FOR_IPV4_VALUE, max_value6) {
            max_value6 = MAX_IPV6_FOR_IPV4_VALUE;
        }
        let max_value4 = get_ipv4_value_from16(max_value6);

        match (min_value4, max_value4) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }
}

impl FieldFilter for FilterIPv6Range {
    fn to_string(&self) -> String {
        let min_value = Ipv6Addr::from(self.min_value);
        let max_value = Ipv6Addr::from(self.max_value);
        format!("ipv6_range({min_value}, {max_value})")
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_ipv6_range(v, self.min_value, self.max_value)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value;
        let max_value = self.max_value;

        if ipv6_less(max_value, min_value) {
            bm.reset_bits();
            return;
        }

        // PORT NOTE: Go's per-`valueType` block-result path matches string/dict/
        // const columns, projects the range onto ipv4 for ipv4 columns, and resets
        // the rest. The port routes all non-const columns through the decoded
        // per-row values with `match_ipv6_range`. This is identical to Go: an
        // ipv4 value `a.b.c.d` decodes to `::ffff:a.b.c.d`, whose ordering matches
        // the ipv4 projection, and non-ip values return false (Go's resets).
        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !match_ipv6_range(v, min_value, max_value) {
                bm.reset_bits();
            }
            return;
        }
        match_column_by_generic(br, bm, r, "", &|v, _| {
            match_ipv6_range(v, min_value, max_value)
        });
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value;
        let max_value = self.max_value;

        if ipv6_less(max_value, min_value) {
            bm.reset_bits();
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_ipv6_range(&v, min_value, max_value) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                bm.reset_bits();
                return;
            }
        };

        match ch.value_type {
            ValueType::STRING => match_string_by_ipv6_range(bs, &ch, bm, min_value, max_value),
            ValueType::DICT => match_values_dict_by_ipv6_range(bs, &ch, bm, min_value, max_value),
            ValueType::IPV4 => match self.get_min_max_ipv4_values() {
                Some((min4, max4)) => match_ipv4_by_range(bs, &ch, bm, min4, max4),
                None => bm.reset_bits(),
            },
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::TIMESTAMP_ISO8601 => bm.reset_bits(),
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// helpers (Go filter_ipv6_range.go)
// ---------------------------------------------------------------------------

fn match_values_dict_by_ipv6_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: [u8; 16],
    max_value: [u8; 16],
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_ipv6_range(v, min_value, max_value)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_ipv6_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: [u8; 16],
    max_value: [u8; 16],
) {
    visit_values(bs, ch, bm, |v| {
        match_ipv6_range(to_unsafe_string(v), min_value, max_value)
    });
}

/// Port of Go `matchIPv6Range`.
pub(crate) fn match_ipv6_range(s: &str, min_value: [u8; 16], max_value: [u8; 16]) -> bool {
    match try_parse_ipv6(s) {
        Some(ip) => !(ipv6_less(ip, min_value) || ipv6_less(max_value, ip)),
        None => false,
    }
}

/// Port of Go `ipv6Less` (lexicographic byte comparison).
pub(crate) fn ipv6_less(a: [u8; 16], b: [u8; 16]) -> bool {
    for i in 0..16 {
        if a[i] < b[i] {
            return true;
        }
        if a[i] > b[i] {
            return false;
        }
    }
    false
}

/// Port of Go `tryParseIPv6` (parser.go): `netip.ParseAddr(s).As16()`.
///
/// PORT NOTE: `tryParseIPv6` lives in Go's `parser.go` (unported); homed here
/// `pub(crate)`. `As16()` maps a parsed IPv4 to its ipv4-mapped ipv6 form, which
/// `Ipv4Addr::to_ipv6_mapped` reproduces exactly.
pub(crate) fn try_parse_ipv6(s: &str) -> Option<[u8; 16]> {
    if s.len() < 2 || s.len() > 45 {
        return None;
    }
    match s.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => Some(v4.to_ipv6_mapped().octets()),
        Ok(IpAddr::V6(v6)) => Some(v6.octets()),
        Err(_) => None,
    }
}

/// Port of Go `getIPv4ValueFrom16`.
fn get_ipv4_value_from16(a: [u8; 16]) -> Option<u32> {
    Ipv6Addr::from(a).to_ipv4_mapped().map(u32::from)
}

// Port of Go `minIPv6ForIPv4Value` / `maxIPv6ForIPv4Value`.
const MIN_IPV6_FOR_IPV4_VALUE: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 0];
const MAX_IPV6_FOR_IPV4_VALUE: [u8; 16] =
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255];

#[cfg(test)]
mod tests {
    use super::*;

    fn ip6(s: &str) -> [u8; 16] {
        try_parse_ipv6(s).unwrap()
    }

    #[test]
    fn test_ipv6_less() {
        assert!(ipv6_less(ip6("::1"), ip6("::2")));
        assert!(!ipv6_less(ip6("::2"), ip6("::1")));
        assert!(!ipv6_less(ip6("::1"), ip6("::1")));
    }

    #[test]
    fn test_match_ipv6_range() {
        let min = ip6("::1");
        let max = ip6("::ff");
        assert!(match_ipv6_range("::1", min, max));
        assert!(match_ipv6_range("::80", min, max));
        assert!(match_ipv6_range("::ff", min, max));
        assert!(!match_ipv6_range("::0", min, max));
        assert!(!match_ipv6_range("::100", min, max));
        assert!(!match_ipv6_range("foobar", min, max));
        assert!(!match_ipv6_range("", min, max));
    }

    #[test]
    fn test_to_string() {
        let f = FilterIPv6Range {
            min_value: ip6("::1"),
            max_value: ip6("::2"),
            min_max_ipv4: OnceLock::new(),
        };
        assert_eq!(f.to_string(), "ipv6_range(::1, ::2)");
    }
}
