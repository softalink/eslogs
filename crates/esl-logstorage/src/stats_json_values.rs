//! Port of `lib/logstorage/stats_json_values.go` — the `json_values(...)` stats
//! function and its base (unsorted) processor.
//!
//! This module is the shared home for helpers reused by the `sorted` and `topk`
//! variants: [`StatsJSONValues`], [`BySortField`], [`marshal_json_values`] and
//! [`get_matching_columns`].
//!
//! PORT NOTE — allocator: Go's `newStatsProcessor(a *chunkedAllocator)` arena is
//! dropped (see `crate::stats`). Processors own their state.
//!
//! PORT NOTE — captured config: Go's `statsProcessor` methods receive the
//! `statsFunc` on every call and downcast it (`sf.(*statsJSONValues)`). The
//! frozen `crate::stats::StatsFunc` trait has no `as_any`, so a `&dyn StatsFunc`
//! cannot be downcast. Instead each processor captures the config it needs
//! (`field_filters`, `sort_fields`, `limit`) at `new_stats_processor` time; the
//! `sf` parameter is accepted but unused.
//!
//! PORT NOTE — `BySortField`: Go defines `bySortField` in `pipe_sort.go`, which
//! is not ported yet. A minimal port lives here (the only current consumer);
//! it should move to the `pipe_sort` port when that lands.
//!
//! PORT NOTE — `marshal_json_values`: Go defines `marshalJSONValues` in
//! `stats_uniq_values.go`; it is provided here (as a `pub(crate)` helper) until
//! that file is ported, since all three `json_values` variants depend on it.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::{BlockResult, ColRef};
use crate::parser::quote_field_filter_if_needed;
use crate::prefix_filter;
use crate::rows::{Field, marshal_fields_to_json};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_json_values_sorted::StatsJSONValuesSortedProcessor;
use crate::stats_json_values_topk::StatsJSONValuesTopkProcessor;

/// A single sort field for `json_values(...) sort by (...)`.
///
/// PORT NOTE: port of Go's `bySortField` (`pipe_sort.go`); see module docs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BySortField {
    /// The name of the field to sort by.
    pub(crate) name: Vec<u8>,
    /// Whether sorting is in descending order.
    pub(crate) is_desc: bool,
}

impl BySortField {
    pub(crate) fn new(name: impl Into<Vec<u8>>, is_desc: bool) -> Self {
        Self {
            name: name.into(),
            is_desc,
        }
    }
}

impl std::fmt::Display for BySortField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::parser::quote_token_bytes_if_needed(&self.name))?;
        if self.is_desc {
            f.write_str(" desc")?;
        }
        Ok(())
    }
}

/// The `json_values(...)` stats function.
///
/// Port of Go's `statsJSONValues`.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StatsJSONValues {
    /// Field filters for fields to select from logs.
    pub(crate) field_filters: Vec<Vec<u8>>,

    /// Optional fields for sorting the selected logs; empty means unsorted.
    pub(crate) sort_fields: Vec<BySortField>,

    /// Optional limit on the number of selected logs; `0` means no limit.
    pub(crate) limit: u64,
}

impl StatsJSONValues {
    /// PORT NOTE: replaces the parser-driven `parseStatsJSONValues`
    /// constructor (deferred until `lexer` is ported). Exposed for the future
    /// parser and for tests.
    pub(crate) fn new(
        field_filters: Vec<Vec<u8>>,
        sort_fields: Vec<BySortField>,
        limit: u64,
    ) -> Self {
        Self {
            field_filters,
            sort_fields,
            limit,
        }
    }
}

impl StatsFunc for StatsJSONValues {
    fn to_string(&self) -> String {
        let mut s = format!("json_values({})", field_names_string(&self.field_filters));

        if !self.sort_fields.is_empty() {
            let a: Vec<String> = self.sort_fields.iter().map(|sf| sf.to_string()).collect();
            s += &format!(" sort by ({})", a.join(", "));
        }

        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filters(&self.field_filters);
        for sf in &self.sort_fields {
            pf.add_allow_filter(&sf.name);
        }
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        let sort_fields_len = self.sort_fields.len();

        if sort_fields_len == 0 {
            return Box::new(StatsJSONValuesProcessor::with_config(
                self.field_filters.clone(),
                self.limit,
            ));
        }

        if self.limit == 0 {
            let mut svp = StatsJSONValuesSortedProcessor::default();
            svp.sort_fields_len = sort_fields_len;
            svp.field_filters = self.field_filters.clone();
            svp.sort_fields = self.sort_fields.clone();
            return Box::new(svp);
        }

        let mut svp = StatsJSONValuesTopkProcessor::default();
        svp.sort_fields_len = sort_fields_len;
        svp.field_filters = self.field_filters.clone();
        svp.sort_fields = self.sort_fields.clone();
        svp.limit = self.limit;
        Box::new(svp)
    }
}

/// The base (unsorted) processor for `json_values(...)`.
///
/// Port of Go's `statsJSONValuesProcessor`.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct StatsJSONValuesProcessor {
    pub(crate) entries: Vec<Vec<u8>>,

    fields_buf: Vec<Field>,

    // Captured config (see module docs).
    field_filters: Vec<Vec<u8>>,
    limit: u64,
}

impl StatsJSONValuesProcessor {
    /// Constructs an empty processor (matches Go's `newStatsJSONValuesProcessor`).
    // Ported for Go parity; not yet wired into a caller (see PARITY.md).
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn with_config(field_filters: Vec<Vec<u8>>, limit: u64) -> Self {
        Self {
            field_filters,
            limit,
            ..Self::default()
        }
    }

    fn limit_reached(&self) -> bool {
        self.limit > 0 && self.entries.len() as u64 > self.limit
    }

    fn update_state_for_row(&mut self, br: &mut BlockResult, cs: &[ColRef], row_idx: usize) -> i64 {
        self.fields_buf = cs
            .iter()
            .map(|&c| {
                let name = br.column_name(c).to_vec();
                let value = br.column_get_value_at_row(c, row_idx).to_vec();
                Field { name, value }
            })
            .collect();

        let mut buf = Vec::new();
        marshal_fields_to_json(&mut buf, &self.fields_buf);
        let delta = std::mem::size_of::<Vec<u8>>() as i64 + buf.len() as i64;
        self.entries.push(buf);
        delta
    }
}

impl StatsProcessor for StatsJSONValuesProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if self.limit_reached() {
            return 0;
        }

        let mc = get_matching_columns(br, &self.field_filters);
        let mut state_size_increase = 0;
        for row_idx in 0..br.rows_len() {
            state_size_increase += self.update_state_for_row(br, &mc, row_idx);
        }
        state_size_increase
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        if self.limit_reached() {
            return 0;
        }

        let mc = get_matching_columns(br, &self.field_filters);
        self.update_state_for_row(br, &mc, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        if self.limit_reached() {
            return;
        }
        let src = other
            .as_any()
            .downcast_ref::<StatsJSONValuesProcessor>()
            .expect("merge_state: other must be a StatsJSONValuesProcessor");
        self.entries.extend(src.entries.iter().cloned());
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_var_uint64(dst, self.entries.len() as u64);
        for v in &self.entries {
            encoding::marshal_bytes(dst, v);
        }
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (entries_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal entriesLen".to_string());
        }
        let mut src = &src[n as usize..];

        let mut entries = Vec::with_capacity(entries_len as usize);
        let mut state_size_increase = std::mem::size_of::<Vec<u8>>() as i64 * entries_len as i64;
        for _ in 0..entries_len {
            let (v, n) = encoding::unmarshal_bytes(src);
            let v = match v {
                Some(v) if n > 0 => v,
                _ => return Err("cannot unmarshal value".to_string()),
            };
            src = &src[n as usize..];

            state_size_increase += v.len() as i64;
            entries.push(v.to_vec());
        }
        if !src.is_empty() {
            return Err(format!(
                "unexpected tail left after unmarshaling entries; len(tail)={}",
                src.len()
            ));
        }

        self.entries = entries;
        Ok(state_size_increase)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let entries = if self.limit > 0 && self.entries.len() as u64 > self.limit {
            &self.entries[..self.limit as usize]
        } else {
            &self.entries[..]
        };
        marshal_json_values(dst, entries);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Appends the JSON array of pre-marshaled `items` to `dst`.
///
/// PORT NOTE: port of `marshalJSONValues` (`stats_uniq_values.go`); see module docs.
pub(crate) fn marshal_json_values(dst: &mut Vec<u8>, items: &[Vec<u8>]) {
    if items.is_empty() {
        dst.extend_from_slice(b"[]");
        return;
    }
    dst.push(b'[');
    dst.extend_from_slice(&items[0]);
    for item in &items[1..] {
        dst.push(b',');
        dst.extend_from_slice(item);
    }
    dst.push(b']');
}

/// Returns the columns of `br` matching `filters`, sorted by name.
///
/// PORT NOTE: port of `getMatchingColumns` + `matchingColumns.sort`
/// (`block_result.go`). The Go pooling (`getMatchingColumns`/`putMatchingColumns`)
/// is dropped; the result is an owned `Vec<ColRef>`.
pub(crate) fn get_matching_columns(br: &mut BlockResult, filters: &[Vec<u8>]) -> Vec<ColRef> {
    let mut cs: Vec<ColRef> = if is_single_field(filters) {
        vec![br.get_column_by_name(&filters[0])]
    } else {
        get_matching_columns_slow(br, filters)
    };

    if cs.len() > 1 {
        cs.sort_by(|&a, &b| br.column_name(a).cmp(br.column_name(b)));
    }
    cs
}

fn is_single_field(filters: &[Vec<u8>]) -> bool {
    filters.len() == 1 && !prefix_filter::is_wildcard_filter(&filters[0])
}

fn get_matching_columns_slow(br: &mut BlockResult, filters: &[Vec<u8>]) -> Vec<ColRef> {
    let all = br.get_columns();
    let names: Vec<Vec<u8>> = all.iter().map(|&c| br.column_name(c).to_vec()).collect();

    let mut dst: Vec<ColRef> = Vec::new();

    // Add columns matching the given filters.
    for (i, &c) in all.iter().enumerate() {
        if prefix_filter::match_filters(filters, &names[i]) {
            dst.push(c);
        }
    }

    // Add empty columns for non-wildcard filters that don't match a real column.
    for f in filters {
        if prefix_filter::is_wildcard_filter(f) {
            continue;
        }
        if !names.iter().any(|n| n == f) {
            dst.push(br.get_column_by_name(f));
        }
    }

    dst
}

fn field_names_string(fields: &[Vec<u8>]) -> String {
    fields
        .iter()
        .map(|f| quote_field_filter_if_needed(f))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_result::BlockResult;
    use crate::rows::Field;
    use crate::stats_json_values_sorted::StatsJSONValuesSortedEntry;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn rows(spec: &[&[(&str, &str)]]) -> Vec<Vec<Field>> {
        spec.iter()
            .map(|r| r.iter().map(|&(n, v)| field(n, v)).collect())
            .collect()
    }

    fn run(
        field_filters: &[&str],
        sort_fields: Vec<BySortField>,
        limit: u64,
        spec: &[&[(&str, &str)]],
    ) -> String {
        let sv = StatsJSONValues::new(
            field_filters
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect(),
            sort_fields,
            limit,
        );
        let mut sfp = sv.new_stats_processor();

        let rows = rows(spec);
        let mut br = BlockResult::default();
        br.must_init_from_rows(&rows);

        sfp.update_stats_for_all_rows(&sv, &mut br);

        let mut dst = Vec::new();
        sfp.finalize_stats(&sv, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    // Port of the pipe cases in TestStatsJSONValues, exercised directly on the
    // processors (the parser/pipe wiring is deferred).
    #[test]
    fn test_stats_json_values() {
        // value collection, insertion order (unsorted base processor).
        assert_eq!(
            run(&["*"], vec![], 0, &[&[("a", "1")], &[("a", "2")]]),
            r#"[{"a":"1"},{"a":"2"}]"#
        );

        // all fields, sorted ascending by a (sorted processor).
        assert_eq!(
            run(
                &["*"],
                vec![BySortField::new("a", false)],
                0,
                &[
                    &[("b", "3"), ("_msg", "abc"), ("a", "2")],
                    &[("a", "1"), ("_msg", "def")],
                    &[("a", "3"), ("b", "54")],
                ]
            ),
            r#"[{"_msg":"def","a":"1"},{"_msg":"abc","a":"2","b":"3"},{"a":"3","b":"54"}]"#
        );

        // all fields, sorted with limit (topk processor).
        assert_eq!(
            run(
                &["*"],
                vec![BySortField::new("a", false)],
                2,
                &[
                    &[("b", "3"), ("_msg", "abc"), ("a", "2")],
                    &[("a", "1"), ("_msg", "def")],
                    &[("a", "3"), ("b", "54")],
                ]
            ),
            r#"[{"_msg":"def","a":"1"},{"_msg":"abc","a":"2","b":"3"}]"#
        );

        // selected fields, sorted with limit (topk processor).
        assert_eq!(
            run(
                &["b", "_msg"],
                vec![BySortField::new("a", false)],
                2,
                &[
                    &[("a", "2"), ("_msg", "abc"), ("b", "3")],
                    &[("_msg", "def"), ("a", "1")],
                    &[("b", "54"), ("a", "3")],
                ]
            ),
            r#"[{"_msg":"def"},{"_msg":"abc","b":"3"}]"#
        );

        // reverse order with limit 1 (topk processor).
        assert_eq!(
            run(
                &["*"],
                vec![BySortField::new("a", true)],
                1,
                &[
                    &[("b", "3"), ("_msg", "abc"), ("a", "2")],
                    &[("_msg", "def"), ("a", "1")],
                    &[("a", "3"), ("b", "54")],
                ]
            ),
            r#"[{"a":"3","b":"54"}]"#
        );

        // multiple sorting columns without limit (sorted processor).
        assert_eq!(
            run(
                &["*"],
                vec![BySortField::new("a", true), BySortField::new("b", false)],
                0,
                &[
                    &[("a", "3"), ("b", "123")],
                    &[("a", "1")],
                    &[("b", "54"), ("a", "3")],
                ]
            ),
            r#"[{"a":"3","b":"54"},{"a":"3","b":"123"},{"a":"1"}]"#
        );

        // multiple sorting columns with limit (topk processor).
        assert_eq!(
            run(
                &["*"],
                vec![BySortField::new("a", true), BySortField::new("b", false)],
                2,
                &[
                    &[("a", "3"), ("b", "123")],
                    &[("a", "1")],
                    &[("b", "54"), ("a", "3")],
                ]
            ),
            r#"[{"a":"3","b":"54"},{"a":"3","b":"123"}]"#
        );
    }

    // Port of TestStatsJSONValuesProcessor_ExportImportState.
    #[test]
    fn test_stats_json_values_processor_export_import_state() {
        fn check(sjp: &StatsJSONValuesProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            sjp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected, "unexpected dataLen");

            let mut sjp2 = StatsJSONValuesProcessor::new();
            sjp2.import_state(&data, None).unwrap();
            assert_eq!(sjp, &sjp2, "unexpected state imported");
        }

        // empty state
        let sjp = StatsJSONValuesProcessor::new();
        check(&sjp, 1);

        // non-empty state
        let mut sjp = StatsJSONValuesProcessor::new();
        sjp.entries = vec![b"foo".to_vec(), b"bar".to_vec(), b"baz".to_vec()];
        check(&sjp, 13);
    }

    // Port of TestStatsJSONValuesSortedProcessor_ExportImportState.
    #[test]
    fn test_stats_json_values_sorted_processor_export_import_state() {
        fn check(
            sjp: &mut StatsJSONValuesSortedProcessor,
            sort_fields_len: usize,
            data_len_expected: usize,
        ) {
            sjp.sort_fields_len = sort_fields_len;
            let mut data = Vec::new();
            sjp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected, "unexpected dataLen");

            let mut sjp2 = StatsJSONValuesSortedProcessor::default();
            sjp2.sort_fields_len = sort_fields_len;
            sjp2.import_state(&data, None).unwrap();
            assert_eq!(sjp, &sjp2, "unexpected state imported");
        }

        // empty state
        let mut sjp = StatsJSONValuesSortedProcessor::default();
        check(&mut sjp, 0, 1);

        // non-empty state
        let mut sjp = StatsJSONValuesSortedProcessor::default();
        sjp.entries = vec![
            StatsJSONValuesSortedEntry::new_for_test(
                "foo",
                vec!["v1-for-foo".to_string(), "v2-for-foo".to_string()],
            ),
            StatsJSONValuesSortedEntry::new_for_test(
                "bar",
                vec!["v1-for-bar".to_string(), "v2-for-bar".to_string()],
            ),
        ];
        check(&mut sjp, 2, 53);
    }

    // Port of TestStatsJSONValuesTopkProcessor_ExportImportState.
    #[test]
    fn test_stats_json_values_topk_processor_export_import_state() {
        fn check(
            sjp: &mut StatsJSONValuesTopkProcessor,
            sort_fields_len: usize,
            data_len_expected: usize,
        ) {
            sjp.sort_fields_len = sort_fields_len;
            let mut data = Vec::new();
            sjp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected, "unexpected dataLen");

            let mut sjp2 = StatsJSONValuesTopkProcessor::default();
            sjp2.sort_fields_len = sort_fields_len;
            sjp2.import_state(&data, None).unwrap();
            assert_eq!(sjp, &sjp2, "unexpected state imported");
        }

        // empty state
        let mut sjp = StatsJSONValuesTopkProcessor::default();
        check(&mut sjp, 0, 1);

        // non-empty state
        let mut sjp = StatsJSONValuesTopkProcessor::default();
        sjp.entries = vec![
            StatsJSONValuesSortedEntry::new_for_test(
                "foo",
                vec!["v1-for-foo".to_string(), "v2-for-foo".to_string()],
            ),
            StatsJSONValuesSortedEntry::new_for_test(
                "bar",
                vec!["v1-for-bar".to_string(), "v2-for-bar".to_string()],
            ),
        ];
        check(&mut sjp, 2, 53);
    }
}
