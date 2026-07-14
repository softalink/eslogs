//! Port of EsLogs `lib/logstorage/filter_generic.go`.
//!
//! `FilterGeneric` applies a [`FieldFilter`] to a single field name (or, when
//! the name ends with `*`, to every field with the matching prefix). It is the
//! wrapper returned by `new_filter_phrase`, `new_filter_exact`, etc.
//!
//! This module also hosts the small **shared** helpers that several filter
//! files reach for (`match_*` byte helpers, `visit`-side column utilities are
//! in `filter_phrase.rs`), exported `pub(crate)` so the sibling filter modules
//! (including the ones owned by the other filter batches) can call them.

use crate::bitmap::{Bitmap, get_bitmap, put_bitmap};
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::{FieldFilter, Filter};
use crate::log_rows::get_canonical_column_name;
use crate::prefix_filter;
use crate::prefix_filter::is_wildcard_filter;
use crate::rows::Field;
use crate::stream_filter::quote_token_if_needed;
use crate::tokenizer::is_token_rune;

// ---------------------------------------------------------------------------
// FilterGeneric
// ---------------------------------------------------------------------------

/// `FilterGeneric` applies the given field filter `f` to the given field name.
///
/// PORT NOTE: Go stores the concrete field filter behind the unexported
/// `fieldFilter` interface; the port holds it as `Box<dyn FieldFilter>`.
pub(crate) struct FilterGeneric {
    /// The name of the field to apply `f` to. It may end with `*` when
    /// `is_wildcard` is true.
    pub(crate) field_name: String,

    /// Indicates whether `field_name` is a wildcard ending with `*`. In this
    /// case `f` is applied to all fields with the given prefix until the first
    /// match.
    pub(crate) is_wildcard: bool,

    /// The field filter to apply.
    pub(crate) f: Box<dyn FieldFilter>,
}

/// Wraps `f` into a [`FilterGeneric`] for the given field name.
pub(crate) fn new_filter_generic(field_name: &str, f: Box<dyn FieldFilter>) -> FilterGeneric {
    if is_wildcard_filter(field_name) {
        return FilterGeneric {
            field_name: field_name.to_string(),
            is_wildcard: true,
            f,
        };
    }

    let field_name_canonical = get_canonical_column_name(field_name).to_string();
    FilterGeneric {
        field_name: field_name_canonical,
        is_wildcard: false,
        f,
    }
}

impl Filter for FilterGeneric {
    fn to_string(&self) -> String {
        if !self.is_wildcard {
            return quote_field_name_if_needed(&self.field_name) + &self.f.to_string();
        }
        quote_field_filter_if_needed(&self.field_name) + ":" + &self.f.to_string()
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter(&self.field_name);
    }

    fn is_match_all(&self) -> bool {
        // Go removeStarFilters: `*` == empty-prefix filterPrefix on _msg.
        !self.is_wildcard && is_msg_field_name(&self.field_name) && self.f.is_empty_prefix()
    }

    /// Port of Go `filterGeneric.hasFilterInWithQuery`.
    fn has_filter_in_with_query(&self) -> bool {
        self.f.in_values().is_some_and(|iv| iv.q_text.is_some())
    }

    fn has_direct_subquery(&self) -> bool {
        self.has_filter_in_with_query()
    }

    /// Port of Go `filterGeneric.visitSubqueries` (the type switch on
    /// `*filterIn`/`*filterContainsAny`/`*filterContainsAll` is the
    /// `FieldFilter::in_values_mut` hook). Go visits the parsed `t.values.q`;
    /// the port parses the stored text at `timestamp`, visits the parsed query
    /// and stores the re-rendered text back
    /// (see `Filter::visit_subqueries_mut`).
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        let Some(iv) = self.f.in_values_mut() else {
            return;
        };
        let Some(q_text) = iv.q_text.as_mut() else {
            return;
        };
        let mut q = crate::parser::query::must_parse_query(q_text, timestamp);
        q.visit_subqueries(visit);
        *q_text = q.to_string();
    }

    /// Port of Go `filterGeneric.initFilterInValues` (the type switch on
    /// `*filterIn`/`*filterContainsAny`/`*filterContainsAll` is expressed via
    /// the `FieldFilter::in_values`/`new_with_values` hooks).
    fn init_filter_in_values(
        &self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
    ) -> Result<Option<Box<dyn Filter>>, String> {
        let Some(iv) = self.f.in_values() else {
            return Ok(None);
        };
        let Some(q_text) = &iv.q_text else {
            return Ok(None);
        };
        let values = get_values(q_text, &iv.q_field_name).map_err(|e| {
            format!(
                "cannot obtain unique values for {}: {e}",
                self.f.to_string()
            )
        })?;
        Ok(self.f.new_with_values(&self.field_name, values))
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        if !self.is_wildcard {
            // Fast path - match the row by the given field name.
            return self.f.match_row_by_field(fields, &self.field_name);
        }

        // Slow path - match the row by wildcard.
        let prefix = &self.field_name[..self.field_name.len() - 1];
        for f in fields {
            if !f.name.starts_with(prefix.as_bytes()) {
                continue;
            }
            // Match-only lossy view: the FieldFilter trait takes text names
            // (query-side API); the stored name bytes stay raw.
            if self
                .f
                .match_row_by_field(fields, &String::from_utf8_lossy(&f.name))
            {
                return true;
            }
        }
        false
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        if !self.is_wildcard {
            // Fast path - apply filter only to the given field name.
            self.f
                .apply_to_block_search_by_field(bs, bm, &self.field_name);
            return;
        }

        // Slow path - apply filter to all the matching fields.
        let prefix = self.field_name[..self.field_name.len() - 1].to_string();

        let mut bm_result = get_bitmap(bm.bits_len);
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_result.copy_from(bm);

        // Special columns.
        for &field_name in SPECIAL_COLUMNS.iter() {
            if !field_name.starts_with(&prefix) {
                continue;
            }
            if bs.is_hidden_field(field_name) {
                continue;
            }
            bm_tmp.copy_from(&bm_result);
            self.f
                .apply_to_block_search_by_field(bs, &mut bm_tmp, field_name);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }

        // Collect const-column and column-header names before mutating bs via
        // the per-field accessors (the columns header borrows bs).
        // Match-only lossy views: the FieldFilter trait takes text names
        // (query-side API); the stored name bytes stay raw in the headers.
        let (const_names, col_names): (Vec<String>, Vec<String>) = {
            let csh = bs.get_columns_header();
            let const_names = csh
                .const_columns
                .iter()
                .map(|cc| String::from_utf8_lossy(&cc.name).into_owned())
                .collect();
            let col_names = csh
                .column_headers
                .iter()
                .map(|ch| String::from_utf8_lossy(&ch.name).into_owned())
                .collect();
            (const_names, col_names)
        };

        for name in &const_names {
            if is_special_column(name) {
                continue;
            }
            if !name.starts_with(&prefix) {
                continue;
            }
            if bs.is_hidden_field(name) {
                continue;
            }
            bm_tmp.copy_from(&bm_result);
            self.f.apply_to_block_search_by_field(bs, &mut bm_tmp, name);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }

        for name in &col_names {
            if is_special_column(name) {
                continue;
            }
            if !name.starts_with(&prefix) {
                continue;
            }
            if bs.is_hidden_field(name) {
                continue;
            }
            bm_tmp.copy_from(&bm_result);
            self.f.apply_to_block_search_by_field(bs, &mut bm_tmp, name);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }

        bm.and_not(&bm_result);
        put_bitmap(bm_tmp);
        put_bitmap(bm_result);
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if !self.is_wildcard {
            // Fast path - apply filter to the given field name.
            self.f
                .apply_to_block_result_by_field(br, bm, &self.field_name);
            return;
        }

        // Slow path - apply filter to all the matching fields.
        let prefix = self.field_name[..self.field_name.len() - 1].to_string();

        let mut bm_result = get_bitmap(bm.bits_len);
        let mut bm_tmp = get_bitmap(bm.bits_len);
        bm_result.copy_from(bm);

        let cols = br.get_columns();
        // Match-only lossy views (see the block-search slow path above).
        let names: Vec<String> = cols
            .iter()
            .map(|&r| String::from_utf8_lossy(br.column_name(r)).into_owned())
            .collect();
        for name in &names {
            if !name.starts_with(&prefix) {
                continue;
            }
            bm_tmp.copy_from(&bm_result);
            self.f.apply_to_block_result_by_field(br, &mut bm_tmp, name);
            bm_result.and_not(&bm_tmp);
            if bm_result.is_zero() {
                put_bitmap(bm_tmp);
                put_bitmap(bm_result);
                return;
            }
        }

        bm.and_not(&bm_result);
        put_bitmap(bm_tmp);
        put_bitmap(bm_result);
    }
}

// ---------------------------------------------------------------------------
// Field-name quoting helpers (Go filter_generic.go + parser.go)
// ---------------------------------------------------------------------------

/// Port of Go `quoteFieldNameIfNeeded`.
pub(crate) fn quote_field_name_if_needed(s: &str) -> String {
    if is_msg_field_name(s) {
        return String::new();
    }
    quote_token_if_needed(s) + ":"
}

/// Port of Go `isMsgFieldName`.
pub(crate) fn is_msg_field_name(field_name: &str) -> bool {
    field_name.is_empty() || field_name == "_msg"
}

/// Port of Go `quoteFieldFilterIfNeeded` (defined in parser.go, still unported).
///
/// PORT NOTE: homed here — its only current consumer is `FilterGeneric::to_string`.
/// Go calls `needQuoteToken(wildcard)` + `strconv.Quote`; the port derives both
/// from the `pub(crate)` `quote_token_if_needed` (which quotes iff needed),
/// avoiding a dependency on stream_filter's private `need_quote_token`.
pub(crate) fn quote_field_filter_if_needed(s: &str) -> String {
    if !is_wildcard_filter(s) {
        return quote_token_if_needed(s);
    }
    let wildcard = &s[..s.len() - 1];
    let quoted = quote_token_if_needed(wildcard);
    if wildcard.is_empty() || quoted == wildcard {
        return s.to_string();
    }
    format!("{quoted}*")
}

// ---------------------------------------------------------------------------
// Special columns (Go block_result.go: isSpecialColumn / specialColumns)
// ---------------------------------------------------------------------------

/// Port of Go `specialColumns` (defined in block_result.go, not yet ported
/// there). Homed here as a shared `pub(crate)` helper; dedup with block_result
/// once it ports this.
pub(crate) const SPECIAL_COLUMNS: [&str; 4] = ["_msg", "_time", "_stream", "_stream_id"];

/// Port of Go `isSpecialColumn` (defined in block_result.go, not yet ported
/// there). Homed here as a shared `pub(crate)` helper; dedup with block_result
/// once it ports this.
pub(crate) fn is_special_column(c: &str) -> bool {
    if c.is_empty() {
        // This is a _msg column.
        return true;
    }
    if !c.starts_with('_') {
        return false;
    }
    c == "_time" || c == "_stream" || c == "_stream_id"
}

// ---------------------------------------------------------------------------
// Token trimming helpers (Go filter_regexp.go: skipFirstToken/skipLastToken)
// ---------------------------------------------------------------------------

/// Port of Go `skipFirstLastToken`.
///
/// PORT NOTE: `skipFirstToken`/`skipLastToken`/`skipFirstLastToken` are defined
/// in filter_regexp.go (a different filter batch). They are small and shared by
/// filter_substring/filter_prefix/filter_exact_prefix (this batch); homed here
/// `pub(crate)` — dedup with filter_regexp.rs when it lands.
pub(crate) fn skip_first_last_token(s: &str) -> &str {
    skip_last_token(skip_first_token(s))
}

/// Port of Go `skipFirstToken`.
pub(crate) fn skip_first_token(s: &str) -> &str {
    let mut s = s;
    loop {
        match s.chars().next() {
            Some(r) if is_token_rune(r) => s = &s[r.len_utf8()..],
            _ => return s,
        }
    }
}

/// Port of Go `skipLastToken`.
pub(crate) fn skip_last_token(s: &str) -> &str {
    let mut s = s;
    loop {
        match s.chars().next_back() {
            Some(r) if is_token_rune(r) => s = &s[..s.len() - r.len_utf8()],
            _ => return s,
        }
    }
}

/// Byte form of [`skip_last_token`] for raw-byte prefixes: Go's
/// `utf8.DecodeLastRuneInString` yields `RuneError` on an invalid trailing
/// sequence, which is not a token rune, so trimming stops there — mirrored by
/// [`decode_rune_at_end`].
pub(crate) fn skip_last_token_bytes(s: &[u8]) -> &[u8] {
    let mut s = s;
    while !s.is_empty() {
        let r = decode_rune_at_end(s);
        if !is_token_rune(r) {
            break;
        }
        // `r` is a validly decoded rune here (RUNE_ERROR is not a token
        // rune), so it occupies exactly `len_utf8()` bytes.
        s = &s[..s.len() - r.len_utf8()];
    }
    s
}

/// Port of Go `getTokensSkipLast`: tokenizes `s` after trimming its last
/// token. Operates on raw bytes; the tokens are emitted by the byte tokenizer
/// (the ingest-side one), keeping bloom-filter hashes consistent for prefixes
/// containing invalid UTF-8.
pub(crate) fn get_tokens_skip_last_bytes(s: &[u8]) -> Vec<Vec<u8>> {
    let trimmed = skip_last_token_bytes(s);
    let mut dst: Vec<&[u8]> = Vec::new();
    crate::tokenizer::tokenize_bytes(&mut dst, std::slice::from_ref(&trimmed));
    dst.into_iter().map(|t| t.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Byte-oriented rune helpers (shared by match_phrase / match_prefix)
// ---------------------------------------------------------------------------

/// Rust equivalent of Go's `utf8.RuneError`.
pub(crate) const RUNE_ERROR: char = '\u{FFFD}';

/// Returns the index of the first occurrence of `needle` in `haystack`, or
/// `None`. Mirrors Go's `strings.Index` for the non-empty-needle case.
pub(crate) fn index_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Decodes the first rune of `b`, returning [`RUNE_ERROR`] on invalid input.
/// Port of Go's `utf8.DecodeRuneInString` (rune value only).
pub(crate) fn decode_rune_at_start(b: &[u8]) -> char {
    match std::str::from_utf8(b) {
        Ok(s) => s.chars().next().unwrap_or(RUNE_ERROR),
        Err(e) => {
            let v = e.valid_up_to();
            if v > 0 {
                std::str::from_utf8(&b[..v])
                    .unwrap()
                    .chars()
                    .next()
                    .unwrap_or(RUNE_ERROR)
            } else {
                RUNE_ERROR
            }
        }
    }
}

/// Decodes the last rune of `b`, returning [`RUNE_ERROR`] on invalid input.
/// Port of Go's `utf8.DecodeLastRuneInString` (rune value only).
pub(crate) fn decode_rune_at_end(b: &[u8]) -> char {
    let start = b.len().saturating_sub(4);
    for i in start..b.len() {
        if let Some(c) = std::str::from_utf8(&b[i..])
            .ok()
            .and_then(|s| s.chars().next_back())
        {
            return c;
        }
    }
    RUNE_ERROR
}

/// Returns the rune ending at byte offset `offset` of `b` (i.e. the rune just
/// before `offset`), mirroring Go's `rune(s[offset-1])` / `DecodeLastRuneInString`
/// combination: ASCII bytes are returned directly; otherwise the last rune of
/// `b[..offset]` is decoded.
pub(crate) fn rune_before(b: &[u8], offset: usize) -> char {
    let byte = b[offset - 1];
    if byte < 0x80 {
        byte as char
    } else {
        decode_rune_at_end(&b[..offset])
    }
}

/// Returns the rune starting at byte offset `offset` of `b`, mirroring Go's
/// `rune(s[offset])` / `DecodeRuneInString` combination.
pub(crate) fn rune_at(b: &[u8], offset: usize) -> char {
    let byte = b[offset];
    if byte < 0x80 {
        byte as char
    } else {
        decode_rune_at_start(&b[offset..])
    }
}

/// Snapshots the fields of `ch` needed by the block-search value helpers.
///
/// PORT NOTE: `ColumnHeader` is not `Clone`, and Go passes a `*columnHeader`
/// pointer while the block-search accessors need `&mut bs`. The filters obtain
/// `ch` from `bs.get_column_header` (an immutable borrow of `bs`), so they
/// snapshot the required fields into an owned `ColumnHeader` to release the
/// borrow before calling the `&mut bs` value/bloom accessors (which only read
/// `name`/offsets/sizes and dict values).
pub(crate) fn clone_column_header(ch: &ColumnHeader) -> ColumnHeader {
    let mut c = ColumnHeader {
        name: ch.name.clone(),
        value_type: ch.value_type,
        min_value: ch.min_value,
        max_value: ch.max_value,
        values_offset: ch.values_offset,
        values_size: ch.values_size,
        bloom_filter_offset: ch.bloom_filter_offset,
        bloom_filter_size: ch.bloom_filter_size,
        ..Default::default()
    };
    c.values_dict.values = ch.values_dict.values.clone();
    c
}
