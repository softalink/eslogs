//! Port of EsLogs `lib/logstorage/filter_stream.go`.
//!
//! `FilterStream` is the filter for `{}` aka `_stream:{...}`.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::indexdb::Indexdb;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::StreamFilter;
use crate::stream_id::StreamID;
use crate::tenant_id::TenantID;
use crate::values_encoder::ValueType;

/// `FilterStream` is the filter for `{}` aka `_stream:{...}`.
pub(crate) struct FilterStream {
    /// The stream filter to apply.
    f: StreamFilter,

    /// The list of tenantIDs to search for streamIDs.
    ///
    /// PORT NOTE: Go assigns this struct field directly just before the search
    /// (`storage_search.go`, unported). The port keeps it a `pub(crate)` field
    /// so the future search setup can populate it the same way.
    pub(crate) tenant_ids: Vec<TenantID>,

    /// The indexdb to search for streamIDs.
    ///
    /// PORT NOTE: Go stores `*indexdb`, assigned just before the search. The
    /// port holds the shared `Arc<Indexdb>` (indexdb is shared via `Arc`), set
    /// by the future search setup.
    pub(crate) idb: Option<Arc<Indexdb>>,

    stream_ids: OnceLock<HashSet<StreamID>>,
}

pub(crate) fn new_filter_stream(f: StreamFilter) -> FilterStream {
    FilterStream {
        f,
        tenant_ids: Vec::new(),
        idb: None,
        stream_ids: OnceLock::new(),
    }
}

impl FilterStream {
    fn get_stream_ids(&self) -> &HashSet<StreamID> {
        self.stream_ids.get_or_init(|| {
            let idb = self
                .idb
                .as_ref()
                .expect("BUG: filterStream.idb must be set before search");
            let stream_ids = idb.search_stream_ids(&self.tenant_ids, &self.f);
            stream_ids.into_iter().collect()
        })
    }

    fn match_column_by_stream_name(
        br: &mut BlockResult,
        bm: &mut Bitmap,
        r: ColRef,
        f: &StreamFilter,
    ) {
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| f.match_stream_name(to_unsafe_string(&values[idx])));
    }
}

impl Filter for FilterStream {
    fn to_string(&self) -> String {
        self.f.to_string()
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_stream");
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let v = get_field_value_by_name(fields, "_stream");
        self.f.match_stream_name(v)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.f.is_empty() {
            return;
        }

        let r = br.get_column_by_name("_stream");
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_string();
            if !self.f.match_stream_name(&v) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            bm.reset_bits();
            return;
        }

        match br.column_value_type(r) {
            // PORT NOTE: Go's `valueTypeDict` case builds a per-dict-entry match
            // table from `c.dictValues`. `BlockResult` does not expose
            // `dictValues`, so the port routes the dict case through the
            // already-decoded per-row values, like the shared block-result
            // helpers. The result is identical.
            ValueType::STRING | ValueType::DICT => {
                Self::match_column_by_stream_name(br, bm, r, &self.f);
            }
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => {
                bm.reset_bits();
            }
            other => panicf!("FATAL: unknown valueType={}", other.0),
        }
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        if self.f.is_empty() {
            return;
        }
        let stream_ids = self.get_stream_ids();
        let sid = bs.block_header().stream_id;
        if !stream_ids.contains(&sid) {
            bm.reset_bits();
        }
    }
}
