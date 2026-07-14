//! Port of EsLogs `lib/logstorage/filter_stream.go`.
//!
//! `FilterStream` is the filter for `{}` aka `_stream:{...}`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::StreamFilter;
use crate::stream_id::StreamID;
use crate::values_encoder::ValueType;

/// `FilterStream` is the filter for `{}` aka `_stream:{...}`.
pub(crate) struct FilterStream {
    /// The stream filter to apply.
    f: StreamFilter,

    /// Matching streamIDs per partition indexdb, resolved lazily during the
    /// search.
    ///
    /// PORT NOTE: Go's `initStreamFilters` (storage_search.go) copies the
    /// filter tree once per partition, binding `tenantIDs` + that partition's
    /// `*indexdb` to each `filterStream`. The Rust filter tree is shared
    /// immutably across partitions and workers, so the binding is inverted:
    /// [`Filter::apply_to_block_search`] reaches the partition's indexdb via
    /// the block search (`bs.p -> partition -> idb`) and the tenantIDs via
    /// `bs.pso`, and this per-idb cache plays the role of Go's per-partition
    /// filter copies (the cache lives only as long as the query's filter
    /// tree; entries are keyed by indexdb identity, which is pinned by the
    /// partition references held for the whole search).
    stream_ids_by_idb: Mutex<HashMap<usize, Arc<HashSet<StreamID>>>>,
}

pub(crate) fn new_filter_stream(f: StreamFilter) -> FilterStream {
    FilterStream {
        f,
        stream_ids_by_idb: Mutex::new(HashMap::new()),
    }
}

impl FilterStream {
    /// Resolves (and caches) the streamIDs matching the filter in the
    /// partition the searched block belongs to (Go `getStreamIDs`, with the
    /// per-partition binding done lazily — see the struct PORT NOTE).
    fn get_stream_ids_for_search(&self, bs: &BlockSearch<'_>) -> Arc<HashSet<StreamID>> {
        let pt = bs
            .part()
            .pt
            .as_ref()
            .expect("BUG: searched part must belong to a partition")
            .upgrade()
            .expect("BUG: partition closed while a search references its parts");
        let idb = &pt.idb;
        let key = Arc::as_ptr(idb) as usize;
        let mut cache = self.stream_ids_by_idb.lock().unwrap();
        if let Some(ids) = cache.get(&key) {
            return Arc::clone(ids);
        }
        let ids: Arc<HashSet<StreamID>> = Arc::new(
            idb.search_stream_ids(&bs.search_options().stream_filter_tenant_ids, &self.f)
                .into_iter()
                .collect(),
        );
        cache.insert(key, Arc::clone(&ids));
        ids
    }

    fn match_column_by_stream_name(
        br: &mut BlockResult,
        bm: &mut Bitmap,
        r: ColRef,
        f: &StreamFilter,
    ) {
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| f.match_stream_name(&values[idx]));
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
        let v = get_field_value_by_name(fields, b"_stream");
        self.f.match_stream_name(v)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.f.is_empty() {
            return;
        }

        let r = br.get_column_by_name(b"_stream");
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_vec();
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
        let stream_ids = self.get_stream_ids_for_search(bs);
        let sid = bs.block_header().stream_id;
        if !stream_ids.contains(&sid) {
            bm.reset_bits();
        }
    }

    fn as_stream_filter(&self) -> Option<&StreamFilter> {
        Some(&self.f)
    }

    fn take_stream_filter(&mut self) -> Option<StreamFilter> {
        Some(std::mem::take(&mut self.f))
    }
}
