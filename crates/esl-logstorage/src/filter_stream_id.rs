//! Port of EsLogs `lib/logstorage/filter_stream_id.go`.
//!
//! `FilterStreamID` is the filter for `_stream_id:id`.

use std::collections::HashSet;
use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_id::StreamID;
use crate::values_encoder::ValueType;

/// `FilterStreamID` is the filter for `_stream_id:id`.
pub(crate) struct FilterStreamID {
    stream_ids: Vec<StreamID>,

    /// If set, then `stream_ids` must be populated from this subquery before
    /// filter execution (Go `q *Query`).
    ///
    /// PORT NOTE: Go stores the parsed subquery; the Rust port stores its
    /// rendered text (the established subquery pattern — see pipe_join.rs) and
    /// `storage_search::init_subqueries` re-parses it at the outer query
    /// timestamp before execution.
    q_text: Option<String>,

    /// The field name for obtaining values from if `q_text` is set
    /// (Go `qFieldName`).
    q_field_name: String,

    stream_ids_map: OnceLock<HashSet<Vec<u8>>>,
}

pub(crate) fn new_filter_stream_id(stream_ids: Vec<StreamID>) -> FilterStreamID {
    FilterStreamID {
        stream_ids,
        q_text: None,
        q_field_name: String::new(),
        stream_ids_map: OnceLock::new(),
    }
}

/// Port of Go `newFilterStreamIDFromQuery`.
pub(crate) fn new_filter_stream_id_from_query(
    q_text: String,
    q_field_name: String,
) -> FilterStreamID {
    FilterStreamID {
        stream_ids: Vec::new(),
        q_text: Some(q_text),
        q_field_name,
        stream_ids_map: OnceLock::new(),
    }
}

impl FilterStreamID {
    fn get_stream_ids_map(&self) -> &HashSet<Vec<u8>> {
        self.stream_ids_map.get_or_init(|| {
            let mut m = HashSet::with_capacity(self.stream_ids.len());
            for stream_id in &self.stream_ids {
                let mut k = Vec::new();
                stream_id.marshal_string(&mut k);
                m.insert(k);
            }
            m
        })
    }

    fn match_column_by_stream_ids_map(
        br: &mut BlockResult,
        bm: &mut Bitmap,
        r: ColRef,
        m: &HashSet<Vec<u8>>,
    ) {
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| {
            let v = to_unsafe_string(&values[idx]);
            m.contains(v.as_bytes())
        });
    }
}

impl Filter for FilterStreamID {
    fn to_string(&self) -> String {
        if let Some(q_text) = &self.q_text {
            return format!("_stream_id:in({q_text})");
        }

        let stream_ids = &self.stream_ids;
        if stream_ids.len() == 1 {
            let mut b = Vec::new();
            stream_ids[0].marshal_string(&mut b);
            return format!("_stream_id:{}", to_unsafe_string(&b));
        }

        let a: Vec<String> = stream_ids
            .iter()
            .map(|stream_id| {
                let mut b = Vec::new();
                stream_id.marshal_string(&mut b);
                to_unsafe_string(&b).to_string()
            })
            .collect();
        format!("_stream_id:in({})", a.join(","))
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_stream_id");
    }

    /// Go `hasFilterInWithQueryForFilter`'s `*filterStreamID` arm (`t.q != nil`).
    fn has_filter_in_with_query(&self) -> bool {
        self.q_text.is_some()
    }

    /// Port of the `*filterStreamID` arm of Go `initFilterInValuesForFilter`'s
    /// copyFunc: resolves the subquery values and converts them to a literal
    /// streamID list.
    fn init_filter_in_values(
        &self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
    ) -> Result<Option<Box<dyn Filter>>, String> {
        let Some(q_text) = &self.q_text else {
            return Ok(None);
        };
        let values = get_values(q_text, &self.q_field_name).map_err(|e| {
            format!(
                "cannot obtain unique values for {}: {e}",
                Filter::to_string(self)
            )
        })?;

        // convert values to streamID list
        let mut stream_ids = Vec::with_capacity(values.len());
        for v in &values {
            let mut sid = StreamID::default();
            if sid.try_unmarshal_from_string(v) {
                stream_ids.push(sid);
            }
        }

        Ok(Some(Box::new(new_filter_stream_id(stream_ids))))
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let m = self.get_stream_ids_map();
        let v = get_field_value_by_name(fields, "_stream_id");
        m.contains(v.as_bytes())
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        let m = self.get_stream_ids_map();

        if m.is_empty() {
            bm.reset_bits();
            return;
        }

        let r = br.get_column_by_name("_stream_id");
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_string();
            if !m.contains(v.as_bytes()) {
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
            // table from `c.dictValues` and maps encoded indices through it.
            // `BlockResult` does not expose `dictValues`, so the port routes the
            // dict case through the already-decoded per-row values, exactly like
            // the shared `apply_to_block_result_generic` helper. The result is
            // identical.
            ValueType::STRING | ValueType::DICT => {
                Self::match_column_by_stream_ids_map(br, bm, r, m);
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
        let m = self.get_stream_ids_map();
        if m.is_empty() {
            bm.reset_bits();
            return;
        }

        let mut bb = Vec::new();
        bs.block_header().stream_id.marshal_string(&mut bb);
        if !m.contains(&bb) {
            bm.reset_bits();
        }
    }
}
