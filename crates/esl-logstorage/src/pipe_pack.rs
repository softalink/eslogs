//! Port of EsLogs `lib/logstorage/pipe_pack.go`.
//!
//! Shared scaffolding for the `pack_*` pipes (`pack_json`, `pack_logfmt`),
//! which serialize a set of fields into a single result field. It exposes
//! [`update_needed_fields_for_pipe_pack`] and [`new_pipe_pack_processor`],
//! parameterized by a `marshal_fields` function.

use std::sync::Arc;

use esl_common::atomicutil::Slice;

use crate::block_result::{BlockResult, ColRef, ResultColumn};
use crate::pipe::PipeProcessor;
use crate::prefix_filter;
use crate::rows::Field;

/// Serializes `fields` into `dst` (Go `marshalFields`).
pub(crate) type MarshalFieldsFn = fn(&mut Vec<u8>, &[Field]);

/// Port of Go's `updateNeededFieldsForPipePack`.
pub(crate) fn update_needed_fields_for_pipe_pack(
    pf: &mut prefix_filter::Filter,
    result_field: &str,
    field_filters: &[String],
) {
    if pf.match_string(result_field) {
        pf.add_deny_filter(result_field);
        if !field_filters.is_empty() {
            pf.add_allow_filters(field_filters);
        } else {
            pf.add_allow_filter("*");
        }
    }
}

/// Port of Go's `newPipePackProcessor`.
pub(crate) fn new_pipe_pack_processor(
    pp_next: Arc<dyn PipeProcessor>,
    result_field: String,
    fields: Vec<String>,
    marshal_fields: MarshalFieldsFn,
) -> Arc<dyn PipeProcessor> {
    Arc::new(PipePackProcessor {
        pp_next,
        result_field,
        fields,
        marshal_fields,
        shards: Slice::default(),
    })
}

struct PipePackProcessor {
    pp_next: Arc<dyn PipeProcessor>,
    result_field: String,
    fields: Vec<String>,
    marshal_fields: MarshalFieldsFn,
    shards: Slice<std::sync::Mutex<PipePackProcessorShard>>,
}

#[derive(Default)]
struct PipePackProcessorShard {
    rc: ResultColumn,
    buf: Vec<u8>,
    fields: Vec<Field>,
    cs: Vec<ColRef>,
    cs_names: Vec<String>,
}

impl PipeProcessor for PipePackProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let shard_arc = self.shards.get(worker_id);
        let mut guard = shard_arc.lock().unwrap();
        let shard = &mut *guard;

        shard.rc.name = self.result_field.clone();

        let cs_all = br.get_columns();
        let cs_all_names: Vec<String> = cs_all
            .iter()
            .map(|&c| br.column_name(c).to_string())
            .collect();

        shard.cs.clear();
        shard.cs_names.clear();
        if self.fields.is_empty() {
            for (i, &c) in cs_all.iter().enumerate() {
                shard.cs.push(c);
                shard.cs_names.push(cs_all_names[i].clone());
            }
        } else {
            for (i, &c) in cs_all.iter().enumerate() {
                let name = &cs_all_names[i];
                for f in &self.fields {
                    if name == f || (f.ends_with('*') && name.starts_with(&f[..f.len() - 1])) {
                        shard.cs.push(c);
                        shard.cs_names.push(name.clone());
                        break;
                    }
                }
            }
        }

        let rows_len = br.rows_len();
        for row_idx in 0..rows_len {
            shard.fields.clear();
            for (i, &c) in shard.cs.iter().enumerate() {
                let v = br.column_get_value_at_row(c, row_idx).to_string();
                shard.fields.push(Field {
                    name: shard.cs_names[i].clone(),
                    value: v,
                });
            }
            shard.buf.clear();
            (self.marshal_fields)(&mut shard.buf, &shard.fields);
            shard.rc.add_value(&shard.buf);
        }

        // PORT NOTE: Go's `addResultColumn` borrows `shard.rc`; the Rust API
        // consumes it, so the column is cloned and then reset for reuse.
        br.add_result_column(shard.rc.clone());
        self.pp_next.write_block(worker_id, br);

        shard.rc.reset();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
