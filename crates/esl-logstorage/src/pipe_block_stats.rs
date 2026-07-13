//! Port of `pipe_block_stats.go` — the `| block_stats` pipe.
//!
//! For every column of every input block it emits one row describing that
//! column's on-disk footprint: `field`, `type`, `values_bytes`, `bloom_bytes`,
//! `dict_items`, `dict_bytes`, `rows`, `_stream`, `part_path`.
//!
//! Both Go branches are ported: for block-search-backed blocks (Go
//! `br.bs != nil`) the real per-column statistics are reported via the
//! `BlockResult::block_stats_*` accessors (`getStreamStr`/`partPath`,
//! `getColumnHeader().valuesSize/bloomFilterSize/valuesDict`,
//! `timestampsHeader.blockSize`); for pipeline-generated blocks the in-memory
//! branch reports `_stream` `"{}"`, `part_path` `"inmemory"` and zero sizes
//! (const rows report the const value's byte length, matching Go).
//!
//! PORT NOTE: Go flushes the writer every 500 rows; here each input block emits
//! exactly one output block (column count per block is tiny), which is
//! behaviorally equivalent for the next pipe.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::marshal_uint64_string;

/// The `| block_stats` pipe.
pub struct PipeBlockStats {}

/// Builds a [`PipeBlockStats`] (Go `parsePipeBlockStats` result).
pub(crate) fn new_pipe_block_stats() -> PipeBlockStats {
    PipeBlockStats {}
}

impl Pipe for PipeBlockStats {
    /// Port of Go `pipeBlockStats.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        "block_stats".to_string()
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("*");
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeBlockStatsProcessor { pp_next })
    }
}

struct PipeBlockStatsProcessor {
    pp_next: Arc<dyn PipeProcessor>,
}

/// Column order emitted by `block_stats` (Go `pipeBlockStatsWriteContext`).
const COLUMN_NAMES: [&str; 9] = [
    "field",
    "type",
    "values_bytes",
    "bloom_bytes",
    "dict_items",
    "dict_bytes",
    "rows",
    "_stream",
    "part_path",
];

struct RowValues {
    column_name: String,
    column_type: String,
    values_size: u64,
    bloom_size: u64,
    dict_items: u64,
    dict_size: u64,
}

impl PipeProcessor for PipeBlockStatsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        // Go: `stream, partPath = "{}", "inmemory"` unless `br.bs != nil`.
        let (stream, part_path) = br
            .block_stats_stream_and_part_path()
            .unwrap_or_else(|| ("{}".to_string(), "inmemory".to_string()));
        let rows_len = br.rows_len() as u64;

        let cols = br.get_columns();
        let mut rows: Vec<RowValues> = Vec::with_capacity(cols.len());
        for &c in &cols {
            let name = br.column_name(c).to_string();
            if br.column_is_const(c) {
                let values_size = br.column_get_value_at_row(c, 0).len() as u64;
                rows.push(RowValues {
                    column_name: name,
                    column_type: "const".to_string(),
                    values_size,
                    bloom_size: 0,
                    dict_items: 0,
                    dict_size: 0,
                });
            } else if br.column_is_time(c) {
                rows.push(RowValues {
                    column_name: name,
                    column_type: "time".to_string(),
                    values_size: br.block_stats_timestamps_block_size(),
                    bloom_size: 0,
                    dict_items: 0,
                    dict_size: 0,
                });
            } else {
                let is_dict = br.column_value_type(c) == crate::values_encoder::ValueType::DICT;
                match br.block_stats_column_header(&name, is_dict) {
                    Some((values_size, bloom_size, dict_items, dict_size)) => {
                        rows.push(RowValues {
                            column_name: name,
                            column_type: br.column_value_type(c).to_string(),
                            values_size,
                            bloom_size,
                            dict_items,
                            dict_size,
                        });
                    }
                    None => {
                        rows.push(RowValues {
                            column_name: name,
                            column_type: "inmemory".to_string(),
                            values_size: 0,
                            bloom_size: 0,
                            dict_items: 0,
                            dict_size: 0,
                        });
                    }
                }
            }
        }

        let mut rcs: Vec<ResultColumn> = COLUMN_NAMES
            .iter()
            .map(|name| ResultColumn {
                name: name.to_string(),
                values: Vec::new(),
            })
            .collect();

        for r in &rows {
            rcs[0].add_value(r.column_name.as_bytes());
            rcs[1].add_value(r.column_type.as_bytes());
            add_uint64(&mut rcs[2], r.values_size);
            add_uint64(&mut rcs[3], r.bloom_size);
            add_uint64(&mut rcs[4], r.dict_items);
            add_uint64(&mut rcs[5], r.dict_size);
            add_uint64(&mut rcs[6], rows_len);
            rcs[7].add_value(stream.as_bytes());
            rcs[8].add_value(part_path.as_bytes());
        }

        let rows_count = rows.len();
        let mut out = BlockResult::default();
        out.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(worker_id, &mut out);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn add_uint64(rc: &mut ResultColumn, n: u64) {
    let mut b = Vec::new();
    marshal_uint64_string(&mut b, n);
    rc.add_value(&b);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;
    use std::sync::Mutex;

    struct Collector {
        blocks: Mutex<Vec<Vec<Field>>>,
    }
    impl Collector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                blocks: Mutex::new(Vec::new()),
            })
        }
    }
    impl PipeProcessor for Collector {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_string(),
                    });
                }
                out.push(fields);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn get<'a>(row: &'a [Field], name: &str) -> &'a str {
        row.iter()
            .find(|f| f.name == name)
            .map(|f| f.value.as_str())
            .unwrap_or("")
    }

    #[test]
    fn test_block_stats_const_and_varying() {
        // Column "k" is constant across rows (→ const); column "v" varies
        // (→ inmemory).
        let pipe = new_pipe_block_stats();
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[
            vec![field("k", "same"), field("v", "1")],
            vec![field("k", "same"), field("v", "2")],
            vec![field("k", "same"), field("v", "3")],
        ]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        // One output row per input column.
        assert_eq!(out.len(), 2);
        for r in out.iter() {
            assert_eq!(get(r, "rows"), "3");
            assert_eq!(get(r, "_stream"), "{}");
            assert_eq!(get(r, "part_path"), "inmemory");
            match get(r, "field") {
                "k" => {
                    assert_eq!(get(r, "type"), "const");
                    // const value "same" is 4 bytes.
                    assert_eq!(get(r, "values_bytes"), "4");
                }
                "v" => {
                    assert_eq!(get(r, "type"), "inmemory");
                    assert_eq!(get(r, "values_bytes"), "0");
                }
                other => panic!("unexpected field {other}"),
            }
            assert_eq!(get(r, "bloom_bytes"), "0");
            assert_eq!(get(r, "dict_items"), "0");
            assert_eq!(get(r, "dict_bytes"), "0");
        }
    }

    #[test]
    fn test_block_stats_empty_block_emits_nothing() {
        let pipe = new_pipe_block_stats();
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        assert!(sink.blocks.lock().unwrap().is_empty());
    }

    #[test]
    fn test_block_stats_to_string() {
        assert_eq!(new_pipe_block_stats().to_string(), "block_stats");
    }

    /// File-part branch (Go `br.bs != nil`): after the in-memory parts are
    /// flushed to disk, `block_stats` must report the real `_stream`,
    /// `part_path` and per-column on-disk statistics instead of the
    /// `"inmemory"` placeholders.
    #[test]
    fn test_block_stats_file_part_end_to_end() {
        use std::sync::Mutex;

        use crate::log_rows::get_log_rows;
        use crate::parser::ParseQuery;
        use crate::storage::{Storage, StorageConfig};
        use crate::storage_search::{DataBlock, WriteDataBlockFn};
        use crate::tenant_id::TenantID;

        let path = std::env::temp_dir().join(format!(
            "esl-logstorage-blockstats-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let tenant = TenantID {
            account_id: 0,
            project_id: 0,
        };

        let base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        // 10 unique `_msg` values keep that column above the 8-unique-values
        // dict threshold (→ `string` type); `level` stays dict with 2 items.
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        for i in 0..10 {
            let mut fields = vec![
                field("_msg", &format!("unique message number {i}")),
                field("host", "node-1"),
                field("level", if i % 2 == 0 { "info" } else { "warn" }),
            ];
            lr.must_add(tenant, base + i as i64, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();
        // Closing flushes the in-memory parts to file parts; reopen so the
        // search below reads them back from disk.
        s.must_close();
        let s = Storage::must_open_storage(&path, &cfg);

        let q = ParseQuery("* | block_stats").expect("parse query");
        let captured: Arc<Mutex<Vec<Vec<Field>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
            let n = db.rows_count();
            let mut out = cap.lock().unwrap();
            let columns = db.get_columns(false).to_vec();
            for i in 0..n {
                let mut row = Vec::with_capacity(columns.len());
                for c in &columns {
                    row.push(Field {
                        name: c.name.clone(),
                        value: String::from_utf8_lossy(&c.values[i]).into_owned(),
                    });
                }
                out.push(row);
            }
        });
        s.run_query(&[tenant], &q, write).expect("run_query");

        let rows = captured.lock().unwrap();
        assert!(!rows.is_empty(), "block_stats must emit per-column rows");
        let storage_path = path.to_string_lossy().into_owned();
        for r in rows.iter() {
            assert!(
                get(r, "part_path").starts_with(&storage_path),
                "part_path must be the real on-disk part path, got {:?}",
                get(r, "part_path")
            );
            assert_eq!(get(r, "_stream"), r#"{host="node-1"}"#);
            assert_eq!(get(r, "rows"), "10");
        }
        let by_field = |name: &str| -> &Vec<Field> {
            rows.iter()
                .find(|r| get(r, "field") == name)
                .unwrap_or_else(|| panic!("missing block_stats row for column {name}"))
        };

        // `_time` reports the timestamps block size.
        let time_row = by_field("_time");
        assert_eq!(get(time_row, "type"), "time");
        assert!(get(time_row, "values_bytes").parse::<u64>().unwrap() > 0);

        // `_msg` holds 5 distinct values → a real column type with on-disk
        // values and a bloom filter.
        let msg_row = by_field("_msg");
        assert_eq!(get(msg_row, "type"), "string");
        assert!(get(msg_row, "values_bytes").parse::<u64>().unwrap() > 0);
        assert!(get(msg_row, "bloom_bytes").parse::<u64>().unwrap() > 0);

        // `level` holds 2 distinct values → dict-encoded with 2 dict items.
        let level_row = by_field("level");
        assert_eq!(get(level_row, "type"), "dict");
        assert_eq!(get(level_row, "dict_items"), "2");
        assert_eq!(
            get(level_row, "dict_bytes"),
            (("info".len() + "warn".len()) as u64).to_string()
        );

        drop(rows);
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }
}
