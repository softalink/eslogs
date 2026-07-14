//! Port of `pipe_set_stream_fields.go` — the `| set_stream_fields ...` pipe,
//! which recomputes the `_stream` field (and clears `_stream_id`) from the set
//! of fields selected by `streamFieldFilters`, optionally gated by an `if(...)`
//! filter.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::IfFilter;
use crate::prefix_filter::{self, match_filters};
use crate::rows::Field;
use crate::stats_count_uniq::field_names_string;
use crate::stream_tags::{get_stream_tags, put_stream_tags};

/// `pipeSetStreamFields` implements `| set_stream_fields ...`.
pub struct PipeSetStreamFields {
    pub(crate) stream_field_filters: Vec<String>,

    /// Optional filter for skipping setting stream fields.
    pub(crate) iff: Option<Arc<IfFilter>>,
}

/// Constructs a `set_stream_fields` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeSetStreamFields` is lexer-dependent and deferred;
/// this constructor takes the parsed stream field filters and optional
/// `if (...)` filter directly.
pub(crate) fn new_pipe_set_stream_fields(
    stream_field_filters: Vec<String>,
    iff: Option<Arc<IfFilter>>,
) -> PipeSetStreamFields {
    PipeSetStreamFields {
        stream_field_filters,
        iff,
    }
}

impl Pipe for PipeSetStreamFields {
    /// Port of Go `pipeSetStreamFields.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    /// Go `hasFilterInWithQuery` for this pipe: checks the `if (...)` filter.
    fn has_filter_in_with_query(&self) -> bool {
        self.iff
            .as_ref()
            .is_some_and(|iff| iff.has_filter_in_with_query())
    }

    /// Go `initFilterInValues` for this pipe: rewrites the `if (...)` filter.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.init_filter_in_values(get_values, timestamp)?
        {
            self.iff = Some(Arc::new(iff_new));
        }
        Ok(())
    }

    /// Go `visitSubqueries` for this pipe: propagates into the `if (...)` filter.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.visit_subqueries_mut(timestamp, visit)
        {
            self.iff = Some(Arc::new(iff_new));
        }
    }

    fn to_string(&self) -> String {
        let mut s = "set_stream_fields".to_string();
        if let Some(iff) = &self.iff {
            s += " ";
            s += &iff.to_string();
        }
        s += " ";
        s += &field_names_string(&self.stream_field_filters);
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if !pf.match_string("_stream") {
            return;
        }

        if let Some(iff) = &self.iff {
            pf.add_allow_filters(&iff.allow_filters);
        } else {
            pf.add_deny_filter("_stream");
        }
        pf.add_allow_filters(&self.stream_field_filters);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeSetStreamFieldsProcessorShard::default()))
            .collect();
        Arc::new(PipeSetStreamFieldsProcessor {
            stream_field_filters: self.stream_field_filters.clone(),
            iff: self.iff.clone(),
            pp_next,
            shards,
        })
    }
}

struct PipeSetStreamFieldsProcessor {
    stream_field_filters: Vec<String>,
    iff: Option<Arc<IfFilter>>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeSetStreamFieldsProcessorShard>>,
}

#[derive(Default)]
struct PipeSetStreamFieldsProcessorShard {
    bm: Bitmap,
    rcs: [ResultColumn; 2],

    /// Scratch reused when building the sorted stream tags for a row.
    tags: Vec<Field>,
}

impl PipeProcessor for PipeSetStreamFieldsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();
        let rows_len = br.rows_len();

        let has_iff = self.iff.is_some();
        if let Some(iff) = &self.iff {
            shard.bm.init(rows_len);
            shard.bm.set_bits();
            iff.f.apply_to_block_result(br, &mut shard.bm);
            if shard.bm.is_zero() {
                drop(shard);
                self.pp_next.write_block(worker_id, br);
                return;
            }
        }

        // Determine the columns contributing to the stream (deduped is not
        // needed — block column names are unique).
        let cs = br.get_columns();
        let names: Vec<String> = cs.iter().map(|&c| br.column_name(c).to_string()).collect();
        let matching: Vec<(ColRef, String)> = cs
            .iter()
            .zip(names.iter())
            .filter(|(_, name)| match_filters(&self.stream_field_filters, name.as_str()))
            .map(|(&c, name)| (c, name.clone()))
            .collect();

        let stream_column = br.get_column_by_name("_stream");
        let stream_id_column = br.get_column_by_name("_stream_id");

        shard.rcs[0].name = "_stream".to_string();
        shard.rcs[1].name = "_stream_id".to_string();

        for row_idx in 0..rows_len {
            let (stream, stream_id) = if !has_iff || shard.bm.is_set_bit(row_idx) {
                let stream = set_log_stream_fields(&matching, br, row_idx, &mut shard.tags);
                (stream, Vec::new())
            } else {
                let stream = br.column_get_value_at_row(stream_column, row_idx).to_vec();
                let stream_id = br
                    .column_get_value_at_row(stream_id_column, row_idx)
                    .to_vec();
                (stream, stream_id)
            };
            shard.rcs[0].add_value(&stream);
            shard.rcs[1].add_value(&stream_id);
        }

        let rc0 = std::mem::take(&mut shard.rcs[0]);
        let rc1 = std::mem::take(&mut shard.rcs[1]);
        br.add_result_column(rc0);
        br.add_result_column(rc1);
        drop(shard);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Port of Go's `(*pipeSetStreamFieldsProcessorShard).setLogStreamFields`.
///
/// PORT NOTE: Go adds the matching fields to a pooled `StreamTags`, then
/// `sort.Sort`s it before `marshalString`. `StreamTags` exposes no public
/// sort-then-marshal helper, so this collects the mapped tags (empty values
/// skipped, empty name mapped to `_msg` — mirroring `StreamTags.Add`), sorts
/// them by the same `Field` ordering, and feeds them to `StreamTags` in sorted
/// order — producing identical output.
fn set_log_stream_fields(
    matching: &[(ColRef, String)],
    br: &mut BlockResult,
    row_idx: usize,
    tags: &mut Vec<Field>,
) -> Vec<u8> {
    tags.clear();
    for (c, name) in matching {
        let v = br.column_get_value_at_row(*c, row_idx);
        if v.is_empty() {
            continue;
        }
        let tag_name = if name.is_empty() {
            "_msg"
        } else {
            name.as_str()
        };
        tags.push(Field {
            name: tag_name.to_string(),
            value: v.to_vec(),
        });
    }

    tags.sort_by(|a, b| {
        if a.less(b) {
            std::cmp::Ordering::Less
        } else if b.less(a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });

    let mut st = get_stream_tags();
    for t in tags.iter() {
        st.add(&t.name, &t.value);
    }
    let mut buf = Vec::new();
    st.marshal_string(&mut buf);
    put_stream_tags(st);

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Filter;
    use crate::filter_and::new_filter_and;
    use crate::filter_exact::new_filter_exact;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeSetStreamFieldsSuccess` /
    // `TestParsePipeSetStreamFieldsFailure` exercise the lexer-based
    // `parsePipeSetStreamFields`, which is deferred; they are omitted until the
    // LogsQL parser is ported.

    fn set_stream_fields(filters: &[&str], iff: Option<Arc<IfFilter>>) -> PipeSetStreamFields {
        new_pipe_set_stream_fields(filters.iter().map(|s| s.to_string()).collect(), iff)
    }

    fn exact_iff(field: &str, value: &str) -> Arc<IfFilter> {
        let f: Arc<dyn Filter> = Arc::new(new_filter_exact(field, value));
        Arc::new(IfFilter::new(f))
    }

    fn and_iff(pairs: &[(&str, &str)]) -> Arc<IfFilter> {
        let filters: Vec<Box<dyn Filter>> = pairs
            .iter()
            .map(|(f, v)| Box::new(new_filter_exact(f, v)) as Box<dyn Filter>)
            .collect();
        let f: Arc<dyn Filter> = Arc::new(new_filter_and(filters));
        Arc::new(IfFilter::new(f))
    }

    #[test]
    fn test_pipe_set_stream_fields() {
        let p = set_stream_fields(&["foo", "bar"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "aaa"), ("bar", "bb"), ("baz", "c")],
                    &[
                        ("_stream_id", "asbc"),
                        ("_stream", "asdfdsafs"),
                        ("foo", "abc"),
                        ("baz", "ghkl"),
                        ("d", "foobar"),
                    ],
                ]),
            ),
            &rows(&[
                &[
                    ("_stream_id", ""),
                    ("_stream", r#"{bar="bb",foo="aaa"}"#),
                    ("foo", "aaa"),
                    ("bar", "bb"),
                    ("baz", "c"),
                ],
                &[
                    ("_stream_id", ""),
                    ("_stream", r#"{foo="abc"}"#),
                    ("foo", "abc"),
                    ("baz", "ghkl"),
                    ("d", "foobar"),
                ],
            ]),
        );

        // conditional set_stream_fields
        let p = set_stream_fields(&["foo", "bar"], Some(exact_iff("baz", "c")));
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "aaa"), ("bar", "bb"), ("baz", "c")],
                    &[
                        ("_stream_id", "asbc"),
                        ("_stream", "asdfdsafs"),
                        ("foo", "abc"),
                        ("baz", "ghkl"),
                        ("d", "foobar"),
                    ],
                ]),
            ),
            &rows(&[
                &[
                    ("_stream_id", ""),
                    ("_stream", r#"{bar="bb",foo="aaa"}"#),
                    ("foo", "aaa"),
                    ("bar", "bb"),
                    ("baz", "c"),
                ],
                &[
                    ("_stream_id", "asbc"),
                    ("_stream", "asdfdsafs"),
                    ("foo", "abc"),
                    ("baz", "ghkl"),
                    ("d", "foobar"),
                ],
            ]),
        );
    }

    #[test]
    fn test_pipe_set_stream_fields_update_needed_fields() {
        // all the needed fields
        let p = set_stream_fields(&["x", "y"], None);
        assert_needed_fields(&p, "*", "", "*", "_stream");
        let p = set_stream_fields(&["x", "y"], Some(exact_iff("f1", "a")));
        assert_needed_fields(&p, "*", "", "*", "");

        // unneeded fields do not intersect with the requested fields
        let p = set_stream_fields(&["x", "y"], None);
        assert_needed_fields(&p, "*", "f1,f2", "*", "_stream,f1,f2");
        let p = set_stream_fields(&["x", "y"], Some(exact_iff("f1", "a")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "f2");

        // unneeded fields intersect with the requested fields
        let p = set_stream_fields(&["f1", "y"], None);
        assert_needed_fields(&p, "*", "f1,f2", "*", "_stream,f2");
        let p = set_stream_fields(&["x", "f2"], Some(exact_iff("f1", "a")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "");

        // needed fields do not intersect with the requested fields
        let p = set_stream_fields(&["x", "y"], None);
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");
        let p = set_stream_fields(&["x", "y"], Some(and_iff(&[("f1", "a"), ("f3", "b")])));
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");

        // needed fields intersect with output field
        let p = set_stream_fields(&["x", "y"], None);
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");
        let p = set_stream_fields(&["x", "y"], None);
        assert_needed_fields(&p, "f1,f2,_stream", "", "f1,f2,x,y", "");
        let p = set_stream_fields(&["x", "y"], Some(and_iff(&[("f1", "a"), ("f3", "b")])));
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");
        let p = set_stream_fields(&["x", "y"], Some(and_iff(&[("f1", "a"), ("f3", "b")])));
        assert_needed_fields(&p, "f1,f2,_stream", "", "_stream,f1,f2,f3,x,y", "");
    }
}
