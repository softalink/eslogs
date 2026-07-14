//! Port of EsLogs `lib/logstorage/net_query_runner.go`.
//!
//! `NetQueryRunner` drives a *distributed* query: it splits a parsed query into
//! a part that runs at remote storage nodes (`q_remote`) and a chain of pipes
//! that run locally on the results streamed back (`pipes_local`), then executes
//! them with `run_pipes`. Its entry points are [`new_net_query_runner`] (build
//! the split + eagerly resolve remote subqueries) and [`NetQueryRunner::run`]
//! (execute).
//!
//! PORT NOTE — single-node: EsLogs runs this exact code path even on a
//! single node. The `net_search` closure passed to `run` is the only remote
//! seam; the cluster transport (eslselect → eslstorage over
//! `/internal/select/query`) lives in `esl-storage/src/netselect.rs`, which
//! wires this runner into its `Storage::run_query`.
//!
//! PORT NOTE — `QueryContext`: Go threads a `*QueryContext` (context /
//! tenantIDs / query stats) through the runner. The ported runner passes the
//! `Query` alone; tenant ids, query stats and partial-response policy are
//! captured by the caller's `run_net_query` / `net_search` closures (matching
//! the netselect module convention). Context cancellation is likewise handled
//! by the caller (see the netselect PORT NOTE); `run_pipes` still owns the
//! per-run internal stop flag used by the pipes themselves.
//!
//! PORT NOTE — subquery init: Go `initSubqueries(qctx, runQuery, eagerExecute)`
//! resolves `in(...)` values and `join` maps eagerly and, with
//! `eagerExecute == true` (the remote half), also inlines `union` subquery
//! results as `union rows(...)`. The port resolves local-half `union`
//! subqueries eagerly as well ([`Pipe::init_union_query_eager`]) instead of
//! wiring Go's lazy `runQuery` callback into the pipe: the lazy wiring
//! (`RunUnionQueryFn`) requires a `'static` callback, while the runner borrows
//! the caller's `run_net_query`. The observable results are identical; only
//! the execution moment differs (runner construction vs. pipe flush).
//!
//! PORT NOTE — shared helpers: `run_pipes`, the `BlockResult` → `DataBlock`
//! sink (`BlockResultWriter`), `initFilterInValues` and `isLastPipeUniq` are
//! reused from `storage_search.rs` (Go shares the same functions). The
//! subquery *executors* (`getFieldValuesGeneric` / `getRows`) are duplicated
//! here generic over [`RunNetQueryFn`] instead of the local `Storage`,
//! mirroring how Go parameterizes them over `runQuery`.
//! `Query.addFieldsFilters` / `toFieldsFilters` / `getNeededColumns(pipes)`
//! (parser.go) are hosted here to keep the cluster-split surface in one
//! module.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::block_result::BlockResult;
use crate::parser::{ParseQueryAtTimestamp, Query};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::Field;
use crate::storage_search::{
    BlockResultWriter, DataBlock, WriteDataBlockFn, init_filter_in_values_for_query,
    is_last_pipe_uniq, run_pipes,
};

/// Runs the given (distributed) query and passes the result data blocks to the
/// write callback (Go `RunNetQueryFunc`).
pub type RunNetQueryFn<'a> = dyn Fn(&Query, WriteDataBlockFn) -> Result<(), String> + Sync + 'a;

/// A runner for a distributed query (Go `NetQueryRunner`).
pub struct NetQueryRunner {
    /// The query to execute at remote storage nodes (Go `qRemote`).
    q_remote: Query,

    /// Pipes to execute locally after receiving the data from remote storage
    /// nodes (Go `pipesLocal`).
    pipes_local: Vec<Box<dyn Pipe>>,

    /// The sink for the resulting data blocks (Go `writeBlock`, already
    /// adapted from the caller's `WriteDataBlockFunc`).
    write_block: Arc<dyn PipeProcessor>,
}

/// Creates a new [`NetQueryRunner`] for the given query
/// (Go `NewNetQueryRunner`).
///
/// `run_net_query` is used for running the distributed subqueries embedded in
/// `q`. The query results are sent to `write_net_block`.
pub fn new_net_query_runner(
    q: &Query,
    run_net_query: &RunNetQueryFn<'_>,
    write_net_block: WriteDataBlockFn,
) -> Result<NetQueryRunner, String> {
    let (mut q_remote, pipes_local) = split_query_to_remote_and_local(q);

    // Eagerly execute all the subqueries for the remote query
    // and replace them with the query results directly in q_remote.
    // This is needed for proper propagation of subquery results to remote
    // storage nodes.
    init_subqueries_net(&mut q_remote, run_net_query)?;

    // Initialize subqueries inside the local parts.
    let mut q_local = match ParseQueryAtTimestamp("*", q.get_timestamp()) {
        Ok(q_local) => q_local,
        Err(err) => {
            esl_common::panicf!("BUG: cannot parse '*' query: {err}");
            unreachable!()
        }
    };
    q_local.pipes = pipes_local;
    init_subqueries_net(&mut q_local, run_net_query)?;

    Ok(NetQueryRunner {
        q_remote,
        pipes_local: q_local.pipes,
        write_block: Arc::new(BlockResultWriter { f: write_net_block }),
    })
}

impl NetQueryRunner {
    /// Runs the query (Go `NetQueryRunner.Run`).
    ///
    /// The `concurrency` limits the number of parallel workers processing the
    /// query results at the local host.
    ///
    /// `net_search` must execute the given query at remote storage nodes and
    /// pass the results to the given write callback.
    pub fn run(
        &self,
        concurrency: usize,
        net_search: impl FnOnce(&AtomicBool, &Query, WriteDataBlockFn) -> Result<(), String>,
    ) -> Result<(), String> {
        run_pipes(
            &self.pipes_local,
            concurrency,
            |stop, head| {
                // Go `writeBlockToPipes.newDataBlockWriter()`: adapt the
                // incoming DataBlocks into BlockResults for the local pipes.
                let head = Arc::clone(head);
                let write_net_block: WriteDataBlockFn =
                    Arc::new(move |worker_id, db: &mut DataBlock| {
                        if db.rows_count() == 0 {
                            return;
                        }
                        let mut br = BlockResult::default();
                        db.init_block_result(&mut br);
                        head.write_block(worker_id, &mut br);
                    });
                net_search(stop, &self.q_remote, write_net_block)
            },
            Arc::clone(&self.write_block),
        )
    }
}

/// Splits `q` into a remotely executed query and locally executed pipes
/// (Go `splitQueryToRemoteAndLocal`).
pub(crate) fn split_query_to_remote_and_local(q: &Query) -> (Query, Vec<Box<dyn Pipe>>) {
    let timestamp = q.get_timestamp();
    let mut q_remote = q.clone(timestamp);
    // Go `qRemote.enablePrintOptions()`.
    //
    // PORT NOTE: Go marks the query and all its subqueries via
    // `visitSubqueries`; the Rust pipes store subqueries as rendered text, so
    // only the top-level query is marked.
    q_remote.opts.need_print = true;

    let (pipes_remote, pipes_local) = get_remote_and_local_pipes(&q_remote);
    q_remote.drop_all_pipes();
    q_remote.pipes = pipes_remote;

    if !q_remote.is_fixed_output_fields_order() {
        // Limit fields to select at the remote storage if the output fields
        // aren't fixed.
        let pf = get_needed_columns(&pipes_local);
        add_fields_filters(&mut q_remote, &pf);
    }

    (q_remote, pipes_local)
}

/// Return type of [`get_remote_and_local_pipes`]: the remote pipe chain and
/// the local pipe chain.
type RemoteAndLocalPipes = (Vec<Box<dyn Pipe>>, Vec<Box<dyn Pipe>>);

/// Go `getRemoteAndLocalPipes`.
fn get_remote_and_local_pipes(q: &Query) -> RemoteAndLocalPipes {
    let timestamp = q.get_timestamp();

    let mut pipes_remote: Vec<Box<dyn Pipe>> = Vec::new();
    let mut pipes_local: Vec<Box<dyn Pipe>> = Vec::new();

    for (i, p) in q.pipes.iter().enumerate() {
        let (p_remote, ps_local) = p.split_to_remote_and_local(timestamp);
        if let Some(p_remote) = p_remote {
            pipes_remote.push(p_remote);
            if ps_local.is_empty() {
                continue;
            }
        } else if ps_local.is_empty() {
            esl_common::panicf!("BUG: psLocal must be non non-empty here");
        }

        pipes_local.extend(ps_local);
        // Go appends q.pipes[i+1:] by pointer; the port clones the trailing
        // pipes via their rendered text.
        for p_tail in &q.pipes[i + 1..] {
            pipes_local.push(crate::pipe::clone_pipe(p_tail.as_ref(), timestamp));
        }
        break;
    }

    (pipes_remote, pipes_local)
}

/// Go `getNeededColumns(pipes)` (parser.go): the columns needed at the input
/// of the given pipe chain.
fn get_needed_columns(pipes: &[Box<dyn Pipe>]) -> prefix_filter::Filter {
    let mut pf = prefix_filter::Filter::default();
    pf.add_allow_filter("*");

    for p in pipes.iter().rev() {
        p.update_needed_fields(&mut pf);
    }

    pf
}

/// Go `Query.addFieldsFilters` (parser.go): appends `| delete ...` /
/// `| fields ...` pipes matching `pf` to `q`.
fn add_fields_filters(q: &mut Query, pf: &prefix_filter::Filter) {
    let q_str = format!("*{}", to_fields_filters(pf));
    let q_tmp = match ParseQueryAtTimestamp(&q_str, q.get_timestamp()) {
        Ok(q_tmp) => q_tmp,
        Err(err) => {
            esl_common::panicf!("BUG: cannot parse query with fields filters: {err}");
            unreachable!()
        }
    };
    q.pipes.extend(q_tmp.pipes);
}

/// Go `toFieldsFilters` (parser.go). Also used by the `stream_context` wiring
/// in `storage_search::init_subqueries`.
pub(crate) fn to_fields_filters(pf: &prefix_filter::Filter) -> String {
    if pf.match_nothing() {
        return " | delete *".to_string();
    }
    if pf.match_all() {
        return String::new();
    }

    let mut q_str = String::new();

    let deny_filters = pf.get_deny_filters();
    if !deny_filters.is_empty() {
        q_str += " | delete ";
        q_str += &crate::stats_count::field_names_string(&deny_filters);
    }

    let allow_filters = pf.get_allow_filters();
    if !allow_filters.is_empty() && !prefix_filter::match_all(&allow_filters) {
        q_str += " | fields ";
        q_str += &crate::stats_count::field_names_string(&allow_filters);
    }

    q_str
}

/// Go `initSubqueries` generalized over `run_net_query` (see the module
/// PORT NOTE: `union` subqueries are resolved eagerly on both halves, so the
/// Go `eagerExecute` flag disappears).
fn init_subqueries_net(q: &mut Query, run_net_query: &RunNetQueryFn<'_>) -> Result<(), String> {
    let timestamp = q.get_timestamp();

    // `in(<subquery>)` filter values (Go `initFilterInValues`).
    if q.has_filter_in_with_query() {
        // Go caches subquery results in an `inValuesCache` keyed by the
        // subquery string; the cache is folded into the closure.
        let mut cache: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        let mut get_field_values =
            |q_text: &str, field_name: &str| -> Result<Vec<Vec<u8>>, String> {
                if let Some(values) = cache.get(q_text) {
                    return Ok(values.clone());
                }
                let q_sub = ParseQueryAtTimestamp(q_text, timestamp)
                    .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
                let values = get_field_values_net(&q_sub, run_net_query, field_name)?;
                cache.insert(q_text.to_string(), values.clone());
                Ok(values)
            };
        init_filter_in_values_for_query(q, &mut get_field_values, timestamp)
            .map_err(|e| format!("cannot initialize `in` subqueries: {e}"))?;
    }

    // `join` maps (Go `initJoinMaps`).
    if q.pipes.iter().any(|p| p.is_join_pipe()) {
        let mut get_join_rows = |q_text: &str| -> Result<Vec<Vec<Field>>, String> {
            let q_sub = ParseQueryAtTimestamp(q_text, timestamp)
                .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
            get_rows_net(&q_sub, run_net_query)
        };
        for p in &mut q.pipes {
            p.init_join_map(&mut get_join_rows)
                .map_err(|e| format!("cannot initialize `join` subqueries: {e}"))?;
        }
    }

    // `union` subqueries (Go `initUnionQueries` with `eagerExecute == true`).
    if q.pipes.iter().any(|p| p.is_union_pipe()) {
        let mut get_union_rows = |q_text: &str| -> Result<Vec<Vec<Field>>, String> {
            let q_sub = ParseQueryAtTimestamp(q_text, timestamp)
                .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
            get_rows_net(&q_sub, run_net_query)
        };
        for p in &mut q.pipes {
            p.init_union_query_eager(&mut get_union_rows)
                .map_err(|e| format!("cannot initialize 'union' subqueries: {e}"))?;
        }
    }

    Ok(())
}

/// Go `getFieldValuesGeneric` over `run_net_query`: appends
/// `| uniq by (field_name)` to `q` (unless it already ends with a `uniq` pipe)
/// and collects the resulting unique values.
fn get_field_values_net(
    q: &Query,
    run_net_query: &RunNetQueryFn<'_>,
    field_name: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let q_holder;
    let q = if is_last_pipe_uniq(&q.pipes) {
        q
    } else {
        let mut q_new = q.clone(q.get_timestamp());
        let quoted_field_name = crate::parser::quote_token_if_needed(field_name);
        q_new.must_append_pipe(&format!("uniq by ({quoted_field_name})"));
        q_holder = q_new;
        &q_holder
    };

    let values: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let values_w = Arc::clone(&values);
    let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        if db.rows_count() == 0 {
            return;
        }

        let cs = db.get_columns(false);
        if cs.len() != 1 {
            esl_common::panicf!("BUG: expecting one column; got {} columns", cs.len());
        }

        let mut dst = values_w.lock().unwrap();
        // Raw byte clone: subquery values keep invalid UTF-8 byte-exact
        // (Go strings are arbitrary bytes).
        dst.extend(cs[0].values.iter().cloned());
    });

    run_net_query(q, write_block)?;

    let values = std::mem::take(&mut *values.lock().unwrap());
    Ok(values)
}

/// Go `getRows` over `run_net_query`: runs `q` and collects its result rows
/// (dropping empty-valued fields), bounded by a state-size budget of 20% of
/// the allowed memory (mirrors the storage_search.rs port of the same Go
/// function).
fn get_rows_net(q: &Query, run_net_query: &RunNetQueryFn<'_>) -> Result<Vec<Vec<Field>>, String> {
    let max_state_size = (esl_common::memory::allowed() as f64 * 0.2) as i64;
    let state_size_budget = Arc::new(AtomicI64::new(max_state_size));
    let rows: Arc<Mutex<Vec<Vec<Field>>>> = Arc::new(Mutex::new(Vec::new()));

    let rows_w = Arc::clone(&rows);
    let budget_w = Arc::clone(&state_size_budget);
    let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        if db.rows_count() == 0 {
            return;
        }
        if budget_w.load(Ordering::SeqCst) < 0 {
            // The state size is too big. Stop processing data in order to
            // avoid OOM crash.
            return;
        }

        let rows_count = db.rows_count();
        let cs = db.get_columns(false);

        let mut block_rows: Vec<Vec<Field>> = Vec::with_capacity(rows_count);
        let mut block_size = 0i64;
        for row_idx in 0..rows_count {
            let mut fields: Vec<Field> = Vec::with_capacity(cs.len());
            for c in cs {
                let v = &c.values[row_idx];
                if v.is_empty() {
                    continue;
                }
                let name = c.name.clone();
                let value = v.clone();
                block_size +=
                    (name.len() + value.len()) as i64 + 2 * std::mem::size_of::<String>() as i64;
                fields.push(Field { name, value });
            }
            block_size += std::mem::size_of::<Vec<Field>>() as i64;
            block_rows.push(fields);
        }
        budget_w.fetch_sub(block_size, Ordering::SeqCst);
        rows_w.lock().unwrap().extend(block_rows);
    });

    run_net_query(q, write_block)?;

    if state_size_budget.load(Ordering::SeqCst) < 0 {
        return Err(format!(
            "cannot load rows for [{q}] because they occupy more than {}MB of memory",
            max_state_size / (1 << 20)
        ));
    }

    let rows = std::mem::take(&mut *rows.lock().unwrap());
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of Go `TestSplitQueryToRemoteAndLocal`.
    #[test]
    fn test_split_query_to_remote_and_local() {
        fn f(q_str: &str, remote_query_expected: &str, local_pipes_expected: &str) {
            let q = ParseQueryAtTimestamp(q_str, 0)
                .unwrap_or_else(|e| panic!("cannot parse query [{q_str}]: {e}"));

            let q_str_before = q.to_string();
            let (q_remote, pipes_local) = split_query_to_remote_and_local(&q);
            let q_str_after = q.to_string();

            assert_eq!(
                q_str_before, q_str_after,
                "the query unexpectedly changed in split_query_to_remote_and_local()"
            );

            assert_eq!(
                q_remote.to_string(),
                remote_query_expected,
                "unexpected remote query for [{q_str}]"
            );

            let local_pipes = pipes_local
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(" | ");
            assert_eq!(
                local_pipes, local_pipes_expected,
                "unexpected local pipes for [{q_str}]"
            );
        }

        f("*", "*", "");
        f(
            r#"* | format "foo<bar>" | count() x"#,
            r#"* | format "foo<bar>" | stats_remote count(*) as x"#,
            "stats_local import_state(x) as x",
        );
        f(
            "foo | sort by (_time desc) | limit 30 | keep a, _time",
            "foo | sort by (_time desc) limit 30 | fields _time, a",
            "sort by (_time desc) limit 30 | fields a, _time",
        );
        f(
            "foo | sort by (_time desc) | limit 0 | keep a, _time",
            "foo | limit 0 | fields _time, a",
            "limit 0 | fields a, _time",
        );
        f(
            "foo | sort by (_time desc) | offset 0 | limit 30 | keep a, _time",
            "foo | sort by (_time desc) limit 30 | fields _time, a",
            "sort by (_time desc) limit 30 | fields a, _time",
        );
        f(
            "foo | sort by (_time desc) | offset 10 | limit 30 | keep a, _time",
            "foo | sort by (_time desc) limit 40 | fields _time, a",
            "sort by (_time desc) offset 10 limit 30 | fields a, _time",
        );

        f(
            "foo | blocks_count",
            "foo | blocks_count",
            r#"stats sum("blocks_count") as "blocks_count""#,
        );
        f(
            "foo | blocks_count as x",
            "foo | blocks_count as x",
            "stats sum(x) as x",
        );
        f("foo | block_stats", "foo | block_stats", "");
        f("foo | collapse_nums", "foo | collapse_nums", "");
        f("foo | copy a as b", "foo | copy a as b", "");
        f("foo | decolorize", "foo | decolorize", "");
        f("foo | delete x", "foo | delete x", "");
        f("foo | drop_empty_fields", "foo | drop_empty_fields", "");
        f(
            r#"foo | extract "foo<bar>baz""#,
            r#"foo | extract "foo<bar>baz""#,
            "",
        );
        f(
            r#"foo | extract_regexp "foo(?P<ip>[^;]+)""#,
            r#"foo | extract_regexp "foo(?P<ip>[^;]+)""#,
            "",
        );
        f(
            "foo | facets",
            "foo | facets 18446744073709551615",
            "stats by (field_name, field_value) sum(hits) as hits | total_stats by (field_name) count(*) as field_values_count | filter field_values_count:<=1000 | delete field_values_count | sort by (hits desc) partition by (field_name) limit 10 | sort by (field_name, hits desc, field_value) | fields field_name, field_value, hits",
        );
        f(
            "foo | field_names",
            "foo | field_names",
            "stats by (name) sum(hits) as hits",
        );
        f(
            "foo | field_names as hits",
            "foo | field_names as hitss",
            "stats by (hitss) sum(hits) as hits | rename hitss as hits",
        );
        f(
            "foo | field_values x",
            "foo | field_values x",
            "field_values_local x",
        );
        f("foo | fields x, y", "foo | fields x, y", "");
        f("foo | filter a:b", "foo a:b", "");
        f(
            "foo | first 10 by (x)",
            "foo | sort by (x) limit 10",
            "sort by (x) limit 10",
        );
        f(r#"foo | format "x<y>""#, r#"foo | format "x<y>""#, "");
        f(
            "foo | generate_sequence 10",
            "foo | delete *",
            "generate_sequence 10",
        );
        f("foo | join by (x) (bar)", "foo", "join by (x) (bar)");
        f(
            "foo | json_array_len (x) y",
            "foo | json_array_len(x) as y",
            "",
        );
        f("foo | hash(x) as y", "foo | hash(x) as y", "");
        f(
            "foo | last 10 by (x)",
            "foo | sort by (x) desc limit 10",
            "sort by (x) desc limit 10",
        );
        f("foo | len(x) as y", "foo | len(x) as y", "");
        f("foo | limit 10", "foo | limit 10", "limit 10");
        f("foo | math x+y as z", "foo | math (x + y) as z", "");
        f("foo | offset 10", "foo", "offset 10");
        f("foo | pack_json", "foo | pack_json", "");
        f("foo | pack_logfmt", "foo | pack_logfmt", "");
        f(
            "foo | query_stats",
            "foo | query_stats",
            "query_stats_local",
        );
        f("foo | rename x as y", "foo | rename x as y", "");
        f(r#"foo | replace ("x", "y")"#, "foo | replace (x, y)", "");
        f(
            r#"foo | replace_regexp ("x", "y")"#,
            "foo | replace_regexp (x, y)",
            "",
        );
        f(
            "foo | running_stats by (x) sum(y) as z",
            "foo | delete z",
            "running_stats by (x) sum(y) as z",
        );
        f("foo | sample 10", "foo | sample 10", "");
        f(r#"foo | split ",""#, r#"foo | split ",""#, "");
        f(
            "foo | stats by (x) count() as y",
            "foo | stats_remote by (x) count(*) as y",
            "stats_local by (x) import_state(y) as y",
        );
        f(
            "foo | stats by (x,a) count() as y, sum(q) as b",
            "foo | stats_remote by (x, a) count(*) as y, sum(q) as b",
            "stats_local by (x, a) import_state(y) as y, import_state(b) as b",
        );
        f(
            "foo | stream_context before 10 after 3",
            "foo",
            "stream_context before 10 after 3",
        );
        f("foo | time_add 1h", "foo | time_add 1h", "");
        f(
            "foo | top 10 by (x)",
            "foo | stats by (x) count(*) as hits",
            "stats by (x) sum(hits) as hits | first 10 by (hits desc, x) | fields x, hits",
        );
        f(
            "foo | top 10 by (x) rank",
            "foo | stats by (x) count(*) as hits",
            "stats by (x) sum(hits) as hits | first 10 by (hits desc, x) rank | fields x, hits, rank",
        );
        f(
            "foo | top 10 by (x) rank as y",
            "foo | stats by (x) count(*) as hits",
            "stats by (x) sum(hits) as hits | first 10 by (hits desc, x) rank as y | fields x, hits, y",
        );
        f(
            "foo | total_stats by (x) sum(y) as z",
            "foo | delete z",
            "total_stats by (x) sum(y) as z",
        );
        f("foo | union (bar)", "foo", "union (bar)");
        f("foo | uniq by (x)", "foo | uniq by (x)", "uniq by (x)");
        f(
            "foo | uniq by (x) limit 3",
            "foo | uniq by (x) limit 3",
            "uniq by (x) limit 3",
        );
        f(
            "foo | uniq by (x) hits",
            "foo | uniq by (x) with hits",
            "uniq_local by (x) limit 0",
        );
        f(
            "foo | uniq by (x) hits limit 5",
            "foo | uniq by (x) with hits limit 5",
            "uniq_local by (x) limit 5",
        );
        f("foo | unpack_json", "foo | unpack_json", "");
        f("foo | unpack_logfmt", "foo | unpack_logfmt", "");
        f("foo | unpack_syslog", "foo | unpack_syslog", "");
        f("foo | unpack_words", "foo | unpack_words", "");
        f("foo | unroll by (x)", "foo | unroll by (x)", "");
    }
}
