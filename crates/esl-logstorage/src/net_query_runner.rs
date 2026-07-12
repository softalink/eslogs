//! Port of EsLogs `lib/logstorage/net_query_runner.go`.
//!
//! `NetQueryRunner` drives a *distributed* query: it splits a parsed query into
//! a part that runs at remote storage nodes (`qRemote`) and a chain of pipes
//! that run locally on the results streamed back (`pipesLocal`), then executes
//! them with `runPipes`. Its only entry points are `NewNetQueryRunner` (build
//! the split + eagerly resolve remote subqueries) and `Run` (execute).
//!
//! PORT NOTE — deferred pending `splitToRemoteAndLocal` (blocking):
//! The query surface this file needs is now largely ported (`Query`,
//! `ParseQuery`, `Query::clone`/`get_timestamp`/`drop_all_pipes`/
//! `is_fixed_output_fields_order` in `parser/`, plus `run_pipes`/
//! `get_needed_columns`/`WriteDataBlockFn` in `storage_search.rs`), but the
//! core of `NewNetQueryRunner` is `splitQueryToRemoteAndLocal`, which calls
//! `pipe.splitToRemoteAndLocal` on every pipe. That method is **deliberately
//! omitted from the `Pipe` trait** for the single-node port (see pipe.rs
//! PORT NOTES) — implementing it means adding remote/local pipe splits to all
//! ~56 pipe impls (`pipe_*_remote` / `pipe_*_local` pairs). `initSubqueries`
//! (the other dependency) is likewise deferred in `storage_search.rs`.
//! Single-node EsLogs serves queries via `Storage::run_query` directly,
//! so this runner is not needed until the cluster/eslselect-over-RPC layer
//! lands. The structure is transcribed below as an executable spec so the
//! wiring is mechanical once `splitToRemoteAndLocal` exists. This module is
//! registered in `lib.rs` and intentionally exposes no symbols yet (no tests —
//! `net_query_runner::` matches zero cases and passes).
//!
//! PORT NOTE — single-node: EsLogs runs this exact code path even on a
//! single node. The `netSearch` closure passed to `Run` is the only remote
//! seam; for single-node it is a *local-only* callback that executes `qRemote`
//! against the local storage (no network hop). So the port that lands here does
//! **not** need any cluster/RPC types — it needs only the local `Query` +
//! `runPipes` surface above. The remote/RPC transport (eslselect → eslstorage)
//! belongs to the `app/` layer and stays out of `lib/logstorage`.
//!
//! Go structure being ported (see net_query_runner.go):
//!
//! ```text
//! type RunNetQueryFunc = fn(&QueryContext, WriteDataBlockFunc) -> Result<(), String>
//!
//! struct NetQueryRunner {
//!     qctx:        QueryContext,          // the query context
//!     q_remote:    Query,                 // query executed at remote storage nodes
//!     pipes_local: Vec<Box<dyn Pipe>>,    // pipes executed locally on returned data
//!     write_block: writeBlockResultFunc,  // sink for the resulting data block
//! }
//!
//! fn new_net_query_runner(qctx, run_net_query, write_net_block) -> Result<NetQueryRunner>:
//!     run_query = |qctx, write_block| run_net_query(qctx, write_block.new_data_block_writer())
//!     (q_remote, pipes_local) = split_query_to_remote_and_local(qctx.query)
//!     // eagerly execute remote subqueries so their results propagate to remote nodes
//!     q_remote = init_subqueries(qctx.with_query(q_remote), run_query, true)?
//!     // local subqueries (e.g. `union(...)`) may resolve lazily
//!     q_local = parse_query("*"); q_local.pipes = pipes_local
//!     q_local = init_subqueries(qctx.with_query(q_local), run_query, false)?
//!     write_block = write_net_block.new_block_result_writer()
//!     NetQueryRunner { qctx, q_remote, pipes_local: q_local.pipes, write_block }
//!
//! fn run(&self, ctx, concurrency, net_search):
//!     search = |stop_ch, write_to_pipes| net_search(stop_ch, &self.q_remote,
//!                                                    write_to_pipes.new_data_block_writer())
//!     run_pipes(self.qctx.with_context(ctx), &self.pipes_local, search,
//!               self.write_block, concurrency)
//!
//! fn split_query_to_remote_and_local(q) -> (Query, Vec<Box<dyn Pipe>>):
//!     q_remote = q.clone(q.get_timestamp()); q_remote.enable_print_options()
//!     (pipes_remote, pipes_local) = get_remote_and_local_pipes(&q_remote)
//!     q_remote.drop_all_pipes(); q_remote.pipes = pipes_remote
//!     if !q_remote.is_fixed_output_fields_order():
//!         // restrict remote-selected fields when output order isn't fixed
//!         q_remote.add_fields_filters(get_needed_columns(&pipes_local))
//!     (q_remote, pipes_local)
//!
//! fn get_remote_and_local_pipes(q) -> (Vec<Box<dyn Pipe>>, Vec<Box<dyn Pipe>>):
//!     for (i, p) in q.pipes: (p_remote, ps_local) = p.split_to_remote_and_local(ts)
//!         push p_remote if any; if ps_local empty and p_remote present: continue
//!         assert ps_local non-empty (Go: "BUG: psLocal must be non non-empty here")
//!         pipes_local.extend(ps_local); pipes_local.extend(q.pipes[i+1..]); break
//! ```
