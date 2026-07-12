//! Port of EsLogs `app/eslstorage/netselect/netselect.go`: the
//! cluster-mode select client, which sends queries to the remote
//! `-storageNode` nodes over the `/internal/select/*` and `/internal/delete/*`
//! protocol and merges the responses.
//!
//! PORT NOTE — server-side dependency: the `/internal/select/*` and
//! `/internal/delete/*` HTTP handlers live in Go `app/eslselect/internalselect`,
//! which is not ported yet. This module ports the client side faithfully; it
//! becomes reachable end-to-end once the internalselect server handlers land in
//! esl-select.
//!
//! PORT NOTE — `QueryContext`: the Go methods take a `*logstorage.QueryContext`
//! bundling `Context` (cancellation), `TenantIDs`, `Query`, `QueryStats`,
//! `AllowPartialResponse` and `HiddenFieldsFilters`. The port passes
//! `tenant_ids` / `q` / `qs` / `allow_partial_response` explicitly:
//!   * `Context` cancellation is dropped (the std-TCP client cannot cancel an
//!     in-flight request; failed sibling queries simply run to completion);
//!   * `HiddenFieldsFilters` does not exist in the ported query surface, so the
//!     `hidden_fields_filters` request arg is always `null` (Go's zero value).
//!
//! PORT NOTE — `NetQueryRunner`: like Go, [`Storage::run_query`] splits the
//! query via `esl_logstorage::net_query_runner::new_net_query_runner` into a
//! remote part sent to every storage node (e.g. `stats` becomes
//! `stats_remote`, which exports serialized per-group states) and local pipes
//! that merge the returned per-node partial results (e.g. `stats_local`,
//! which imports those states). The per-node protocol implementation
//! (`runQuery`, the block framing and the query-stats blocks) is ported
//! faithfully.
//!
//! PORT NOTE — HTTP transport and the `esl_select_remote_send_errors_total`
//! metric: see [`crate::http_client`] and the netinsert module note; the
//! counter is kept as a plain atomic.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use esl_common::encoding as vlencoding;
use esl_common::encoding::zstd;

use esl_logstorage::delete_task::{DeleteTask, unmarshal_delete_tasks_from_json};
use esl_logstorage::net_query_runner::new_net_query_runner;
use esl_logstorage::parser::{Filter, Query};
use esl_logstorage::pipe_field_values_local::merge_values_with_hits;
use esl_logstorage::query_stats::QueryStats;
use esl_logstorage::storage_search::{DataBlock, ValueWithHits, WriteDataBlockFn};
use esl_logstorage::tenant_id::{
    TenantID, marshal_tenant_ids_to_json, unmarshal_tenant_ids_from_json,
};

use crate::http_client::{AuthConfig, HttpResponse, new_multipart_request_body};

/// FieldNamesProtocolVersion is the version of the protocol used for
/// /internal/select/field_names HTTP endpoint.
///
/// It must be updated every time the protocol changes.
pub const FIELD_NAMES_PROTOCOL_VERSION: &str = "v5";

/// FieldValuesProtocolVersion is the version of the protocol used for
/// /internal/select/field_values HTTP endpoint.
pub const FIELD_VALUES_PROTOCOL_VERSION: &str = "v5";

/// StreamFieldNamesProtocolVersion is the version of the protocol used for
/// /internal/select/stream_field_names HTTP endpoint.
pub const STREAM_FIELD_NAMES_PROTOCOL_VERSION: &str = "v5";

/// StreamFieldValuesProtocolVersion is the version of the protocol used for
/// /internal/select/stream_field_values HTTP endpoint.
pub const STREAM_FIELD_VALUES_PROTOCOL_VERSION: &str = "v5";

/// StreamsProtocolVersion is the version of the protocol used for
/// /internal/select/streams HTTP endpoint.
pub const STREAMS_PROTOCOL_VERSION: &str = "v5";

/// StreamIDsProtocolVersion is the version of the protocol used for
/// /internal/select/stream_ids HTTP endpoint.
pub const STREAM_IDS_PROTOCOL_VERSION: &str = "v5";

/// QueryProtocolVersion is the version of the protocol used for
/// /internal/select/query HTTP endpoint.
pub const QUERY_PROTOCOL_VERSION: &str = "v5";

/// DeleteRunTaskProtocolVersion is the version of the protocol used for
/// /internal/delete/run_task HTTP endpoint.
pub const DELETE_RUN_TASK_PROTOCOL_VERSION: &str = "v1";

/// DeleteStopTaskProtocolVersion is the version of the protocol used for
/// /internal/delete/stop_task HTTP endpoint.
pub const DELETE_STOP_TASK_PROTOCOL_VERSION: &str = "v1";

/// DeleteActiveTasksProtocolVersion is the version of the protocol used for
/// /internal/delete/active_tasks endpoint.
pub const DELETE_ACTIVE_TASKS_PROTOCOL_VERSION: &str = "v1";

/// Error type distinguishing unavailable backends from other errors
/// (Go wraps unavailable-backend errors into `httpserver.ErrorWithStatusCode`
/// with `http.StatusBadGateway` and detects them via `errors.As`).
#[derive(Debug, Clone)]
enum NetError {
    /// The storage node cannot be connected to (Go `isUnavailableBackendError`).
    Unavailable(String),
    Other(String),
}

impl NetError {
    fn message(&self) -> &str {
        match self {
            NetError::Unavailable(msg) | NetError::Other(msg) => msg,
        }
    }
}

/// Port of Go `isUnavailableBackendError`.
fn is_unavailable_backend_error(err: &NetError) -> bool {
    matches!(err, NetError::Unavailable(_))
}

/// Storage is a network storage for querying remote storage nodes in the
/// cluster (Go `netselect.Storage`).
pub struct Storage {
    sns: Vec<StorageNode>,

    disable_compression: bool,
}

struct StorageNode {
    /// "http" or "https" (Go derives it from the `isTLS` argument, the port
    /// from the presence of a TLS config on `ac`).
    #[allow(dead_code, reason = "parity with Go; requests derive TLS from ac")]
    scheme: &'static str,

    /// addr is TCP address of the storage node to query.
    addr: String,

    /// ac is auth config used for setting request headers such as
    /// Authorization.
    ac: AuthConfig,

    /// sendErrors counts failed send attempts for this storage node.
    send_errors: AtomicU64,
}

/// Returns new Storage for the given addrs and the given auth_cfgs
/// (Go `NewStorage`).
///
/// If disable_compression is set, then uncompressed responses are received
/// from storage nodes.
///
/// Call [`Storage::must_stop`] on the returned storage when it is no longer
/// needed.
pub fn new_storage(
    addrs: &[String],
    auth_cfgs: Vec<AuthConfig>,
    disable_compression: bool,
) -> Storage {
    let sns = addrs
        .iter()
        .zip(auth_cfgs)
        .map(|(addr, ac)| StorageNode {
            scheme: if ac.tls().is_some() { "https" } else { "http" },
            addr: addr.clone(),
            ac,
            send_errors: AtomicU64::new(0),
        })
        .collect();
    Storage {
        sns,
        disable_compression,
    }
}

impl StorageNode {
    /// Port of Go `storageNode.runQuery` — streams the data blocks returned by
    /// `/internal/select/query` into `process_block`.
    fn run_query(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        process_block: &mut dyn FnMut(&mut DataBlock),
    ) -> Result<(), NetError> {
        let args = s.get_common_args(
            QUERY_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );

        let qs_local = QueryStats::default();

        let path = "/internal/select/query";
        let response_body = self.get_response_body_for_path_and_args(path, &args)?;
        let result = self.process_query_response(s, &qs_local, &response_body, path, process_block);
        // Go: `defer qctx.QueryStats.UpdateAtomic(qsLocal)`.
        qs.update_atomic(&qs_local);
        result
    }

    /// Parses the block stream of a `/internal/select/query` response
    /// (the loop body of Go `storageNode.runQuery`).
    ///
    /// PORT NOTE: Go reads the blocks incrementally from `resp.Body`; the port
    /// parses the fully buffered response (see `http_client`).
    fn process_query_response(
        &self,
        s: &Storage,
        qs_local: &QueryStats,
        response_body: &[u8],
        req_url: &str,
        process_block: &mut dyn FnMut(&mut DataBlock),
    ) -> Result<(), NetError> {
        let mut body = response_body;
        let mut buf: Vec<u8> = Vec::new();
        let mut db = DataBlock::default();
        loop {
            if body.is_empty() {
                // The end of response stream
                return Ok(());
            }
            if body.len() < 8 {
                return Err(NetError::Other(format!(
                    "cannot read block size from {req_url:?}: unexpected end of response"
                )));
            }
            let block_len = vlencoding::unmarshal_uint64(&body[..8]);
            body = &body[8..];
            if block_len > body.len() as u64 {
                return Err(NetError::Other(format!(
                    "cannot read block with size of {block_len} bytes from {req_url:?}: only {} bytes left",
                    body.len()
                )));
            }
            let block = &body[..block_len as usize];
            body = &body[block_len as usize..];

            let src: &[u8] = if !s.disable_compression {
                buf.clear();
                if let Err(err) = zstd::decompress(&mut buf, block) {
                    return Err(NetError::Other(format!(
                        "cannot decompress data block: {err}"
                    )));
                }
                &buf
            } else {
                block
            };

            let mut src = src;
            while !src.is_empty() {
                let is_query_stats_block = src[0] == 1;
                src = &src[1..];

                if is_query_stats_block {
                    src = unmarshal_query_stats(qs_local, src).map_err(|err| {
                        NetError::Other(format!(
                            "cannot unmarshal query stats received from {req_url:?}: {err}"
                        ))
                    })?;
                    continue;
                }

                let n = db.unmarshal_inplace(src).map_err(|err| {
                    NetError::Other(format!(
                        "cannot unmarshal data block received from {req_url:?}: {err}"
                    ))
                })?;
                src = &src[n..];

                process_block(&mut db);
            }
        }
    }

    /// Port of Go `storageNode.getFieldNames`.
    fn get_field_names(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            FIELD_NAMES_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("filter".to_string(), filter.to_string()));

        self.get_values_with_hits(s, qs, "/internal/select/field_names", &args)
    }

    /// Port of Go `storageNode.getFieldValues`.
    #[allow(clippy::too_many_arguments)]
    fn get_field_values(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            FIELD_VALUES_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("field".to_string(), field_name.to_string()));
        args.push(("filter".to_string(), filter.to_string()));
        args.push(("limit".to_string(), format!("{limit}")));

        self.get_values_with_hits(s, qs, "/internal/select/field_values", &args)
    }

    /// Port of Go `storageNode.getStreamFieldNames`.
    fn get_stream_field_names(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            STREAM_FIELD_NAMES_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("filter".to_string(), filter.to_string()));

        self.get_values_with_hits(s, qs, "/internal/select/stream_field_names", &args)
    }

    /// Port of Go `storageNode.getStreamFieldValues`.
    #[allow(clippy::too_many_arguments)]
    fn get_stream_field_values(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            STREAM_FIELD_VALUES_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("field".to_string(), field_name.to_string()));
        args.push(("filter".to_string(), filter.to_string()));
        args.push(("limit".to_string(), format!("{limit}")));

        self.get_values_with_hits(s, qs, "/internal/select/stream_field_values", &args)
    }

    /// Port of Go `storageNode.getStreams`.
    fn get_streams(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            STREAMS_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("limit".to_string(), format!("{limit}")));

        self.get_values_with_hits(s, qs, "/internal/select/streams", &args)
    }

    /// Port of Go `storageNode.getStreamIDs`.
    fn get_stream_ids(
        &self,
        s: &Storage,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let mut args = s.get_common_args(
            STREAM_IDS_PROTOCOL_VERSION,
            tenant_ids,
            q,
            allow_partial_response,
        );
        args.push(("limit".to_string(), format!("{limit}")));

        self.get_values_with_hits(s, qs, "/internal/select/stream_ids", &args)
    }

    /// Port of Go `storageNode.getTenantIDs`.
    fn get_tenant_ids(&self, start: i64, end: i64) -> Result<Vec<TenantID>, NetError> {
        let args = vec![
            ("start".to_string(), format!("{start}")),
            ("end".to_string(), format!("{end}")),
        ];

        let path = "/internal/select/tenant_ids";
        let data = self.get_plain_response_body_for_path_and_args(path, &args)?;
        unmarshal_tenant_ids_from_json(&data).map_err(|err| {
            NetError::Other(format!(
                "cannot unmarshal tenantIDs received from {path:?}; data={:?}: {err}",
                String::from_utf8_lossy(&data)
            ))
        })
    }

    /// Port of Go `storageNode.getValuesWithHits`.
    fn get_values_with_hits(
        &self,
        s: &Storage,
        qs: &QueryStats,
        path: &str,
        args: &[(String, String)],
    ) -> Result<Vec<ValueWithHits>, NetError> {
        let data = self.get_response_for_path_and_args(s, path, args)?;
        unmarshal_values_with_hits(qs, &data).map_err(NetError::Other)
    }

    /// Port of Go `storageNode.getResponseForPathAndArgs` (the possibly
    /// compressed variant).
    fn get_response_for_path_and_args(
        &self,
        s: &Storage,
        path: &str,
        args: &[(String, String)],
    ) -> Result<Vec<u8>, NetError> {
        let body = self.get_response_body_for_path_and_args(path, args)?;
        if s.disable_compression {
            return Ok(body);
        }
        let mut decompressed = Vec::new();
        zstd::decompress(&mut decompressed, &body).map_err(NetError::Other)?;
        Ok(decompressed)
    }

    /// Port of Go `storageNode.getResponseBodyForPathAndArgs`.
    ///
    /// The args are encoded as `multipart/form-data` in order to avoid the
    /// 10MB limit on the `application/x-www-form-urlencoded` request body
    /// size, which would reject too long queries.
    /// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1462
    fn get_response_body_for_path_and_args(
        &self,
        path: &str,
        args: &[(String, String)],
    ) -> Result<Vec<u8>, NetError> {
        let (req_body, content_type) = new_multipart_request_body(args);
        let mut headers = vec![("Content-Type".to_string(), content_type)];
        match self.ac.get_auth_header() {
            Ok(auth) => {
                if !auth.is_empty() {
                    headers.push(("Authorization".to_string(), auth));
                }
            }
            Err(err) => {
                return Err(NetError::Other(format!(
                    "cannot set auth headers at {path:?}: {err}"
                )));
            }
        }

        // send the request to the storage node
        let resp: HttpResponse = match crate::http_client::do_request(
            &self.addr,
            self.ac.tls(),
            "POST",
            path,
            &headers,
            Some(&req_body),
        ) {
            Ok(resp) => resp,
            Err(err) => {
                // Mirror Go: wrap connection errors so `getFirstError` can
                // differentiate unavailable backends from configuration errors.
                // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/576
                return Err(NetError::Unavailable(format!(
                    "cannot connect to storage node at {:?}: {err}",
                    self.addr
                )));
            }
        };

        if resp.status_code != 200 {
            return Err(NetError::Other(format!(
                "unexpected response status code from {path:?} at {:?}: {}; want 200; response: {:?}",
                self.addr,
                resp.status_code,
                String::from_utf8_lossy(&resp.body)
            )));
        }

        Ok(resp.body)
    }

    /// Port of Go `storageNode.deleteRunTask`.
    fn delete_run_task(
        &self,
        task_id: &str,
        timestamp: i64,
        tenant_ids: &[TenantID],
        f: &Filter,
    ) -> Result<(), NetError> {
        let args = vec![
            (
                "version".to_string(),
                DELETE_RUN_TASK_PROTOCOL_VERSION.to_string(),
            ),
            ("task_id".to_string(), task_id.to_string()),
            ("timestamp".to_string(), format!("{timestamp}")),
            (
                "tenant_ids".to_string(),
                String::from_utf8_lossy(&marshal_tenant_ids_to_json(tenant_ids)).into_owned(),
            ),
            ("filter".to_string(), f.to_string()),
        ];

        let path = "/internal/delete/run_task";
        let data = self.get_plain_response_body_for_path_and_args(path, &args)?;
        if !data.is_empty() {
            return Err(NetError::Other(format!(
                "unexpected response body received from {path:?}: {:?}",
                String::from_utf8_lossy(&data)
            )));
        }

        Ok(())
    }

    /// Port of Go `storageNode.deleteStopTask`.
    fn delete_stop_task(&self, task_id: &str) -> Result<(), NetError> {
        let args = vec![
            (
                "version".to_string(),
                DELETE_STOP_TASK_PROTOCOL_VERSION.to_string(),
            ),
            ("task_id".to_string(), task_id.to_string()),
        ];

        let path = "/internal/delete/stop_task";
        let data = self.get_plain_response_body_for_path_and_args(path, &args)?;
        if !data.is_empty() {
            return Err(NetError::Other(format!(
                "unexpected response body received from {path:?}: {:?}",
                String::from_utf8_lossy(&data)
            )));
        }

        Ok(())
    }

    /// Port of Go `storageNode.deleteActiveTasks`.
    fn delete_active_tasks(&self) -> Result<Vec<DeleteTask>, NetError> {
        let args = vec![(
            "version".to_string(),
            DELETE_ACTIVE_TASKS_PROTOCOL_VERSION.to_string(),
        )];

        let path = "/internal/delete/active_tasks";
        let data = self.get_plain_response_body_for_path_and_args(path, &args)?;

        unmarshal_delete_tasks_from_json(&data).map_err(|err| {
            NetError::Other(format!(
                "cannot parse response from {path:?}: {err}; response body: {:?}",
                String::from_utf8_lossy(&data)
            ))
        })
    }

    /// Port of Go `storageNode.getPlainResponseBodyForPathAndArgs`
    /// (uncompressed responses).
    fn get_plain_response_body_for_path_and_args(
        &self,
        path: &str,
        args: &[(String, String)],
    ) -> Result<Vec<u8>, NetError> {
        self.get_response_body_for_path_and_args(path, args)
    }

    /// Port of Go `storageNode.handleError`.
    ///
    /// PORT NOTE: Go additionally cancels the remaining parallel requests via
    /// `cancel()` and suppresses errors that arrive after the context is done;
    /// the port has no cancellation (see the module PORT NOTE), so sibling
    /// requests run to completion and the error is returned as-is.
    fn handle_error(&self, err: Option<NetError>) -> Option<NetError> {
        let err = err?;
        self.send_errors.fetch_add(1, Ordering::Relaxed);
        Some(err)
    }
}

impl Storage {
    /// Stops the s (Go `MustStop`).
    pub fn must_stop(&self) {
        // Nothing to do: the nodes hold no background resources in the port.
    }

    /// Runs the given query and calls write_block for the returned data blocks
    /// (Go `RunQuery`): the query is split via `NetQueryRunner` into a remote
    /// part executed at every storage node and local pipes merging the
    /// per-node partial results.
    pub fn run_query(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        write_block: WriteDataBlockFn,
    ) -> Result<(), String> {
        // Go passes `s.RunQuery` (the full distributed runner) so subqueries
        // embedded in `q` are split recursively as well.
        let run_net_query = |q_sub: &Query, wb: WriteDataBlockFn| -> Result<(), String> {
            self.run_query(qs, tenant_ids, q_sub, allow_partial_response, wb)
        };
        let nqr = new_net_query_runner(q, &run_net_query, write_block)?;

        // PORT NOTE: Go's local pipe workers adapt dynamically to any workerID
        // (`atomicutil.Slice`), and the per-node responses are streamed with
        // workerID == nodeIdx; the ported pipe processors size their shards up
        // front, so the concurrency must cover every node index.
        let concurrency = q.get_concurrency().max(self.sns.len());
        nqr.run(concurrency, |_stop, q_remote, wb| {
            // PORT NOTE: the stop token is unused — the std-TCP client cannot
            // cancel in-flight node requests (see the module PORT NOTE).
            self.run_query_internal(qs, tenant_ids, q_remote, allow_partial_response, wb)
        })
    }

    /// Fans the (already split) query out to every storage node
    /// (Go `Storage.runQuery`).
    fn run_query_internal(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        write_block: WriteDataBlockFn,
    ) -> Result<(), String> {
        let errs = self.for_each_node(|node_idx, sn| {
            let mut process_block = |db: &mut DataBlock| {
                write_block(node_idx, db);
            };
            sn.run_query(
                self,
                qs,
                tenant_ids,
                q,
                allow_partial_response,
                &mut process_block,
            )
            .err()
        });
        get_first_error(errs, allow_partial_response)
    }

    /// Executes the query and returns field names seen in results
    /// (Go `GetFieldNames`).
    ///
    /// If the filter is non-empty, then only the field names containing the
    /// filter substring are returned.
    pub fn get_field_names(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(0, false, allow_partial_response, |sn| {
            sn.get_field_names(self, qs, tenant_ids, q, allow_partial_response, filter)
        })
    }

    /// Executes the query and returns unique values for the field_name seen in
    /// results (Go `GetFieldValues`).
    ///
    /// If the filter is non-empty, then only the field values containing the
    /// filter substring are returned.
    ///
    /// If limit > 0, then up to limit unique values are returned.
    #[allow(clippy::too_many_arguments)]
    pub fn get_field_values(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(limit, true, allow_partial_response, |sn| {
            sn.get_field_values(
                self,
                qs,
                tenant_ids,
                q,
                allow_partial_response,
                field_name,
                filter,
                limit,
            )
        })
    }

    /// Executes the query and returns stream field names seen in results
    /// (Go `GetStreamFieldNames`).
    pub fn get_stream_field_names(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(0, false, allow_partial_response, |sn| {
            sn.get_stream_field_names(self, qs, tenant_ids, q, allow_partial_response, filter)
        })
    }

    /// Executes the query and returns stream field values for the given
    /// field_name seen in results (Go `GetStreamFieldValues`).
    #[allow(clippy::too_many_arguments)]
    pub fn get_stream_field_values(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(limit, true, allow_partial_response, |sn| {
            sn.get_stream_field_values(
                self,
                qs,
                tenant_ids,
                q,
                allow_partial_response,
                field_name,
                filter,
                limit,
            )
        })
    }

    /// Executes the query and returns streams seen in query results
    /// (Go `GetStreams`).
    pub fn get_streams(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(limit, true, allow_partial_response, |sn| {
            sn.get_streams(self, qs, tenant_ids, q, allow_partial_response, limit)
        })
    }

    /// Executes the query and returns streamIDs seen in query results
    /// (Go `GetStreamIDs`).
    pub fn get_stream_ids(
        &self,
        qs: &QueryStats,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_values_with_hits(limit, true, allow_partial_response, |sn| {
            sn.get_stream_ids(self, qs, tenant_ids, q, allow_partial_response, limit)
        })
    }

    /// Starts deletion of logs for the given filter f at the given tenant_ids
    /// (Go `DeleteRunTask`).
    pub fn delete_run_task(
        &self,
        task_id: &str,
        timestamp: i64,
        tenant_ids: &[TenantID],
        f: &Filter,
    ) -> Result<(), String> {
        // Return an error to the caller when at least a single storage node is
        // unavailable. This improves awareness of the caller about unavailable
        // storage nodes. If some storage node is unavailable, then the deletion
        // task can start on arbitrary number of the remaining available nodes.
        // It is OK to re-run the delete task in this case.
        let allow_partial_response = false;

        let errs =
            self.for_each_node(|_, sn| sn.delete_run_task(task_id, timestamp, tenant_ids, f).err());
        get_first_error(errs, allow_partial_response)
    }

    /// Stops the delete task with the given task_id (Go `DeleteStopTask`).
    pub fn delete_stop_task(&self, task_id: &str) -> Result<(), String> {
        // Return an error to the caller when at least a single storage node is
        // unavailable (the deletion task can remain uncanceled on such nodes;
        // it is OK to stop the delete task multiple times in this case).
        let allow_partial_response = false;

        let errs = self.for_each_node(|_, sn| sn.delete_stop_task(task_id).err());
        get_first_error(errs, allow_partial_response)
    }

    /// Returns the list of active delete tasks started via delete_run_task
    /// (Go `DeleteActiveTasks`).
    pub fn delete_active_tasks(&self) -> Result<Vec<DeleteTask>, String> {
        // Return an error when at least a single storage node is unavailable,
        // since this prevents from returning the full list of active tasks.
        let allow_partial_response = false;

        let results: Mutex<Vec<Vec<DeleteTask>>> = Mutex::new(Vec::new());
        let errs = self.for_each_node(|_, sn| match sn.delete_active_tasks() {
            Ok(tasks) => {
                results.lock().unwrap().push(tasks);
                None
            }
            Err(err) => Some(err),
        });
        get_first_error(errs, allow_partial_response)?;

        // Merge tasks received from storage nodes.
        let mut merged: Vec<DeleteTask> = Vec::new();
        for tasks in results.into_inner().unwrap() {
            for dt in tasks {
                if !merged.iter().any(|t| t.task_id == dt.task_id) {
                    merged.push(dt);
                }
            }
        }

        Ok(merged)
    }

    /// Returns tenantIDs for the given start and end (Go `GetTenantIDs`).
    pub fn get_tenant_ids(&self, start: i64, end: i64) -> Result<Vec<TenantID>, String> {
        // Return an error when at least a single storage node is unavailable,
        // since this may result in incomplete list of the returned tenantIDs.
        let allow_partial_response = false;

        let results: Mutex<Vec<Vec<TenantID>>> = Mutex::new(Vec::new());
        let errs = self.for_each_node(|_, sn| match sn.get_tenant_ids(start, end) {
            Ok(tenant_ids) => {
                results.lock().unwrap().push(tenant_ids);
                None
            }
            Err(err) => Some(err),
        });
        get_first_error(errs, allow_partial_response)?;

        // Deduplicate tenantIDs.
        let mut merged: Vec<TenantID> = Vec::new();
        for tenant_ids in results.into_inner().unwrap() {
            for tenant_id in tenant_ids {
                if !merged.iter().any(|t| t.equal(&tenant_id)) {
                    merged.push(tenant_id);
                }
            }
        }

        Ok(merged)
    }

    /// Port of Go `Storage.getValuesWithHits`.
    fn get_values_with_hits(
        &self,
        limit: u64,
        reset_hits_on_limit_exceeded: bool,
        allow_partial_response: bool,
        callback: impl Fn(&StorageNode) -> Result<Vec<ValueWithHits>, NetError> + Sync,
    ) -> Result<Vec<ValueWithHits>, String> {
        let results: Mutex<Vec<Vec<ValueWithHits>>> = Mutex::new(Vec::new());
        let errs = self.for_each_node(|_, sn| match callback(sn) {
            Ok(vhs) => {
                results.lock().unwrap().push(vhs);
                None
            }
            Err(err) => Some(err),
        });
        get_first_error(errs, allow_partial_response)?;

        let results = results.into_inner().unwrap();
        Ok(merge_values_with_hits(
            results,
            limit,
            reset_hits_on_limit_exceeded,
        ))
    }

    /// Fans a callback out to every node in parallel and collects the per-node
    /// errors (Go: the repeated `sync.WaitGroup` + `errs[nodeIdx]` pattern
    /// combined with `storageNode.handleError`).
    fn for_each_node(
        &self,
        callback: impl Fn(usize, &StorageNode) -> Option<NetError> + Sync,
    ) -> Vec<Option<NetError>> {
        let mut errs: Vec<Option<NetError>> = Vec::with_capacity(self.sns.len());
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(self.sns.len());
            for (node_idx, sn) in self.sns.iter().enumerate() {
                let callback = &callback;
                handles.push(scope.spawn(move || {
                    let err = callback(node_idx, sn);
                    sn.handle_error(err)
                }));
            }
            for h in handles {
                errs.push(h.join().expect("netselect worker panicked"));
            }
        });
        errs
    }

    /// Port of Go `storageNode.getCommonArgs` (hoisted to `Storage` since the
    /// only per-node input, `disableCompression`, lives on the Storage).
    fn get_common_args(
        &self,
        version: &str,
        tenant_ids: &[TenantID],
        q: &Query,
        allow_partial_response: bool,
    ) -> Vec<(String, String)> {
        // ATTENTION: the *ProtocolVersion consts must be incremented every time
        // the set of common args changes or its format changes.
        vec![
            ("version".to_string(), version.to_string()),
            (
                "tenant_ids".to_string(),
                String::from_utf8_lossy(&marshal_tenant_ids_to_json(tenant_ids)).into_owned(),
            ),
            ("query".to_string(), q.to_string()),
            ("timestamp".to_string(), format!("{}", q.get_timestamp())),
            (
                "disable_compression".to_string(),
                format!("{}", self.disable_compression),
            ),
            (
                "allow_partial_response".to_string(),
                format!("{allow_partial_response}"),
            ),
            // PORT NOTE: HiddenFieldsFilters is not carried by the ported query
            // surface; "null" matches Go's marshaled zero value.
            ("hidden_fields_filters".to_string(), "null".to_string()),
        ]
    }
}

/// Port of Go `getFirstError`.
fn get_first_error(
    errs: Vec<Option<NetError>>,
    allow_partial_response: bool,
) -> Result<(), String> {
    if errs.is_empty() {
        esl_common::panicf!("BUG: len(errs) must be bigger than 0");
    }

    if !allow_partial_response {
        if let Some(err) = errs.iter().flatten().next() {
            return Err(err.message().to_string());
        }
        return Ok(());
    }

    // allow_partial_response == true. Return the error only if all the backends
    // are unavailable or if some of the backends are improperly configured.
    for err in &errs {
        let Some(err) = err else {
            // At least a single eslstorage returned full response.
            return Ok(());
        };
        if !is_unavailable_backend_error(err) {
            // Return the first error, which isn't related to the backend
            // unavailability, to the client, since this error may point to
            // configuration issues, which must be fixed ASAP.
            return Err(format!(
                "the eslstorage node is available, but it returns an error, which may point to configuration issues: {}",
                err.message()
            ));
        }
    }

    Err(format!(
        "all the eslstorage nodes are unavailable for querying; a sample error: {}",
        errs[0].as_ref().map(NetError::message).unwrap_or_default()
    ))
}

/// Port of Go `unmarshalValuesWithHits`.
fn unmarshal_values_with_hits(qs: &QueryStats, src: &[u8]) -> Result<Vec<ValueWithHits>, String> {
    // Unmarshal ValuesWithHits at first
    if src.len() < 8 {
        return Err("missing length of ValueWithHits entries".to_string());
    }
    let vhs_len = vlencoding::unmarshal_uint64(&src[..8]);
    let mut src = &src[8..];

    let mut vhs = vec![ValueWithHits::default(); vhs_len as usize];
    for i in 0..vhs.len() {
        let n = vhs[i].unmarshal_inplace(src).map_err(|err| {
            format!(
                "cannot unmarshal ValueWithHits #{i} out of {}: {err}",
                vhs.len()
            )
        })?;
        src = &src[n..];
        // PORT NOTE: Go clones vh.Value since it points into src; the ported
        // unmarshal_inplace already produces an owned String.
    }

    // Unmarshal query stats
    let qs_local = QueryStats::default();
    let tail = unmarshal_query_stats(&qs_local, src)
        .map_err(|err| format!("cannot unmarshal query stats: {err}"))?;
    qs.update_atomic(&qs_local);
    if !tail.is_empty() {
        return Err(format!(
            "unexpected tail left after query stats; len(tail)={}",
            tail.len()
        ));
    }

    Ok(vhs)
}

/// Port of Go `unmarshalQueryStats`.
fn unmarshal_query_stats<'a>(qs: &QueryStats, src: &'a [u8]) -> Result<&'a [u8], String> {
    let mut db = DataBlock::default();
    let n = db
        .unmarshal_inplace(src)
        .map_err(|err| format!("cannot unmarshal data block: {err}"))?;
    qs.update_from_data_block(&db)
        .map_err(|err| format!("cannot read query stats: {err}"))?;
    Ok(&src[n..])
}

// PORT NOTE: upstream has no netselect test file; the tests below cover the
// ported wire-format helpers and error classification.
#[cfg(test)]
mod tests {
    use super::*;
    use esl_logstorage::storage_search::BlockColumn;
    use std::sync::Arc;

    fn query_stats_data_block(rows_found: u64) -> DataBlock {
        let names = [
            "BytesReadColumnsHeaders",
            "BytesReadColumnsHeaderIndexes",
            "BytesReadBloomFilters",
            "BytesReadValues",
            "BytesReadTimestamps",
            "BytesReadBlockHeaders",
            "BlocksProcessed",
            "RowsProcessed",
            "RowsFound",
            "ValuesRead",
            "TimestampsRead",
            "BytesProcessedUncompressedValues",
        ];
        let mut db = DataBlock::default();
        let columns = names
            .iter()
            .map(|&name| BlockColumn {
                name: name.to_string(),
                values: vec![if name == "RowsFound" {
                    format!("{rows_found}").into_bytes()
                } else {
                    b"0".to_vec()
                }],
            })
            .collect();
        db.set_columns(columns);
        db
    }

    #[test]
    fn test_unmarshal_values_with_hits() {
        // Marshal two ValueWithHits entries + the trailing query-stats block,
        // mirroring the response layout produced by Go's internalselect
        // handlers.
        let mut data = Vec::new();
        esl_common::encoding::marshal_uint64(&mut data, 2);
        let vh1 = ValueWithHits {
            value: "foo".to_string(),
            hits: 10,
        };
        let vh2 = ValueWithHits {
            value: "bar".to_string(),
            hits: 5,
        };
        vh1.marshal(&mut data);
        vh2.marshal(&mut data);
        query_stats_data_block(42).marshal(&mut data);

        let qs = QueryStats::default();
        let vhs = unmarshal_values_with_hits(&qs, &data).unwrap();
        assert_eq!(vhs, vec![vh1, vh2]);
        assert_eq!(qs.rows_found.load(std::sync::atomic::Ordering::SeqCst), 42);

        // Truncated input must fail.
        assert!(unmarshal_values_with_hits(&qs, &data[..4]).is_err());
    }

    #[test]
    fn test_process_query_response_block_stream() {
        // Build a response containing one query-stats block and one data
        // block, framed like Go's /internal/select/query response stream.
        let mut payload = Vec::new();
        payload.push(1u8); // query stats block marker
        query_stats_data_block(7).marshal(&mut payload);
        payload.push(0u8); // data block marker
        let mut db = DataBlock::default();
        db.set_columns(vec![BlockColumn {
            name: "_msg".to_string(),
            values: vec![b"hello".to_vec(), b"world".to_vec()],
        }]);
        db.marshal(&mut payload);

        let mut response = Vec::new();
        esl_common::encoding::marshal_uint64(&mut response, payload.len() as u64);
        response.extend_from_slice(&payload);

        let s = new_storage(
            &["127.0.0.1:1".to_string()],
            vec![AuthConfig::default()],
            true,
        );
        let qs = QueryStats::default();
        let mut blocks = 0usize;
        let mut rows = 0usize;
        s.sns[0]
            .process_query_response(&s, &qs, &response, "/internal/select/query", &mut |db| {
                blocks += 1;
                rows += db.rows_count();
            })
            .unwrap();
        assert_eq!((blocks, rows), (1, 2));
        assert_eq!(qs.rows_found.load(std::sync::atomic::Ordering::SeqCst), 7);
    }

    #[test]
    fn test_get_first_error() {
        // No errors → Ok regardless of allow_partial_response.
        assert!(get_first_error(vec![None, None], false).is_ok());
        assert!(get_first_error(vec![None, None], true).is_ok());

        // allow_partial_response=false returns the first error.
        let errs = vec![
            None,
            Some(NetError::Unavailable("conn refused".to_string())),
        ];
        assert!(get_first_error(errs, false).is_err());

        // allow_partial_response=true tolerates unavailable backends when at
        // least one node succeeded.
        let errs = vec![
            None,
            Some(NetError::Unavailable("conn refused".to_string())),
        ];
        assert!(get_first_error(errs, true).is_ok());

        // ... but non-unavailability errors are returned.
        let errs = vec![
            Some(NetError::Other("bad config".to_string())),
            Some(NetError::Unavailable("conn refused".to_string())),
        ];
        let err = get_first_error(errs, true).unwrap_err();
        assert!(err.contains("configuration issues"), "{err}");

        // All backends unavailable → error.
        let errs = vec![Some(NetError::Unavailable("conn refused".to_string()))];
        let err = get_first_error(errs, true).unwrap_err();
        assert!(
            err.contains("all the eslstorage nodes are unavailable"),
            "{err}"
        );
    }

    #[test]
    fn test_run_query_unreachable_node() {
        // Querying unreachable nodes must return an error rather than hang.
        let s = new_storage(
            &["127.0.0.1:1".to_string()],
            vec![AuthConfig::default()],
            true,
        );
        let qs = QueryStats::default();
        let q = esl_logstorage::parser::ParseQueryAtTimestamp("*", 0).unwrap();
        let write_block: WriteDataBlockFn = Arc::new(|_, _| {});
        let err = s
            .run_query(&qs, &[TenantID::default()], &q, false, write_block)
            .unwrap_err();
        assert!(err.contains("cannot connect"), "{err}");
        s.must_stop();
    }
}
