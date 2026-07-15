//! Port of EsLogs `app/eslagent/kubernetescollector` — the kubelet-based
//! container-log collector: Kubernetes client config (in-cluster / kubeconfig),
//! pod discovery + watch, container log path discovery under
//! `/var/log/containers`, CRI / docker `json-file` log line parsing (incl.
//! klog), and the collector loop wiring parsed lines to tailers/remotewrite.
//!
//! Go sources: kubernetes.go, collector.go, client.go, client_config.go,
//! processor.go (+ processor_test.go, processor_timing_test.go).
//!
//! PORT NOTE (TLS): Go talks to the Kubernetes API server over HTTPS via
//! `promauth` + `net/http`. The port speaks https via `esl_common::tlsutil`
//! (rustls over the same blocking TCP streams): buffered requests go through
//! `esl_storage::http_client::do_request` and the streaming watch reader runs
//! over [`WatchConn`] (plain TCP or TLS). The TLS client config is built
//! eagerly at config-load time, failing at startup instead of on the first
//! request like Go's lazy `promauth` `getTLSConfigCached`.
//!
//! PORT NOTE (deps): Go uses `gopkg.in/yaml.v2` for kubeconfig parsing and
//! `valyala/fastjson` for JSON. The port carries a minimal JSON value parser
//! and parses kubeconfig with the `yaml-rust2` crate (a full YAML library) —
//! so flow style, anchors/aliases and quoted/block/folded scalars all work,
//! matching Go's yaml.v2. Both feed the shared [`Value`] tree.
//!
//! PORT NOTE (siblings): `app/eslagent/tail` and `app/eslagent/remotewrite` are
//! owned by sibling porting agents and are still stubs. This module defines
//! Go-shaped local traits ([`Tailer`], [`TailProcessor`], [`LogRowsStorage`])
//! and registration hooks ([`set_tailer_factory`], [`set_log_rows_storage`])
//! for the orchestrator to reconcile once the siblings land.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use yaml_rust2::{Yaml, YamlLoader};

use esl_common::flagutil::{ArrayString, Flag};
use esl_common::logger::{LogThrottler, with_throttler};
use esl_common::metrics::Counter;
use esl_common::timeutil::BackoffTimer;
use esl_common::tlsutil::{self, TLSConfig, TlsClientConfig, TlsClientStream};
use esl_common::{errorf, fasttime, fatalf, infof, panicf, warnf};

use esl_insert::common_params::DEFAULT_MSG_VALUE;

use esl_logstorage::json_parser::{JSONParser, get_json_parser, put_json_parser};
use esl_logstorage::log_rows::{LogRows, estimated_json_row_len, get_log_rows, put_log_rows};
use esl_logstorage::parser::{Filter as LogsQlFilter, ParseFilter};
use esl_logstorage::rows::{Field, Fields, marshal_fields_to_json, rename_field};
use esl_logstorage::tenant_id::{TenantID, parse_tenant_id};
use esl_logstorage::values_encoder::try_parse_timestamp_rfc3339_nano;

use esl_storage::http_client::do_request;

// ===========================================================================
// Sibling interfaces (app/eslagent/tail, app/eslagent/remotewrite,
// app/eslinsert/insertutil)
// ===========================================================================

/// Port of Go `insertutil.LogRowsStorage` — the interface implemented by
/// `remotewrite.Storage`.
///
/// PORT NOTE: the esl-insert port dropped this interface (its request handlers
/// take the storage explicitly), and the sibling remotewrite module is not
/// ported yet, so the trait lives here until the orchestrator reconciles.
pub trait LogRowsStorage: Send + Sync {
    /// MustAddRows must add lr to the underlying storage.
    fn must_add_rows(&self, lr: &LogRows);

    /// CanWriteData must return an error if logs cannot be added to the
    /// underlying storage.
    fn can_write_data(&self) -> Result<(), String>;
}

/// Port of Go `tail.Processor` — processes log lines from a single file.
///
/// PORT NOTE: the sibling tail module is a stub; this trait mirrors the Go
/// interface shape so the tailer port can accept `Box<dyn TailProcessor>`.
pub trait TailProcessor: Send {
    /// Processes a log line and returns true if it should be committed to the
    /// checkpoints DB.
    fn try_add_line(&mut self, line: &[u8]) -> bool;

    /// Flushes any internally accumulated state.
    fn flush(&mut self);

    /// Releases all resources associated with the processor.
    fn must_close(&mut self);
}

/// Port of the Go `tail.Tailer` surface used by this module.
///
/// PORT NOTE: the sibling tail module is a stub; the collector codes against
/// this Go-shaped trait and the orchestrator wires the real tailer via
/// [`set_tailer_factory`].
pub trait Tailer: Send + Sync {
    /// Go `(*Tailer).StartRead`.
    fn start_read(&self, file_path: &str, proc: Box<dyn TailProcessor>);
    /// Go `(*Tailer).IsTailing`.
    fn is_tailing(&self, file_path: &str) -> bool;
    /// Go `(*Tailer).CleanupCheckpoints`.
    fn cleanup_checkpoints(&self);
    /// Go `(*Tailer).Stop`.
    fn stop(&self);
}

/// Constructor for a [`Tailer`] from a checkpoints path
/// (Go `tail.Start(checkpointsPath)`).
pub type TailerFactory = Box<dyn Fn(&str) -> Box<dyn Tailer> + Send + Sync>;

static TAILER_FACTORY: OnceLock<TailerFactory> = OnceLock::new();
static LOG_ROWS_STORAGE: OnceLock<Arc<dyn LogRowsStorage>> = OnceLock::new();

/// Registers the tailer constructor used by [`init`]
/// (Go calls `tail.Start` directly; see the module PORT NOTE on siblings).
pub fn set_tailer_factory(f: TailerFactory) {
    let _ = TAILER_FACTORY.set(f);
}

/// Registers the log-rows storage used by the collector
/// (Go uses the package-level `var storage = &remotewrite.Storage{}`).
pub fn set_log_rows_storage(s: Arc<dyn LogRowsStorage>) {
    let _ = LOG_ROWS_STORAGE.set(s);
}

// ===========================================================================
// kubernetes.go
// ===========================================================================

static ENABLED: Flag<bool> = Flag::new(
    "kubernetesCollector",
    "Whether to enable collecting logs from Kubernetes",
    || false,
);
esl_common::register_flag!(ENABLED);
static CHECKPOINTS_PATH: Flag<String> = Flag::new(
    "kubernetesCollector.checkpointsPath",
    "Path to file with checkpoints for Kubernetes logs. \
     Checkpoints are used to persist the read offsets for Kubernetes container logs. \
     When eslagent is restarted, it resumes reading logs from the stored offsets to avoid log duplication; \
     if this flag isn't set, then checkpoints are saved into eslagent-kubernetes-checkpoints.json under -tmpDataPath directory",
    String::new,
);
esl_common::register_flag!(CHECKPOINTS_PATH);
static LOGS_PATH: Flag<String> = Flag::new(
    "kubernetesCollector.logsPath",
    "Path to the directory with Kubernetes container logs (usually /var/log/containers). \
     This should point to the kubelet-managed directory containing symlinks to pod logs. \
     eslagent must have read access to this directory and to the target log files, typically located under /var/log/pods and /var/lib on the host",
    || "/var/log/containers".to_string(),
);
esl_common::register_flag!(LOGS_PATH);
static EXCLUDE_FILTER: Flag<String> = Flag::new(
    "kubernetesCollector.excludeFilter",
    "Optional LogsQL filter for excluding container logs. \
     The filter is applied to container metadata fields (e.g., kubernetes.pod_namespace, kubernetes.container_name) before reading the log files. \
     This significantly reduces CPU and I/O usage by skipping logs from unwanted containers. \
     See https://docs.victoriametrics.com/victorialogs/vlagent/#filtering-kubernetes-logs",
    String::new,
);
esl_common::register_flag!(EXCLUDE_FILTER);

static COLLECTOR: Mutex<Option<KubernetesCollector>> = Mutex::new(None);

/// Starts the Kubernetes log collector if `-kubernetesCollector` is set
/// (Go `Init`).
pub fn init(tmp_data_path: &str) {
    if !*ENABLED.get() {
        return;
    }

    let (cfg, is_local) = load_kube_api_config().unwrap_or_else(|err| {
        fatalf!("cannot load Kubernetes config: {err}");
        unreachable!()
    });

    let client = new_kube_api_client(cfg).unwrap_or_else(|err| {
        fatalf!("cannot create Kubernetes client: {err}");
        unreachable!()
    });

    let current_node_name = get_current_node_name(&client, is_local).unwrap_or_else(|err| {
        fatalf!("cannot get current node name: {err}");
        unreachable!()
    });

    let mut path = CHECKPOINTS_PATH.get().clone();
    if path.is_empty() {
        path = std::path::Path::new(tmp_data_path)
            .join("eslagent-kubernetes-checkpoints.json")
            .to_string_lossy()
            .into_owned();
    }

    let mut exclude_f: Option<LogsQlFilter> = None;
    let exclude_filter = EXCLUDE_FILTER.get();
    if !exclude_filter.is_empty() {
        match ParseFilter(exclude_filter) {
            Ok(f) => exclude_f = Some(f),
            Err(err) => {
                fatalf!(
                    "cannot parse LogsQL -kubernetesCollector.excludeFilter={exclude_filter:?}: {err}"
                );
                unreachable!()
            }
        }
    }

    let kc = start_kubernetes_collector(
        client,
        &current_node_name,
        LOGS_PATH.get(),
        &path,
        exclude_f,
    )
    .unwrap_or_else(|err| {
        fatalf!("cannot start kubernetes collector: {err}");
        unreachable!()
    });
    *COLLECTOR.lock().unwrap() = Some(kc);

    infof!("started Kubernetes log collector for node {current_node_name:?}");
}

/// Stops the collector started by [`init`] (Go `Stop`).
pub fn stop() {
    let kc = COLLECTOR.lock().unwrap().take();
    if let Some(kc) = kc {
        kc.stop();
    }
}

// PORT NOTE: Go bounds getCurrentNodeName with a 30-second context; the
// std-TCP client applies a fixed 30s per-request timeout instead.
fn get_current_node_name(client: &KubeApiClient, is_local: bool) -> Result<String, String> {
    if is_local {
        return get_current_node_name_local(client);
    }
    get_current_node_name_in_cluster(client)
}

fn get_current_node_name_local(client: &KubeApiClient) -> Result<String, String> {
    let nodes = client
        .get_nodes()
        .map_err(|err| format!("cannot get nodes from the cluster: {err}"))?;
    let Some(first_node) = nodes.into_iter().next() else {
        return Err("cannot find any nodes in the cluster".to_string());
    };
    Ok(first_node)
}

fn get_current_node_name_in_cluster(client: &KubeApiClient) -> Result<String, String> {
    let ns =
        get_current_namespace().map_err(|err| format!("cannot get current namespace: {err}"))?;

    let pod_name = hostname().map_err(|err| format!("cannot get hostname: {err}"))?;

    let current_pod = client
        .get_pod(&ns, &pod_name)
        .map_err(|err| format!("cannot get pod {pod_name:?} at namespace {ns:?}: {err}"))?;

    Ok(current_pod.spec.node_name)
}

fn get_current_namespace() -> Result<String, String> {
    let ns = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .map_err(|err| format!("cannot read current namespace: {err}"))?;
    Ok(ns.trim().to_string())
}

/// PORT NOTE: Go uses `os.Hostname`; std Rust has no hostname API, so the
/// port checks the HOSTNAME/COMPUTERNAME env vars (set inside Kubernetes pods
/// and on Windows respectively) and falls back to /proc.
fn hostname() -> Result<String, String> {
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(h) = std::env::var(var)
            && !h.is_empty()
        {
            return Ok(h);
        }
    }
    match std::fs::read_to_string("/proc/sys/kernel/hostname") {
        Ok(h) => Ok(h.trim().to_string()),
        Err(err) => Err(format!("cannot determine hostname: {err}")),
    }
}

// ===========================================================================
// client_config.go
// ===========================================================================

/// Returns true when any TLS option is set (Go `promauth.Options` carries a
/// nil-able `*TLSConfig` pointer instead; the port folds "no TLS options"
/// into an all-default [`TLSConfig`]).
fn tls_config_is_set(tc: &TLSConfig) -> bool {
    !(tc.ca.is_empty()
        && tc.ca_file.is_empty()
        && tc.cert.is_empty()
        && tc.cert_file.is_empty()
        && tc.key.is_empty()
        && tc.key_file.is_empty()
        && tc.server_name.is_empty()
        && tc.min_version.is_empty()
        && !tc.insecure_skip_verify)
}

/// Go `promauth.Options` subset used by the kubernetes collector.
#[derive(Debug, Default)]
struct AuthOptions {
    bearer_token: String,
    bearer_token_file: String,
    tls_config: TLSConfig,
}

impl AuthOptions {
    /// Go `promauth.Options.NewConfig` (validation subset).
    ///
    /// PORT NOTE: the TLS client config is built (and validated) eagerly here,
    /// failing at startup instead of on the first request like Go's lazy
    /// `getTLSConfigCached` — the established port pattern (see
    /// esl-storage/src/http_client.rs `Options::new_config`).
    fn new_config(&self) -> Result<AuthConfig, String> {
        if !self.bearer_token.is_empty() && !self.bearer_token_file.is_empty() {
            return Err(
                "both bearer_token and bearer_token_file are set; only one can be set".to_string(),
            );
        }
        let tls = if tls_config_is_set(&self.tls_config) {
            let cfg = tlsutil::new_tls_client_config(&self.tls_config)
                .map_err(|err| format!("cannot initialize tls: {err}"))?;
            Some(cfg)
        } else {
            None
        };
        Ok(AuthConfig {
            bearer_token: self.bearer_token.clone(),
            bearer_token_file: self.bearer_token_file.clone(),
            tls,
        })
    }
}

/// Go `promauth.Config` subset: request auth headers + the built TLS config.
#[derive(Debug, Default)]
struct AuthConfig {
    bearer_token: String,
    bearer_token_file: String,
    tls: Option<TlsClientConfig>,
}

impl AuthConfig {
    /// Returns the `Authorization` header value, or an empty string when no
    /// auth is configured (Go `promauth.Config.SetHeaders` subset).
    fn get_auth_header(&self) -> Result<String, String> {
        if !self.bearer_token.is_empty() {
            return Ok(format!("Bearer {}", self.bearer_token));
        }
        if !self.bearer_token_file.is_empty() {
            let token = std::fs::read_to_string(&self.bearer_token_file).map_err(|err| {
                format!(
                    "cannot read bearer token from {:?}: {err}",
                    self.bearer_token_file
                )
            })?;
            return Ok(format!("Bearer {}", token.trim_end_matches(['\r', '\n'])));
        }
        Ok(String::new())
    }
}

/// Go `kubeAPIConfig`.
struct KubeApiConfig {
    server: String,
    ac: AuthConfig,
}

/// Go `loadKubeAPIConfig`. Returns the config and whether it came from a
/// local kubeconfig file (`is_local`).
fn load_kube_api_config() -> Result<(KubeApiConfig, bool), String> {
    let in_cluster_err = match load_in_cluster_config() {
        Ok(cfg) => return Ok((cfg, false)),
        Err(err) => err,
    };

    match load_local_config() {
        Ok(cfg) => {
            warnf!(
                "cannot load in-cluster Kubernetes config: {in_cluster_err}; will use local config with server {:?} instead. \
                 Local Kubernetes config is intended for testing purposes only and must not be used in production. \
                 See https://docs.victoriametrics.com/victorialogs/vlagent/#kubernetes-collector-configuration for proper in-cluster setup",
                cfg.server
            );
            Ok((cfg, true))
        }
        Err(local_err) => Err(format!(
            "cannot load discovery config from in-cluster config: {in_cluster_err}; and from local config: {local_err}"
        )),
    }
}

/// Loads Kubernetes API configuration from within a pod running in a
/// Kubernetes cluster (Go `loadInClusterConfig`).
///
/// It uses the service account token and CA certificate mounted by Kubernetes
/// at standard paths (/var/run/secrets/kubernetes.io/serviceaccount/) and
/// discovers the API server endpoint through KUBERNETES_SERVICE_HOST and
/// KUBERNETES_SERVICE_PORT environment variables.
fn load_in_cluster_config() -> Result<KubeApiConfig, String> {
    let host = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default();
    let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_default();
    if host.is_empty() || port.is_empty() {
        return Err(
            "KUBERNETES_SERVICE_HOST/KUBERNETES_SERVICE_PORT environment variables are not set"
                .to_string(),
        );
    }

    const BEARER_TOKEN_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    // Verify that eslagent is running in a Kubernetes cluster.
    if let Err(err) = std::fs::metadata(BEARER_TOKEN_FILE) {
        return Err(err.to_string());
    }

    let opts = AuthOptions {
        bearer_token_file: BEARER_TOKEN_FILE.to_string(),
        tls_config: TLSConfig {
            ca_file: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt".to_string(),
            ..TLSConfig::default()
        },
        ..AuthOptions::default()
    };
    let ac = opts
        .new_config()
        .map_err(|err| format!("cannot initialize in-cluster auth config: {err}"))?;

    let server = format!("https://{}", join_host_port(&host, &port));
    Ok(KubeApiConfig { server, ac })
}

/// Go `net.JoinHostPort`.
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        return format!("[{host}]:{port}");
    }
    format!("{host}:{port}")
}

/// Go `kubeConfig` — the ~/.kube/config file structure.
#[derive(Debug, Default)]
struct KubeConfig {
    clusters: Vec<KubeConfigCluster>,
    users: Vec<KubeConfigUser>,
    contexts: Vec<KubeConfigContext>,
    current_context: String,
}

impl KubeConfig {
    fn find_user(&self, name: &str) -> Option<&KubeConfigUser> {
        self.users.iter().find(|u| u.name == name)
    }

    fn find_context(&self, context: &str) -> Option<&KubeConfigContext> {
        self.contexts.iter().find(|c| c.name == context)
    }

    fn find_cluster(&self, cluster: &str) -> Option<&KubeConfigCluster> {
        self.clusters.iter().find(|cl| cl.name == cluster)
    }

    fn from_value(v: &Value) -> KubeConfig {
        KubeConfig {
            clusters: v
                .item("clusters")
                .arr()
                .iter()
                .map(KubeConfigCluster::from_value)
                .collect(),
            users: v
                .item("users")
                .arr()
                .iter()
                .map(KubeConfigUser::from_value)
                .collect(),
            contexts: v
                .item("contexts")
                .arr()
                .iter()
                .map(KubeConfigContext::from_value)
                .collect(),
            current_context: v.item("current-context").str().to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct KubeConfigCluster {
    name: String,
    server: String,
    certificate_authority: String,
    certificate_authority_data: String,
}

impl KubeConfigCluster {
    fn from_value(v: &Value) -> KubeConfigCluster {
        let cl = v.item("cluster");
        KubeConfigCluster {
            name: v.item("name").str().to_string(),
            server: cl.item("server").str().to_string(),
            certificate_authority: cl.item("certificate-authority").str().to_string(),
            certificate_authority_data: cl.item("certificate-authority-data").str().to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct KubeConfigUser {
    name: String,
    token: String,
    client_certificate: String,
    client_certificate_data: String,
    client_key: String,
    client_key_data: String,
}

impl KubeConfigUser {
    fn from_value(v: &Value) -> KubeConfigUser {
        let u = v.item("user");
        KubeConfigUser {
            name: v.item("name").str().to_string(),
            token: u.item("token").str().to_string(),
            client_certificate: u.item("client-certificate").str().to_string(),
            client_certificate_data: u.item("client-certificate-data").str().to_string(),
            client_key: u.item("client-key").str().to_string(),
            client_key_data: u.item("client-key-data").str().to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct KubeConfigContext {
    name: String,
    cluster: String,
    user: String,
}

impl KubeConfigContext {
    fn from_value(v: &Value) -> KubeConfigContext {
        let c = v.item("context");
        KubeConfigContext {
            name: v.item("name").str().to_string(),
            cluster: c.item("cluster").str().to_string(),
            user: c.item("user").str().to_string(),
        }
    }
}

/// Loads Kubernetes API configuration from a local kubeconfig file
/// (Go `loadLocalConfig`). It reads the kubeconfig file from the KUBECONFIG
/// environment variable or falls back to ~/.kube/config.
fn load_local_config() -> Result<KubeApiConfig, String> {
    let mut config_path = std::env::var("KUBECONFIG").unwrap_or_default();
    if config_path.is_empty() {
        config_path = std::path::Path::new(&std::env::var("HOME").unwrap_or_default())
            .join(".kube")
            .join("config")
            .to_string_lossy()
            .into_owned();
    }

    let raw_config = std::fs::read_to_string(&config_path).map_err(|err| err.to_string())?;
    local_config_from_yaml(&raw_config, &config_path)
}

/// Parses a kubeconfig document into a [`KubeApiConfig`] — the
/// file-independent tail of Go `loadLocalConfig`, split out so tests can
/// exercise kubeconfig parsing without touching `KUBECONFIG`/`HOME`.
fn local_config_from_yaml(raw_config: &str, config_path: &str) -> Result<KubeApiConfig, String> {
    let v = yaml_parse(raw_config)
        .map_err(|err| format!("cannot parse yaml {config_path:?}: {err}"))?;
    let cfg = KubeConfig::from_value(&v);

    let Some(cctx) = cfg.find_context(&cfg.current_context) else {
        return Err(format!(
            "cannot find current context {:?} in {config_path:?}",
            cfg.current_context
        ));
    };

    let Some(cl) = cfg.find_cluster(&cctx.cluster) else {
        return Err(format!(
            "cannot find cluster {:?} in {config_path:?}",
            cctx.cluster
        ));
    };

    let mut tls_cfg = TLSConfig::default();

    if !cl.certificate_authority.is_empty() {
        tls_cfg.ca_file = cl.certificate_authority.clone();
    } else if !cl.certificate_authority_data.is_empty() {
        let ca = base64_std_decode(&cl.certificate_authority_data).map_err(|err| {
            format!(
                "cannot decode base64 encoded CA certificate data from file {config_path:?}: {err}"
            )
        })?;
        tls_cfg.ca = String::from_utf8_lossy(&ca).into_owned();
    }

    let Some(u) = cfg.find_user(&cctx.user) else {
        return Err(format!(
            "cannot find current user {:?} in {config_path:?}",
            cctx.user
        ));
    };

    if !u.client_certificate.is_empty() {
        tls_cfg.cert_file = u.client_certificate.clone();
    } else if !u.client_certificate_data.is_empty() {
        let client_cert = base64_std_decode(&u.client_certificate_data).map_err(|err| {
            format!("cannot decode base64 encoded client certificate data from file {config_path:?}: {err}")
        })?;
        tls_cfg.cert = String::from_utf8_lossy(&client_cert).into_owned();
    }

    if !u.client_key.is_empty() {
        tls_cfg.key_file = u.client_key.clone();
    } else if !u.client_key_data.is_empty() {
        let client_cert_key = base64_std_decode(&u.client_key_data).map_err(|err| {
            format!("cannot decode base64 encoded client certificate key data from file {config_path:?}: {err}")
        })?;
        tls_cfg.key = String::from_utf8_lossy(&client_cert_key).into_owned();
    }

    let opts = AuthOptions {
        bearer_token: u.token.clone(),
        tls_config: tls_cfg,
        ..AuthOptions::default()
    };
    let ac = opts.new_config().map_err(|err| {
        format!("cannot initialize local auth config from file {config_path:?}: {err}")
    })?;

    Ok(KubeApiConfig {
        server: cl.server.clone(),
        ac,
    })
}

// ===========================================================================
// client.go
// ===========================================================================

/// Go `kubeAPIClient`.
///
/// PORT NOTE: Go holds an `http.Client` with a pooled transport; the port
/// issues one std-TCP connection per request via
/// `esl_storage::http_client::do_request` (the house pattern) plus a
/// hand-rolled streaming connection for watch requests. Both are upgraded to
/// TLS when the API server URL uses `https://`.
struct KubeApiClient {
    config: KubeApiConfig,
    scheme: String,
    addr: String,
    base_path: String,
    /// `Some` when the connection must use https; see [`new_kube_api_client`].
    tls: Option<TlsClientConfig>,
}

fn new_kube_api_client(config: KubeApiConfig) -> Result<KubeApiClient, String> {
    let Some((scheme, rest)) = config.server.split_once("://") else {
        return Err(format!(
            "cannot parse server URL {:?}: missing scheme",
            config.server
        ));
    };
    let (hostport, base_path) = match rest.find('/') {
        Some(n) => (&rest[..n], rest[n..].trim_end_matches('/')),
        None => (rest, ""),
    };
    if hostport.is_empty() {
        return Err(format!(
            "cannot parse server URL {:?}: missing host",
            config.server
        ));
    }
    let addr = addr_with_default_port(hostport, scheme);

    // PORT NOTE: Go's `http.Client` applies the promauth TLS config only to
    // https URLs; mirror that by keying TLS off the URL scheme. An https
    // server without explicit TLS material gets a default client config
    // (webpki roots), built eagerly here like the rest of the TLS setup.
    let tls = if scheme == "https" {
        match &config.ac.tls {
            Some(cfg) => Some(cfg.clone()),
            None => {
                let cfg = tlsutil::new_tls_client_config(&TLSConfig::default()).map_err(|err| {
                    format!("cannot initialize tls for {:?}: {err}", config.server)
                })?;
                Some(cfg)
            }
        }
    } else {
        None
    };

    Ok(KubeApiClient {
        scheme: scheme.to_string(),
        addr,
        base_path: base_path.to_string(),
        config,
        tls,
    })
}

fn addr_with_default_port(hostport: &str, scheme: &str) -> String {
    let has_port = if hostport.starts_with('[') {
        hostport
            .rfind(']')
            .is_some_and(|i| hostport[i..].contains(':'))
    } else {
        hostport.contains(':')
    };
    if has_port {
        return hostport.to_string();
    }
    let port = if scheme == "https" { 443 } else { 80 };
    format!("{hostport}:{port}")
}

/// Go `watchEvent`.
///
/// PORT NOTE: Go keeps `Object` as `json.RawMessage`; the port keeps the
/// parsed [`Value`] tree plus the raw event line for error messages.
struct WatchEvent {
    event_type: String,
    object: Value,
    raw: String,
}

/// Error classification for the watch loop; replaces Go's sentinel `errGone`,
/// `io.EOF` checks and `ctx.Err()`.
enum WatchError {
    /// The current resourceVersion is no longer valid (HTTP 410).
    Gone,
    /// The Kubernetes API server closed the connection
    /// (Go `io.EOF` / `io.ErrUnexpectedEOF`).
    Eof,
    /// The collector is shutting down (Go `ctx.Err() != nil`).
    Stopped,
    Other(String),
}

impl KubeApiClient {
    fn request_url(&self, path_and_query: &str) -> String {
        format!("{}://{}{}", self.scheme, self.addr, path_and_query)
    }

    /// Go `mustCreateRequest` counterpart: builds the path-and-query string.
    /// Query args are encoded sorted by key, like Go `url.Values.Encode`.
    fn build_path_and_query(&self, url_path: &str, args: &[(String, String)]) -> String {
        let mut pq = format!("{}{}", self.base_path, url_path);
        if !args.is_empty() {
            let mut sorted: Vec<&(String, String)> = args.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            let encoded: Vec<String> = sorted
                .iter()
                .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
                .collect();
            pq.push('?');
            pq.push_str(&encoded.join("&"));
        }
        pq
    }

    /// Starts watching Pod changes on the specified node (Go `watchNodePods`).
    /// It returns a stream of [`WatchEvent`] values representing updates to
    /// those Pods.
    ///
    /// The `resource_version` argument skips already processed events; see
    /// <https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes>
    fn watch_node_pods(
        &self,
        node_name: &str,
        resource_version: &str,
    ) -> Result<PodWatchStream, String> {
        let mut args = vec![
            ("watch".to_string(), "true".to_string()),
            // Watch pods only on the given node.
            // See https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/
            (
                "fieldSelector".to_string(),
                format!("spec.nodeName={node_name}"),
            ),
        ];
        // Set resourceVersion if it is non-empty to skip already processed events.
        if !resource_version.is_empty() {
            args.push(("resourceVersion".to_string(), resource_version.to_string()));
        }

        let pq = self.build_path_and_query("/api/v1/pods", &args);
        let url = self.request_url(&pq);

        let (status_code, mut response) =
            open_streaming_get(&self.addr, self.tls.as_ref(), &pq, &self.config.ac)
                .map_err(|err| format!("cannot do {url:?} GET request: {err}"))?;

        if status_code == 410 && !resource_version.is_empty() {
            // Requested watch operation failed because the historical resourceVersion is too old.
            // Fallback to watching from the beginning.
            // See https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-watch
            return self.watch_node_pods(node_name, "");
        }

        if status_code != 200 {
            let payload = response.read_error_payload();
            return Err(format!(
                "unexpected status code {status_code} from {url:?}; response: {payload:?}"
            ));
        }

        Ok(response)
    }

    /// Go `readResourceGeneric`: issues a GET request and decodes the JSON
    /// response into a [`Value`] tree.
    fn read_resource_generic(
        &self,
        url_path: &str,
        args: &[(String, String)],
    ) -> Result<Value, String> {
        let pq = self.build_path_and_query(url_path, args);
        let url = self.request_url(&pq);

        let mut headers: Vec<(String, String)> = Vec::new();
        let auth = self.config.ac.get_auth_header()?;
        if !auth.is_empty() {
            headers.push(("Authorization".to_string(), auth));
        }

        let resp = do_request(&self.addr, self.tls.as_ref(), "GET", &pq, &headers, None)
            .map_err(|err| format!("cannot do {url:?} GET request: {err}"))?;

        if resp.status_code != 200 {
            return Err(format!(
                "unexpected status code {} from {url:?}; response: {:?}",
                resp.status_code,
                String::from_utf8_lossy(&resp.body)
            ));
        }

        json_parse(&resp.body).map_err(|err| format!("cannot decode response body: {err}"))
    }

    /// Returns a list of pods on the given node (Go `getNodePods`).
    fn get_node_pods(&self, node_name: &str) -> Result<PodList, String> {
        let args = vec![(
            "fieldSelector".to_string(),
            format!("spec.nodeName={node_name}"),
        )];
        let v = self.read_resource_generic("/api/v1/pods", &args)?;
        Ok(PodList::from_value(&v))
    }

    /// Returns the pod with the given namespace and name (Go `getPod`).
    fn get_pod(&self, namespace: &str, pod_name: &str) -> Result<Pod, String> {
        let v = self.read_resource_generic(
            &format!("/api/v1/namespaces/{namespace}/pods/{pod_name}"),
            &[],
        )?;
        Ok(Pod::from_value(&v))
    }

    /// Returns the list of node names in the Kubernetes cluster (Go `getNodes`).
    fn get_nodes(&self) -> Result<Vec<String>, String> {
        let v = self.read_resource_generic("/api/v1/nodes", &[])?;
        let nl = NodeList::from_value(&v);
        Ok(nl.items.into_iter().map(|n| n.metadata.name).collect())
    }

    /// Returns a node by its name (Go `getNodeByName`).
    fn get_node_by_name(&self, node_name: &str) -> Result<Node, String> {
        let v = self.read_resource_generic(&format!("/api/v1/nodes/{node_name}"), &[])?;
        Ok(Node::from_value(&v))
    }

    /// Retrieves the list of namespaces in the Kubernetes cluster
    /// (Go `getNamespaces`).
    fn get_namespaces(&self) -> Result<NamespaceList, String> {
        let v = self.read_resource_generic("/api/v1/namespaces", &[])?;
        Ok(NamespaceList::from_value(&v))
    }
}

/// Go `url.QueryEscape`.
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

/// Opens a streaming HTTP/1.1 GET request (over plain TCP or TLS) and returns
/// the parsed status code plus a [`PodWatchStream`] over the (possibly
/// chunked) response body.
///
/// PORT NOTE: `esl_storage::http_client::do_request` buffers whole responses;
/// watch responses are unbounded streams, so this module carries its own
/// streaming reader. The socket read timeout is 1s so the watch loop can
/// observe collector shutdown (Go cancels the request via context instead).
fn open_streaming_get(
    addr: &str,
    tls: Option<&TlsClientConfig>,
    path_and_query: &str,
    ac: &AuthConfig,
) -> Result<(u16, PodWatchStream), String> {
    use std::net::ToSocketAddrs;

    let sock_addr = addr
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {addr:?}: {err}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {addr:?}: no addresses"))?;
    let tcp = TcpStream::connect_timeout(&sock_addr, Duration::from_secs(30))
        .map_err(|err| format!("cannot connect to {addr:?}: {err}"))?;
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(30)));
    // Keep the read timeout long for the TLS handshake; it is lowered to 1s
    // below so the watch read loop can observe collector shutdown.
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(30)));
    let _ = tcp.set_nodelay(true);

    let mut stream = match tls {
        None => WatchConn::Plain(tcp),
        Some(cfg) => {
            let host = host_without_port(addr);
            WatchConn::Tls(Box::new(tlsutil::client_connect(cfg, host, tcp)?))
        }
    };
    stream.set_read_timeout(Duration::from_secs(1));

    // When the user overrode the TLS server name, use it as the `Host` header
    // too (Go: `req.Host = ac.tlsServerName` in promauth.Config.SetHeaders).
    let host_header = match tls {
        Some(cfg) if !cfg.server_name.is_empty() => cfg.server_name.as_str(),
        _ => addr,
    };
    let mut req =
        format!("GET {path_and_query} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n");
    let auth = ac.get_auth_header()?;
    if !auth.is_empty() {
        req.push_str(&format!("Authorization: {auth}\r\n"));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;

    // Read the response head.
    let mut head = Vec::with_capacity(4096);
    let deadline = Instant::now() + Duration::from_secs(30);
    let header_end = loop {
        if let Some(pos) = find_subslice(&head, b"\r\n\r\n") {
            break pos;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "timeout while reading response headers from {addr:?}"
            ));
        }
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) => {
                return Err(format!(
                    "connection to {addr:?} closed while reading response headers"
                ));
            }
            Ok(n) => head.extend_from_slice(&buf[..n]),
            Err(err) if is_timeout(&err) => continue,
            Err(err) => return Err(format!("cannot read response headers from {addr:?}: {err}")),
        }
    };

    let head_str = std::str::from_utf8(&head[..header_end])
        .map_err(|err| format!("non-utf8 response headers from {addr:?}: {err}"))?;
    let mut lines = head_str.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            format!("cannot parse response status line {status_line:?} from {addr:?}")
        })?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name == "content-length" {
            content_length = value.parse().ok();
        } else if name == "transfer-encoding" && value.eq_ignore_ascii_case("chunked") {
            chunked = true;
        }
    }

    let mut pws = PodWatchStream {
        stream,
        chunked,
        content_remaining: content_length,
        phase: ChunkPhase::Size,
        raw: Vec::new(),
        decoded: Vec::new(),
    };
    let leftover = head[header_end + 4..].to_vec();
    pws.feed(&leftover)?;
    Ok((status_code, pws))
}

fn is_timeout(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Strips the `:port` suffix from a `host:port` address (IPv6 literals keep
/// Go's `[host]:port` bracket form); the result feeds TLS SNI/verification.
///
/// PORT NOTE: duplicated from esl-storage/src/http_client.rs (private there).
fn host_without_port(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return &rest[..end];
    }
    match addr.rsplit_once(':') {
        // An unbracketed IPv6 literal contains more colons; treat it as a
        // bare host without port.
        Some((host, _)) if !host.contains(':') => host,
        _ => addr,
    }
}

/// The transport under a watch stream: plain TCP or rustls-over-TCP
/// (Go gets this polymorphism for free from `net/http`'s `resp.Body`).
enum WatchConn {
    Plain(TcpStream),
    /// Boxed: `StreamOwned` embeds large rustls buffers (clippy
    /// `large_enum_variant`).
    Tls(Box<TlsClientStream>),
}

impl WatchConn {
    fn set_read_timeout(&self, timeout: Duration) {
        let sock = match self {
            WatchConn::Plain(s) => s,
            WatchConn::Tls(s) => &s.sock,
        };
        let _ = sock.set_read_timeout(Some(timeout));
    }
}

impl Read for WatchConn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            WatchConn::Plain(s) => s.read(buf),
            // PORT NOTE: a peer omitting TLS close_notify surfaces as
            // `UnexpectedEof`; map it to a clean EOF. The response framing
            // (Content-Length/chunked) is validated by [`PodWatchStream`], so
            // truncation is still detected — same trust model as Go's
            // net/http (see `TolerantEofReader` in esl-storage http_client).
            WatchConn::Tls(s) => match s.read(buf) {
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(0),
                other => other,
            },
        }
    }
}

impl Write for WatchConn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            WatchConn::Plain(s) => s.write(buf),
            WatchConn::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            WatchConn::Plain(s) => s.flush(),
            WatchConn::Tls(s) => s.flush(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChunkPhase {
    Size,
    Data(usize),
    DataEnd,
    Done,
}

/// Go `podWatchStream` — a streaming reader over a watch response body.
struct PodWatchStream {
    stream: WatchConn,
    chunked: bool,
    /// Remaining body bytes for `Content-Length` framing (`None` = read to EOF).
    content_remaining: Option<usize>,
    phase: ChunkPhase,
    /// Undecoded bytes (chunked framing) read from the socket.
    raw: Vec<u8>,
    /// Decoded body bytes not yet consumed as event lines.
    decoded: Vec<u8>,
}

impl PodWatchStream {
    /// Feeds raw socket bytes through the transfer framing into `decoded`.
    fn feed(&mut self, buf: &[u8]) -> Result<(), String> {
        if !self.chunked {
            let take = match &mut self.content_remaining {
                Some(rem) => {
                    let take = buf.len().min(*rem);
                    *rem -= take;
                    take
                }
                None => buf.len(),
            };
            self.decoded.extend_from_slice(&buf[..take]);
            return Ok(());
        }

        self.raw.extend_from_slice(buf);
        loop {
            match self.phase {
                ChunkPhase::Size => {
                    let Some(pos) = find_subslice(&self.raw, b"\r\n") else {
                        return Ok(());
                    };
                    let line = std::str::from_utf8(&self.raw[..pos])
                        .map_err(|_| "non-utf8 chunk size".to_string())?;
                    let size_str = line.split(';').next().unwrap_or_default().trim();
                    let size = usize::from_str_radix(size_str, 16)
                        .map_err(|_| format!("cannot parse chunk size {size_str:?}"))?;
                    self.raw.drain(..pos + 2);
                    if size == 0 {
                        self.phase = ChunkPhase::Done;
                        return Ok(());
                    }
                    self.phase = ChunkPhase::Data(size);
                }
                ChunkPhase::Data(n) => {
                    if self.raw.is_empty() {
                        return Ok(());
                    }
                    let take = n.min(self.raw.len());
                    self.decoded.extend_from_slice(&self.raw[..take]);
                    self.raw.drain(..take);
                    if take == n {
                        self.phase = ChunkPhase::DataEnd;
                    } else {
                        self.phase = ChunkPhase::Data(n - take);
                        return Ok(());
                    }
                }
                ChunkPhase::DataEnd => {
                    if self.raw.len() < 2 {
                        return Ok(());
                    }
                    self.raw.drain(..2); // trailing "\r\n" after the chunk data
                    self.phase = ChunkPhase::Size;
                }
                ChunkPhase::Done => return Ok(()),
            }
        }
    }

    fn body_finished(&self) -> bool {
        if self.chunked {
            return self.phase == ChunkPhase::Done;
        }
        self.content_remaining == Some(0)
    }

    /// Reads and decodes the events stream, invoking `h` for every event
    /// (Go `readEvents`).
    ///
    /// PORT NOTE: Go uses `json.Decoder` over the stream; the Kubernetes watch
    /// API emits newline-delimited JSON, which the port relies on.
    fn read_events(
        &mut self,
        stop: &AtomicBool,
        mut h: impl FnMut(WatchEvent) -> Result<(), WatchError>,
    ) -> WatchError {
        loop {
            // Drain complete event lines from the decoded body.
            while let Some(pos) = self.decoded.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.decoded.drain(..=pos).collect();
                let line = trim_ascii_line(&line);
                if line.is_empty() {
                    continue;
                }
                let event = match parse_watch_event(line) {
                    Ok(e) => e,
                    Err(err) => {
                        return WatchError::Other(format!(
                            "cannot parse WatchEvent json response: {err}"
                        ));
                    }
                };
                if let Err(err) = h(event) {
                    return err;
                }
            }

            if self.body_finished() {
                return WatchError::Eof;
            }
            if stop.load(Ordering::Acquire) {
                return WatchError::Stopped;
            }

            let mut buf = [0u8; 16 * 1024];
            match self.stream.read(&mut buf) {
                Ok(0) => return WatchError::Eof,
                Ok(n) => {
                    if let Err(err) = self.feed(&buf[..n]) {
                        return WatchError::Other(format!("cannot decode watch response: {err}"));
                    }
                }
                Err(err) if is_timeout(&err) => {
                    if stop.load(Ordering::Acquire) {
                        return WatchError::Stopped;
                    }
                }
                Err(err) => return WatchError::Other(format!("cannot read watch response: {err}")),
            }
        }
    }

    /// Reads the (bounded) remainder of a non-200 response body for error
    /// reporting.
    fn read_error_payload(&mut self) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut buf = [0u8; 4096];
        while self.decoded.len() < 64 * 1024 && !self.body_finished() && Instant::now() < deadline {
            match self.stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if self.feed(&buf[..n]).is_err() {
                        break;
                    }
                }
                Err(err) if is_timeout(&err) => continue,
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(&self.decoded).into_owned()
    }
}

fn trim_ascii_line(line: &[u8]) -> &[u8] {
    let mut line = line;
    while let Some((&last, rest)) = line.split_last() {
        if last == b'\n' || last == b'\r' || last == b' ' || last == b'\t' {
            line = rest;
        } else {
            break;
        }
    }
    line
}

fn parse_watch_event(line: &[u8]) -> Result<WatchEvent, String> {
    let v = json_parse(line)?;
    Ok(WatchEvent {
        event_type: v.item("type").str().to_string(),
        object: v.item("object").clone(),
        raw: String::from_utf8_lossy(line).into_owned(),
    })
}

// ---------------------------------------------------------------------------
// Kubernetes API object subset (client.go type declarations)
// ---------------------------------------------------------------------------

/// Go `podList` — a Kubernetes PodList object.
#[derive(Debug, Default, Clone)]
struct PodList {
    items: Vec<Pod>,
    metadata: ListMeta,
}

impl PodList {
    fn from_value(v: &Value) -> PodList {
        PodList {
            items: v.item("items").arr().iter().map(Pod::from_value).collect(),
            metadata: ListMeta::from_value(v.item("metadata")),
        }
    }
}

/// Go `pod` — a Kubernetes Pod object.
#[derive(Debug, Default, Clone)]
struct Pod {
    metadata: ObjectMeta,
    spec: PodSpec,
    status: PodStatus,
}

impl Pod {
    fn from_value(v: &Value) -> Pod {
        Pod {
            metadata: ObjectMeta::from_value(v.item("metadata")),
            spec: PodSpec::from_value(v.item("spec")),
            status: PodStatus::from_value(v.item("status")),
        }
    }
}

/// Go `objectMeta` — a Kubernetes ObjectMeta object.
///
/// PORT NOTE: Go uses `map[string]string` (random iteration order); the port
/// uses `BTreeMap` so the derived label/annotation fields have a
/// deterministic order.
#[derive(Debug, Default, Clone)]
struct ObjectMeta {
    name: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    namespace: String,
    resource_version: String,
}

impl ObjectMeta {
    fn from_value(v: &Value) -> ObjectMeta {
        ObjectMeta {
            name: v.item("name").str().to_string(),
            labels: v.item("labels").string_map(),
            annotations: v.item("annotations").string_map(),
            namespace: v.item("namespace").str().to_string(),
            resource_version: v.item("resourceVersion").str().to_string(),
        }
    }
}

/// Go `podSpec` — a Kubernetes PodSpec object.
#[derive(Debug, Default, Clone)]
struct PodSpec {
    node_name: String,
    containers: Vec<PodContainer>,
    init_containers: Vec<PodContainer>,
}

impl PodSpec {
    fn from_value(v: &Value) -> PodSpec {
        PodSpec {
            node_name: v.item("nodeName").str().to_string(),
            containers: v
                .item("containers")
                .arr()
                .iter()
                .map(PodContainer::from_value)
                .collect(),
            init_containers: v
                .item("initContainers")
                .arr()
                .iter()
                .map(PodContainer::from_value)
                .collect(),
        }
    }
}

/// Go `podContainer` — a Kubernetes Container object.
#[derive(Debug, Default, Clone)]
struct PodContainer {
    name: String,
}

impl PodContainer {
    fn from_value(v: &Value) -> PodContainer {
        PodContainer {
            name: v.item("name").str().to_string(),
        }
    }
}

/// Go `podStatus` — a Kubernetes PodStatus object.
#[derive(Debug, Default, Clone)]
struct PodStatus {
    pod_ip: String,
    container_statuses: Vec<ContainerStatus>,
    init_container_statuses: Vec<ContainerStatus>,
}

impl PodStatus {
    fn from_value(v: &Value) -> PodStatus {
        PodStatus {
            pod_ip: v.item("podIP").str().to_string(),
            container_statuses: v
                .item("containerStatuses")
                .arr()
                .iter()
                .map(ContainerStatus::from_value)
                .collect(),
            init_container_statuses: v
                .item("initContainerStatuses")
                .arr()
                .iter()
                .map(ContainerStatus::from_value)
                .collect(),
        }
    }

    /// Go `findContainerStatus`.
    fn find_container_status(&self, container_name: &str) -> Option<&ContainerStatus> {
        self.container_statuses
            .iter()
            .find(|cs| cs.name == container_name)
    }

    /// Go `findInitContainerStatus`.
    fn find_init_container_status(&self, container_name: &str) -> Option<&ContainerStatus> {
        self.init_container_statuses
            .iter()
            .find(|cs| cs.name == container_name)
    }
}

/// Go `containerStatus` — a Kubernetes ContainerStatus object.
#[derive(Debug, Default, Clone)]
struct ContainerStatus {
    name: String,
    container_id: String,
}

impl ContainerStatus {
    fn from_value(v: &Value) -> ContainerStatus {
        ContainerStatus {
            name: v.item("name").str().to_string(),
            container_id: v.item("containerID").str().to_string(),
        }
    }
}

/// Go `listMeta` — a Kubernetes ListMeta object.
#[derive(Debug, Default, Clone)]
struct ListMeta {
    resource_version: String,
}

impl ListMeta {
    fn from_value(v: &Value) -> ListMeta {
        ListMeta {
            resource_version: v.item("resourceVersion").str().to_string(),
        }
    }
}

/// Go `nodeList` — a Kubernetes NodeList object.
#[derive(Debug, Default, Clone)]
struct NodeList {
    items: Vec<Node>,
}

impl NodeList {
    fn from_value(v: &Value) -> NodeList {
        NodeList {
            items: v.item("items").arr().iter().map(Node::from_value).collect(),
        }
    }
}

/// Go `node` — a Kubernetes Node object.
#[derive(Debug, Default, Clone)]
struct Node {
    metadata: ObjectMeta,
}

impl Node {
    fn from_value(v: &Value) -> Node {
        Node {
            metadata: ObjectMeta::from_value(v.item("metadata")),
        }
    }
}

/// Go `namespaceList` — a Kubernetes NamespaceList object.
#[derive(Debug, Default, Clone)]
struct NamespaceList {
    items: Vec<Namespace>,
}

impl NamespaceList {
    fn from_value(v: &Value) -> NamespaceList {
        NamespaceList {
            items: v
                .item("items")
                .arr()
                .iter()
                .map(Namespace::from_value)
                .collect(),
        }
    }
}

/// Go `namespace` — a Kubernetes Namespace object.
#[derive(Debug, Default, Clone)]
struct Namespace {
    metadata: ObjectMeta,
}

impl Namespace {
    fn from_value(v: &Value) -> Namespace {
        Namespace {
            metadata: ObjectMeta::from_value(v.item("metadata")),
        }
    }
}

// ===========================================================================
// collector.go
// ===========================================================================

/// Shared state of the collector (fields of Go `kubernetesCollector` that the
/// watch thread needs).
struct CollectorInner {
    client: KubeApiClient,

    current_node: Node,

    /// PORT NOTE: Go accesses the namespaces map without a lock (only the
    /// startup goroutine and then only the watch goroutine touch it); the port
    /// wraps it in a Mutex since Rust cannot express that hand-off.
    namespaces: Mutex<HashMap<String, Namespace>>,

    /// excludeFilter specifies criteria for excluding containers from
    /// processing, matched against common metadata fields.
    /// See [`get_common_fields`] for available fields.
    exclude_filter: Option<LogsQlFilter>,

    /// logsPath is the path to the directory containing Kubernetes container
    /// logs. This is typically /var/log/containers in standard Kubernetes
    /// deployments, but may vary depending on the eslagent mount configuration.
    /// This directory contains symlinks with specific filenames to actual files.
    logs_path: String,

    tailer: Box<dyn Tailer>,

    /// PORT NOTE: Go uses the package-level `var storage = &remotewrite.Storage{}`;
    /// the port captures the registered storage here.
    storage: Arc<dyn LogRowsStorage>,

    /// Replaces Go's `context.Context` cancellation.
    stopped: AtomicBool,
}

/// Go `kubernetesCollector`.
struct KubernetesCollector {
    inner: Arc<CollectorInner>,
    stop_tx: Option<Sender<()>>,
    watch_thread: Option<JoinHandle<()>>,
}

/// Starts watching the Kubernetes cluster on the given node and starts
/// collecting container logs (Go `startKubernetesCollector`).
///
/// The collector monitors container logs in the specified `logs_path`
/// directory and uses `checkpoints_path` to track reading progress.
/// The caller must call `stop()` when the collector is no longer needed.
fn start_kubernetes_collector(
    client: KubeApiClient,
    current_node_name: &str,
    logs_path: &str,
    checkpoints_path: &str,
    exclude_filter: Option<LogsQlFilter>,
) -> Result<KubernetesCollector, String> {
    std::fs::metadata(logs_path).map_err(|err| format!("cannot access logs dir: {err}"))?;

    let current_node = client.get_node_by_name(current_node_name).map_err(|err| {
        format!("cannot get information about current node {current_node_name:?}: {err}")
    })?;

    let tailer_factory = TAILER_FACTORY.get().ok_or_else(|| {
        "no tailer registered: call kubernetescollector::set_tailer_factory() before init(); \
         PORT NOTE: Go constructs tail.Start(checkpointsPath) directly, but the tail module port is pending"
            .to_string()
    })?;
    let storage = Arc::clone(LOG_ROWS_STORAGE.get().ok_or_else(|| {
        "no log rows storage registered: call kubernetescollector::set_log_rows_storage() before init(); \
         PORT NOTE: Go uses the global remotewrite.Storage{}, but the remotewrite module port is pending"
            .to_string()
    })?);

    let tailer = tailer_factory(checkpoints_path);

    let inner = Arc::new(CollectorInner {
        client,
        current_node,
        namespaces: Mutex::new(HashMap::new()),
        exclude_filter,
        logs_path: logs_path.to_string(),
        tailer,
        storage,
        stopped: AtomicBool::new(false),
    });

    let pl = inner
        .client
        .get_node_pods(current_node_name)
        .map_err(|err| format!("cannot get Pods on node {current_node_name:?}: {err}"))?;

    // Start reading existing Pod logs.
    for pod in &pl.items {
        start_read_pod_logs(&inner, pod);
    }
    // Cleanup checkpoints for deleted Pods.
    inner.tailer.cleanup_checkpoints();

    // Begin watching for new Pods and start reading their logs.
    let (stop_tx, stop_rx) = channel::<()>();
    let inner2 = Arc::clone(&inner);
    let resource_version = pl.metadata.resource_version.clone();
    let watch_thread = std::thread::Builder::new()
        .name("kubernetescollector".to_string())
        .spawn(move || {
            watch_for_pods_updates(&inner2, resource_version, &stop_rx);
        })
        .map_err(|err| format!("cannot spawn the Pods watch thread: {err}"))?;

    Ok(KubernetesCollector {
        inner,
        stop_tx: Some(stop_tx),
        watch_thread: Some(watch_thread),
    })
}

impl KubernetesCollector {
    /// Go `(*kubernetesCollector).stop`.
    fn stop(mut self) {
        self.inner.stopped.store(true, Ordering::Release);
        // Dropping the sender wakes any BackoffTimer::wait immediately.
        drop(self.stop_tx.take());
        if let Some(h) = self.watch_thread.take() {
            let _ = h.join();
        }
        self.inner.tailer.stop();
    }
}

/// Watches Pods scheduled on the current node and calls
/// [`start_read_pod_logs`] for each new or modified Pod
/// (Go `watchForPodsUpdates`).
fn watch_for_pods_updates(
    inner: &Arc<CollectorInner>,
    mut resource_version: String,
    stop_rx: &Receiver<()>,
) {
    let current_node_name = inner.current_node.metadata.name.clone();

    let mut bt = BackoffTimer::new(200_000_000, 30_000_000_000); // 200ms .. 30s

    let mut error_fired = false;

    // Go `lastEOF := time.Time{}` — the first EOF is always tolerated.
    let mut last_eof_nanos = i64::MIN / 2;

    loop {
        if inner.stopped.load(Ordering::Acquire) {
            return;
        }

        let mut ws = match inner
            .client
            .watch_node_pods(&current_node_name, &resource_version)
        {
            Ok(ws) => ws,
            Err(err) => {
                if inner.stopped.load(Ordering::Acquire) {
                    return;
                }
                error_fired = true;
                errorf!(
                    "failed to start watching Pods on node {current_node_name:?}: {err}; will retry in {}",
                    format_backoff_delay(bt.current_delay())
                );
                if !bt.wait(stop_rx) {
                    return;
                }
                continue;
            }
        };

        let err = {
            let rv = &mut resource_version;
            let ef = &mut error_fired;
            let btr = &mut bt;
            ws.read_events(&inner.stopped, |event| {
                handle_watch_event(inner, &current_node_name, event, rv, btr, ef)
            })
        };
        drop(ws); // Go `_ = r.close()`

        match err {
            WatchError::Stopped => return,
            WatchError::Gone => {
                // The resourceVersion is no longer valid, see:
                // https://kubernetes.io/docs/reference/using-api/api-concepts/#410-gone-responses
                continue;
            }
            WatchError::Eof => {
                if inner.stopped.load(Ordering::Acquire) {
                    return;
                }
                let now = now_unix_nanos();
                if now - last_eof_nanos > 60_000_000_000 {
                    // Kubernetes API server closed the connection.
                    // This is expected to happen from time to time.
                    // Ignore EOF errors happening not more often than once per minute.
                    last_eof_nanos = now;
                    continue;
                }
                error_fired = true;
                errorf!(
                    "failed to read Pod events from the Kubernetes API: unexpected EOF; will retry in {}",
                    format_backoff_delay(bt.current_delay())
                );
                if !bt.wait(stop_rx) {
                    return;
                }
            }
            WatchError::Other(msg) => {
                if inner.stopped.load(Ordering::Acquire) {
                    return;
                }
                error_fired = true;
                errorf!(
                    "failed to read Pod events from the Kubernetes API: {msg}; will retry in {}",
                    format_backoff_delay(bt.current_delay())
                );
                if !bt.wait(stop_rx) {
                    return;
                }
            }
        }
    }
}

/// Go `handleEvent` closure inside `watchForPodsUpdates`.
fn handle_watch_event(
    inner: &CollectorInner,
    current_node_name: &str,
    event: WatchEvent,
    resource_version: &mut String,
    bt: &mut BackoffTimer,
    error_fired: &mut bool,
) -> Result<(), WatchError> {
    match event.event_type.as_str() {
        "ADDED" | "MODIFIED" => {
            bt.reset();

            if *error_fired {
                infof!("successfully re-established watching Pods on Node {current_node_name:?}");
            }
            *error_fired = false;

            // PORT NOTE: Go panics when the event object cannot be parsed as
            // JSON; the raw JSON is validated by read_events already, and the
            // Value → Pod extraction defaults missing fields like
            // encoding/json does.
            let pod = Pod::from_value(&event.object);

            start_read_pod_logs(inner, &pod);

            // Update resourceVersion to the latest seen.
            *resource_version = pod.metadata.resource_version.clone();
            Ok(())
        }
        "DELETED" => {
            // Ignore deleted pods.
            Ok(())
        }
        "ERROR" => {
            let code = event.object.item("code").num() as i64;
            if code == 410 && !resource_version.is_empty() {
                // The resourceVersion is no longer valid, see:
                // https://kubernetes.io/docs/reference/using-api/api-concepts/#410-gone-responses
                resource_version.clear();
                return Err(WatchError::Gone);
            }
            Err(WatchError::Other(format!(
                "unexpected error message: {:?}",
                event.raw
            )))
        }
        _ => Err(WatchError::Other(format!(
            "unexpected event type {:?}: {:?}",
            event.event_type, event.raw
        ))),
    }
}

/// Go `(*kubernetesCollector).startReadPodLogs`.
fn start_read_pod_logs(inner: &CollectorInner, pod: &Pod) {
    let ns = must_get_namespace(inner, &pod.metadata.namespace);

    let start_read = |pc: &PodContainer, cs: &ContainerStatus| {
        let file_path = get_log_file_path(&inner.logs_path, pod, pc, cs);
        if inner.tailer.is_tailing(&file_path) {
            return;
        }

        let common_fields = get_common_fields(&inner.current_node, &ns, pod, cs);
        if let Some(f) = &inner.exclude_filter
            && f.match_row(&common_fields)
        {
            // Filter matches - skip this container.
            return;
        }

        let proc = new_log_file_processor(Arc::clone(&inner.storage), &common_fields);
        inner.tailer.start_read(&file_path, Box::new(proc));
    };

    for pc in &pod.spec.containers {
        match pod.status.find_container_status(&pc.name) {
            Some(cs) if !cs.container_id.is_empty() => start_read(pc, cs),
            // Container in the pod is not running.
            _ => {}
        }
    }

    for pc in &pod.spec.init_containers {
        match pod.status.find_init_container_status(&pc.name) {
            Some(cs) if !cs.container_id.is_empty() => start_read(pc, cs),
            // Container in the pod is not running.
            _ => {}
        }
    }
}

/// Go `getCommonFields`.
fn get_common_fields(n: &Node, ns: &Namespace, p: &Pod, cs: &ContainerStatus) -> Vec<Field> {
    let mut fs = Fields::default();

    // Fields should match vector.dev kubernetes_source for easy migration.
    fs.add("kubernetes.container_name", &cs.name);
    fs.add("kubernetes.pod_name", &p.metadata.name);
    fs.add("kubernetes.pod_namespace", &p.metadata.namespace);
    fs.add("kubernetes.container_id", &cs.container_id);
    fs.add("kubernetes.pod_ip", &p.status.pod_ip);
    fs.add("kubernetes.pod_node_name", &p.spec.node_name);

    for (k, v) in &p.metadata.labels {
        fs.add(format!("kubernetes.pod_labels.{k}"), v);
    }
    for (k, v) in &p.metadata.annotations {
        fs.add(format!("kubernetes.pod_annotations.{k}"), v);
    }

    for (k, v) in &n.metadata.labels {
        fs.add(format!("kubernetes.node_labels.{k}"), v);
    }
    for (k, v) in &n.metadata.annotations {
        fs.add(format!("kubernetes.node_annotations.{k}"), v);
    }

    for (k, v) in &ns.metadata.labels {
        fs.add(format!("kubernetes.namespace_labels.{k}"), v);
    }
    for (k, v) in &ns.metadata.annotations {
        fs.add(format!("kubernetes.namespace_annotations.{k}"), v);
    }

    fs.fields
}

/// Go `(*kubernetesCollector).getLogFilePath`.
fn get_log_file_path(logs_path: &str, p: &Pod, pc: &PodContainer, cs: &ContainerStatus) -> String {
    let mut cid = cs.container_id.as_str();
    // Trim the container runtime prefix from the container ID.
    // A container ID format has the form "docker://<container_id>" or "containerd://<container_id>".
    if let Some(n) = cs.container_id.find("://") {
        cid = &cs.container_id[n + "://".len()..];
    }

    if p.metadata.name.is_empty()
        || p.metadata.namespace.is_empty()
        || pc.name.is_empty()
        || cid.is_empty()
    {
        panicf!(
            "FATAL: got invalid container info from Kubernetes API: pod name {:?}, namespace {:?}, container name {:?}, container ID {:?}",
            p.metadata.name,
            p.metadata.namespace,
            pc.name,
            cid
        );
    }

    let filename = format!(
        "{}_{}_{}-{}.log",
        p.metadata.name, p.metadata.namespace, pc.name, cid
    );
    // PORT NOTE: Go uses slash-based path.Join here (not filepath.Join).
    format!("{}/{}", logs_path.trim_end_matches('/'), filename)
}

/// Go `(*kubernetesCollector).mustGetNamespace`.
fn must_get_namespace(inner: &CollectorInner, ns_name: &str) -> Namespace {
    {
        // Fast path: the namespace is already cached.
        let namespaces = inner.namespaces.lock().unwrap();
        if let Some(ns) = namespaces.get(ns_name) {
            return ns.clone();
        }
    }

    // Slow path: the namespace is not cached.
    must_update_namespaces(inner);
    let namespaces = inner.namespaces.lock().unwrap();
    match namespaces.get(ns_name) {
        Some(ns) => ns.clone(),
        None => {
            panicf!(
                "FATAL: namespace {ns_name:?} is not found in the list of namespaces returned by Kubernetes API"
            );
            unreachable!()
        }
    }
}

/// Go `(*kubernetesCollector).mustUpdateNamespaces`.
fn must_update_namespaces(inner: &CollectorInner) {
    let nl = match inner.client.get_namespaces() {
        Ok(nl) => nl,
        Err(err) => {
            panicf!("FATAL: cannot get namespaces from Kubernetes API: {err}");
            unreachable!()
        }
    };

    let mut namespaces = inner.namespaces.lock().unwrap();
    namespaces.clear();
    for ns in nl.items {
        namespaces.insert(ns.metadata.name.clone(), ns);
    }
}

fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// Formats a backoff delay for log messages.
///
/// PORT NOTE: Go prints `time.Duration` (e.g. "800ms", "1.6s");
/// timeutil's Go-duration formatter is private, so this prints milliseconds.
fn format_backoff_delay(nanos: i64) -> String {
    format!("{}ms", nanos / 1_000_000)
}

// ===========================================================================
// processor.go
// ===========================================================================

static TENANT_ID: Flag<String> = Flag::new(
    "kubernetesCollector.tenantID",
    "Default tenant ID to use for logs collected from Kubernetes pods in format: <accountID>:<projectID>. \
     See https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
    || "0:0".to_string(),
);
esl_common::register_flag!(TENANT_ID);
static IGNORE_FIELDS: Flag<ArrayString> = Flag::new(
    "kubernetesCollector.ignoreFields",
    "Fields to ignore across logs ingested from Kubernetes",
    || ArrayString(Vec::new()),
);
esl_common::register_flag!(IGNORE_FIELDS);
static DECOLORIZE_FIELDS: Flag<ArrayString> = Flag::new(
    "kubernetesCollector.decolorizeFields",
    "Fields to remove ANSI color codes across logs ingested from Kubernetes",
    || ArrayString(Vec::new()),
);
esl_common::register_flag!(DECOLORIZE_FIELDS);
static MSG_FIELD: Flag<ArrayString> = Flag::new(
    "kubernetesCollector.msgField",
    "Fields that may contain the _msg field. \
     Default: message,msg,log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field",
    || ArrayString(Vec::new()),
);
esl_common::register_flag!(MSG_FIELD);
static TIME_FIELD: Flag<ArrayString> = Flag::new(
    "kubernetesCollector.timeField",
    "Fields that may contain the _time field. \
     Default: time,timestamp,ts. If none of the specified fields is found in the log line, then the write time will be used. \
     See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field",
    || ArrayString(Vec::new()),
);
esl_common::register_flag!(TIME_FIELD);
static EXTRA_FIELDS: Flag<String> = Flag::new(
    "kubernetesCollector.extraFields",
    "Extra fields in JSON format to add to each log line collected from Kubernetes Pods. \
     For example: -kubernetesCollector.extraFields='{\"cluster\":\"cluster-1\",\"env\":\"production\"}'",
    String::new,
);
esl_common::register_flag!(EXTRA_FIELDS);
static STREAM_FIELDS: Flag<ArrayString> = Flag::new(
    "kubernetesCollector.streamFields",
    "Comma-separated list of fields to use as log stream fields for logs ingested from Kubernetes Pods. \
     Default: kubernetes.container_name,kubernetes.pod_name,kubernetes.pod_namespace. \
     See: https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields",
    || ArrayString(Vec::new()),
);
esl_common::register_flag!(STREAM_FIELDS);

static INCLUDE_POD_LABELS: Flag<bool> = Flag::new(
    "kubernetesCollector.includePodLabels",
    "Include Pod labels as additional fields in the log entries. \
     Even this setting is disabled, Pod labels are available for filtering via -kubernetesCollector.excludeFilter flag",
    || true,
);
esl_common::register_flag!(INCLUDE_POD_LABELS);
static INCLUDE_POD_ANNOTATIONS: Flag<bool> = Flag::new(
    "kubernetesCollector.includePodAnnotations",
    "Include Pod annotations as additional fields in the log entries. \
     Even this setting is disabled, Pod annotations are available for filtering via -kubernetesCollector.excludeFilter flag",
    || false,
);
esl_common::register_flag!(INCLUDE_POD_ANNOTATIONS);
static INCLUDE_NODE_LABELS: Flag<bool> = Flag::new(
    "kubernetesCollector.includeNodeLabels",
    "Include Node labels as additional fields in the log entries. \
     Even this setting is disabled, Node labels are available for filtering via -kubernetesCollector.excludeFilter flag",
    || false,
);
esl_common::register_flag!(INCLUDE_NODE_LABELS);
static INCLUDE_NODE_ANNOTATIONS: Flag<bool> = Flag::new(
    "kubernetesCollector.includeNodeAnnotations",
    "Include Node annotations as additional fields in the log entries. \
     Even this setting is disabled, Node annotations are available for filtering via -kubernetesCollector.excludeFilter flag",
    || false,
);
esl_common::register_flag!(INCLUDE_NODE_ANNOTATIONS);
static INCLUDE_NAMESPACE_LABELS: Flag<bool> = Flag::new(
    "kubernetesCollector.includeNamespaceLabels",
    "Include Namespace labels as additional fields in the log entries. \
     Even this setting is disabled, Namespace labels are available for filtering via -kubernetesCollector.excludeFilter flag",
    || false,
);
esl_common::register_flag!(INCLUDE_NAMESPACE_LABELS);
static INCLUDE_NAMESPACE_ANNOTATIONS: Flag<bool> = Flag::new(
    "kubernetesCollector.includeNamespaceAnnotations",
    "Include Namespace annotations as additional fields in the log entries. \
     Even this setting is disabled, Namespace annotations are available for filtering via -kubernetesCollector.excludeFilter flag",
    || false,
);
esl_common::register_flag!(INCLUDE_NAMESPACE_ANNOTATIONS);

/// The maximum log line size that EsLogs can accept.
/// See <https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have>
const MAX_LOG_LINE_SIZE: usize = 2 * 1024 * 1024;

/// Go `logFileProcessor`.
struct LogFileProcessor {
    storage: Arc<dyn LogRowsStorage>,
    lr: Option<LogRows>,
    tenant_id: TenantID,

    /// commonFields are common fields for the given log file.
    common_fields: Vec<Field>,
    common_fields_json_len: usize,

    /// fieldsBuf is used for constructing log fields from commonFields and the
    /// actual log line fields before sending them to EsLogs.
    fields_buf: Vec<Field>,

    partial_cri_stdout: PartialCriLineState,
    partial_cri_stderr: PartialCriLineState,

    rows_ingested_local: u64,
    bytes_ingested_local: u64,
}

/// Returns a new [`LogFileProcessor`] for the given storage
/// (Go `newLogFileProcessor`).
fn new_log_file_processor(
    storage: Arc<dyn LogRowsStorage>,
    common_fields: &[Field],
) -> LogFileProcessor {
    let fs: Vec<Field> = common_fields
        .iter()
        // Metadata field names are engine-generated ASCII ("kubernetes.*");
        // the lossy view only feeds the prefix check, names stay raw.
        .filter(|f| should_include_metadata_field(&String::from_utf8_lossy(&f.name)))
        .cloned()
        .collect();
    let common_fields_json_len = estimated_json_row_len(&fs);

    let sfs = get_stream_fields();
    let efs = get_extra_fields();

    let sfs_refs: Vec<&str> = sfs.iter().map(String::as_str).collect();
    let ignore_refs: Vec<&str> = IGNORE_FIELDS.get().iter().map(String::as_str).collect();
    let decolorize_refs: Vec<&str> = DECOLORIZE_FIELDS.get().iter().map(String::as_str).collect();
    let lr = get_log_rows(
        &sfs_refs,
        &ignore_refs,
        &decolorize_refs,
        efs,
        DEFAULT_MSG_VALUE.get(),
    );

    LogFileProcessor {
        storage,
        lr: Some(lr),
        tenant_id: get_tenant_id(),
        common_fields: fs,
        common_fields_json_len,
        fields_buf: Vec::new(),
        partial_cri_stdout: PartialCriLineState::default(),
        partial_cri_stderr: PartialCriLineState::default(),
        rows_ingested_local: 0,
        bytes_ingested_local: 0,
    }
}

static INVALID_CRI_LINE_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("invalid_cri_log_line", Duration::from_secs(5)));

/// Go `partialCRILineState`.
///
/// PORT NOTE: Go pools `bytesutil.ByteBuffer` values; the port allocates a
/// plain `Vec<u8>` on demand.
#[derive(Default)]
struct PartialCriLineState {
    /// content accumulates the content of partial CRI log lines.
    /// Can be truncated if it exceeds [`MAX_LOG_LINE_SIZE`].
    content: Option<Vec<u8>>,
    /// size tracks the actual size of the content.
    size: usize,
}

impl PartialCriLineState {
    fn reset(&mut self) {
        self.content = None;
        self.size = 0;
    }

    fn is_empty_content(&self) -> bool {
        self.content.as_ref().is_none_or(|b| b.is_empty())
    }
}

/// Result of joining partial CRI lines.
///
/// PORT NOTE: Go's `joinPartialLines` returns a `[]byte` aliasing either the
/// input line or the accumulation buffer; the borrow checker requires the
/// distinction to be explicit, hence `FastPath` (caller uses the input
/// content) vs `Joined` (ownership of the joined buffer moves out).
enum JoinResult {
    /// The log content is not yet complete (Go `ok == false`).
    NotReady,
    /// The line must be skipped (too large), but committed to checkpoints.
    Skip,
    /// Fast path: the log line is complete and not split.
    FastPath(i64),
    /// The joined content of a split log line.
    Joined(i64, Vec<u8>),
}

impl TailProcessor for LogFileProcessor {
    /// Go `(*logFileProcessor).TryAddLine`.
    fn try_add_line(&mut self, log_line: &[u8]) -> bool {
        if log_line.is_empty() {
            return true;
        }

        if log_line[0] == b'{' {
            // Most likely, eslagent is running in Docker,
            // so fallback to the 'json-file' logging driver.
            match parse_cri_line_json(log_line) {
                Ok((timestamp, content)) => {
                    self.add_line_internal(timestamp, content.as_bytes());
                }
                Err(err) => {
                    ROWS_DROPPED_TOTAL_INVALID_CRI.inc();
                    // Display-only lossy conversions (R5): log message context.
                    let pod = String::from_utf8_lossy(must_get_field_val_by_name(
                        &self.common_fields,
                        b"kubernetes.pod_name",
                    ));
                    let namespace = String::from_utf8_lossy(must_get_field_val_by_name(
                        &self.common_fields,
                        b"kubernetes.pod_namespace",
                    ));
                    INVALID_CRI_LINE_LOGGER.errorf(format_args!(
                        "skipping invalid json-file log line {:?} from Pod {pod:?} in Namespace {namespace:?}: {err}; \
                         see https://docs.victoriametrics.com/victorialogs/vlagent/#troubleshooting for more details",
                        String::from_utf8_lossy(log_line)
                    ));
                }
            }
            return true;
        }

        let cri_line = match parse_cri_line(log_line) {
            Ok(cri_line) => cri_line,
            Err(err) => {
                ROWS_DROPPED_TOTAL_INVALID_CRI.inc();
                self.partial_cri_stdout.reset();
                self.partial_cri_stderr.reset();
                // Display-only lossy conversions (R5): log message context.
                let pod = String::from_utf8_lossy(must_get_field_val_by_name(
                    &self.common_fields,
                    b"kubernetes.pod_name",
                ));
                let namespace = String::from_utf8_lossy(must_get_field_val_by_name(
                    &self.common_fields,
                    b"kubernetes.pod_namespace",
                ));
                INVALID_CRI_LINE_LOGGER.errorf(format_args!(
                    "skipping invalid CRI log line {:?} from Pod {pod:?} in Namespace {namespace:?}: {err}; \
                     see https://docs.victoriametrics.com/victorialogs/vlagent/#troubleshooting for more details",
                    String::from_utf8_lossy(log_line)
                ));
                return true;
            }
        };

        match self.join_partial_lines(&cri_line) {
            JoinResult::NotReady => {
                // The log content is not yet complete.
                false
            }
            JoinResult::Skip => true,
            JoinResult::FastPath(timestamp) => {
                if !cri_line.content.is_empty() {
                    self.add_line_internal(timestamp, cri_line.content);
                }
                true
            }
            JoinResult::Joined(timestamp, content) => {
                if !content.is_empty() {
                    self.add_line_internal(timestamp, &content);
                }
                true
            }
        }
    }

    /// Go `(*logFileProcessor).Flush`.
    fn flush(&mut self) {
        self.flush_metrics();
    }

    /// Go `(*logFileProcessor).MustClose`.
    fn must_close(&mut self) {
        self.flush();
        self.partial_cri_stdout.reset();
        self.partial_cri_stderr.reset();
        if let Some(lr) = self.lr.take() {
            put_log_rows(lr);
        }
    }
}

static LOG_LINE_EXCEEDS_MAX_LINE_SIZE_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("log_line_exceeds_max_line_size", Duration::from_secs(5)));

impl LogFileProcessor {
    fn partial_state_mut(&mut self, stream: Stream) -> &mut PartialCriLineState {
        match stream {
            Stream::Stdout => &mut self.partial_cri_stdout,
            Stream::Stderr => &mut self.partial_cri_stderr,
        }
    }

    /// Go `(*logFileProcessor).joinPartialLines`.
    fn join_partial_lines(&mut self, cri_line: &CriLine<'_>) -> JoinResult {
        let state = self.partial_state_mut(cri_line.stream);
        if !cri_line.partial && state.is_empty_content() {
            // Fast path: the log line is complete and not split.
            // (Go resets the state via the deferred reset in TryAddLine.)
            state.reset();
            return JoinResult::FastPath(cri_line.timestamp);
        }
        // Slow path: line is split into multiple lines.
        self.join_partial_lines_slow(cri_line)
    }

    /// Go `(*logFileProcessor).joinPartialLinesSlow`.
    fn join_partial_lines_slow(&mut self, cri_line: &CriLine<'_>) -> JoinResult {
        if cri_line.partial {
            // The log line is split into multiple lines.
            // Accumulate the content until the full line is received.
            let state = self.partial_state_mut(cri_line.stream);
            if state.content.is_none() {
                state.content = Some(Vec::new());
            }
            state.size += cri_line.content.len();
            if state.size <= MAX_LOG_LINE_SIZE {
                state
                    .content
                    .as_mut()
                    .unwrap()
                    .extend_from_slice(cri_line.content);
            }
            return JoinResult::NotReady;
        }

        // The final part of the split log line received.
        let state = self.partial_state_mut(cri_line.stream);
        state.size += cri_line.content.len();
        if state.size > MAX_LOG_LINE_SIZE {
            // Discard the too large log line.
            let size = state.size;
            state.reset();
            TOO_LONG_LINES_SKIPPED.inc();
            // Display-only lossy conversions (R5): log message context.
            let pod = String::from_utf8_lossy(must_get_field_val_by_name(
                &self.common_fields,
                b"kubernetes.pod_name",
            ));
            let namespace = String::from_utf8_lossy(must_get_field_val_by_name(
                &self.common_fields,
                b"kubernetes.pod_namespace",
            ));
            LOG_LINE_EXCEEDS_MAX_LINE_SIZE_LOGGER.warnf(format_args!(
                "skipping log entry from Pod {pod:?} in namespace {namespace:?}: entry size of {:.2} MiB exceeds the maximum allowed size of {} MiB",
                size as f64 / 1024.0 / 1024.0,
                MAX_LOG_LINE_SIZE / 1024 / 1024
            ));
            return JoinResult::Skip;
        }

        let mut content = state.content.take().unwrap_or_default();
        content.extend_from_slice(cri_line.content);
        state.reset();
        JoinResult::Joined(cri_line.timestamp, content)
    }

    /// Go `(*logFileProcessor).addLineInternal`.
    fn add_line_internal(&mut self, cri_timestamp: i64, line: &[u8]) {
        let mut parser = get_json_parser();

        let (mut timestamp, ok) = parse_log_row_content(&mut parser, line);
        if !ok {
            // Go aliases the raw line bytes as the `_msg` value
            // (`bytesutil.ToUnsafeString`); with byte-valued `Field`s the
            // port stores an unstructured container log line containing
            // invalid UTF-8 verbatim, exactly like Go.
            parser.fields_mut().push(Field {
                name: b"_msg".to_vec(),
                value: line.to_vec(),
            });
        }

        if timestamp <= 0 {
            // Timestamp from the log line is missing or invalid, use the timestamp from Container Runtime Interface.
            timestamp = cri_timestamp;
        }

        if parser.fields().len() > 1000 {
            let mut line_buf = Vec::new();
            marshal_fields_to_json(&mut line_buf, parser.fields());
            warnf!(
                "dropping log line with {} fields; {}",
                parser.fields().len(),
                String::from_utf8_lossy(&line_buf)
            );
            ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS.inc();
            put_json_parser(parser);
            return;
        }

        self.add_row(timestamp, &parser);
        put_json_parser(parser);

        self.rows_ingested_local += 1;
        self.bytes_ingested_local += (self.common_fields_json_len + line.len()) as u64;
        if self.rows_ingested_local > 128 {
            self.flush_metrics();
        }
    }

    /// Go `(*logFileProcessor).addRow`.
    fn add_row(&mut self, timestamp: i64, parser: &JSONParser) {
        self.fields_buf.clear();
        self.fields_buf.extend_from_slice(&self.common_fields);
        self.fields_buf.extend_from_slice(parser.fields());

        let lr = self
            .lr
            .as_mut()
            .expect("BUG: LogFileProcessor used after MustClose");
        lr.must_add(self.tenant_id, timestamp, &mut self.fields_buf, -1);
        self.storage.must_add_rows(lr);
        lr.reset_keep_settings();
    }

    /// Go `(*logFileProcessor).flushMetrics`.
    fn flush_metrics(&mut self) {
        if self.rows_ingested_local == 0 {
            return;
        }
        ROWS_INGESTED_TOTAL.add(self.rows_ingested_local);
        BYTES_INGESTED_TOTAL.add(self.bytes_ingested_local);
        self.rows_ingested_local = 0;
        self.bytes_ingested_local = 0;
    }
}

/// Go `parseLogRowContent`. Returns the parsed timestamp (0 when missing) and
/// whether the line was recognized as structured content; on success the
/// parsed fields are left in `p`.
fn parse_log_row_content(p: &mut JSONParser, data: &[u8]) -> (i64, bool) {
    if data.is_empty() {
        return (0, false);
    }

    match data[0] {
        b'{' => {
            if p.parse_log_message(data, &[], "").is_err() {
                return (0, false);
            }

            // Try to parse timestamp from the time fields.
            let mut timestamp = 0i64;
            let n = field_index(p.fields(), get_time_fields());
            if n >= 0 {
                let f = &mut p.fields_mut()[n as usize];
                // R3: invalid UTF-8 fails the timestamp parse, matching Go's
                // parse semantics on arbitrary bytes.
                if let Some(v) = std::str::from_utf8(&f.value)
                    .ok()
                    .and_then(try_parse_timestamp_rfc3339_nano)
                {
                    timestamp = v;
                    // Set the time field to empty string to ignore it during data ingestion.
                    f.value.clear();
                }
            }

            // Rename the message field to _msg.
            let msg_fields: Vec<&str> = get_msg_fields().iter().map(String::as_str).collect();
            rename_field(p.fields_mut(), &msg_fields, "_msg");

            (timestamp, true)
        }
        b'I' | b'W' | b'E' | b'F' => {
            let ts = fasttime::unix_timestamp();
            let current = (ts as i64) * 1_000_000_000;
            // PORT NOTE: Go runs tryParseKlog on the unvalidated raw bytes
            // viewed as a string, so a klog line whose message contains
            // invalid UTF-8 still parses and Go stores the raw message
            // bytes. The port lossy-converts the line first so the klog
            // structure (level/_msg/key="value" fields) matches Go; the
            // remaining divergence is the message bytes themselves: Go keeps
            // e.g. a raw 0xFF where the port stores U+FFFD (Field is a
            // String; a String→bytes refactor would close it). klog headers
            // are ASCII, so the lossy pass never alters the parsed layout.
            let src = String::from_utf8_lossy(data);
            let dst = std::mem::take(p.fields_mut());
            match try_parse_klog(dst, &src, current) {
                Some((timestamp, fields)) => {
                    *p.fields_mut() = fields;
                    (timestamp, true)
                }
                None => (0, false),
            }
        }
        _ => (0, false),
    }
}

/// Parses the given string in Kubernetes Log format and returns the parsed
/// fields (Go `tryParseKlog`). See <https://github.com/kubernetes/klog/>
///
/// `current` is the current time as unix nanoseconds (Go passes `time.Time`).
fn try_parse_klog(mut dst: Vec<Field>, src: &str, current: i64) -> Option<(i64, Vec<Field>)> {
    if src.len() < "I0101 00:00:00.000000 1 p:1] m".len() {
        return None;
    }

    // Parse level.
    let level = get_klog_level(src.as_bytes()[0]);
    if !src.is_char_boundary(1) {
        return None;
    }
    let mut src = &src[1..];
    dst.push(Field {
        name: b"level".to_vec(),
        value: level.as_bytes().to_vec(),
    });

    // Parse timestamp (layout "0102 15:04:05.000000").
    const TS_LAYOUT_LEN: usize = "0102 15:04:05.000000".len();
    let kt = parse_klog_timestamp(&src.as_bytes()[..TS_LAYOUT_LEN])?;
    src = &src[TS_LAYOUT_LEN..];

    // Go: t = t.AddDate(current.Year(), 0, 0), i.e. the timestamp gets the
    // current year (normalizing Feb 29 on non-leap years like Go's AddDate).
    let (current_year, _, _) = civil_from_days(current.div_euclid(86_400_000_000_000));
    let mut timestamp = klog_unix_nanos(current_year, &kt);
    if timestamp - 24 * 3600 * 1_000_000_000 > current {
        // Adjust time to the previous year.
        timestamp = klog_unix_nanos(current_year - 1, &kt);
    }

    // Remove trailing spaces.
    if src.is_empty() || src.as_bytes()[0] != b' ' {
        return None;
    }
    src = src.trim_start_matches(' ');

    // Parse thread ID.
    let n = src.find(' ')?;
    if n == 0 {
        return None;
    }
    let thread_id = &src[..n];
    src = &src[n + 1..];
    dst.push(Field {
        name: b"thread_id".to_vec(),
        value: thread_id.as_bytes().to_vec(),
    });

    // Parse file:line.
    let n = src.find(']')?;
    if n == 0 {
        return None;
    }
    let source_line = &src[..n];
    src = &src[n + 1..];
    if src.is_empty() || src.as_bytes()[0] != b' ' {
        return None;
    }
    src = &src[1..];
    dst.push(Field {
        name: b"source_line".to_vec(),
        value: source_line.as_bytes().to_vec(),
    });

    // Parse log content.
    let dst = try_parse_klog_content(dst, src)?;

    Some((timestamp, dst))
}

/// Parsed klog timestamp components (Go parses into a year-0 `time.Time`).
struct KlogTimestamp {
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    micros: i64,
}

fn parse_klog_timestamp(b: &[u8]) -> Option<KlogTimestamp> {
    // Layout: "0102 15:04:05.000000" — strictly positional, like Go's
    // fixed-width time.Parse.
    if b.len() < 20 || b[4] != b' ' || b[7] != b':' || b[10] != b':' || b[13] != b'.' {
        return None;
    }
    let month = parse_fixed_digits(&b[0..2])?;
    let day = parse_fixed_digits(&b[2..4])?;
    let hour = parse_fixed_digits(&b[5..7])?;
    let minute = parse_fixed_digits(&b[8..10])?;
    let second = parse_fixed_digits(&b[11..13])?;
    let micros = parse_fixed_digits(&b[14..20])?;

    // Go time.Parse validates the day against the (zero-value, leap) year 0.
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(0, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    Some(KlogTimestamp {
        month,
        day,
        hour,
        minute,
        second,
        micros,
    })
}

fn parse_fixed_digits(b: &[u8]) -> Option<i64> {
    let mut n: i64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + i64::from(c - b'0');
    }
    Some(n)
}

fn klog_unix_nanos(year: i64, kt: &KlogTimestamp) -> i64 {
    let days = days_from_civil(year, kt.month, kt.day);
    let secs = days * 86_400 + kt.hour * 3_600 + kt.minute * 60 + kt.second;
    secs * 1_000_000_000 + kt.micros * 1_000
}

/// Go `tryParseKlogContent`.
fn try_parse_klog_content(mut dst: Vec<Field>, src: &str) -> Option<Vec<Field>> {
    if src.is_empty() {
        return None;
    }
    if !src.starts_with('"') {
        // Fast path: message is not quoted and does not contain additional key="value" fields.
        dst.push(Field {
            name: b"_msg".to_vec(),
            value: src.as_bytes().to_vec(),
        });
        return Some(dst);
    }

    // Slow path: message is quoted and contains additional key="value" fields.
    let prefix = quoted_prefix(src)?;
    let msg = unquote(prefix)?;
    let mut src = &src[prefix.len()..];
    dst.push(Field {
        name: b"_msg".to_vec(),
        value: msg.into_bytes(),
    });

    // Parse key="value" pairs.
    while !src.is_empty() {
        if src.as_bytes()[0] == b' ' {
            src = &src[1..];
        }

        let n = src.find('=')?;
        if n == 0 {
            return None;
        }
        let key = &src[..n];
        src = &src[n + 1..];

        let prefix = quoted_prefix(src)?;
        let value = unquote(prefix)?;
        src = &src[prefix.len()..];

        dst.push(Field {
            name: key.as_bytes().to_vec(),
            value: value.into_bytes(),
        });
    }

    Some(dst)
}

/// Go `strconv.QuotedPrefix` limited to double-quoted strings
/// (klog only emits those).
fn quoted_prefix(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    if b.is_empty() || b[0] != b'"' {
        return None;
    }
    let mut i = 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return Some(&s[..i + 1]),
            b'\n' => return None,
            _ => i += 1,
        }
    }
    None
}

/// Go `strconv.Unquote` subset for double-quoted strings.
///
/// PORT NOTE: escapes decode to raw bytes exactly like Go's
/// `strconv.UnquoteChar` (`\xHH`/`\NNN` octal emit the single byte, `\u`/`\U`
/// emit the rune's UTF-8), so any value whose decoded bytes are valid UTF-8 —
/// including UTF-8 spelled via `\x` escapes like `\xc3\xa9` → `é` — matches
/// Go byte-for-byte. Divergence (Field values are Rust `String`s, which must
/// hold valid UTF-8): when the decoded bytes are NOT valid UTF-8, Go stores
/// the raw bytes while the port U+FFFD-replaces each invalid sequence (e.g.
/// `"\xff"` → Go `0xFF`, port `"\u{FFFD}"`). A String→bytes Field refactor
/// would close this.
fn unquote(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() < 2 || b[0] != b'"' || b[b.len() - 1] != b'"' {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    let ib = inner.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(inner.len());
    let mut i = 0;
    while i < ib.len() {
        if ib[i] != b'\\' {
            if ib[i] == b'"' || ib[i] == b'\n' {
                return None;
            }
            // inner is a &str, so unescaped bytes are already valid UTF-8;
            // copy them through unchanged (Go copies runes the same way).
            out.push(ib[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= ib.len() {
            return None;
        }
        let c = ib[i];
        i += 1;
        match c {
            b'a' => out.push(0x07),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0C),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0B),
            b'\\' => out.push(b'\\'),
            // Go's unquoteChar rejects \' inside double-quoted strings (the
            // escape is only valid for the quote char).
            b'"' => out.push(b'"'),
            b'x' => {
                // Go appends the raw byte, even if >= 0x80.
                let v = parse_hex_digits(ib, &mut i, 2)?;
                out.push(v as u8);
            }
            b'u' => {
                let v = parse_hex_digits(ib, &mut i, 4)?;
                let c = char::from_u32(v)?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
            b'U' => {
                let v = parse_hex_digits(ib, &mut i, 8)?;
                let c = char::from_u32(v)?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
            b'0'..=b'7' => {
                // Octal escape: exactly 3 digits, raw byte, <= 255 (Go
                // errors on \400..\777); the first digit already consumed.
                let mut v = u32::from(c - b'0');
                for _ in 0..2 {
                    if i >= ib.len() || !(b'0'..=b'7').contains(&ib[i]) {
                        return None;
                    }
                    v = v * 8 + u32::from(ib[i] - b'0');
                    i += 1;
                }
                if v > 255 {
                    return None;
                }
                out.push(v as u8);
            }
            _ => return None,
        }
    }
    Some(
        String::from_utf8(out)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()),
    )
}

fn parse_hex_digits(b: &[u8], i: &mut usize, count: usize) -> Option<u32> {
    let mut v: u32 = 0;
    for _ in 0..count {
        if *i >= b.len() {
            return None;
        }
        let d = (b[*i] as char).to_digit(16)?;
        v = v * 16 + d;
        *i += 1;
    }
    Some(v)
}

/// Returns the string representation of the given klog level character
/// (Go `getKlogLevel`).
/// See <https://github.com/kubernetes/klog/blob/main/internal/severity/severity.go#L41-L47>
fn get_klog_level(l: u8) -> &'static str {
    match l {
        b'I' => "INFO",
        b'W' => "WARNING",
        b'E' => "ERROR",
        b'F' => "FATAL",
        _ => "UNKNOWN",
    }
}

/// Go `fieldIndex`.
fn field_index(fields: &[Field], names: &[String]) -> isize {
    for n in names {
        for (j, f) in fields.iter().enumerate() {
            if f.name == n.as_bytes() && !f.value.is_empty() {
                return j as isize;
            }
        }
    }
    -1
}

// Go package-level metric vars from `kubernetescollector/processor.go`
// (`vl_` rebranded to `esl_`; shared families use get_or_create like Go).
static ROWS_INGESTED_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter(r#"esl_rows_ingested_total{type="kubernetes_logs"}"#)
});
static BYTES_INGESTED_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter(
        r#"esl_bytes_ingested_total{type="kubernetes_logs"}"#,
    )
});
static TOO_LONG_LINES_SKIPPED: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter("esl_too_long_lines_skipped_total")
});
static ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter(
        r#"esl_rows_dropped_total{reason="too_many_fields"}"#,
    )
});
static ROWS_DROPPED_TOTAL_INVALID_CRI: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter(
        r#"esl_rows_dropped_total{reason="invalid_cri_line"}"#,
    )
});

/// Go `stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stream {
    Stdout,
    Stderr,
}

/// Go `criLine`.
struct CriLine<'a> {
    /// timestamp of the log entry, from the perspective of Container Runtime.
    timestamp: i64,
    /// stream contains the output stream such as stdout or stderr.
    stream: Stream,
    /// partial is true if the log line is split into multiple lines.
    partial: bool,
    /// content of the log entry.
    content: &'a [u8],
}

/// Parses a log line in CRI format (Go `parseCRILine`).
fn parse_cri_line(b: &[u8]) -> Result<CriLine<'_>, String> {
    let n = b
        .iter()
        .position(|&c| c == b' ')
        .ok_or_else(|| "unexpected end of timestamp".to_string())?;
    let v = &b[..n];
    let b = &b[n + 1..];
    let ts_str = std::str::from_utf8(v)
        .map_err(|_| format!("invalid timestamp {:?}", String::from_utf8_lossy(v)))?;
    let timestamp = try_parse_timestamp_rfc3339_nano(ts_str)
        .ok_or_else(|| format!("invalid timestamp {ts_str:?}"))?;

    let n = b
        .iter()
        .position(|&c| c == b' ')
        .ok_or_else(|| "unexpected end of stream".to_string())?;
    let stream = if &b[..n] == b"stdout" {
        Stream::Stdout
    } else {
        Stream::Stderr
    };
    let b = &b[n + 1..];

    let n = b
        .iter()
        .position(|&c| c == b' ')
        .ok_or_else(|| "unexpected end of follow flag".to_string())?;
    let v = &b[..n];
    let b = &b[n + 1..];
    if v.len() != 1 {
        return Err("invalid length of follow flag".to_string());
    }
    let partial = v[0] == b'P';

    let content = b;

    Ok(CriLine {
        timestamp,
        stream,
        partial,
        content,
    })
}

/// Parses a log line in JSON format used by Docker 'json-file' logging driver
/// (Go `parseCRILineJSON`).
/// See <https://docs.docker.com/engine/logging/drivers/json-file/>
///
/// Returns the timestamp and the (owned, unescaped) log content; json-file
/// lines are always complete (Go sets `partial: false`).
fn parse_cri_line_json(b: &[u8]) -> Result<(i64, String), String> {
    let v = json_parse(b)?;
    let Value::Object(_) = v else {
        return Err("value doesn't contain object".to_string());
    };

    let log_content = match v.get("log") {
        None => return Err("missing 'log' field".to_string()),
        Some(f) => f
            .as_str()
            .ok_or_else(|| "'log' field doesn't contain string".to_string())?,
    };

    let timestamp_str = match v.get("time") {
        None => return Err("missing 'time' field".to_string()),
        Some(f) => f
            .as_str()
            .ok_or_else(|| "'time' field doesn't contain string".to_string())?,
    };
    let timestamp = try_parse_timestamp_rfc3339_nano(timestamp_str)
        .ok_or_else(|| format!("invalid timestamp {timestamp_str:?}"))?;

    Ok((timestamp, log_content.to_string()))
}

/// Go `getTenantID` (`sync.Once` → `OnceLock`).
fn get_tenant_id() -> TenantID {
    static PARSED_TENANT_ID: OnceLock<TenantID> = OnceLock::new();
    *PARSED_TENANT_ID.get_or_init(|| {
        let s = TENANT_ID.get();
        match parse_tenant_id(s) {
            Ok(v) => v,
            Err(err) => {
                fatalf!("cannot parse -kubernetesCollector.tenantID={s:?}: {err}");
                unreachable!()
            }
        }
    })
}

/// Go `getExtraFields`.
fn get_extra_fields() -> &'static [Field] {
    static PARSED_EXTRA_FIELDS: OnceLock<Vec<Field>> = OnceLock::new();
    PARSED_EXTRA_FIELDS.get_or_init(|| {
        let s = EXTRA_FIELDS.get();
        if s.is_empty() {
            return Vec::new();
        }

        let mut p = get_json_parser();
        if let Err(err) = p.parse_log_message(s.as_bytes(), &[], "") {
            fatalf!("cannot parse -kubernetesCollector.extraFields={s:?}: {err}");
            unreachable!()
        }

        let mut fields = p.fields().to_vec();
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        put_json_parser(p);
        fields
    })
}

const DEFAULT_MSG_FIELDS: [&str; 3] = ["message", "msg", "log"];

/// Go `getMsgFields`.
fn get_msg_fields() -> &'static [String] {
    static V: OnceLock<Vec<String>> = OnceLock::new();
    V.get_or_init(|| {
        let v = MSG_FIELD.get();
        if v.is_empty() {
            return DEFAULT_MSG_FIELDS.iter().map(|s| s.to_string()).collect();
        }
        v.0.clone()
    })
}

const DEFAULT_TIME_FIELDS: [&str; 3] = ["time", "timestamp", "ts"];

/// Go `getTimeFields`.
fn get_time_fields() -> &'static [String] {
    static V: OnceLock<Vec<String>> = OnceLock::new();
    V.get_or_init(|| {
        let v = TIME_FIELD.get();
        if v.is_empty() {
            return DEFAULT_TIME_FIELDS.iter().map(|s| s.to_string()).collect();
        }
        v.0.clone()
    })
}

/// defaultStreamFields is a list of default _stream fields.
/// Must be synced with [`get_common_fields`].
const DEFAULT_STREAM_FIELDS: [&str; 3] = [
    "kubernetes.container_name",
    "kubernetes.pod_name",
    "kubernetes.pod_namespace",
];

/// Go `getStreamFields`.
fn get_stream_fields() -> &'static [String] {
    static V: OnceLock<Vec<String>> = OnceLock::new();
    V.get_or_init(|| {
        let v = STREAM_FIELDS.get();
        if v.is_empty() {
            return DEFAULT_STREAM_FIELDS
                .iter()
                .map(|s| s.to_string())
                .collect();
        }
        v.0.clone()
    })
}

/// Go `shouldIncludeMetadataField` (+ `initMetadataIncludeFlags`).
fn should_include_metadata_field(field: &str) -> bool {
    static METADATA_INCLUDE_FLAGS: OnceLock<[(&'static str, bool); 6]> = OnceLock::new();
    let flags = METADATA_INCLUDE_FLAGS.get_or_init(|| {
        [
            ("kubernetes.pod_labels.", *INCLUDE_POD_LABELS.get()),
            (
                "kubernetes.pod_annotations.",
                *INCLUDE_POD_ANNOTATIONS.get(),
            ),
            ("kubernetes.node_labels.", *INCLUDE_NODE_LABELS.get()),
            (
                "kubernetes.node_annotations.",
                *INCLUDE_NODE_ANNOTATIONS.get(),
            ),
            (
                "kubernetes.namespace_labels.",
                *INCLUDE_NAMESPACE_LABELS.get(),
            ),
            (
                "kubernetes.namespace_annotations.",
                *INCLUDE_NAMESPACE_ANNOTATIONS.get(),
            ),
        ]
    });

    for (prefix, include) in flags {
        if field.starts_with(prefix) {
            return *include;
        }
    }
    // Not a metadata field.
    true
}

/// Go `mustGetFieldValByName`.
fn must_get_field_val_by_name<'a>(common_fields: &'a [Field], field_name: &[u8]) -> &'a [u8] {
    match common_fields.iter().find(|f| f.name == field_name) {
        Some(f) => &f.value,
        None => panic!(
            "BUG: cannot find field {:?} in commonFields",
            // Display-only lossy view of the raw name bytes (panic text).
            String::from_utf8_lossy(field_name)
        ),
    }
}

// ===========================================================================
// Local support: date math, base64, JSON and YAML value parsing
// ===========================================================================
//
// PORT NOTE: these replace Go std/vendored packages that are not available in
// the workspace: `time` civil-date math, `encoding/base64` and
// `valyala/fastjson` (the esl-logstorage port is crate-private). Kubeconfig
// YAML is parsed with the `yaml-rust2` crate (see `yaml_parse`), the
// `gopkg.in/yaml.v2` stand-in.

fn is_leap_year(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days since the unix epoch for the given civil date (Howard Hinnant's
/// days_from_civil algorithm). Out-of-range days normalize forward, matching
/// Go's `time.Date`/`AddDate` normalization.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil date `(year, month, day)` for the given days since the unix epoch
/// (Howard Hinnant's civil_from_days algorithm).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Standard base64 decoding (RFC 4648, with padding); replaces Go's
/// `encoding/base64.StdEncoding`.
fn base64_std_decode(s: &str) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Result<u32, String> {
        match c {
            b'A'..=b'Z' => Ok(u32::from(c - b'A')),
            b'a'..=b'z' => Ok(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Ok(u32::from(c - b'0') + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("illegal base64 data at input byte {:?}", c as char)),
        }
    }

    let b: Vec<u8> = s.bytes().filter(|&c| c != b'\r' && c != b'\n').collect();
    let mut out = Vec::with_capacity(b.len() / 4 * 3);
    for chunk in b.chunks(4) {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        let n_data = chunk.len() - pad;
        if pad > 2 || n_data < 2 {
            return Err("illegal base64 data: invalid padding".to_string());
        }
        let mut acc: u32 = 0;
        for &c in &chunk[..n_data] {
            acc = (acc << 6) | val(c)?;
        }
        acc <<= 6 * (4 - n_data);
        out.push((acc >> 16) as u8);
        if n_data >= 3 {
            out.push((acc >> 8) as u8);
        }
        if n_data == 4 {
            out.push(acc as u8);
        }
    }
    Ok(out)
}

/// A parsed JSON (or kubeconfig-YAML) value tree.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

static NULL_VALUE: Value = Value::Null;

impl Value {
    fn get(&self, key: &str) -> Option<&Value> {
        if let Value::Object(m) = self {
            return m.iter().find(|(k, _)| k == key).map(|(_, v)| v);
        }
        None
    }

    /// Like [`Value::get`], but returns `Null` for missing keys, so lookups
    /// can be chained like `encoding/json` struct decoding.
    fn item(&self, key: &str) -> &Value {
        self.get(key).unwrap_or(&NULL_VALUE)
    }

    fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            return Some(s);
        }
        None
    }

    fn str(&self) -> &str {
        self.as_str().unwrap_or("")
    }

    fn num(&self) -> f64 {
        if let Value::Number(n) = self {
            return *n;
        }
        0.0
    }

    fn arr(&self) -> &[Value] {
        if let Value::Array(a) = self {
            return a;
        }
        &[]
    }

    fn string_map(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        if let Value::Object(m) = self {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    out.insert(k.clone(), s.to_string());
                }
            }
        }
        out
    }
}

/// Minimal JSON parser producing a [`Value`] tree.
fn json_parse(b: &[u8]) -> Result<Value, String> {
    let mut r = JsonReader { b, i: 0 };
    r.skip_ws();
    let v = r.parse_value()?;
    r.skip_ws();
    if r.i != b.len() {
        return Err(format!("unexpected trailing data at position {}", r.i));
    }
    Ok(v)
}

struct JsonReader<'a> {
    b: &'a [u8],
    i: usize,
}

impl JsonReader<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\r' | b'\n') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.peek() == Some(c) {
            self.i += 1;
            return Ok(());
        }
        Err(format!("expected {:?} at position {}", c as char, self.i))
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        match self.peek() {
            None => Err("unexpected end of JSON input".to_string()),
            Some(b'n') => self.parse_literal(b"null", Value::Null),
            Some(b't') => self.parse_literal(b"true", Value::Bool(true)),
            Some(b'f') => self.parse_literal(b"false", Value::Bool(false)),
            Some(b'"') => Ok(Value::String(self.parse_string()?)),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(_) => self.parse_number(),
        }
    }

    fn parse_literal(&mut self, lit: &[u8], v: Value) -> Result<Value, String> {
        if self.b.len() - self.i >= lit.len() && &self.b[self.i..self.i + lit.len()] == lit {
            self.i += lit.len();
            return Ok(v);
        }
        Err(format!("invalid literal at position {}", self.i))
    }

    fn parse_number(&mut self) -> Result<Value, String> {
        let start = self.i;
        while self.i < self.b.len()
            && matches!(
                self.b[self.i],
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
            )
        {
            self.i += 1;
        }
        let s = std::str::from_utf8(&self.b[start..self.i])
            .map_err(|_| format!("invalid number at position {start}"))?;
        let n: f64 = s
            .parse()
            .map_err(|_| format!("invalid number {s:?} at position {start}"))?;
        Ok(Value::Number(n))
    }

    fn parse_array(&mut self) -> Result<Value, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Value::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(Value::Array(items));
                }
                _ => return Err(format!("expected ',' or ']' at position {}", self.i)),
            }
        }
    }

    fn parse_object(&mut self) -> Result<Value, String> {
        self.expect(b'{')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(Value::Object(items));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.parse_value()?;
            items.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(Value::Object(items));
                }
                _ => return Err(format!("expected ',' or '}}' at position {}", self.i)),
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Err("unexpected end of string".to_string());
            };
            match c {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    let Some(esc) = self.peek() else {
                        return Err("unexpected end of escape sequence".to_string());
                    };
                    self.i += 1;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{08}'),
                        b'f' => out.push('\u{0C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.parse_u16_hex()?;
                            let cp = if (0xD800..0xDC00).contains(&hi) {
                                // Surrogate pair.
                                if self.peek() == Some(b'\\')
                                    && self.b.get(self.i + 1) == Some(&b'u')
                                {
                                    self.i += 2;
                                    let lo = self.parse_u16_hex()?;
                                    if (0xDC00..0xE000).contains(&lo) {
                                        0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                                    } else {
                                        0xFFFD
                                    }
                                } else {
                                    0xFFFD
                                }
                            } else if (0xDC00..0xE000).contains(&hi) {
                                0xFFFD
                            } else {
                                hi
                            };
                            out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                        }
                        _ => return Err(format!("invalid escape character {:?}", esc as char)),
                    }
                }
                _ => {
                    let rest = std::str::from_utf8(&self.b[self.i..])
                        .map_err(|_| "invalid UTF-8 in string".to_string())?;
                    let ch = rest.chars().next().unwrap();
                    out.push(ch);
                    self.i += ch.len_utf8();
                }
            }
        }
    }

    fn parse_u16_hex(&mut self) -> Result<u32, String> {
        if self.b.len() - self.i < 4 {
            return Err("truncated \\u escape".to_string());
        }
        let mut v: u32 = 0;
        for _ in 0..4 {
            let d = (self.b[self.i] as char)
                .to_digit(16)
                .ok_or_else(|| "invalid \\u escape".to_string())?;
            v = v * 16 + d;
            self.i += 1;
        }
        Ok(v)
    }
}

/// Parses a kubeconfig YAML document into a [`Value`] tree using the
/// `yaml-rust2` library — the `gopkg.in/yaml.v2` stand-in. Unlike the former
/// minimal block-style parser, this handles the full YAML surface Go accepts:
/// flow style (`{a: b}`, `[x, y]`), anchors/aliases (`&a`/`*a`) and quoted or
/// block/folded (`|`, `>`, `"..."`, `'...'`) scalars.
///
/// Only the first document is used (a kubeconfig is a single document); an
/// empty input yields [`Value::Null`], and malformed YAML returns an error so
/// callers still fail clearly.
fn yaml_parse(src: &str) -> Result<Value, String> {
    let docs = YamlLoader::load_from_str(src).map_err(|err| err.to_string())?;
    match docs.first() {
        Some(doc) => Ok(yaml_to_value(doc)),
        None => Ok(Value::Null),
    }
}

/// Converts a `yaml-rust2` [`Yaml`] node into the shared [`Value`] tree.
///
/// Scalar nodes that YAML resolves to a non-string type (integers, floats,
/// booleans) are folded to their string spelling, matching Go's yaml.v2 when
/// it decodes into the `string`-typed kubeconfig structs: the field readers
/// only ever call [`Value::str`], so an unquoted numeric token still reads
/// back as its text. Anchors/aliases are already expanded by the loader.
fn yaml_to_value(y: &Yaml) -> Value {
    match y {
        Yaml::Null | Yaml::BadValue | Yaml::Alias(_) => Value::Null,
        Yaml::String(s) => Value::String(s.clone()),
        Yaml::Boolean(b) => Value::String(b.to_string()),
        Yaml::Integer(i) => Value::String(i.to_string()),
        Yaml::Real(s) => Value::String(s.clone()),
        Yaml::Array(a) => Value::Array(a.iter().map(yaml_to_value).collect()),
        Yaml::Hash(h) => Value::Object(
            h.iter()
                .filter_map(|(k, v)| yaml_key_to_string(k).map(|k| (k, yaml_to_value(v))))
                .collect(),
        ),
    }
}

/// Renders a YAML mapping key as a string. Kubeconfig keys are always plain
/// string scalars; non-scalar keys are dropped, matching how the struct
/// readers would ignore anything they cannot name.
fn yaml_key_to_string(y: &Yaml) -> Option<String> {
    match y {
        Yaml::String(s) => Some(s.clone()),
        Yaml::Boolean(b) => Some(b.to_string()),
        Yaml::Integer(i) => Some(i.to_string()),
        Yaml::Real(s) => Some(s.clone()),
        _ => None,
    }
}

// ===========================================================================
// Tests (processor_test.go, processor_timing_test.go)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of Go `testLogRowsStorage` — implements the
    /// `insertutil.LogRowsStorage` interface.
    #[derive(Default)]
    struct TestLogRowsStorage {
        log_rows: Mutex<Vec<String>>,
    }

    impl LogRowsStorage for TestLogRowsStorage {
        fn must_add_rows(&self, lr: &LogRows) {
            let mut rows = self.log_rows.lock().unwrap();
            for i in 0..lr.rows_count() {
                rows.push(lr.get_row_string(i));
            }
        }

        fn can_write_data(&self) -> Result<(), String> {
            Ok(())
        }
    }

    impl TestLogRowsStorage {
        fn verify(&self, expected: &str) -> Result<(), String> {
            let got = self.log_rows.lock().unwrap().join("\n");
            if got != expected {
                return Err(format!("unexpected rows\ngot:\n{got}\nwant:\n{expected}"));
            }
            Ok(())
        }

        fn rows_len(&self) -> usize {
            self.log_rows.lock().unwrap().len()
        }
    }

    fn unix_nanos_at(year: i64, month: i64, day: i64) -> i64 {
        days_from_civil(year, month, day) * 86_400 * 1_000_000_000
    }

    #[test]
    fn test_processor() {
        fn f(input: &[&str], results_expected: &[&str]) {
            let storage = Arc::new(TestLogRowsStorage::default());
            let common_fields = get_common_fields(
                &Node::default(),
                &Namespace::default(),
                &Pod::default(),
                &ContainerStatus::default(),
            );
            let mut proc = new_log_file_processor(
                Arc::clone(&storage) as Arc<dyn LogRowsStorage>,
                &common_fields,
            );

            for s in input {
                proc.try_add_line(s.as_bytes());
            }

            let expected = results_expected.join("\n");
            if let Err(err) = storage.verify(&expected) {
                panic!("unexpected result: {err}");
            }
            proc.must_close();
        }

        // Full line
        let input = [r#"2025-10-16T15:37:36.330062387Z stderr F foo bar"#];
        let expected_contents =
            [r#"{"_msg":"foo bar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}"#];
        f(&input, &expected_contents);

        // Multiple full lines
        let input = [
            r#"2025-10-16T15:37:36.1Z stderr F foo"#,
            r#"2025-10-16T15:37:36.2Z stderr F bar"#,
            r#"2025-10-16T15:37:36.3Z stderr F buz"#,
            r#"2025-10-16T15:37:36.4Z stderr F ping"#,
            r#"2025-10-16T15:37:36.5Z stderr F pong"#,
        ];
        let expected_contents = [
            r#"{"_msg":"foo","_stream":"{}","_time":"2025-10-16T15:37:36.1Z"}"#,
            r#"{"_msg":"bar","_stream":"{}","_time":"2025-10-16T15:37:36.2Z"}"#,
            r#"{"_msg":"buz","_stream":"{}","_time":"2025-10-16T15:37:36.3Z"}"#,
            r#"{"_msg":"ping","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}"#,
            r#"{"_msg":"pong","_stream":"{}","_time":"2025-10-16T15:37:36.5Z"}"#,
        ];
        f(&input, &expected_contents);

        // Partial line
        let input = [
            r#"2025-10-16T15:37:36Z stderr P foo"#,
            r#"2025-10-16T15:37:36.330062387Z stderr F bar"#,
        ];
        let expected_contents =
            [r#"{"_msg":"foobar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}"#];
        f(&input, &expected_contents);

        // Mixed full and partial lines
        let input = [
            r#"2025-10-16T15:37:36Z stderr P foo"#,
            r#"2025-10-16T15:37:36Z stderr P bar"#,
            r#"2025-10-16T15:37:36.330062387Z stderr F buz"#,
            r#"2025-10-16T15:37:36.4Z stderr F ping"#,
            r#"2025-10-16T15:37:36Z stderr P pong"#,
            r#"2025-10-16T15:37:36.5Z stderr F last"#,
        ];
        let expected_contents = [
            r#"{"_msg":"foobarbuz","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}"#,
            r#"{"_msg":"ping","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}"#,
            r#"{"_msg":"ponglast","_stream":"{}","_time":"2025-10-16T15:37:36.5Z"}"#,
        ];
        f(&input, &expected_contents);

        // Interleaved streams must keep independent partial state.
        let input = [
            r#"2025-10-16T15:37:36.1Z stdout P 1"#,
            r#"2025-10-16T15:37:36.2Z stderr F 2"#,
            r#"2025-10-16T15:37:36.3Z stdout P 3"#,
            r#"2025-10-16T15:37:36.4Z stdout F 4"#,
        ];
        let expected_contents = [
            r#"{"_msg":"2","_stream":"{}","_time":"2025-10-16T15:37:36.2Z"}"#,
            r#"{"_msg":"134","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}"#,
        ];
        f(&input, &expected_contents);

        // Max log line size
        let first_line = "a".repeat(MAX_LOG_LINE_SIZE / 2 - "2025-10-16T15:37:36Z stderr P ".len());
        let second_line =
            "b".repeat(MAX_LOG_LINE_SIZE / 2 - "2025-10-16T15:37:36.330062387Z stderr F ".len());
        let input = [
            format!("2025-10-16T15:37:36Z stderr P {first_line}"),
            format!("2025-10-16T15:37:36.330062387Z stderr F {second_line}"),
        ];
        let expected = [format!(
            "{{\"_msg\":\"{first_line}{second_line}\",\"_stream\":\"{{}}\",\"_time\":\"2025-10-16T15:37:36.330062387Z\"}}"
        )];
        let input_refs: Vec<&str> = input.iter().map(String::as_str).collect();
        let expected_refs: Vec<&str> = expected.iter().map(String::as_str).collect();
        f(&input_refs, &expected_refs);

        // Too long partial line
        let input = [
            format!(
                "2025-10-16T15:37:36Z stderr P {}",
                "a".repeat(MAX_LOG_LINE_SIZE)
            ),
            format!(
                "2025-10-16T15:37:36.330062387Z stderr F {}",
                "b".repeat(MAX_LOG_LINE_SIZE)
            ),
            "2025-10-16T15:37:36.4Z stderr F complete line".to_string(),
        ];
        let expected_contents =
            [r#"{"_msg":"complete line","_stream":"{}","_time":"2025-10-16T15:37:36.4Z"}"#];
        let input_refs: Vec<&str> = input.iter().map(String::as_str).collect();
        f(&input_refs, &expected_contents);

        // Empty line
        let input = [r#"2025-10-16T15:37:36Z stderr F "#];
        let expected_contents: [&str; 0] = [];
        f(&input, &expected_contents);

        // Test driver json-file
        let input =
            [r#"{"log":"foo\tbar","stream":"stderr","time":"2025-10-16T15:37:36.330062387Z"}"#];
        let expected_contents =
            [r#"{"_msg":"foo\tbar","_stream":"{}","_time":"2025-10-16T15:37:36.330062387Z"}"#];
        f(&input, &expected_contents);
    }

    #[test]
    fn test_parse_klog() {
        let current = unix_nanos_at(1971, 12, 20);

        let f = |src: &str, fields_expected: &str, timestamp_expected: i64| {
            let Some((timestamp, fields)) = try_parse_klog(Vec::new(), src, current) else {
                panic!("cannot parse klog line {src:?}");
            };

            let mut got = Vec::new();
            marshal_fields_to_json(&mut got, &fields);
            let got = String::from_utf8(got).unwrap();
            assert_eq!(
                got, fields_expected,
                "unexpected result\ngot:\n{got}\nwant:\n{fields_expected}"
            );

            assert_eq!(
                timestamp, timestamp_expected,
                "unexpected timestamp; got {timestamp}; want {timestamp_expected}"
            );
        };

        // Parse simple line
        let input = r#"I1215 07:34:12.017826       94 serving.go:374] foobar"#;
        let want =
            r#"{"level":"INFO","thread_id":"94","source_line":"serving.go:374","_msg":"foobar"}"#;
        let timestamp_expected: i64 = 61630452017826000;
        f(input, want, timestamp_expected);

        // Parse multiple words
        let input = r#"I1215 07:34:12.017826       24 serving.go:374] Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)"#;
        let want = r#"{"level":"INFO","thread_id":"24","source_line":"serving.go:374","_msg":"Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)"}"#;
        let timestamp_expected = 61630452017826000;
        f(input, want, timestamp_expected);

        // Parse key="value" pair
        let input = r#"I1215 07:34:11.695645       42 controller.go:824] "Starting provisioner controller" component="rancher.io/local-path_local-path-provisioner-5cf85fd84d-bf8vk_626b5057-e081-4b71-9a19-5e371ae0211b""#;
        let want = r#"{"level":"INFO","thread_id":"42","source_line":"controller.go:824","_msg":"Starting provisioner controller","component":"rancher.io/local-path_local-path-provisioner-5cf85fd84d-bf8vk_626b5057-e081-4b71-9a19-5e371ae0211b"}"#;
        let timestamp_expected = 61630451695645000;
        f(input, want, timestamp_expected);

        // Parse key="value" pairs
        let input = r#"I1215 10:34:26.907803       1 server.go:191] "Failed probe" probe="metric-storage-ready" err="no metrics to serve""#;
        let want = r#"{"level":"INFO","thread_id":"1","source_line":"server.go:191","_msg":"Failed probe","probe":"metric-storage-ready","err":"no metrics to serve"}"#;
        let timestamp_expected = 61641266907803000;
        f(input, want, timestamp_expected);

        // Parse quoted msg without additional fields
        let input = r#"I1215 07:34:12.324492       1234 tlsconfig.go:240] "Starting DynamicServingCertificateController""#;
        let want = r#"{"level":"INFO","thread_id":"1234","source_line":"tlsconfig.go:240","_msg":"Starting DynamicServingCertificateController"}"#;
        let timestamp_expected = 61630452324492000;
        f(input, want, timestamp_expected);

        // Adjust time to the previous year
        let input = r#"I1221 00:00:00.000001       1234 main.go:1] foo"#;
        let want = r#"{"level":"INFO","thread_id":"1234","source_line":"main.go:1","_msg":"foo"}"#;
        let timestamp_expected = 30585600000001000;
        f(input, want, timestamp_expected);
    }

    // PORT-ONLY TEST: pins the strconv.Unquote escape semantics of unquote().
    // `\xHH`/octal escapes decode to raw bytes like Go, so UTF-8 spelled via
    // `\x` matches Go byte-for-byte; decoded bytes that are NOT valid UTF-8
    // are U+FFFD-replaced where Go keeps the raw bytes (see the PORT NOTE on
    // unquote).
    #[test]
    fn test_unquote_escapes() {
        // \x escapes composing valid UTF-8 match Go exactly.
        assert_eq!(unquote(r#""\xc3\xa9""#).unwrap(), "é");
        // Octal escapes composing valid UTF-8 match Go exactly.
        assert_eq!(unquote(r#""\303\251""#).unwrap(), "é");
        // \u/\U escapes.
        assert_eq!(unquote(r#""é \U0001F600""#).unwrap(), "é 😀");
        // Lone invalid byte: Go stores raw 0xFF, the port replaces it.
        assert_eq!(unquote(r#""\xff""#).unwrap(), "\u{FFFD}");
        // Go errors on octal values > 255, on surrogate \u escapes, and on
        // \' inside double-quoted strings.
        assert!(unquote(r#""\400""#).is_none());
        assert!(unquote(r#""\ud800""#).is_none());
        assert!(unquote(r#""don\'t""#).is_none());
    }

    // PORT-ONLY TEST: pins the invalid-UTF-8 handling of
    // parse_log_row_content. Go parses klog from the raw bytes and stores
    // the raw message bytes; the port lossy-converts first, so the klog
    // structure matches Go and only invalid sequences become U+FFFD.
    #[test]
    fn test_parse_log_row_content_invalid_utf8() {
        // klog line with a raw 0xFF byte in the message still parses as klog.
        let mut p = get_json_parser();
        let (_, ok) = parse_log_row_content(
            &mut p,
            b"I1215 07:34:12.017826       94 serving.go:374] a\xffb",
        );
        assert!(ok, "expected klog line to parse");
        let mut got = Vec::new();
        marshal_fields_to_json(&mut got, p.fields());
        let got = String::from_utf8(got).unwrap();
        assert_eq!(
            got,
            "{\"level\":\"INFO\",\"thread_id\":\"94\",\"source_line\":\"serving.go:374\",\"_msg\":\"a\u{FFFD}b\"}",
            "unexpected result: {got}"
        );
        put_json_parser(p);

        // Non-klog, non-JSON line with invalid UTF-8 is not structured
        // content; the caller stores it as a lossy `_msg`.
        let mut p = get_json_parser();
        let (_, ok) = parse_log_row_content(&mut p, b"a\xffb");
        assert!(!ok, "expected unstructured line");
        put_json_parser(p);
    }

    #[test]
    fn test_parse_klog_failure() {
        let f = |src: &str| {
            if let Some((_, fields)) = try_parse_klog(Vec::new(), src, now_unix_nanos()) {
                let mut got = Vec::new();
                marshal_fields_to_json(&mut got, &fields);
                panic!("unexpected success; got\n{}", String::from_utf8_lossy(&got));
            }
        };

        // Empty line
        f("");
        f("   ");

        // Invalid timestamp
        f("I foobar");
        f("Ifoobar");
        f("I1215 01:34:12.000000999 1 main.go:1] foo");
        f("I1215 01:34:12.000000");
        f("I1215 01:34:12.");
        f("I1215 01:34");
        f("I1215 01");
        f("I1215 ");
        f("I1215");
        f("I12");
        f("I");

        // Missing msg
        f("I1215 07:34:12.017826       1 serving.go:374] ");
        f("I1215 07:34:12.017826       1 serving.go:374]");
        f("I1215 07:34:12.017826       1 serving.go:374");

        // Missing thread ID
        f("I1215 07:34:12.017826");
        f("I1215 07:34:12.017826 ");
        f("I1215 07:34:12.324492 1234tlsconfig.go:240] foo");

        // Unfinished quoted msg
        f(r#"I1215 07:34:12.324492       1234 tlsconfig.go:240] "Starting"#);

        // Unfinished key="value" pair
        f(
            r#"I1215 07:34:12.324309       1 configmap_cafile_content.go:202] "Starting controller" name="client-"#,
        );
    }

    #[test]
    fn test_parse_cri_line() {
        let f = |line: &str,
                 stream_expected: Stream,
                 timestamp_expected: i64,
                 partial_expected: bool,
                 content_expected: &str| {
            let cri_line = match parse_cri_line(line.as_bytes()) {
                Ok(cri_line) => cri_line,
                Err(err) => panic!("cannot parse CRI log line {line:?}: {err}"),
            };
            assert_eq!(
                cri_line.timestamp, timestamp_expected,
                "unexpected timestamp; got {}; want {timestamp_expected}",
                cri_line.timestamp
            );
            assert_eq!(
                cri_line.stream, stream_expected,
                "unexpected stream; got {:?}; want {stream_expected:?}",
                cri_line.stream
            );
            assert_eq!(
                cri_line.partial, partial_expected,
                "unexpected partial; got {}; want {partial_expected}",
                cri_line.partial
            );
            assert_eq!(
                cri_line.content,
                content_expected.as_bytes(),
                "unexpected content; got {:?}; want {content_expected:?}",
                String::from_utf8_lossy(cri_line.content)
            );
        };

        // Full line
        f(
            r#"2025-10-16T15:37:36.330062387Z stderr F foo bar"#,
            Stream::Stderr,
            1760629056330062387,
            false,
            "foo bar",
        );

        // Partial line
        f(
            r#"2025-10-16T15:37:36Z stdout P partial log line"#,
            Stream::Stdout,
            1760629056000000000,
            true,
            "partial log line",
        );

        // Empty content
        f(
            r#"2025-10-16T15:37:36Z stdout P "#,
            Stream::Stdout,
            1760629056000000000,
            true,
            "",
        );

        // Content with spaces
        f(
            r#"2025-10-16T15:37:36Z stdout F  "#,
            Stream::Stdout,
            1760629056000000000,
            false,
            " ",
        );
        f(
            r#"2025-10-16T15:37:36Z stdout F      "#,
            Stream::Stdout,
            1760629056000000000,
            false,
            "     ",
        );
    }

    // -----------------------------------------------------------------------
    // processor_timing_test.go
    //
    // PORT NOTE: the Go benchmarks are ported as plain correctness checks per
    // the porting instructions — each benchmark corpus is fed through the
    // processor once and the number of ingested rows is asserted.
    // -----------------------------------------------------------------------

    fn run_processor_lines(log_lines: &[&str], rows_expected: usize) {
        let storage = Arc::new(TestLogRowsStorage::default());
        let common_fields = vec![Field {
            name: b"name".to_vec(),
            value: b"benchmarkProcessor".to_vec(),
        }];
        let mut proc = new_log_file_processor(
            Arc::clone(&storage) as Arc<dyn LogRowsStorage>,
            &common_fields,
        );
        for line in log_lines {
            proc.try_add_line(line.as_bytes());
        }
        proc.must_close();
        assert_eq!(
            storage.rows_len(),
            rows_expected,
            "unexpected rows count; got {}; want {rows_expected}",
            storage.rows_len()
        );
    }

    #[test]
    fn test_processor_full_lines() {
        let data = [
            "2025-10-16T15:37:36.330062387Z stderr F foo",
            "2025-10-16T15:37:36.330062387Z stderr F bar",
            "2025-10-16T15:37:36.330062387Z stderr F buz",
        ];
        run_processor_lines(&data, 3);
    }

    #[test]
    fn test_processor_partial_lines() {
        let input = [
            "2025-10-16T15:37:36.330062387Z stderr P foo",
            "2025-10-16T15:37:36.330062387Z stderr P bar",
            "2025-10-16T15:37:36.330062387Z stderr F buz",
        ];
        run_processor_lines(&input, 1);
    }

    #[test]
    fn test_processor_klog() {
        let input = [
            r#"2025-12-15T10:34:25.637326000Z stderr F I1215 10:34:25.637326       1 serving.go:374] Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)"#,
            r#"2025-12-15T10:34:25.872911000Z stderr F I1215 10:34:25.872911       1 handler.go:275] Adding GroupVersion metrics.k8s.io v1beta1 to ResourceManager"#,
            r#"2025-12-15T10:34:25.977313000Z stderr F I1215 10:34:25.977313       1 requestheader_controller.go:169] Starting RequestHeaderAuthRequestController"#,
            r#"2025-12-15T10:34:25.977317000Z stderr F I1215 10:34:25.977317       1 configmap_cafile_content.go:202] "Starting controller" name="client-ca::kube-system::extension-apiserver-authentication::client-ca-file""#,
            r#"2025-12-15T10:34:25.977332000Z stderr F I1215 10:34:25.977332       1 shared_informer.go:311] Waiting for caches to sync for RequestHeaderAuthRequestController"#,
            r#"2025-12-15T10:34:25.977336000Z stderr F I1215 10:34:25.977336       1 shared_informer.go:311] Waiting for caches to sync for client-ca::kube-system::extension-apiserver-authentication::requestheader-client-ca-file"#,
            r#"2025-12-15T10:34:25.977526000Z stderr F I1215 10:34:25.977526       1 dynamic_serving_content.go:132] "Starting controller" name="serving-cert::/tmp/apiserver.crt::/tmp/apiserver.key""#,
            r#"2025-12-15T10:34:25.977591000Z stderr F I1215 10:34:25.977591       1 secure_serving.go:213] Serving securely on [::]:10250"#,
            r#"2025-12-15T10:34:25.977605000Z stderr F I1215 10:34:25.977605       1 tlsconfig.go:240] "Starting DynamicServingCertificateController""#,
            r#"2025-12-15T10:34:26.077533000Z stderr F I1215 10:34:26.077533       1 shared_informer.go:318] Caches are synced for RequestHeaderAuthRequestController"#,
            r#"2025-12-15T10:34:26.948143000Z stderr F I1215 10:34:26.948143       1 server.go:191] "Failed probe" probe="metric-storage-ready" err="no metrics to serve""#,
        ];
        run_processor_lines(&input, 11);
    }

    #[test]
    fn test_processor_json() {
        let input = [
            r#"2025-12-15T10:34:25.637326000Z stderr F {"message":"Generated self-signed cert","file":"/tmp/apiserver.crt","key":"/tmp/apiserver.key","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.872911000Z stderr F {"message":"Adding GroupVersion metrics.k8s.io v1beta1 to ResourceManager","component":"handler","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977313000Z stderr F {"message":"Starting RequestHeaderAuthRequestController","controller":"requestheader","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977317000Z stderr F {"message":"Starting controller","name":"client-ca::kube-system::extension-apiserver-authentication::client-ca-file","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977332000Z stderr F {"message":"Waiting for caches to sync for RequestHeaderAuthRequestController","controller":"shared_informer","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977336000Z stderr F {"message":"Waiting for caches to sync","controller":"client-ca::kube-system::extension-apiserver-authentication::requestheader-client-ca-file","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977526000Z stderr F {"message":"Starting controller","name":"serving-cert::/tmp/apiserver.crt::/tmp/apiserver.key","component":"dynamic_serving","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977591000Z stderr F {"message":"Serving securely on [::]:10250","component":"secure_serving","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:25.977605000Z stderr F {"message":"Starting DynamicServingCertificateController","component":"tlsconfig","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:26.077533000Z stderr F {"message":"Caches are synced for RequestHeaderAuthRequestController","controller":"shared_informer","severity":"INFO","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
            r#"2025-12-15T10:34:26.948143000Z stderr F {"message":"Failed probe","probe":"metric-storage-ready","error":"no metrics to serve","severity":"ERROR","kubernetes.container_name":"test-container","kubernetes.pod_name":"test-pod","kubernetes.pod_namespace":"test-namespace"}"#,
        ];
        run_processor_lines(&input, 11);
    }

    // -----------------------------------------------------------------------
    // TLS: kubeconfig parsing + client config build, https requests and the
    // streaming watch reader over TLS.
    //
    // PORT NOTE: no upstream test file corresponds to these; Go gets TLS from
    // net/http + promauth (covered by lib/promauth tests upstream).
    // -----------------------------------------------------------------------

    /// Minimal std base64 encoder for building kubeconfig `*-data` fields.
    fn base64_std_encode(data: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let n = (u32::from(chunk[0]) << 16)
                | (u32::from(chunk.get(1).copied().unwrap_or(0)) << 8)
                | u32::from(chunk.get(2).copied().unwrap_or(0));
            out.push(TABLE[(n >> 18) as usize & 63] as char);
            out.push(TABLE[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                TABLE[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                TABLE[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }

    /// Generates a self-signed cert for localhost/127.0.0.1 and returns
    /// `(cert_pem, key_pem)`.
    fn generate_test_cert() -> (String, String) {
        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        (ck.cert.pem(), ck.key_pair.serialize_pem())
    }

    /// Creates a unique temp dir for a test and returns its path.
    fn test_temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("esl-agent-k8s-tls-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn kubeconfig_yaml(cluster_fields: &str, user_fields: &str) -> String {
        format!(
            "current-context: ctx\n\
             clusters:\n\
             - name: c1\n\
             \x20 cluster:\n\
             \x20   server: https://127.0.0.1:6443\n\
             {cluster_fields}\
             contexts:\n\
             - name: ctx\n\
             \x20 context:\n\
             \x20   cluster: c1\n\
             \x20   user: u1\n\
             users:\n\
             - name: u1\n\
             \x20 user:\n\
             \x20   token: test-token\n\
             {user_fields}"
        )
    }

    #[test]
    fn test_local_config_tls_inline_data() {
        let (cert_pem, key_pem) = generate_test_cert();
        let cluster = format!(
            "\x20   certificate-authority-data: {}\n",
            base64_std_encode(cert_pem.as_bytes())
        );
        let user = format!(
            "\x20   client-certificate-data: {}\n\x20   client-key-data: {}\n",
            base64_std_encode(cert_pem.as_bytes()),
            base64_std_encode(key_pem.as_bytes())
        );
        let yaml = kubeconfig_yaml(&cluster, &user);

        let cfg = local_config_from_yaml(&yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.server, "https://127.0.0.1:6443");
        assert_eq!(cfg.ac.bearer_token, "test-token");
        // The TLS client config is built eagerly from the inline PEM data.
        assert!(cfg.ac.tls.is_some());

        let client = new_kube_api_client(cfg).unwrap();
        assert_eq!(client.scheme, "https");
        assert!(client.tls.is_some());
    }

    #[test]
    fn test_local_config_tls_files() {
        let (cert_pem, key_pem) = generate_test_cert();
        let dir = test_temp_dir("kubeconfig-files");
        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&ca_path, &cert_pem).unwrap();
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();

        let cluster = format!(
            "\x20   certificate-authority: {}\n",
            ca_path.to_str().unwrap()
        );
        let user = format!(
            "\x20   client-certificate: {}\n\x20   client-key: {}\n",
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap()
        );
        let yaml = kubeconfig_yaml(&cluster, &user);

        let cfg = local_config_from_yaml(&yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.server, "https://127.0.0.1:6443");
        assert!(cfg.ac.tls.is_some());
        assert!(new_kube_api_client(cfg).unwrap().tls.is_some());
    }

    #[test]
    fn test_local_config_tls_broken_fails_eagerly() {
        let (cert_pem, _) = generate_test_cert();
        // A client certificate without the matching key must fail when the
        // auth config is built, not on the first request.
        let user = format!(
            "\x20   client-certificate-data: {}\n",
            base64_std_encode(cert_pem.as_bytes())
        );
        let yaml = kubeconfig_yaml("", &user);
        let Err(err) = local_config_from_yaml(&yaml, "test-kubeconfig") else {
            panic!("expecting non-nil error");
        };
        assert!(err.contains("cannot initialize"), "unexpected error: {err}");
    }

    // -----------------------------------------------------------------------
    // Kubeconfig YAML surface now parsed with the full `yaml-rust2` library.
    // Each case below uses a YAML feature the former minimal block-style
    // parser could NOT handle but Go's yaml.v2 accepts, and asserts the
    // server/token fields still extract correctly.
    // -----------------------------------------------------------------------

    #[test]
    fn test_local_config_yaml_flow_style() {
        // Flow-style mappings `{...}` and sequences `[...]`.
        let yaml = "current-context: ctx\n\
             clusters: [{name: c1, cluster: {server: \"https://127.0.0.1:6443\"}}]\n\
             contexts: [{name: ctx, context: {cluster: c1, user: u1}}]\n\
             users: [{name: u1, user: {token: flow-token}}]\n";
        let cfg = local_config_from_yaml(yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.server, "https://127.0.0.1:6443");
        assert_eq!(cfg.ac.bearer_token, "flow-token");
    }

    #[test]
    fn test_local_config_yaml_anchor_alias() {
        // The current context points at cluster c2, whose `cluster` block is
        // the alias `*clusterdef`; if aliases were not resolved the server
        // would come back empty.
        let yaml = "current-context: ctx\n\
             clusters:\n\
             - name: c1\n\
             \x20 cluster: &clusterdef\n\
             \x20   server: https://10.0.0.1:6443\n\
             - name: c2\n\
             \x20 cluster: *clusterdef\n\
             contexts:\n\
             - name: ctx\n\
             \x20 context: {cluster: c2, user: u1}\n\
             users:\n\
             - name: u1\n\
             \x20 user:\n\
             \x20   token: anchor-token\n";
        let cfg = local_config_from_yaml(yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.server, "https://10.0.0.1:6443");
        assert_eq!(cfg.ac.bearer_token, "anchor-token");
    }

    #[test]
    fn test_local_config_yaml_double_quoted_escapes() {
        // A double-quoted scalar with `\t` and `\"` escapes.
        let yaml = "current-context: ctx\n\
             clusters:\n\
             - name: c1\n\
             \x20 cluster:\n\
             \x20   server: https://127.0.0.1:6443\n\
             contexts:\n\
             - name: ctx\n\
             \x20 context: {cluster: c1, user: u1}\n\
             users:\n\
             - name: u1\n\
             \x20 user:\n\
             \x20   token: \"ab\\tcd\\\"ef\"\n";
        let cfg = local_config_from_yaml(yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.ac.bearer_token, "ab\tcd\"ef");
    }

    #[test]
    fn test_local_config_yaml_block_and_folded_scalars() {
        // Folded (`>`) server and block-literal (`|`) token; both keep the
        // trailing newline yaml.v2 produces and the old parser never could.
        let yaml = "current-context: ctx\n\
             clusters:\n\
             - name: c1\n\
             \x20 cluster:\n\
             \x20   server: >\n\
             \x20     https://127.0.0.1:6443\n\
             contexts:\n\
             - name: ctx\n\
             \x20 context: {cluster: c1, user: u1}\n\
             users:\n\
             - name: u1\n\
             \x20 user:\n\
             \x20   token: |\n\
             \x20     secret-token\n";
        let cfg = local_config_from_yaml(yaml, "test-kubeconfig").unwrap();
        assert_eq!(cfg.server, "https://127.0.0.1:6443\n");
        assert_eq!(cfg.ac.bearer_token, "secret-token\n");
    }

    #[test]
    fn test_local_config_yaml_malformed_errors() {
        // An unterminated flow sequence is a YAML syntax error; the parse
        // must fail clearly rather than silently returning an empty config.
        let Err(err) = local_config_from_yaml("clusters: [unclosed", "test-kubeconfig") else {
            panic!("expecting non-nil error");
        };
        assert!(err.contains("cannot parse yaml"), "unexpected error: {err}");
    }

    /// Spawns a one-shot https server that reads one request head and answers
    /// with `response`, returning `(addr, ca_pem, captured-request handle)`.
    fn spawn_tls_api_server(
        name: &str,
        response: Vec<u8>,
    ) -> (String, String, std::thread::JoinHandle<Vec<u8>>) {
        let (cert_pem, key_pem) = generate_test_cert();
        let dir = test_temp_dir(name);
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();
        let server_cfg = esl_common::tlsutil::get_server_tls_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            "",
            &[],
        )
        .unwrap();

        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (tcp, _) = ln.accept().unwrap();
            let mut stream = esl_common::tlsutil::server_accept(&server_cfg, tcp).unwrap();
            let mut req = vec![0u8; 8192];
            let mut n = 0;
            while !req[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                let m = stream.read(&mut req[n..]).unwrap();
                assert!(m > 0, "request truncated");
                n += m;
            }
            stream.write_all(&response).unwrap();
            stream.conn.send_close_notify();
            // Flush after send_close_notify BEFORE the socket is shut down,
            // or client reads fail with Broken pipe.
            stream.flush().unwrap();
            req.truncate(n);
            req
        });
        (addr, cert_pem, handle)
    }

    #[test]
    fn test_get_nodes_over_tls() {
        let body = br#"{"items":[{"metadata":{"name":"node-a"}},{"metadata":{"name":"node-b"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect();
        let (addr, ca_pem, handle) = spawn_tls_api_server("get-nodes", response);

        let opts = AuthOptions {
            bearer_token: "test-token".to_string(),
            tls_config: TLSConfig {
                ca: ca_pem,
                ..TLSConfig::default()
            },
            ..AuthOptions::default()
        };
        let cfg = KubeApiConfig {
            server: format!("https://{addr}"),
            ac: opts.new_config().unwrap(),
        };
        let client = new_kube_api_client(cfg).unwrap();

        let nodes = client.get_nodes().unwrap();
        assert_eq!(nodes, vec!["node-a".to_string(), "node-b".to_string()]);

        let req = String::from_utf8(handle.join().unwrap()).unwrap();
        assert!(
            req.starts_with("GET /api/v1/nodes HTTP/1.1\r\n"),
            "unexpected request: {req:?}"
        );
        assert!(
            req.contains("Authorization: Bearer test-token\r\n"),
            "missing auth header: {req:?}"
        );
    }

    #[test]
    fn test_watch_node_pods_over_tls() {
        let events = [
            r#"{"type":"ADDED","object":{"metadata":{"name":"pod-1","namespace":"ns1","resourceVersion":"10"}}}"#,
            r#"{"type":"MODIFIED","object":{"metadata":{"name":"pod-1","namespace":"ns1","resourceVersion":"11"}}}"#,
        ];
        // Chunked framing with one watch event line per chunk, like the
        // Kubernetes API server streams watch responses.
        let mut response =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        for e in events {
            response.extend_from_slice(format!("{:x}\r\n{e}\n\r\n", e.len() + 1).as_bytes());
        }
        response.extend_from_slice(b"0\r\n\r\n");
        let (addr, _ca_pem, handle) = spawn_tls_api_server("watch-pods", response);

        // insecure_skip_verify must be honored (promauth semantics).
        let opts = AuthOptions {
            bearer_token: "test-token".to_string(),
            tls_config: TLSConfig {
                insecure_skip_verify: true,
                ..TLSConfig::default()
            },
            ..AuthOptions::default()
        };
        let cfg = KubeApiConfig {
            server: format!("https://{addr}"),
            ac: opts.new_config().unwrap(),
        };
        let client = new_kube_api_client(cfg).unwrap();

        let mut stream = client.watch_node_pods("node-1", "").unwrap();
        let stop = AtomicBool::new(false);
        let mut got: Vec<(String, String)> = Vec::new();
        let err = stream.read_events(&stop, |event| {
            got.push((
                event.event_type.clone(),
                event
                    .object
                    .item("metadata")
                    .item("resourceVersion")
                    .str()
                    .to_string(),
            ));
            Ok(())
        });
        assert!(matches!(err, WatchError::Eof));
        assert_eq!(
            got,
            vec![
                ("ADDED".to_string(), "10".to_string()),
                ("MODIFIED".to_string(), "11".to_string()),
            ]
        );

        let req = String::from_utf8(handle.join().unwrap()).unwrap();
        assert!(
            req.starts_with(
                "GET /api/v1/pods?fieldSelector=spec.nodeName%3Dnode-1&watch=true HTTP/1.1\r\n"
            ),
            "unexpected request: {req:?}"
        );
        assert!(
            req.contains("Authorization: Bearer test-token\r\n"),
            "missing auth header: {req:?}"
        );
    }
}
