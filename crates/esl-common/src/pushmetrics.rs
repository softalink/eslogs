//! Port of Softalink LLC `lib/pushmetrics`: the `-pushmetrics.*` command-line
//! flags plus the periodic push of the `/metrics` payload
//! ([`crate::appmetrics::write_prometheus_metrics`]) to every
//! `-pushmetrics.url`.
//!
//! PORT NOTE: Go's `InitWith` (vmctl-only) and `StopAndPush`
//! (vmbackup/vmrestore-only) entry points are not ported — the EsLogs
//! binaries only use `Init`/`Stop`.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::flagutil::{ArrayString, ExtendedDuration, Flag};
use crate::metrics::MetricsWriter;
use crate::metrics::push::{PushCancel, PushOptions, init_push_ext_with_options};
use crate::{fatalf, flagutil};

static PUSH_URL: Flag<ArrayString> = Flag::new(
    "pushmetrics.url",
    "Optional URL to push metrics exposed at /metrics page. \
     See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#push-metrics . \
     By default, metrics exposed at /metrics page aren't pushed to any remote storage",
    ArrayString::default,
);
crate::register_flag!(PUSH_URL);

static PUSH_INTERVAL: Flag<ExtendedDuration> = Flag::new(
    "pushmetrics.interval",
    "Interval for pushing metrics to every -pushmetrics.url",
    || {
        let mut d = ExtendedDuration::default();
        d.set("10s").expect("BUG: cannot parse default interval");
        d
    },
);
crate::register_flag!(PUSH_INTERVAL);

static PUSH_EXTRA_LABEL: Flag<ArrayString> = Flag::new(
    "pushmetrics.extraLabel",
    "Optional labels to add to metrics pushed to every -pushmetrics.url . \
     For example, -pushmetrics.extraLabel='instance=\"foo\"' adds instance=\"foo\" label \
     to all the metrics pushed to every -pushmetrics.url",
    ArrayString::default,
);
crate::register_flag!(PUSH_EXTRA_LABEL);

static PUSH_HEADER: Flag<ArrayString> = Flag::new(
    "pushmetrics.header",
    "Optional HTTP request header to send to every -pushmetrics.url . \
     For example, -pushmetrics.header='Authorization: Basic foobar' adds \
     'Authorization: Basic foobar' header to every request to every -pushmetrics.url",
    ArrayString::default,
);
crate::register_flag!(PUSH_HEADER);

static DISABLE_COMPRESSION: Flag<bool> = Flag::new(
    "pushmetrics.disableCompression",
    "Whether to disable request body compression when pushing metrics to every -pushmetrics.url",
    || false,
);
crate::register_flag!(DISABLE_COMPRESSION);

/// Registers `-pushmetrics.url` as a secret flag: it can contain basic auth
/// creds, so it mustn't be visible when exposing the flags.
///
/// PORT NOTE: Go does this in the package `init()`; Rust has no
/// life-before-main, so the binaries call this before `logger::init` (the
/// same pattern as vlagent's `remotewrite.InitSecretFlags`).
pub fn init_secret_flags() {
    flagutil::register_secret_flag("pushmetrics.url");
}

struct State {
    cancel: Arc<PushCancel>,
    handles: Vec<JoinHandle<()>>,
}

static STATE: Mutex<Option<State>> = Mutex::new(None);

/// Starts the periodic push of `/metrics` to every `-pushmetrics.url`
/// (Go `pushmetrics.Init`). Must be called after `logger::init`.
pub fn init() {
    let extra_labels = PUSH_EXTRA_LABEL.get().join(",");
    let interval = PUSH_INTERVAL.get().duration();
    let cancel = Arc::new(PushCancel::new());
    let mut handles = Vec::new();
    for pu in PUSH_URL.get().iter() {
        let opts = PushOptions {
            extra_labels: extra_labels.clone(),
            headers: PUSH_HEADER.get().to_vec(),
            disable_compression: *DISABLE_COMPRESSION.get(),
            method: String::new(),
        };
        let writer: MetricsWriter = Arc::new(crate::appmetrics::write_prometheus_metrics);
        match init_push_ext_with_options(Arc::clone(&cancel), pu, interval, writer, &opts) {
            Ok(h) => handles.push(h),
            Err(err) => {
                fatalf!("cannot initialize pushmetrics: {err}");
            }
        }
    }
    *STATE.lock().unwrap_or_else(|e| e.into_inner()) = Some(State { cancel, handles });
}

/// Stops the periodic push of metrics started by [`init`] and waits until the
/// push workers exit (Go `pushmetrics.Stop`).
///
/// It is important to stop the push of metrics before disposing resources
/// these metrics are attached to.
pub fn stop() {
    let st = STATE.lock().unwrap_or_else(|e| e.into_inner()).take();
    let Some(st) = st else { return };
    st.cancel.cancel();
    for h in st.handles {
        let _ = h.join();
    }
}
