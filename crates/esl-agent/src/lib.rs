//! Port of EsLogs `app/eslagent` — the log shipper (file tailing,
//! kubernetes discovery, remote write over the native /internal/insert
//! protocol). Modules are being filled in by porting agents.
pub mod filecollector;
pub mod kubernetescollector;
mod oauth2;
pub mod persistentqueue;
pub mod remotewrite;
pub mod tail;
