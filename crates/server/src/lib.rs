//! The traumfaenger server: a DuckDB raw log of AI provider exchanges, with a
//! WebSocket ingestion API (JSON or MessagePack) and a separate read-only
//! query API. The wire contract lives in `traumfaenger-api`.

pub mod analytics;
pub mod config;
pub mod details;
pub mod http;
pub mod ingest;
pub mod projection;
pub mod store;
