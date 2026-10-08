//! The ai-ledger server: a DuckDB-backed ledger of AI calls with separate
//! ingestion and query HTTP APIs. The wire contract lives in `ai-ledger-api`.

pub mod config;
pub mod http;
pub mod store;
