//! Rust client of traumfaenger.
//!
//! - [`Database`] records AI provider exchanges and streams them to the
//!   database's ingestion WebSocket in the background (MessagePack or JSON).
//!   Transport adapters open an [`HttpRecording`] or a [`SocketRecording`]
//!   per exchange; `traumfaenger-rig` is one such adapter.
//! - [`operation`] tags everything recorded inside a future with an
//!   operation, session and metadata.
//! - [`QueryClient`] reads the query API.
//!
//! ```no_run
//! use traumfaenger_client::{Database, DatabaseOptions, Operation, operation};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let database = Database::new(DatabaseOptions::new("http://traumfaenger:7600", "my-service").token("…"))?;
//! operation(Operation::new("listing.parse").meta("listing_id", 123), async {
//!     if let Some(mut recording) = database.record_http("https://api.example.com/v1/chat/completions") {
//!         recording.request_body(br#"{"model":"m"}"#);
//!         recording.status(200);
//!         recording.response_body(br#"{"choices":[]}"#, Some("application/json"));
//!         recording.finish(traumfaenger_client::EndReason::Complete, None);
//!     }
//! })
//! .await;
//! database.close(std::time::Duration::from_secs(5)).await;
//! # Ok(())
//! # }
//! ```

mod context;
mod database;
mod query;
mod recording;
mod sender;
pub mod text;

pub use context::{Operation, current, operation, scope};
pub use database::{Database, DatabaseOptions, InvalidOptions, UNATTRIBUTED};
pub use query::{QueryClient, QueryError};
pub use recording::{HttpRecording, SegmentOptions, SocketRecording};
pub use sender::{Encoding, ErrorHandler, Stats};
pub use traumfaenger_api::{self as api, Direction, EndReason};
