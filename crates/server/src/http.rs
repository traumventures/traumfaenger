//! The two HTTP APIs. They share the store but nothing else: separate routers,
//! separate listeners and separate bearer tokens.

use std::sync::Arc;

use ai_ledger_api::{
    ApiError, CallRecord, ErrorCode, HealthResponse, IngestResponse, ListCallsQuery, RejectedCall, SummaryQuery,
    limits, routes,
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use serde::Deserialize;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::{decompression::RequestDecompressionLayer, trace::TraceLayer};

use crate::store::{Store, StoreError};

type SharedStore = Arc<Store>;

/// Ingestion requests processed at once. Each can hold up to
/// `limits::MAX_BODY_BYTES` decompressed plus its parsed records in memory;
/// further requests wait.
const MAX_CONCURRENT_INGESTS: usize = 4;

/// Write-only API: `POST /v1/calls`. Accepts gzip-compressed bodies; the
/// body limit applies after decompression.
pub fn ingest_router(store: SharedStore, token: Option<String>) -> Router {
    let api = Router::new()
        .route(routes::INGEST_CALLS, post(ingest_calls))
        .layer(DefaultBodyLimit::max(limits::MAX_BODY_BYTES))
        .layer(RequestDecompressionLayer::new())
        .layer(GlobalConcurrencyLimitLayer::new(MAX_CONCURRENT_INGESTS))
        .route_layer(middleware::from_fn_with_state(Arc::new(token), require_bearer));
    finish(api, "ingest", store)
}

/// Read-only API: list, fetch and summarise collected calls.
pub fn query_router(store: SharedStore, token: Option<String>) -> Router {
    let api = Router::new()
        .route(routes::LIST_CALLS, get(list_calls))
        .route(routes::GET_CALL, get(get_call))
        .route(routes::SUMMARY, get(summary))
        .route_layer(middleware::from_fn_with_state(Arc::new(token), require_bearer));
    finish(api, "query", store)
}

fn finish(api: Router<SharedStore>, name: &'static str, store: SharedStore) -> Router {
    api.route(routes::HEALTH, get(move || health(name)))
        .fallback(|| async { error(ErrorCode::NotFound, "no such route") })
        .method_not_allowed_fallback(|| async {
            error(ErrorCode::MethodNotAllowed, "method not allowed on this route")
        })
        .layer(TraceLayer::new_for_http())
        .with_state(store)
}

async fn health(api: &'static str) -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok".into(), api: api.into(), version: env!("CARGO_PKG_VERSION").into() })
}

async fn require_bearer(State(token): State<Arc<Option<String>>>, request: Request, next: Next) -> Response {
    if let Some(expected) = token.as_deref() {
        let presented = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !presented.is_some_and(|presented| constant_time_eq(presented.as_bytes(), expected.as_bytes())) {
            return error(ErrorCode::Unauthorized, "missing or invalid bearer token");
        }
    }
    next.run(request).await
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The ingestion body with records left unparsed, so one malformed record
/// (wrong shape, invalid escape such as a lone surrogate) is rejected on its
/// own instead of failing the whole batch.
#[derive(Deserialize)]
struct RawIngestRequest {
    calls: Vec<Box<serde_json::value::RawValue>>,
}

async fn ingest_calls(
    State(store): State<SharedStore>,
    body: Result<Json<RawIngestRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match body {
        Ok(body) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return error(ErrorCode::PayloadTooLarge, format!("body exceeds {} bytes", limits::MAX_BODY_BYTES));
        }
        Err(rejection) => return error(ErrorCode::InvalidRequest, rejection.body_text()),
    };
    if request.calls.is_empty() || request.calls.len() > limits::MAX_BATCH_SIZE {
        return error(
            ErrorCode::InvalidRequest,
            format!("`calls` must contain 1 to {} records", limits::MAX_BATCH_SIZE),
        );
    }

    let mut valid = Vec::with_capacity(request.calls.len());
    let mut rejected = Vec::new();
    for (index, value) in request.calls.into_iter().enumerate() {
        let record = serde_json::from_str::<CallRecord>(value.get())
            .map_err(|parse_error| parse_error.to_string())
            .and_then(|record| record.validate().map(|()| record));
        match record {
            Ok(record) => valid.push(record),
            Err(reason) => rejected.push(RejectedCall { index: index as u32, reason }),
        }
    }
    if !rejected.is_empty() {
        tracing::warn!(rejected = rejected.len(), first = %rejected[0].reason, "rejected call records");
    }

    let received_at = Utc::now();
    run(store, move |store| -> Result<_, HandlerError> {
        let inserted = if valid.is_empty() { Default::default() } else { store.insert(&valid, received_at)? };
        Ok(IngestResponse { accepted: inserted.accepted as u32, duplicates: inserted.duplicates as u32, rejected })
    })
    .await
}

async fn list_calls(
    State(store): State<SharedStore>,
    query: Result<Query<ListCallsQuery>, QueryRejection>,
) -> Response {
    match query {
        Ok(Query(query)) => run(store, move |store| store.list(&query)).await,
        Err(rejection) => error(ErrorCode::InvalidRequest, rejection.body_text()),
    }
}

async fn get_call(State(store): State<SharedStore>, id: Result<Path<String>, PathRejection>) -> Response {
    let Ok(Path(id)) = id else {
        return error(ErrorCode::InvalidRequest, "invalid call id in path");
    };
    run(store, move |store| -> Result<_, HandlerError> {
        let call = store.get(&id)?;
        call.ok_or(HandlerError::NotFound(id))
    })
    .await
}

async fn summary(State(store): State<SharedStore>, query: Result<Query<SummaryQuery>, QueryRejection>) -> Response {
    match query {
        Ok(Query(query)) => run(store, move |store| store.summary(&query)).await,
        Err(rejection) => error(ErrorCode::InvalidRequest, rejection.body_text()),
    }
}

/// Why a handler could not produce its value.
enum HandlerError {
    Store(StoreError),
    NotFound(String),
}

impl From<StoreError> for HandlerError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Runs a store operation on a blocking thread and maps the result to HTTP.
async fn run<T, E>(store: SharedStore, operation: impl FnOnce(&Store) -> Result<T, E> + Send + 'static) -> Response
where
    T: serde::Serialize + Send + 'static,
    E: Into<HandlerError> + Send + 'static,
{
    match tokio::task::spawn_blocking(move || operation(&store)).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(failure)) => match failure.into() {
            HandlerError::NotFound(id) => error(ErrorCode::NotFound, format!("no call with id `{id}`")),
            HandlerError::Store(StoreError::Invalid(message)) => error(ErrorCode::InvalidRequest, message),
            HandlerError::Store(failure) => {
                tracing::error!(error = %failure, "store operation failed");
                error(ErrorCode::Internal, "internal error")
            }
        },
        Err(join_error) => {
            tracing::error!(error = %join_error, "store task panicked");
            error(ErrorCode::Internal, "internal error")
        }
    }
}

fn error(code: ErrorCode, message: impl Into<String>) -> Response {
    let status = match code {
        ErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
        ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = (status, Json(ApiError { error: code, message: message.into() })).into_response();
    if code == ErrorCode::Unauthorized {
        response.headers_mut().insert(header::WWW_AUTHENTICATE, header::HeaderValue::from_static("Bearer"));
    }
    response
}
