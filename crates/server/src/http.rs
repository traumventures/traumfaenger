//! The two APIs. They share the store but nothing else: separate routers,
//! separate listeners and separate bearer tokens.
//!
//! Ingestion: a WebSocket (`GET /v1/ingest`, subprotocol JSON or MessagePack)
//! for streaming batches with in-order acks, and `POST /v1/exchanges` for one
//! JSON batch per request.

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{BytesRejection, PathRejection, QueryRejection},
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tokio::sync::Semaphore;
use tower_http::{decompression::RequestDecompressionLayer, trace::TraceLayer};
use traumfaenger_api::{
    ApiError, ErrorCode, HealthResponse, ListQuery, ServerMessage, SummaryQuery, limits, protocols, routes,
};

use crate::{
    details,
    ingest::{self, BatchError, Encoding},
    store::{Store, StoreError},
};

type SharedStore = Arc<Store>;

#[derive(Clone)]
struct IngestState {
    store: SharedStore,
    /// Batches decoded and stored at once, across all connections. Each can
    /// hold up to `limits::MAX_BATCH_BYTES` plus its decoded records.
    permits: Arc<Semaphore>,
}

/// Write-only API.
pub fn ingest_router(store: SharedStore, token: Option<String>) -> Router {
    let state = IngestState { store, permits: Arc::new(Semaphore::new(limits::MAX_IN_FLIGHT_BATCHES)) };
    let api = Router::new()
        .route(routes::INGEST_WS, get(ingest_socket))
        .route(routes::INGEST_HTTP, post(ingest_http))
        .layer(DefaultBodyLimit::max(limits::MAX_BATCH_BYTES))
        .layer(RequestDecompressionLayer::new())
        .route_layer(middleware::from_fn_with_state(Arc::new(token), require_bearer))
        .with_state(state);
    finish(api, "ingest")
}

/// Read-only API: the raw log, analytics over processed calls, call and
/// session details, stored texts and processing status.
pub fn query_router(store: SharedStore, token: Option<String>) -> Router {
    let api = Router::new()
        .route(routes::LIST_EXCHANGES, get(list))
        .route(routes::GET_EXCHANGE, get(get_exchange))
        .route(routes::SUMMARY, get(summary))
        .route(routes::ANALYTICS, get(analytics))
        .route(routes::CALL, get(call))
        .route(routes::SESSION, get(session))
        .route(routes::CONTENT, get(content))
        .route(routes::PROCESSING, get(processing))
        .route_layer(middleware::from_fn_with_state(Arc::new(token), require_bearer))
        .with_state(store);
    finish(api, "query")
}

fn finish(api: Router, name: &'static str) -> Router {
    api.route(routes::HEALTH, get(move || health(name)))
        .fallback(|| async { error(ErrorCode::NotFound, "no such route") })
        .method_not_allowed_fallback(|| async {
            error(ErrorCode::MethodNotAllowed, "method not allowed on this route")
        })
        .layer(TraceLayer::new_for_http())
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

async fn ingest_socket(State(state): State<IngestState>, upgrade: WebSocketUpgrade) -> Response {
    let offered: Vec<String> = upgrade
        .requested_protocols()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(',').map(|protocol| protocol.trim().to_owned()).collect::<Vec<_>>())
        .collect();
    let Some(encoding) = Encoding::negotiate(offered.iter().map(String::as_str)) else {
        return error(
            ErrorCode::InvalidRequest,
            format!("offer the subprotocol `{}` or `{}`", protocols::MSGPACK, protocols::JSON),
        );
    };
    upgrade
        .protocols([encoding.protocol()])
        .max_message_size(limits::MAX_BATCH_BYTES)
        .max_frame_size(limits::MAX_BATCH_BYTES)
        .on_upgrade(move |socket| serve_socket(socket, state, encoding))
}

/// Handles one batch after another, so acks come back in the order the
/// batches were sent; the socket is not read while a batch is processed.
async fn serve_socket(mut socket: WebSocket, state: IngestState, encoding: Encoding) {
    while let Some(Ok(message)) = socket.recv().await {
        let bytes = match (encoding, message) {
            (Encoding::Json, Message::Text(text)) => Bytes::from(text),
            (Encoding::Msgpack, Message::Binary(bytes)) => bytes,
            (_, Message::Close(_)) => break,
            (_, Message::Ping(_) | Message::Pong(_)) => continue,
            (_, _) => {
                let kind = if encoding == Encoding::Json { "text" } else { "binary" };
                let reply = BatchError {
                    batch: None,
                    code: ErrorCode::InvalidRequest,
                    message: format!("this protocol uses {kind} messages"),
                };
                if send(&mut socket, encoding, &reply.into_message()).await.is_err() {
                    break;
                }
                continue;
            }
        };
        let reply = run_batch(&state, move |store| {
            let batch = ingest::decode(encoding, &bytes)?;
            let id = batch.id;
            ingest::store(store, Some(id), batch.records).map(|result| ServerMessage::Ack { batch: id, result })
        })
        .await
        .unwrap_or_else(BatchError::into_message);
        if send(&mut socket, encoding, &reply).await.is_err() {
            break;
        }
    }
}

async fn send(socket: &mut WebSocket, encoding: Encoding, message: &ServerMessage) -> Result<(), axum::Error> {
    let bytes = encoding.encode(message);
    let message = match encoding {
        Encoding::Json => Message::Text(String::from_utf8(bytes).unwrap_or_default().into()),
        Encoding::Msgpack => Message::Binary(bytes.into()),
    };
    socket.send(message).await
}

/// Runs batch work on a blocking thread, at most `MAX_IN_FLIGHT_BATCHES` at once.
async fn run_batch<T: Send + 'static>(
    state: &IngestState,
    work: impl FnOnce(&Store) -> Result<T, BatchError> + Send + 'static,
) -> Result<T, BatchError> {
    let internal = || BatchError { batch: None, code: ErrorCode::Internal, message: "internal error".into() };
    let Ok(_permit) = state.permits.clone().acquire_owned().await else { return Err(internal()) };
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || work(&store)).await.unwrap_or_else(|join_error| {
        tracing::error!(error = %join_error, "ingest task panicked");
        Err(internal())
    })
}

async fn ingest_http(State(state): State<IngestState>, body: Result<Bytes, BytesRejection>) -> Response {
    let body = match body {
        Ok(body) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return error(ErrorCode::PayloadTooLarge, format!("body exceeds {} bytes", limits::MAX_BATCH_BYTES));
        }
        Err(rejection) => return error(ErrorCode::InvalidRequest, rejection.body_text()),
    };
    match run_batch(&state, move |store| ingest::store(store, None, ingest::decode_http(&body)?)).await {
        Ok(result) => Json(result).into_response(),
        Err(failure) => error(failure.code, failure.message),
    }
}

async fn list(State(store): State<SharedStore>, query: Result<Query<ListQuery>, QueryRejection>) -> Response {
    match query {
        Ok(Query(query)) => run(store, move |store| store.list(&query)).await,
        Err(rejection) => error(ErrorCode::InvalidRequest, rejection.body_text()),
    }
}

async fn get_exchange(State(store): State<SharedStore>, id: Result<Path<String>, PathRejection>) -> Response {
    let Ok(Path(id)) = id else {
        return error(ErrorCode::InvalidRequest, "invalid exchange id in path");
    };
    run(store, move |store| -> Result<_, HandlerError> {
        let exchange = store.get(&id)?;
        exchange.ok_or_else(|| HandlerError::NotFound(format!("no exchange with id `{id}`")))
    })
    .await
}

async fn analytics(
    State(store): State<SharedStore>,
    source: Result<Path<String>, PathRejection>,
    query: Result<Query<Vec<(String, String)>>, QueryRejection>,
) -> Response {
    let (Ok(Path(source)), Ok(Query(params))) = (source, query) else {
        return error(ErrorCode::InvalidRequest, "invalid source or query string");
    };
    run(store, move |store| store.analytics(&source, &params)).await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DetailQuery {
    max_chars: Option<usize>,
    new_only: Option<bool>,
}

impl DetailQuery {
    fn max_chars(&self) -> Result<usize, HandlerError> {
        match self.max_chars {
            None => Ok(details::DEFAULT_MAX_CHARS),
            Some(chars) if (1..=details::MAX_MAX_CHARS).contains(&chars) => Ok(chars),
            Some(_) => Err(HandlerError::Store(StoreError::Invalid(format!(
                "`max_chars` must be between 1 and {}",
                details::MAX_MAX_CHARS
            )))),
        }
    }
}

async fn call(
    State(store): State<SharedStore>,
    id: Result<Path<String>, PathRejection>,
    query: Result<Query<DetailQuery>, QueryRejection>,
) -> Response {
    let (Ok(Path(id)), Ok(Query(query))) = (id, query) else {
        return error(ErrorCode::InvalidRequest, "invalid call id or query string");
    };
    run(store, move |store| -> Result<_, HandlerError> {
        let call = store.call(&id, query.max_chars()?)?;
        call.ok_or_else(|| HandlerError::NotFound(format!("no processed call with id `{id}`")))
    })
    .await
}

async fn session(
    State(store): State<SharedStore>,
    id: Result<Path<String>, PathRejection>,
    query: Result<Query<DetailQuery>, QueryRejection>,
) -> Response {
    let (Ok(Path(id)), Ok(Query(query))) = (id, query) else {
        return error(ErrorCode::InvalidRequest, "invalid session id or query string");
    };
    run(store, move |store| -> Result<_, HandlerError> {
        let session = store.session(&id, query.max_chars()?, query.new_only.unwrap_or(true))?;
        session.ok_or_else(|| HandlerError::NotFound(format!("no calls in session `{id}`")))
    })
    .await
}

async fn content(State(store): State<SharedStore>, hash: Result<Path<String>, PathRejection>) -> Response {
    let Ok(Path(hash)) = hash else {
        return error(ErrorCode::InvalidRequest, "invalid content hash in path");
    };
    run(store, move |store| -> Result<_, HandlerError> {
        let content = store.content(&hash)?;
        content.ok_or_else(|| HandlerError::NotFound(format!("no content with hash `{hash}`")))
    })
    .await
}

async fn processing(State(store): State<SharedStore>) -> Response {
    run(store, |store| store.processing_status()).await
}

async fn summary(State(store): State<SharedStore>, query: Result<Query<SummaryQuery>, QueryRejection>) -> Response {
    match query {
        Ok(Query(query)) => run(store, move |store| store.summary(&query)).await,
        Err(rejection) => error(ErrorCode::InvalidRequest, rejection.body_text()),
    }
}

/// Why a query handler could not produce its value.
enum HandlerError {
    Store(StoreError),
    NotFound(String),
}

impl From<StoreError> for HandlerError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// Runs a store query on a blocking thread and maps the result to HTTP.
async fn run<T, E>(store: SharedStore, operation: impl FnOnce(&Store) -> Result<T, E> + Send + 'static) -> Response
where
    T: serde::Serialize + Send + 'static,
    E: Into<HandlerError> + Send + 'static,
{
    match tokio::task::spawn_blocking(move || operation(&store)).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(failure)) => match failure.into() {
            HandlerError::NotFound(message) => error(ErrorCode::NotFound, message),
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
