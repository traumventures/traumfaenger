//! End-to-end tests of both routers against an in-memory store.

use std::sync::Arc;

use ai_ledger_server::{http, store::Store};
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

const INGEST_TOKEN: &str = "ingest-token-0123456789";
const QUERY_TOKEN: &str = "query-token-0123456789";

fn routers() -> (Router, Router) {
    let store = Arc::new(Store::open(":memory:", None).unwrap());
    (http::ingest_router(store.clone(), Some(INGEST_TOKEN.into())), http::query_router(store, Some(QUERY_TOKEN.into())))
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => request.header(header::CONTENT_TYPE, "application/json").body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn call(id: &str, feature: &str, cost: f64) -> Value {
    json!({
        "id": id,
        "occurred_at": "2026-10-08T10:00:00Z",
        "feature": feature,
        "entity_type": "listing",
        "entity_id": "42",
        "trace_id": "t-1",
        "provider": "openrouter",
        "model": "openai/gpt-5-mini",
        "status": "ok",
        "input_tokens": 1000,
        "output_tokens": 200,
        "cost_usd": cost,
        "latency_ms": 900,
    })
}

#[tokio::test]
async fn ingestion_and_query_are_separate_apis_with_separate_tokens() {
    let (ingest, query) = routers();
    let batch = json!({ "calls": [call("a", "listing.parse", 0.01), call("b", "listing.parse", 0.02)] });

    // The ingestion API only accepts its own token and only writes.
    assert_eq!(send(&ingest, Method::POST, "/v1/calls", None, Some(batch.clone())).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        send(&ingest, Method::POST, "/v1/calls", Some(QUERY_TOKEN), Some(batch.clone())).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, body) = send(&ingest, Method::POST, "/v1/calls", Some(INGEST_TOKEN), Some(batch.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "accepted": 2, "duplicates": 0 }));
    assert_eq!(
        send(&ingest, Method::GET, "/v1/calls", Some(INGEST_TOKEN), None).await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(send(&ingest, Method::GET, "/v1/summary", Some(INGEST_TOKEN), None).await.0, StatusCode::NOT_FOUND);

    // Retrying the same batch is idempotent.
    let (_, body) = send(&ingest, Method::POST, "/v1/calls", Some(INGEST_TOKEN), Some(batch.clone())).await;
    assert_eq!(body, json!({ "accepted": 0, "duplicates": 2 }));

    // The query API only accepts its own token and only reads.
    assert_eq!(send(&query, Method::GET, "/v1/calls", Some(INGEST_TOKEN), None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        send(&query, Method::POST, "/v1/calls", Some(QUERY_TOKEN), Some(batch)).await.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    let (status, body) = send(&query, Method::GET, "/v1/calls?entity_id=42&limit=1", Some(QUERY_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["calls"].as_array().unwrap().len(), 1);
    assert_eq!(body["calls"][0]["id"], "b");
    assert_eq!(body["calls"][0]["attempt"], 1);
    let cursor = body["next_cursor"].as_str().unwrap().to_owned();
    let (_, body) =
        send(&query, Method::GET, &format!("/v1/calls?limit=1&cursor={cursor}"), Some(QUERY_TOKEN), None).await;
    assert_eq!(body["calls"][0]["id"], "a");
    assert_eq!(body["next_cursor"], Value::Null);

    let (status, body) = send(&query, Method::GET, "/v1/calls/a", Some(QUERY_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cost_usd"], 0.01);
    let (status, body) = send(&query, Method::GET, "/v1/calls/missing", Some(QUERY_TOKEN), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");

    let (status, body) = send(
        &query,
        Method::GET,
        "/v1/summary?group_by=feature,model&from=2026-10-08T00:00:00Z",
        Some(QUERY_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["group"], json!({ "feature": "listing.parse", "model": "openai/gpt-5-mini" }));
    assert_eq!(body["rows"][0]["calls"], 2);
    assert_eq!(body["rows"][0]["input_tokens"], 2000);
    assert!((body["rows"][0]["cost_usd"].as_f64().unwrap() - 0.03).abs() < 1e-9);
}

#[tokio::test]
async fn rejects_invalid_requests_with_typed_errors() {
    let (ingest, query) = routers();
    let post = |body: Value| {
        let ingest = ingest.clone();
        async move { send(&ingest, Method::POST, "/v1/calls", Some(INGEST_TOKEN), Some(body)).await }
    };

    let (status, body) = post(json!({ "calls": [] })).await;
    assert_eq!((status, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_request")));

    let mut negative = call("a", "f", -1.0);
    let (status, body) = post(json!({ "calls": [call("ok", "f", 0.0), negative.clone()] })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["message"].as_str().unwrap().starts_with("calls[1]:"));
    // A rejected batch stores nothing.
    let (_, listed) = send(&query, Method::GET, "/v1/calls", Some(QUERY_TOKEN), None).await;
    assert_eq!(listed["calls"], json!([]));

    negative["unknown_field"] = json!(1);
    negative["cost_usd"] = json!(0.0);
    assert_eq!(post(json!({ "calls": [negative] })).await.0, StatusCode::BAD_REQUEST);

    let too_many: Vec<Value> = (0..1001).map(|index| call(&index.to_string(), "f", 0.0)).collect();
    assert_eq!(post(json!({ "calls": too_many })).await.0, StatusCode::BAD_REQUEST);

    let huge = "x".repeat(9 * 1024 * 1024);
    let (status, body) = post(json!({ "calls": [], "pad": huge })).await;
    assert_eq!((status, body["error"].as_str()), (StatusCode::PAYLOAD_TOO_LARGE, Some("payload_too_large")));

    for uri in ["/v1/calls?limit=0", "/v1/calls?status=maybe", "/v1/calls?bogus=1", "/v1/summary?group_by=cost"] {
        let (status, body) = send(&query, Method::GET, uri, Some(QUERY_TOKEN), None).await;
        assert_eq!((status, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_request")), "{uri}");
    }
}

#[tokio::test]
async fn health_is_public_and_names_the_api() {
    let (ingest, query) = routers();
    let (status, body) = send(&ingest, Method::GET, "/health", None, None).await;
    assert_eq!((status, body["api"].as_str()), (StatusCode::OK, Some("ingest")));
    let (status, body) = send(&query, Method::GET, "/health", None, None).await;
    assert_eq!((status, body["api"].as_str()), (StatusCode::OK, Some("query")));
}

#[tokio::test]
async fn auth_can_be_disabled_explicitly() {
    let store = Arc::new(Store::open(":memory:", None).unwrap());
    let query = http::query_router(store, None);
    assert_eq!(send(&query, Method::GET, "/v1/summary", None, None).await.0, StatusCode::OK);
}
