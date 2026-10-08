//! End-to-end tests of both routers against an in-memory store.

use std::{io::Write, sync::Arc};

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

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn request(method: Method, uri: &str, token: Option<&str>, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    match body {
        Some(body) => builder.header(header::CONTENT_TYPE, "application/json").body(Body::from(body.to_string())),
        None => builder.body(Body::empty()),
    }
    .unwrap()
}

fn record(id: &str, operation: &str) -> Value {
    json!({
        "id": id,
        "service": "traumwohnung-web",
        "environment": "production",
        "operation": operation,
        "session_id": "chat-1",
        "metadata": { "listing_id": "42" },
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "request": "{\"model\":\"openai/gpt-5-mini\",\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"Hi\"}]}",
        "response": ["data: {\"choices\":[{\"delta\":{\"content\":\"Hallo\"}}]}", "data: [DONE]"],
        "http_status": 200,
        "started_at": "2026-10-08T10:00:00Z",
        "ttft_ms": 300,
        "latency_ms": 900,
    })
}

#[tokio::test]
async fn ingestion_and_query_are_separate_apis_with_separate_tokens() {
    let (ingest, query) = routers();
    let batch = json!({ "calls": [record("a", "support.chat"), record("b", "support.chat")] });
    let post = |token: Option<&'static str>, body: Value| request(Method::POST, "/v1/calls", token, Some(body));

    assert_eq!(send(&ingest, post(None, batch.clone())).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(send(&ingest, post(Some(QUERY_TOKEN), batch.clone())).await.0, StatusCode::UNAUTHORIZED);
    let (status, body) = send(&ingest, post(Some(INGEST_TOKEN), batch.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "accepted": 2, "duplicates": 0, "rejected": [] }));
    // Retrying is idempotent.
    let (_, body) = send(&ingest, post(Some(INGEST_TOKEN), batch.clone())).await;
    assert_eq!(body, json!({ "accepted": 0, "duplicates": 2, "rejected": [] }));
    let (status, body) = send(&ingest, request(Method::GET, "/v1/calls", Some(INGEST_TOKEN), None)).await;
    assert_eq!((status, body["error"].as_str()), (StatusCode::METHOD_NOT_ALLOWED, Some("method_not_allowed")));

    assert_eq!(
        send(&query, request(Method::GET, "/v1/calls", Some(INGEST_TOKEN), None)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(send(&query, post(Some(QUERY_TOKEN), batch)).await.0, StatusCode::METHOD_NOT_ALLOWED);

    let (status, body) =
        send(&query, request(Method::GET, "/v1/calls?session_id=chat-1&limit=1", Some(QUERY_TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    let first = &body["calls"][0];
    assert_eq!(first["id"], "b");
    assert_eq!(first["metadata"], json!({ "listing_id": "42" }));
    assert!(first.get("request").is_none(), "list returns envelopes without bodies");
    let cursor = body["next_cursor"].as_str().unwrap().to_owned();
    let (_, body) =
        send(&query, request(Method::GET, &format!("/v1/calls?limit=1&cursor={cursor}"), Some(QUERY_TOKEN), None))
            .await;
    assert_eq!(body["calls"][0]["id"], "a");
    assert_eq!(body["next_cursor"], Value::Null);

    let (status, body) = send(&query, request(Method::GET, "/v1/calls/a", Some(QUERY_TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["request"], record("a", "x")["request"]);
    assert_eq!(body["response"], record("a", "x")["response"]);
    assert_eq!(body["seq"], 1);
    let (status, body) = send(&query, request(Method::GET, "/v1/calls/missing", Some(QUERY_TOKEN), None)).await;
    assert_eq!((status, body["error"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));

    let (status, body) =
        send(&query, request(Method::GET, "/v1/summary?group_by=service,operation", Some(QUERY_TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["group"], json!({ "service": "traumwohnung-web", "operation": "support.chat" }));
    assert_eq!(body["rows"][0]["calls"], 2);
    assert_eq!(body["rows"][0]["ttft_ms_p95"], 300.0);
}

#[tokio::test]
async fn invalid_records_are_rejected_individually() {
    let (ingest, query) = routers();
    let mut unknown_field = record("c", "o");
    unknown_field["added_later"] = json!({ "input": 1 });
    let mut bad_status = record("d", "o");
    bad_status["http_status"] = json!(42);
    let mut missing = record("e", "o");
    missing.as_object_mut().unwrap().remove("operation");
    let batch = json!({ "calls": [record("a", "o"), unknown_field, record("b", "o"), bad_status, "nope", missing] });

    let (status, body) = send(&ingest, request(Method::POST, "/v1/calls", Some(INGEST_TOKEN), Some(batch))).await;
    assert_eq!(status, StatusCode::OK);
    // Unknown fields are ignored for forward compatibility.
    assert_eq!(body["accepted"], 3);
    let rejected: Vec<u64> =
        body["rejected"].as_array().unwrap().iter().map(|r| r["index"].as_u64().unwrap()).collect();
    assert_eq!(rejected, [3, 4, 5]);
    assert!(body["rejected"][2]["reason"].as_str().unwrap().contains("operation"));

    // A lone surrogate escape breaks only its own record, not the batch.
    let broken = record("g", "o").to_string().replace(r#"\"Hi\""#, r#"\"broken \ud800\""#);
    assert!(broken.contains(r#"broken \ud800"#));
    let raw_request = Request::builder()
        .method(Method::POST)
        .uri("/v1/calls")
        .header(header::AUTHORIZATION, format!("Bearer {INGEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(r#"{{"calls":[{},{broken}]}}"#, record("f", "o"))))
        .unwrap();
    let (status, body) = send(&ingest, raw_request).await;
    assert_eq!((status, body["accepted"].as_u64()), (StatusCode::OK, Some(1)));
    assert_eq!(body["rejected"][0]["index"], 1);

    let (_, listed) = send(&query, request(Method::GET, "/v1/calls", Some(QUERY_TOKEN), None)).await;
    assert_eq!(listed["calls"].as_array().unwrap().len(), 4);
}

#[tokio::test]
async fn rejects_malformed_batches_with_typed_errors() {
    let (ingest, query) = routers();
    let post = |body: Value| request(Method::POST, "/v1/calls", Some(INGEST_TOKEN), Some(body));

    for body in [json!({ "calls": [] }), json!({ "records": [] }), json!([1]), json!({ "calls": {} })] {
        let (status, response) = send(&ingest, post(body)).await;
        assert_eq!((status, response["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_request")));
    }
    let too_many: Vec<Value> = (0..501).map(|index| record(&index.to_string(), "o")).collect();
    assert_eq!(send(&ingest, post(json!({ "calls": too_many }))).await.0, StatusCode::BAD_REQUEST);

    let (status, response) = send(&ingest, post(json!({ "calls": [], "pad": "x".repeat(33 * 1024 * 1024) }))).await;
    assert_eq!((status, response["error"].as_str()), (StatusCode::PAYLOAD_TOO_LARGE, Some("payload_too_large")));

    for uri in ["/v1/calls?limit=0", "/v1/calls?failed=maybe", "/v1/calls?bogus=1", "/v1/summary?group_by=model"] {
        let (status, body) = send(&query, request(Method::GET, uri, Some(QUERY_TOKEN), None)).await;
        assert_eq!((status, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_request")), "{uri}");
    }
}

#[tokio::test]
async fn accepts_gzip_bodies_and_limits_them_after_decompression() {
    let (ingest, query) = routers();
    let gzip = |body: &Value| {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body.to_string().as_bytes()).unwrap();
        encoder.finish().unwrap()
    };
    let post_gzip = |bytes: Vec<u8>| {
        Request::builder()
            .method(Method::POST)
            .uri("/v1/calls")
            .header(header::AUTHORIZATION, format!("Bearer {INGEST_TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(bytes))
            .unwrap()
    };

    let (status, body) = send(&ingest, post_gzip(gzip(&json!({ "calls": [record("z", "o")] })))).await;
    assert_eq!((status, body["accepted"].as_u64()), (StatusCode::OK, Some(1)));
    assert_eq!(send(&query, request(Method::GET, "/v1/calls/z", Some(QUERY_TOKEN), None)).await.0, StatusCode::OK);

    // A small compressed body that inflates past the limit is refused.
    let bomb = gzip(&json!({ "calls": [], "pad": "0".repeat(40 * 1024 * 1024) }));
    assert!(bomb.len() < 1024 * 1024);
    assert_eq!(send(&ingest, post_gzip(bomb)).await.0, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn health_is_public_and_names_the_api() {
    let (ingest, query) = routers();
    let (status, body) = send(&ingest, request(Method::GET, "/health", None, None)).await;
    assert_eq!((status, body["api"].as_str()), (StatusCode::OK, Some("ingest")));
    let (status, body) = send(&query, request(Method::GET, "/health", None, None)).await;
    assert_eq!((status, body["api"].as_str()), (StatusCode::OK, Some("query")));
}
