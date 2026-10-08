//! End-to-end tests of both APIs on real listeners against an in-memory store.

use std::{io::Write, net::SocketAddr, sync::Arc};

use ai_ledger_api::protocols;
use ai_ledger_server::{http, store::Store};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{self, Message, client::IntoClientRequest, http::HeaderValue};

const INGEST_TOKEN: &str = "ingest-token-0123456789";
const QUERY_TOKEN: &str = "query-token-0123456789";

struct Servers {
    ingest: SocketAddr,
    query: SocketAddr,
    http: reqwest_lite::Client,
}

/// A tiny HTTP client over hyper-less std: enough for JSON requests in tests.
mod reqwest_lite {
    use std::net::SocketAddr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy)]
    pub struct Client;

    impl Client {
        pub async fn send(
            self,
            addr: SocketAddr,
            method: &str,
            path: &str,
            token: Option<&str>,
            headers: &[(&str, &str)],
            body: &[u8],
        ) -> (u16, serde_json::Value) {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut head = format!("{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n");
            if let Some(token) = token {
                head.push_str(&format!("Authorization: Bearer {token}\r\n"));
            }
            for (name, value) in headers {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let text = String::from_utf8_lossy(&response);
            let status: u16 = text[9..12].parse().unwrap();
            let (headers, body) = text.split_once("\r\n\r\n").unwrap();
            let body = if headers.to_ascii_lowercase().contains("transfer-encoding: chunked") {
                dechunk(body)
            } else {
                body.to_owned()
            };
            (status, serde_json::from_str(&body).unwrap_or(serde_json::Value::Null))
        }
    }

    fn dechunk(mut body: &str) -> String {
        let mut out = String::new();
        while let Some((size, rest)) = body.split_once("\r\n") {
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            out.push_str(&rest[..size]);
            body = &rest[size + 2..];
        }
        out
    }
}

async fn start() -> Servers {
    let store = Arc::new(Store::open(":memory:", None).unwrap());
    let ingest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let query_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let servers = Servers {
        ingest: ingest_listener.local_addr().unwrap(),
        query: query_listener.local_addr().unwrap(),
        http: reqwest_lite::Client,
    };
    let ingest = http::ingest_router(store.clone(), Some(INGEST_TOKEN.into()));
    let query = http::query_router(store, Some(QUERY_TOKEN.into()));
    tokio::spawn(async move { axum::serve(ingest_listener, ingest).await });
    tokio::spawn(async move { axum::serve(query_listener, query).await });
    servers
}

impl Servers {
    async fn get(&self, path: &str) -> (u16, Value) {
        self.http.send(self.query, "GET", path, Some(QUERY_TOKEN), &[], b"").await
    }
}

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(addr: SocketAddr, token: Option<&str>, protocols: &str) -> Result<Socket, tungstenite::Error> {
    let mut request = format!("ws://{addr}/v1/ingest").into_client_request().unwrap();
    if let Some(token) = token {
        request.headers_mut().insert("authorization", HeaderValue::from_str(&format!("Bearer {token}")).unwrap());
    }
    request.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_str(protocols).unwrap());
    let (socket, response) = tokio_tungstenite::connect_async(request).await?;
    assert!(response.headers().get("sec-websocket-protocol").is_some());
    Ok(socket)
}

fn exchange(id: &str, operation: &str) -> Value {
    json!({
        "id": id,
        "service": "traumwohnung-web",
        "environment": "production",
        "operation": operation,
        "session_id": "chat-1",
        "metadata": { "listing_id": "42" },
        "transport": "sse",
        "url": "https://openrouter.ai/api/v1/chat/completions?key=secret",
        "started_at": "2026-10-08T10:00:00Z",
        "status": 200,
        "frames": [
            { "t_ms": 0, "dir": "out", "data": "{\"model\":\"m\",\"stream\":true}" },
            { "t_ms": 300, "dir": "in", "data": "data: {\"choices\":[{\"delta\":{\"content\":\"Hallo\"}}]}" },
            { "t_ms": 310, "dir": "in", "data": "data: [DONE]" },
        ],
        "end": { "at_ms": 320, "reason": "complete" },
    })
}

fn ws_segment(id: &str, segment: u32, is_final: bool) -> Value {
    let mut value = exchange(id, "voice.interview");
    value["transport"] = json!("ws");
    value["url"] = json!("wss://api.deepslate.example/v1/realtime");
    value["status"] = json!(101);
    value["segment"] = json!(segment);
    value["final"] = json!(is_final);
    let base = segment * 10_000;
    value["frames"] = json!([
        { "t_ms": base, "dir": "out", "data": format!("{{\"type\":\"input_text\",\"n\":{segment}}}") },
        { "t_ms": base + 400, "dir": "in", "data": format!("{{\"type\":\"transcript.delta\",\"n\":{segment}}}") },
    ]);
    if is_final {
        value["end"] = json!({ "at_ms": base + 500, "reason": "closed", "close_code": 1000 });
    } else {
        value.as_object_mut().unwrap().remove("end");
    }
    value
}

fn batch(id: u64, exchanges: Vec<Value>) -> Value {
    json!({ "type": "batch", "id": id, "exchanges": exchanges })
}

async fn send_msgpack(socket: &mut Socket, value: &Value) {
    socket.send(Message::Binary(rmp_serde::to_vec_named(value).unwrap().into())).await.unwrap();
}

async fn receive(socket: &mut Socket) -> Value {
    match socket.next().await.unwrap().unwrap() {
        Message::Binary(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("unexpected message {other:?}"),
    }
}

#[tokio::test]
async fn msgpack_socket_acks_pipelined_batches_in_order() {
    let servers = start().await;
    let mut socket =
        connect(servers.ingest, Some(INGEST_TOKEN), &format!("{}, {}", protocols::JSON, protocols::MSGPACK))
            .await
            .unwrap();

    let mut unknown_field = exchange("c", "support.chat");
    unknown_field["added_later"] = json!(1);
    let mut bad = exchange("d", "support.chat");
    bad["status"] = json!(42);
    send_msgpack(&mut socket, &batch(1, vec![exchange("a", "support.chat"), unknown_field, bad, json!("nope")])).await;
    send_msgpack(&mut socket, &batch(2, vec![ws_segment("ws-1", 0, false), exchange("a", "support.chat")])).await;
    send_msgpack(&mut socket, &batch(3, vec![ws_segment("ws-1", 1, true)])).await;

    let first = receive(&mut socket).await;
    assert_eq!(
        (first["type"].as_str(), first["batch"].as_u64(), first["accepted"].as_u64()),
        (Some("ack"), Some(1), Some(2))
    );
    let rejected: Vec<u64> =
        first["rejected"].as_array().unwrap().iter().map(|r| r["index"].as_u64().unwrap()).collect();
    assert_eq!(rejected, [2, 3]);
    let second = receive(&mut socket).await;
    assert_eq!(
        (second["batch"].as_u64(), second["accepted"].as_u64(), second["duplicates"].as_u64()),
        (Some(2), Some(1), Some(1))
    );
    assert_eq!(receive(&mut socket).await["batch"], 3);

    // Text on a MessagePack socket and an empty batch are errors, not disconnects.
    socket.send(Message::Text("{}".into())).await.unwrap();
    let error = receive(&mut socket).await;
    assert_eq!((error["type"].as_str(), error["error"].as_str()), (Some("error"), Some("invalid_request")));
    send_msgpack(&mut socket, &batch(4, vec![])).await;
    let error = receive(&mut socket).await;
    assert_eq!((error["type"].as_str(), error["batch"].as_u64()), (Some("error"), Some(4)));
    socket.close(None).await.unwrap();

    let (status, joined) = servers.get("/v1/exchanges/ws-1").await;
    assert_eq!(status, 200);
    assert_eq!((joined["segments"].as_u64(), joined["complete"].as_bool()), (Some(2), Some(true)));
    assert_eq!(joined["frames"].as_array().unwrap().len(), 4);
    assert_eq!(joined["end"]["close_code"], 1000);

    let (_, stored) = servers.get("/v1/exchanges/a").await;
    assert_eq!(stored["url"], "https://openrouter.ai/api/v1/chat/completions");
    assert_eq!(stored["frames"], exchange("a", "x")["frames"]);
}

#[tokio::test]
async fn json_socket_and_negotiation_and_auth() {
    let servers = start().await;
    let mut socket = connect(servers.ingest, Some(INGEST_TOKEN), protocols::JSON).await.unwrap();
    socket.send(Message::Text(batch(7, vec![exchange("j", "listing.parse")]).to_string().into())).await.unwrap();
    let ack = receive(&mut socket).await;
    assert_eq!((ack["batch"].as_u64(), ack["accepted"].as_u64()), (Some(7), Some(1)));
    // A lone surrogate escape rejects only its record.
    let broken = exchange("k", "o").to_string().replace("Hallo", "\\ud800");
    let raw = format!(r#"{{"type":"batch","id":8,"exchanges":[{},{broken}]}}"#, exchange("l", "o"));
    socket.send(Message::Text(raw.into())).await.unwrap();
    let ack = receive(&mut socket).await;
    assert_eq!((ack["accepted"].as_u64(), ack["rejected"][0]["index"].as_u64()), (Some(1), Some(1)));
    socket.send(Message::Text("not json".into())).await.unwrap();
    assert_eq!(receive(&mut socket).await["batch"], Value::Null);

    let rejected_status = |error: tungstenite::Error| match error {
        tungstenite::Error::Http(response) => response.status().as_u16(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(rejected_status(connect(servers.ingest, None, protocols::JSON).await.err().unwrap()), 401);
    assert_eq!(rejected_status(connect(servers.ingest, Some(QUERY_TOKEN), protocols::JSON).await.err().unwrap()), 401);
    assert_eq!(rejected_status(connect(servers.ingest, Some(INGEST_TOKEN), "chat").await.err().unwrap()), 400);
}

#[tokio::test]
async fn http_ingestion_accepts_gzip_and_limits_after_decompression() {
    let servers = start().await;
    let gzip = |body: &Value| {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body.to_string().as_bytes()).unwrap();
        encoder.finish().unwrap()
    };
    let post = |body: Vec<u8>, gzipped: bool| {
        let servers = &servers;
        async move {
            let mut headers = vec![("Content-Type", "application/json")];
            if gzipped {
                headers.push(("Content-Encoding", "gzip"));
            }
            servers.http.send(servers.ingest, "POST", "/v1/exchanges", Some(INGEST_TOKEN), &headers, &body).await
        }
    };
    let (status, body) = post(gzip(&json!({ "exchanges": [exchange("h", "o")] })), true).await;
    assert_eq!((status, body["accepted"].as_u64()), (200, Some(1)));
    let (status, body) = post(json!({ "exchanges": [] }).to_string().into_bytes(), false).await;
    assert_eq!((status, body["error"].as_str()), (400, Some("invalid_request")));
    let bomb = gzip(&json!({ "exchanges": [], "pad": "0".repeat(40 * 1024 * 1024) }));
    assert!(bomb.len() < 1024 * 1024);
    assert_eq!(post(bomb, true).await.0, 413);
    let unauthorised = servers.http.send(servers.ingest, "POST", "/v1/exchanges", Some(QUERY_TOKEN), &[], b"{}").await;
    assert_eq!(unauthorised.0, 401);
}

#[tokio::test]
async fn query_api_lists_gets_and_summarises() {
    let servers = start().await;
    let mut socket = connect(servers.ingest, Some(INGEST_TOKEN), protocols::MSGPACK).await.unwrap();
    send_msgpack(&mut socket, &batch(1, vec![exchange("a", "support.chat"), exchange("b", "support.chat")])).await;
    receive(&mut socket).await;

    let (status, body) = servers.get("/v1/exchanges?session_id=chat-1&limit=1").await;
    assert_eq!(status, 200);
    assert_eq!((body["entries"][0]["id"].as_str(), body["entries"][0]["frame_count"].as_u64()), (Some("b"), Some(3)));
    assert!(body["entries"][0].get("frames").is_none(), "the list has no frames");
    let cursor = body["next_cursor"].as_str().unwrap().to_owned();
    let (_, body) = servers.get(&format!("/v1/exchanges?limit=1&cursor={cursor}")).await;
    assert_eq!((body["entries"][0]["id"].as_str(), &body["next_cursor"]), (Some("a"), &Value::Null));

    let (status, body) = servers.get("/v1/exchanges/missing").await;
    assert_eq!((status, body["error"].as_str()), (404, Some("not_found")));
    let (status, body) = servers.get("/v1/summary?group_by=service,transport").await;
    assert_eq!(status, 200);
    assert_eq!(body["rows"][0]["group"], json!({ "service": "traumwohnung-web", "transport": "sse" }));
    assert_eq!(
        (body["rows"][0]["exchanges"].as_u64(), body["rows"][0]["duration_ms_avg"].as_f64()),
        (Some(2), Some(320.0))
    );

    for path in [
        "/v1/exchanges?limit=0",
        "/v1/exchanges?transport=pigeon",
        "/v1/exchanges?bogus=1",
        "/v1/summary?group_by=model",
    ] {
        let (status, body) = servers.get(path).await;
        assert_eq!((status, body["error"].as_str()), (400, Some("invalid_request")), "{path}");
    }
    let wrong = servers.http.send(servers.query, "GET", "/v1/exchanges", Some(INGEST_TOKEN), &[], b"").await;
    assert_eq!(wrong.0, 401);
    let health = servers.http.send(servers.query, "GET", "/health", None, &[], b"").await;
    assert_eq!((health.0, health.1["api"].as_str()), (200, Some("query")));
    let health = servers.http.send(servers.ingest, "GET", "/health", None, &[], b"").await;
    assert_eq!((health.0, health.1["api"].as_str()), (200, Some("ingest")));
}
