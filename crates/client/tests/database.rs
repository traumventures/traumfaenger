//! End-to-end: the Rust client against an in-process traumfaenger server.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::net::TcpListener;
use traumfaenger_client::{
    Database, DatabaseOptions, Direction, Encoding, EndReason, Operation, QueryClient, UNATTRIBUTED,
    api::{ListQuery, SummaryQuery, Transport},
    operation,
};
use traumfaenger_server::{http, store::Store};

const INGEST_TOKEN: &str = "ingest-token-0123456789";
const QUERY_TOKEN: &str = "query-token-0123456789";

async fn start() -> (String, QueryClient) {
    let store = Arc::new(Store::open(":memory:", None).unwrap());
    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let query = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let urls = (format!("http://{}", ingest.local_addr().unwrap()), format!("http://{}", query.local_addr().unwrap()));
    let ingest_router = http::ingest_router(store.clone(), Some(INGEST_TOKEN.into()));
    let query_router = http::query_router(store, Some(QUERY_TOKEN.into()));
    tokio::spawn(async move { axum::serve(ingest, ingest_router).await });
    tokio::spawn(async move { axum::serve(query, query_router).await });
    (urls.0, QueryClient::new(urls.1, Some(QUERY_TOKEN.into())))
}

fn database(url: &str, encoding: Encoding, errors: &Arc<Mutex<Vec<String>>>) -> Database {
    let errors = errors.clone();
    Database::new(
        DatabaseOptions::new(url, "rust-service")
            .token(INGEST_TOKEN)
            .encoding(encoding)
            .on_error(move |message| errors.lock().unwrap().push(message.to_owned())),
    )
    .unwrap()
}

async fn only(query: &QueryClient, operation: &str) -> traumfaenger_client::api::StoredExchange {
    let page = query.list(&ListQuery { operation: Some(operation.into()), ..Default::default() }).await.unwrap();
    let id = &page.entries.first().expect("an exchange").id;
    query.get_exchange(id).await.unwrap().unwrap()
}

#[tokio::test]
async fn records_http_sse_and_websocket_exchanges_in_both_encodings() {
    let (ingest_url, query) = start().await;
    for encoding in [Encoding::Msgpack, Encoding::Json] {
        let errors = Arc::new(Mutex::new(Vec::new()));
        let database = database(&ingest_url, encoding, &errors);
        let tag = format!("{encoding:?}").to_lowercase();

        operation(Operation::new(format!("parse.{tag}")).session("s-1").meta("listing_id", 123), async {
            let mut recording = database.record_http("https://api.example.com/v1/chat/completions?key=secret").unwrap();
            recording.request_body(br#"{"model":"m"}"#);
            recording.status(200);
            recording.response_body(br#"{"choices":[]}"#, Some("application/json"));
            recording.finish(EndReason::Complete, None);

            // A stream dropped half-way records itself as cancelled.
            let mut stream = database.record_http("https://api.example.com/v1/chat/completions").unwrap();
            stream.request_body(br#"{"stream":true}"#);
            stream.status(200);
            stream.stream_chunk(b"data: {\"a\":1}\n\nda");
            stream.stream_chunk(b"ta: {\"a\":2}\n\n");
            drop(stream);
        })
        .await;

        operation(Operation::new(format!("voice.{tag}")), async {
            let mut socket = database.record_socket("wss://api.voice.example/v1/realtime").unwrap();
            socket.opened();
            socket.text(Direction::Out, r#"{"type":"session.update"}"#);
            socket.flush_segment();
            socket.text(Direction::In, r#"{"type":"transcript.delta"}"#);
            socket.binary(Direction::Out, 16);
            socket.close(Some(1000), None);
        })
        .await;

        database.flush().await;
        assert_eq!(database.stats().queued, 0, "{tag}: {:?}", errors.lock().unwrap());

        let list =
            query.list(&ListQuery { operation: Some(format!("parse.{tag}")), ..Default::default() }).await.unwrap();
        let by = |transport| list.entries.iter().find(|entry| entry.transport == transport).unwrap().id.clone();
        let http = query.get_exchange(&by(Transport::Http)).await.unwrap().unwrap();
        assert_eq!(http.url, "https://api.example.com/v1/chat/completions");
        assert_eq!(http.service, "rust-service");
        assert_eq!(http.metadata["listing_id"], "123");
        assert_eq!(http.frames.len(), 2);

        let stream = query.get_exchange(&by(Transport::Sse)).await.unwrap().unwrap();
        assert_eq!(stream.end.unwrap().reason, EndReason::Cancelled);
        let events: Vec<&str> = stream.frames[1..].iter().map(|frame| frame.data.as_str()).collect();
        assert_eq!(events, [r#"data: {"a":1}"#, r#"data: {"a":2}"#]);

        let voice = only(&query, &format!("voice.{tag}")).await;
        assert_eq!((voice.segments, voice.complete, voice.status), (2, true, Some(101)));
        assert_eq!(voice.frames[2].data, "[traumfaenger: unrecorded binary message, 16 bytes]");
        assert_eq!(voice.end.unwrap().close_code, Some(1000));

        database.close(Duration::from_secs(1)).await;
        assert!(errors.lock().unwrap().is_empty(), "{:?}", errors.lock().unwrap());
    }

    let summary = query.summary(&SummaryQuery::default()).await.unwrap();
    assert_eq!(summary.rows[0].exchanges, 6);
}

#[tokio::test]
async fn recordings_outside_an_operation_are_unattributed_and_late_ones_are_dropped() {
    let (ingest_url, query) = start().await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let database = database(&ingest_url, Encoding::Msgpack, &errors);
    database.record_http("https://api.example.com/x").unwrap().finish(EndReason::Error, Some("connect failed"));
    database.flush().await;
    let exchange = only(&query, UNATTRIBUTED).await;
    assert_eq!(exchange.end.unwrap().error.as_deref(), Some("connect failed"));

    database.close(Duration::from_secs(1)).await;
    database.record_http("https://api.example.com/x").unwrap().finish(EndReason::Complete, None);
    assert_eq!(database.stats().dropped, 1);
    assert!(database.record_http("ftp://x").is_none());
}

#[tokio::test]
async fn an_unreachable_database_backs_off_and_close_keeps_its_deadline() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let database = database("http://127.0.0.1:1", Encoding::Msgpack, &errors);
    database.record_http("https://api.example.com/x").unwrap().finish(EndReason::Complete, None);
    database.flush().await;
    database.flush().await; // backing off: returns at once
    let stats = database.stats();
    assert_eq!((stats.queued, stats.failed_sends), (1, 1));

    let start = std::time::Instant::now();
    database.close(Duration::from_millis(500)).await;
    assert!(start.elapsed() < Duration::from_secs(3));
    assert_eq!(database.stats().queued, 1);
    assert!(errors.lock().unwrap().iter().any(|message| message.contains("unsent")));
}

#[tokio::test]
async fn close_flushes_open_sockets_and_streams_without_chunks_stay_sse() {
    let (ingest_url, query) = start().await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let database = database(&ingest_url, Encoding::Msgpack, &errors);
    let mut socket = operation(Operation::new("open.socket"), async {
        let mut socket = database.record_socket("wss://api.voice.example/v1/realtime").unwrap();
        socket.opened();
        socket.text(Direction::Out, "still open");
        socket
    })
    .await;
    operation(Operation::new("empty.stream"), async {
        let mut stream = database.record_http("https://api.example.com/v1/chat").unwrap();
        stream.status(200);
        stream.streaming();
        stream.mark_truncated();
        stream.finish(EndReason::Complete, None);
    })
    .await;
    database.close(Duration::from_secs(1)).await;
    socket.text(Direction::In, "after close");

    let open = only(&query, "open.socket").await;
    assert_eq!((open.segments, open.complete), (1, false));
    assert_eq!(open.frames[0].data, "still open");
    let empty = only(&query, "empty.stream").await;
    assert_eq!((empty.transport, empty.truncated), (Transport::Sse, true));
    assert_eq!(database.stats().dropped, 0, "nothing was recorded after close yet");
    drop(socket);
    assert_eq!(database.stats().dropped, 1, "the final segment came after close");
}

#[tokio::test]
async fn quiet_sockets_are_cut_into_segments() {
    let (ingest_url, query) = start().await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut options = DatabaseOptions::new(&ingest_url, "rust-service").token(INGEST_TOKEN);
    options.segments.interval = Duration::from_millis(200);
    options.flush_interval = Duration::from_millis(100);
    let errors_handle = errors.clone();
    let database =
        Database::new(options.on_error(move |message| errors_handle.lock().unwrap().push(message.to_owned()))).unwrap();
    let _socket = operation(Operation::new("quiet.socket"), async {
        let mut socket = database.record_socket("wss://api.voice.example/v1/realtime").unwrap();
        socket.text(Direction::Out, "hello");
        socket
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    database.flush().await;
    let quiet = only(&query, "quiet.socket").await;
    assert_eq!((quiet.segments, quiet.frames.len()), (1, 1));
}

#[tokio::test]
#[allow(clippy::result_large_err)] // tungstenite fixes the handshake callback's error type.
async fn a_server_that_stops_reading_cannot_hold_close_past_its_deadline() {
    // Completes the WebSocket handshake, then never reads.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            let callback =
                |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
                 mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    response.headers_mut().insert("sec-websocket-protocol", "traumfaenger.v1.msgpack".parse().unwrap());
                    Ok(response)
                };
            if let Ok(socket) = tokio_tungstenite::accept_hdr_async(stream, callback).await {
                held.push(socket);
            }
        }
    });
    let errors = Arc::new(Mutex::new(Vec::new()));
    let database = database(&url, Encoding::Msgpack, &errors);
    let big = "x".repeat(3 * 1024 * 1024);
    for _ in 0..12 {
        let mut recording = database.record_http("https://api.example.com/x").unwrap();
        recording.request_body(big.as_bytes());
        recording.finish(EndReason::Complete, None);
    }
    let start = std::time::Instant::now();
    database.close(Duration::from_secs(1)).await;
    assert!(start.elapsed() < Duration::from_secs(4), "close took {:?}", start.elapsed());
    assert!(database.stats().queued > 0);
}

#[test]
fn close_returns_after_the_runtime_of_the_sender_is_gone() {
    let first = tokio::runtime::Runtime::new().unwrap();
    let database = first
        .block_on(async { Database::new(DatabaseOptions::new("http://127.0.0.1:1", "s").on_error(|_| {})).unwrap() });
    drop(first);
    let second = tokio::runtime::Runtime::new().unwrap();
    second.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), database.close(Duration::from_secs(1))).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), database.flush()).await.unwrap();
    });
    assert!(Database::new(DatabaseOptions::new("http://x", "s")).is_err(), "outside a runtime");
}

#[tokio::test]
async fn connecting_respects_the_close_deadline() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    // Not routable: connecting hangs until a timeout.
    let database = database("http://10.255.255.1:7600", Encoding::Msgpack, &errors);
    database.record_http("https://api.example.com/x").unwrap().finish(EndReason::Complete, None);
    let start = std::time::Instant::now();
    database.close(Duration::from_millis(500)).await;
    assert!(start.elapsed() < Duration::from_secs(3), "close took {:?}", start.elapsed());
}
