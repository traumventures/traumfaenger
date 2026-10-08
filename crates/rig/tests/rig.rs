//! End-to-end: a real rig OpenAI client, sending through `Recorded`, against
//! a mock OpenAI-compatible provider, recorded into an in-process database.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::ws::{Message, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::{any, post},
};
use futures_util::StreamExt;
use rig_core::{
    http_client::{NoBody, Request},
    providers::openai::OpenAIConfig,
    ws_client::{ConnectOptions, Frame, WebSocketClientExt},
};
use rig_reqwest::ReqwestClient;
use rig_tungstenite::TungsteniteClient;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use traumfaenger_client::{
    Database, DatabaseOptions, EndReason, Operation, QueryClient,
    api::{Direction, ListQuery, StoredExchange, Transport},
    operation,
};
use traumfaenger_rig::Recorded;
use traumfaenger_server::{http, store::Store};

async fn provider() -> String {
    async fn chat(Json(body): Json<Value>) -> Response {
        let usage = json!({ "prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9 });
        let base = json!({ "id": "gen-1", "created": 0, "model": "mock-model" });
        if body["stream"] != json!(true) {
            let mut reply = base.clone();
            reply["object"] = json!("chat.completion");
            reply["choices"] = json!([{ "index": 0, "message": { "role": "assistant", "content": "Hallo" }, "finish_reason": "stop" }]);
            reply["usage"] = usage;
            return Json(reply).into_response();
        }
        let chunk = |delta: Value, finish: Value, usage: Value| {
            let mut chunk = base.clone();
            chunk["object"] = json!("chat.completion.chunk");
            chunk["choices"] = json!([{ "index": 0, "delta": delta, "finish_reason": finish }]);
            if !usage.is_null() {
                chunk["usage"] = usage;
            }
            format!("data: {chunk}\n\n")
        };
        let body = chunk(json!({ "role": "assistant", "content": "Hal" }), Value::Null, Value::Null)
            + &chunk(json!({ "content": "lo" }), Value::Null, Value::Null)
            + &chunk(json!({}), json!("stop"), usage)
            + "data: [DONE]\n\n";
        ([("content-type", "text/event-stream")], body).into_response()
    }
    async fn realtime(upgrade: WebSocketUpgrade) -> Response {
        upgrade.on_upgrade(|mut socket| async move {
            while let Some(Ok(Message::Text(text))) = socket.recv().await {
                if socket.send(Message::Text(format!("echo {text}").into())).await.is_err() {
                    break;
                }
            }
        })
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route("/v1/chat/completions", post(chat)).route("/v1/realtime", any(realtime));
    tokio::spawn(async move { axum::serve(listener, app).await });
    url
}

async fn database() -> (Database, QueryClient) {
    let store = Arc::new(Store::open(":memory:", None).unwrap());
    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let query = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingest_url = format!("http://{}", ingest.local_addr().unwrap());
    let query_url = format!("http://{}", query.local_addr().unwrap());
    let (ingest_router, query_router) = (http::ingest_router(store.clone(), None), http::query_router(store, None));
    tokio::spawn(async move { axum::serve(ingest, ingest_router).await });
    tokio::spawn(async move { axum::serve(query, query_router).await });
    let database = Database::new(DatabaseOptions::new(ingest_url, "rig-service")).unwrap();
    (database, QueryClient::new(query_url, None))
}

async fn only(query: &QueryClient, operation: &str) -> StoredExchange {
    let page = query.list(&ListQuery { operation: Some(operation.into()), ..Default::default() }).await.unwrap();
    assert_eq!(page.entries.len(), 1, "{operation}: {:?}", page.entries);
    query.get_exchange(&page.entries[0].id).await.unwrap().unwrap()
}

#[tokio::test]
async fn records_rig_calls_and_streams() {
    let provider = provider().await;
    let (database, query) = database().await;
    let openai = OpenAIConfig::new("sk-test")
        .with_base_url(format!("{provider}/v1"))
        .connect(Recorded::new(ReqwestClient::default(), database.clone()));
    let model = openai.chat("mock-model");

    let reply = operation(Operation::new("listing.parse").meta("listing_id", 123), model.call("Hi")).await.unwrap();
    assert!(format!("{reply:?}").contains("Hallo"));

    let streamed = operation(Operation::new("support.chat").session("chat-1"), async {
        let mut stream = model.stream("Hi").unwrap();
        let mut items = 0;
        while let Some(item) = stream.next().await {
            item.unwrap();
            items += 1;
        }
        items
    })
    .await;
    assert!(streamed > 0);
    database.flush().await;

    let call = only(&query, "listing.parse").await;
    assert_eq!(call.service, "rig-service");
    assert_eq!(call.metadata["listing_id"], "123");
    assert_eq!(call.url, format!("{provider}/v1/chat/completions"));
    assert_eq!(call.status, Some(200));
    let request: Value = serde_json::from_str(&call.frames[0].data).unwrap();
    assert_eq!(request["model"], "mock-model");
    assert!(call.frames.last().unwrap().data.contains("\"prompt_tokens\":7"));

    let stream = only(&query, "support.chat").await;
    assert_eq!(stream.session_id.as_deref(), Some("chat-1"));
    assert_eq!(stream.transport, Transport::Sse, "{:?}", stream.frames);
    // rig stops reading after `data: [DONE]` and drops the body before its
    // end; the record says so instead of guessing that the stream was over.
    let end = stream.end.as_ref().unwrap();
    assert_eq!(end.reason, EndReason::Cancelled);
    assert_eq!(end.error.as_deref(), Some("the reader stopped before the end of the response"));
    let events: Vec<&str> = stream.frames[1..].iter().map(|frame| frame.data.as_str()).collect();
    assert_eq!(events.len(), 4, "{events:?}");
    assert_eq!(events[3], "data: [DONE]");
    assert!(events[2].contains("\"usage\""));

    database.close(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn records_rig_websocket_sessions() {
    let provider = provider().await;
    let (database, query) = database().await;
    let client = Recorded::new(TungsteniteClient::new(), database.clone());
    let url = format!("{}/v1/realtime?key=secret", provider.replace("http", "ws"));

    operation(Operation::new("voice.session"), async {
        let request = Request::builder().uri(&url).body(NoBody).unwrap();
        let mut connection = client.connect(request, ConnectOptions::new()).await.unwrap();
        connection.send(Frame::Text("hello".into())).await.unwrap();
        assert_eq!(connection.recv().await.unwrap(), Some(Frame::Text("echo hello".into())));
        connection.close(None).await.unwrap();
    })
    .await;
    database.flush().await;

    let session = only(&query, "voice.session").await;
    assert_eq!(session.transport, Transport::Ws);
    assert_eq!(session.url, format!("{}/v1/realtime", provider.replace("http", "ws")));
    assert_eq!((session.status, session.complete), (Some(101), true));
    let frames: Vec<(Direction, &str)> = session.frames.iter().map(|frame| (frame.dir, frame.data.as_str())).collect();
    assert_eq!(frames, [(Direction::Out, "hello"), (Direction::In, "echo hello")]);
    database.close(Duration::from_secs(1)).await;
}
