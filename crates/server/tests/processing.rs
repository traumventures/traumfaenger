//! Processing the raw log and querying the result: projection, analytics,
//! session trajectories, contents and the query API's auth.

use axum::body::Body;
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use traumfaenger_api::Exchange;
use traumfaenger_server::{http, projection, store::Store};

const OPENROUTER: &str = "https://openrouter.ai/api/v1/chat/completions";
const SYSTEM: &str = "You are the support agent. Be brief.";

fn exchange(id: &str, at: &str, messages: Value, response: Value, extra: Value) -> Exchange {
    let mut value = json!({
        "id": id,
        "service": "app",
        "operation": "support-agent",
        "session_id": "s1",
        "metadata": { "user": "u1" },
        "transport": "http",
        "url": OPENROUTER,
        "started_at": at,
        "status": 200,
        "frames": [
            { "t_ms": 0, "dir": "out", "data": json!({ "model": "m", "tools": [{ "type": "function", "function": { "name": "lookup" } }], "messages": messages }).to_string() },
            { "t_ms": 1000, "dir": "in", "data": response.to_string() },
        ],
        "end": { "at_ms": 1000, "reason": "complete" },
    });
    if let (Some(target), Some(extra)) = (value.as_object_mut(), extra.as_object()) {
        target.extend(extra.clone());
    }
    serde_json::from_value(value).unwrap()
}

fn answer(content: Option<&str>, tool_call: Option<(&str, &str)>, cost: f64) -> Value {
    let tool_calls = tool_call
        .map(|(id, name)| json!([{ "id": id, "type": "function", "function": { "name": name, "arguments": "{}" } }]));
    json!({
        "model": "m-served", "provider": "P",
        "choices": [{ "index": 0, "finish_reason": if tool_call.is_some() { "tool_calls" } else { "stop" },
                      "message": { "role": "assistant", "content": content, "tool_calls": tool_calls } }],
        "usage": { "prompt_tokens": 1000, "completion_tokens": 100, "cost": cost,
                   "prompt_tokens_details": { "cached_tokens": 800 },
                   "cost_details": { "upstream_inference_prompt_cost": cost * 0.75, "upstream_inference_completions_cost": cost * 0.25 } },
    })
}

/// One agent turn: the model calls `lookup`, gets an error back, answers.
/// Plus an unrelated listing-parse call and a rate-limited one.
fn conversation() -> Vec<Exchange> {
    let user = json!({ "role": "user", "content": "Where is my flat?" });
    let call = json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "t1", "type": "function", "function": { "name": "lookup", "arguments": "{}" } }] });
    let result = json!({ "role": "tool", "tool_call_id": "t1", "content": "{\"error\":\"not found\"}" });
    let system = json!({ "role": "system", "content": SYSTEM });
    vec![
        exchange(
            "c1",
            "2026-10-09T10:00:00Z",
            json!([system, user]),
            answer(None, Some(("t1", "lookup")), 0.004),
            json!({}),
        ),
        exchange(
            "c2",
            "2026-10-09T10:00:01.250Z",
            json!([system, user, call, result]),
            answer(Some("Not found, sorry."), None, 0.006),
            json!({}),
        ),
        exchange(
            "c3",
            "2026-10-09T11:30:00Z",
            json!([{ "role": "system", "content": "Parse the listing." }, { "role": "user", "content": "2 rooms" }]),
            answer(Some("{}"), None, 0.001),
            json!({ "operation": "listing-parse", "session_id": null, "metadata": { "user": "u2" } }),
        ),
        exchange(
            "c4",
            "2026-10-09T11:31:00Z",
            json!([{ "role": "user", "content": "x" }]),
            json!({ "error": { "code": 429, "message": "slow down" } }),
            json!({ "operation": "listing-parse", "session_id": null, "status": 429 }),
        ),
    ]
}

fn processed_store() -> Store {
    let store = Store::open(":memory:", None).unwrap();
    store.insert(&conversation(), Utc::now()).unwrap();
    store.prepare_processing().unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    store
}

fn analytics(store: &Store, source: &str, query: &str) -> Vec<Value> {
    let params: Vec<(String, String)> =
        query.split('&').filter_map(|pair| pair.split_once('=')).map(|(k, v)| (k.to_owned(), v.to_owned())).collect();
    let response = store.analytics(source, &params).unwrap();
    serde_json::to_value(response.rows).unwrap().as_array().unwrap().clone()
}

#[test]
fn processes_calls_tool_round_trips_and_shared_contents() {
    let store = processed_store();
    let status = store.processing_status().unwrap();
    assert_eq!((status.calls, status.processed_seq, status.latest_seq), (4, 4, 4));
    // The system prompt, user message and tool definitions repeat; stored once.
    let parts: u64 = analytics(&store, "parts", "metrics=parts")[0]["parts"].as_u64().unwrap();
    assert!(status.contents < parts, "{} contents for {parts} parts", status.contents);

    let tools = analytics(&store, "tools", "metrics=tool_calls,answered,failed,gap_ms_p50&group_by=tool");
    assert_eq!(tools, [json!({ "tool": "lookup", "tool_calls": 1, "answered": 1, "failed": 1, "gap_ms_p50": 250.0 })]);

    let rows = analytics(
        &store,
        "calls",
        "metrics=calls,turns,agent_turns,sessions,tool_calls,cost_usd,input_cost_usd,cache_hit_rate&operation=support-agent",
    );
    let row = &rows[0];
    assert_eq!(
        (row["calls"].as_u64(), row["turns"].as_u64(), row["agent_turns"].as_u64()),
        (Some(2), Some(1), Some(1))
    );
    assert_eq!((row["sessions"].as_u64(), row["tool_calls"].as_u64()), (Some(1), Some(1)));
    assert!((row["cost_usd"].as_f64().unwrap() - 0.010).abs() < 1e-12);
    assert!((row["input_cost_usd"].as_f64().unwrap() - 0.0075).abs() < 1e-12);
    assert_eq!(row["cache_hit_rate"].as_f64(), Some(0.8));
}

#[test]
fn analytics_groups_buckets_and_filters() {
    let store = processed_store();
    let rows = analytics(&store, "calls", "metrics=calls,failed_calls&group_by=operation,outcome&order=calls");
    assert_eq!(rows[0], json!({ "operation": "support-agent", "outcome": "ok", "calls": 2, "failed_calls": 0 }));
    assert!(
        rows.contains(
            &json!({ "operation": "listing-parse", "outcome": "rate_limited", "calls": 1, "failed_calls": 1 })
        )
    );

    let hourly = analytics(&store, "calls", "metrics=calls&interval=1h");
    assert_eq!(
        hourly,
        [json!({ "time": "2026-10-09T10:00:00Z", "calls": 2 }), json!({ "time": "2026-10-09T11:00:00Z", "calls": 2 })]
    );
    // Grafana sends epoch milliseconds.
    let from = chrono::DateTime::parse_from_rfc3339("2026-10-09T11:00:00Z").unwrap().timestamp_millis();
    let late = analytics(&store, "calls", &format!("metrics=calls&from={from}"));
    assert_eq!(late[0]["calls"].as_u64(), Some(2));

    let by_user = analytics(&store, "calls", "metrics=cost_usd&group_by=metadata.user&metadata.user=u2");
    assert_eq!(by_user, [json!({ "metadata.user": "u2", "cost_usd": 0.001 })]);
    let all = analytics(&store, "calls", "metrics=calls&operation=*&model=*");
    assert_eq!(all, [json!({ "calls": 4 })], "`*` filters nothing");

    // Cost allocated to what the requests carried: system prompts, history, tool results.
    let kinds = analytics(&store, "parts", "metrics=allocated_cost_usd&group_by=kind&side=request");
    let total: f64 = kinds.iter().map(|row| row["allocated_cost_usd"].as_f64().unwrap_or(0.0)).sum();
    assert!((total - (0.004 + 0.006 + 0.001) * 0.75).abs() < 1e-9, "{kinds:?}");

    for (source, query) in [
        ("calls", "group_by=model"),
        ("calls", "metrics=nope"),
        ("calls", "metrics=calls&group_by=metadata.x'y"),
        ("calls", "metrics=calls&interval=5x"),
        ("calls", "metrics=calls&order=time"),
        ("tools", "metrics=tool_calls&group_by=metadata.user"),
        ("nope", "metrics=calls"),
    ] {
        let params: Vec<(String, String)> = query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert!(store.analytics(source, &params).is_err(), "{source}?{query}");
    }
}

#[test]
fn session_lists_what_each_call_added() {
    let store = processed_store();
    let session = store.session("s1", 100, true).unwrap().unwrap();
    let added: Vec<Vec<String>> = session
        .calls
        .iter()
        .map(|call| call.parts.iter().map(|part| format!("{} {}", part.side, part.kind)).collect())
        .collect();
    assert_eq!(
        added,
        [
            vec!["request tool_definitions", "request text", "request text", "response tool_call"],
            vec!["request tool_result", "response text"],
        ]
    );
    assert!(session.calls[0].turn_start && !session.calls[1].turn_start);
    let full = store.session("s1", 5, false).unwrap().unwrap();
    assert_eq!(full.calls[1].parts.len(), 6);
    let system = full.calls[1].parts.iter().find(|part| part.role == "system" && part.kind == "text").unwrap();
    assert_eq!((system.text.as_str(), system.clipped), ("You a", true));
    assert_eq!(store.content(&system.hash).unwrap().unwrap().text, SYSTEM);
    assert!(store.session("none", 100, true).unwrap().is_none());
}

#[test]
fn reprocessing_replaces_rows_and_sweeps() {
    let store = processed_store();
    assert!(!store.prepare_processing().unwrap(), "same parser version: nothing to redo");
    let before = store.processing_status().unwrap();
    // Reprocessing everything replaces rows instead of duplicating them.
    store.restart_processing().unwrap();
    assert_eq!(store.processing_status().unwrap().processed_seq, 0);
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    let after = store.processing_status().unwrap();
    assert_eq!((after.calls, after.contents, after.processed_seq), (before.calls, before.contents, 4));
    assert_eq!(analytics(&store, "tools", "metrics=tool_calls,answered")[0], json!({ "tool_calls": 1, "answered": 1 }));
    assert_eq!(store.sweep_contents().unwrap(), 0);
}

#[tokio::test]
async fn query_api_requires_the_query_token() {
    let store = std::sync::Arc::new(processed_store());
    let router = http::query_router(store, Some("query-token-0123456789".into()));
    let request = |token: Option<&str>, path: &str| {
        let mut builder = axum::http::Request::get(path);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).unwrap()
    };
    for path in ["/v1/analytics/calls?metrics=calls", "/v1/sessions/s1", "/v1/calls/c1", "/v1/processing"] {
        let denied = router.clone().oneshot(request(None, path)).await.unwrap();
        assert_eq!(denied.status(), 401, "{path}");
        let allowed = router.clone().oneshot(request(Some("query-token-0123456789"), path)).await.unwrap();
        assert_eq!(allowed.status(), 200, "{path}");
    }
    let response = router
        .clone()
        .oneshot(request(Some("query-token-0123456789"), "/v1/analytics/calls?metrics=calls&group_by=model"))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["rows"][0], json!({ "model": "m-served", "calls": 3 }));
    let missing = router.oneshot(request(Some("query-token-0123456789"), "/v1/calls/none")).await.unwrap();
    assert_eq!(missing.status(), 404);
}

fn ws_record(id: &str, segment: u32, is_final: bool) -> Exchange {
    let mut value = json!({
        "id": id, "segment": segment, "final": is_final, "service": "app", "operation": "voice",
        "transport": "ws", "url": "wss://realtime.example/v1", "started_at": "2026-10-09T10:00:00Z",
        "frames": [{ "t_ms": segment, "dir": "in", "data": format!("frame {segment}") }],
    });
    if is_final {
        value["end"] = json!({ "at_ms": 100, "reason": "complete" });
    }
    serde_json::from_value(value).unwrap()
}

fn call_ids(store: &Store) -> Vec<String> {
    let rows = analytics(store, "calls", "metrics=calls&group_by=call_id");
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| {
            assert_eq!(row["calls"], 1, "one row per exchange: {row}");
            row["call_id"].as_str().unwrap().to_owned()
        })
        .collect();
    ids.sort();
    ids
}

#[test]
fn small_windows_process_every_exchange_once() {
    let store = Store::open(":memory:", None).unwrap();
    store.prepare_processing().unwrap();
    // A WebSocket exchange that (wrongly) sends two final records around
    // three plain calls: a window must not jump past the plain calls.
    store.insert(&[ws_record("ws", 0, true)], Utc::now()).unwrap();
    let plain: Vec<Exchange> = conversation().into_iter().take(3).collect();
    for exchange in plain {
        store.insert(&[exchange], Utc::now()).unwrap();
    }
    store.insert(&[ws_record("ws", 1, true)], Utc::now()).unwrap();
    while !store.process_batch(2).unwrap().caught_up {}
    assert_eq!(call_ids(&store), ["c1", "c2", "c3", "ws"]);
    assert_eq!(store.processing_status().unwrap().processed_seq, 5);
}

#[test]
fn a_segment_after_the_final_record_reprocesses_the_exchange() {
    let store = Store::open(":memory:", None).unwrap();
    store.prepare_processing().unwrap();
    store.insert(&[ws_record("late", 1, true)], Utc::now()).unwrap();
    assert_eq!(store.process_batch(projection::BATCH).unwrap().processed, 1);
    store.insert(&[ws_record("late", 0, false)], Utc::now()).unwrap();
    assert_eq!(store.process_batch(projection::BATCH).unwrap().processed, 1, "reprocessed with the late segment");
    assert_eq!(store.process_batch(projection::BATCH).unwrap().processed, 0);
    assert_eq!(call_ids(&store), ["late"]);
}

#[test]
fn rejects_implausible_start_times() {
    let mut far = conversation().remove(0);
    far.started_at = "+262142-12-31T23:59:59Z".parse().unwrap();
    assert!(far.validate().unwrap_err().contains("started_at"));
}

#[test]
fn repeated_tool_call_ids_link_to_their_own_results() {
    // Two conversations without a session, both numbering their first tool
    // call `functions.lookup:0`, interleaved in time.
    let call = |args: &str| json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "functions.lookup:0", "type": "function", "function": { "name": "lookup", "arguments": args } }] });
    let result = |text: &str| json!({ "role": "tool", "tool_call_id": "functions.lookup:0", "content": text });
    let user = |text: &str| json!({ "role": "user", "content": text });
    let answer_with = |args: &str| {
        let mut value = answer(None, Some(("functions.lookup:0", "lookup")), 0.001);
        value["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] = json!(args);
        value
    };
    let no_session = json!({ "session_id": null });
    let exchanges = vec![
        exchange("a1", "2026-10-09T10:00:00Z", json!([user("a")]), answer_with("{\"q\":\"a\"}"), no_session.clone()),
        exchange("b1", "2026-10-09T10:00:01Z", json!([user("b")]), answer_with("{\"q\":\"b\"}"), no_session.clone()),
        exchange(
            "a2",
            "2026-10-09T10:00:02Z",
            json!([user("a"), call("{\"q\":\"a\"}"), result("ra")]),
            answer(Some("done a"), None, 0.001),
            no_session.clone(),
        ),
        exchange(
            "b2",
            "2026-10-09T10:00:03Z",
            json!([user("b"), call("{\"q\":\"b\"}"), result("rb")]),
            answer(Some("done b"), None, 0.001),
            no_session,
        ),
    ];
    let store = Store::open(":memory:", None).unwrap();
    store.insert(&exchanges, Utc::now()).unwrap();
    store.prepare_processing().unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    let mut rows = analytics(&store, "tools", "metrics=tool_calls,answered&group_by=call_id,result_call_id");
    rows.sort_by_key(|row| row["call_id"].as_str().unwrap().to_owned());
    assert_eq!(
        rows,
        [
            json!({ "call_id": "a1", "result_call_id": "a2", "tool_calls": 1, "answered": 1 }),
            json!({ "call_id": "b1", "result_call_id": "b2", "tool_calls": 1, "answered": 1 }),
        ]
    );
}

#[test]
fn retention_keeps_open_exchanges_until_they_are_processed() {
    let store = Store::open(":memory:", None).unwrap();
    store.prepare_processing().unwrap();
    let received = Utc::now() - chrono::Duration::days(2);
    store.insert(&[ws_record("open", 0, false)], received).unwrap();
    store.insert(&conversation()[..1], received).unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    // Both records are below the checkpoint, but the WebSocket exchange is
    // still open: only the finished call goes.
    let cutoff = Utc::now() - chrono::Duration::days(1);
    assert_eq!(store.delete_received_before(cutoff).unwrap(), 1);
    store.insert(&[ws_record("open", 1, true)], Utc::now()).unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    assert_eq!(call_ids(&store), ["c1", "open"]);
    // Processed, but its final record is not past the cutoff yet: deleting
    // the old segment would leave a reprocessing with half the exchange.
    assert_eq!(store.delete_received_before(cutoff).unwrap(), 0);
    assert_eq!(store.delete_received_before(Utc::now() + chrono::Duration::seconds(1)).unwrap(), 2);
    // A record that never finishes goes a week after the cutoff.
    store.insert(&[ws_record("abandoned", 0, false)], Utc::now() - chrono::Duration::days(9)).unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    assert_eq!(store.delete_received_before(cutoff).unwrap(), 1);
}

#[test]
fn a_reused_tool_call_id_is_not_linked_to_another_conversations_result() {
    // Like above, but conversation A's result is processed in an earlier
    // window than B's: B must not fall back to A's result.
    let call = |args: &str| json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "functions.lookup:0", "type": "function", "function": { "name": "lookup", "arguments": args } }] });
    let result = |text: &str| json!({ "role": "tool", "tool_call_id": "functions.lookup:0", "content": text });
    let user = |text: &str| json!({ "role": "user", "content": text });
    let answer_with = |args: &str| {
        let mut value = answer(None, Some(("functions.lookup:0", "lookup")), 0.001);
        value["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] = json!(args);
        value
    };
    let no_session = json!({ "session_id": null });
    let store = Store::open(":memory:", None).unwrap();
    store.prepare_processing().unwrap();
    store
        .insert(
            &[
                exchange(
                    "a1",
                    "2026-10-09T10:00:00Z",
                    json!([user("a")]),
                    answer_with("{\"q\":\"a\"}"),
                    no_session.clone(),
                ),
                exchange(
                    "b1",
                    "2026-10-09T10:00:01Z",
                    json!([user("b")]),
                    answer_with("{\"q\":\"b\"}"),
                    no_session.clone(),
                ),
                exchange(
                    "a2",
                    "2026-10-09T10:00:02Z",
                    json!([user("a"), call("{\"q\":\"a\"}"), result("ra")]),
                    answer(Some("done a"), None, 0.001),
                    no_session.clone(),
                ),
            ],
            Utc::now(),
        )
        .unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    store
        .insert(
            &[exchange(
                "b2",
                "2026-10-09T10:00:03Z",
                json!([user("b"), call("{\"q\":\"b\"}"), result("rb")]),
                answer(Some("done b"), None, 0.001),
                no_session,
            )],
            Utc::now(),
        )
        .unwrap();
    while !store.process_batch(projection::BATCH).unwrap().caught_up {}
    let mut rows = analytics(&store, "tools", "metrics=tool_calls&group_by=call_id,result_call_id");
    rows.sort_by_key(|row| row["call_id"].as_str().unwrap().to_owned());
    assert_eq!(
        rows,
        [
            json!({ "call_id": "a1", "result_call_id": "a2", "tool_calls": 1 }),
            json!({ "call_id": "b1", "result_call_id": "b2", "tool_calls": 1 }),
        ]
    );
}
