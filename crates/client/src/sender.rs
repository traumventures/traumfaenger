//! The ingestion connection: one WebSocket to the database that carries
//! batches of exchanges, pipelined, with in-order acks. Runs as one tokio task.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::{
    net::TcpStream,
    sync::{Notify, oneshot},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::HeaderValue,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
use traumfaenger_api::{ErrorCode, Exchange, ServerMessage, limits, protocols, routes};

/// Encoding of the ingestion socket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Encoding {
    /// Offers MessagePack and JSON; the server picks MessagePack.
    #[default]
    Msgpack,
    /// Offers only JSON.
    Json,
}

/// Called when sending fails or exchanges are rejected or dropped.
pub type ErrorHandler = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub(crate) struct SenderConfig {
    pub ingest_url: String,
    pub token: Option<String>,
    pub encoding: Encoding,
    pub flush_interval: Duration,
    pub max_batch_bytes: usize,
    pub max_queue_bytes: usize,
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub ack_timeout: Duration,
    pub on_error: ErrorHandler,
}

/// Counters of the sender.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    /// Exchanges waiting to be sent or acknowledged.
    pub queued: usize,
    /// Exchanges the server stored (including duplicates of earlier sends).
    pub sent: u64,
    /// Exchanges the server rejected as invalid.
    pub rejected: u64,
    /// Exchanges dropped: queue full, record too large, after close, or refused by the server.
    pub dropped: u64,
    /// Failed connections, timeouts and server errors; their batches are sent again later.
    pub failed_sends: u64,
}

struct Pending {
    exchange: Exchange,
    bytes: usize,
}

struct InFlight {
    id: u64,
    items: Vec<Pending>,
    sent_at: Instant,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Pending>,
    queued_bytes: usize,
    in_flight_items: usize,
    stats: Stats,
    waiters: Vec<oneshot::Sender<()>>,
    flush_requested: bool,
    force: bool,
    backing_off: bool,
    closing: Option<Instant>,
    close_done: Vec<oneshot::Sender<()>>,
    closed: bool,
}

impl State {
    fn idle(&self) -> bool {
        self.queue.is_empty() && self.in_flight_items == 0
    }

    fn requeue(&mut self, items: Vec<Pending>) {
        self.queued_bytes += items.iter().map(|item| item.bytes).sum::<usize>();
        for item in items.into_iter().rev() {
            self.queue.push_front(item);
        }
    }

    fn notify(&mut self) {
        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(());
        }
    }
}

pub(crate) struct Shared {
    state: Mutex<State>,
    wake: Notify,
    config: SenderConfig,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn report(&self, message: &str) {
        let handler = self.config.on_error.clone();
        // A panicking handler must not take the sender down.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(message)));
    }
}

/// Handle to the sender task. The task stops after [`Sender::close`], or
/// flushes for up to five seconds and stops once the handle is dropped.
pub(crate) struct Sender {
    shared: Arc<Shared>,
}

impl Sender {
    /// Starts the sender task on `runtime`.
    pub fn start(config: SenderConfig, runtime: &tokio::runtime::Handle) -> Self {
        let shared = Arc::new(Shared { state: Mutex::default(), wake: Notify::new(), config });
        runtime.spawn(run(shared.clone()));
        Self { shared }
    }

    pub fn push(&self, exchange: Exchange) {
        // JSON size is an upper bound of the MessagePack size, so batches fit either encoding.
        let bytes = serde_json::to_vec(&exchange).map_or(usize::MAX, |json| json.len());
        let mut state = self.shared.lock();
        if state.closed || state.closing.is_some() {
            state.stats.dropped += 1;
            drop(state);
            self.shared.report("traumfaenger: exchange recorded after close() was dropped");
            return;
        }
        if bytes > limits::MAX_BATCH_BYTES - 1024 {
            state.stats.dropped += 1;
            drop(state);
            self.shared.report(&format!("traumfaenger: dropped an exchange of {bytes} bytes, larger than a batch"));
            return;
        }
        state.queue.push_back(Pending { exchange, bytes });
        state.queued_bytes += bytes;
        let mut dropped = 0;
        while state.queued_bytes > self.shared.config.max_queue_bytes && state.queue.len() > 1 {
            if let Some(oldest) = state.queue.pop_front() {
                state.queued_bytes -= oldest.bytes;
                dropped += 1;
            }
        }
        state.stats.dropped += dropped;
        let due =
            state.queued_bytes >= self.shared.config.max_batch_bytes || state.queue.len() >= limits::MAX_BATCH_SIZE;
        drop(state);
        if dropped > 0 {
            self.shared.report(&format!("traumfaenger: queue full, dropped {dropped} oldest exchanges"));
        }
        if due {
            self.shared.wake.notify_one();
        }
    }

    pub fn stats(&self) -> Stats {
        let state = self.shared.lock();
        Stats { queued: state.queue.len() + state.in_flight_items, ..state.stats }
    }

    /// Sends everything queued and waits for the acks. Returns early when the
    /// connection fails or is backing off after a failure.
    pub async fn flush(&self) {
        let receiver = {
            let mut state = self.shared.lock();
            if state.idle() || state.backing_off || state.closed {
                return;
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.push(sender);
            state.flush_requested = true;
            receiver
        };
        self.shared.wake.notify_one();
        let _ = receiver.await;
    }

    /// Keeps sending until everything is acknowledged or `timeout` passed,
    /// then stops the task. Tries at least once even while backing off.
    pub async fn close(&self, timeout: Duration) {
        let receiver = {
            let mut state = self.shared.lock();
            if state.closed {
                return;
            }
            let (sender, receiver) = oneshot::channel();
            state.close_done.push(sender);
            state.closing.get_or_insert(Instant::now() + timeout);
            state.force = true;
            receiver
        };
        self.shared.wake.notify_one();
        let _ = receiver.await;
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.closing.get_or_insert(Instant::now() + Duration::from_secs(5));
        state.force = true;
        drop(state);
        self.shared.wake.notify_one();
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Connection {
    socket: Socket,
    encoding: Encoding,
    in_flight: VecDeque<InFlight>,
    last_activity: Instant,
}

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

struct Task {
    shared: Arc<Shared>,
    connection: Option<Connection>,
    backoff: Duration,
    retry_at: Instant,
    last_send: Instant,
    next_batch: u64,
}

/// Marks the sender closed and releases every waiter when the task ends,
/// also when it is cancelled with its runtime or panics.
struct Finished(Arc<Shared>);

impl Drop for Finished {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.closed = true;
        state.notify();
        for done in state.close_done.drain(..) {
            let _ = done.send(());
        }
    }
}

async fn run(shared: Arc<Shared>) {
    let _finished = Finished(shared.clone());
    let mut task = Task {
        shared,
        connection: None,
        backoff: Duration::ZERO,
        retry_at: Instant::now(),
        last_send: Instant::now(),
        next_batch: 1,
    };
    while !task.step().await {}
}

impl Task {
    fn config(&self) -> &SenderConfig {
        &self.shared.config
    }

    /// `limit`, shortened to the close deadline when closing.
    fn bounded(&self, limit: Duration) -> Duration {
        match self.shared.lock().closing {
            Some(deadline) => limit.min(deadline.saturating_duration_since(Instant::now())),
            None => limit,
        }
    }

    /// One round. Returns true when the task should stop.
    async fn step(&mut self) -> bool {
        let now = Instant::now();
        let (idle, due, force, closing) = {
            let mut state = self.shared.lock();
            let due = state.flush_requested
                || state.closing.is_some()
                || state.queued_bytes >= self.shared.config.max_batch_bytes
                || state.queue.len() >= limits::MAX_BATCH_SIZE
                || now.duration_since(self.last_send) >= self.shared.config.flush_interval;
            let force = std::mem::take(&mut state.force);
            (state.idle(), due, force, state.closing)
        };

        if let Some(deadline) = closing
            && (idle || now >= deadline)
        {
            self.shutdown().await;
            return true;
        }

        if self.connection.is_none() && !idle && due && (force || now >= self.retry_at) {
            self.connect().await;
        }
        if self.connection.is_some() && due {
            self.pump().await;
        }
        {
            let mut state = self.shared.lock();
            if state.idle() {
                state.flush_requested = false;
                state.notify();
            }
        }

        let tick = self.config().flush_interval.min(Duration::from_millis(250));
        let wake = self.shared.wake.notified();
        if let Some(connection) = &mut self.connection {
            tokio::select! {
                () = wake => {}
                message = connection.socket.next() => self.receive(message).await,
                () = tokio::time::sleep(tick) => self.watch().await,
            }
        } else {
            tokio::select! {
                () = wake => {}
                () = tokio::time::sleep(tick) => {}
            }
        }
        false
    }

    async fn connect(&mut self) {
        let config = self.config().clone();
        let limit = self.bounded(config.connect_timeout);
        let attempt = tokio::time::timeout(limit, open_socket(&config)).await;
        match attempt {
            Ok(Ok((socket, encoding))) => {
                self.shared.lock().backing_off = false;
                self.connection =
                    Some(Connection { socket, encoding, in_flight: VecDeque::new(), last_activity: Instant::now() });
            }
            Ok(Err(error)) => self.failed(&format!("traumfaenger: connecting failed: {error}")),
            Err(_) => self.failed(&format!("traumfaenger: connecting took longer than {} ms", limit.as_millis())),
        }
    }

    fn failed(&mut self, message: &str) {
        self.backoff = (self.backoff * 2).clamp(MIN_BACKOFF, MAX_BACKOFF);
        self.retry_at = Instant::now() + self.backoff;
        {
            let mut state = self.shared.lock();
            state.stats.failed_sends += 1;
            state.backing_off = true;
            state.flush_requested = false;
            state.notify();
        }
        self.shared.report(message);
    }

    /// Drops the connection; unacknowledged batches go back to the front of the queue.
    async fn lose(&mut self, reason: &str, failure: bool) {
        let Some(mut connection) = self.connection.take() else { return };
        let unacked: Vec<Pending> = connection.in_flight.drain(..).flat_map(|batch| batch.items).collect();
        let idle = {
            let mut state = self.shared.lock();
            state.in_flight_items = 0;
            state.requeue(unacked);
            state.idle()
        };
        let code = if failure { CloseCode::Library(4000) } else { CloseCode::Normal };
        let mut text = reason.to_owned();
        crate::text::clip(&mut text, 120);
        let limit = self.bounded(Duration::from_secs(1));
        let _ =
            tokio::time::timeout(limit, connection.socket.close(Some(CloseFrame { code, reason: text.into() }))).await;
        if failure && !idle {
            self.failed(&format!("traumfaenger: {reason}"));
        } else {
            self.shared.lock().notify();
        }
    }

    async fn pump(&mut self) {
        loop {
            let Some(connection) = self.connection.as_mut() else { return };
            if connection.in_flight.len() >= limits::MAX_IN_FLIGHT_BATCHES {
                return;
            }
            let items = {
                let mut state = self.shared.lock();
                let mut items = Vec::new();
                let mut bytes = 0;
                while let Some(next) = state.queue.front() {
                    if items.len() >= limits::MAX_BATCH_SIZE
                        || (!items.is_empty() && bytes + next.bytes + 1 > self.shared.config.max_batch_bytes)
                    {
                        break;
                    }
                    bytes += next.bytes;
                    items.extend(state.queue.pop_front());
                }
                state.queued_bytes -= bytes;
                state.in_flight_items += items.len();
                items
            };
            if items.is_empty() {
                return;
            }
            let id = self.next_batch;
            self.next_batch += 1;
            let message = encode(connection.encoding, id, &items);
            connection.in_flight.push_back(InFlight { id, items, sent_at: Instant::now() });
            connection.last_activity = Instant::now();
            self.last_send = Instant::now();
            // A server that stops reading must not freeze the task.
            let limit = self.bounded(self.shared.config.ack_timeout);
            let Some(connection) = self.connection.as_mut() else { return };
            match tokio::time::timeout(limit, connection.socket.send(message)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.lose(&format!("sending failed: {error}"), true).await;
                    return;
                }
                Err(_) => {
                    self.lose(&format!("sending took longer than {} ms", limit.as_millis()), true).await;
                    return;
                }
            }
        }
    }

    async fn receive(&mut self, message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>) {
        let decoded: Result<ServerMessage, String> = match message {
            Some(Ok(Message::Binary(bytes))) => rmp_serde::from_slice(&bytes).map_err(|e| e.to_string()),
            Some(Ok(Message::Text(text))) => serde_json::from_str(&text).map_err(|e| e.to_string()),
            Some(Ok(Message::Close(frame))) => {
                let why = frame.map(|frame| format!("{} {}", u16::from(frame.code), frame.reason)).unwrap_or_default();
                self.lose(&format!("ingestion connection closed ({why})"), true).await;
                return;
            }
            Some(Ok(_)) => return,
            Some(Err(error)) => {
                self.lose(&format!("ingestion connection failed: {error}"), true).await;
                return;
            }
            None => {
                self.lose("ingestion connection closed", true).await;
                return;
            }
        };
        let Some(connection) = self.connection.as_mut() else { return };
        connection.last_activity = Instant::now();
        let message = match decoded {
            Ok(message) => message,
            Err(error) => {
                self.shared.report(&format!("traumfaenger: unreadable server message: {error}"));
                return;
            }
        };
        // The server answers batches in order; an error without a batch id is for the oldest one.
        let batch_id = match &message {
            ServerMessage::Ack { batch, .. } => Some(*batch),
            ServerMessage::Error { batch, .. } => *batch,
        };
        let index = match batch_id {
            None => (!connection.in_flight.is_empty()).then_some(0),
            Some(id) => connection.in_flight.iter().position(|batch| batch.id == id),
        };
        let batch = index.and_then(|index| connection.in_flight.remove(index));
        if let Some(batch) = &batch {
            self.shared.lock().in_flight_items -= batch.items.len();
        }
        match message {
            ServerMessage::Ack { result, .. } => {
                self.backoff = Duration::ZERO;
                let mut state = self.shared.lock();
                state.stats.sent += u64::from(result.accepted) + u64::from(result.duplicates);
                state.stats.rejected += result.rejected.len() as u64;
                drop(state);
                if let Some(first) = result.rejected.first() {
                    self.shared.report(&format!(
                        "traumfaenger rejected {} exchanges (#{}: {})",
                        result.rejected.len(),
                        first.index,
                        first.reason
                    ));
                }
            }
            ServerMessage::Error { error: ErrorCode::Internal, message, .. } => {
                // Not stored; send it again after a backoff instead of in a tight loop.
                if let Some(batch) = batch {
                    self.shared.lock().requeue(batch.items);
                }
                self.lose(&format!("server error: {message}"), true).await;
                return;
            }
            ServerMessage::Error { error, message, .. } => {
                if let Some(batch) = &batch {
                    self.shared.lock().stats.dropped += batch.items.len() as u64;
                }
                self.shared.report(&format!("traumfaenger refused a batch: {error:?}: {message}"));
            }
        }
        self.pump().await;
    }

    /// Timeouts for acks, and closing an idle connection.
    async fn watch(&mut self) {
        let Some(connection) = &self.connection else { return };
        let now = Instant::now();
        let oldest = connection.in_flight.front().map(|batch| batch.sent_at);
        let quiet = now.duration_since(connection.last_activity);
        if oldest.is_some_and(|sent_at| now.duration_since(sent_at) > self.config().ack_timeout) {
            let timeout = self.config().ack_timeout.as_millis();
            self.lose(&format!("no ack within {timeout} ms"), true).await;
        } else if self.shared.lock().idle() && quiet > self.config().idle_timeout {
            self.lose("idle", false).await;
        }
    }

    async fn shutdown(&mut self) {
        self.lose("closed by the client", false).await;
        let mut state = self.shared.lock();
        state.closed = true;
        let left = state.queue.len() + state.in_flight_items;
        state.notify();
        for done in state.close_done.drain(..) {
            let _ = done.send(());
        }
        drop(state);
        if left > 0 {
            self.shared.report(&format!("traumfaenger: closed with {left} exchanges unsent"));
        }
    }
}

#[derive(Serialize)]
struct BatchMessage<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    id: u64,
    exchanges: Vec<&'a Exchange>,
}

fn encode(encoding: Encoding, id: u64, items: &[Pending]) -> Message {
    let message = BatchMessage { kind: "batch", id, exchanges: items.iter().map(|item| &item.exchange).collect() };
    match encoding {
        Encoding::Msgpack => Message::Binary(rmp_serde::to_vec_named(&message).unwrap_or_default().into()),
        Encoding::Json => Message::Text(serde_json::to_string(&message).unwrap_or_default().into()),
    }
}

async fn open_socket(config: &SenderConfig) -> Result<(Socket, Encoding), String> {
    let base = config.ingest_url.trim_end_matches('/');
    let base = base.strip_prefix("http").map_or_else(|| base.to_owned(), |rest| format!("ws{rest}"));
    let mut request = format!("{base}{}", routes::INGEST_WS).into_client_request().map_err(|e| e.to_string())?;
    let offered = match config.encoding {
        Encoding::Msgpack => format!("{}, {}", protocols::MSGPACK, protocols::JSON),
        Encoding::Json => protocols::JSON.to_owned(),
    };
    let headers = request.headers_mut();
    headers.insert("sec-websocket-protocol", HeaderValue::from_str(&offered).map_err(|e| e.to_string())?);
    if let Some(token) = &config.token {
        let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| e.to_string())?;
        headers.insert("authorization", value);
    }
    let (socket, response) = tokio_tungstenite::connect_async(request).await.map_err(|e| e.to_string())?;
    let chosen = response.headers().get("sec-websocket-protocol").and_then(|value| value.to_str().ok());
    let encoding = match chosen {
        Some(protocol) if protocol == protocols::MSGPACK => Encoding::Msgpack,
        Some(protocol) if protocol == protocols::JSON => Encoding::Json,
        other => return Err(format!("server chose unknown subprotocol {other:?}")),
    };
    Ok((socket, encoding))
}
