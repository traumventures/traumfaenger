//! The database handle: records exchanges and sends them in the background.

use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use traumfaenger_api::{Exchange, limits};

use crate::{
    context,
    recording::{self, Context, HttpRecording, SegmentOptions, SocketRecording, SocketState},
    sender::{Encoding, ErrorHandler, Sender, SenderConfig, Stats},
    text::{clean_line, clean_metadata, clean_url},
};

/// Operation name of exchanges recorded outside [`context::operation`].
pub const UNATTRIBUTED: &str = "unattributed";

/// Settings of a [`Database`]. Build with [`DatabaseOptions::new`].
#[derive(Clone)]
pub struct DatabaseOptions {
    /// Base URL of the ingestion listener, for example `http://traumfaenger:7600`.
    pub ingest_url: String,
    /// Bearer token of the ingestion API.
    pub token: Option<String>,
    /// Name of this service, for example `my-service`. Use one database per environment.
    pub service: String,
    pub encoding: Encoding,
    /// How often queued exchanges are sent. Defaults to 2 seconds.
    pub flush_interval: Duration,
    /// Send early once this many bytes are queued. Defaults to 4 MiB.
    pub max_batch_bytes: usize,
    /// Oldest exchanges are dropped beyond this many queued bytes. Defaults to 64 MiB.
    pub max_queue_bytes: usize,
    /// Close the ingestion connection after this long without traffic. Defaults to 30 seconds.
    pub idle_timeout: Duration,
    /// Give up on connecting after this long. Defaults to 10 seconds.
    pub connect_timeout: Duration,
    /// Reconnect when a batch has no ack after this long. Defaults to 30 seconds.
    pub ack_timeout: Duration,
    /// How recorded WebSocket sessions are split into segments.
    pub segments: SegmentOptions,
    /// Called when sending fails or exchanges are rejected or dropped. Defaults to `tracing::warn!`.
    pub on_error: ErrorHandler,
}

impl DatabaseOptions {
    pub fn new(ingest_url: impl Into<String>, service: impl Into<String>) -> Self {
        Self {
            ingest_url: ingest_url.into(),
            token: None,
            service: service.into(),
            encoding: Encoding::default(),
            flush_interval: Duration::from_secs(2),
            max_batch_bytes: 4 * 1024 * 1024,
            max_queue_bytes: 64 * 1024 * 1024,
            idle_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            ack_timeout: Duration::from_secs(30),
            segments: SegmentOptions::default(),
            on_error: Arc::new(|message| tracing::warn!(target: "traumfaenger", "{message}")),
        }
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn on_error(mut self, handler: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_error = Arc::new(handler);
        self
    }
}

/// Error building a [`Database`].
#[derive(Debug)]
pub struct InvalidOptions(String);

impl std::fmt::Display for InvalidOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidOptions {}

struct Inner {
    service: String,
    segments: SegmentOptions,
    sender: Sender,
    /// Open socket recordings, to cut their segments when they are quiet and on close.
    sockets: Mutex<Vec<Weak<Mutex<SocketState>>>>,
}

impl Inner {
    /// Pushes the open segment of every recorded socket (`all`) or only of
    /// the ones whose segment interval passed.
    fn cut_segments(&self, all: bool) {
        let sockets: Vec<Arc<Mutex<SocketState>>> = {
            let mut sockets = self.sockets.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            sockets.retain(|socket| socket.upgrade().is_some_and(|state| !recording::lock(&state).finished()));
            sockets.iter().filter_map(Weak::upgrade).collect()
        };
        for socket in sockets {
            let record = {
                let mut state = recording::lock(&socket);
                if all { state.take_segment() } else { state.take_due_segment() }
            };
            if let Some(record) = record {
                self.sender.push(record);
            }
        }
    }
}

/// Every second, cuts the segments of sessions that were quiet for the
/// segment interval. Stops when the database is dropped.
async fn cut_quiet_segments(inner: Weak<Inner>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(inner) = inner.upgrade() else { return };
        inner.cut_segments(false);
    }
}

/// A connection to one traumfaenger database. Cheap to clone; every clone
/// shares one ingestion connection. Must be created inside a tokio runtime.
///
/// Recording never changes or delays the calls being recorded: exchanges are
/// queued and sent by a background task.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

impl Database {
    /// Fails on invalid options, or when called outside a tokio runtime.
    pub fn new(options: DatabaseOptions) -> Result<Self, InvalidOptions> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| InvalidOptions("a Database must be created inside a tokio runtime".into()))?;
        let (service, changed) = clean_line(&options.service);
        let Some(service) = service.filter(|_| !changed) else {
            return Err(InvalidOptions(format!(
                "`service` must be a non-empty single line of at most {} bytes",
                limits::MAX_NAME_LEN
            )));
        };
        let sender = Sender::start(
            SenderConfig {
                ingest_url: options.ingest_url,
                token: options.token,
                encoding: options.encoding,
                flush_interval: options.flush_interval,
                max_batch_bytes: options.max_batch_bytes.min(limits::MAX_BATCH_BYTES - 1024),
                max_queue_bytes: options.max_queue_bytes,
                idle_timeout: options.idle_timeout,
                connect_timeout: options.connect_timeout,
                ack_timeout: options.ack_timeout,
                on_error: options.on_error,
            },
            &runtime,
        );
        let inner = Arc::new(Inner { service, segments: options.segments, sender, sockets: Mutex::default() });
        runtime.spawn(cut_quiet_segments(Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    pub(crate) fn register_socket(&self, state: &Arc<Mutex<SocketState>>) {
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        sockets.retain(|socket| socket.strong_count() > 0);
        sockets.push(Arc::downgrade(state));
    }

    /// The context of the current [`context::operation`], cleaned to the limits.
    pub(crate) fn context(&self) -> Context {
        let current = context::current().unwrap_or_default();
        let (operation, operation_changed) = clean_line(&current.operation);
        let (session_id, session_changed) = match &current.session_id {
            Some(session_id) => clean_line(session_id),
            None => (None, false),
        };
        let (metadata, metadata_changed) = clean_metadata(&current.metadata);
        Context {
            service: self.inner.service.clone(),
            operation: operation.unwrap_or_else(|| UNATTRIBUTED.to_owned()),
            session_id,
            metadata,
            clipped: operation_changed || session_changed || metadata_changed,
        }
    }

    /// Starts recording an HTTP or SSE exchange with `url`. `None` when the
    /// URL is not an absolute http(s) URL.
    pub fn record_http(&self, url: &str) -> Option<HttpRecording> {
        let url = clean_url(url).filter(|url| url.starts_with("http"))?;
        Some(HttpRecording::new(self.clone(), self.context(), url))
    }

    /// Starts recording a WebSocket session with `url`. `None` when the URL is
    /// not an absolute ws(s) or http(s) URL.
    pub fn record_socket(&self, url: &str) -> Option<SocketRecording> {
        let url = clean_url(url)?;
        Some(SocketRecording::new(self.clone(), self.context(), url, self.inner.segments))
    }

    /// Queues a complete exchange, for adapters that build records themselves.
    pub fn push(&self, exchange: Exchange) {
        self.inner.sender.push(exchange);
    }

    /// Sends everything queued and waits for the acks, unless the connection is failing.
    pub async fn flush(&self) {
        self.inner.sender.flush().await;
    }

    /// Sends the open part of recorded sockets, then keeps sending until
    /// everything is acknowledged or `timeout` passed, then stops. Exchanges
    /// recorded afterwards are dropped and counted. Await it before the tokio
    /// runtime shuts down; a runtime that ends first loses what is queued.
    pub async fn close(&self, timeout: Duration) {
        self.inner.cut_segments(true);
        self.inner.sender.close(timeout).await;
    }

    pub fn stats(&self) -> Stats {
        self.inner.sender.stats()
    }
}
