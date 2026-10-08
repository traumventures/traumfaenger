//! Turning what goes over the wire into canonical [`Exchange`] records.
//!
//! Transport adapters (such as the rig integration) open a recording when a
//! request starts, feed it what they send and receive, and finish it. A
//! recording that is dropped unfinished records itself as cancelled, so a
//! caller dropping a stream is still recorded.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use traumfaenger_api::{Direction, EndReason, Exchange, ExchangeEnd, Frame, Transport, limits};

use crate::{
    database::Database,
    text::{SseSplitter, binary_placeholder, body_text, clip, describe, prepare_data},
};

/// Context captured when a recording starts.
#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub service: String,
    pub operation: String,
    pub session_id: Option<String>,
    pub metadata: std::collections::BTreeMap<String, String>,
    /// Context had to be shortened or partly dropped to fit the limits.
    pub clipped: bool,
}

/// Collects the frames of one record within the size limits.
#[derive(Default)]
struct Frames {
    frames: Vec<Frame>,
    bytes: usize,
    truncated: bool,
}

impl Frames {
    fn add(&mut self, t_ms: u64, dir: Direction, data: &str) {
        if self.frames.len() >= limits::MAX_FRAMES {
            self.truncated = true;
            return;
        }
        let mut prepared = prepare_data(data);
        let room = limits::MAX_FRAME_BYTES.min(limits::MAX_RECORD_BYTES.saturating_sub(self.bytes));
        if clip(&mut prepared, room) {
            self.truncated = true;
            if prepared.is_empty() {
                return;
            }
        }
        self.bytes += prepared.len();
        self.frames.push(Frame { t_ms, dir, data: prepared });
    }
}

/// The events of a stream within the record limits. When a stream is too
/// long, the middle is dropped: the start and the end (where usage and the
/// finish reason arrive) are kept, with one placeholder frame in between.
#[derive(Default)]
struct StreamEvents {
    head: Vec<(u64, String)>,
    head_bytes: usize,
    tail: std::collections::VecDeque<(u64, String)>,
    tail_bytes: usize,
    dropped_events: usize,
    dropped_bytes: usize,
    dropped_at: u64,
}

impl StreamEvents {
    /// Leaves room for the request frame (at most `MAX_FRAME_BYTES`) and the placeholder.
    const HALF_BYTES: usize = (limits::MAX_RECORD_BYTES - limits::MAX_FRAME_BYTES) / 2 - 1024;
    const HALF_FRAMES: usize = (limits::MAX_FRAMES - 2) / 2;

    fn push(&mut self, t: u64, data: String) {
        if self.tail.is_empty()
            && self.dropped_events == 0
            && self.head.len() < Self::HALF_FRAMES
            && self.head_bytes + data.len() <= Self::HALF_BYTES
        {
            self.head_bytes += data.len();
            self.head.push((t, data));
            return;
        }
        let mut data = data;
        clip(&mut data, Self::HALF_BYTES);
        self.tail_bytes += data.len();
        self.tail.push_back((t, data));
        while self.tail.len() > 1 && (self.tail.len() > Self::HALF_FRAMES || self.tail_bytes > Self::HALF_BYTES) {
            let Some((t, dropped)) = self.tail.pop_front() else { break };
            self.tail_bytes -= dropped.len();
            self.dropped_events += 1;
            self.dropped_bytes += dropped.len();
            self.dropped_at = t;
        }
    }

    fn drain_into(&mut self, frames: &mut Frames) {
        for (t, data) in self.head.drain(..) {
            frames.add(t, Direction::In, &data);
        }
        if self.dropped_events > 0 {
            let marker =
                format!("[traumfaenger: omitted {} events, {} bytes]", self.dropped_events, self.dropped_bytes);
            frames.add(self.dropped_at, Direction::In, &marker);
            frames.truncated = true;
        }
        for (t, data) in self.tail.drain(..) {
            frames.add(t, Direction::In, &data);
        }
    }
}

fn end(at_ms: u64, reason: EndReason, error: Option<&str>, close_code: Option<u16>) -> ExchangeEnd {
    ExchangeEnd { at_ms: at_ms.min(limits::MAX_DURATION_MS), reason, error: error.map(describe), close_code }
}

/// What identifies one record apart from its frames and end.
struct Head<'a> {
    id: String,
    segment: u32,
    transport: Transport,
    url: &'a str,
    started_at: DateTime<Utc>,
    status: Option<u16>,
}

fn exchange(context: &Context, head: Head<'_>, frames: Frames, end: Option<ExchangeEnd>) -> Exchange {
    let Head { id, segment, transport, url, started_at, status } = head;
    Exchange {
        id,
        segment,
        is_final: end.is_some(),
        service: context.service.clone(),
        operation: context.operation.clone(),
        session_id: context.session_id.clone(),
        metadata: (!context.metadata.is_empty()).then(|| context.metadata.clone()),
        transport,
        url: url.to_owned(),
        started_at,
        status,
        truncated: context.clipped || frames.truncated,
        frames: frames.frames,
        end,
    }
}

/// One HTTP request and its response, buffered or streamed (SSE).
///
/// Open it with [`Database::record_http`]. Feed it the request body, the
/// response status and either the whole body or the stream's chunks, then
/// [`finish`](Self::finish) it.
pub struct HttpRecording {
    database: Database,
    context: Context,
    url: String,
    started_at: DateTime<Utc>,
    start: Instant,
    request: Option<String>,
    status: Option<u16>,
    body: Option<(u64, String)>,
    stream: Option<(SseSplitter, StreamEvents)>,
    truncated: bool,
    finished: bool,
}

impl HttpRecording {
    pub(crate) fn new(database: Database, context: Context, url: String) -> Self {
        Self {
            database,
            context,
            url,
            started_at: Utc::now(),
            start: Instant::now(),
            request: None,
            status: None,
            body: None,
            stream: None,
            truncated: false,
            finished: false,
        }
    }

    /// The response is a stream (SSE), even if no chunk arrives.
    pub fn streaming(&mut self) {
        self.stream.get_or_insert_with(Default::default);
    }

    /// The adapter dropped part of the response, for example beyond the size limits.
    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn elapsed(&self) -> u64 {
        self.start.elapsed().as_millis().min(u128::from(limits::MAX_DURATION_MS)) as u64
    }

    /// The request body as sent.
    pub fn request_body(&mut self, body: &[u8]) {
        self.request = Some(String::from_utf8_lossy(body).into_owned());
    }

    /// A request body that is not recorded, for example a multipart form.
    pub fn request_placeholder(&mut self, kind: &str) {
        self.request = Some(binary_placeholder(kind, None));
    }

    pub fn status(&mut self, status: u16) {
        self.status = Some(status);
    }

    /// The complete body of a buffered response.
    pub fn response_body(&mut self, body: &[u8], content_type: Option<&str>) {
        let at = self.elapsed();
        self.body = Some((at, body_text(body, content_type)));
    }

    /// One chunk of a streamed (SSE) response; events are stamped when they complete.
    pub fn stream_chunk(&mut self, chunk: &[u8]) {
        let at = self.elapsed();
        let (splitter, events) = self.stream.get_or_insert_with(Default::default);
        for event in splitter.push_bytes(chunk) {
            events.push(at, event);
        }
    }

    /// Ends the recording and hands the exchange to the database.
    pub fn finish(mut self, reason: EndReason, error: Option<&str>) {
        self.complete(reason, error);
    }

    fn complete(&mut self, reason: EndReason, error: Option<&str>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let at = self.elapsed();
        let mut frames = Frames { truncated: self.truncated, ..Frames::default() };
        frames.add(0, Direction::Out, self.request.as_deref().unwrap_or(""));
        let transport = if let Some((mut splitter, mut events)) = self.stream.take() {
            for event in splitter.end() {
                events.push(at, event);
            }
            events.drain_into(&mut frames);
            Transport::Sse
        } else {
            if let Some((t, body)) = self.body.take() {
                frames.add(t, Direction::In, &body);
            }
            Transport::Http
        };
        let record = exchange(
            &self.context,
            Head {
                id: uuid::Uuid::new_v4().to_string(),
                segment: 0,
                transport,
                url: &self.url,
                started_at: self.started_at,
                status: self.status,
            },
            frames,
            Some(end(at, reason, error, None)),
        );
        self.database.push(record);
    }
}

impl Drop for HttpRecording {
    fn drop(&mut self) {
        self.complete(EndReason::Cancelled, Some("the reader stopped before the end of the response"));
    }
}

/// Settings of WebSocket recordings.
#[derive(Clone, Copy, Debug)]
pub struct SegmentOptions {
    /// A session is sent in segments at least this often (checked when frames arrive).
    pub interval: Duration,
    /// ...or once a segment has this many frames.
    pub max_frames: usize,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self { interval: Duration::from_secs(10), max_frames: 500 }
    }
}

/// A WebSocket session with a provider, sent in segments while it runs.
///
/// Open it with [`Database::record_socket`], call [`opened`](Self::opened)
/// once the handshake succeeded, feed every message in both directions and
/// [`close`](Self::close) it. Dropped unclosed, it records itself as closed.
/// The database also cuts a segment when a session has been quiet for the
/// segment interval, and on [`Database::close`].
pub struct SocketRecording {
    database: Database,
    state: Arc<Mutex<SocketState>>,
}

/// The part of a socket recording the database can reach to cut segments.
pub(crate) struct SocketState {
    context: Context,
    id: String,
    url: String,
    started_at: DateTime<Utc>,
    start: Instant,
    options: SegmentOptions,
    segment: u32,
    segment_started: Instant,
    frames: Frames,
    status: Option<u16>,
    finished: bool,
}

impl SocketState {
    fn elapsed(&self) -> u64 {
        self.start.elapsed().as_millis().min(u128::from(limits::MAX_DURATION_MS)) as u64
    }

    fn record(&mut self, frames: Frames, end: Option<ExchangeEnd>) -> Exchange {
        exchange(
            &self.context,
            Head {
                id: self.id.clone(),
                segment: self.segment,
                transport: Transport::Ws,
                url: &self.url,
                started_at: self.started_at,
                status: self.status,
            },
            frames,
            end,
        )
    }

    /// The frames collected so far as a non-final segment, if any.
    pub(crate) fn take_segment(&mut self) -> Option<Exchange> {
        if self.finished || self.frames.frames.is_empty() || self.segment >= limits::MAX_SEGMENT {
            return None;
        }
        let frames = std::mem::take(&mut self.frames);
        let record = self.record(frames, None);
        self.segment += 1;
        self.segment_started = Instant::now();
        Some(record)
    }

    /// A segment when this one is older than the segment interval.
    pub(crate) fn take_due_segment(&mut self) -> Option<Exchange> {
        if self.segment_started.elapsed() >= self.options.interval { self.take_segment() } else { None }
    }

    fn take_final(&mut self, close_code: Option<u16>, error: Option<&str>) -> Option<Exchange> {
        if self.finished {
            return None;
        }
        let reason = if error.is_some() { EndReason::Error } else { EndReason::Closed };
        let at = self.elapsed();
        let frames = std::mem::take(&mut self.frames);
        let record = self.record(frames, Some(end(at, reason, error, close_code)));
        self.finished = true;
        Some(record)
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished
    }
}

pub(crate) fn lock(state: &Mutex<SocketState>) -> std::sync::MutexGuard<'_, SocketState> {
    state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SocketRecording {
    pub(crate) fn new(database: Database, context: Context, url: String, options: SegmentOptions) -> Self {
        let state = SocketState {
            context,
            id: uuid::Uuid::new_v4().to_string(),
            url,
            started_at: Utc::now(),
            start: Instant::now(),
            options: SegmentOptions { max_frames: options.max_frames.clamp(1, limits::MAX_FRAMES), ..options },
            segment: 0,
            segment_started: Instant::now(),
            frames: Frames::default(),
            status: None,
            finished: false,
        };
        let state = Arc::new(Mutex::new(state));
        database.register_socket(&state);
        Self { database, state }
    }

    /// The handshake succeeded (status 101).
    pub fn opened(&mut self) {
        lock(&self.state).status = Some(101);
    }

    /// A text message.
    pub fn text(&mut self, dir: Direction, text: &str) {
        self.add(dir, text);
    }

    /// A binary message; only its size is recorded.
    pub fn binary(&mut self, dir: Direction, len: usize) {
        self.add(dir, &binary_placeholder("binary message", Some(len)));
    }

    fn add(&mut self, dir: Direction, data: &str) {
        let mut records = Vec::new();
        {
            let mut state = lock(&self.state);
            if state.finished {
                return;
            }
            if state.start.elapsed().as_millis() >= u128::from(limits::MAX_DURATION_MS) {
                let error = format!("recording stopped after {} ms", limits::MAX_DURATION_MS);
                records.extend(state.take_final(None, Some(&error)));
            } else {
                // Start a new segment instead of clipping when this one is full or old.
                let full = state.frames.frames.len() >= state.options.max_frames
                    || state.frames.bytes + data.len() > limits::MAX_RECORD_BYTES
                    || state.segment_started.elapsed() >= state.options.interval;
                if full {
                    records.extend(state.take_segment());
                }
                let at = state.elapsed();
                state.frames.add(at, dir, data);
            }
        }
        for record in records {
            self.database.push(record);
        }
    }

    /// Sends the frames collected so far as a non-final segment.
    pub fn flush_segment(&mut self) {
        let record = lock(&self.state).take_segment();
        if let Some(record) = record {
            self.database.push(record);
        }
    }

    /// The session ended. `error` marks an abnormal end.
    pub fn close(mut self, close_code: Option<u16>, error: Option<&str>) {
        self.complete(close_code, error);
    }

    fn complete(&mut self, close_code: Option<u16>, error: Option<&str>) {
        let record = lock(&self.state).take_final(close_code, error);
        if let Some(record) = record {
            self.database.push(record);
        }
    }
}

impl Drop for SocketRecording {
    fn drop(&mut self) {
        self.complete(None, None);
    }
}
