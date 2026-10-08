//! [rig](https://docs.rs/rig-core) integration of traumfaenger.
//!
//! [`Recorded`] wraps the HTTP client (and, for realtime models, the
//! WebSocket client) a rig provider sends through. Every request is recorded
//! into a traumfaenger [`Database`] exactly as it went over the wire: unary
//! calls as `http`, streamed replies as `sse` with one frame per event,
//! WebSocket sessions as `ws`. Recording never changes what rig sends or
//! receives; it only observes.
//!
//! ```no_run
//! use rig_core::providers::openai::OpenAIConfig;
//! use traumfaenger_client::{Database, DatabaseOptions, Operation, operation};
//! use traumfaenger_rig::Recorded;
//!
//! # async fn run(http: rig_core::http_client::DynHttpClient) -> Result<(), Box<dyn std::error::Error>> {
//! let database = Database::new(DatabaseOptions::new("http://traumfaenger:7600", "my-service"))?;
//! let openai = OpenAIConfig::from_env()?.connect(Recorded::new(http, database));
//! let model = openai.chat("gpt-5-mini");
//! let reply = operation(Operation::new("listing.parse").meta("listing_id", 123), model.call("Hi")).await?;
//! # let _ = reply;
//! # Ok(())
//! # }
//! ```
//!
//! The operation context lives in a tokio task-local: run the rig call
//! inside [`traumfaenger_client::operation`] in the same task.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::Stream;
use rig_core::{
    http_client::{
        self, BoxedStream, Error, HttpClientExt, LazyBody, MultipartForm, Request, Response, StreamingResponse,
    },
    wasm_compat::{WasmBoxedFuture, WasmCompatSend},
    ws_client::{BoxedWebSocketConnection, CloseFrame, ConnectOptions, Frame, WebSocketClientExt, WebSocketConnection},
};
use traumfaenger_client::{Database, Direction, EndReason, HttpRecording, SocketRecording, api::limits};

pub use traumfaenger_client;

/// A rig HTTP or WebSocket client whose traffic is recorded into a database.
#[derive(Clone)]
pub struct Recorded<C> {
    inner: C,
    database: Database,
}

impl<C> Recorded<C> {
    pub fn new(inner: C, database: Database) -> Self {
        Self { inner, database }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    pub fn database(&self) -> &Database {
        &self.database
    }
}

fn content_type<T>(response: &Response<T>) -> Option<String> {
    response.headers().get("content-type").and_then(|value| value.to_str().ok()).map(str::to_owned)
}

/// Records a failed send: a non-success response is a complete exchange
/// with its status and body; anything else is a transport error.
fn record_error(recording: Option<HttpRecording>, error: &Error) {
    let Some(mut recording) = recording else { return };
    match (error.non_success_status(), error.non_success_body()) {
        (Some(status), body) => {
            recording.status(status.as_u16());
            recording.response_body(body.unwrap_or("").as_bytes(), None);
            recording.finish(EndReason::Complete, None);
        }
        (None, _) => recording.finish(EndReason::Error, Some(&error.to_string())),
    }
}

/// Wraps a buffered body so it is recorded when rig reads it.
fn recorded_body<U>(
    body: LazyBody<Bytes>,
    recording: Option<HttpRecording>,
    content_type: Option<String>,
) -> LazyBody<U>
where
    U: From<Bytes> + WasmCompatSend + 'static,
{
    Box::pin(async move {
        let bytes = body.await;
        if let Some(mut recording) = recording {
            match &bytes {
                Ok(bytes) => {
                    recording.response_body(bytes, content_type.as_deref());
                    recording.finish(EndReason::Complete, None);
                }
                Err(error) => recording.finish(EndReason::Error, Some(&error.to_string())),
            }
        }
        bytes.map(U::from)
    })
}

impl<C: HttpClientExt> HttpClientExt for Recorded<C> {
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        T: Into<Bytes>,
        T: WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let req = req.map(Into::<Bytes>::into);
        let mut recording = self.database.record_http(&req.uri().to_string());
        if let Some(recording) = &mut recording {
            recording.request_body(req.body());
        }
        let sending = self.inner.send::<Bytes, Bytes>(req);
        async move {
            match sending.await {
                Ok(response) => {
                    if let Some(recording) = &mut recording {
                        recording.status(response.status().as_u16());
                    }
                    let content_type = content_type(&response);
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(parts, recorded_body(body, recording, content_type)))
                }
                Err(error) => {
                    record_error(recording, &error);
                    Err(error)
                }
            }
        }
    }

    fn send_multipart<U>(
        &self,
        req: Request<MultipartForm>,
    ) -> impl Future<Output = http_client::Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let mut recording = self.database.record_http(&req.uri().to_string());
        if let Some(recording) = &mut recording {
            recording.request_placeholder("multipart form");
        }
        let sending = self.inner.send_multipart::<Bytes>(req);
        async move {
            match sending.await {
                Ok(response) => {
                    if let Some(recording) = &mut recording {
                        recording.status(response.status().as_u16());
                    }
                    let content_type = content_type(&response);
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(parts, recorded_body(body, recording, content_type)))
                }
                Err(error) => {
                    record_error(recording, &error);
                    Err(error)
                }
            }
        }
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes> + WasmCompatSend,
    {
        let req = req.map(Into::<Bytes>::into);
        let mut recording = self.database.record_http(&req.uri().to_string());
        if let Some(recording) = &mut recording {
            recording.request_body(req.body());
        }
        let sending = self.inner.send_streaming(req);
        async move {
            match sending.await {
                Ok(response) => {
                    if let Some(recording) = &mut recording {
                        recording.status(response.status().as_u16());
                    }
                    let content_type = content_type(&response);
                    let sse = content_type.as_deref().is_some_and(|value| value.contains("text/event-stream"));
                    if sse && let Some(recording) = &mut recording {
                        recording.streaming();
                    }
                    let (parts, body) = response.into_parts();
                    let stream: BoxedStream = Box::pin(RecordedStream {
                        inner: body,
                        recording,
                        sse,
                        content_type,
                        buffered: Vec::new(),
                        clipped: false,
                    });
                    Ok(Response::from_parts(parts, stream))
                }
                Err(error) => {
                    record_error(recording, &error);
                    Err(error)
                }
            }
        }
    }
}

/// Passes a streamed body through chunk by chunk and records it: SSE
/// replies event by event, anything else as one buffered body. Dropped
/// before its end, the recording ends as cancelled.
struct RecordedStream {
    inner: BoxedStream,
    recording: Option<HttpRecording>,
    sse: bool,
    content_type: Option<String>,
    buffered: Vec<u8>,
    clipped: bool,
}

impl RecordedStream {
    fn finish(&mut self, reason: EndReason, error: Option<&str>) {
        let Some(mut recording) = self.recording.take() else { return };
        if !self.sse {
            recording.response_body(&std::mem::take(&mut self.buffered), self.content_type.as_deref());
        }
        if self.clipped {
            recording.mark_truncated();
        }
        recording.finish(reason, error);
    }
}

impl Stream for RecordedStream {
    type Item = http_client::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let item = this.inner.as_mut().poll_next(cx);
        match &item {
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(recording) = &mut this.recording {
                    if this.sse {
                        recording.stream_chunk(chunk);
                    } else if !this.clipped {
                        if this.buffered.len() + chunk.len() > limits::MAX_RECORD_BYTES {
                            this.clipped = true;
                        } else {
                            this.buffered.extend_from_slice(chunk);
                        }
                    }
                }
            }
            Poll::Ready(Some(Err(error))) => {
                let message = error.to_string();
                this.finish(EndReason::Error, Some(&message));
            }
            Poll::Ready(None) => this.finish(EndReason::Complete, None),
            Poll::Pending => {}
        }
        item
    }
}

impl<C: WebSocketClientExt> WebSocketClientExt for Recorded<C> {
    fn connect(
        &self,
        request: Request<http_client::NoBody>,
        options: ConnectOptions,
    ) -> impl Future<Output = http_client::Result<BoxedWebSocketConnection>> + WasmCompatSend {
        let recording = self.database.record_socket(&request.uri().to_string());
        let connecting = self.inner.connect(request, options);
        async move {
            match connecting.await {
                Ok(connection) => {
                    let Some(mut recording) = recording else { return Ok(connection) };
                    recording.opened();
                    let connection: BoxedWebSocketConnection =
                        Box::new(RecordedConnection { inner: connection, recording: Some(recording) });
                    Ok(connection)
                }
                Err(error) => {
                    if let Some(recording) = recording {
                        recording.close(None, Some(&error.to_string()));
                    }
                    Err(error)
                }
            }
        }
    }
}

/// A WebSocket connection whose frames are recorded, in both directions.
struct RecordedConnection {
    inner: BoxedWebSocketConnection,
    recording: Option<SocketRecording>,
}

impl RecordedConnection {
    fn record(&mut self, dir: Direction, frame: &Frame) {
        let Some(recording) = &mut self.recording else { return };
        match frame {
            Frame::Text(text) => recording.text(dir, text),
            Frame::Binary(bytes) => recording.binary(dir, bytes.len()),
            Frame::Close(close) => {
                let code = close.as_ref().map(|close| close.code);
                if let Some(recording) = self.recording.take() {
                    recording.close(code, None);
                }
            }
            Frame::Ping(_) | Frame::Pong(_) => {}
        }
    }
}

impl WebSocketConnection for RecordedConnection {
    fn send(&mut self, frame: Frame) -> WasmBoxedFuture<'_, http_client::Result<()>> {
        Box::pin(async move {
            self.record(Direction::Out, &frame);
            self.inner.send(frame).await
        })
    }

    fn recv(&mut self) -> WasmBoxedFuture<'_, http_client::Result<Option<Frame>>> {
        Box::pin(async move {
            let received = self.inner.recv().await;
            match &received {
                Ok(Some(frame)) => self.record(Direction::In, frame),
                Ok(None) => {
                    if let Some(recording) = self.recording.take() {
                        recording.close(None, None);
                    }
                }
                Err(error) => {
                    if let Some(recording) = self.recording.take() {
                        recording.close(None, Some(&error.to_string()));
                    }
                }
            }
            received
        })
    }

    fn close(&mut self, frame: Option<CloseFrame>) -> WasmBoxedFuture<'_, http_client::Result<()>> {
        Box::pin(async move {
            if let Some(recording) = self.recording.take() {
                recording.close(frame.as_ref().map(|frame| frame.code), None);
            }
            self.inner.close(frame).await
        })
    }
}
