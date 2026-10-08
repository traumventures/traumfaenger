//! Decoding and storing ingestion batches, shared by the WebSocket and the
//! HTTP path.
//!
//! Records are decoded one by one, so a malformed record (wrong shape,
//! invalid escape, invalid UTF-8) is rejected on its own instead of failing
//! the whole batch.

use chrono::Utc;
use serde::Deserialize;
use traumfaenger_api::{ErrorCode, Exchange, IngestResult, RejectedExchange, ServerMessage, limits, protocols};

use crate::store::{Store, StoreError};

/// Encoding negotiated as the WebSocket subprotocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    Json,
    Msgpack,
}

impl Encoding {
    /// Picks the encoding from the offered subprotocols; MessagePack wins.
    pub fn negotiate<'a>(offered: impl Iterator<Item = &'a str> + Clone) -> Option<Self> {
        if offered.clone().any(|protocol| protocol.trim() == protocols::MSGPACK) {
            Some(Self::Msgpack)
        } else if offered.into_iter().any(|protocol| protocol.trim() == protocols::JSON) {
            Some(Self::Json)
        } else {
            None
        }
    }

    pub fn protocol(self) -> &'static str {
        match self {
            Self::Json => protocols::JSON,
            Self::Msgpack => protocols::MSGPACK,
        }
    }

    /// Serialises a server message. JSON goes in text messages, MessagePack in binary ones.
    pub fn encode(self, message: &ServerMessage) -> Vec<u8> {
        match self {
            Self::Json => serde_json::to_vec(message).unwrap_or_default(),
            Self::Msgpack => rmp_serde::to_vec_named(message).unwrap_or_default(),
        }
    }
}

/// A batch whose envelope decoded; its records are decoded individually.
pub struct DecodedBatch {
    pub id: u64,
    pub records: Vec<Result<Exchange, String>>,
}

/// Why a whole message could not be used.
#[derive(Debug)]
pub struct BatchError {
    pub batch: Option<u64>,
    pub code: ErrorCode,
    pub message: String,
}

impl BatchError {
    fn invalid(batch: Option<u64>, message: impl Into<String>) -> Self {
        Self { batch, code: ErrorCode::InvalidRequest, message: message.into() }
    }

    pub fn into_message(self) -> ServerMessage {
        ServerMessage::Error { batch: self.batch, error: self.code, message: self.message }
    }
}

#[derive(Deserialize)]
struct JsonBatch {
    #[serde(rename = "type")]
    kind: String,
    id: u64,
    exchanges: Vec<Box<serde_json::value::RawValue>>,
}

#[derive(Deserialize)]
struct MsgpackBatch {
    #[serde(rename = "type")]
    kind: String,
    id: u64,
    exchanges: Vec<rmpv::Value>,
}

/// Only the id, to address an error to a batch whose body did not decode.
#[derive(Deserialize)]
struct BatchId {
    id: u64,
}

/// Decodes one socket message in the negotiated encoding.
pub fn decode(encoding: Encoding, bytes: &[u8]) -> Result<DecodedBatch, BatchError> {
    let (kind, id, records) = match encoding {
        Encoding::Json => {
            let batch: JsonBatch = serde_json::from_slice(bytes).map_err(|error| {
                BatchError::invalid(serde_json::from_slice::<BatchId>(bytes).ok().map(|b| b.id), error.to_string())
            })?;
            let records =
                batch.exchanges.iter().map(|raw| serde_json::from_str(raw.get()).map_err(|e| e.to_string())).collect();
            (batch.kind, batch.id, records)
        }
        Encoding::Msgpack => {
            let batch: MsgpackBatch = rmp_serde::from_slice(bytes).map_err(|error| {
                BatchError::invalid(rmp_serde::from_slice::<BatchId>(bytes).ok().map(|b| b.id), error.to_string())
            })?;
            // Each record is re-encoded and decoded with rmp_serde, which (unlike
            // rmpv's deserializer) reads enums written as strings.
            let records = batch
                .exchanges
                .iter()
                .map(|value| {
                    let mut bytes = Vec::new();
                    rmpv::encode::write_value(&mut bytes, value).map_err(|e| e.to_string())?;
                    rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())
                })
                .collect();
            (batch.kind, batch.id, records)
        }
    };
    if kind != "batch" {
        return Err(BatchError::invalid(Some(id), format!("unknown message type `{kind}`")));
    }
    Ok(DecodedBatch { id, records })
}

/// Decodes the body of `POST /v1/exchanges`.
pub fn decode_http(bytes: &[u8]) -> Result<Vec<Result<Exchange, String>>, BatchError> {
    #[derive(Deserialize)]
    struct Body {
        exchanges: Vec<Box<serde_json::value::RawValue>>,
    }
    let body: Body = serde_json::from_slice(bytes).map_err(|error| BatchError::invalid(None, error.to_string()))?;
    Ok(body.exchanges.iter().map(|raw| serde_json::from_str(raw.get()).map_err(|e| e.to_string())).collect())
}

/// Validates the records and stores the valid ones.
pub fn store(
    store: &Store,
    batch: Option<u64>,
    records: Vec<Result<Exchange, String>>,
) -> Result<IngestResult, BatchError> {
    if records.is_empty() || records.len() > limits::MAX_BATCH_SIZE {
        return Err(BatchError::invalid(
            batch,
            format!("a batch must contain 1 to {} exchanges", limits::MAX_BATCH_SIZE),
        ));
    }
    let mut valid = Vec::with_capacity(records.len());
    let mut rejected = Vec::new();
    for (index, record) in records.into_iter().enumerate() {
        match record.and_then(|record| record.validate().map(|()| record)) {
            Ok(record) => valid.push(record),
            Err(reason) => rejected.push(RejectedExchange { index: index as u32, reason }),
        }
    }
    if let Some(first) = rejected.first() {
        tracing::warn!(rejected = rejected.len(), first = %first.reason, "rejected exchanges");
    }
    let inserted = if valid.is_empty() {
        Default::default()
    } else {
        store.insert(&valid, Utc::now()).map_err(|error| {
            match error {
                StoreError::Invalid(message) => return BatchError::invalid(batch, message),
                other => tracing::error!(error = %other, "storing a batch failed"),
            }
            BatchError { batch, code: ErrorCode::Internal, message: "internal error".into() }
        })?
    };
    Ok(IngestResult { accepted: inserted.accepted as u32, duplicates: inserted.duplicates as u32, rejected })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exchange_json() -> serde_json::Value {
        serde_json::json!({
            "id": "x", "service": "s", "operation": "o", "transport": "http",
            "url": "https://x/y", "started_at": "2026-10-08T10:00:00Z", "status": 200,
            "frames": [{ "t_ms": 0, "dir": "out", "data": "{}" }],
            "end": { "at_ms": 5, "reason": "complete" },
        })
    }

    #[test]
    fn negotiates_msgpack_first() {
        assert_eq!(Encoding::negotiate([protocols::JSON, protocols::MSGPACK].into_iter()), Some(Encoding::Msgpack));
        assert_eq!(Encoding::negotiate([protocols::JSON].into_iter()), Some(Encoding::Json));
        assert_eq!(Encoding::negotiate(["chat"].into_iter()), None);
    }

    #[test]
    fn decodes_both_encodings_record_by_record() {
        let batch = serde_json::json!({ "type": "batch", "id": 3, "exchanges": [exchange_json(), { "id": 1 }] });
        let json = decode(Encoding::Json, &serde_json::to_vec(&batch).unwrap()).unwrap();
        let msgpack = decode(Encoding::Msgpack, &rmp_serde::to_vec_named(&batch).unwrap()).unwrap();
        for decoded in [json, msgpack] {
            assert_eq!(decoded.id, 3);
            assert_eq!(decoded.records[0].as_ref().unwrap().id, "x");
            assert!(decoded.records[1].is_err());
        }
    }

    #[test]
    fn reports_the_batch_id_when_the_envelope_is_unusable() {
        let wrong_type = serde_json::json!({ "type": "hello", "id": 9, "exchanges": [] });
        let error = decode(Encoding::Json, &serde_json::to_vec(&wrong_type).unwrap()).err().unwrap();
        assert_eq!(error.batch, Some(9));
        let no_list = serde_json::json!({ "type": "batch", "id": 4, "exchanges": 1 });
        assert_eq!(
            decode(Encoding::Msgpack, &rmp_serde::to_vec_named(&no_list).unwrap()).err().unwrap().batch,
            Some(4)
        );
        assert_eq!(decode(Encoding::Json, b"not json").err().unwrap().batch, None);
    }
}
