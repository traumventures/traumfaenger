//! Small helpers for reading provider JSON.

use serde_json::Value;

/// Parses a body, ignoring the whitespace some gateways send as keep-alive
/// before the JSON (OpenRouter does on slow requests).
pub fn body(text: &str) -> Option<Value> {
    serde_json::from_str(text.trim()).ok()
}

pub fn string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

pub fn number(value: &Value, key: &str) -> Option<u64> {
    match value.get(key)? {
        Value::Number(number) => number.as_u64().or_else(|| number.as_f64().filter(|n| *n >= 0.0).map(|n| n as u64)),
        _ => None,
    }
}

/// `value[a][b]…` as a count.
pub fn path_number(value: &Value, path: &[&str]) -> Option<u64> {
    let (last, parents) = path.split_last()?;
    let mut current = value;
    for key in parents {
        current = current.get(*key)?;
    }
    number(current, last)
}

/// Text of a value that is either a string or something else (stored as JSON).
pub fn text_or_json(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Sum of optional counts; `None` only when every term is `None`.
pub fn sum(terms: &[Option<u64>]) -> Option<u64> {
    terms.iter().flatten().copied().reduce(|a, b| a + b)
}
