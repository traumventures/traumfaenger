//! The operation context: what every exchange recorded inside
//! [`operation`] is tagged with. Kept in a tokio task-local, so it follows
//! the awaited code of one task. Tasks spawned inside it do not inherit it;
//! wrap their futures in [`operation`] again (or [`current`] + [`scope`]).

use std::{collections::BTreeMap, future::Future};

/// Context of the exchanges recorded inside [`operation`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Operation {
    /// The kind of work, for example `listing.parse`.
    pub operation: String,
    /// Groups the exchanges of one conversation, for example one support chat.
    pub session_id: Option<String>,
    /// Flat key-value pairs, for example `{ "listing_id": "123" }`.
    pub metadata: BTreeMap<String, String>,
}

impl Operation {
    pub fn new(operation: impl Into<String>) -> Self {
        Self { operation: operation.into(), ..Self::default() }
    }

    pub fn session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn meta(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.metadata.insert(key.into(), value.to_string());
        self
    }
}

tokio::task_local! {
    static CURRENT: Operation;
}

/// The context in effect, if any.
pub fn current() -> Option<Operation> {
    CURRENT.try_with(Clone::clone).ok()
}

/// Runs `future` with exactly `context`, without merging.
pub async fn scope<F: Future>(context: Operation, future: F) -> F::Output {
    CURRENT.scope(context, future).await
}

/// Runs `future` with an operation context. Nested operations override
/// `operation` and `session_id` (when set) and merge `metadata`.
pub async fn operation<F: Future>(context: Operation, future: F) -> F::Output {
    let merged = match current() {
        None => context,
        Some(outer) => {
            let mut metadata = outer.metadata;
            metadata.extend(context.metadata);
            Operation { operation: context.operation, session_id: context.session_id.or(outer.session_id), metadata }
        }
    };
    CURRENT.scope(merged, future).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nested_operations_merge() {
        assert_eq!(current(), None);
        let inner = operation(Operation::new("outer").session("s").meta("a", 1).meta("b", 1), async {
            operation(Operation::new("inner").meta("b", 2), async { current() }).await
        })
        .await
        .unwrap();
        assert_eq!(inner.operation, "inner");
        assert_eq!(inner.session_id.as_deref(), Some("s"));
        assert_eq!(inner.metadata, BTreeMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]));
    }
}
