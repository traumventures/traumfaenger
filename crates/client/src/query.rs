//! Client of the read-only query API.

use std::time::Duration;

use serde::de::DeserializeOwned;
use traumfaenger_api::{
    ApiError, ErrorCode, HealthResponse, ListQuery, ListResponse, StoredExchange, SummaryQuery, SummaryResponse, routes,
};

/// A non-2xx response, or a request that never got one (`code` is `None`).
#[derive(Debug)]
pub struct QueryError {
    /// HTTP status; 0 when no response arrived.
    pub status: u16,
    pub code: Option<ErrorCode>,
    pub message: String,
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "traumfaenger query failed ({}): {}", self.status, self.message)
    }
}

impl std::error::Error for QueryError {}

/// Client of the read-only query API.
#[derive(Clone)]
pub struct QueryClient {
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
}

impl QueryClient {
    /// `base_url` of the query listener, for example `http://traumfaenger:7601`.
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Self {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap_or_default();
        Self { base_url: base_url.into().trim_end_matches('/').to_owned(), token, http }
    }

    /// One page of the raw log (records without frames), most recently stored first.
    pub async fn list(&self, query: &ListQuery) -> Result<ListResponse, QueryError> {
        self.get(routes::LIST_EXCHANGES, query).await
    }

    /// One exchange with all segments and frames, or `None` when the id is unknown.
    pub async fn get_exchange(&self, id: &str) -> Result<Option<StoredExchange>, QueryError> {
        let mut url = reqwest::Url::parse(&self.base_url).map_err(network)?;
        url.path_segments_mut().map_err(|()| network("base URL cannot have a path"))?.pop_if_empty().extend([
            "v1",
            "exchanges",
            id,
        ]);
        match self.send(self.http.get(url)).await {
            Ok(exchange) => Ok(Some(exchange)),
            Err(error) if error.code == Some(ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Collection statistics, most exchanges first.
    pub async fn summary(&self, query: &SummaryQuery) -> Result<SummaryResponse, QueryError> {
        self.get(routes::SUMMARY, query).await
    }

    pub async fn health(&self) -> Result<HealthResponse, QueryError> {
        self.get(routes::HEALTH, &()).await
    }

    async fn get<T: DeserializeOwned, Q: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        query: &Q,
    ) -> Result<T, QueryError> {
        self.send(self.http.get(format!("{}{path}", self.base_url)).query(query)).await
    }

    async fn send<T: DeserializeOwned>(&self, mut request: reqwest::RequestBuilder) -> Result<T, QueryError> {
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(network)?;
        let status = response.status();
        let body = response.bytes().await.map_err(network)?;
        if status.is_success() {
            return serde_json::from_slice(&body).map_err(|error| QueryError {
                status: status.as_u16(),
                code: None,
                message: format!("unreadable response: {error}"),
            });
        }
        let error: Option<ApiError> = serde_json::from_slice(&body).ok();
        Err(QueryError {
            status: status.as_u16(),
            code: error.as_ref().map(|error| error.error),
            message: error.map_or_else(|| String::from_utf8_lossy(&body).into_owned(), |error| error.message),
        })
    }
}

fn network(error: impl std::fmt::Display) -> QueryError {
    QueryError { status: 0, code: None, message: error.to_string() }
}
