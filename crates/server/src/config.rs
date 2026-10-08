//! Configuration from environment variables.

use std::{env, fmt, net::SocketAddr};

#[derive(Clone)]
pub struct Config {
    /// DuckDB file path, or `:memory:`.
    pub database: String,
    /// Optional DuckDB `memory_limit`, for example `512MB`.
    pub memory_limit: Option<String>,
    pub ingest_addr: SocketAddr,
    pub query_addr: SocketAddr,
    /// `None` only when authentication is explicitly disabled.
    pub ingest_token: Option<String>,
    pub query_token: Option<String>,
    /// Delete raw records this many days after receipt. `None` keeps them.
    pub retention_days: Option<u32>,
}

#[derive(Debug)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Tokens shorter than this are rejected so a placeholder never guards prod.
const MIN_TOKEN_LEN: usize = 16;

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok().filter(|value| !value.is_empty()))
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let addr = |name: &str, default: &str| -> Result<SocketAddr, ConfigError> {
            let value = lookup(name).unwrap_or_else(|| default.to_owned());
            value.parse().map_err(|_| ConfigError(format!("{name} must be host:port, got `{value}`")))
        };
        let ingest_addr = addr("TRAUMFAENGER_INGEST_ADDR", "127.0.0.1:7600")?;
        let query_addr = addr("TRAUMFAENGER_QUERY_ADDR", "127.0.0.1:7601")?;

        let auth_disabled = match lookup("TRAUMFAENGER_AUTH").as_deref() {
            None | Some("required") => false,
            Some("disabled") => true,
            Some(other) => {
                return Err(ConfigError(format!("TRAUMFAENGER_AUTH must be `required` or `disabled`, got `{other}`")));
            }
        };
        let (ingest_token, query_token) = if auth_disabled {
            (None, None)
        } else {
            let token = |name: &str| -> Result<String, ConfigError> {
                let value = lookup(name).ok_or_else(|| {
                    ConfigError(format!("{name} is required (or set TRAUMFAENGER_AUTH=disabled for local use)"))
                })?;
                if value.len() < MIN_TOKEN_LEN {
                    return Err(ConfigError(format!("{name} must be at least {MIN_TOKEN_LEN} characters")));
                }
                Ok(value)
            };
            let ingest = token("TRAUMFAENGER_INGEST_TOKEN")?;
            let query = token("TRAUMFAENGER_QUERY_TOKEN")?;
            if ingest == query {
                return Err(ConfigError(
                    "TRAUMFAENGER_INGEST_TOKEN and TRAUMFAENGER_QUERY_TOKEN must differ: writers must not be able to read"
                        .into(),
                ));
            }
            (Some(ingest), Some(query))
        };

        let retention_days = match lookup("TRAUMFAENGER_RETENTION_DAYS") {
            None => None,
            Some(value) => match value.parse::<u32>() {
                Ok(days) if days > 0 => Some(days),
                _ => {
                    return Err(ConfigError(format!(
                        "TRAUMFAENGER_RETENTION_DAYS must be a positive number of days, got `{value}`"
                    )));
                }
            },
        };

        Ok(Self {
            retention_days,
            database: lookup("TRAUMFAENGER_DATABASE").unwrap_or_else(|| "traumfaenger.duckdb".to_owned()),
            memory_limit: lookup("TRAUMFAENGER_MEMORY_LIMIT"),
            ingest_addr,
            query_addr,
            ingest_token,
            query_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let env: HashMap<String, String> = pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        Config::from_lookup(|name| env.get(name).cloned())
    }

    #[test]
    fn requires_two_distinct_tokens_unless_auth_is_disabled() {
        assert!(config(&[]).is_err());
        assert!(config(&[("TRAUMFAENGER_INGEST_TOKEN", "ingest-token-0123456789")]).is_err());
        assert!(
            config(&[("TRAUMFAENGER_INGEST_TOKEN", "short"), ("TRAUMFAENGER_QUERY_TOKEN", "query-token-0123456789")])
                .is_err()
        );
        let same = "same-token-0123456789";
        assert!(config(&[("TRAUMFAENGER_INGEST_TOKEN", same), ("TRAUMFAENGER_QUERY_TOKEN", same)]).is_err());

        let ok = config(&[
            ("TRAUMFAENGER_INGEST_TOKEN", "ingest-token-0123456789"),
            ("TRAUMFAENGER_QUERY_TOKEN", "query-token-0123456789"),
        ])
        .unwrap();
        assert_eq!(ok.ingest_addr.port(), 7600);
        assert_eq!(ok.query_addr.port(), 7601);

        let open = config(&[("TRAUMFAENGER_AUTH", "disabled")]).unwrap();
        assert!(open.ingest_token.is_none() && open.query_token.is_none());
        assert!(config(&[("TRAUMFAENGER_AUTH", "off")]).is_err());

        assert_eq!(open.retention_days, None);
        let days = config(&[("TRAUMFAENGER_AUTH", "disabled"), ("TRAUMFAENGER_RETENTION_DAYS", "30")]).unwrap();
        assert_eq!(days.retention_days, Some(30));
        assert!(config(&[("TRAUMFAENGER_AUTH", "disabled"), ("TRAUMFAENGER_RETENTION_DAYS", "0")]).is_err());
    }
}
