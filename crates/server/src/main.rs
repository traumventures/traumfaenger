use std::sync::Arc;

use ai_ledger_server::{config::Config, http, store::Store};
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Config::from_env()?;
    if config.ingest_token.is_none() {
        tracing::warn!("authentication is disabled (AI_LEDGER_AUTH=disabled); bind to a private interface only");
    }
    let store = Arc::new(Store::open(&config.database, config.memory_limit.as_deref())?);

    let ingest_listener = TcpListener::bind(config.ingest_addr).await?;
    let query_listener = TcpListener::bind(config.query_addr).await?;
    tracing::info!(
        ingest = %ingest_listener.local_addr()?,
        query = %query_listener.local_addr()?,
        database = %config.database,
        "ai-ledger listening"
    );

    if let Some(days) = config.retention_days {
        tokio::spawn(enforce_retention(store.clone(), days));
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutting down");
        let _ = shutdown_tx.send(true);
    });

    let ingest = axum::serve(ingest_listener, http::ingest_router(store.clone(), config.ingest_token))
        .with_graceful_shutdown(shutdown(shutdown_rx.clone()));
    let query = axum::serve(query_listener, http::query_router(store.clone(), config.query_token))
        .with_graceful_shutdown(shutdown(shutdown_rx));
    tokio::try_join!(ingest.into_future(), query.into_future())?;

    store.checkpoint()?;
    Ok(())
}

/// Hourly: delete raw records older than the retention window.
async fn enforce_retention(store: Arc<Store>, days: u32) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
    loop {
        interval.tick().await;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        let store = store.clone();
        match tokio::task::spawn_blocking(move || store.delete_received_before(cutoff)).await {
            Ok(Ok(0)) => {}
            Ok(Ok(deleted)) => tracing::info!(deleted, days, "deleted raw calls past retention"),
            Ok(Err(error)) => tracing::error!(%error, "retention cleanup failed"),
            Err(error) => tracing::error!(%error, "retention task panicked"),
        }
    }
}

async fn shutdown(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
