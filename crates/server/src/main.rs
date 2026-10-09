use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;
use traumfaenger_server::{config::Config, http, store::Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `traumfaenger-server healthcheck`: for container health checks in images
    // without curl. Asks `/health` on both listeners of the running server.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let config = Config::from_env()?;
        for addr in [config.ingest_addr, config.query_addr] {
            if let Err(error) = healthcheck(addr) {
                eprintln!("unhealthy: {addr}: {error}");
                std::process::exit(1);
            }
        }
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(serve())
}

/// One plain HTTP/1.0 request to `/health`; the answer must be a 200.
fn healthcheck(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read, Write};
    let target = if addr.ip().is_unspecified() {
        let loopback: IpAddr = if addr.is_ipv4() { Ipv4Addr::LOCALHOST.into() } else { Ipv6Addr::LOCALHOST.into() };
        SocketAddr::new(loopback, addr.port())
    } else {
        addr
    };
    let timeout = Duration::from_secs(3);
    let mut stream = std::net::TcpStream::connect_timeout(&target, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut head = [0u8; 12];
    stream.read_exact(&mut head)?;
    if &head[9..12] == b"200" {
        Ok(())
    } else {
        Err(format!("status {}", String::from_utf8_lossy(&head[9..12])).into())
    }
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Config::from_env()?;
    if config.ingest_token.is_none() {
        tracing::warn!("authentication is disabled (TRAUMFAENGER_AUTH=disabled); bind to a private interface only");
    }
    let store = Arc::new(Store::open(&config.database, config.memory_limit.as_deref())?);

    let ingest_listener = TcpListener::bind(config.ingest_addr).await?;
    let query_listener = TcpListener::bind(config.query_addr).await?;
    tracing::info!(
        ingest = %ingest_listener.local_addr()?,
        query = %query_listener.local_addr()?,
        database = %config.database,
        "traumfaenger listening"
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
