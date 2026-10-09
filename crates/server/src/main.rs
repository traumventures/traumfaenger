use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;
use traumfaenger_server::{config::Config, http, projection, store::Store};

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
    // `traumfaenger-server import <export.jsonl[.gz]> <database>`: loads an
    // export of `GET /v1/exchanges/{id}` responses into a database and
    // processes it, for working on processing and analytics locally.
    if std::env::args().nth(1).as_deref() == Some("import") {
        let args: Vec<String> = std::env::args().skip(2).collect();
        let [export, database] = args.as_slice() else {
            eprintln!("usage: traumfaenger-server import <export.jsonl[.gz]> <database>");
            std::process::exit(2);
        };
        return import(export, database);
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(serve())
}

fn import(export: &str, database: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader, Read};
    use traumfaenger_api::{Exchange, StoredExchange};

    let started = std::time::Instant::now();
    let store = Store::open(database, None)?;
    let file = std::fs::File::open(export)?;
    let reader: Box<dyn Read> =
        if export.ends_with(".gz") { Box::new(flate2::read::MultiGzDecoder::new(file)) } else { Box::new(file) };
    let (mut batch, mut accepted, mut duplicates) = (Vec::new(), 0, 0);
    let mut flush = |batch: &mut Vec<Exchange>| -> Result<(), Box<dyn std::error::Error>> {
        let inserted = store.insert(batch, chrono::Utc::now())?;
        accepted += inserted.accepted;
        duplicates += inserted.duplicates;
        batch.clear();
        Ok(())
    };
    for line in BufReader::new(reader).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let stored: StoredExchange = serde_json::from_str(&line)?;
        batch.push(Exchange {
            id: stored.id,
            segment: 0,
            is_final: true,
            service: stored.service,
            operation: stored.operation,
            session_id: stored.session_id,
            metadata: Some(stored.metadata).filter(|metadata| !metadata.is_empty()),
            transport: stored.transport,
            url: stored.url,
            started_at: stored.started_at,
            status: stored.status,
            frames: stored.frames,
            end: stored.end,
            truncated: stored.truncated,
        });
        if batch.len() == traumfaenger_api::limits::MAX_BATCH_SIZE {
            flush(&mut batch)?;
        }
    }
    flush(&mut batch)?;
    let loaded = started.elapsed();
    store.prepare_processing()?;
    let mut processed = 0;
    loop {
        let progress = store.process_batch(projection::BATCH)?;
        processed += progress.processed;
        if progress.caught_up {
            break;
        }
    }
    store.sweep_contents()?;
    store.checkpoint()?;
    println!(
        "imported {accepted} exchanges ({duplicates} already there) in {:.1}s, processed {processed} in {:.1}s",
        loaded.as_secs_f64(),
        (started.elapsed() - loaded).as_secs_f64()
    );
    Ok(())
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

    tokio::spawn(run_processing(store.clone()));
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

/// Processes new raw records into calls, continuously.
async fn run_processing(store: Arc<Store>) {
    let prepare = store.clone();
    let mut sweep_when_caught_up = match tokio::task::spawn_blocking(move || prepare.prepare_processing()).await {
        Ok(Ok(restarted)) => {
            if restarted {
                tracing::info!("parser version changed, reprocessing the raw log");
            }
            restarted
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "cannot start processing");
            return;
        }
        Err(error) => {
            tracing::error!(%error, "processing task panicked");
            return;
        }
    };
    // A failing window is retried in ever smaller steps down to one record.
    // Errors are retried forever (the database may recover); a record whose
    // processing keeps panicking is skipped, so it cannot stop everything
    // behind it.
    let mut limit = projection::BATCH;
    let mut panics = 0;
    loop {
        let worker = store.clone();
        match tokio::task::spawn_blocking(move || worker.process_batch(limit)).await {
            Ok(Ok(progress)) => {
                limit = projection::BATCH;
                panics = 0;
                if progress.caught_up {
                    if sweep_when_caught_up {
                        sweep_when_caught_up = false;
                        let sweeper = store.clone();
                        match tokio::task::spawn_blocking(move || sweeper.sweep_contents()).await {
                            Ok(Ok(removed)) => tracing::info!(removed, "reprocessing done, removed unused contents"),
                            Ok(Err(error)) => tracing::error!(%error, "content sweep failed"),
                            Err(error) => tracing::error!(%error, "content sweep panicked"),
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            Ok(Err(error)) => {
                tracing::error!(%error, limit, "processing failed, retrying");
                limit = (limit / 10).max(1);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Err(error) => {
                tracing::error!(%error, limit, "processing panicked, retrying");
                if limit > 1 {
                    limit = (limit / 10).max(1);
                } else {
                    panics += 1;
                    if panics >= 3 {
                        panics = 0;
                        let skipper = store.clone();
                        match tokio::task::spawn_blocking(move || skipper.skip_record()).await {
                            Ok(Ok(seq)) => tracing::error!(?seq, "skipped a raw record that keeps panicking"),
                            Ok(Err(error)) => tracing::error!(%error, "cannot skip the failing record"),
                            Err(error) => tracing::error!(%error, "skipping the failing record panicked"),
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
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
