use std::{sync::Arc, time::Duration};

use clap::Parser;
use fusen_rs::{Server, ServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = neoengram_server::config::Config::parse();
    let listen_addr = config.validate_listen_addr()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if config.allow_insecure_non_loopback {
        tracing::warn!(
            "INSECURE: listening on non-loopback address {listen_addr} without TLS. \
             This server provides plaintext HTTP; use TLS termination (Nginx, ingress) \
             in production."
        );
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        tracing::info!(data_dir = %config.data_dir, "opening authority databases");
        let authority = neoengram_server::app_state::open_authority(&config.data_dir)
            .await
            .map_err(|e| {
                tracing::error!("{e}");
                e
            })?;

        tracing::info!("running database integrity checks");
        authority.integrity_check().await.map_err(|e| {
            tracing::error!("database integrity check failed: {e}");
            format!("integrity check failed: {e}")
        })?;

        let control = neoengram_server::app_state::build_control_plane(&authority);

        let state = Arc::new(neoengram_server::app_state::AppState::new(
            authority,
            control,
            &config.dev_principal_id,
        ));

        let server_config = ServerConfig::builder()
            .graceful_shutdown_timeout(Duration::from_secs(config.graceful_shutdown_secs))
            .build()
            .map_err(|e| format!("failed to build server config: {e}"))?;

        let server = Server::builder(listen_addr.to_string())
            .config(server_config)
            .interface(neoengram_server::system_api::SystemApiServer::new(
                neoengram_server::system_api::SystemApiImpl {
                    state: state.clone(),
                },
            ))
            .interface(neoengram_server::job_api::JobApiServer::new(
                neoengram_server::job_api::JobApiImpl {
                    state: state.clone(),
                },
            ))
            .build()
            .map_err(|e| format!("failed to build server: {e}"))?;

        state.set_ready();
        tracing::info!(addr = %listen_addr, "neoengram-server is ready");

        let running = server.start().await.map_err(|e| {
            tracing::error!("server failed to start: {e}");
            format!("server start error: {e}")
        })?;

        let handle = running.handle();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let sigint = async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("received SIGINT, draining connections");
            };
            let sigterm_future = async {
                let _ = sigterm.recv().await;
                tracing::info!("received SIGTERM, draining connections");
            };
            tokio::select! {
                _ = sigint => {}
                _ = sigterm_future => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("received shutdown signal, draining connections");
        }

        state.set_not_ready();
        tracing::info!("readiness set to false, draining in-flight requests");

        handle.shutdown().await.map_err(|e| {
            tracing::error!("graceful shutdown timed out: {e}");
            format!("shutdown error: {e}")
        })?;

        running.wait().await.map_err(|e| {
            tracing::error!("server wait error: {e}");
            format!("server wait error: {e}")
        })?;

        tracing::info!("neoengram-server shut down");
        Ok::<_, String>(())
    })?;

    Ok(())
}
