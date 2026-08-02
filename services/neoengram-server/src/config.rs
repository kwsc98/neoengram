use std::net::{IpAddr, SocketAddr};

use clap::Parser;

/// NeoEngram central control-plane HTTP server.
#[derive(Debug, Parser)]
#[command(name = "neoengram-server", version)]
pub struct Config {
    /// Listening address (default: 127.0.0.1:8080).
    #[arg(
        long = "listen-addr",
        env = "NEOENGRAM_LISTEN_ADDR",
        default_value = "127.0.0.1:8080"
    )]
    pub listen_addr: SocketAddr,

    /// Data directory for the SQLite authority database.
    #[arg(long = "data-dir", env = "NEOENGRAM_DATA_DIR")]
    pub data_dir: String,

    /// Development principal ID (default: neoengram-server-dev).
    #[arg(
        long = "dev-principal-id",
        env = "NEOENGRAM_DEV_PRINCIPAL_ID",
        default_value = "neoengram-server-dev"
    )]
    pub dev_principal_id: String,

    /// Request timeout in seconds (default: 30).
    #[arg(
        long = "request-timeout-secs",
        env = "NEOENGRAM_REQUEST_TIMEOUT_SECS",
        default_value = "30"
    )]
    pub request_timeout_secs: u64,

    /// Graceful shutdown timeout in seconds (default: 30).
    #[arg(
        long = "graceful-shutdown-secs",
        env = "NEOENGRAM_GRACEFUL_SHUTDOWN_SECS",
        default_value = "30"
    )]
    pub graceful_shutdown_secs: u64,

    /// Allow listening on a non-loopback address without TLS (insecure).
    #[arg(
        long = "allow-insecure-non-loopback",
        env = "NEOENGRAM_ALLOW_INSECURE_NON_LOOPBACK",
        default_value = "false"
    )]
    pub allow_insecure_non_loopback: bool,
}

impl Config {
    /// Validates the listen address security policy.
    ///
    /// Non-loopback addresses require `--allow-insecure-non-loopback`. Returns the validated
    /// address on success.
    pub fn validate_listen_addr(&self) -> Result<SocketAddr, String> {
        let addr = self.listen_addr;
        let is_loopback = match addr.ip() {
            IpAddr::V4(ip) => ip.is_loopback(),
            IpAddr::V6(ip) => ip.is_loopback(),
        };
        if !is_loopback && !self.allow_insecure_non_loopback {
            return Err(format!(
                "refusing to listen on non-loopback address {addr} without \
                 --allow-insecure-non-loopback. \
                 This server provides plaintext HTTP; use TLS termination (Nginx, ingress) or set \
                 the flag to acknowledge this warning."
            ));
        }
        Ok(addr)
    }
}
