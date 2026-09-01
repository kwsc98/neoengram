use std::{
    collections::BTreeSet,
    convert::Infallible,
    error::Error,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use clap::Parser;
use http::{
    header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
    Method, Request, Response, StatusCode,
};
use http_body_util::{BodyExt as _, Either};
use hyper::{body::Body, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto::Builder as ConnectionBuilder,
};
use neoengram_domain::protocol::{ContentDigest, UnixMillis};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

mod bootstrap;
mod central_http;
mod peer;
mod public_listener;
mod s3_backend;
mod s3_read_channel;
#[allow(dead_code)]
mod transfer_quic;
mod transport_config;
mod tunnel;

use bootstrap::{
    validate_workload_trust_domain, BootstrapError, GatewayBootstrap, GatewayBootstrapConfig,
};
use peer::H2PeerForwarder;
use transport_config::{GatewayTransferTlsConfig, GatewayTransportConfig};
#[cfg(test)]
use tunnel::NDJSON_CONTENT_TYPE;
use tunnel::{
    error_response, json_response, GatewayBody, GatewayIdentity, GatewayTunnel, JSON_CONTENT_TYPE,
};

#[derive(Debug, Clone)]
struct TransferUpstreamRoute {
    agent_id: String,
    upstream: SocketAddr,
}

fn parse_transfer_upstream_route(value: &str) -> Result<TransferUpstreamRoute, String> {
    let (agent_id, upstream) = value
        .split_once('=')
        .ok_or_else(|| "transfer upstream route must use AGENT_ID=HOST:PORT syntax".to_owned())?;
    if agent_id.is_empty() {
        return Err("transfer upstream route Agent ID must not be empty".to_owned());
    }
    neoengram_domain::protocol::AgentId::new(agent_id)
        .map_err(|error| format!("transfer upstream route Agent ID is invalid: {error}"))?;
    let upstream = upstream
        .parse::<SocketAddr>()
        .map_err(|error| format!("transfer upstream route address is invalid: {error}"))?;
    Ok(TransferUpstreamRoute {
        agent_id: agent_id.to_owned(),
        upstream,
    })
}

const DEFAULT_AGENT_LISTEN: &str = "0.0.0.0:8081";
const DEFAULT_CONTROL_LISTEN: &str = "0.0.0.0:8082";
const DEFAULT_PEER_LISTEN: &str = "0.0.0.0:8083";
const DEFAULT_CONSOLE_HOST: &str = "localhost";
// Local development uses path-style S3 against the Gateway's loopback public listener. Production
// deployments override this with their DNS name (or a literal IP) through NEOENGRAM_GATEWAY_S3_HOST.
const DEFAULT_S3_HOST: &str = "127.0.0.1";
const DEFAULT_WEB_ROOT: &str = "apps/neoengram-web/dist";
const DEFAULT_CENTRAL_UPSTREAM: &str = "http://127.0.0.1:8080";
const DEFAULT_PUBLIC_MAX_STREAMS: usize = 128;
const MAX_PUBLIC_STREAMS: usize = 16_384;
const MAX_CONFIGURED_REQUEST_BYTES: usize =
    neoengram_domain::protocol::MAX_GATEWAY_OPAQUE_PAYLOAD_BYTES;
const MAX_CONFIGURED_DEADLINE_MILLIS: u64 = 300_000;
const MAX_PRE_STOP_DRAIN_SECONDS: u64 = 300;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
// HTTP/2 PINGs bound half-open Central control connections independently of application traffic.
// The interval is shorter than the Central heartbeat watchdog/RouteLease TTL, while the bounded
// acknowledgement timeout prevents a dead peer from holding the Gateway's one-session slot.
const CONTROL_H2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const CONTROL_H2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(name = "neoengram-gateway", version, about)]
struct GatewayConfig {
    #[arg(long, env = "NEOENGRAM_GATEWAY_EDGE_CLUSTER_ID")]
    edge_cluster_id: String,
    #[arg(long, env = "NEOENGRAM_GATEWAY_POOL_ID")]
    gateway_pool_id: String,
    #[arg(long, env = "NEOENGRAM_GATEWAY_REPLICA_ID")]
    gateway_replica_id: String,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_AGENT_LISTEN",
        default_value = DEFAULT_AGENT_LISTEN
    )]
    agent_listen: SocketAddr,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_CONTROL_LISTEN",
        default_value = DEFAULT_CONTROL_LISTEN
    )]
    control_listen: SocketAddr,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_PEER_LISTEN",
        default_value = DEFAULT_PEER_LISTEN
    )]
    peer_listen: SocketAddr,
    /// Optional QUIC object-transfer listener. Enabling this endpoint requires workload mTLS
    /// material even for loopback development, because TransferTicket is a bearer capability.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_LISTEN")]
    transfer_listen: Option<SocketAddr>,
    /// Selects which endpoint tuple this Gateway fences before relaying toward the source.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_RELAY_ROLE")]
    transfer_relay_role: Option<transfer_quic::TransferRelayRole>,
    /// Static next QUIC hop toward the source: target Gateway to source Gateway, or source
    /// Gateway to source Agent. The signed ticket never supplies this address.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_UPSTREAM")]
    transfer_upstream: Option<SocketAddr>,
    /// TLS ServerName expected from the configured transfer upstream.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_UPSTREAM_SERVER_NAME")]
    transfer_upstream_server_name: Option<String>,
    /// Optional per-source next-hop directory. Each entry uses `agent-id=host:port`; the signed
    /// ticket selects only the Agent identity, while this deployment-owned directory supplies the
    /// network address. Repeated entries are rejected during config validation.
    #[arg(
        long = "transfer-upstream-route",
        env = "NEOENGRAM_GATEWAY_TRANSFER_UPSTREAM_ROUTE",
        value_parser = parse_transfer_upstream_route
    )]
    transfer_upstream_routes: Vec<TransferUpstreamRoute>,
    /// Optional current session generation for the endpoint selected by transfer_relay_role.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_SESSION_GENERATION")]
    transfer_session_generation: Option<u64>,
    /// Optional current mount generation for the endpoint selected by transfer_relay_role.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_MOUNT_GENERATION")]
    transfer_mount_generation: Option<u64>,
    /// Optional current route generation for the endpoint selected by transfer_relay_role.
    #[arg(long, env = "NEOENGRAM_GATEWAY_TRANSFER_ROUTE_GENERATION")]
    transfer_route_generation: Option<u64>,
    /// Optional browser/S3 listener. Existing workload-only deployments remain unchanged until
    /// this address is configured explicitly.
    #[arg(long, env = "NEOENGRAM_GATEWAY_PUBLIC_LISTEN")]
    public_listen: Option<SocketAddr>,
    /// Public TLS certificate for browser/S3 clients.  Workload mTLS files above are separate.
    #[arg(long, env = "NEOENGRAM_GATEWAY_PUBLIC_TLS_CERTIFICATE_FILE")]
    public_tls_certificate_file: Option<std::path::PathBuf>,
    /// Public TLS private key matching `public_tls_certificate_file`.
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_PUBLIC_TLS_PRIVATE_KEY_FILE",
        hide_env_values = true
    )]
    public_tls_private_key_file: Option<std::path::PathBuf>,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_CONSOLE_HOST",
        default_value = DEFAULT_CONSOLE_HOST
    )]
    console_host: String,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_S3_HOST",
        default_value = DEFAULT_S3_HOST
    )]
    s3_host: String,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_WEB_ROOT",
        default_value = DEFAULT_WEB_ROOT
    )]
    web_root: std::path::PathBuf,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_CENTRAL_API_UPSTREAM",
        default_value = DEFAULT_CENTRAL_UPSTREAM
    )]
    central_upstream: url::Url,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_S3_MAX_STREAMS",
        default_value_t = DEFAULT_PUBLIC_MAX_STREAMS
    )]
    s3_max_streams: usize,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_MAX_CONNECTIONS_PER_LISTENER",
        default_value_t = 1024
    )]
    max_connections_per_listener: usize,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_MAX_IN_FLIGHT_REQUESTS_PER_LISTENER",
        default_value_t = 256
    )]
    max_in_flight_requests_per_listener: usize,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_MAX_REQUEST_BYTES",
        default_value_t = MAX_CONFIGURED_REQUEST_BYTES
    )]
    max_request_bytes: usize,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_REQUEST_DEADLINE_MILLIS",
        default_value_t = 10_000
    )]
    request_deadline_millis: u64,
    /// Long-lived SPIFFE trust domain used to bind every workload certificate.
    #[arg(long, env = "NEOENGRAM_GATEWAY_WORKLOAD_TRUST_DOMAIN")]
    workload_trust_domain: Option<String>,
    #[command(flatten)]
    transport: GatewayTransportConfig,
    #[command(flatten)]
    transfer_transport: GatewayTransferTlsConfig,
    #[command(flatten)]
    bootstrap: GatewayBootstrapConfig,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_LOG",
        default_value = "neoengram_gateway=info"
    )]
    log: String,
    /// Signals the Gateway process to enter drain before Kubernetes sends SIGTERM.
    #[arg(long, hide = true)]
    pre_stop_drain: bool,
    #[arg(
        long,
        env = "NEOENGRAM_GATEWAY_PRE_STOP_DRAIN_SECONDS",
        default_value_t = 20
    )]
    pre_stop_drain_seconds: u64,
}

impl GatewayConfig {
    fn validate(&self) -> Result<(), String> {
        neoengram_domain::protocol::EdgeClusterId::new(&self.edge_cluster_id)
            .map_err(|error| format!("NEOENGRAM_GATEWAY_EDGE_CLUSTER_ID is invalid: {error}"))?;
        neoengram_domain::protocol::GatewayPoolId::new(&self.gateway_pool_id)
            .map_err(|error| format!("NEOENGRAM_GATEWAY_POOL_ID is invalid: {error}"))?;
        neoengram_domain::protocol::GatewayReplicaId::new(&self.gateway_replica_id)
            .map_err(|error| format!("NEOENGRAM_GATEWAY_REPLICA_ID is invalid: {error}"))?;
        if self.agent_listen == self.control_listen
            || self.agent_listen == self.peer_listen
            || self.control_listen == self.peer_listen
            || self.transfer_listen.is_some_and(|transfer| {
                [self.agent_listen, self.control_listen, self.peer_listen].contains(&transfer)
            })
            || self.public_listen.is_some_and(|public| {
                [self.agent_listen, self.control_listen, self.peer_listen].contains(&public)
            })
        {
            return Err("Gateway listeners must use distinct addresses".into());
        }
        if self
            .transfer_listen
            .is_some_and(|transfer| self.public_listen.is_some_and(|public| transfer == public))
        {
            return Err("Gateway listeners must use distinct addresses".into());
        }
        let relay_fields = [
            self.transfer_relay_role.is_some(),
            self.transfer_upstream.is_some(),
            self.transfer_upstream_server_name.is_some(),
            !self.transfer_upstream_routes.is_empty(),
        ];
        let has_upstream =
            self.transfer_upstream.is_some() || !self.transfer_upstream_routes.is_empty();
        if relay_fields.iter().any(|configured| *configured)
            && (self.transfer_relay_role.is_none()
                || self.transfer_upstream_server_name.is_none()
                || !has_upstream)
        {
            return Err(
                "transfer relay role, an upstream route, and upstream ServerName must be configured together"
                    .into(),
            );
        }
        if self.transfer_relay_role.is_some() && self.transfer_listen.is_none() {
            return Err("transfer listener is required when transfer relay is configured".into());
        }
        if self
            .transfer_upstream
            .zip(self.transfer_listen)
            .is_some_and(|(upstream, listener)| upstream == listener)
        {
            return Err("transfer upstream must differ from the local transfer listener".into());
        }
        let mut route_agents = BTreeSet::new();
        for route in &self.transfer_upstream_routes {
            if !route_agents.insert(route.agent_id.as_str()) {
                return Err(format!(
                    "transfer upstream route for Agent {} is configured more than once",
                    route.agent_id
                ));
            }
            if self.transfer_listen == Some(route.upstream) {
                return Err(
                    "transfer upstream route must differ from the local transfer listener".into(),
                );
            }
        }
        if let Some(server_name) = &self.transfer_upstream_server_name {
            rustls_pki_types::ServerName::try_from(server_name.clone())
                .map_err(|error| format!("transfer upstream ServerName is invalid: {error}"))?;
        }
        let generation_fields = [
            self.transfer_session_generation,
            self.transfer_mount_generation,
            self.transfer_route_generation,
        ];
        let configured_generations = generation_fields
            .iter()
            .filter(|generation| generation.is_some())
            .count();
        if configured_generations != 0 && configured_generations != generation_fields.len() {
            return Err(
                "transfer session, mount, and route generations must be configured together".into(),
            );
        }
        if configured_generations != 0 && self.transfer_relay_role.is_none() {
            return Err("transfer generations require a configured relay role".into());
        }
        // Generation fences are learned from the Central-authorized Agent channel at runtime.
        // Static values remain supported for deployments that want a startup fence, but an
        // omitted tuple starts fail-closed and is populated after channel.opened.
        if generation_fields
            .iter()
            .flatten()
            .any(|generation| *generation == 0)
        {
            return Err("transfer generations must be greater than zero".into());
        }
        let console_host = public_listener::normalize_configured_host(&self.console_host)
            .ok_or_else(|| "console_host must be a valid hostname without a port".to_owned())?;
        let s3_host = public_listener::normalize_configured_host(&self.s3_host)
            .ok_or_else(|| "s3_host must be a valid hostname without a port".to_owned())?;
        if console_host == s3_host {
            return Err("console_host and s3_host must be distinct non-empty hostnames".into());
        }
        if self.s3_max_streams == 0 || self.s3_max_streams > MAX_PUBLIC_STREAMS {
            return Err(format!(
                "s3_max_streams must be in 1..={MAX_PUBLIC_STREAMS}"
            ));
        }
        match (
            &self.public_tls_certificate_file,
            &self.public_tls_private_key_file,
        ) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(
                    "public TLS certificate and private key files must be configured together"
                        .into(),
                );
            }
        }
        if self.public_listen.is_none()
            && (self.public_tls_certificate_file.is_some()
                || self.public_tls_private_key_file.is_some())
        {
            return Err("public listener must be configured when public TLS is configured".into());
        }
        if self
            .public_listen
            .is_some_and(|address| !address.ip().is_loopback())
            && self.public_tls_certificate_file.is_none()
        {
            return Err(
                "exposed public listener requires public TLS certificate and private key".into(),
            );
        }
        let workload_loopback = [self.agent_listen, self.control_listen, self.peer_listen]
            .iter()
            .all(|address| address.ip().is_loopback())
            && self
                .transfer_listen
                .is_none_or(|address| address.ip().is_loopback());
        if let Some(public_listen) = self.public_listen {
            if !matches!(self.central_upstream.scheme(), "http" | "https")
                || self.central_upstream.host_str().is_none()
                || !self.central_upstream.username().is_empty()
                || self.central_upstream.password().is_some()
                || (self.central_upstream.path() != "/" && !self.central_upstream.path().is_empty())
                || self.central_upstream.query().is_some()
                || self.central_upstream.fragment().is_some()
            {
                return Err(
                    "central_upstream must be an origin-form http(s) URL without credentials or a path"
                        .into(),
                );
            }
            if self.central_upstream.scheme() == "http"
                && (!workload_loopback || !public_listen.ip().is_loopback())
            {
                return Err(
                    "plain HTTP Central upstream is permitted only for loopback-only development"
                        .into(),
                );
            }
            if self.central_upstream.scheme() == "https"
                && (self.transport.tls_certificate_file.is_none()
                    || self.transport.tls_private_key_file.is_none()
                    || self.transport.tls_client_ca_file.is_none())
            {
                return Err(
                    "HTTPS Central upstream requires the Gateway workload certificate, key, and client CA"
                        .into(),
                );
            }
        }
        if self.max_connections_per_listener == 0 {
            return Err("max_connections_per_listener must be positive".into());
        }
        if self.max_in_flight_requests_per_listener == 0 {
            return Err("max_in_flight_requests_per_listener must be positive".into());
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > MAX_CONFIGURED_REQUEST_BYTES {
            return Err(format!(
                "max_request_bytes must be in 1..={MAX_CONFIGURED_REQUEST_BYTES}"
            ));
        }
        if self.request_deadline_millis == 0
            || self.request_deadline_millis > MAX_CONFIGURED_DEADLINE_MILLIS
        {
            return Err(format!(
                "request_deadline_millis must be in 1..={MAX_CONFIGURED_DEADLINE_MILLIS}"
            ));
        }
        self.pre_stop_drain_duration()?;
        self.transport
            .validate_listener_exposure([self.agent_listen, self.control_listen, self.peer_listen])
            .map_err(|error| error.to_string())?;
        self.transfer_transport
            .validate()
            .map_err(|error| error.to_string())?;
        self.bootstrap
            .validate()
            .map_err(|error| error.to_string())?;
        if let Some(trust_domain) = &self.workload_trust_domain {
            validate_workload_trust_domain(trust_domain).map_err(|error| error.to_string())?;
        }
        let listeners_are_exposed = [self.agent_listen, self.control_listen, self.peer_listen]
            .iter()
            .any(|address| !address.ip().is_loopback())
            || self
                .transfer_listen
                .is_some_and(|address| !address.ip().is_loopback());
        let tls_is_configured = self.transport.tls_certificate_file.is_some()
            || self.transport.tls_private_key_file.is_some()
            || self.transport.tls_client_ca_file.is_some()
            || self.transfer_transport.is_configured();
        let bootstrap_is_configured = self.bootstrap.private_key_file.is_some()
            || self.bootstrap.activation_token_file.is_some()
            || self.bootstrap.certificate_chain_file.is_some();
        // Local development may complete the one-time bootstrap over loopback HTTP. Exposed
        // deployments still require a server-authenticated TLS bootstrap channel.
        let loopback_bootstrap = bootstrap_is_configured && workload_loopback;
        if bootstrap_is_configured
            && !loopback_bootstrap
            && (self.transport.tls_certificate_file.is_none()
                || self.transport.tls_private_key_file.is_none())
        {
            return Err(
                "Gateway bootstrap requires the listener server certificate and private key".into(),
            );
        }
        if (listeners_are_exposed || tls_is_configured || bootstrap_is_configured)
            && self.workload_trust_domain.is_none()
        {
            return Err(
                "NEOENGRAM_GATEWAY_WORKLOAD_TRUST_DOMAIN is required for TLS or exposed listeners"
                    .into(),
            );
        }
        if self.transfer_listen.is_some() && !self.transfer_transport.is_configured() {
            return Err(
                "QUIC transfer listener requires the dedicated transfer certificate, private key, and client CA"
                    .into(),
            );
        }
        if self.transfer_listen.is_none() && self.transfer_transport.is_configured() {
            return Err(
                "dedicated transfer TLS material requires a configured transfer listener".into(),
            );
        }
        if bootstrap_is_configured
            && (self.transfer_relay_role.is_some()
                || self.transfer_upstream.is_some()
                || self.transfer_upstream_server_name.is_some()
                || !self.transfer_upstream_routes.is_empty()
                || configured_generations != 0)
        {
            return Err(
                "transfer relay routing cannot be enabled while Gateway bootstrap is active".into(),
            );
        }
        EnvFilter::try_new(&self.log)
            .map_err(|error| format!("NEOENGRAM_GATEWAY_LOG is invalid: {error}"))?;
        Ok(())
    }

    fn pre_stop_drain_duration(&self) -> Result<Duration, String> {
        if self.pre_stop_drain_seconds == 0
            || self.pre_stop_drain_seconds > MAX_PRE_STOP_DRAIN_SECONDS
        {
            return Err(format!(
                "pre_stop_drain_seconds must be in 1..={MAX_PRE_STOP_DRAIN_SECONDS}"
            ));
        }
        Ok(Duration::from_secs(self.pre_stop_drain_seconds))
    }
}

#[derive(Debug, Clone, Copy)]
enum ListenerRole {
    Agent,
    Control,
    Peer,
}

impl ListenerRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Control => "control",
            Self::Peer => "peer",
        }
    }
}

#[derive(Clone)]
struct ListenerState {
    role: ListenerRole,
    edge_cluster_id: neoengram_domain::protocol::EdgeClusterId,
    gateway_pool_id: neoengram_domain::protocol::GatewayPoolId,
    tunnel: Arc<GatewayTunnel>,
    s3_read_channels: Arc<s3_read_channel::S3ReadChannelRegistry>,
    request_admission: Arc<Semaphore>,
    max_request_bytes: usize,
    request_deadline: Duration,
    tls_acceptor: Option<TlsAcceptor>,
    bootstrap: Option<Arc<GatewayBootstrap>>,
    workload_trust_domain: Option<Arc<str>>,
    lifecycle: GatewayLifecycle,
}

#[derive(Clone, Default)]
struct GatewayLifecycle {
    pub(crate) draining: Arc<AtomicBool>,
}

impl GatewayLifecycle {
    fn begin_drain(&self) -> bool {
        !self.draining.swap(true, Ordering::AcqRel)
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
enum PeerAuth {
    /// No client certificate was presented on the optional Agent listener.
    Anonymous,
    /// A verified workload certificate identified an Agent in this EdgeCluster.
    Agent(neoengram_domain::protocol::AgentId),
    /// Loopback plaintext development transport. TLS identity checks do not apply here.
    Development,
    /// A verified Central workload certificate on the control listener.
    Central,
    /// A verified same-pool Gateway Replica identity from the peer mTLS URI SAN.
    #[allow(dead_code)]
    GatewayReplica(neoengram_domain::protocol::GatewayReplicaId),
    /// The same identity plus the exact DER leaf observed during the TLS handshake. The
    /// fingerprint is checked against Central's short-lived peer directory before delivery.
    GatewayReplicaWithCertificate {
        replica_id: neoengram_domain::protocol::GatewayReplicaId,
        certificate_fingerprint: ContentDigest,
    },
}

impl PeerAuth {
    fn is_authenticated(&self) -> bool {
        !matches!(self, Self::Anonymous)
    }

    fn agent_id(&self) -> Option<neoengram_domain::protocol::AgentId> {
        match self {
            Self::Agent(agent_id) => Some(agent_id.clone()),
            Self::Anonymous
            | Self::Development
            | Self::Central
            | Self::GatewayReplica(_)
            | Self::GatewayReplicaWithCertificate { .. } => None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = GatewayConfig::parse();
    if config.pre_stop_drain {
        let drain_duration = config
            .pre_stop_drain_duration()
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
        run_pre_stop_drain(drain_duration)?;
        return Ok(());
    }
    config
        .validate()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    initialize_logging(&config.log)?;
    run(config).await
}

fn initialize_logging(filter: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_new(filter)?)
        .try_init()?;
    Ok(())
}

async fn run(config: GatewayConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    let identity = GatewayIdentity {
        edge_cluster_id: neoengram_domain::protocol::EdgeClusterId::new(&config.edge_cluster_id)?,
        gateway_pool_id: neoengram_domain::protocol::GatewayPoolId::new(&config.gateway_pool_id)?,
        gateway_replica_id: neoengram_domain::protocol::GatewayReplicaId::new(
            &config.gateway_replica_id,
        )?,
        software_version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    if config.bootstrap.private_key_file.is_some() {
        // The explicit loopback development profile may bootstrap over plaintext. Production
        // bootstrap still validates the short-lived server-authenticated TLS certificate.
        let loopback_only = [
            config.agent_listen,
            config.control_listen,
            config.peer_listen,
        ]
        .iter()
        .all(|address| address.ip().is_loopback());
        if !loopback_only {
            config
                .transport
                .validate_local_bootstrap_server_identity()?;
        }
    } else {
        config.transport.validate_local_server_identity(
            identity.edge_cluster_id.as_str(),
            identity.gateway_pool_id.as_str(),
            identity.gateway_replica_id.as_str(),
            config.workload_trust_domain.as_deref(),
        )?;
    }
    if config.transfer_listen.is_some() {
        if config.bootstrap.private_key_file.is_some() {
            config
                .transfer_transport
                .validate_local_bootstrap_server_identity()?;
        } else {
            config.transfer_transport.validate_local_identity(
                identity.edge_cluster_id.as_str(),
                identity.gateway_pool_id.as_str(),
                identity.gateway_replica_id.as_str(),
                config.workload_trust_domain.as_deref(),
            )?;
        }
    }
    let [agent_tls, control_tls, peer_server_tls] = config
        .transport
        .load_server_configs([
            config.agent_listen,
            config.control_listen,
            config.peer_listen,
        ])?
        .map(|config| config.map(TlsAcceptor::from));
    let transfer_tls = if config.transfer_listen.is_some() {
        Some(config.transfer_transport.load_server_config()?)
    } else {
        None
    };
    // Build the matching client policy up front as well. The peer relay uses this same policy;
    // validating both directions at startup prevents a listener that can receive transfers but
    // cannot establish the required one-hop mTLS connection after activation.
    let transfer_client_tls = if config.transfer_listen.is_some() {
        Some(config.transfer_transport.load_client_config()?)
    } else {
        None
    };
    let public_tls = match (
        config.public_tls_certificate_file.as_deref(),
        config.public_tls_private_key_file.as_deref(),
    ) {
        (Some(certificate), Some(private_key)) => Some(TlsAcceptor::from(
            transport_config::GatewayTransportConfig::load_public_server_config(
                certificate,
                private_key,
            )?,
        )),
        (None, None) => None,
        _ => unreachable!("public TLS pair was validated before startup"),
    };
    let bootstrap = config.bootstrap.load(
        identity.clone(),
        config.workload_trust_domain.as_deref(),
        config.transport.tls_certificate_file.as_deref(),
        config.transport.client_ca_file(),
    )?;
    let peer_client_tls = config.transport.load_peer_client_config()?;
    let allow_loopback_http = [
        config.agent_listen,
        config.control_listen,
        config.peer_listen,
    ]
    .iter()
    .all(|address| address.ip().is_loopback());
    let peer_forwarder = Arc::new(H2PeerForwarder::new(
        identity.clone(),
        peer_client_tls.clone(),
        config.workload_trust_domain.clone().map(Arc::<str>::from),
        allow_loopback_http,
    ));
    let mut transfer_fence = transfer_quic::QuicTransferFence::for_role(
        config
            .transfer_relay_role
            .unwrap_or(transfer_quic::TransferRelayRole::Target),
        identity.gateway_pool_id.clone(),
        identity.edge_cluster_id.clone(),
    );
    if let (Some(session), Some(mount), Some(route)) = (
        config.transfer_session_generation,
        config.transfer_mount_generation,
        config.transfer_route_generation,
    ) {
        transfer_fence = transfer_fence.with_generations(session, mount, route);
    }
    // Keep deployment-owned source addresses in a directory shared by the control tunnel and
    // QUIC relay. The tunnel activates/deactivates entries with the Central route lease, so a
    // reconnect cannot leave a stale source hop eligible while retaining the configured address.
    let transfer_upstreams = config
        .transfer_listen
        .map(|_| transfer_quic::TransferUpstreamDirectory::new());
    let manages_source_routes =
        config.transfer_relay_role == Some(transfer_quic::TransferRelayRole::Source);
    if let Some(directory) = &transfer_upstreams {
        for route in &config.transfer_upstream_routes {
            let agent_id = neoengram_domain::protocol::AgentId::new(route.agent_id.clone())
                .map_err(|error| {
                    std::io::Error::other(format!("transfer route Agent ID is invalid: {error}"))
                })?;
            if manages_source_routes {
                directory.configure(agent_id, route.upstream);
            } else {
                // A target Gateway's per-source directory points at a remote source Gateway; its
                // local target Agent lease cannot activate/deactivate that remote hop. The
                // target transfer fence still gates admission, so keep the deployment route
                // available while the source Gateway enforces its own lease.
                directory.set(agent_id, route.upstream);
            }
        }
    }
    let tunnel = Arc::new(
        GatewayTunnel::with_peer_forwarder_and_transfer_fence_and_upstreams(
            identity,
            peer_forwarder.clone(),
            config
                .transfer_listen
                .is_some()
                .then(|| transfer_fence.clone()),
            manages_source_routes
                .then(|| transfer_upstreams.clone())
                .flatten(),
        ),
    );
    let s3_read_channels = Arc::new(s3_read_channel::S3ReadChannelRegistry::with_peer_reader(
        tunnel.clone(),
        peer_forwarder,
    ));
    let agent = TcpListener::bind(config.agent_listen).await?;
    let control = TcpListener::bind(config.control_listen).await?;
    let peer = TcpListener::bind(config.peer_listen).await?;
    let public = match config.public_listen {
        Some(address) => Some((
            TcpListener::bind(address).await?,
            address.ip().is_loopback(),
        )),
        None => None,
    };
    let transfer = match (config.transfer_listen, transfer_tls, transfer_client_tls) {
        (Some(address), Some(server_tls), Some(client_tls)) => {
            let mut listener =
                transfer_quic::QuicTransferListener::bind(address, server_tls, transfer_fence)
                    .map_err(std::io::Error::other)?;
            if let Some(server_name) = config.transfer_upstream_server_name.as_deref() {
                let directory = transfer_upstreams
                    .clone()
                    .expect("transfer upstream directory is created with transfer listener");
                let connector = transfer_quic::QuinnTransferConnectionFactory::bind_with_directory(
                    config.transfer_upstream,
                    Arc::<str>::from(server_name),
                    client_tls,
                    directory,
                )
                .map_err(std::io::Error::other)?;
                listener = listener.with_relay(Arc::new(
                    transfer_quic::ConnectedTransferRelay::new(Arc::new(connector)),
                ));
            }
            Some(listener)
        }
        (None, None, None) => None,
        _ => unreachable!("transfer listener and TLS configuration are paired"),
    };
    let lifecycle = GatewayLifecycle::default();
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let mut listeners = JoinSet::new();

    spawn_listener(
        &mut listeners,
        agent,
        ListenerRole::Agent,
        &config,
        tunnel.clone(),
        s3_read_channels.clone(),
        bootstrap.clone(),
        agent_tls,
        lifecycle.clone(),
        shutdown_receiver.clone(),
    );
    spawn_listener(
        &mut listeners,
        control,
        ListenerRole::Control,
        &config,
        tunnel.clone(),
        s3_read_channels.clone(),
        bootstrap.clone(),
        control_tls,
        lifecycle.clone(),
        shutdown_receiver.clone(),
    );
    spawn_listener(
        &mut listeners,
        peer,
        ListenerRole::Peer,
        &config,
        tunnel.clone(),
        s3_read_channels.clone(),
        bootstrap,
        peer_server_tls,
        lifecycle.clone(),
        shutdown_receiver.clone(),
    );
    if let Some((public, allow_loopback_hosts)) = public {
        let central_client = central_http::CentralHttpClient::new(peer_client_tls.clone())
            .map_err(std::io::Error::other)?;
        let s3_backend = Arc::new(
            s3_backend::CentralS3Backend::new_with_client(
                config.gateway_pool_id.clone(),
                &config.central_upstream,
                central_client.clone(),
            )
            .map_err(std::io::Error::other)?
            .with_object_reader(s3_read_channels),
        );
        public_listener::spawn_listener(
            &mut listeners,
            public,
            public_tls,
            public_listener::PublicListenerState::new(
                public_listener::PublicListenerConfig::new(
                    config.console_host.clone(),
                    config.s3_host.clone(),
                    config.web_root.clone(),
                    Some(config.central_upstream.clone()),
                    config.s3_max_streams,
                    allow_loopback_hosts,
                ),
                public_listener::PublicListenerLimits::new(
                    config.max_connections_per_listener,
                    config.max_in_flight_requests_per_listener,
                    config.max_request_bytes,
                    Duration::from_millis(config.request_deadline_millis),
                ),
                lifecycle.draining.clone(),
            )
            .with_s3_backend(s3_backend)
            .with_central_client(central_client),
            shutdown_receiver.clone(),
        );
    }
    if let Some(transfer) = transfer {
        listeners.spawn(transfer_quic::serve(
            transfer,
            config.max_connections_per_listener,
            shutdown_receiver.clone(),
        ));
    }

    let drain_duration = config
        .pre_stop_drain_duration()
        .expect("GatewayConfig was validated before signal handling");
    tokio::select! {
        signal = shutdown_signal(lifecycle, tunnel, drain_duration) => signal?,
        result = listeners.join_next() => {
            match result {
                Some(Ok(Ok(()))) => {
                    return Err(std::io::Error::other("Gateway listener stopped unexpectedly").into());
                }
                Some(Ok(Err(error))) => return Err(error.into()),
                Some(Err(error)) => return Err(error.into()),
                None => return Err(std::io::Error::other("Gateway listeners were not started").into()),
            }
        }
    }

    let _ = shutdown_sender.send(true);
    while let Some(result) = listeners.join_next().await {
        result??;
    }
    Ok(())
}

// Each listener receives its own role, admission limits, TLS policy and shutdown channel; keeping
// those boundaries explicit prevents accidental policy sharing between Agent, Central and peer I/O.
#[allow(clippy::too_many_arguments)]
fn spawn_listener(
    listeners: &mut JoinSet<std::io::Result<()>>,
    listener: TcpListener,
    role: ListenerRole,
    config: &GatewayConfig,
    tunnel: Arc<GatewayTunnel>,
    s3_read_channels: Arc<s3_read_channel::S3ReadChannelRegistry>,
    bootstrap: Option<Arc<GatewayBootstrap>>,
    tls_acceptor: Option<TlsAcceptor>,
    lifecycle: GatewayLifecycle,
    shutdown: watch::Receiver<bool>,
) {
    let state = ListenerState {
        role,
        edge_cluster_id: neoengram_domain::protocol::EdgeClusterId::new(&config.edge_cluster_id)
            .expect("GatewayConfig was validated before listener construction"),
        gateway_pool_id: neoengram_domain::protocol::GatewayPoolId::new(&config.gateway_pool_id)
            .expect("GatewayConfig was validated before listener construction"),
        tunnel,
        s3_read_channels,
        request_admission: Arc::new(Semaphore::new(config.max_in_flight_requests_per_listener)),
        max_request_bytes: config.max_request_bytes,
        request_deadline: Duration::from_millis(config.request_deadline_millis),
        tls_acceptor,
        bootstrap,
        workload_trust_domain: config.workload_trust_domain.clone().map(Arc::<str>::from),
        lifecycle,
    };
    let connection_admission = Arc::new(Semaphore::new(config.max_connections_per_listener));
    listeners.spawn(serve_listener(
        listener,
        state,
        connection_admission,
        shutdown,
    ));
}

async fn serve_listener(
    listener: TcpListener,
    state: ListenerState,
    connection_admission: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    info!(
        listener = state.role.as_str(),
        address = %listener.local_addr()?,
        "Gateway listener started"
    );
    loop {
        let (mut socket, peer_address) = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            accepted = listener.accept() => accepted?,
        };
        let permit = match connection_admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    listener = state.role.as_str(),
                    %peer_address,
                    "Gateway connection admission limit reached"
                );
                let _ = socket.shutdown().await;
                continue;
            }
        };
        let state = state.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(serve_connection(
            socket,
            peer_address,
            state,
            permit,
            shutdown,
        ));
    }
}

async fn serve_connection(
    socket: TcpStream,
    peer_address: SocketAddr,
    state: ListenerState,
    _permit: OwnedSemaphorePermit,
    shutdown: watch::Receiver<bool>,
) {
    let role = state.role;
    let result = if let Some(acceptor) = state.tls_acceptor.clone() {
        let tls_stream = match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(socket))
            .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                warn!(listener = role.as_str(), %peer_address, %error, "Gateway TLS handshake failed");
                return;
            }
            Err(_) => {
                warn!(listener = role.as_str(), %peer_address, "Gateway TLS handshake timed out");
                return;
            }
        };
        let peer_certificates = tls_stream.get_ref().1.peer_certificates();
        let peer_auth = match validate_peer_certificate(&state, peer_certificates) {
            Ok(identity) => identity,
            Err(error) => {
                warn!(listener = role.as_str(), %peer_address, %error, "Gateway peer identity rejected");
                return;
            }
        };
        let certificate_deadline = match peer_certificates
            .map(certificate_not_after_deadline)
            .transpose()
        {
            Ok(deadline) => deadline,
            Err(error) => {
                warn!(listener = role.as_str(), %peer_address, %error, "Gateway peer certificate is expired or has an invalid validity window");
                return;
            }
        };
        serve_http_connection(tls_stream, state, peer_auth, certificate_deadline, shutdown).await
    } else {
        // Plaintext listeners are restricted to loopback by transport validation and are only
        // supported for local development.
        serve_http_connection(socket, state, PeerAuth::Development, None, shutdown).await
    };
    if let Err(error) = result {
        error!(
            listener = role.as_str(),
            %peer_address,
            %error,
            "Gateway connection failed"
        );
    }
}

async fn serve_http_connection<I>(
    io: I,
    state: ListenerState,
    peer_auth: PeerAuth,
    certificate_deadline: Option<Instant>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request| {
        let request_id = request.headers().get("x-request-id").cloned();
        let state = state.clone();
        let peer_auth = peer_auth.clone();
        async move {
            let mut response = handle_request(request, state, peer_auth).await?;
            if let Some(request_id) = request_id {
                response.headers_mut().insert("x-request-id", request_id);
            }
            Ok::<_, Infallible>(response)
        }
    });
    let mut builder = ConnectionBuilder::new(TokioExecutor::new());
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(CONTROL_H2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(CONTROL_H2_KEEPALIVE_TIMEOUT);
    let connection = builder.serve_connection(TokioIo::new(io), service);
    tokio::pin!(connection);
    if let Some(deadline) = certificate_deadline {
        tokio::select! {
            result = &mut connection => result,
            changed = shutdown.changed() => {
                if changed.is_ok() && !*shutdown.borrow() {
                    connection.await
                } else {
                    Ok(())
                }
            }
            _ = tokio::time::sleep_until(deadline.into()) => Ok(()),
        }
    } else {
        tokio::select! {
            result = &mut connection => result,
            changed = shutdown.changed() => {
                if changed.is_ok() && !*shutdown.borrow() {
                    connection.await
                } else {
                    Ok(())
                }
            }
        }
    }
}

async fn handle_request<B>(
    request: Request<B>,
    state: ListenerState,
    peer_auth: PeerAuth,
) -> Result<Response<GatewayBody>, Infallible>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
{
    if request.method() == Method::GET && request.uri().path() == "/health/live" {
        return Ok(json_response(
            StatusCode::OK,
            JSON_CONTENT_TYPE,
            serde_json::json!({
                "service": "neoengram-gateway",
                "listener": state.role.as_str(),
                "status": "live"
            }),
        ));
    }
    if request.method() == Method::GET && request.uri().path() == "/health/ready" {
        if state.lifecycle.is_draining() {
            return Ok(problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway_draining",
                "Gateway is draining",
                "The Gateway stopped admitting traffic before shutdown",
            ));
        }
        if let Some(bootstrap) = &state.bootstrap {
            if bootstrap.restart_required().await {
                return Ok(problem_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gateway_restart_required",
                    "Gateway restart required",
                    "A workload certificate was installed; restart the Gateway with the workload certificate configured",
                ));
            }
        }
        if state.tunnel.is_ready().await {
            return Ok(json_response(
                StatusCode::OK,
                JSON_CONTENT_TYPE,
                serde_json::json!({
                    "service": "neoengram-gateway",
                    "listener": state.role.as_str(),
                    "status": "ready"
                }),
            ));
        }
        return Ok(problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway_not_ready",
            "Gateway is not ready",
            "No authenticated Central control session is active",
        ));
    }

    if state.lifecycle.is_draining() {
        return Ok(unavailable_response(
            "Gateway is draining and does not accept new requests",
        ));
    }

    // TLS performs the same role check during normal listener setup, but keep the application
    // boundary fail-closed as well. This protects alternate transports and direct handler tests
    // from accidentally treating an Agent or Replica as Central.
    if matches!(state.role, ListenerRole::Control)
        && !matches!(peer_auth, PeerAuth::Central | PeerAuth::Development)
    {
        return Ok(problem_response(
            StatusCode::UNAUTHORIZED,
            "central_identity_required",
            "Central identity required",
            "Only Central or loopback development transport may use the Gateway control listener",
        ));
    }

    // Bootstrap is deliberately a server-authenticated, pre-activation channel.  A workload
    // certificate from another role must not be able to smuggle an authenticated request through
    // the anonymous bootstrap exception on the Agent listener.  Central's bootstrap client does
    // not present a client certificate; loopback development remains the only explicit exception.
    if matches!(state.role, ListenerRole::Agent)
        && is_gateway_bootstrap_path(request.uri().path())
        && !matches!(peer_auth, PeerAuth::Anonymous | PeerAuth::Development)
    {
        return Ok(problem_response(
            StatusCode::FORBIDDEN,
            "bootstrap_client_identity_rejected",
            "Bootstrap client identity rejected",
            "Gateway bootstrap endpoints accept only server-authenticated Central bootstrap traffic",
        ));
    }

    if let Some(bootstrap) = &state.bootstrap {
        if bootstrap.restart_required().await && !is_gateway_bootstrap_path(request.uri().path()) {
            return Ok(problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway_restart_required",
                "Gateway restart required",
                "A workload certificate was installed; restart the Gateway with the workload certificate configured",
            ));
        }
    }

    if matches!(state.role, ListenerRole::Agent)
        && !peer_auth.is_authenticated()
        && !anonymous_agent_path_allowed(request.uri().path())
    {
        return Ok(problem_response(
            StatusCode::UNAUTHORIZED,
            "workload_certificate_required",
            "Workload certificate required",
            "This Agent action requires an authenticated workload certificate",
        ));
    }

    // Replica activation is intentionally a server-authenticated bootstrap path.  A client
    // certificate is not a substitute for possession of the activation key/token, and accepting
    // an already-authenticated Agent, Central, or Replica here would let a workload identity
    // bypass the explicit bootstrap state machine.  Keep the development plaintext exception
    // for loopback test deployments only.
    if matches!(state.role, ListenerRole::Agent)
        && is_gateway_bootstrap_path(request.uri().path())
        && !matches!(peer_auth, PeerAuth::Anonymous | PeerAuth::Development)
    {
        return Ok(problem_response(
            StatusCode::FORBIDDEN,
            "bootstrap_identity_rejected",
            "Bootstrap identity rejected",
            "Gateway bootstrap requires the unauthenticated server-authenticated bootstrap path",
        ));
    }

    if content_length_exceeds(&request, state.max_request_bytes) {
        return Ok(problem_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "Request is too large",
            "The request body exceeds the configured Gateway limit",
        ));
    }
    let permit = match state.request_admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(unavailable_response(
                "Gateway request admission limit reached",
            ));
        }
    };
    if matches!(state.role, ListenerRole::Agent)
        && request.uri().path() == neoengram_domain::protocol::S3_READ_CHANNEL_PATH
    {
        let mut response = state
            .s3_read_channels
            .open_channel(request, peer_auth.agent_id())
            .await;
        if let Either::Right(body) = response.body_mut() {
            body.retain_request_permit(permit);
        } else {
            drop(permit);
        }
        return Ok(response);
    }
    if matches!(state.role, ListenerRole::Peer)
        && request.uri().path() == neoengram_domain::protocol::S3_READ_PEER_PATH
    {
        let (source_replica_id, certificate_fingerprint) = match &peer_auth {
            PeerAuth::GatewayReplica(replica_id) => (Some(replica_id.clone()), None),
            PeerAuth::GatewayReplicaWithCertificate {
                replica_id,
                certificate_fingerprint,
            } => (Some(replica_id.clone()), Some(*certificate_fingerprint)),
            PeerAuth::Development => (
                request
                    .headers()
                    .get("x-neoengram-source-replica")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| {
                        neoengram_domain::protocol::GatewayReplicaId::new(value).ok()
                    }),
                None,
            ),
            _ => (None, None),
        };
        let mut response = state
            .s3_read_channels
            .open_peer_stream(request, source_replica_id, certificate_fingerprint)
            .await;
        if let Either::Right(body) = response.body_mut() {
            body.retain_request_permit(permit);
        } else {
            drop(permit);
        }
        return Ok(response);
    }
    let result = match state.role {
        ListenerRole::Control
            if request.uri().path() == neoengram_domain::protocol::GATEWAY_CONTROL_CHANNEL_PATH =>
        {
            state.tunnel.open_control(request).await
        }
        ListenerRole::Agent if is_gateway_bootstrap_path(request.uri().path()) => {
            drop(permit);
            return Ok(handle_gateway_bootstrap(request, &state).await);
        }
        ListenerRole::Agent => {
            state
                .tunnel
                .forward_agent(
                    request,
                    peer_auth.agent_id(),
                    state.max_request_bytes,
                    state.request_deadline,
                )
                .await
        }
        ListenerRole::Peer
            if request.uri().path() == neoengram_domain::protocol::GATEWAY_PEER_FORWARD_PATH =>
        {
            let response = handle_peer_forward(request, &state, &peer_auth).await;
            drop(permit);
            return Ok(response);
        }
        ListenerRole::Peer => Err(tunnel::TunnelError::Invalid(
            "unknown Gateway peer action path",
        )),
        ListenerRole::Control => Err(tunnel::TunnelError::Invalid(
            "unknown Gateway control action path",
        )),
    };
    let mut response = result.unwrap_or_else(|error| error_response(&error));
    if let Either::Right(body) = response.body_mut() {
        body.retain_request_permit(permit);
    } else {
        drop(permit);
    }
    Ok(response)
}

async fn handle_peer_forward<B>(
    request: Request<B>,
    state: &ListenerState,
    peer_auth: &PeerAuth,
) -> Response<GatewayBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
{
    if request.method() != Method::POST
        || request.version() != http::Version::HTTP_2
        || !request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json_content_type)
    {
        return problem_response(
            StatusCode::BAD_REQUEST,
            "peer_protocol_invalid",
            "Invalid peer forwarding request",
            "Gateway peer forwarding requires an HTTP/2 JSON POST",
        );
    }
    let body = match tokio::time::timeout(
        state.request_deadline,
        collect_bootstrap_body(request.into_body(), state.max_request_bytes),
    )
    .await
    {
        Err(_) => {
            return problem_response(
                StatusCode::GATEWAY_TIMEOUT,
                "peer_deadline_exceeded",
                "Peer forwarding deadline exceeded",
                "The peer forwarding request exceeded its bounded deadline",
            );
        }
        Ok(Err(BootstrapBodyError::TooLarge)) => {
            return problem_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "peer_request_too_large",
                "Peer forwarding request is too large",
                "The peer forwarding frame exceeds the configured limit",
            );
        }
        Ok(Err(BootstrapBodyError::Read)) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "peer_body_invalid",
                "Peer forwarding request could not be read",
                "The peer forwarding request body could not be read",
            );
        }
        Ok(Ok(body)) => body,
    };
    let frame = match neoengram_domain::protocol::GatewayControlFrame::decode_json(&body) {
        Ok(frame) => frame,
        Err(_) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "peer_protocol_invalid",
                "Invalid peer forwarding frame",
                "The peer forwarding body is not a valid Gateway frame",
            );
        }
    };
    let source = match peer_auth {
        PeerAuth::GatewayReplica(replica_id) => (replica_id.clone(), None),
        PeerAuth::GatewayReplicaWithCertificate {
            replica_id,
            certificate_fingerprint,
        } => (replica_id.clone(), Some(certificate_fingerprint)),
        // Plaintext is only possible on loopback development listeners. It has no SAN to bind;
        // the frame identity is accepted solely to keep socket-free local development usable.
        PeerAuth::Development => (frame.gateway_replica_id.clone(), None),
        _ => {
            return problem_response(
                StatusCode::FORBIDDEN,
                "peer_identity_rejected",
                "Peer identity rejected",
                "Only a Gateway Replica workload may use the peer listener",
            );
        }
    };
    let request_id = frame.request_id.clone();
    let connection_id = frame.connection_id.clone();
    // Bound the potentially blocking Agent queue admission by both the listener budget and the
    // absolute frame deadline. A peer must not retain an in-flight request by advertising a far
    // future deadline or by leaving the owner stream's bounded queue full.
    let now = now_unix_ms();
    let remaining_ms = frame.deadline_unix_ms.get().saturating_sub(now.get());
    let budget = state
        .request_deadline
        .min(Duration::from_millis(remaining_ms));
    let result = match tokio::time::timeout(
        budget,
        state
            .tunnel
            .accept_peer_forward_with_fingerprint(frame, &source.0, source.1),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(neoengram_domain::protocol::GatewayControlError {
            code: neoengram_domain::protocol::GatewayErrorCode::DeadlineExceeded,
            detail: "owner Agent stream delivery exceeded the bounded deadline".to_owned(),
            retryable: false,
        }),
    };
    let message = match result {
        Ok(accepted) => {
            neoengram_domain::protocol::GatewayControlMessage::PeerForwardAccepted(accepted)
        }
        Err(error) => neoengram_domain::protocol::GatewayControlMessage::Error(error),
    };
    let now = now_unix_ms();
    let response_frame = neoengram_domain::protocol::GatewayControlFrame {
        wire_version: neoengram_domain::protocol::CURRENT_WIRE_VERSION,
        gateway_pool_id: state.gateway_pool_id.clone(),
        gateway_replica_id: state.tunnel.identity().gateway_replica_id.clone(),
        connection_id,
        sequence: neoengram_domain::protocol::SequenceNumber::new(1),
        request_id,
        trace_id: None,
        sent_at_unix_ms: now,
        deadline_unix_ms: UnixMillis::new(now.get().saturating_add(10_000)),
        hop_count: 1,
        message,
        extensions: neoengram_domain::protocol::Extensions::new(),
    };
    match response_frame.encode_ndjson() {
        Ok(bytes) => json_response(
            StatusCode::OK,
            JSON_CONTENT_TYPE,
            serde_json::from_slice(&bytes[..bytes.len().saturating_sub(1)])
                .expect("validated peer response must serialize as JSON"),
        ),
        Err(_) => problem_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "peer_internal_error",
            "Peer forwarding response failed",
            "The Gateway could not encode the peer forwarding response",
        ),
    }
}

fn now_unix_ms() -> UnixMillis {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    UnixMillis::new(u64::try_from(millis).unwrap_or(u64::MAX))
}

/// Returns the local deadline at which a TLS client connection must be closed. Rustls validates
/// the peer certificate during the handshake, but an already-established HTTP/2 connection is
/// otherwise allowed to outlive the leaf certificate. Binding the connection lifetime to
/// `notAfter` makes certificate rotation and expiry effective for long-lived control streams.
fn certificate_not_after_deadline(
    peer_certificates: &[rustls::pki_types::CertificateDer<'_>],
) -> Result<Instant, String> {
    let leaf = peer_certificates
        .first()
        .ok_or_else(|| "the client certificate chain is empty".to_owned())?;
    let (_, certificate) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|error| format!("client certificate is invalid DER: {error}"))?;
    let not_after_seconds = certificate.validity().not_after.timestamp();
    let not_after_millis = i128::from(not_after_seconds)
        .checked_mul(1_000)
        .ok_or_else(|| "client certificate expiry timestamp overflowed".to_owned())?;
    let now_millis = i128::from(now_unix_ms().get());
    if not_after_millis <= now_millis {
        return Err("client certificate is expired".to_owned());
    }
    let remaining_millis = u64::try_from(not_after_millis - now_millis)
        .map_err(|_| "client certificate expiry duration is out of range".to_owned())?;
    Ok(Instant::now() + Duration::from_millis(remaining_millis))
}

/// Applies the identity checks that rustls deliberately leaves to the application. A trusted CA
/// is not sufficient: the leaf must carry clientAuth, Replica peer leaves must also carry
/// serverAuth, and every role must have exactly one role-specific SPIFFE URI SAN.
/// In particular, an Agent or Replica certificate from the shared workload CA cannot authenticate
/// as Central on the control listener.
fn validate_peer_certificate(
    state: &ListenerState,
    peer_certificates: Option<&[rustls::pki_types::CertificateDer<'_>]>,
) -> Result<PeerAuth, String> {
    let Some(peer_certificates) = peer_certificates else {
        return match state.role {
            ListenerRole::Agent => Ok(PeerAuth::Anonymous),
            ListenerRole::Control | ListenerRole::Peer => {
                Err("a client certificate is required on this listener".to_owned())
            }
        };
    };
    let leaf = peer_certificates
        .first()
        .ok_or_else(|| "the client certificate chain is empty".to_owned())?;
    let (remainder, certificate) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|error| format!("client certificate is invalid DER: {error}"))?;
    if !remainder.is_empty() {
        return Err("client certificate contains trailing DER bytes".to_owned());
    }
    let eku = certificate
        .extended_key_usage()
        .map_err(|error| format!("client certificate EKU is invalid: {error}"))?
        .ok_or_else(|| "client certificate has no extended key usage".to_owned())?;
    if !eku.value.client_auth {
        return Err("client certificate does not permit client authentication".to_owned());
    }
    if matches!(state.role, ListenerRole::Peer) && !eku.value.server_auth {
        return Err("Gateway peer certificate does not permit server authentication".to_owned());
    }
    let san = certificate
        .subject_alternative_name()
        .map_err(|error| format!("client certificate SAN is invalid: {error}"))?;
    let uris = san
        .map(|san| {
            san.value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::URI(uri) => Some(*uri),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    match state.role {
        ListenerRole::Agent => {
            let uri = uris
                .as_slice()
                .first()
                .copied()
                .ok_or_else(|| "Agent certificate must contain one URI SAN".to_owned())?;
            if uris.len() != 1 {
                return Err("Agent certificate must contain exactly one URI SAN".to_owned());
            }
            let agent_id = parse_agent_workload_uri(
                uri,
                &state.edge_cluster_id,
                state.workload_trust_domain.as_deref(),
            )?;
            Ok(PeerAuth::Agent(agent_id))
        }
        ListenerRole::Peer => {
            let uri =
                uris.as_slice().first().copied().ok_or_else(|| {
                    "Gateway peer certificate must contain one URI SAN".to_owned()
                })?;
            if uris.len() != 1 {
                return Err("Gateway peer certificate must contain exactly one URI SAN".to_owned());
            }
            let replica_id = parse_gateway_replica_uri(
                uri,
                &state.edge_cluster_id,
                &state.gateway_pool_id,
                state.workload_trust_domain.as_deref(),
            )?;
            Ok(PeerAuth::GatewayReplicaWithCertificate {
                replica_id,
                certificate_fingerprint: ContentDigest::hash(leaf.as_ref()),
            })
        }
        ListenerRole::Control => {
            let uri = uris
                .as_slice()
                .first()
                .copied()
                .ok_or_else(|| "Central certificate must contain one URI SAN".to_owned())?;
            if uris.len() != 1 {
                return Err("Central certificate must contain exactly one URI SAN".to_owned());
            }
            parse_central_workload_uri(uri, state.workload_trust_domain.as_deref())?;
            Ok(PeerAuth::Central)
        }
    }
}

fn parse_workload_uri(
    value: &str,
    expected_trust_domain: Option<&str>,
) -> Result<Vec<String>, String> {
    if value.contains(['?', '#', '%']) {
        return Err("workload URI SAN must not contain query, fragment, or escapes".to_owned());
    }
    let url = Url::parse(value).map_err(|error| format!("workload URI SAN is invalid: {error}"))?;
    if url.scheme() != "spiffe"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str() != value
    {
        return Err("workload URI SAN is not canonical SPIFFE syntax".to_owned());
    }
    let trust_domain = url
        .host_str()
        .ok_or_else(|| "workload URI SAN has no trust domain".to_owned())?;
    let expected = expected_trust_domain.ok_or_else(|| {
        "Gateway workload trust domain is required for certificate identity validation".to_owned()
    })?;
    if trust_domain != expected {
        return Err(
            "workload URI SAN trust domain does not match Gateway configuration".to_owned(),
        );
    }
    Ok(url
        .path_segments()
        .ok_or_else(|| "workload URI SAN has no path".to_owned())?
        .map(str::to_owned)
        .collect())
}

fn parse_agent_workload_uri(
    value: &str,
    expected_cluster: &neoengram_domain::protocol::EdgeClusterId,
    expected_trust_domain: Option<&str>,
) -> Result<neoengram_domain::protocol::AgentId, String> {
    let segments = parse_workload_uri(value, expected_trust_domain)?;
    if segments.len() != 5
        || segments[0] != "workloads"
        || segments[1] != "edge-clusters"
        || segments[2] != expected_cluster.as_str()
        || segments[3] != "agents"
    {
        return Err("Agent certificate URI SAN is outside this EdgeCluster".to_owned());
    }
    neoengram_domain::protocol::AgentId::new(&segments[4])
        .map_err(|_| "Agent certificate URI SAN contains an invalid AgentId".to_owned())
}

fn parse_central_workload_uri(
    value: &str,
    expected_trust_domain: Option<&str>,
) -> Result<(), String> {
    let segments = parse_workload_uri(value, expected_trust_domain)?;
    if segments.as_slice() != ["workloads", "central"] {
        return Err("Central certificate URI SAN is not the Central workload identity".to_owned());
    }
    Ok(())
}

fn parse_gateway_replica_uri(
    value: &str,
    expected_cluster: &neoengram_domain::protocol::EdgeClusterId,
    expected_pool: &neoengram_domain::protocol::GatewayPoolId,
    expected_trust_domain: Option<&str>,
) -> Result<neoengram_domain::protocol::GatewayReplicaId, String> {
    let segments = parse_workload_uri(value, expected_trust_domain)?;
    if segments.len() != 7
        || segments[0] != "workloads"
        || segments[1] != "edge-clusters"
        || segments[2] != expected_cluster.as_str()
        || segments[3] != "gateway-pools"
        || segments[4] != expected_pool.as_str()
        || segments[5] != "gateway-replicas"
    {
        return Err("Gateway peer certificate URI SAN is outside this GatewayPool".to_owned());
    }
    neoengram_domain::protocol::GatewayReplicaId::new(&segments[6])
        .map_err(|_| "Gateway peer certificate URI SAN contains an invalid ReplicaId".to_owned())
}

fn anonymous_agent_path_allowed(path: &str) -> bool {
    matches!(
        path,
        neoengram_domain::protocol::AGENT_ENROLLMENT_BOOTSTRAP_PATH
            | neoengram_domain::protocol::AGENT_ENROLLMENT_STATUS_QUERY_PATH
            | neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            | neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH
    )
}

fn is_gateway_bootstrap_path(path: &str) -> bool {
    matches!(
        path,
        neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            | neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH
    )
}

async fn handle_gateway_bootstrap<B>(
    request: Request<B>,
    state: &ListenerState,
) -> Response<GatewayBody>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
{
    if request.method() != Method::POST {
        return problem_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Method not allowed",
            "Gateway bootstrap endpoints require POST",
        );
    }
    if !request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_json_content_type)
    {
        return problem_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Unsupported media type",
            "Gateway bootstrap endpoints require application/json",
        );
    }
    let Some(bootstrap) = &state.bootstrap else {
        return problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "bootstrap_not_configured",
            "Gateway bootstrap is unavailable",
            "This Gateway Replica has no bootstrap identity configured",
        );
    };
    let path = request.uri().path().to_owned();
    let body = match tokio::time::timeout(
        state.request_deadline,
        collect_bootstrap_body(request.into_body(), state.max_request_bytes),
    )
    .await
    {
        Err(_) => {
            return problem_response(
                StatusCode::GATEWAY_TIMEOUT,
                "deadline_exceeded",
                "Gateway bootstrap deadline exceeded",
                "The Gateway bootstrap request exceeded its deadline",
            );
        }
        Ok(Err(BootstrapBodyError::TooLarge)) => {
            return problem_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Request is too large",
                "The Gateway bootstrap request exceeds its size limit",
            );
        }
        Ok(Err(BootstrapBodyError::Read)) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "Gateway bootstrap request is invalid",
                "The Gateway bootstrap request body could not be read",
            );
        }
        Ok(Ok(body)) => body,
    };
    let result = if path == neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH {
        bootstrap.prove(&body).await.map(|proof| {
            json_response(
                StatusCode::OK,
                JSON_CONTENT_TYPE,
                serde_json::to_value(proof)
                    .expect("validated Gateway bootstrap proof must serialize"),
            )
        })
    } else {
        bootstrap.install_certificate(&body).await.map(|()| {
            json_response(
                StatusCode::OK,
                JSON_CONTENT_TYPE,
                serde_json::json!({ "installed": true }),
            )
        })
    };
    result.unwrap_or_else(bootstrap_error_response)
}

fn is_json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(JSON_CONTENT_TYPE))
}

#[derive(Debug, Clone, Copy)]
enum BootstrapBodyError {
    TooLarge,
    Read,
}

async fn collect_bootstrap_body<B>(
    mut body: B,
    max_bytes: usize,
) -> Result<Vec<u8>, BootstrapBodyError>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn Error + Send + Sync>> + Send + Sync + 'static,
{
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| BootstrapBodyError::Read)?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        if collected.len().saturating_add(bytes.len()) > max_bytes {
            return Err(BootstrapBodyError::TooLarge);
        }
        collected.extend_from_slice(&bytes);
    }
    Ok(collected)
}

fn bootstrap_error_response(error: BootstrapError) -> Response<GatewayBody> {
    match error {
        BootstrapError::Invalid(_) => problem_response(
            StatusCode::BAD_REQUEST,
            "bootstrap_invalid",
            "Gateway bootstrap request is invalid",
            "The Gateway bootstrap request failed validation",
        ),
        BootstrapError::CredentialRejected => problem_response(
            StatusCode::FORBIDDEN,
            "bootstrap_credential_rejected",
            "Gateway bootstrap credential rejected",
            "The Gateway bootstrap credential or identity was rejected",
        ),
        BootstrapError::Expired => problem_response(
            StatusCode::GONE,
            "bootstrap_challenge_expired",
            "Gateway bootstrap challenge expired",
            "The Gateway bootstrap challenge is no longer valid",
        ),
        BootstrapError::ChallengeInProgress
        | BootstrapError::AlreadyActivated
        | BootstrapError::NoChallenge => problem_response(
            StatusCode::CONFLICT,
            "bootstrap_conflict",
            "Gateway bootstrap state conflict",
            "The Gateway bootstrap request does not match the current activation state",
        ),
        BootstrapError::Configuration(_) | BootstrapError::Key | BootstrapError::Persistence(_) => {
            problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "bootstrap_unavailable",
                "Gateway bootstrap is unavailable",
                "The Gateway could not complete the bootstrap operation",
            )
        }
    }
}

fn content_length_exceeds<B: Body>(request: &Request<B>, max_bytes: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > max_bytes as u64)
        || request
            .body()
            .size_hint()
            .upper()
            .is_some_and(|length| length > max_bytes as u64)
}

fn unavailable_response(detail: &'static str) -> Response<GatewayBody> {
    let mut response = problem_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "Gateway route is unavailable",
        detail,
    );
    response
        .headers_mut()
        .insert(RETRY_AFTER, http::HeaderValue::from_static("1"));
    response
}

fn problem_response(
    status: StatusCode,
    code: &'static str,
    title: &'static str,
    detail: &'static str,
) -> Response<GatewayBody> {
    json_response(
        status,
        "application/problem+json",
        serde_json::json!({
            "type": format!("https://neoengram.dev/problems/{code}"),
            "title": title,
            "status": status.as_u16(),
            "code": code,
            "detail": detail
        }),
    )
}

async fn shutdown_signal(
    lifecycle: GatewayLifecycle,
    tunnel: Arc<GatewayTunnel>,
    drain_duration: Duration,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;
        let mut drain = signal(SignalKind::user_defined1())?;
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        loop {
            tokio::select! {
                result = &mut interrupt => return result,
                _ = terminate.recv() => return Ok(()),
                received = drain.recv() => {
                    if received.is_none() {
                        return Err(std::io::Error::other("Gateway drain signal stream closed"));
                    }
                    if lifecycle.begin_drain() {
                        let deadline_unix_ms = UnixMillis::new(
                            now_unix_ms().get().saturating_add(
                                u64::try_from(drain_duration.as_millis()).unwrap_or(u64::MAX),
                            ),
                        );
                        match tunnel
                            .begin_drain(deadline_unix_ms, "Gateway process shutdown")
                            .await
                        {
                            Ok(()) => info!(
                                deadline_unix_ms = deadline_unix_ms.get(),
                                "Gateway drain announced to Central; waiting for termination signal"
                            ),
                            Err(error) => warn!(
                                %error,
                                deadline_unix_ms = deadline_unix_ms.get(),
                                "Gateway drain is fenced locally but Central notification failed"
                            ),
                        }
                    }
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (lifecycle, tunnel, drain_duration);
        tokio::signal::ctrl_c().await
    }
}

#[cfg(unix)]
fn run_pre_stop_drain(drain_duration: Duration) -> std::io::Result<()> {
    use rustix::process::{kill_process, Pid, Signal};

    kill_process(Pid::INIT, Signal::USR1)
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
    std::thread::sleep(drain_duration);
    Ok(())
}

#[cfg(not(unix))]
fn run_pre_stop_drain(_drain_duration: Duration) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "the Kubernetes preStop drain helper requires Unix signals",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};

    fn test_tunnel() -> Arc<GatewayTunnel> {
        Arc::new(GatewayTunnel::new(GatewayIdentity {
            edge_cluster_id: neoengram_domain::protocol::EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: neoengram_domain::protocol::GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: neoengram_domain::protocol::GatewayReplicaId::new("replica-a")
                .unwrap(),
            software_version: "test".to_owned(),
        }))
    }

    fn state(role: ListenerRole, max_request_bytes: usize) -> ListenerState {
        let tunnel = test_tunnel();
        ListenerState {
            role,
            edge_cluster_id: neoengram_domain::protocol::EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: neoengram_domain::protocol::GatewayPoolId::new("pool-a").unwrap(),
            tunnel: tunnel.clone(),
            s3_read_channels: Arc::new(s3_read_channel::S3ReadChannelRegistry::new(tunnel)),
            request_admission: Arc::new(Semaphore::new(1)),
            max_request_bytes,
            request_deadline: Duration::from_secs(1),
            tls_acceptor: None,
            bootstrap: None,
            workload_trust_domain: None,
            lifecycle: GatewayLifecycle::default(),
        }
    }

    #[test]
    fn configuration_requires_distinct_bounded_listeners() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "127.0.0.1:8081".parse().unwrap();
        config.control_listen = "127.0.0.1:8082".parse().unwrap();
        config.peer_listen = "127.0.0.1:8083".parse().unwrap();
        assert!(config.public_listen.is_none());
        config.validate().unwrap();
        config.public_listen = Some(config.agent_listen);
        assert!(config.validate().is_err());
        config.public_listen = None;
        config.peer_listen = config.agent_listen;
        assert!(config.validate().is_err());
        config.peer_listen = DEFAULT_PEER_LISTEN.parse().unwrap();
        config.max_request_bytes = MAX_CONFIGURED_REQUEST_BYTES + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn transfer_listener_requires_dedicated_mtls_material() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "127.0.0.1:8081".parse().unwrap();
        config.control_listen = "127.0.0.1:8082".parse().unwrap();
        config.peer_listen = "127.0.0.1:8083".parse().unwrap();
        config.transfer_listen = Some("127.0.0.1:8084".parse().unwrap());
        config.workload_trust_domain = Some("mesh.example.test".to_owned());
        assert!(config.validate().is_err());

        config.transfer_transport.transfer_tls_certificate_file =
            Some("/transfer-listener.crt".into());
        config.transfer_transport.transfer_tls_private_key_file =
            Some("/transfer-listener.key".into());
        config.transfer_transport.transfer_tls_client_ca_file = Some("/transfer-ca.crt".into());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn transfer_relay_requires_a_complete_role_route_and_generation_tuple() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
            "--transfer-listen",
            "127.0.0.1:8084",
            "--transfer-relay-role",
            "target",
            "--transfer-tls-certificate-file",
            "/transfer-listener.crt",
            "--transfer-tls-private-key-file",
            "/transfer-listener.key",
            "--transfer-tls-client-ca-file",
            "/transfer-ca.crt",
            "--workload-trust-domain",
            "mesh.example.test",
        ])
        .unwrap();
        config.agent_listen = "127.0.0.1:8081".parse().unwrap();
        config.control_listen = "127.0.0.1:8082".parse().unwrap();
        config.peer_listen = "127.0.0.1:8083".parse().unwrap();
        assert!(config.validate().is_err());

        config.transfer_upstream = Some("127.0.0.1:8184".parse().unwrap());
        config.transfer_upstream_server_name = Some("localhost".to_owned());
        // The route tuple is learned from the Central-authorized Agent channel. A relay may
        // therefore start without static generations and remains fail-closed until channel.opened.
        config.validate().unwrap();
        config.transfer_session_generation = Some(3);
        config.transfer_mount_generation = Some(4);
        assert!(config.validate().is_err());
        config.transfer_route_generation = Some(5);
        config.validate().unwrap();

        config.transfer_upstream = None;
        config.transfer_upstream_routes = vec![TransferUpstreamRoute {
            agent_id: "agent-source".to_owned(),
            upstream: "127.0.0.1:8185".parse().unwrap(),
        }];
        config.validate().unwrap();
        config.transfer_upstream_routes.push(TransferUpstreamRoute {
            agent_id: "agent-source".to_owned(),
            upstream: "127.0.0.1:8186".parse().unwrap(),
        });
        assert!(config.validate().is_err());

        config.bootstrap.private_key_file = Some("/bootstrap-private-key.pem".into());
        config.bootstrap.activation_token_file = Some("/bootstrap-token".into());
        config.bootstrap.certificate_chain_file = Some("/bootstrap-chain.pem".into());
        assert!(config.validate().is_err());

        config.transfer_relay_role = None;
        config.transfer_upstream = None;
        config.transfer_upstream_server_name = None;
        config.transfer_upstream_routes.clear();
        config.transfer_session_generation = None;
        config.transfer_mount_generation = None;
        config.transfer_route_generation = None;
        config.validate().unwrap();
    }

    #[test]
    fn public_hostnames_are_compared_after_dns_normalization() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "127.0.0.1:8081".parse().unwrap();
        config.control_listen = "127.0.0.1:8082".parse().unwrap();
        config.peer_listen = "127.0.0.1:8083".parse().unwrap();
        config.console_host = "Console.Example.Test".to_owned();
        config.s3_host = "console.example.test.".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn pre_stop_drain_helper_has_a_bounded_wait() {
        let config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
            "--pre-stop-drain",
            "--pre-stop-drain-seconds",
            "17",
        ])
        .unwrap();
        assert!(config.pre_stop_drain);
        assert_eq!(
            config.pre_stop_drain_duration().unwrap(),
            Duration::from_secs(17)
        );
    }

    #[test]
    fn json_content_type_requires_one_exact_media_type() {
        assert!(is_json_content_type("application/json"));
        assert!(is_json_content_type("Application/JSON; charset=utf-8"));
        assert!(!is_json_content_type("application/jsonx"));
        assert!(!is_json_content_type("text/application/json"));
    }

    #[test]
    fn exposed_or_tls_gateway_requires_a_workload_trust_domain() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "0.0.0.0:8081".parse().unwrap();
        config.control_listen = "0.0.0.0:8082".parse().unwrap();
        config.peer_listen = "0.0.0.0:8083".parse().unwrap();
        assert!(config.validate().is_err());

        config.agent_listen = "127.0.0.1:8081".parse().unwrap();
        config.control_listen = "127.0.0.1:8082".parse().unwrap();
        config.peer_listen = "127.0.0.1:8083".parse().unwrap();
        config.transport.tls_certificate_file = Some("/listener.crt".into());
        config.transport.tls_private_key_file = Some("/listener.key".into());
        config.transport.tls_client_ca_file = Some("/workload-ca.crt".into());
        assert!(config.validate().is_err());

        config.workload_trust_domain = Some("mesh.example.test".to_owned());
        config.agent_listen = "0.0.0.0:8081".parse().unwrap();
        config.control_listen = "0.0.0.0:8082".parse().unwrap();
        config.peer_listen = "0.0.0.0:8083".parse().unwrap();
        config.validate().unwrap();
        assert!(config.bootstrap.private_key_file.is_none());
        assert!(config.bootstrap.activation_token_file.is_none());
        assert!(config.bootstrap.certificate_chain_file.is_none());
    }

    #[test]
    fn exposed_gateway_rejects_plain_central_upstream() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "0.0.0.0:8081".parse().unwrap();
        config.control_listen = "0.0.0.0:8082".parse().unwrap();
        config.peer_listen = "0.0.0.0:8083".parse().unwrap();
        config.public_listen = Some("0.0.0.0:8080".parse().unwrap());
        config.public_tls_certificate_file = Some("/public.crt".into());
        config.public_tls_private_key_file = Some("/public.key".into());
        config.transport.tls_certificate_file = Some("/listener.crt".into());
        config.transport.tls_private_key_file = Some("/listener.key".into());
        config.transport.tls_client_ca_file = Some("/workload-ca.crt".into());
        config.workload_trust_domain = Some("mesh.example.test".to_owned());
        assert!(config.validate().is_err());
    }

    #[test]
    fn https_central_upstream_requires_workload_client_material() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.public_listen = Some("127.0.0.1:8080".parse().unwrap());
        config.central_upstream = "https://central.example.test:8080".parse().unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn workload_only_gateway_does_not_validate_unused_central_upstream() {
        let mut config = GatewayConfig::try_parse_from([
            "neoengram-gateway",
            "--edge-cluster-id",
            "cluster-a",
            "--gateway-pool-id",
            "pool-a",
            "--gateway-replica-id",
            "replica-a",
        ])
        .unwrap();
        config.agent_listen = "0.0.0.0:8081".parse().unwrap();
        config.control_listen = "0.0.0.0:8082".parse().unwrap();
        config.peer_listen = "0.0.0.0:8083".parse().unwrap();
        config.transport.tls_certificate_file = Some("/listener.crt".into());
        config.transport.tls_private_key_file = Some("/listener.key".into());
        config.transport.tls_client_ca_file = Some("/workload-ca.crt".into());
        config.workload_trust_domain = Some("mesh.example.test".to_owned());

        assert!(config.public_listen.is_none());
        assert_eq!(config.central_upstream.scheme(), "http");
        config.validate().unwrap();
    }

    #[test]
    fn workload_uri_identity_is_bound_to_trust_domain_cluster_and_role() {
        let cluster = neoengram_domain::protocol::EdgeClusterId::new("cluster-a").unwrap();
        let pool = neoengram_domain::protocol::GatewayPoolId::new("pool-a").unwrap();
        parse_central_workload_uri(
            "spiffe://mesh.example.test/workloads/central",
            Some("mesh.example.test"),
        )
        .unwrap();
        assert!(parse_central_workload_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/agents/agent-a",
            Some("mesh.example.test"),
        )
        .is_err());
        let agent = parse_agent_workload_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/agents/agent-a",
            &cluster,
            Some("mesh.example.test"),
        )
        .unwrap();
        assert_eq!(agent.as_str(), "agent-a");
        assert!(parse_agent_workload_uri(
            "spiffe://other.example.test/workloads/edge-clusters/cluster-a/agents/agent-a",
            &cluster,
            Some("mesh.example.test"),
        )
        .is_err());
        assert!(parse_agent_workload_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-b/agents/agent-a",
            &cluster,
            Some("mesh.example.test"),
        )
        .is_err());
        assert!(parse_agent_workload_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/agents/agent-a",
            &cluster,
            None,
        )
        .is_err());
        let peer = parse_gateway_replica_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-b",
            &cluster,
            &pool,
            Some("mesh.example.test"),
        )
        .unwrap();
        assert_eq!(peer.as_str(), "replica-b");
        assert!(parse_gateway_replica_uri(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-b/gateway-replicas/replica-b",
            &cluster,
            &pool,
            Some("mesh.example.test"),
        )
        .is_err());
    }

    #[test]
    fn control_listener_rejects_non_central_workload_certificates() {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        fn certificate(uri: &str) -> rustls_pki_types::CertificateDer<'static> {
            let mut parameters =
                CertificateParams::new(Vec::<String>::new()).expect("certificate parameters");
            parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
            parameters
                .subject_alt_names
                .push(SanType::URI(uri.try_into().expect("workload URI SAN")));
            let key = KeyPair::generate().expect("certificate key");
            rustls_pki_types::CertificateDer::from(
                parameters
                    .self_signed(&key)
                    .expect("workload certificate")
                    .der()
                    .to_vec(),
            )
        }

        let mut control = state(ListenerRole::Control, 16);
        control.workload_trust_domain = Some(Arc::<str>::from("mesh.example.test"));
        let central = certificate("spiffe://mesh.example.test/workloads/central");
        assert!(matches!(
            validate_peer_certificate(&control, Some(std::slice::from_ref(&central))),
            Ok(PeerAuth::Central)
        ));
        assert!(
            certificate_not_after_deadline(std::slice::from_ref(&central)).unwrap()
                > Instant::now()
        );

        let agent = certificate(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/agents/agent-a",
        );
        assert!(validate_peer_certificate(&control, Some(std::slice::from_ref(&agent))).is_err());
        let replica = certificate(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a",
        );
        assert!(validate_peer_certificate(&control, Some(std::slice::from_ref(&replica))).is_err());
    }

    #[test]
    fn peer_listener_requires_both_client_and_server_auth_eku() {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        fn certificate(with_server_auth: bool) -> rustls_pki_types::CertificateDer<'static> {
            let mut parameters =
                CertificateParams::new(Vec::<String>::new()).expect("certificate parameters");
            parameters.extended_key_usages = if with_server_auth {
                vec![
                    ExtendedKeyUsagePurpose::ClientAuth,
                    ExtendedKeyUsagePurpose::ServerAuth,
                ]
            } else {
                vec![ExtendedKeyUsagePurpose::ClientAuth]
            };
            parameters.subject_alt_names.push(SanType::URI(
                "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-b"
                    .try_into()
                    .expect("workload URI SAN"),
            ));
            let key = KeyPair::generate().expect("certificate key");
            rustls_pki_types::CertificateDer::from(
                parameters
                    .self_signed(&key)
                    .expect("workload certificate")
                    .der()
                    .to_vec(),
            )
        }

        let mut peer = state(ListenerRole::Peer, 16);
        peer.workload_trust_domain = Some(Arc::<str>::from("mesh.example.test"));
        let client_only = certificate(false);
        assert!(
            validate_peer_certificate(&peer, Some(std::slice::from_ref(&client_only))).is_err()
        );

        let mutual = certificate(true);
        assert!(matches!(
            validate_peer_certificate(&peer, Some(std::slice::from_ref(&mutual))),
            Ok(PeerAuth::GatewayReplicaWithCertificate { .. })
        ));
    }

    #[tokio::test]
    async fn http2_connection_is_closed_at_the_client_certificate_deadline() {
        use hyper::client::conn::http2;
        use hyper_util::rt::{TokioExecutor, TokioIo};

        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (_shutdown, receiver) = watch::channel(false);
        let server = tokio::spawn(serve_http_connection(
            server_io,
            state(ListenerRole::Agent, 1024),
            PeerAuth::Development,
            Some(Instant::now() + Duration::from_millis(100)),
            receiver,
        ));
        let (mut sender, connection) =
            http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
                .await
                .unwrap();
        let driver = tokio::spawn(connection);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/health/live")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("certificate deadline must close the server connection")
            .unwrap()
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .expect("client H2 driver must observe the certificate-bound close")
            .unwrap();
        assert!(sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/health/live")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn liveness_is_available_but_readiness_fails_closed() {
        let live = handle_request(
            Request::builder()
                .uri("/health/live")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state(ListenerRole::Agent, 16),
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(live.status(), StatusCode::OK);

        let ready = handle_request(
            Request::builder()
                .uri("/health/ready")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state(ListenerRole::Control, 16),
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn bootstrap_paths_reject_authenticated_non_bootstrap_identities() {
        let response = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri(neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH)
                .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
                .body(Full::new(Bytes::from_static(b"{}")))
                .unwrap(),
            state(ListenerRole::Agent, 1024),
            PeerAuth::Central,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(problem["code"], "bootstrap_client_identity_rejected");

        let development = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri(neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH)
                .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
                .body(Full::new(Bytes::from_static(b"{}")))
                .unwrap(),
            state(ListenerRole::Agent, 1024),
            PeerAuth::Development,
        )
        .await
        .unwrap();
        // Development is permitted through the identity gate; the test state simply has no
        // bootstrap material configured, so the handler reports that configuration boundary.
        assert_eq!(development.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn draining_keeps_liveness_and_fails_readiness_closed() {
        let draining = state(ListenerRole::Agent, 16);
        assert!(draining.lifecycle.begin_drain());
        assert!(!draining.lifecycle.begin_drain());

        let live = handle_request(
            Request::builder()
                .uri("/health/live")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            draining.clone(),
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(live.status(), StatusCode::OK);

        let protocol = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri(neoengram_domain::protocol::AGENT_SESSION_OPEN_PATH)
                .body(Full::new(Bytes::new()))
                .unwrap(),
            draining.clone(),
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(protocol.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(protocol.headers()[RETRY_AFTER], "1");

        let ready = handle_request(
            Request::builder()
                .uri("/health/ready")
                .body(Full::new(Bytes::new()))
                .unwrap(),
            draining,
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = ready.into_body().collect().await.unwrap().to_bytes();
        let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(problem["code"], "gateway_draining");
    }

    #[tokio::test]
    async fn protocol_requests_are_bounded_and_unavailable() {
        let oversized = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri("/gateway/control")
                .body(Full::new(Bytes::from_static(b"too large")))
                .unwrap(),
            state(ListenerRole::Peer, 4),
            PeerAuth::GatewayReplica(
                neoengram_domain::protocol::GatewayReplicaId::new("replica-b").unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let unavailable = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri("/gateway/control")
                .body(Full::new(Bytes::from_static(b"{}")))
                .unwrap(),
            state(ListenerRole::Peer, 4),
            PeerAuth::GatewayReplica(
                neoengram_domain::protocol::GatewayReplicaId::new("replica-b").unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(unavailable.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(unavailable.headers().get(RETRY_AFTER).is_none());
    }

    #[tokio::test]
    async fn streaming_response_holds_request_admission_until_drop() {
        let state = state(ListenerRole::Control, 1024);
        let admission = state.request_admission.clone();
        let response = handle_request(
            Request::builder()
                .method(Method::POST)
                .version(http::Version::HTTP_2)
                .uri(neoengram_domain::protocol::GATEWAY_CONTROL_CHANNEL_PATH)
                .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
                .body(Full::new(Bytes::new()))
                .unwrap(),
            state,
            PeerAuth::Central,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(admission.clone().try_acquire_owned().is_err());
        drop(response);
        assert!(admission.try_acquire_owned().is_ok());
    }

    #[tokio::test]
    async fn control_listener_rejects_anonymous_agent_and_replica_auth() {
        for peer_auth in [
            PeerAuth::Anonymous,
            PeerAuth::Agent(neoengram_domain::protocol::AgentId::new("agent-a").unwrap()),
            PeerAuth::GatewayReplica(
                neoengram_domain::protocol::GatewayReplicaId::new("replica-a").unwrap(),
            ),
        ] {
            let response = handle_request(
                Request::builder()
                    .method(Method::POST)
                    .uri(neoengram_domain::protocol::GATEWAY_CONTROL_CHANNEL_PATH)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
                state(ListenerRole::Control, 1024),
                peer_auth,
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
                "central_identity_required"
            );
        }
    }

    #[tokio::test]
    async fn anonymous_agent_requests_are_limited_to_enrollment() {
        let protected = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri(neoengram_domain::protocol::AGENT_SESSION_HEARTBEAT_REPORT_PATH)
                .body(Full::new(Bytes::from_static(b"{}")))
                .unwrap(),
            state(ListenerRole::Agent, 16),
            PeerAuth::Anonymous,
        )
        .await
        .unwrap();
        assert_eq!(protected.status(), StatusCode::UNAUTHORIZED);
        let body = protected.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["code"],
            "workload_certificate_required"
        );

        for path in [
            neoengram_domain::protocol::AGENT_ENROLLMENT_BOOTSTRAP_PATH,
            neoengram_domain::protocol::AGENT_ENROLLMENT_STATUS_QUERY_PATH,
            neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH,
            neoengram_domain::protocol::GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH,
        ] {
            let enrollment = handle_request(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .body(Full::new(Bytes::from_static(b"{}")))
                    .unwrap(),
                state(ListenerRole::Agent, 16),
                PeerAuth::Anonymous,
            )
            .await
            .unwrap();
            assert_ne!(enrollment.status(), StatusCode::UNAUTHORIZED);
        }
    }
}
