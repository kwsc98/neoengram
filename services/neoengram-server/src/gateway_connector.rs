use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt, fs,
    future::Future,
    io,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use http::{
    header::{ACCEPT, CONTENT_TYPE},
    Request, Version,
};
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    client::conn::http2,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use neoengram_protocol::{
    CertificateGeneration, ContentDigest, GatewayControlFrame, GatewayControlMessage,
    GatewayControlNdjsonDecoder, GatewayReplicaId, ProtocolVersion, RequestId, UnixMillis,
    AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS, AGENT_ROUTE_LEASE_TTL_MS, GATEWAY_CONTROL_CHANNEL_PATH,
};
use neoengramd::{
    GatewayCredentialState, GatewayRegistryRepository, GatewayReplicaRecord, GatewayReplicaState,
};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
    time::{sleep, timeout, Sleep},
};
use tokio_rustls::TlsConnector;
use url::{Position, Url};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::{
    gateway_transport::{
        AuthenticatedGatewayReplica, CentralGatewayControl, CentralGatewaySession,
    },
    service::{parse_workload_identity_from_certificate_der, WorkloadIdentity},
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(2);
// The Gateway consumes the first immediate interval tick before its heartbeat loop, so the
// first heartbeat normally arrives one renew interval after hello. Two intervals leave room for
// startup scheduling while still fencing a connection that never completes its hello.
const GATEWAY_HELLO_TIMEOUT: Duration =
    Duration::from_millis(AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS * 2);
// This is deliberately the same bound used for Agent route leases. A Replica that misses this
// entire window is no longer considered a live control owner, even if a half-open H2 stream stays
// allocated in the client.
const GATEWAY_HEARTBEAT_IDLE_TIMEOUT: Duration = Duration::from_millis(AGENT_ROUTE_LEASE_TTL_MS);
const GATEWAY_FRAME_ACCEPT_TIMEOUT: Duration = Duration::from_secs(10);
// Keep the H2 transport itself live even when no Central command is in flight. This bounds a
// half-open Gateway socket on both sides, so Central's reconnect cannot be blocked by a stale
// request body that never observes a FIN.
const CONTROL_H2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const CONTROL_H2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_BODY_BUFFER: usize = 256;
const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
const MAX_TLS_CA_BYTES: u64 = 1024 * 1024;
const MAX_TLS_CERTIFICATE_BYTES: u64 = 1024 * 1024;
const MAX_TLS_PRIVATE_KEY_BYTES: u64 = 64 * 1024;

trait GatewayControlIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> GatewayControlIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Enforces the verified Gateway leaf deadline in Hyper's executor-owned H2 socket task. The
/// public client `Connection` future is only a dispatcher; active streams can keep the actual H2
/// I/O task alive after it is dropped, so the underlying TLS stream itself must fail at expiry.
struct CertificateBoundIo<I> {
    inner: I,
    deadline: Option<Pin<Box<Sleep>>>,
    expired: bool,
}

impl<I> CertificateBoundIo<I> {
    fn new(inner: I, deadline: Option<Instant>) -> Self {
        Self {
            inner,
            deadline: deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline.into()))),
            expired: false,
        }
    }

    fn poll_expired(&mut self, context: &mut Context<'_>) -> bool {
        if !self.expired
            && self
                .deadline
                .as_mut()
                .is_some_and(|deadline| deadline.as_mut().poll(context).is_ready())
        {
            self.expired = true;
        }
        self.expired
    }

    fn expired_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Gateway server certificate expired",
        )
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for CertificateBoundIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.poll_expired(context) {
            return Poll::Ready(Err(Self::expired_error()));
        }
        Pin::new(&mut this.inner).poll_read(context, buffer)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for CertificateBoundIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if this.poll_expired(context) {
            return Poll::Ready(Err(Self::expired_error()));
        }
        Pin::new(&mut this.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.poll_expired(context) {
            return Poll::Ready(Err(Self::expired_error()));
        }
        Pin::new(&mut this.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.poll_expired(context) {
            return Poll::Ready(Err(Self::expired_error()));
        }
        Pin::new(&mut this.inner).poll_shutdown(context)
    }
}

/// TLS material and endpoint policy used by Central's outbound Gateway connector.
///
/// The client configuration is built once at process startup and shared by all replica
/// connectors. It always validates the Gateway certificate against the configured CA and the
/// endpoint server name. In development, callers may explicitly construct a loopback-only
/// plaintext policy; production callers must load a complete mTLS identity.
#[derive(Clone)]
pub struct GatewayConnectorConfig {
    tls: Option<Arc<ClientConfig>>,
    workload_trust_domain: Option<Arc<str>>,
    allow_loopback_http: bool,
}

impl fmt::Debug for GatewayConnectorConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayConnectorConfig")
            .field("tls_configured", &self.tls.is_some())
            .field("workload_trust_domain", &self.workload_trust_domain)
            .field("allow_loopback_http", &self.allow_loopback_http)
            .finish()
    }
}

impl GatewayConnectorConfig {
    /// Creates the explicit development policy. Plaintext is accepted only for loopback
    /// endpoints; public or non-loopback HTTP is rejected by `parse_endpoint`.
    #[must_use]
    pub fn loopback_development() -> Self {
        Self {
            tls: None,
            workload_trust_domain: None,
            allow_loopback_http: true,
        }
    }

    /// Loads a complete Central client mTLS identity and trust bundle from bounded PEM files.
    pub fn load_mtls(
        ca_file: &Path,
        certificate_file: &Path,
        private_key_file: &Path,
        workload_trust_domain: impl Into<String>,
        allow_loopback_http: bool,
    ) -> Result<Self, GatewayConnectorError> {
        let ca_pem = read_tls_file(ca_file, "CA bundle", MAX_TLS_CA_BYTES)?;
        let certificate_pem = read_tls_file(
            certificate_file,
            "client certificate",
            MAX_TLS_CERTIFICATE_BYTES,
        )?;
        let private_key_pem = read_tls_file(
            private_key_file,
            "client private key",
            MAX_TLS_PRIVATE_KEY_BYTES,
        )?;
        let ca_certificates = CertificateDer::pem_slice_iter(&ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                GatewayConnectorError::TlsConfiguration(format!(
                    "CA bundle is not valid PEM: {error}"
                ))
            })?;
        if ca_certificates.is_empty() {
            return Err(GatewayConnectorError::TlsConfiguration(
                "CA bundle contains no certificates".into(),
            ));
        }
        let client_certificates = CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                GatewayConnectorError::TlsConfiguration(format!(
                    "client certificate is not valid PEM: {error}"
                ))
            })?;
        if client_certificates.is_empty() {
            return Err(GatewayConnectorError::TlsConfiguration(
                "client certificate contains no certificates".into(),
            ));
        }
        let mut private_keys = PrivateKeyDer::pem_slice_iter(&private_key_pem);
        let private_key = private_keys
            .next()
            .ok_or_else(|| {
                GatewayConnectorError::TlsConfiguration(
                    "client private key contains no supported PEM key".into(),
                )
            })?
            .map_err(|error| {
                GatewayConnectorError::TlsConfiguration(format!(
                    "client private key is not a supported PEM key: {error}"
                ))
            })?;
        if private_keys.next().is_some() {
            return Err(GatewayConnectorError::TlsConfiguration(
                "client private key must contain exactly one PEM key".into(),
            ));
        }
        let mut roots = RootCertStore::empty();
        for certificate in ca_certificates {
            roots.add(certificate).map_err(|error| {
                GatewayConnectorError::TlsConfiguration(format!(
                    "CA bundle contains an invalid trust anchor: {error}"
                ))
            })?;
        }
        let workload_trust_domain = workload_trust_domain.into();
        validate_workload_trust_domain(&workload_trust_domain)?;
        let mut tls = ClientConfig::builder_with_provider(
            rustls::crypto::aws_lc_rs::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .map_err(|error| {
            GatewayConnectorError::TlsConfiguration(format!(
                "Central TLS protocol versions are invalid: {error}"
            ))
        })?
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certificates, private_key)
        .map_err(|error| {
            GatewayConnectorError::TlsConfiguration(format!(
                "Central client TLS identity is invalid: {error}"
            ))
        })?;
        // A reconnect after leaf rotation must obtain and verify the new Gateway chain. Resumed
        // sessions carry the previous peer chain without running certificate verification again.
        tls.resumption = rustls::client::Resumption::disabled();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Self {
            tls: Some(Arc::new(tls)),
            workload_trust_domain: Some(Arc::<str>::from(workload_trust_domain)),
            allow_loopback_http,
        })
    }

    fn tls(&self) -> Option<Arc<ClientConfig>> {
        self.tls.clone()
    }

    fn workload_trust_domain(&self) -> Option<&str> {
        self.workload_trust_domain.as_deref()
    }

    fn allows_loopback_http(&self) -> bool {
        self.allow_loopback_http
    }
}

/// Central-side stream body. The receiver stays bounded so a slow Gateway applies H2 backpressure
/// to Central's control producer instead of accumulating frames in an unbounded task queue.
struct CentralStreamingBody {
    frames: mpsc::Receiver<Bytes>,
}

impl Body for CentralStreamingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.get_mut().frames)
            .poll_recv(context)
            .map(|frame| frame.map(|bytes| Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_closed() && self.frames.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayConnectorError {
    #[error("Gateway endpoint is invalid: {0}")]
    Endpoint(String),
    #[error("only loopback HTTP Gateway endpoints are permitted without mTLS")]
    InsecureEndpoint,
    #[error("Gateway HTTPS endpoint requires the configured Central mTLS identity")]
    MissingTlsConfiguration,
    #[error("Gateway TLS configuration is invalid: {0}")]
    TlsConfiguration(String),
    #[error("Gateway TLS handshake failed: {0}")]
    TlsHandshake(String),
    #[error("Gateway workload identity is invalid: {0}")]
    PeerIdentity(String),
    #[error("Gateway TCP connection failed: {0}")]
    Connect(String),
    #[error("Gateway HTTP/2 handshake failed: {0}")]
    Handshake(String),
    #[error("Gateway Replica hello timed out")]
    HelloTimeout,
    #[error("Gateway returned an invalid control stream: {0}")]
    Protocol(String),
    #[error("Gateway control session failed: {0}")]
    Session(String),
    #[error("Gateway Replica heartbeat idle timeout")]
    HeartbeatTimeout,
    #[error("Gateway server certificate expired")]
    ServerCertificateExpired,
}

/// A supervisor that discovers Active Gateway replicas belonging to Ready Pools from the Central
/// registry and maintains one outbound control connection per replica. It deliberately never
/// accepts an endpoint list from a caller; the SQLite/Postgres-backed registry remains the
/// authority.
pub struct RunningGatewayConnector {
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for RunningGatewayConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningGatewayConnector")
            .finish_non_exhaustive()
    }
}

impl RunningGatewayConnector {
    pub fn start(
        registry: Arc<dyn GatewayRegistryRepository>,
        control: Arc<CentralGatewayControl>,
        poll_interval: Duration,
        connector_config: GatewayConnectorConfig,
    ) -> Self {
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(supervise(
            registry,
            control,
            poll_interval,
            connector_config,
            receiver,
        ));
        Self {
            shutdown,
            task: Mutex::new(Some(task)),
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

async fn supervise(
    registry: Arc<dyn GatewayRegistryRepository>,
    control: Arc<CentralGatewayControl>,
    poll_interval: Duration,
    connector_config: GatewayConnectorConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut running: BTreeMap<GatewayReplicaId, RunningReplicaConnector> = BTreeMap::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        match discover_replicas(&registry).await {
            Ok(replicas) => {
                reconcile_connectors(&mut running, replicas, |replica| {
                    let connector = ReplicaConnector {
                        replica,
                        control: control.clone(),
                        config: connector_config.clone(),
                    };
                    tokio::spawn(async move {
                        connector.run().await;
                    })
                })
                .await;
            }
            Err(error) => tracing::warn!(%error, "Gateway replica discovery failed"),
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = sleep(poll_interval) => {}
        }
    }
    for (_, running) in running {
        running.task.abort();
        let _ = running.task.await;
    }
}

/// The part of a Replica record that determines the identity of a Central-side connection.
///
/// Registry `resource_version` is deliberately not included. Replica heartbeats update that
/// version as an observation watermark; treating every heartbeat as a connection replacement would
/// make a healthy session reconnect on every discovery poll. Endpoint and certificate changes are
/// persisted through the same CAS and are tracked explicitly here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicaConnectionRevision {
    edge_cluster_id: neoengram_protocol::EdgeClusterId,
    gateway_pool_id: neoengram_protocol::GatewayPoolId,
    control_endpoint: String,
    peer_endpoint: String,
    bootstrap_endpoint: String,
    software_version: String,
    supported_protocol_versions: BTreeSet<ProtocolVersion>,
    capabilities: BTreeSet<String>,
    certificate_generation: CertificateGeneration,
}

impl ReplicaConnectionRevision {
    fn from_record(record: &GatewayReplicaRecord) -> Option<Self> {
        if record.state != GatewayReplicaState::Active
            || record.credential.state != GatewayCredentialState::Active
        {
            return None;
        }
        Some(Self {
            edge_cluster_id: record.edge_cluster_id.clone(),
            gateway_pool_id: record.gateway_pool_id.clone(),
            control_endpoint: record.control_endpoint.clone(),
            peer_endpoint: record.peer_endpoint.clone(),
            bootstrap_endpoint: record.bootstrap_endpoint.clone(),
            software_version: record.software_version.clone(),
            supported_protocol_versions: record.supported_protocol_versions.clone(),
            capabilities: record.capabilities.clone(),
            certificate_generation: record.credential.certificate_generation?,
        })
    }
}

struct RunningReplicaConnector {
    revision: ReplicaConnectionRevision,
    task: JoinHandle<()>,
}

/// Reconciles connection tasks against one successful, Central-authoritative Registry snapshot.
///
/// The spawner is injected so the lifecycle policy can be tested without opening sockets. In
/// production it creates a `ReplicaConnector` task. A task is cancelled before a replacement is
/// started, which fences the old endpoint/generation and prevents duplicate Central sessions.
async fn reconcile_connectors<F>(
    running: &mut BTreeMap<GatewayReplicaId, RunningReplicaConnector>,
    replicas: Vec<GatewayReplicaRecord>,
    mut spawn: F,
) where
    F: FnMut(GatewayReplicaRecord) -> JoinHandle<()>,
{
    let desired = replicas
        .into_iter()
        .filter_map(|replica| {
            let revision = ReplicaConnectionRevision::from_record(&replica)?;
            Some((replica.gateway_replica_id.clone(), (revision, replica)))
        })
        .collect::<BTreeMap<_, _>>();

    let stale = running
        .iter()
        .filter_map(|(id, current)| {
            let replacement = desired.get(id);
            (current.task.is_finished()
                || replacement.is_none_or(|(revision, _)| *revision != current.revision))
            .then(|| id.clone())
        })
        .collect::<Vec<_>>();

    for id in stale {
        if let Some(current) = running.remove(&id) {
            if !current.task.is_finished() {
                current.task.abort();
            }
            let _ = current.task.await;
        }
    }

    for (id, (revision, replica)) in desired {
        if running.contains_key(&id) {
            continue;
        }
        running.insert(
            id,
            RunningReplicaConnector {
                revision,
                task: spawn(replica),
            },
        );
    }
}

async fn discover_replicas(
    registry: &Arc<dyn GatewayRegistryRepository>,
) -> Result<Vec<GatewayReplicaRecord>, neoengramd::CentralError> {
    let mut replicas = Vec::new();
    let mut pool_after = None;
    loop {
        let pools = registry
            .list_pools(&neoengramd::GatewayPoolListRequest {
                edge_cluster_id: None,
                // A Replica endpoint is only connectable while its owning Pool is Ready.  The
                // state predicate is evaluated by the authoritative repository, rather than
                // filtering an all-state snapshot in process memory.
                state: Some(neoengramd::GatewayPoolState::Ready),
                after: pool_after,
                limit: neoengramd::GATEWAY_REGISTRY_MAX_PAGE_SIZE,
            })
            .await?;
        let pool_count = pools.len();
        let next_pool_after = pools.last().map(|pool| pool.gateway_pool_id.clone());
        for pool in pools {
            let mut pool_replicas = Vec::new();
            let mut replica_after = None;
            loop {
                let mut page = registry
                    .list_replicas(&neoengramd::GatewayReplicaListRequest {
                        gateway_pool_id: pool.gateway_pool_id.clone(),
                        state: Some(GatewayReplicaState::Active),
                        after: replica_after,
                        limit: neoengramd::GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                    })
                    .await?;
                let page_count = page.len();
                replica_after = page.last().map(|record| record.gateway_replica_id.clone());
                pool_replicas.append(&mut page);
                if page_count < neoengramd::GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                    break;
                }
            }

            // Pool state can change while the paginated Replica snapshot is being read.  Re-read
            // the Pool before admitting any endpoint from that snapshot.  A changed resource
            // version is treated as a race even when the state is still Ready; the next poll will
            // obtain a coherent snapshot.  If the Pool disappeared or stopped being Ready, the
            // whole Pool is omitted and existing sessions are reconciled away on this poll.
            let current_pool = registry.get_pool(&pool.gateway_pool_id).await?;
            if pool_snapshot_is_current_ready(&pool, current_pool.as_ref()) {
                replicas.extend(pool_replicas.into_iter().filter(|replica| {
                    replica.gateway_pool_id == pool.gateway_pool_id
                        && replica.edge_cluster_id == pool.edge_cluster_id
                }));
            }
        }
        pool_after = next_pool_after;
        if pool_count < neoengramd::GATEWAY_REGISTRY_MAX_PAGE_SIZE {
            break;
        }
    }
    Ok(replicas)
}

fn pool_snapshot_is_current_ready(
    observed: &neoengramd::GatewayPoolRecord,
    current: Option<&neoengramd::GatewayPoolRecord>,
) -> bool {
    current.is_some_and(|current| {
        current.state == neoengramd::GatewayPoolState::Ready
            && current.gateway_pool_id == observed.gateway_pool_id
            && current.edge_cluster_id == observed.edge_cluster_id
            && current.resource_version == observed.resource_version
    })
}

struct ReplicaConnector {
    replica: GatewayReplicaRecord,
    control: Arc<CentralGatewayControl>,
    config: GatewayConnectorConfig,
}

struct GatewaySessionCloseGuard(Arc<CentralGatewaySession>);

impl Drop for GatewaySessionCloseGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct AbortTaskOnDrop(Option<JoinHandle<()>>);

impl AbortTaskOnDrop {
    fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }
}

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

impl ReplicaConnector {
    async fn run(self) {
        loop {
            match self.connect_once().await {
                Ok(()) => {
                    tracing::info!(replica_id = %self.replica.gateway_replica_id, "Gateway control session ended")
                }
                Err(error) => {
                    tracing::warn!(replica_id = %self.replica.gateway_replica_id, %error, "Gateway control connection failed")
                }
            }
            sleep(DEFAULT_RECONNECT_DELAY).await;
            // A new registry generation/state is picked up by the supervisor before a future
            // task is spawned. This task remains bounded to the persisted Replica identity.
        }
    }

    async fn connect_once(&self) -> Result<(), GatewayConnectorError> {
        let endpoint = parse_endpoint(
            &self.replica.control_endpoint,
            self.config.allows_loopback_http(),
        )?;
        let host = endpoint_host(&endpoint).ok_or_else(|| {
            GatewayConnectorError::Endpoint("Gateway endpoint has no host".into())
        })?;
        let port = endpoint.port_or_known_default().ok_or_else(|| {
            GatewayConnectorError::Endpoint("Gateway endpoint has no port".into())
        })?;
        let stream = timeout(
            DEFAULT_CONNECT_TIMEOUT,
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| GatewayConnectorError::Connect("TCP connect timed out".into()))?
        .map_err(|error| GatewayConnectorError::Connect(error.to_string()))?;
        let (stream, identity, certificate_deadline) = match endpoint.scheme() {
            "http" => (
                Box::new(stream) as Box<dyn GatewayControlIo>,
                authenticated_replica_from_registry(&self.replica)?,
                None,
            ),
            "https" => {
                let config = self
                    .config
                    .tls()
                    .ok_or(GatewayConnectorError::MissingTlsConfiguration)?;
                let server_name = ServerName::try_from(host.clone()).map_err(|error| {
                    GatewayConnectorError::Endpoint(format!(
                        "Gateway endpoint has an invalid TLS server name: {error}"
                    ))
                })?;
                let tls_stream = timeout(
                    DEFAULT_CONNECT_TIMEOUT,
                    TlsConnector::from(config).connect(server_name, stream),
                )
                .await
                .map_err(|_| GatewayConnectorError::TlsHandshake("TLS handshake timed out".into()))?
                .map_err(|error| GatewayConnectorError::TlsHandshake(error.to_string()))?;
                let peer_certificates = tls_stream.get_ref().1.peer_certificates();
                let identity = authenticated_replica_from_tls(
                    &self.replica,
                    peer_certificates,
                    self.config.workload_trust_domain().ok_or_else(|| {
                        GatewayConnectorError::TlsConfiguration(
                            "workload trust domain is missing".into(),
                        )
                    })?,
                )?;
                let certificate_deadline = gateway_server_certificate_deadline(
                    peer_certificates,
                    self.replica.credential.certificate_not_after_unix_ms,
                )?;
                (
                    Box::new(tls_stream) as Box<dyn GatewayControlIo>,
                    identity,
                    Some(certificate_deadline),
                )
            }
            _ => {
                return Err(GatewayConnectorError::Endpoint(
                    "Gateway endpoint must use HTTP or HTTPS".into(),
                ));
            }
        };
        let stream = CertificateBoundIo::new(stream, certificate_deadline);
        let mut handshake = http2::Builder::new(TokioExecutor::new());
        handshake
            .keep_alive_interval(CONTROL_H2_KEEPALIVE_INTERVAL)
            .keep_alive_timeout(CONTROL_H2_KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        let (mut sender, connection) = timeout(
            DEFAULT_CONNECT_TIMEOUT,
            handshake.handshake(TokioIo::new(stream)),
        )
        .await
        .map_err(|_| GatewayConnectorError::Handshake("HTTP/2 handshake timed out".into()))?
        .map_err(|error| GatewayConnectorError::Handshake(error.to_string()))?;
        let _connection_task = AbortTaskOnDrop::new(tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "Gateway H2 connection driver ended");
            }
        }));

        let (outgoing, outgoing_body) = mpsc::channel(CONTROL_BODY_BUFFER);
        let request_id = RequestId::new(format!(
            "central-gateway-{}",
            self.replica.gateway_replica_id
        ))
        .map_err(|error| GatewayConnectorError::Protocol(error.to_string()))?;
        let uri = endpoint
            .join(GATEWAY_CONTROL_CHANNEL_PATH)
            .map_err(|error| GatewayConnectorError::Endpoint(error.to_string()))?;
        let request = Request::builder()
            .method("POST")
            .uri(uri.as_str())
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .header(ACCEPT, NDJSON_CONTENT_TYPE)
            .header("x-request-id", request_id.as_str())
            .body(CentralStreamingBody {
                frames: outgoing_body,
            })
            .map_err(|error| GatewayConnectorError::Protocol(error.to_string()))?;
        let mut response: hyper::Response<Incoming> = before_server_certificate_expiry(
            async {
                timeout(DEFAULT_CONNECT_TIMEOUT, sender.send_request(request))
                    .await
                    .map_err(|_| {
                        GatewayConnectorError::Handshake("Gateway response timed out".into())
                    })?
                    .map_err(|error| GatewayConnectorError::Connect(error.to_string()))
            },
            certificate_deadline,
        )
        .await?;
        if !response.status().is_success() || response.version() != Version::HTTP_2 {
            return Err(GatewayConnectorError::Protocol(format!(
                "Gateway control response status/version rejected: {} {:?}",
                response.status(),
                response.version()
            )));
        }
        let mut decoder = GatewayControlNdjsonDecoder::new();
        let (hello, initial_frames) = before_server_certificate_expiry(
            read_initial_gateway_frames(response.body_mut(), &mut decoder),
            certificate_deadline,
        )
        .await?;
        let connection_id = hello.connection_id.clone();
        let (session, mut central_frames) = before_server_certificate_expiry(
            async {
                self.control
                    .open(identity, hello)
                    .await
                    .map_err(|error| GatewayConnectorError::Session(error.to_string()))
            },
            certificate_deadline,
        )
        .await?;
        let _session_close = GatewaySessionCloseGuard(session.clone());
        // Install Central's short-lived peer credential directory before the sender task starts.
        // A Gateway that has not received this snapshot must fail peer forwarding closed; the
        // first directory therefore precedes every subsequent heartbeat or application frame on
        // this control stream.
        before_server_certificate_expiry(
            async {
                session
                    .send_peer_directory()
                    .await
                    .map_err(|error| GatewayConnectorError::Session(error.to_string()))
            },
            certificate_deadline,
        )
        .await?;
        let mut sender_fence = session.subscribe_fence();
        let sender_task = tokio::spawn(async move {
            loop {
                if *sender_fence.borrow() {
                    break;
                }
                let frame = tokio::select! {
                    biased;
                    changed = sender_fence.changed() => {
                        if changed.is_err() || *sender_fence.borrow() {
                            break;
                        }
                        continue;
                    }
                    frame = central_frames.recv() => {
                        let Some(frame) = frame else { break; };
                        frame
                    }
                };
                let Ok(bytes) = frame.encode_ndjson() else {
                    break;
                };
                let sent = tokio::select! {
                    biased;
                    changed = sender_fence.changed() => {
                        if changed.is_err() || *sender_fence.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = outgoing.send(Bytes::from(bytes)) => result,
                };
                if sent.is_err() {
                    break;
                }
            }
        });

        let mut reader_fence = session.subscribe_fence();
        let mut heartbeat_watchdog =
            GatewayHeartbeatWatchdog::new(Instant::now(), GATEWAY_HEARTBEAT_IDLE_TIMEOUT);
        let session_result = before_server_certificate_expiry(
            async {
                for frame in initial_frames {
                    if frame.connection_id != connection_id {
                        return Err(GatewayConnectorError::Protocol(
                            "Gateway changed control connection identity".into(),
                        ));
                    }
                    let is_heartbeat = is_replica_heartbeat(&frame.message);
                    timeout(
                        GATEWAY_FRAME_ACCEPT_TIMEOUT,
                        session.accept_from_connector(frame),
                    )
                    .await
                    .map_err(|_| {
                        GatewayConnectorError::Session(
                            "Gateway frame admission exceeded its bounded deadline".into(),
                        )
                    })?
                    .map_err(|error| GatewayConnectorError::Session(error.to_string()))?;
                    heartbeat_watchdog.observe(is_heartbeat, Instant::now());
                }

                loop {
                    let heartbeat_wait = heartbeat_watchdog.remaining(Instant::now());
                    if heartbeat_wait.is_zero() {
                        return Err(GatewayConnectorError::HeartbeatTimeout);
                    }
                    let Some(frame) = wait_for_gateway_body_frame(
                        response.body_mut(),
                        &mut reader_fence,
                        heartbeat_wait,
                    )
                    .await?
                    else {
                        break;
                    };
                    let Ok(bytes) = frame.into_data() else {
                        continue;
                    };
                    for frame in decode_gateway_frames(&mut decoder, &bytes)? {
                        if frame.connection_id != connection_id {
                            return Err(GatewayConnectorError::Protocol(
                                "Gateway changed control connection identity".into(),
                            ));
                        }
                        let is_heartbeat = is_replica_heartbeat(&frame.message);
                        timeout(
                            GATEWAY_FRAME_ACCEPT_TIMEOUT,
                            session.accept_from_connector(frame),
                        )
                        .await
                        .map_err(|_| {
                            GatewayConnectorError::Session(
                                "Gateway frame admission exceeded its bounded deadline".into(),
                            )
                        })?
                        .map_err(|error| GatewayConnectorError::Session(error.to_string()))?;
                        heartbeat_watchdog.observe(is_heartbeat, Instant::now());
                    }
                }
                decoder
                    .finish()
                    .map_err(|error| GatewayConnectorError::Protocol(error.to_string()))?;
                Ok(())
            },
            certificate_deadline,
        )
        .await;
        session.close();
        sender_task.abort();
        let _ = sender_task.await;
        session_result
    }
}

/// Bounds application processing as well as the certificate-bound socket. This prevents a hello,
/// route mutation, or already-buffered frame from being accepted after the server certificate
/// deadline.
async fn before_server_certificate_expiry<T, F>(
    future: F,
    certificate_deadline: Option<Instant>,
) -> Result<T, GatewayConnectorError>
where
    F: Future<Output = Result<T, GatewayConnectorError>>,
{
    if let Some(deadline) = certificate_deadline {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline.into()) => {
                Err(GatewayConnectorError::ServerCertificateExpired)
            }
            result = future => result,
        }
    } else {
        future.await
    }
}

/// Tracks the last *validated Replica heartbeat* for one Central control session.
///
/// Business frames intentionally do not refresh this watchdog. They prove that some bytes are
/// still moving, but they do not update the Registry's Replica health watermark; allowing them to
/// suppress the heartbeat timeout would let a broken heartbeat task keep an otherwise stale
/// Replica session allocated indefinitely. `observe` is called only after `CentralGatewaySession`
/// accepts the frame, so malformed or unauthenticated data never extends the deadline.
#[derive(Debug, Clone, Copy)]
struct GatewayHeartbeatWatchdog {
    last_heartbeat: Instant,
    idle_timeout: Duration,
}

impl GatewayHeartbeatWatchdog {
    fn new(now: Instant, idle_timeout: Duration) -> Self {
        Self {
            last_heartbeat: now,
            idle_timeout,
        }
    }

    fn observe(&mut self, is_heartbeat: bool, now: Instant) {
        if is_heartbeat {
            self.last_heartbeat = now;
        }
    }

    fn remaining(&self, now: Instant) -> Duration {
        self.idle_timeout
            .checked_sub(now.saturating_duration_since(self.last_heartbeat))
            .unwrap_or_default()
    }
}

fn is_replica_heartbeat(message: &GatewayControlMessage) -> bool {
    matches!(message, GatewayControlMessage::ReplicaHeartbeat(_))
}

async fn wait_for_gateway_body_frame<B>(
    body: &mut B,
    reader_fence: &mut watch::Receiver<bool>,
    heartbeat_wait: Duration,
) -> Result<Option<Frame<Bytes>>, GatewayConnectorError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    loop {
        let frame = tokio::select! {
            biased;
            changed = reader_fence.changed() => {
                if changed.is_err() || *reader_fence.borrow() {
                    return Err(GatewayConnectorError::Session(
                        "Gateway control session was fenced".into(),
                    ));
                }
                continue;
            }
            _ = sleep(heartbeat_wait) => {
                return Err(GatewayConnectorError::HeartbeatTimeout);
            }
            frame = body.frame() => frame,
        };
        return frame
            .map(|frame| frame.map_err(|error| GatewayConnectorError::Protocol(error.to_string())))
            .transpose();
    }
}

async fn read_initial_gateway_frames<B>(
    body: &mut B,
    decoder: &mut GatewayControlNdjsonDecoder,
) -> Result<(GatewayControlFrame, Vec<GatewayControlFrame>), GatewayConnectorError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    read_initial_gateway_frames_with_timeout(body, decoder, GATEWAY_HELLO_TIMEOUT).await
}

async fn read_initial_gateway_frames_with_timeout<B>(
    body: &mut B,
    decoder: &mut GatewayControlNdjsonDecoder,
    hello_timeout: Duration,
) -> Result<(GatewayControlFrame, Vec<GatewayControlFrame>), GatewayConnectorError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    timeout(
        hello_timeout,
        read_initial_gateway_frames_without_timeout(body, decoder),
    )
    .await
    .map_err(|_| GatewayConnectorError::HelloTimeout)?
}

async fn read_initial_gateway_frames_without_timeout<B>(
    body: &mut B,
    decoder: &mut GatewayControlNdjsonDecoder,
) -> Result<(GatewayControlFrame, Vec<GatewayControlFrame>), GatewayConnectorError>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let mut frames = Vec::new();
    while frames.is_empty() {
        let Some(frame) = body.frame().await else {
            return Err(GatewayConnectorError::Protocol(
                "Gateway closed before Replica hello".into(),
            ));
        };
        let frame = frame.map_err(|error| GatewayConnectorError::Protocol(error.to_string()))?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        frames.extend(decode_gateway_frames(decoder, &bytes)?);
    }
    let hello = frames.remove(0);
    Ok((hello, frames))
}

fn decode_gateway_frames(
    decoder: &mut GatewayControlNdjsonDecoder,
    bytes: &[u8],
) -> Result<Vec<GatewayControlFrame>, GatewayConnectorError> {
    decoder
        .push(bytes)
        .map_err(|error| GatewayConnectorError::Protocol(error.to_string()))?
        .into_iter()
        .map(|line| {
            GatewayControlFrame::decode_json(&line)
                .map_err(|error| GatewayConnectorError::Protocol(error.to_string()))
        })
        .collect()
}

fn parse_endpoint(value: &str, allow_loopback_http: bool) -> Result<Url, GatewayConnectorError> {
    let endpoint =
        Url::parse(value).map_err(|error| GatewayConnectorError::Endpoint(error.to_string()))?;
    if &endpoint[..Position::BeforePath] != value
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        return Err(GatewayConnectorError::Endpoint(
            "Gateway endpoint must be a canonical origin".into(),
        ));
    }
    if endpoint.scheme() == "http" {
        let loopback = is_loopback_host(&endpoint);
        if !allow_loopback_http || !loopback {
            return Err(GatewayConnectorError::InsecureEndpoint);
        }
    }
    if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
        return Err(GatewayConnectorError::Endpoint(
            "Gateway endpoint must use HTTP or HTTPS".into(),
        ));
    }
    Ok(endpoint)
}

fn is_loopback_host(endpoint: &Url) -> bool {
    endpoint.host().is_some_and(|host| match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    })
}

fn endpoint_host(endpoint: &Url) -> Option<String> {
    endpoint.host().map(|host| match host {
        url::Host::Domain(domain) => domain.to_owned(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => address.to_string(),
    })
}

fn read_tls_file(
    path: &Path,
    kind: &'static str,
    max_bytes: u64,
) -> Result<Vec<u8>, GatewayConnectorError> {
    let metadata = fs::metadata(path).map_err(|error| {
        GatewayConnectorError::TlsConfiguration(format!("{kind} could not be read: {error}"))
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(GatewayConnectorError::TlsConfiguration(format!(
            "{kind} must be a regular file containing 1..={max_bytes} bytes"
        )));
    }
    #[cfg(unix)]
    if kind == "client private key" {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(GatewayConnectorError::TlsConfiguration(
                "client private key must not be readable or writable by group/other users".into(),
            ));
        }
    }
    let bytes = fs::read(path).map_err(|error| {
        GatewayConnectorError::TlsConfiguration(format!("{kind} could not be read: {error}"))
    })?;
    if bytes.len() as u64 > max_bytes {
        return Err(GatewayConnectorError::TlsConfiguration(format!(
            "{kind} exceeds the {max_bytes} byte limit"
        )));
    }
    if kind != "client private key"
        && bytes
            .windows(b"PRIVATE KEY".len())
            .any(|part| part == b"PRIVATE KEY")
    {
        return Err(GatewayConnectorError::TlsConfiguration(format!(
            "{kind} must not contain private key material"
        )));
    }
    Ok(bytes)
}

fn validate_workload_trust_domain(value: &str) -> Result<(), GatewayConnectorError> {
    if value.trim() != value {
        return Err(GatewayConnectorError::TlsConfiguration(
            "workload trust domain must not contain surrounding whitespace".into(),
        ));
    }
    if value.is_empty() || value.len() > 253 || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(GatewayConnectorError::TlsConfiguration(
            "workload trust domain is not a valid DNS name".into(),
        ));
    }
    let valid = value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && label
                .bytes()
                .last()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    });
    if valid {
        Ok(())
    } else {
        Err(GatewayConnectorError::TlsConfiguration(
            "workload trust domain is not a valid DNS name".into(),
        ))
    }
}

fn authenticated_replica_from_registry(
    replica: &GatewayReplicaRecord,
) -> Result<AuthenticatedGatewayReplica, GatewayConnectorError> {
    let certificate_generation = replica.credential.certificate_generation.ok_or_else(|| {
        GatewayConnectorError::PeerIdentity("Replica has no certificate generation".into())
    })?;
    Ok(AuthenticatedGatewayReplica {
        edge_cluster_id: replica.edge_cluster_id.clone(),
        gateway_pool_id: replica.gateway_pool_id.clone(),
        gateway_replica_id: replica.gateway_replica_id.clone(),
        certificate_generation,
    })
}

/// Computes the earliest fail-closed deadline from the leaf observed in the TLS handshake and the
/// Central-authoritative Registry expiry. The Registry may be slightly earlier because X.509 time
/// is encoded at whole-second precision, so using the minimum preserves the issued policy window.
fn gateway_server_certificate_deadline(
    peer_certificates: Option<&[CertificateDer<'_>]>,
    registry_not_after: Option<UnixMillis>,
) -> Result<Instant, GatewayConnectorError> {
    let certificates = peer_certificates.ok_or_else(|| {
        GatewayConnectorError::PeerIdentity("Gateway did not present a peer certificate".into())
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        GatewayConnectorError::PeerIdentity("Gateway presented an empty certificate chain".into())
    })?;
    let (remainder, certificate) = X509Certificate::from_der(leaf.as_ref()).map_err(|error| {
        GatewayConnectorError::PeerIdentity(format!(
            "Gateway server certificate is invalid DER: {error}"
        ))
    })?;
    if !remainder.is_empty() {
        return Err(GatewayConnectorError::PeerIdentity(
            "Gateway server certificate contains trailing DER data".into(),
        ));
    }
    let certificate_not_after = i128::from(certificate.validity().not_after.timestamp())
        .checked_mul(1_000)
        .ok_or_else(|| {
            GatewayConnectorError::PeerIdentity(
                "Gateway server certificate expiry timestamp overflowed".into(),
            )
        })?;
    let registry_not_after = registry_not_after.ok_or_else(|| {
        GatewayConnectorError::PeerIdentity(
            "Gateway Registry record has no certificate expiry".into(),
        )
    })?;
    let not_after_millis = certificate_not_after.min(i128::from(registry_not_after.get()));
    let now_instant = Instant::now();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        GatewayConnectorError::TlsHandshake("system clock precedes Unix epoch".into())
    })?;
    let now_millis = i128::try_from(now.as_millis())
        .map_err(|_| GatewayConnectorError::TlsHandshake("system clock is out of range".into()))?;
    if not_after_millis <= now_millis {
        return Err(GatewayConnectorError::ServerCertificateExpired);
    }
    let remaining_millis = u64::try_from(not_after_millis - now_millis).map_err(|_| {
        GatewayConnectorError::PeerIdentity(
            "Gateway server certificate expiry duration is out of range".into(),
        )
    })?;
    now_instant
        .checked_add(Duration::from_millis(remaining_millis))
        .ok_or_else(|| {
            GatewayConnectorError::PeerIdentity(
                "Gateway server certificate deadline overflowed".into(),
            )
        })
}

/// Extracts and checks the Gateway workload URI SAN from the certificate that rustls has already
/// authenticated. The registry fingerprint is checked as an additional binding so a trusted CA
/// cannot present a different, otherwise-valid Replica certificate at this endpoint.
fn authenticated_replica_from_tls(
    replica: &GatewayReplicaRecord,
    peer_certificates: Option<&[CertificateDer<'_>]>,
    expected_trust_domain: &str,
) -> Result<AuthenticatedGatewayReplica, GatewayConnectorError> {
    let certificates = peer_certificates.ok_or_else(|| {
        GatewayConnectorError::PeerIdentity("Gateway did not present a peer certificate".into())
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        GatewayConnectorError::PeerIdentity("Gateway presented an empty certificate chain".into())
    })?;
    let expected_fingerprint = replica
        .credential
        .certificate_fingerprint
        .as_ref()
        .ok_or_else(|| {
            GatewayConnectorError::PeerIdentity(
                "Replica has no persisted certificate fingerprint".into(),
            )
        })?;
    if ContentDigest::hash(leaf.as_ref()) != *expected_fingerprint {
        return Err(GatewayConnectorError::PeerIdentity(
            "Gateway certificate fingerprint does not match the Registry record".into(),
        ));
    }
    let identity_uri =
        parse_workload_identity_from_certificate_der(leaf.as_ref(), expected_trust_domain)
            .map_err(|error| GatewayConnectorError::PeerIdentity(error.to_string()))?;
    let WorkloadIdentity::GatewayReplica {
        edge_cluster_id,
        gateway_pool_id,
        gateway_replica_id,
    } = identity_uri.identity()
    else {
        return Err(GatewayConnectorError::PeerIdentity(
            "Gateway certificate URI SAN is not a Gateway Replica identity".into(),
        ));
    };
    if edge_cluster_id != &replica.edge_cluster_id
        || gateway_pool_id != &replica.gateway_pool_id
        || gateway_replica_id != &replica.gateway_replica_id
    {
        return Err(GatewayConnectorError::PeerIdentity(
            "Gateway certificate identity does not match the Registry record".into(),
        ));
    }
    authenticated_replica_from_registry(replica)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        future,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use neoengram_core::ContentDigest;
    use neoengram_protocol::{
        Ed25519PublicKeySpki, EdgeClusterId, Extensions, GatewayOpaqueBytes, GatewayPoolId,
        Generation, PrincipalId, PrincipalKind, PrincipalRef, ResourceVersion, UnixMillis,
        PROTOCOL_VERSION_V1,
    };
    use neoengramd::{
        GatewayPoolRecord, GatewayPoolState, GatewayReplicaCertificateRecord,
        GatewayReplicaCredential, InMemoryGatewayRegistry,
    };

    use super::*;

    struct StopCounter(Arc<AtomicUsize>);

    impl Drop for StopCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PendingBody;

    impl Body for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let _ = (self, context);
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn hello_watchdog_rejects_a_half_open_stream_without_network_io() {
        let mut body = PendingBody;
        let mut decoder = GatewayControlNdjsonDecoder::new();
        let error = read_initial_gateway_frames_with_timeout(
            &mut body,
            &mut decoder,
            Duration::from_millis(1),
        )
        .await
        .expect_err("a stream that never emits hello must be bounded");
        assert!(matches!(error, GatewayConnectorError::HelloTimeout));
    }

    #[tokio::test]
    async fn heartbeat_watchdog_closes_a_half_open_session_without_network_io() {
        let mut body = PendingBody;
        let (_fence_sender, mut fence) = watch::channel(false);
        let error = wait_for_gateway_body_frame(&mut body, &mut fence, Duration::from_millis(1))
            .await
            .expect_err("an established stream without heartbeat must be bounded");
        assert!(matches!(error, GatewayConnectorError::HeartbeatTimeout));
    }

    #[tokio::test]
    async fn socket_and_application_work_stop_at_the_server_certificate_deadline() {
        use tokio::io::AsyncReadExt;

        let (client_io, _server_io) = tokio::io::duplex(1024);
        let mut client_io =
            CertificateBoundIo::new(client_io, Some(Instant::now() + Duration::from_millis(10)));
        let mut byte = [0_u8; 1];
        let error = tokio::time::timeout(Duration::from_secs(1), client_io.read(&mut byte))
            .await
            .expect("certificate deadline must wake a pending socket read")
            .expect_err("certificate deadline must fail the socket read");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

        let error = before_server_certificate_expiry(
            future::pending::<Result<(), GatewayConnectorError>>(),
            Some(Instant::now()),
        )
        .await
        .expect_err("application work must fail closed at the certificate deadline");
        assert!(matches!(
            error,
            GatewayConnectorError::ServerCertificateExpired
        ));
    }

    #[test]
    fn heartbeat_watchdog_only_refreshes_on_a_validated_replica_heartbeat() {
        use neoengram_protocol::GatewayBackpressure;

        let start = Instant::now();
        let idle_timeout = Duration::from_secs(30);
        let mut watchdog = GatewayHeartbeatWatchdog::new(start, idle_timeout);

        let business =
            GatewayControlMessage::Backpressure(GatewayBackpressure { retry_after_ms: 1 });
        watchdog.observe(
            is_replica_heartbeat(&business),
            start + Duration::from_secs(10),
        );
        assert_eq!(
            watchdog.remaining(start + Duration::from_secs(10)),
            Duration::from_secs(20)
        );

        let heartbeat =
            GatewayControlMessage::ReplicaHeartbeat(neoengram_protocol::GatewayReplicaHeartbeat {
                connected_agents: 0,
                active_streams: 0,
                queue_depth: 0,
            });
        watchdog.observe(
            is_replica_heartbeat(&heartbeat),
            start + Duration::from_secs(10),
        );
        assert_eq!(
            watchdog.remaining(start + Duration::from_secs(10)),
            idle_timeout
        );
        assert!(watchdog
            .remaining(start + Duration::from_secs(40))
            .is_zero());
    }

    fn pending_connector(started: &Arc<AtomicUsize>, stopped: &Arc<AtomicUsize>) -> JoinHandle<()> {
        started.fetch_add(1, Ordering::SeqCst);
        let stop_counter = StopCounter(stopped.clone());
        tokio::spawn(async move {
            let _stop_counter = stop_counter;
            future::pending::<()>().await;
        })
    }

    #[tokio::test]
    async fn discovery_connects_only_active_replicas_in_a_current_ready_pool() {
        let registry = Arc::new(InMemoryGatewayRegistry::new());
        let initial_pool = provisioning_pool();
        registry.insert_pool(initial_pool.clone()).await.unwrap();

        let mut pending = active_replica();
        pending.state = GatewayReplicaState::Pending;
        pending.credential.activation_consumed_at_unix_ms = None;
        pending.credential.public_key_fingerprint = None;
        pending.credential.certificate_generation = None;
        pending.credential.certificate_fingerprint = None;
        pending.credential.certificate_not_after_unix_ms = None;
        pending.credential.certificate = None;
        pending.credential.state = GatewayCredentialState::PendingActivation;
        pending.resource_version = ResourceVersion::new(1);
        pending.updated_at_unix_ms = UnixMillis::new(100);
        registry.insert_replica(pending.clone()).await.unwrap();

        let public_key = Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
        let leaf = GatewayOpaqueBytes::new(b"discovery-leaf".to_vec()).unwrap();
        let mut prepared = pending;
        prepared.credential.public_key_fingerprint = Some(public_key.fingerprint());
        prepared.credential.certificate_generation = Some(CertificateGeneration::new(1));
        prepared.credential.certificate_fingerprint = Some(ContentDigest::hash(leaf.as_bytes()));
        prepared.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(21_600_000));
        prepared.credential.certificate = Some(GatewayReplicaCertificateRecord {
            request_id: RequestId::new("discovery-certificate-request").unwrap(),
            public_key_spki: public_key,
            certificate_generation: CertificateGeneration::new(1),
            not_before_unix_ms: UnixMillis::new(150),
            not_after_unix_ms: UnixMillis::new(21_600_000),
            server_names: BTreeSet::new(),
            leaf_certificate_der: leaf,
            issuer_chain_der: vec![GatewayOpaqueBytes::new(b"discovery-issuer".to_vec()).unwrap()],
        });
        prepared.credential.state = GatewayCredentialState::PendingCertificateDelivery;
        prepared.resource_version = ResourceVersion::new(2);
        prepared.updated_at_unix_ms = UnixMillis::new(150);
        registry.replace_replica(1, prepared.clone()).await.unwrap();

        let mut active = prepared;
        active.state = GatewayReplicaState::Active;
        active.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(160));
        active.credential.state = GatewayCredentialState::Active;
        active.resource_version = ResourceVersion::new(3);
        active.updated_at_unix_ms = UnixMillis::new(160);
        registry.replace_replica(2, active).await.unwrap();

        let repository: Arc<dyn GatewayRegistryRepository> = registry.clone();
        assert!(discover_replicas(&repository).await.unwrap().is_empty());

        let mut ready = initial_pool;
        ready.state = GatewayPoolState::Ready;
        ready.config_generation = Generation::new(2);
        ready.resource_version = ResourceVersion::new(2);
        ready.updated_at_unix_ms = UnixMillis::new(200);
        registry.replace_pool(1, ready.clone()).await.unwrap();
        let discovered = discover_replicas(&repository).await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].gateway_replica_id, replica_id());

        ready.state = GatewayPoolState::Draining;
        ready.resource_version = ResourceVersion::new(3);
        ready.updated_at_unix_ms = UnixMillis::new(300);
        registry.replace_pool(2, ready).await.unwrap();
        assert!(discover_replicas(&repository).await.unwrap().is_empty());
    }

    #[test]
    fn discovery_drops_a_pool_snapshot_changed_during_replica_pagination() {
        let observed = ready_pool();
        assert!(pool_snapshot_is_current_ready(&observed, Some(&observed)));

        let mut changed = observed.clone();
        changed.resource_version = ResourceVersion::new(observed.resource_version.get() + 1);
        assert!(!pool_snapshot_is_current_ready(&observed, Some(&changed)));

        changed.state = GatewayPoolState::Draining;
        assert!(!pool_snapshot_is_current_ready(&observed, Some(&changed)));
        assert!(!pool_snapshot_is_current_ready(&observed, None));
    }

    #[tokio::test]
    async fn reconciliation_fences_connection_changes_but_not_heartbeat_versions() {
        let started = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicUsize::new(0));
        let mut running = BTreeMap::new();
        let replica = active_replica();

        reconcile_connectors(&mut running, vec![replica.clone()], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        let initial_task = running[&replica_id()].task.id();
        assert_eq!(started.load(Ordering::SeqCst), 1);

        reconcile_connectors(&mut running, vec![replica.clone()], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        assert_eq!(running[&replica_id()].task.id(), initial_task);
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(stopped.load(Ordering::SeqCst), 0);

        let mut endpoint_changed = replica.clone();
        endpoint_changed.control_endpoint = "http://127.0.0.1:18083".to_owned();
        reconcile_connectors(&mut running, vec![endpoint_changed.clone()], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        let endpoint_task = running[&replica_id()].task.id();
        assert_ne!(endpoint_task, initial_task);
        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(stopped.load(Ordering::SeqCst), 1);

        let mut version_changed = endpoint_changed.clone();
        // Heartbeat persistence advances only the Registry observation watermark.
        version_changed.resource_version = ResourceVersion::new(3);
        reconcile_connectors(&mut running, vec![version_changed.clone()], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        let version_task = running[&replica_id()].task.id();
        assert_eq!(version_task, endpoint_task);
        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(stopped.load(Ordering::SeqCst), 1);

        let mut generation_changed = version_changed.clone();
        generation_changed.credential.certificate_generation = Some(CertificateGeneration::new(2));
        reconcile_connectors(&mut running, vec![generation_changed.clone()], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        assert_ne!(running[&replica_id()].task.id(), version_task);
        assert_eq!(started.load(Ordering::SeqCst), 3);
        assert_eq!(stopped.load(Ordering::SeqCst), 2);

        let mut revoked = generation_changed.clone();
        revoked.state = GatewayReplicaState::Revoked;
        revoked.credential.state = GatewayCredentialState::Revoked;
        reconcile_connectors(&mut running, vec![revoked], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        assert!(running.is_empty());
        assert_eq!(stopped.load(Ordering::SeqCst), 3);

        reconcile_connectors(&mut running, vec![generation_changed], |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        assert_eq!(started.load(Ordering::SeqCst), 4);
        reconcile_connectors(&mut running, Vec::new(), |_| {
            pending_connector(&started, &stopped)
        })
        .await;
        assert!(running.is_empty());
        assert_eq!(stopped.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn reconciliation_restarts_a_finished_connector() {
        let started = Arc::new(AtomicUsize::new(0));
        let mut running = BTreeMap::new();
        let replica = active_replica();

        reconcile_connectors(&mut running, vec![replica.clone()], |_| {
            started.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async {})
        })
        .await;
        let first_task = running[&replica_id()].task.id();
        tokio::task::yield_now().await;
        assert!(running[&replica_id()].task.is_finished());

        reconcile_connectors(&mut running, vec![replica], |_| {
            started.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async {})
        })
        .await;
        assert_ne!(running[&replica_id()].task.id(), first_task);
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn insecure_endpoint_is_loopback_only() {
        assert!(parse_endpoint("http://127.0.0.1:8082", true).is_ok());
        assert!(parse_endpoint("http://[::1]:8082", true).is_ok());
        assert!(matches!(
            parse_endpoint("http://gateway.example:8082", true),
            Err(GatewayConnectorError::InsecureEndpoint)
        ));
        assert!(matches!(
            parse_endpoint("http://127.0.0.1:8082", false),
            Err(GatewayConnectorError::InsecureEndpoint)
        ));
        assert!(parse_endpoint("https://gateway.example:8443", false).is_ok());
        assert!(parse_endpoint("https://gateway.example:8443/", false).is_err());
    }

    #[test]
    fn workload_trust_domain_is_validated_before_loading_identity() {
        assert!(validate_workload_trust_domain("mesh.example.test").is_ok());
        assert!(validate_workload_trust_domain("Mesh.example.test").is_err());
        assert!(validate_workload_trust_domain("mesh..example.test").is_err());
    }

    #[tokio::test]
    async fn initial_data_preserves_frames_after_the_replica_hello() {
        use http_body_util::Full;
        use neoengram_protocol::{
            Extensions, GatewayConnectionId, GatewayControlMessage, GatewayReplicaHeartbeat,
            GatewayReplicaHello, SequenceNumber,
        };

        let hello = connector_frame(
            1,
            GatewayControlMessage::ReplicaHello(GatewayReplicaHello {
                edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
                software_version: "0.2.0".to_owned(),
                supported_protocol_versions: BTreeSet::from([PROTOCOL_VERSION_V1]),
                capabilities: neoengram_protocol::gateway_capabilities_v1(),
            }),
        );
        let heartbeat = connector_frame(
            2,
            GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                connected_agents: 3,
                active_streams: 2,
                queue_depth: 1,
            }),
        );
        let mut coalesced = hello.encode_ndjson().unwrap();
        coalesced.extend(heartbeat.encode_ndjson().unwrap());
        let mut body = Full::new(Bytes::from(coalesced));
        let mut decoder = GatewayControlNdjsonDecoder::new();

        let (decoded_hello, followups) = read_initial_gateway_frames(&mut body, &mut decoder)
            .await
            .unwrap();

        assert_eq!(decoded_hello, hello);
        assert_eq!(followups, vec![heartbeat]);
        assert_eq!(decoder.pending_bytes(), 0);

        fn connector_frame(sequence: u64, message: GatewayControlMessage) -> GatewayControlFrame {
            GatewayControlFrame {
                protocol_version: PROTOCOL_VERSION_V1,
                gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
                gateway_replica_id: replica_id(),
                connection_id: GatewayConnectionId::new("control-connection-a").unwrap(),
                sequence: SequenceNumber::new(sequence),
                request_id: RequestId::new(format!("request-{sequence}")).unwrap(),
                trace_id: None,
                sent_at_unix_ms: UnixMillis::new(1_000),
                deadline_unix_ms: UnixMillis::new(2_000),
                hop_count: 0,
                message,
                extensions: Extensions::new(),
            }
        }
    }

    #[test]
    fn mtls_loader_requires_real_bounded_pem_files() {
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.pem");
        let cert = directory.path().join("cert.pem");
        let key = directory.path().join("key.pem");
        std::fs::write(&ca, b"not a certificate").unwrap();
        std::fs::write(&cert, b"not a certificate").unwrap();
        std::fs::write(&key, b"not a private key").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let error = GatewayConnectorConfig::load_mtls(&ca, &cert, &key, "mesh.example.test", false)
            .expect_err("invalid PEM must fail closed");
        assert!(matches!(error, GatewayConnectorError::TlsConfiguration(_)));
    }

    #[tokio::test]
    async fn loaded_mtls_config_validates_hostname_and_presents_central_identity() {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
            KeyUsagePurpose,
        };
        use rustls::{server::WebPkiClientVerifier, ServerConfig};
        use rustls_pki_types::PrivatePkcs8KeyDer;
        use tokio_rustls::TlsAcceptor;

        let ca_key = KeyPair::generate().expect("CA key");
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_parameters.self_signed(&ca_key).expect("CA certificate");

        let server_key = KeyPair::generate().expect("Gateway key");
        let mut server_parameters = CertificateParams::new(vec!["gateway.example.test".to_owned()])
            .expect("Gateway certificate parameters");
        server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_parameters
            .signed_by(&server_key, &ca, &ca_key)
            .expect("Gateway certificate");

        let client_key = KeyPair::generate().expect("Central key");
        let mut client_parameters =
            CertificateParams::new(Vec::<String>::new()).expect("Central certificate parameters");
        client_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_parameters
            .signed_by(&client_key, &ca, &ca_key)
            .expect("Central certificate");

        let directory = tempfile::tempdir().unwrap();
        let ca_path = directory.path().join("ca.pem");
        let client_certificate_path = directory.path().join("central.pem");
        let client_key_path = directory.path().join("central-key.pem");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(
            &client_certificate_path,
            format!("{}{}", client_certificate.pem(), ca.pem()),
        )
        .unwrap();
        std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&client_key_path, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        let client_config = GatewayConnectorConfig::load_mtls(
            &ca_path,
            &client_certificate_path,
            &client_key_path,
            "mesh.example.test",
            false,
        )
        .expect("Central mTLS config")
        .tls()
        .expect("TLS enabled");

        let mut client_roots = RootCertStore::empty();
        client_roots.add(ca.der().clone()).expect("client CA");
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(client_roots), provider.clone())
                .build()
                .expect("client verifier");
        let server_key_der = PrivatePkcs8KeyDer::from(server_key.serialize_der());
        let mut server_config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("safe TLS protocol versions")
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![server_certificate.der().clone(), ca.der().clone()],
                server_key_der.into(),
            )
            .expect("Gateway TLS config");
        server_config.alpn_protocols = vec![b"h2".to_vec()];

        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let connector = TlsConnector::from(client_config);
        let (central_io, gateway_io) = tokio::io::duplex(64 * 1024);
        let accept = acceptor.clone().accept(gateway_io);
        let connect = connector.clone().connect(
            ServerName::try_from("gateway.example.test").expect("server name"),
            central_io,
        );
        let (gateway_tls, central_tls) = tokio::join!(accept, connect);
        let gateway_tls = gateway_tls.expect("Gateway accepts Central mTLS identity");
        let central_tls = central_tls.expect("Central validates Gateway CA and hostname");
        assert_eq!(
            gateway_tls.get_ref().1.peer_certificates().unwrap()[0].as_ref(),
            client_certificate.der().as_ref()
        );
        assert_eq!(
            central_tls.get_ref().1.peer_certificates().unwrap()[0].as_ref(),
            server_certificate.der().as_ref()
        );
        assert_eq!(
            central_tls.get_ref().1.alpn_protocol(),
            Some(b"h2".as_slice())
        );

        let (central_io, gateway_io) = tokio::io::duplex(64 * 1024);
        let accept = acceptor.accept(gateway_io);
        let connect = connector.connect(
            ServerName::try_from("wrong.example.test").expect("server name"),
            central_io,
        );
        let (_gateway_tls, central_tls) = tokio::join!(accept, connect);
        assert!(central_tls.is_err(), "hostname mismatch must fail closed");
    }

    #[test]
    fn verified_leaf_uri_san_and_fingerprint_bind_the_registry_identity() {
        use rcgen::{CertificateParams, KeyPair, SanType};

        let mut parameters = CertificateParams::new(vec!["gateway.example.test".to_owned()])
            .expect("certificate parameters");
        parameters.subject_alt_names.push(SanType::URI(
            "spiffe://mesh.example.test/workloads/edge-clusters/cluster-a/gateway-pools/pool-a/gateway-replicas/replica-a"
                .try_into()
                .expect("URI IA5 string"),
        ));
        let key_pair = KeyPair::generate().expect("test key");
        let certificate = parameters.self_signed(&key_pair).expect("test certificate");
        let leaf = CertificateDer::from(certificate.der().to_vec());
        let mut replica = active_replica();
        replica.credential.certificate_fingerprint = Some(ContentDigest::hash(leaf.as_ref()));

        let (_, parsed) = X509Certificate::from_der(leaf.as_ref()).unwrap();
        let not_after_unix_ms = UnixMillis::new(
            u64::try_from(parsed.validity().not_after.timestamp()).unwrap() * 1_000,
        );
        assert!(
            gateway_server_certificate_deadline(
                Some(std::slice::from_ref(&leaf)),
                Some(not_after_unix_ms),
            )
            .unwrap()
                > Instant::now()
        );
        assert!(matches!(
            gateway_server_certificate_deadline(
                Some(std::slice::from_ref(&leaf)),
                Some(UnixMillis::new(1)),
            ),
            Err(GatewayConnectorError::ServerCertificateExpired)
        ));

        let identity = authenticated_replica_from_tls(
            &replica,
            Some(std::slice::from_ref(&leaf)),
            "mesh.example.test",
        )
        .expect("matching verified certificate identity");
        assert_eq!(identity.edge_cluster_id, replica.edge_cluster_id);
        assert_eq!(identity.gateway_pool_id, replica.gateway_pool_id);
        assert_eq!(identity.gateway_replica_id, replica.gateway_replica_id);

        assert!(authenticated_replica_from_tls(
            &replica,
            Some(std::slice::from_ref(&leaf)),
            "other.example.test",
        )
        .is_err());
        let mut fingerprint_mismatch = replica.clone();
        fingerprint_mismatch.credential.certificate_fingerprint =
            Some(ContentDigest::hash(b"another certificate"));
        assert!(authenticated_replica_from_tls(
            &fingerprint_mismatch,
            Some(std::slice::from_ref(&leaf)),
            "mesh.example.test",
        )
        .is_err());
        replica.gateway_replica_id = GatewayReplicaId::new("replica-b").unwrap();
        assert!(authenticated_replica_from_tls(
            &replica,
            Some(std::slice::from_ref(&leaf)),
            "mesh.example.test",
        )
        .is_err());
    }

    fn active_replica() -> GatewayReplicaRecord {
        GatewayReplicaRecord {
            gateway_replica_id: replica_id(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            control_endpoint: "http://127.0.0.1:18082".to_owned(),
            peer_endpoint: "https://replica.peer.example".to_owned(),
            bootstrap_endpoint: "https://replica.bootstrap.example".to_owned(),
            software_version: "0.2.0".to_owned(),
            supported_protocol_versions: BTreeSet::from([PROTOCOL_VERSION_V1]),
            capabilities: neoengram_protocol::gateway_capabilities_v1(),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Active,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(b"activation-token"),
                activation_created_at_unix_ms: UnixMillis::new(100),
                activation_expires_at_unix_ms: UnixMillis::new(900_100),
                activation_consumed_at_unix_ms: Some(UnixMillis::new(150)),
                public_key_fingerprint: Some(ContentDigest::hash(b"replica-key")),
                certificate_generation: Some(CertificateGeneration::new(1)),
                certificate_fingerprint: Some(ContentDigest::hash(b"replica-certificate")),
                certificate_not_after_unix_ms: Some(UnixMillis::new(21_600_000)),
                certificate: None,
                state: GatewayCredentialState::Active,
            },
            resource_version: ResourceVersion::new(2),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(200),
        }
    }

    fn provisioning_pool() -> GatewayPoolRecord {
        let actor = PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("gateway-discovery-test").unwrap(),
            extensions: Extensions::new(),
        };
        GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            display_name: "Discovery Pool".to_owned(),
            agent_endpoint: "https://pool-a.agent.example".to_owned(),
            s3_endpoint: None,
            desired_replicas: 2,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Provisioning,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
            created_by: actor.clone(),
            updated_by: actor,
        }
    }

    fn ready_pool() -> GatewayPoolRecord {
        let mut pool = provisioning_pool();
        pool.state = GatewayPoolState::Ready;
        pool.config_generation = Generation::new(2);
        pool.resource_version = ResourceVersion::new(2);
        pool.updated_at_unix_ms = UnixMillis::new(200);
        pool
    }

    fn replica_id() -> GatewayReplicaId {
        GatewayReplicaId::new("replica-a").unwrap()
    }
}
