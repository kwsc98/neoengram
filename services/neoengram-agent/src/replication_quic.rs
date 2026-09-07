//! Agent QUIC source/sink sessions for Commit object replication.
//!
//! This module is deliberately transport-only.  It speaks the bounded `TransferFrame` protocol
//! and delegates all durable state to `ObjectBackend`; Gateways can relay the stream without
//! seeing a Volume path or object bytes.  A caller supplies the Central trust bundle and the
//! immutable ObjectSet so a signed ticket cannot be widened by a peer.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use neoengram_domain::protocol::{
    BatchManifest, BatchManifestPage, CommitObjectSet, EdgeClusterId, GatewayPoolId,
    GatewayReplicaId, MaterializationBatch, MaterializationBatchId, MaterializationBatchTicket,
    MaterializationId, MaterializationManifestSource, MaterializationObjectPlacement,
    MaterializationObjectReceipt, MountGeneration, ObjectChunk, ObjectPlacementState, ObjectProof,
    ObjectReceiptId, ObjectRef, ObjectRequest, ObjectSet, PlacementId,
    SignedMaterializationBatchTicket, SignedTransferTicket, StorageVolumeId, TransferFrame,
    TransferFrameError, TransferTicket, UnixMillis, MAX_TRANSFER_CHUNK_BYTES,
};
use neoengram_domain::TenantId;
use neoengram_domain::{Generation, ObjectId};
use neoengram_runtime::{ObjectBackend, ObjectRange};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rustls_pki_types::{pem::PemObject, CertificateDer};
use url::Url;
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer, X509Certificate},
};

use crate::{
    CentralCommandTrustBundle, InMemoryPlacementInventory, LocalPlacementInventory,
    SharedSessionFence, ValidationMode,
};

/// A transfer frame must make progress within a bounded interval. QUIC's connection idle
/// timeout eventually closes a dead path, but relying on it alone leaves the worker blocked for
/// an unbounded amount of time when an intermediary black-holes packets.
const TRANSFER_FRAME_IO_TIMEOUT: Duration = Duration::from_secs(30);
const TRANSFER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSFER_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Read-only Agent-local inventory used to prove that a Central-selected source Placement is
/// actually the object copy served by this process.  The resolver must not consult Central or
/// accept network-supplied paths: it is deliberately an Agent-local metadata boundary.
pub trait SourcePlacementResolver: std::fmt::Debug + Send + Sync {
    /// Resolve the exact Placement selected by Central for one manifest object.
    ///
    /// `None` is treated as a hard authorization failure.  A resolver may return an error for an
    /// unavailable/corrupt local inventory, which is also fail-closed by the source listener.
    fn resolve(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
    ) -> Result<Option<MaterializationObjectPlacement>, QuicTransferError>;

    /// Resolves the object against an explicit per-object Placement binding.  The default keeps
    /// existing in-process resolvers source-compatible while allowing a route-grouped Batch to
    /// carry different Placement IDs for different objects in its signed manifest.
    fn resolve_selected(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
        placement: &MaterializationManifestSource,
    ) -> Result<Option<MaterializationObjectPlacement>, QuicTransferError> {
        let mut scoped_ticket = ticket.clone();
        scoped_ticket.source.placement_id = placement.placement_id.clone();
        scoped_ticket.source.placement_generation = placement.placement_generation;
        self.resolve(&scoped_ticket, object)
    }

    /// Verifies that the selected Placement is backed by the local CAS before a source stream is
    /// opened.  The default is intentionally a no-op for explicit in-process test resolvers;
    /// mounted-volume production resolvers override it with a content-addressed inventory check.
    fn verify_backend(
        &self,
        _ticket: &MaterializationBatchTicket,
        _object: &ObjectRef,
        _backend: &dyn ObjectBackend,
    ) -> Result<(), QuicTransferError> {
        Ok(())
    }
}

/// Explicit test/development resolver for in-process streams that do not have an Agent inventory.
/// Production listeners must inject a real [`SourcePlacementResolver`] instead.
#[derive(Debug, Clone, Copy, Default)]
pub struct PermissiveSourcePlacementResolver;

impl SourcePlacementResolver for PermissiveSourcePlacementResolver {
    fn resolve(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
    ) -> Result<Option<MaterializationObjectPlacement>, QuicTransferError> {
        Ok(Some(MaterializationObjectPlacement {
            placement_id: ticket.source.placement_id.clone(),
            tenant_id: ticket.tenant_id.clone(),
            object_namespace_id: ticket.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            storage_volume_id: ticket.source.storage_volume_id.clone(),
            archive_id: ticket.source.archive_id.clone(),
            placement_generation: ticket.source.placement_generation,
            state: ObjectPlacementState::Verified,
            failure_domain: "test-permissive".to_owned(),
        }))
    }
}

/// Fail-closed resolver used when a caller has not configured a local placement inventory.
/// Keeping this separate from the explicit permissive test resolver prevents a production
/// listener from accidentally treating a signed ticket as local placement evidence.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectingSourcePlacementResolver;

impl SourcePlacementResolver for RejectingSourcePlacementResolver {
    fn resolve(
        &self,
        _ticket: &MaterializationBatchTicket,
        _object: &ObjectRef,
    ) -> Result<Option<MaterializationObjectPlacement>, QuicTransferError> {
        Ok(None)
    }
}

/// Source resolver used by the mounted-volume runtime.  Agents do not own the Placement
/// authority, but they do own the physical Volume fence.  Binding the resolver to that Volume
/// prevents a valid ticket for another source Volume from being served by this process.  Central
/// supplies an exact Placement binding for each manifest object; the selected ID and generation
/// are checked against local inventory before the backend is opened.
#[derive(Debug, Clone)]
pub struct MountedVolumeSourcePlacementResolver {
    storage_volume_id: StorageVolumeId,
    inventory: Arc<dyn LocalPlacementInventory>,
}

impl MountedVolumeSourcePlacementResolver {
    #[must_use]
    pub fn new(storage_volume_id: StorageVolumeId) -> Self {
        // Keep the one-argument constructor strict for embedded callers: an empty inventory is
        // preferable to deriving a Placement from ticket bytes. Production runtimes should use
        // `with_inventory` with the durable Agent-local inventory.
        Self::with_inventory(
            storage_volume_id,
            Arc::new(InMemoryPlacementInventory::default()),
        )
    }

    #[must_use]
    pub fn with_inventory(
        storage_volume_id: StorageVolumeId,
        inventory: Arc<dyn LocalPlacementInventory>,
    ) -> Self {
        Self {
            storage_volume_id,
            inventory,
        }
    }
}

impl SourcePlacementResolver for MountedVolumeSourcePlacementResolver {
    fn resolve(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
    ) -> Result<Option<MaterializationObjectPlacement>, QuicTransferError> {
        if ticket.source.storage_volume_id.as_ref() != Some(&self.storage_volume_id) {
            return Ok(None);
        }
        self.inventory
            .lookup(
                &ticket.tenant_id,
                &ticket.source.placement_id,
                &ticket.object_namespace_id,
                object.object_id,
            )
            .map_err(|error| {
                QuicTransferError::Backend(format!(
                    "local source placement inventory is unavailable: {error}"
                ))
            })
    }

    fn verify_backend(
        &self,
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
        backend: &dyn ObjectBackend,
    ) -> Result<(), QuicTransferError> {
        verify_local_source_object(backend, &ticket.tenant_id, object)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QuicTransferError {
    #[error("QUIC connection failed: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("QUIC connect failed: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("QUIC stream read failed: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("QUIC stream write failed: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("invalid transfer frame: {0}")]
    Frame(#[from] TransferFrameError),
    #[error("transfer ticket rejected: {0}")]
    Ticket(String),
    #[error("transfer protocol error: {0}")]
    Protocol(String),
    #[error("object backend failed: {0}")]
    Backend(String),
    #[error("transfer ticket expired")]
    Expired,
    #[error("QUIC endpoint I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("QUIC TLS configuration failed: {0}")]
    Tls(String),
    #[error("QUIC peer certificate identity rejected: {0}")]
    PeerIdentity(String),
    #[error("Gateway QUIC preflight timed out")]
    PreflightTimeout,
}

impl QuicTransferError {
    /// Returns whether retrying the same immutable transfer scope may succeed after the network
    /// path recovers. Ticket, frame, protocol and backend failures are intentionally excluded:
    /// those indicate a bad request or local data and must remain fail-closed.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Connection(_)
                | Self::Connect(_)
                | Self::Read(_)
                | Self::Write(_)
                | Self::Io(_)
                | Self::PreflightTimeout
        )
    }
}

/// Optional Agent QUIC network. Central still signs the immutable scope; this config supplies a
/// local source listener and the target Gateway ingress address. It never contains a direct
/// source-Agent address.
#[derive(Debug, Clone)]
pub struct QuicTransferNetworkConfig {
    pub listen: Option<SocketAddr>,
    pub gateway_endpoint: Option<SocketAddr>,
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub client_ca_file: PathBuf,
    pub server_name: String,
}

#[derive(Debug, Clone)]
pub struct QuicTransferNetwork {
    endpoint: Arc<Endpoint>,
    gateway_endpoint: Option<SocketAddr>,
    server_name: Arc<str>,
    /// Trust domain used to bind source-listener peers to Gateway workload identities. This is
    /// configured through [`Self::with_gateway_workload_trust_domain`] so the existing public
    /// network config struct remains source-compatible for embedded callers.
    gateway_workload_trust_domain: Option<Arc<str>>,
    validation_mode: ValidationMode,
}

impl QuicTransferNetwork {
    pub fn bind(config: &QuicTransferNetworkConfig) -> Result<Self, QuicTransferError> {
        if config.listen.is_none() && config.gateway_endpoint.is_none() {
            return Err(QuicTransferError::Protocol(
                "a QUIC listener or source endpoint is required".into(),
            ));
        }
        let certificate_pem = fs::read(&config.certificate_file)
            .map_err(|error| QuicTransferError::Tls(format!("certificate: {error}")))?;
        let private_key_pem = fs::read(&config.private_key_file)
            .map_err(|error| QuicTransferError::Tls(format!("private key: {error}")))?;
        let ca_pem = fs::read(&config.client_ca_file)
            .map_err(|error| QuicTransferError::Tls(format!("client CA: {error}")))?;
        let certificates = rustls_pki_types::CertificateDer::pem_slice_iter(&certificate_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| QuicTransferError::Tls(format!("certificate: {error}")))?;
        if certificates.is_empty() {
            return Err(QuicTransferError::Tls("certificate chain is empty".into()));
        }
        let private_key = rustls_pki_types::PrivateKeyDer::from_pem_slice(&private_key_pem)
            .map_err(|error| QuicTransferError::Tls(format!("private key: {error}")))?;
        let mut roots = rustls::RootCertStore::empty();
        for root in rustls_pki_types::CertificateDer::pem_slice_iter(&ca_pem) {
            let root =
                root.map_err(|error| QuicTransferError::Tls(format!("client CA: {error}")))?;
            roots
                .add(root)
                .map_err(|error| QuicTransferError::Tls(format!("client CA: {error}")))?;
        }
        if roots.is_empty() {
            return Err(QuicTransferError::Tls("client CA is empty".into()));
        }
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let server_builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            provider.clone(),
        )
        .build()
        .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let mut server = server_builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates.clone(), private_key.clone_key())
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        server.alpn_protocols = vec![neoengram_domain::protocol::MATERIALIZATION_TRANSFER_ALPN_V2
            .as_bytes()
            .to_vec()];
        server.send_tls13_tickets = 0;
        server.max_tls13_tickets = 0;
        let server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));
        let mut transport = quinn::TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(64u32.into())
            .keep_alive_interval(Some(TRANSFER_KEEP_ALIVE_INTERVAL));
        let transport = Arc::new(transport);
        server_config.transport = Arc::clone(&transport);

        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, private_key)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        client.alpn_protocols = vec![neoengram_domain::protocol::MATERIALIZATION_TRANSFER_ALPN_V2
            .as_bytes()
            .to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        let client_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let mut client_config = quinn::ClientConfig::new(Arc::new(client_crypto));
        client_config.transport_config(transport);
        let mut endpoint = match config.listen {
            Some(address) => quinn::Endpoint::server(server_config, address)?,
            None => quinn::Endpoint::client("0.0.0.0:0".parse().expect("valid client bind"))?,
        };
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint: Arc::new(endpoint),
            gateway_endpoint: config.gateway_endpoint,
            server_name: Arc::from(config.server_name.as_str()),
            gateway_workload_trust_domain: None,
            validation_mode: ValidationMode::Strict,
        })
    }

    /// Binds the SPIFFE trust domain used by the source listener's Gateway peer fence. The
    /// runtime supplies the same value used by the HTTPS Gateway verifier. Keeping this as a
    /// builder avoids adding a required field to [`QuicTransferNetworkConfig`], whose struct
    /// literal is part of the embedded API.
    #[must_use]
    pub fn with_gateway_workload_trust_domain(mut self, trust_domain: impl Into<Arc<str>>) -> Self {
        self.gateway_workload_trust_domain = Some(trust_domain.into());
        self
    }

    /// Applies the same validation profile to both the source listener and target client.
    /// Development is only constructible from a loopback-validated Agent configuration.
    #[must_use]
    pub fn with_validation_mode(mut self, mode: ValidationMode) -> Self {
        self.validation_mode = mode;
        self
    }

    #[must_use]
    pub fn validation_mode(&self) -> ValidationMode {
        self.validation_mode
    }

    pub fn local_addr(&self) -> Result<SocketAddr, QuicTransferError> {
        self.endpoint.local_addr().map_err(QuicTransferError::Io)
    }

    pub async fn connect_gateway(&self) -> Result<Connection, QuicTransferError> {
        let address = self.gateway_endpoint.ok_or_else(|| {
            QuicTransferError::Protocol("target Gateway endpoint is not configured".into())
        })?;
        let connecting = self.endpoint.connect(address, &self.server_name)?;
        tokio::time::timeout(TRANSFER_CONNECT_TIMEOUT, connecting)
            .await
            .map_err(|_| {
                QuicTransferError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "QUIC Gateway connection timed out",
                ))
            })?
            .map_err(QuicTransferError::from)
    }

    /// Performs the bounded network half of replication preflight. A successful result means the
    /// configured Gateway endpoint completed QUIC TLS/ALPN negotiation with this Agent identity
    /// and accepted the v2 preflight frame. It does not authorize a transfer or consume object
    /// bytes; only a Central-signed ticket can open a transfer.
    pub async fn preflight_gateway(&self) -> Result<(), QuicTransferError> {
        let connection = tokio::time::timeout(Duration::from_secs(5), self.connect_gateway())
            .await
            .map_err(|_| QuicTransferError::PreflightTimeout)??;
        let (mut send, mut recv) =
            tokio::time::timeout(Duration::from_secs(5), connection.open_bi())
                .await
                .map_err(|_| QuicTransferError::PreflightTimeout)??;
        send_frame(&mut send, &TransferFrame::Preflight).await?;
        // Explicitly finish the probe stream so the Gateway can distinguish a complete probe
        // from a client that disappeared before sending its first frame.
        let _ = send.finish();
        let response = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut recv))
            .await
            .map_err(|_| QuicTransferError::PreflightTimeout)??;
        if !matches!(response, TransferFrame::PreflightAck) {
            return Err(QuicTransferError::Protocol(
                "Gateway returned an invalid replication preflight response".into(),
            ));
        }
        connection.close(0u32.into(), b"replication preflight");
        Ok(())
    }

    /// Serves source streams until the session is fenced or shutdown is requested.  The backend
    /// is selected only after the signed ticket has been validated, so a peer cannot choose an
    /// arbitrary artifact path by manipulating the QUIC handshake.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve_source(
        &self,
        trust_bundle: Arc<CentralCommandTrustBundle>,
        local_tenant_id: TenantId,
        local_agent_id: neoengram_domain::AgentId,
        session_fence: SharedSessionFence,
        local_mount_generation: MountGeneration,
        execution: Arc<crate::FilesystemExecution>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), QuicTransferError> {
        self.serve_source_with_volume(
            trust_bundle,
            local_tenant_id,
            local_agent_id,
            None,
            session_fence,
            local_mount_generation,
            execution,
            shutdown,
        )
        .await
    }

    /// Source listener entry point with an explicit local Volume identity. Production runtimes
    /// should use this variant so a valid ticket for another Volume cannot be mapped to this
    /// Agent's artifact-scoped CAS.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve_source_with_volume(
        &self,
        trust_bundle: Arc<CentralCommandTrustBundle>,
        local_tenant_id: TenantId,
        local_agent_id: neoengram_domain::AgentId,
        local_storage_volume_id: Option<StorageVolumeId>,
        session_fence: SharedSessionFence,
        local_mount_generation: MountGeneration,
        execution: Arc<crate::FilesystemExecution>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), QuicTransferError> {
        let resolver: Arc<dyn SourcePlacementResolver> = match local_storage_volume_id.clone() {
            Some(volume) => Arc::new(MountedVolumeSourcePlacementResolver::new(volume)),
            None => Arc::new(RejectingSourcePlacementResolver),
        };
        self.serve_source_with_volume_and_resolver(
            trust_bundle,
            local_tenant_id,
            local_agent_id,
            local_storage_volume_id,
            session_fence,
            local_mount_generation,
            execution,
            resolver,
            shutdown,
        )
        .await
    }

    /// Source listener entry point with an explicit local Placement resolver. Production callers
    /// should provide a resolver backed by the Agent's durable Volume inventory; the resolver is
    /// consulted for every manifest object before the source backend is opened.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve_source_with_volume_and_resolver(
        &self,
        trust_bundle: Arc<CentralCommandTrustBundle>,
        local_tenant_id: TenantId,
        local_agent_id: neoengram_domain::AgentId,
        local_storage_volume_id: Option<StorageVolumeId>,
        session_fence: SharedSessionFence,
        local_mount_generation: MountGeneration,
        execution: Arc<crate::FilesystemExecution>,
        source_resolver: Arc<dyn SourcePlacementResolver>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), QuicTransferError> {
        let Some(_) = self.endpoint.local_addr().ok() else {
            return Ok(());
        };
        loop {
            let incoming = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                    continue;
                }
                incoming = self.endpoint.accept() => incoming,
            };
            let Some(incoming) = incoming else { break };
            let trust_bundle = Arc::clone(&trust_bundle);
            let local_tenant_id = local_tenant_id.clone();
            let local_agent_id = local_agent_id.clone();
            let local_storage_volume_id = local_storage_volume_id.clone();
            let gateway_workload_trust_domain = self.gateway_workload_trust_domain.clone();
            let validation_mode = self.validation_mode;
            let session_fence = session_fence.clone();
            let execution = Arc::clone(&execution);
            let source_resolver = Arc::clone(&source_resolver);
            tokio::spawn(async move {
                let result = async {
                    let connection = incoming.await?;
                    let (send, recv) = connection.accept_bi().await?;
                    // `peer_identity` is a boxed `dyn Any` and is not `Send`; consume it now and
                    // retain only the owned certificate chain across the spawned stream future.
                    let peer_certificates = connection
                        .peer_identity()
                        .and_then(|identity| {
                            identity.downcast::<Vec<CertificateDer<'static>>>().ok()
                        })
                        .map(|certificates| *certificates);
                    let current_session = session_fence
                        .get()
                        .map_err(|error| QuicTransferError::Protocol(error.to_string()))?;
                    let identity = QuicTransferIdentity::new(local_agent_id);
                    let identity = match local_storage_volume_id {
                        Some(storage_volume_id) => identity.with_storage_volume(storage_volume_id),
                        None => identity,
                    }
                    .with_validation_mode(validation_mode)
                    .with_session_mount(
                        current_session.session_generation.get(),
                        local_mount_generation.get(),
                    );
                    let transfer_execution = Arc::clone(&execution);
                    let materialization_execution = Arc::clone(&execution);
                    let transfer_tenant = local_tenant_id.clone();
                    let materialization_tenant = local_tenant_id.clone();
                    serve_quic_unified_source_connection(
                        send,
                        recv,
                        move |ticket| {
                            if ticket.ticket.tenant_id != transfer_tenant {
                                return Err(QuicTransferError::Ticket(
                                    "source Agent tenant does not match transfer ticket".into(),
                                ));
                            }
                            transfer_execution
                                .replication_backend(
                                    ticket.ticket.tenant_id.clone(),
                                    ticket.ticket.artifact_id.clone(),
                                )
                                .map(|backend| Arc::new(backend) as Arc<dyn ObjectBackend>)
                                .map_err(|error| QuicTransferError::Backend(error.to_string()))
                        },
                        move |ticket| {
                            if ticket.ticket.tenant_id != materialization_tenant {
                                return Err(QuicTransferError::Ticket(
                                    "source Agent tenant does not match materialization ticket"
                                        .into(),
                                ));
                            }
                            materialization_execution
                                .replication_backend(
                                    ticket.ticket.tenant_id.clone(),
                                    ticket.ticket.artifact_id.clone(),
                                )
                                .map(|backend| Arc::new(backend) as Arc<dyn ObjectBackend>)
                                .map_err(|error| QuicTransferError::Backend(error.to_string()))
                        },
                        &trust_bundle,
                        current_unix_millis(),
                        Some(identity),
                        peer_certificates.as_deref(),
                        gateway_workload_trust_domain.as_deref(),
                        validation_mode,
                        source_resolver,
                    )
                    .await
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "Agent QUIC source transfer failed");
                }
            });
        }
        self.endpoint
            .close(0u32.into(), b"Agent transfer listener stopped");
        Ok(())
    }
}

/// Optional identity fence for a source or target Agent endpoint.
#[derive(Debug, Clone)]
pub struct QuicTransferIdentity {
    pub agent_id: neoengram_domain::AgentId,
    storage_volume_id: Option<StorageVolumeId>,
    generations: Option<(u64, u64, u64)>,
    session_mount: Option<(u64, u64)>,
    validation_mode: ValidationMode,
}

impl QuicTransferIdentity {
    #[must_use]
    pub fn new(agent_id: neoengram_domain::AgentId) -> Self {
        Self {
            agent_id,
            storage_volume_id: None,
            generations: None,
            session_mount: None,
            validation_mode: ValidationMode::Strict,
        }
    }

    #[must_use]
    pub fn with_validation_mode(mut self, mode: ValidationMode) -> Self {
        self.validation_mode = mode;
        self
    }

    #[must_use]
    pub fn with_storage_volume(mut self, storage_volume_id: StorageVolumeId) -> Self {
        self.storage_volume_id = Some(storage_volume_id);
        self
    }

    #[must_use]
    pub fn with_generations(mut self, session: u64, mount: u64, route: u64) -> Self {
        self.generations = Some((session, mount, route));
        self.session_mount = None;
        self
    }

    /// Fences the Agent session and mounted Volume while leaving the Gateway route generation to
    /// the signed ticket/Gateway fence. Agents do not own the Gateway's route lease, so comparing
    /// that field against the ticket itself would provide no protection.
    #[must_use]
    pub fn with_session_mount(mut self, session: u64, mount: u64) -> Self {
        self.generations = None;
        self.session_mount = Some((session, mount));
        self
    }

    fn check_source(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        if ticket.source.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "ticket source Agent does not match local Agent".into(),
            ));
        }
        if self.storage_volume_id.as_ref() != ticket.source.storage_volume_id.as_ref()
            && self.storage_volume_id.is_some()
        {
            return Err(QuicTransferError::Protocol(
                "ticket source Volume does not match local Volume".into(),
            ));
        }
        if self.validation_mode.is_strict() {
            if let Some((session, mount, route)) = self.generations {
                if ticket.source_session_generation.get() != session
                    || ticket.source_mount_generation.get() != mount
                    || ticket.source_route_generation.get() != route
                {
                    return Err(QuicTransferError::Protocol(
                        "ticket source route generation is stale".into(),
                    ));
                }
            }
            if let Some((session, mount)) = self.session_mount {
                if ticket.source_session_generation.get() != session
                    || ticket.source_mount_generation.get() != mount
                {
                    return Err(QuicTransferError::Protocol(
                        "ticket source session or mount generation is stale".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn check_target(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        if ticket.target.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "ticket target Agent does not match local Agent".into(),
            ));
        }
        if self.storage_volume_id.as_ref() != ticket.target.storage_volume_id.as_ref()
            && self.storage_volume_id.is_some()
        {
            return Err(QuicTransferError::Protocol(
                "ticket target Volume does not match local Volume".into(),
            ));
        }
        if self.validation_mode.is_strict() {
            if let Some((session, mount, route)) = self.generations {
                if ticket.session_generation.get() != session
                    || ticket.mount_generation.get() != mount
                    || ticket.route_generation.get() != route
                {
                    return Err(QuicTransferError::Protocol(
                        "ticket target route generation is stale".into(),
                    ));
                }
            }
            if let Some((session, mount)) = self.session_mount {
                if ticket.session_generation.get() != session
                    || ticket.mount_generation.get() != mount
                {
                    return Err(QuicTransferError::Protocol(
                        "ticket target session or mount generation is stale".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn check_materialization_source(
        &self,
        ticket: &MaterializationBatchTicket,
    ) -> Result<(), QuicTransferError> {
        if ticket.source.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "materialization source Agent does not match local Agent".into(),
            ));
        }
        if self.storage_volume_id.as_ref() != ticket.source.storage_volume_id.as_ref()
            && self.storage_volume_id.is_some()
        {
            return Err(QuicTransferError::Protocol(
                "materialization source Volume does not match local Volume".into(),
            ));
        }
        if self.validation_mode.is_strict() {
            if let Some((session, mount, route)) = self.generations {
                if ticket.source.session_generation.get() != session
                    || ticket.source.mount_generation.get() != mount
                    || ticket.source.route_generation.get() != route
                {
                    return Err(QuicTransferError::Protocol(
                        "materialization source route generation is stale".into(),
                    ));
                }
            }
            if let Some((session, mount)) = self.session_mount {
                if ticket.source.session_generation.get() != session
                    || ticket.source.mount_generation.get() != mount
                {
                    return Err(QuicTransferError::Protocol(
                        "materialization source session or mount generation is stale".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn check_materialization_target(
        &self,
        ticket: &MaterializationBatchTicket,
    ) -> Result<(), QuicTransferError> {
        if ticket.target.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "materialization target Agent does not match local Agent".into(),
            ));
        }
        if self.storage_volume_id.as_ref() != Some(&ticket.target.storage_volume_id)
            && self.storage_volume_id.is_some()
        {
            return Err(QuicTransferError::Protocol(
                "materialization target Volume does not match local Volume".into(),
            ));
        }
        if self.validation_mode.is_strict() {
            if let Some((session, mount, route)) = self.generations {
                if ticket.target.session_generation.get() != session
                    || ticket.target.mount_generation.get() != mount
                    || ticket.target.route_generation.get() != route
                {
                    return Err(QuicTransferError::Protocol(
                        "materialization target route generation is stale".into(),
                    ));
                }
            }
            if let Some((session, mount)) = self.session_mount {
                if ticket.target.session_generation.get() != session
                    || ticket.target.mount_generation.get() != mount
                {
                    return Err(QuicTransferError::Protocol(
                        "materialization target session or mount generation is stale".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Limits the number of bytes requested in one ObjectRequest.
#[derive(Debug, Clone, Copy)]
pub struct QuicTransferClientConfig {
    pub chunk_bytes: usize,
}

impl Default for QuicTransferClientConfig {
    fn default() -> Self {
        Self {
            chunk_bytes: MAX_TRANSFER_CHUNK_BYTES,
        }
    }
}

impl QuicTransferClientConfig {
    fn validate(self) -> Result<Self, QuicTransferError> {
        if self.chunk_bytes == 0 || self.chunk_bytes > MAX_TRANSFER_CHUNK_BYTES {
            return Err(QuicTransferError::Protocol(
                "chunk_bytes exceeds transfer frame limit".into(),
            ));
        }
        Ok(self)
    }
}

/// A target Agent-side QUIC session.  The stream is opened once and resumed offsets are read
/// from the target backend before each object request.
#[derive(Debug)]
pub struct QuicTransferClient {
    connection: Connection,
    config: QuicTransferClientConfig,
    identity: Option<QuicTransferIdentity>,
}

impl QuicTransferClient {
    pub fn new(
        connection: Connection,
        config: QuicTransferClientConfig,
    ) -> Result<Self, QuicTransferError> {
        Ok(Self {
            connection,
            config: config.validate()?,
            identity: None,
        })
    }

    #[must_use]
    pub fn with_identity(mut self, identity: QuicTransferIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Copies an immutable ObjectSet through the QUIC source stream into a mounted target CAS.
    /// Staging is retained on every error, allowing a later invocation with the same transfer ID
    /// and ticket scope to continue at the durable offset.
    pub async fn copy_object_set(
        &self,
        signed_ticket: &SignedTransferTicket,
        object_set: &ObjectSet,
        target: &dyn ObjectBackend,
        trust_bundle: &CentralCommandTrustBundle,
        now_unix_ms: u64,
    ) -> Result<(), QuicTransferError> {
        signed_ticket
            .validate()
            .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
        trust_bundle
            .verify_transfer_ticket(
                signed_ticket,
                neoengram_domain::protocol::UnixMillis::new(now_unix_ms),
            )
            .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
        validate_ticket_set(&signed_ticket.ticket, object_set)?;
        if let Some(identity) = self.identity.as_ref() {
            identity.check_target(&signed_ticket.ticket)?;
        }
        let (mut send, mut recv) = self.connection.open_bi().await?;
        send_frame(
            &mut send,
            &TransferFrame::OpenTransferSigned(signed_ticket.clone()),
        )
        .await?;
        // The ticket binds the digest and allow-list, while this frame carries the immutable
        // object sizes needed by a source Agent that does not own Commit metadata.
        send_frame(
            &mut send,
            &TransferFrame::CommitObjectSet(CommitObjectSet {
                tenant_id: signed_ticket.ticket.tenant_id.clone(),
                commit_id: signed_ticket.ticket.commit_id,
                object_set: object_set.clone(),
            }),
        )
        .await?;
        let mut total = 0_u64;
        for object in &object_set.objects {
            let expected = object.object_spec();
            let mut offset = target
                .staged_size(
                    &signed_ticket.ticket.transfer_id,
                    &signed_ticket.ticket.tenant_id,
                    &object.object_id,
                )
                .map_err(backend_error)?
                .unwrap_or(0);
            if offset > expected.size {
                return Err(QuicTransferError::Protocol(
                    "staged offset exceeds object size".into(),
                ));
            }
            // A reconnect can observe a fully staged object whose prior session disconnected
            // immediately before the publication call. Re-run the idempotent finalize fence.
            if offset == expected.size {
                target
                    .verify_and_publish(
                        &signed_ticket.ticket.transfer_id,
                        &signed_ticket.ticket.tenant_id,
                        &expected,
                    )
                    .map_err(backend_error)?;
                // Tell the source that this object was already durably staged on a previous
                // connection. A zero-length acknowledgement is a resume marker, not a payload
                // transfer, and lets the source retain its complete-object close fence.
                if expected.size != 0 {
                    send_frame(
                        &mut send,
                        &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                            object_id: object.object_id,
                            offset,
                            length: 0,
                            accepted: true,
                        }),
                    )
                    .await?;
                }
            }
            while offset < expected.size {
                let length = (expected.size - offset).min(self.config.chunk_bytes as u64);
                total = total
                    .checked_add(length)
                    .ok_or_else(|| QuicTransferError::Protocol("byte count overflow".into()))?;
                if total > signed_ticket.ticket.max_bytes.get() {
                    return Err(QuicTransferError::Protocol(
                        "transfer exceeds ticket byte limit".into(),
                    ));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectRequest(ObjectRequest {
                        object_id: object.object_id,
                        offset,
                        length,
                    }),
                )
                .await?;
                let frame = read_frame(&mut recv).await?;
                let TransferFrame::ObjectChunk(chunk) = frame else {
                    return Err(protocol_frame("expected ObjectChunk"));
                };
                if chunk.object_id != object.object_id
                    || chunk.offset != offset
                    || chunk.bytes.len() as u64 != length
                {
                    return Err(protocol_frame("ObjectChunk range does not match request"));
                }
                let staged = target
                    .stage_write(
                        &signed_ticket.ticket.transfer_id,
                        &signed_ticket.ticket.tenant_id,
                        &expected,
                        offset,
                        &chunk.bytes,
                    )
                    .map_err(backend_error)?;
                if staged.staged_size != offset + length {
                    return Err(protocol_frame("target acknowledged unexpected offset"));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                        object_id: object.object_id,
                        offset,
                        length,
                        accepted: true,
                    }),
                )
                .await?;
                offset += length;
                if offset == expected.size {
                    let frame = read_frame(&mut recv).await?;
                    let TransferFrame::ObjectProof(proof) = frame else {
                        return Err(protocol_frame("expected ObjectProof"));
                    };
                    if proof.object_id != object.object_id
                        || proof.size != expected.size
                        || proof.digest != object.object_id.digest()
                    {
                        return Err(protocol_frame("ObjectProof does not match object"));
                    }
                    target
                        .verify_and_publish(
                            &signed_ticket.ticket.transfer_id,
                            &signed_ticket.ticket.tenant_id,
                            &expected,
                        )
                        .map_err(backend_error)?;
                }
            }
            if offset == expected.size && expected.size == 0 {
                target
                    .verify_and_publish(
                        &signed_ticket.ticket.transfer_id,
                        &signed_ticket.ticket.tenant_id,
                        &expected,
                    )
                    .map_err(backend_error)?;
            }
        }
        send_frame(
            &mut send,
            &TransferFrame::CloseTransfer(neoengram_domain::protocol::CloseTransfer {
                committed: true,
            }),
        )
        .await?;
        Ok(())
    }

    /// Materializes one Central-planned v2 batch. The target sends the signed capability and the
    /// complete paged manifest before requesting any bytes; the source can therefore serve only
    /// the exact object descriptors selected by Central. The returned receipts are ready for the
    /// normal Agent control/report path and contain no payload bytes.
    #[allow(clippy::too_many_arguments)]
    pub async fn materialize_batch(
        &self,
        signed_ticket: &SignedMaterializationBatchTicket,
        batch: &MaterializationBatch,
        manifest: &BatchManifest,
        pages: &[BatchManifestPage],
        target: &dyn ObjectBackend,
        trust_bundle: &CentralCommandTrustBundle,
        now_unix_ms: u64,
    ) -> Result<Vec<MaterializationObjectReceipt>, QuicTransferError> {
        crate::validate_signed_materialization_batch_with_trust(
            trust_bundle,
            signed_ticket,
            batch,
            manifest,
            pages,
            UnixMillis::new(now_unix_ms),
        )
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
        if let Some(identity) = self.identity.as_ref() {
            identity.check_materialization_target(&signed_ticket.ticket)?;
        }
        let transfer_id = crate::materialization_transfer_id(
            &signed_ticket.ticket.materialization_id,
            &signed_ticket.ticket.object_namespace_id,
        )
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
        let (mut send, mut recv) = self.connection.open_bi().await?;
        send_frame(
            &mut send,
            &TransferFrame::OpenMaterializationSigned(signed_ticket.clone()),
        )
        .await?;
        send_frame(
            &mut send,
            &TransferFrame::MaterializationManifest(manifest.clone()),
        )
        .await?;
        for page in pages {
            send_frame(
                &mut send,
                &TransferFrame::MaterializationManifestPage(page.clone()),
            )
            .await?;
        }

        let mut receipts = Vec::with_capacity(manifest.object_count.get() as usize);
        let mut transferred = 0_u64;
        let mut ordered_pages = pages.to_vec();
        ordered_pages.sort_by_key(|page| page.page_number);
        for object in ordered_pages.iter().flat_map(|page| page.objects.iter()) {
            let expected = object.object_spec();
            let already_published = match target
                .inspect(&signed_ticket.ticket.tenant_id, &expected.id)
                .map_err(backend_error)?
            {
                None => false,
                Some(metadata) if metadata.id == expected.id && metadata.size == expected.size => {
                    true
                }
                Some(_) => {
                    return Err(QuicTransferError::Protocol(
                        "target contains an object with an unexpected size or digest".into(),
                    ));
                }
            };
            let mut offset = if already_published {
                expected.size
            } else {
                target
                    .staged_size(
                        &transfer_id,
                        &signed_ticket.ticket.tenant_id,
                        &object.object_id,
                    )
                    .map_err(backend_error)?
                    .unwrap_or(0)
            };
            if offset > expected.size {
                return Err(QuicTransferError::Protocol(
                    "materialization staged offset exceeds object size".into(),
                ));
            }
            if already_published {
                // A replayed target object is already durable.  The zero-length acknowledgement
                // lets the source mark this manifest member complete without requesting bytes.
                target
                    .verify_and_publish(&transfer_id, &signed_ticket.ticket.tenant_id, &expected)
                    .map_err(backend_error)?;
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                        object_id: object.object_id,
                        offset,
                        length: 0,
                        accepted: true,
                    }),
                )
                .await?;
            } else if expected.size == 0 {
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectRequest(ObjectRequest {
                        object_id: object.object_id,
                        offset: 0,
                        length: 0,
                    }),
                )
                .await?;
                let frame = read_frame(&mut recv).await?;
                let TransferFrame::ObjectProof(proof) = frame else {
                    return Err(protocol_frame("expected empty-object ObjectProof"));
                };
                if proof.object_id != object.object_id
                    || proof.size != 0
                    || proof.digest != object.object_id.digest()
                {
                    return Err(protocol_frame(
                        "empty-object ObjectProof does not match object",
                    ));
                }
                target
                    .verify_and_publish(&transfer_id, &signed_ticket.ticket.tenant_id, &expected)
                    .map_err(backend_error)?;
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                        object_id: object.object_id,
                        offset: 0,
                        length: 0,
                        accepted: true,
                    }),
                )
                .await?;
            } else if offset == expected.size {
                target
                    .verify_and_publish(&transfer_id, &signed_ticket.ticket.tenant_id, &expected)
                    .map_err(backend_error)?;
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                        object_id: object.object_id,
                        offset,
                        length: 0,
                        accepted: true,
                    }),
                )
                .await?;
            }
            while offset < expected.size {
                let length = (expected.size - offset).min(self.config.chunk_bytes as u64);
                transferred = transferred
                    .checked_add(length)
                    .ok_or_else(|| QuicTransferError::Protocol("byte count overflow".into()))?;
                if transferred > signed_ticket.ticket.max_bytes.get() {
                    return Err(QuicTransferError::Protocol(
                        "materialization exceeds ticket byte limit".into(),
                    ));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectRequest(ObjectRequest {
                        object_id: object.object_id,
                        offset,
                        length,
                    }),
                )
                .await?;
                let frame = read_frame(&mut recv).await?;
                let TransferFrame::ObjectChunk(chunk) = frame else {
                    return Err(protocol_frame("expected ObjectChunk"));
                };
                if chunk.object_id != object.object_id
                    || chunk.offset != offset
                    || chunk.bytes.len() as u64 != length
                {
                    return Err(protocol_frame(
                        "materialization ObjectChunk range does not match request",
                    ));
                }
                let staged = target
                    .stage_write(
                        &transfer_id,
                        &signed_ticket.ticket.tenant_id,
                        &expected,
                        offset,
                        &chunk.bytes,
                    )
                    .map_err(backend_error)?;
                if staged.staged_size != offset + length {
                    return Err(protocol_frame(
                        "materialization target acknowledged an unexpected offset",
                    ));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                        object_id: object.object_id,
                        offset,
                        length,
                        accepted: true,
                    }),
                )
                .await?;
                offset += length;
                if offset == expected.size {
                    let frame = read_frame(&mut recv).await?;
                    let TransferFrame::ObjectProof(proof) = frame else {
                        return Err(protocol_frame("expected ObjectProof"));
                    };
                    if proof.object_id != object.object_id
                        || proof.size != expected.size
                        || proof.digest != object.object_id.digest()
                    {
                        return Err(protocol_frame(
                            "materialization ObjectProof does not match object",
                        ));
                    }
                    target
                        .verify_and_publish(
                            &transfer_id,
                            &signed_ticket.ticket.tenant_id,
                            &expected,
                        )
                        .map_err(backend_error)?;
                }
            }
            let receipt_id = materialization_receipt_id(
                &signed_ticket.ticket.materialization_id,
                &signed_ticket.ticket.batch_id,
                signed_ticket.ticket.plan_revision,
                signed_ticket.ticket.batch_attempt,
                object.object_id,
            )?;
            let checkpoint = crate::MaterializationCheckpoint {
                materialization_id: signed_ticket.ticket.materialization_id.clone(),
                batch_id: signed_ticket.ticket.batch_id.clone(),
                plan_revision: signed_ticket.ticket.plan_revision,
                batch_attempt: signed_ticket.ticket.batch_attempt,
                object_id: object.object_id,
                confirmed_offset: expected.size,
            };
            receipts.push(
                crate::materialization_receipt_from_checkpoint(
                    &signed_ticket.ticket,
                    object,
                    receipt_id,
                    signed_ticket.ticket.target.placement_generation,
                    &checkpoint,
                    UnixMillis::new(now_unix_ms),
                )
                .map_err(|error| QuicTransferError::Protocol(error.to_string()))?,
            );
        }
        send_frame(
            &mut send,
            &TransferFrame::CloseTransfer(neoengram_domain::protocol::CloseTransfer {
                committed: true,
            }),
        )
        .await?;
        Ok(receipts)
    }
}

/// Functional sink-session hook for runtimes that do not need to retain a client object.
#[allow(clippy::too_many_arguments)]
pub async fn run_quic_sink_stream(
    connection: Connection,
    signed_ticket: &SignedTransferTicket,
    object_set: &ObjectSet,
    target: &dyn ObjectBackend,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    config: QuicTransferClientConfig,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    let client = QuicTransferClient::new(connection, config)?;
    let client = match identity {
        Some(value) => client.with_identity(value),
        None => client,
    };
    client
        .copy_object_set(signed_ticket, object_set, target, trust_bundle, now_unix_ms)
        .await
}

/// Handles the source side of a transfer stream. The caller has already accepted a QUIC stream;
/// this function validates the signed ticket before opening any object and only serves the
/// supplied ObjectSet.
pub async fn serve_quic_source_stream(
    send: SendStream,
    recv: RecvStream,
    backend: Arc<dyn ObjectBackend>,
    object_set: ObjectSet,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    serve_quic_source_stream_with_object_set(
        send,
        recv,
        move |_| Ok(backend),
        Some(object_set),
        trust_bundle,
        now_unix_ms,
        identity,
    )
    .await
}

/// Handles a source stream when Commit metadata is supplied by the target after the signed
/// ticket. This is the production listener entry point: the source opens no object until both
/// the Central-signed scope and the immutable ObjectSet frame have matched.
pub async fn serve_quic_source_stream_from_ticket(
    send: SendStream,
    recv: RecvStream,
    backend: Arc<dyn ObjectBackend>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    serve_quic_source_stream_with_object_set(
        send,
        recv,
        move |_| Ok(backend),
        None,
        trust_bundle,
        now_unix_ms,
        identity,
    )
    .await
}

/// Handles a clean-slate v2 materialization source stream. The target must provide a bounded
/// manifest descriptor followed by every page declared by that descriptor. No source object is
/// opened until the Central signature, route fence, page digests and batch object IDs all match.
/// This convenience wrapper has no local inventory and therefore rejects source objects; callers
/// that own the Agent inventory must use [`serve_quic_materialization_source_stream_with_resolver`].
pub async fn serve_quic_materialization_source_stream(
    send: SendStream,
    recv: RecvStream,
    backend: Arc<dyn ObjectBackend>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    serve_quic_materialization_source_stream_with_factory_and_resolver(
        send,
        recv,
        move |_| Ok(backend),
        trust_bundle,
        now_unix_ms,
        identity,
        Arc::new(RejectingSourcePlacementResolver),
    )
    .await
}

/// Explicit source-stream entry point for a caller that owns an Agent-local placement inventory.
/// The resolver is mandatory at this boundary; without it a signed ticket is not accepted as
/// proof that this process holds the selected Placement.
pub async fn serve_quic_materialization_source_stream_with_resolver(
    send: SendStream,
    recv: RecvStream,
    backend: Arc<dyn ObjectBackend>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
    source_resolver: Arc<dyn SourcePlacementResolver>,
) -> Result<(), QuicTransferError> {
    serve_quic_materialization_source_stream_with_factory_and_resolver(
        send,
        recv,
        move |_| Ok(backend),
        trust_bundle,
        now_unix_ms,
        identity,
        source_resolver,
    )
    .await
}

/// Source listener entry point for runtimes that select an artifact-scoped backend only after
/// validating the v2 ticket. The callback receives no network address or arbitrary path. A local
/// placement resolver is required for a source listener; callers should use the `_with_resolver`
/// variant below.
pub async fn serve_quic_materialization_source_connection<F>(
    send: SendStream,
    recv: RecvStream,
    backend_factory: F,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError>
where
    F: FnOnce(
        &SignedMaterializationBatchTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
{
    serve_quic_materialization_source_stream_with_factory_and_resolver(
        send,
        recv,
        backend_factory,
        trust_bundle,
        now_unix_ms,
        identity,
        Arc::new(RejectingSourcePlacementResolver),
    )
    .await
}

/// Factory variant with an explicit Agent-local source placement resolver.
pub async fn serve_quic_materialization_source_connection_with_resolver<F>(
    send: SendStream,
    recv: RecvStream,
    backend_factory: F,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
    source_resolver: Arc<dyn SourcePlacementResolver>,
) -> Result<(), QuicTransferError>
where
    F: FnOnce(
        &SignedMaterializationBatchTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
{
    serve_quic_materialization_source_stream_with_factory_and_resolver(
        send,
        recv,
        backend_factory,
        trust_bundle,
        now_unix_ms,
        identity,
        source_resolver,
    )
    .await
}

async fn serve_quic_materialization_source_stream_with_factory_and_resolver(
    send: SendStream,
    mut recv: RecvStream,
    backend_factory: impl FnOnce(
        &SignedMaterializationBatchTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
    source_resolver: Arc<dyn SourcePlacementResolver>,
) -> Result<(), QuicTransferError> {
    let frame = read_frame(&mut recv).await?;
    serve_quic_materialization_source_stream_with_factory_from_first(
        send,
        recv,
        frame,
        backend_factory,
        trust_bundle,
        now_unix_ms,
        identity,
        source_resolver,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_quic_materialization_source_stream_with_factory_from_first(
    mut send: SendStream,
    mut recv: RecvStream,
    frame: TransferFrame,
    backend_factory: impl FnOnce(
        &SignedMaterializationBatchTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
    source_resolver: Arc<dyn SourcePlacementResolver>,
) -> Result<(), QuicTransferError> {
    let TransferFrame::OpenMaterializationSigned(signed) = frame else {
        return Err(protocol_frame(
            "first frame must be a signed materialization ticket",
        ));
    };
    signed
        .validate()
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    trust_bundle
        .verify_materialization_ticket(&signed, UnixMillis::new(now_unix_ms))
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    if let Some(identity) = identity {
        identity.check_materialization_source(&signed.ticket)?;
    }
    let TransferFrame::MaterializationManifest(manifest) = read_frame(&mut recv).await? else {
        return Err(protocol_frame(
            "second frame must be a materialization manifest",
        ));
    };
    let page_count = usize::try_from(manifest.page_count.get())
        .map_err(|_| protocol_frame("materialization manifest page count overflows usize"))?;
    if page_count == 0 || page_count > 65_535 {
        return Err(protocol_frame(
            "materialization manifest page count is out of bounds",
        ));
    }
    let mut pages = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        let TransferFrame::MaterializationManifestPage(page) = read_frame(&mut recv).await? else {
            return Err(protocol_frame("materialization manifest is missing a page"));
        };
        pages.push(page);
    }
    let mut object_ids = pages
        .iter()
        .flat_map(|page| page.objects.iter().map(|object| object.object_id))
        .collect::<Vec<_>>();
    object_ids.sort_unstable();
    let batch = MaterializationBatch {
        batch_id: signed.ticket.batch_id.clone(),
        materialization_id: signed.ticket.materialization_id.clone(),
        plan_revision: signed.ticket.plan_revision,
        batch_attempt: signed.ticket.batch_attempt,
        source: signed.ticket.source.clone(),
        target: signed.ticket.target.clone(),
        manifest_digest: manifest.manifest_digest,
        object_ids,
        object_count: manifest.object_count,
        total_bytes: manifest.total_bytes,
        state: neoengram_domain::protocol::MaterializationBatchState::Transferring,
        max_bytes: signed.ticket.max_bytes,
        deadline_unix_ms: signed.ticket.deadline_unix_ms,
    };
    neoengram_domain::protocol::materialization::validate_materialization_assignment(
        &signed.ticket,
        &batch,
        &manifest,
        &pages,
    )
    .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    let backend = backend_factory(&signed)?;
    let ordered_pages = {
        let mut pages = pages;
        pages.sort_by_key(|page| page.page_number);
        pages
    };
    let objects = ordered_pages
        .iter()
        .flat_map(|page| page.objects.iter())
        .cloned()
        .collect::<Vec<_>>();
    let backend = Arc::clone(&backend);
    for object in &objects {
        let selected_source = ordered_pages
            .iter()
            .flat_map(|page| page.source_placements.iter())
            .find(|source| source.object_id == object.object_id)
            .cloned()
            .ok_or_else(|| {
                QuicTransferError::Ticket(
                    "non-empty materialization object is missing its selected source Placement"
                        .into(),
                )
            })?;
        if selected_source.placement_generation != signed.ticket.source.placement_generation {
            return Err(QuicTransferError::Ticket(
                "manifest source Placement generation differs from the signed route fence".into(),
            ));
        }
        let placement = source_resolver
            .resolve_selected(&signed.ticket, object, &selected_source)?
            .ok_or_else(|| {
                QuicTransferError::Ticket(
                    "source Agent does not hold the ticket's selected Placement".into(),
                )
            })?;
        validate_materialization_source_placement_selected(
            &signed.ticket,
            object,
            &placement,
            &selected_source.placement_id,
            selected_source.placement_generation,
        )?;
        source_resolver.verify_backend(&signed.ticket, object, backend.as_ref())?;
    }
    let mut completed = BTreeSet::new();
    // A target may resume from a durable staging offset, but each subsequent request must begin
    // exactly where the previous acknowledged range ended.  Without this fence a peer could
    // request only the final range and make an incomplete object pass the close check.
    let mut next_offsets = BTreeMap::<ObjectId, u64>::new();
    let mut served_bytes = 0_u64;
    let mut pending: Option<(ObjectId, u64, u64)> = None;
    loop {
        match read_frame(&mut recv).await? {
            TransferFrame::ObjectAck(ack) => {
                let Some((id, offset, length)) = pending.take() else {
                    if ack.accepted
                        && ack.length == 0
                        && objects.iter().any(|object| {
                            object.object_id == ack.object_id && object.size.get() == ack.offset
                        })
                    {
                        completed.insert(ack.object_id);
                        next_offsets.insert(ack.object_id, ack.offset);
                        continue;
                    }
                    return Err(protocol_frame("unexpected materialization ObjectAck"));
                };
                if !ack.accepted
                    || ack.object_id != id
                    || ack.offset != offset
                    || ack.length != length
                {
                    return Err(protocol_frame(
                        "materialization ObjectAck does not match chunk",
                    ));
                }
                if objects.iter().any(|object| {
                    object.object_id == id && offset.saturating_add(length) == object.size.get()
                }) {
                    completed.insert(id);
                }
                next_offsets.insert(id, offset.saturating_add(length));
            }
            TransferFrame::ObjectRequest(request) => {
                if pending.is_some() {
                    return Err(protocol_frame(
                        "materialization ObjectRequest arrived before ObjectAck",
                    ));
                }
                let object = objects
                    .iter()
                    .find(|object| object.object_id == request.object_id)
                    .ok_or_else(|| {
                        QuicTransferError::Protocol(
                            "object is outside the materialization manifest".into(),
                        )
                    })?;
                if completed.contains(&request.object_id) {
                    return Err(protocol_frame(
                        "materialization ObjectRequest arrived after completion",
                    ));
                }
                if object.size.get() == 0 {
                    if request.offset != 0 || request.length != 0 {
                        return Err(protocol_frame(
                            "empty-object request must be zero-length at offset zero",
                        ));
                    }
                    send_frame(
                        &mut send,
                        &TransferFrame::ObjectProof(ObjectProof {
                            object_id: object.object_id,
                            digest: object.object_id.digest(),
                            size: 0,
                        }),
                    )
                    .await?;
                    let frame = read_frame(&mut recv).await?;
                    let TransferFrame::ObjectAck(ack) = frame else {
                        return Err(protocol_frame("expected empty-object ObjectAck"));
                    };
                    if !ack.accepted
                        || ack.object_id != object.object_id
                        || ack.offset != 0
                        || ack.length != 0
                    {
                        return Err(protocol_frame(
                            "empty-object ObjectAck does not match proof",
                        ));
                    }
                    completed.insert(object.object_id);
                    continue;
                }
                if let Some(expected_offset) = next_offsets.get(&request.object_id) {
                    if request.offset != *expected_offset {
                        return Err(protocol_frame(
                            "materialization ObjectRequest range is not contiguous",
                        ));
                    }
                }
                if request.length == 0
                    || request.length > MAX_TRANSFER_CHUNK_BYTES as u64
                    || request
                        .offset
                        .checked_add(request.length)
                        .filter(|end| *end <= object.size.get())
                        .is_none()
                {
                    return Err(protocol_frame(
                        "materialization ObjectRequest range is invalid",
                    ));
                }
                let next_served_bytes = served_bytes
                    .checked_add(request.length)
                    .ok_or_else(|| protocol_frame("materialization byte count overflows u64"))?;
                if next_served_bytes > signed.ticket.max_bytes.get() {
                    return Err(protocol_frame(
                        "materialization source would exceed the signed byte limit",
                    ));
                }
                let mut bytes = Vec::with_capacity(request.length as usize);
                let copied = backend
                    .read_range(
                        &signed.ticket.tenant_id,
                        &object.object_spec(),
                        ObjectRange::new(request.offset, request.length).map_err(backend_error)?,
                        &mut bytes,
                    )
                    .map_err(backend_error)?;
                if copied != request.length {
                    return Err(protocol_frame(
                        "materialization source returned an unexpected range",
                    ));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectChunk(ObjectChunk::new(
                        request.object_id,
                        request.offset,
                        bytes,
                    )?),
                )
                .await?;
                pending = Some((request.object_id, request.offset, request.length));
                if request.offset + request.length == object.size.get() {
                    send_frame(
                        &mut send,
                        &TransferFrame::ObjectProof(ObjectProof {
                            object_id: object.object_id,
                            digest: object.object_id.digest(),
                            size: object.size.get(),
                        }),
                    )
                    .await?;
                }
                served_bytes = next_served_bytes;
            }
            TransferFrame::CloseTransfer(close) => {
                if pending.is_some() || !close.committed || completed.len() != objects.len() {
                    return Err(protocol_frame(
                        "materialization closed before all acknowledgements",
                    ));
                }
                return Ok(());
            }
            _ => return Err(protocol_frame("unexpected materialization transfer frame")),
        }
    }
}

/// Validates the Gateway workload certificate on the Agent source hop. Rustls has already
/// checked the chain against the configured CA, but the CA is shared by Agents and Gateways and
/// therefore does not prove workload role or route scope. The signed materialization ticket gives
/// us the source EdgeCluster and GatewayPool expected for this connection; checking them here
/// binds the peer identity before any source backend is opened.
fn validate_gateway_source_peer_certificate(
    peer_certificates: Option<&[CertificateDer<'static>]>,
    trust_domain: &str,
    expected_edge_cluster_id: &EdgeClusterId,
    expected_gateway_pool_id: &GatewayPoolId,
) -> Result<(), QuicTransferError> {
    let certificates = peer_certificates.ok_or_else(|| {
        QuicTransferError::PeerIdentity("Gateway did not present a workload certificate".to_owned())
    })?;
    let leaf = certificates.first().ok_or_else(|| {
        QuicTransferError::PeerIdentity("Gateway certificate chain is empty".to_owned())
    })?;
    let (remainder, certificate) = X509Certificate::from_der(leaf.as_ref()).map_err(|_| {
        QuicTransferError::PeerIdentity("Gateway workload certificate is invalid DER".to_owned())
    })?;
    if !remainder.is_empty() {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload certificate contains trailing DER data".to_owned(),
        ));
    }

    let eku = certificate
        .extended_key_usage()
        .map_err(|_| {
            QuicTransferError::PeerIdentity(
                "Gateway workload certificate EKU is invalid".to_owned(),
            )
        })?
        .ok_or_else(|| {
            QuicTransferError::PeerIdentity("Gateway workload certificate has no EKU".to_owned())
        })?;
    if !eku.value.client_auth || !eku.value.server_auth {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload certificate must allow client and server authentication".to_owned(),
        ));
    }

    let san = certificate
        .subject_alternative_name()
        .map_err(|_| {
            QuicTransferError::PeerIdentity(
                "Gateway workload certificate SAN is invalid".to_owned(),
            )
        })?
        .ok_or_else(|| {
            QuicTransferError::PeerIdentity("Gateway workload certificate has no SAN".to_owned())
        })?;
    let uris = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        })
        .collect::<Vec<_>>();
    if uris.len() != 1 {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload certificate must contain exactly one URI SAN".to_owned(),
        ));
    }
    let uri = uris[0];
    if uri.contains(['?', '#', '%']) {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload URI SAN is not canonical".to_owned(),
        ));
    }
    let parsed = Url::parse(uri).map_err(|_| {
        QuicTransferError::PeerIdentity("Gateway workload URI SAN is invalid".to_owned())
    })?;
    if parsed.scheme() != "spiffe"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.as_str() != uri
        || parsed.host_str() != Some(trust_domain)
    {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload URI SAN trust domain is not expected".to_owned(),
        ));
    }
    let segments = parsed
        .path_segments()
        .ok_or_else(|| {
            QuicTransferError::PeerIdentity("Gateway workload URI SAN has no path".to_owned())
        })?
        .collect::<Vec<_>>();
    if segments.len() != 7
        || segments[0] != "workloads"
        || segments[1] != "edge-clusters"
        || segments[2] != expected_edge_cluster_id.as_str()
        || segments[3] != "gateway-pools"
        || segments[5] != "gateway-replicas"
    {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload URI SAN is outside the source EdgeCluster".to_owned(),
        ));
    }
    let pool_id = GatewayPoolId::new(segments[4]).map_err(|_| {
        QuicTransferError::PeerIdentity("Gateway workload URI SAN has an invalid pool".to_owned())
    })?;
    if pool_id != *expected_gateway_pool_id {
        return Err(QuicTransferError::PeerIdentity(
            "Gateway workload URI SAN does not identify the source GatewayPool".to_owned(),
        ));
    }
    GatewayReplicaId::new(segments[6]).map_err(|_| {
        QuicTransferError::PeerIdentity(
            "Gateway workload URI SAN has an invalid replica".to_owned(),
        )
    })?;
    Ok(())
}

/// Dispatches one accepted source stream after inspecting its first bounded frame.  The production
/// Agent listener is v2-only: legacy whole-Commit frames are rejected even if a peer manages to
/// negotiate the v2 ALPN, so an old client cannot bypass the protocol boundary by changing only
/// its TLS metadata.  The legacy handler remains available to explicit in-process migration
/// callers and tests through its dedicated entry point below.
#[allow(clippy::too_many_arguments)]
async fn serve_quic_unified_source_connection<F1, F2>(
    send: SendStream,
    mut recv: RecvStream,
    _transfer_backend_factory: F1,
    materialization_backend_factory: F2,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
    peer_certificates: Option<&[CertificateDer<'static>]>,
    gateway_workload_trust_domain: Option<&str>,
    validation_mode: ValidationMode,
    source_resolver: Arc<dyn SourcePlacementResolver>,
) -> Result<(), QuicTransferError>
where
    F1: FnOnce(&SignedTransferTicket) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
    F2: FnOnce(
        &SignedMaterializationBatchTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
{
    let first = read_frame(&mut recv).await?;
    match first {
        frame @ TransferFrame::OpenMaterializationSigned(_) => {
            // Rustls always authenticates the mTLS chain. Strict mode additionally binds the
            // Gateway workload URI/EKU to the source ticket. Development is loopback-only and
            // defers that deployment-owned fence while retaining the signed ticket and all
            // frame/object checks below.
            let TransferFrame::OpenMaterializationSigned(signed) = &frame else {
                unreachable!("source frame was matched above")
            };
            if validation_mode.is_strict() {
                let peer_certificates = peer_certificates.ok_or_else(|| {
                    QuicTransferError::PeerIdentity(
                        "Gateway did not present a workload certificate".to_owned(),
                    )
                })?;
                let trust_domain = gateway_workload_trust_domain.ok_or_else(|| {
                    QuicTransferError::PeerIdentity(
                        "Gateway workload trust domain is not configured".to_owned(),
                    )
                })?;
                validate_gateway_source_peer_certificate(
                    Some(peer_certificates),
                    trust_domain,
                    &signed.ticket.source.edge_cluster_id,
                    &signed.ticket.source.gateway_pool_id,
                )?;
            } else if peer_certificates.is_none_or(|certificates| certificates.is_empty()) {
                return Err(QuicTransferError::PeerIdentity(
                    "Gateway did not present a workload certificate".to_owned(),
                ));
            }
            serve_quic_materialization_source_stream_with_factory_from_first(
                send,
                recv,
                frame,
                materialization_backend_factory,
                trust_bundle,
                now_unix_ms,
                identity,
                source_resolver,
            )
            .await
        }
        TransferFrame::OpenTransfer(_) | TransferFrame::OpenTransferSigned(_) => Err(
            protocol_frame("legacy whole-Commit transfer is disabled on the v2 Agent listener"),
        ),
        _ => Err(protocol_frame(
            "first frame must be a signed transfer or materialization ticket",
        )),
    }
}

/// Source entry point used by the runtime listener. The ticket is verified before the factory is
/// called, and the factory can therefore safely open the artifact-scoped CAS named by the ticket.
pub async fn serve_quic_source_connection<F>(
    send: SendStream,
    recv: RecvStream,
    backend_factory: F,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError>
where
    F: FnOnce(&SignedTransferTicket) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
{
    serve_quic_source_stream_with_object_set(
        send,
        recv,
        backend_factory,
        None,
        trust_bundle,
        now_unix_ms,
        identity,
    )
    .await
}

async fn serve_quic_source_stream_with_object_set(
    send: SendStream,
    mut recv: RecvStream,
    backend_factory: impl FnOnce(
        &SignedTransferTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
    expected_object_set: Option<ObjectSet>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    let frame = read_frame(&mut recv).await?;
    serve_quic_source_stream_with_object_set_from_first(
        send,
        recv,
        frame,
        backend_factory,
        expected_object_set,
        trust_bundle,
        now_unix_ms,
        identity,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_quic_source_stream_with_object_set_from_first(
    mut send: SendStream,
    mut recv: RecvStream,
    frame: TransferFrame,
    backend_factory: impl FnOnce(
        &SignedTransferTicket,
    ) -> Result<Arc<dyn ObjectBackend>, QuicTransferError>,
    expected_object_set: Option<ObjectSet>,
    trust_bundle: &CentralCommandTrustBundle,
    now_unix_ms: u64,
    identity: Option<QuicTransferIdentity>,
) -> Result<(), QuicTransferError> {
    let TransferFrame::OpenTransferSigned(signed) = frame else {
        return Err(protocol_frame("first frame must be signed ticket"));
    };
    signed
        .validate()
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    trust_bundle
        .verify_transfer_ticket(
            &signed,
            neoengram_domain::protocol::UnixMillis::new(now_unix_ms),
        )
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    if let Some(identity) = identity {
        identity.check_source(&signed.ticket)?;
    }
    let TransferFrame::CommitObjectSet(commit_set) = read_frame(&mut recv).await? else {
        return Err(protocol_frame("second frame must be CommitObjectSet"));
    };
    commit_set
        .validate()
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    if commit_set.tenant_id != signed.ticket.tenant_id
        || commit_set.commit_id != signed.ticket.commit_id
    {
        return Err(protocol_frame(
            "CommitObjectSet identity does not match ticket",
        ));
    }
    validate_ticket_set(&signed.ticket, &commit_set.object_set)?;
    if expected_object_set
        .as_ref()
        .is_some_and(|expected| expected != &commit_set.object_set)
    {
        return Err(protocol_frame(
            "CommitObjectSet does not match assigned metadata",
        ));
    }
    let object_set = commit_set.object_set;
    let backend = backend_factory(&signed)?;
    let mut completed = object_set
        .objects
        .iter()
        .filter(|object| object.size.get() == 0)
        .map(|object| object.object_id)
        .collect::<BTreeSet<_>>();
    let mut pending: Option<(ObjectId, u64, u64)> = None;
    loop {
        match read_frame(&mut recv).await? {
            TransferFrame::ObjectAck(ack) => {
                let Some((id, offset, length)) = pending.take() else {
                    if ack.accepted
                        && ack.length == 0
                        && object_set.objects.iter().any(|object| {
                            object.object_id == ack.object_id && object.size.get() == ack.offset
                        })
                    {
                        completed.insert(ack.object_id);
                        continue;
                    }
                    return Err(protocol_frame("unexpected ObjectAck"));
                };
                if !ack.accepted
                    || ack.object_id != id
                    || ack.offset != offset
                    || ack.length != length
                {
                    return Err(protocol_frame("ObjectAck does not match chunk"));
                }
                if let Some(object) = object_set
                    .objects
                    .iter()
                    .find(|object| object.object_id == id)
                {
                    if offset.saturating_add(length) == object.size.get() {
                        completed.insert(id);
                    }
                }
            }
            TransferFrame::ObjectRequest(request) => {
                if pending.is_some() {
                    return Err(protocol_frame("ObjectRequest arrived before ObjectAck"));
                }
                let object = object_set
                    .objects
                    .iter()
                    .find(|object| object.object_id == request.object_id)
                    .ok_or_else(|| {
                        QuicTransferError::Protocol("object is outside assigned ObjectSet".into())
                    })?;
                if request.length == 0
                    || request.length > MAX_TRANSFER_CHUNK_BYTES as u64
                    || request
                        .offset
                        .checked_add(request.length)
                        .filter(|end| *end <= object.size.get())
                        .is_none()
                {
                    return Err(protocol_frame("ObjectRequest range is invalid"));
                }
                let mut bytes = Vec::with_capacity(request.length as usize);
                let copied = backend
                    .read_range(
                        &signed.ticket.tenant_id,
                        &object.object_spec(),
                        ObjectRange::new(request.offset, request.length).map_err(backend_error)?,
                        &mut bytes,
                    )
                    .map_err(backend_error)?;
                if copied != request.length {
                    return Err(protocol_frame("source returned an unexpected range"));
                }
                send_frame(
                    &mut send,
                    &TransferFrame::ObjectChunk(ObjectChunk::new(
                        request.object_id,
                        request.offset,
                        bytes,
                    )?),
                )
                .await?;
                pending = Some((request.object_id, request.offset, request.length));
                if request.offset + request.length == object.size.get() {
                    send_frame(
                        &mut send,
                        &TransferFrame::ObjectProof(ObjectProof {
                            object_id: object.object_id,
                            digest: object.object_id.digest(),
                            size: object.size.get(),
                        }),
                    )
                    .await?;
                }
            }
            TransferFrame::CloseTransfer(close) => {
                if pending.is_some()
                    || !close.committed
                    || completed.len() != object_set.objects.len()
                {
                    return Err(protocol_frame(
                        "transfer closed before all acknowledgements",
                    ));
                }
                return Ok(());
            }
            _ => return Err(protocol_frame("unexpected transfer frame")),
        }
    }
}

fn validate_ticket_set(ticket: &TransferTicket, set: &ObjectSet) -> Result<(), QuicTransferError> {
    if ticket.object_set_digest != set.object_set_digest || ticket.tenant_id.as_str().is_empty() {
        return Err(QuicTransferError::Ticket(
            "ticket ObjectSet or tenant scope mismatch".into(),
        ));
    }
    if set
        .objects
        .iter()
        .any(|object| !ticket.allows(object.object_id))
    {
        return Err(QuicTransferError::Ticket(
            "ticket does not authorize every ObjectSet member".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn validate_materialization_source_placement(
    ticket: &MaterializationBatchTicket,
    object: &ObjectRef,
    placement: &MaterializationObjectPlacement,
) -> Result<(), QuicTransferError> {
    validate_materialization_source_placement_selected(
        ticket,
        object,
        placement,
        &ticket.source.placement_id,
        ticket.source.placement_generation,
    )
}

fn validate_materialization_source_placement_selected(
    ticket: &MaterializationBatchTicket,
    object: &ObjectRef,
    placement: &MaterializationObjectPlacement,
    selected_placement_id: &PlacementId,
    selected_placement_generation: neoengram_domain::PlacementGeneration,
) -> Result<(), QuicTransferError> {
    placement
        .validate_against(object)
        .map_err(|error| QuicTransferError::Ticket(error.to_string()))?;
    if placement.tenant_id != ticket.tenant_id
        || placement.object_namespace_id != ticket.object_namespace_id
        || placement.placement_id != *selected_placement_id
        || placement.placement_generation != selected_placement_generation
        || selected_placement_generation != ticket.source.placement_generation
        || placement.storage_volume_id != ticket.source.storage_volume_id
        || placement.archive_id != ticket.source.archive_id
        || !placement.readable()
    {
        return Err(QuicTransferError::Ticket(
            "source Placement does not match the signed materialization scope".into(),
        ));
    }
    Ok(())
}

/// Derives a stable receipt identity for one object in one batch attempt.  The attempt is part of
/// the identity because Central rejects a receipt replay whose evidence carries a newer attempt;
/// omitting it would make a retry collide with the receipt from the previous source session.
fn materialization_receipt_id(
    materialization_id: &MaterializationId,
    batch_id: &MaterializationBatchId,
    plan_revision: Generation,
    batch_attempt: Generation,
    object_id: ObjectId,
) -> Result<ObjectReceiptId, QuicTransferError> {
    let receipt_digest = blake3::hash(
        format!(
            "{}:{}:{}:{}:{}",
            materialization_id, batch_id, plan_revision, batch_attempt, object_id
        )
        .as_bytes(),
    );
    ObjectReceiptId::new(format!("receipt-{}", &receipt_digest.to_hex()[..32]))
        .map_err(|error| QuicTransferError::Protocol(error.to_string()))
}

fn backend_error(error: impl std::fmt::Display) -> QuicTransferError {
    QuicTransferError::Backend(error.to_string())
}

/// Re-checks the source Volume's content-addressed object immediately before serving a batch.
/// Placement metadata is necessary authorization, but it is not proof that a local file has not
/// been truncated or corrupted since the authority recorded it.  Hashing bounded ranges keeps
/// this check independent of object size and avoids loading a whole object into memory.
fn verify_local_source_object(
    backend: &dyn ObjectBackend,
    tenant_id: &TenantId,
    object: &ObjectRef,
) -> Result<(), QuicTransferError> {
    let expected = object.object_spec();
    let metadata = backend
        .inspect(tenant_id, &expected.id)
        .map_err(backend_error)?
        .ok_or_else(|| {
            QuicTransferError::Backend("selected source object is missing".to_owned())
        })?;
    if metadata.size != expected.size {
        return Err(QuicTransferError::Backend(
            "selected source object size differs from the manifest".to_owned(),
        ));
    }
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut copied_total = 0_u64;
    let chunk_size = u64::try_from(MAX_TRANSFER_CHUNK_BYTES)
        .map_err(|_| QuicTransferError::Protocol("transfer chunk limit overflows u64".into()))?;
    while offset < expected.size {
        let length = (expected.size - offset).min(chunk_size);
        let mut bytes = Vec::with_capacity(
            usize::try_from(length)
                .map_err(|_| QuicTransferError::Protocol("source range exceeds usize".into()))?,
        );
        let copied = backend
            .read_range(
                tenant_id,
                &expected,
                ObjectRange::new(offset, length).map_err(backend_error)?,
                &mut bytes,
            )
            .map_err(backend_error)?;
        if copied != length || bytes.len() as u64 != length {
            return Err(QuicTransferError::Backend(
                "selected source object ended before its declared size".to_owned(),
            ));
        }
        hasher.update(&bytes);
        offset = offset
            .checked_add(length)
            .ok_or_else(|| QuicTransferError::Protocol("source offset overflows u64".into()))?;
        copied_total = copied_total
            .checked_add(copied)
            .ok_or_else(|| QuicTransferError::Protocol("source byte count overflows u64".into()))?;
    }
    let digest = ObjectId::from_bytes(*hasher.finalize().as_bytes());
    if copied_total != expected.size || digest != expected.id {
        return Err(QuicTransferError::Backend(
            "selected source object failed size or BLAKE3 verification".to_owned(),
        ));
    }
    Ok(())
}

fn protocol_frame(message: &str) -> QuicTransferError {
    QuicTransferError::Protocol(message.into())
}

async fn send_frame(send: &mut SendStream, frame: &TransferFrame) -> Result<(), QuicTransferError> {
    let encoded = frame.encode()?;
    tokio::time::timeout(TRANSFER_FRAME_IO_TIMEOUT, send.write_all(&encoded))
        .await
        .map_err(|_| {
            QuicTransferError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "QUIC transfer frame write timed out",
            ))
        })??;
    Ok(())
}

async fn read_frame(recv: &mut RecvStream) -> Result<TransferFrame, QuicTransferError> {
    tokio::time::timeout(TRANSFER_FRAME_IO_TIMEOUT, read_frame_inner(recv))
        .await
        .map_err(|_| {
            QuicTransferError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "QUIC transfer frame read timed out",
            ))
        })?
}

async fn read_frame_inner(recv: &mut RecvStream) -> Result<TransferFrame, QuicTransferError> {
    let mut prefix = [0_u8; 4];
    recv.read_exact(&mut prefix).await?;
    let payload_len = u32::from_be_bytes(prefix) as usize;
    let total = payload_len
        .checked_add(4)
        .ok_or_else(|| protocol_frame("frame length overflow"))?;
    if payload_len == 0 || total > neoengram_domain::protocol::MAX_TRANSFER_FRAME_BYTES {
        return Err(protocol_frame("frame exceeds transfer limit"));
    }
    let mut encoded = vec![0_u8; total];
    encoded[..4].copy_from_slice(&prefix);
    recv.read_exact(&mut encoded[4..]).await?;
    Ok(TransferFrame::decode(&encoded)?)
}

/// Current wall-clock value for callers opening a short-lived transfer session.
#[allow(dead_code)]
pub fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use neoengram_domain::protocol::{
        AgentId, ArtifactId, CommitObject, DecimalU64, EdgeClusterId, GatewayPoolId,
        MaterializationBatchId, MaterializationId, MaterializationSource, MaterializationTarget,
        MountGeneration, ObjectEncoding, ObjectNamespaceId, ObjectRef, ObjectTicketId,
        PlacementGeneration, PlacementId, RouteGeneration, SessionGeneration, StorageVolumeId,
        TaskAttemptId, TaskId, TenantId, TransferEndpoint, TransferId, UnixMillis,
    };
    use neoengram_domain::{CommitId, ContentDigest, Generation};

    fn endpoint(name: &str) -> TransferEndpoint {
        TransferEndpoint {
            placement_id: PlacementId::new(format!("placement-{name}")).unwrap(),
            agent_id: AgentId::new(format!("agent-{name}")).unwrap(),
            gateway_pool_id: GatewayPoolId::new(format!("pool-{name}")).unwrap(),
            edge_cluster_id: EdgeClusterId::new(format!("cluster-{name}")).unwrap(),
            storage_volume_id: Some(StorageVolumeId::new(format!("volume-{name}")).unwrap()),
        }
    }

    fn ticket(set: &ObjectSet) -> TransferTicket {
        TransferTicket {
            transfer_id: TransferId::new("transfer-quic-test").unwrap(),
            tenant_id: TenantId::new("tenant-quic-test").unwrap(),
            artifact_id: ArtifactId::new("artifact-quic-test").unwrap(),
            commit_id: CommitId::from_bytes([7; 32]),
            object_set_digest: set.object_set_digest,
            source: endpoint("source"),
            target: endpoint("target"),
            source_session_generation: SessionGeneration::new(3),
            source_mount_generation: MountGeneration::new(4),
            source_route_generation: RouteGeneration::new(5),
            session_generation: SessionGeneration::new(6),
            mount_generation: MountGeneration::new(7),
            route_generation: RouteGeneration::new(8),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            max_bytes: DecimalU64::new(3),
            allowed_objects: set.objects.iter().map(|object| object.object_id).collect(),
        }
    }

    fn materialization_ticket(object: &ObjectRef) -> MaterializationBatchTicket {
        MaterializationBatchTicket {
            ticket_id: ObjectTicketId::new("ticket-source-placement").unwrap(),
            operation_task_id: TaskId::new("task-materialization-source-placement").unwrap(),
            task_attempt_id: TaskAttemptId::new("task-materialization-source-placement-attempt-1")
                .unwrap(),
            task_attempt: Generation::new(1),
            stage_key: "transfer".to_owned(),
            stage_attempt: Generation::new(1),
            materialization_id: MaterializationId::new("materialization-source-placement").unwrap(),
            batch_id: MaterializationBatchId::new("batch-source-placement").unwrap(),
            plan_revision: Generation::new(1),
            batch_attempt: Generation::new(1),
            tenant_id: TenantId::new("tenant-source-placement").unwrap(),
            artifact_id: ArtifactId::new(object.object_namespace_id.as_str()).unwrap(),
            object_namespace_id: object.object_namespace_id.clone(),
            commit_id: CommitId::from_bytes([8; 32]),
            manifest_digest: ContentDigest::from_bytes([9; 32]),
            source: MaterializationSource {
                placement_id: PlacementId::new("placement-source-v2").unwrap(),
                tenant_id: TenantId::new("tenant-source-placement").unwrap(),
                object_namespace_id: object.object_namespace_id.clone(),
                storage_volume_id: Some(StorageVolumeId::new("volume-source-v2").unwrap()),
                archive_id: None,
                agent_id: AgentId::new("agent-source-v2").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-source-v2").unwrap(),
                gateway_pool_id: GatewayPoolId::new("pool-source-v2").unwrap(),
                placement_generation: PlacementGeneration::new(3),
                session_generation: SessionGeneration::new(4),
                mount_generation: MountGeneration::new(5),
                route_generation: RouteGeneration::new(6),
            },
            target: MaterializationTarget {
                tenant_id: TenantId::new("tenant-source-placement").unwrap(),
                object_namespace_id: object.object_namespace_id.clone(),
                storage_volume_id: StorageVolumeId::new("volume-target-v2").unwrap(),
                agent_id: AgentId::new("agent-target-v2").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-target-v2").unwrap(),
                gateway_pool_id: GatewayPoolId::new("pool-target-v2").unwrap(),
                placement_generation: PlacementGeneration::new(7),
                session_generation: SessionGeneration::new(8),
                mount_generation: MountGeneration::new(9),
                route_generation: RouteGeneration::new(10),
            },
            max_bytes: DecimalU64::new(object.size.get().max(1)),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            capability: neoengram_domain::protocol::COMMIT_MATERIALIZATION_CAPABILITY_V2.to_owned(),
        }
    }

    fn source_placement(
        ticket: &MaterializationBatchTicket,
        object: &ObjectRef,
    ) -> MaterializationObjectPlacement {
        MaterializationObjectPlacement {
            placement_id: ticket.source.placement_id.clone(),
            tenant_id: ticket.tenant_id.clone(),
            object_namespace_id: ticket.object_namespace_id.clone(),
            object_id: object.object_id,
            size: object.size,
            encoding: object.encoding,
            verified_digest: object.object_id.digest(),
            storage_volume_id: ticket.source.storage_volume_id.clone(),
            archive_id: ticket.source.archive_id.clone(),
            placement_generation: ticket.source.placement_generation,
            state: ObjectPlacementState::Verified,
            failure_domain: "volume:volume-source-v2".to_owned(),
        }
    }

    fn gateway_peer_certificate(
        uris: &[&str],
        client_auth: bool,
        server_auth: bool,
    ) -> CertificateDer<'static> {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, SanType};

        let mut parameters = CertificateParams::new(vec!["gateway.example.test".to_owned()])
            .expect("valid test DNS SAN");
        for uri in uris {
            parameters.subject_alt_names.push(SanType::URI(
                (*uri).try_into().expect("test URI SAN must be IA5"),
            ));
        }
        parameters.extended_key_usages = [
            client_auth.then_some(ExtendedKeyUsagePurpose::ClientAuth),
            server_auth.then_some(ExtendedKeyUsagePurpose::ServerAuth),
        ]
        .into_iter()
        .flatten()
        .collect();
        let key = KeyPair::generate().expect("test key");
        CertificateDer::from(
            parameters
                .self_signed(&key)
                .expect("test certificate")
                .der()
                .to_vec(),
        )
    }

    #[test]
    fn source_listener_requires_gateway_workload_identity_and_both_ekus() {
        let edge_cluster_id = EdgeClusterId::new("edge-source").unwrap();
        let gateway_pool_id = GatewayPoolId::new("pool-source").unwrap();
        let uri = "spiffe://mesh.example.test/workloads/edge-clusters/edge-source/gateway-pools/pool-source/gateway-replicas/replica-source";
        let certificate = gateway_peer_certificate(&[uri], true, true);
        let chain = vec![certificate];
        validate_gateway_source_peer_certificate(
            Some(&chain),
            "mesh.example.test",
            &edge_cluster_id,
            &gateway_pool_id,
        )
        .unwrap();

        assert!(validate_gateway_source_peer_certificate(
            None,
            "mesh.example.test",
            &edge_cluster_id,
            &gateway_pool_id,
        )
        .is_err());

        for (client_auth, server_auth) in [(false, true), (true, false), (false, false)] {
            let certificate = gateway_peer_certificate(&[uri], client_auth, server_auth);
            let chain = vec![certificate];
            assert!(validate_gateway_source_peer_certificate(
                Some(&chain),
                "mesh.example.test",
                &edge_cluster_id,
                &gateway_pool_id,
            )
            .is_err());
        }
    }

    #[test]
    fn source_listener_binds_gateway_identity_to_ticket_cluster_and_pool() {
        let edge_cluster_id = EdgeClusterId::new("edge-source").unwrap();
        let gateway_pool_id = GatewayPoolId::new("pool-source").unwrap();
        let valid_uri = "spiffe://mesh.example.test/workloads/edge-clusters/edge-source/gateway-pools/pool-source/gateway-replicas/replica-source";
        let invalid_uris = [
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-other/gateway-pools/pool-source/gateway-replicas/replica-source",
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-source/gateway-pools/pool-other/gateway-replicas/replica-source",
            "spiffe://other.example.test/workloads/edge-clusters/edge-source/gateway-pools/pool-source/gateway-replicas/replica-source",
        ];
        for uri in invalid_uris {
            let certificate = gateway_peer_certificate(&[uri], true, true);
            let chain = vec![certificate];
            assert!(validate_gateway_source_peer_certificate(
                Some(&chain),
                "mesh.example.test",
                &edge_cluster_id,
                &gateway_pool_id,
            )
            .is_err());
        }

        let second_uri =
            "spiffe://mesh.example.test/workloads/edge-clusters/edge-source/gateway-pools/pool-source/gateway-replicas/replica-other";
        let certificate = gateway_peer_certificate(&[valid_uri, second_uri], true, true);
        let chain = vec![certificate];
        assert!(validate_gateway_source_peer_certificate(
            Some(&chain),
            "mesh.example.test",
            &edge_cluster_id,
            &gateway_pool_id,
        )
        .is_err());
    }

    #[test]
    fn client_config_is_bounded_by_wire_chunk_limit() {
        assert!(QuicTransferClientConfig { chunk_bytes: 0 }
            .validate()
            .is_err());
        assert!(QuicTransferClientConfig {
            chunk_bytes: MAX_TRANSFER_CHUNK_BYTES + 1
        }
        .validate()
        .is_err());
        assert!(QuicTransferClientConfig::default().validate().is_ok());
    }

    #[test]
    fn materialization_receipt_identity_changes_for_each_batch_attempt() {
        let materialization_id = MaterializationId::new("materialization-receipt-test").unwrap();
        let batch_id = MaterializationBatchId::new("batch-receipt-test").unwrap();
        let object_id = ObjectId::from_bytes([9; 32]);
        let first = materialization_receipt_id(
            &materialization_id,
            &batch_id,
            Generation::new(4),
            Generation::new(1),
            object_id,
        )
        .unwrap();
        let retry = materialization_receipt_id(
            &materialization_id,
            &batch_id,
            Generation::new(4),
            Generation::new(2),
            object_id,
        )
        .unwrap();
        assert_ne!(first, retry);
        assert_eq!(
            first,
            materialization_receipt_id(
                &materialization_id,
                &batch_id,
                Generation::new(4),
                Generation::new(1),
                object_id,
            )
            .unwrap()
        );
    }

    #[test]
    fn mounted_resolver_requires_exact_local_placement_metadata() {
        let object = ObjectRef::new(
            ObjectNamespaceId::new("artifact-source-v2").unwrap(),
            ObjectId::from_bytes([3; 32]),
            4,
            ObjectEncoding::Raw,
            0,
        );
        let ticket = materialization_ticket(&object);
        let volume = ticket.source.storage_volume_id.clone().unwrap();
        assert!(MountedVolumeSourcePlacementResolver::new(volume.clone())
            .resolve(&ticket, &object)
            .unwrap()
            .is_none());

        let inventory = Arc::new(InMemoryPlacementInventory::default());
        inventory
            .record(source_placement(&ticket, &object))
            .unwrap();
        let resolver = MountedVolumeSourcePlacementResolver::with_inventory(volume, inventory);
        let resolved = resolver.resolve(&ticket, &object).unwrap().unwrap();
        validate_materialization_source_placement(&ticket, &object, &resolved).unwrap();

        let mut mismatch = resolved.clone();
        mismatch.placement_id = PlacementId::new("placement-other").unwrap();
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved.clone();
        mismatch.object_namespace_id = ObjectNamespaceId::new("artifact-other").unwrap();
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved.clone();
        mismatch.size = DecimalU64::new(5);
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved.clone();
        mismatch.encoding = ObjectEncoding::Zstd;
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved.clone();
        mismatch.placement_generation = PlacementGeneration::new(4);
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved.clone();
        mismatch.storage_volume_id = Some(StorageVolumeId::new("volume-other").unwrap());
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
        let mut mismatch = resolved;
        mismatch.state = ObjectPlacementState::Retiring;
        assert!(validate_materialization_source_placement(&ticket, &object, &mismatch).is_err());
    }

    #[test]
    fn ticket_scope_and_generation_fences_are_checked_before_transfer() {
        let object = CommitObject::new(ObjectId::from_bytes([1; 32]), 3, ObjectEncoding::Raw, 0);
        let set = ObjectSet::new(vec![object]).unwrap();
        let transfer = ticket(&set);
        validate_ticket_set(&transfer, &set).unwrap();
        let identity = QuicTransferIdentity::new(AgentId::new("agent-target").unwrap())
            .with_generations(6, 7, 8);
        identity.check_target(&transfer).unwrap();
        let stale = identity.clone().with_generations(6, 7, 9);
        assert!(stale.check_target(&transfer).is_err());
        let session_mount = QuicTransferIdentity::new(AgentId::new("agent-target").unwrap())
            .with_session_mount(6, 7);
        session_mount.check_target(&transfer).unwrap();
        let mut replaced_session = transfer.clone();
        replaced_session.session_generation = SessionGeneration::new(9);
        assert!(session_mount.check_target(&replaced_session).is_err());
        let mut replaced_mount = transfer.clone();
        replaced_mount.mount_generation = MountGeneration::new(10);
        assert!(session_mount.check_target(&replaced_mount).is_err());
        let source_session_mount = QuicTransferIdentity::new(AgentId::new("agent-source").unwrap())
            .with_session_mount(3, 4);
        source_session_mount.check_source(&transfer).unwrap();
        let source_volume = QuicTransferIdentity::new(AgentId::new("agent-source").unwrap())
            .with_storage_volume(StorageVolumeId::new("volume-source").unwrap())
            .with_session_mount(3, 4);
        source_volume.check_source(&transfer).unwrap();
        let wrong_source_volume = source_volume
            .clone()
            .with_storage_volume(StorageVolumeId::new("volume-other").unwrap());
        assert!(wrong_source_volume.check_source(&transfer).is_err());
        let mut replaced_source_session = transfer.clone();
        replaced_source_session.source_session_generation = SessionGeneration::new(9);
        assert!(source_session_mount
            .check_source(&replaced_source_session)
            .is_err());
        let mut replaced_source_mount = transfer.clone();
        replaced_source_mount.source_mount_generation = MountGeneration::new(10);
        assert!(source_session_mount
            .check_source(&replaced_source_mount)
            .is_err());
        let target_volume = QuicTransferIdentity::new(AgentId::new("agent-target").unwrap())
            .with_storage_volume(StorageVolumeId::new("volume-target").unwrap())
            .with_session_mount(6, 7);
        target_volume.check_target(&transfer).unwrap();
        let wrong_target_volume = target_volume
            .clone()
            .with_storage_volume(StorageVolumeId::new("volume-other").unwrap());
        assert!(wrong_target_volume.check_target(&transfer).is_err());
        let development = QuicTransferIdentity::new(AgentId::new("agent-target").unwrap())
            .with_validation_mode(ValidationMode::Development)
            .with_generations(99, 99, 99);
        development
            .check_target(&replaced_mount)
            .expect("development profile defers deployment-owned identity fences");
        let mut unauthorized = transfer;
        unauthorized.allowed_objects.clear();
        assert!(validate_ticket_set(&unauthorized, &set).is_err());
    }

    #[test]
    fn transport_errors_are_retryable_but_protocol_errors_are_not() {
        assert!(
            QuicTransferError::Io(io::Error::new(io::ErrorKind::TimedOut, "test timeout",))
                .is_transient()
        );
        assert!(QuicTransferError::PreflightTimeout.is_transient());
        assert!(!QuicTransferError::Protocol("bad frame".to_owned()).is_transient());
        assert!(!QuicTransferError::Expired.is_transient());
    }

    #[test]
    fn mounted_source_verification_rejects_missing_and_corrupt_objects() {
        let temporary = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("tenant-source-inventory").unwrap();
        let artifact = ArtifactId::new("artifact-source-inventory").unwrap();
        let backend = neoengram_runtime::VolumeCasBackend::open_or_create_artifact_scoped(
            temporary.path(),
            tenant.clone(),
            artifact.clone(),
        )
        .unwrap();
        let expected = neoengram_runtime::ObjectSpec::for_bytes(b"source inventory payload");
        let transfer_id = TransferId::new("source-inventory-transfer").unwrap();
        backend
            .stage_write(
                &transfer_id,
                &tenant,
                &expected,
                0,
                b"source inventory payload",
            )
            .unwrap();
        backend
            .verify_and_publish(&transfer_id, &tenant, &expected)
            .unwrap();
        let object = ObjectRef::new(
            ObjectNamespaceId::new("artifact-source-inventory").unwrap(),
            expected.id,
            expected.size,
            ObjectEncoding::Raw,
            0,
        );
        verify_local_source_object(&backend, &tenant, &object).unwrap();

        let object_path = temporary
            .path()
            .join("tenants")
            .join(tenant.as_str())
            .join("artifacts")
            .join(artifact.as_str())
            .join("objects")
            .join(expected.id.to_hex());
        fs::write(&object_path, b"source inventory corrupte").unwrap();
        assert!(verify_local_source_object(&backend, &tenant, &object).is_err());

        fs::remove_file(object_path).unwrap();
        assert!(verify_local_source_object(&backend, &tenant, &object).is_err());
    }
}
