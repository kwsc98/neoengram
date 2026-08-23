//! Agent QUIC source/sink sessions for Commit object replication.
//!
//! This module is deliberately transport-only.  It speaks the bounded `TransferFrame` protocol
//! and delegates all durable state to `ObjectBackend`; Gateways can relay the stream without
//! seeing a Volume path or object bytes.  A caller supplies the Central trust bundle and the
//! immutable ObjectSet so a signed ticket cannot be widened by a peer.

use std::{
    collections::BTreeSet,
    fs, io,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use neoengram_domain::protocol::{
    CommitObjectSet, ObjectChunk, ObjectProof, ObjectRequest, ObjectSet, SignedTransferTicket,
    TransferFrame, TransferFrameError, TransferTicket, MAX_TRANSFER_CHUNK_BYTES,
};
use neoengram_domain::ObjectId;
use neoengram_domain::TenantId;
use neoengram_runtime::{ObjectBackend, ObjectRange};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rustls_pki_types::pem::PemObject;

use crate::CentralCommandTrustBundle;

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
        server.alpn_protocols = vec![neoengram_domain::protocol::TRANSFER_ALPN
            .as_bytes()
            .to_vec()];
        server.send_tls13_tickets = 0;
        server.max_tls13_tickets = 0;
        let server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_crypto));
        Arc::get_mut(&mut server_config.transport)
            .expect("new QUIC server config has a unique transport")
            .max_concurrent_bidi_streams(64u32.into());

        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, private_key)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        client.alpn_protocols = vec![neoengram_domain::protocol::TRANSFER_ALPN
            .as_bytes()
            .to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        let client_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client)
            .map_err(|error| QuicTransferError::Tls(error.to_string()))?;
        let client_config = quinn::ClientConfig::new(Arc::new(client_crypto));
        let mut endpoint = match config.listen {
            Some(address) => quinn::Endpoint::server(server_config, address)?,
            None => quinn::Endpoint::client("0.0.0.0:0".parse().expect("valid client bind"))?,
        };
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint: Arc::new(endpoint),
            gateway_endpoint: config.gateway_endpoint,
            server_name: Arc::from(config.server_name.as_str()),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, QuicTransferError> {
        self.endpoint.local_addr().map_err(QuicTransferError::Io)
    }

    pub async fn connect_gateway(&self) -> Result<Connection, QuicTransferError> {
        let address = self.gateway_endpoint.ok_or_else(|| {
            QuicTransferError::Protocol("target Gateway endpoint is not configured".into())
        })?;
        Ok(self.endpoint.connect(address, &self.server_name)?.await?)
    }

    /// Serves source streams until the session is fenced or shutdown is requested.  The backend
    /// is selected only after the signed ticket has been validated, so a peer cannot choose an
    /// arbitrary artifact path by manipulating the QUIC handshake.
    pub async fn serve_source(
        &self,
        trust_bundle: Arc<CentralCommandTrustBundle>,
        local_tenant_id: TenantId,
        local_agent_id: neoengram_domain::AgentId,
        execution: Arc<crate::FilesystemExecution>,
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
            let execution = Arc::clone(&execution);
            tokio::spawn(async move {
                let result = async {
                    let connection = incoming.await?;
                    let (send, recv) = connection.accept_bi().await?;
                    let identity = QuicTransferIdentity::new(local_agent_id);
                    serve_quic_source_connection(
                        send,
                        recv,
                        move |ticket| {
                            if ticket.ticket.tenant_id != local_tenant_id {
                                return Err(QuicTransferError::Ticket(
                                    "source Agent tenant does not match transfer ticket".into(),
                                ));
                            }
                            execution
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
    generations: Option<(u64, u64, u64)>,
}

impl QuicTransferIdentity {
    #[must_use]
    pub fn new(agent_id: neoengram_domain::AgentId) -> Self {
        Self {
            agent_id,
            generations: None,
        }
    }

    #[must_use]
    pub fn with_generations(mut self, session: u64, mount: u64, route: u64) -> Self {
        self.generations = Some((session, mount, route));
        self
    }

    fn check_source(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        if ticket.source.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "ticket source Agent does not match local Agent".into(),
            ));
        }
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
        Ok(())
    }

    fn check_target(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        if ticket.target.agent_id != self.agent_id {
            return Err(QuicTransferError::Protocol(
                "ticket target Agent does not match local Agent".into(),
            ));
        }
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
        let (mut send, mut recv) = self.connection.open_bi().await.map_err(|error| {
            QuicTransferError::Protocol(format!("failed to open transfer stream: {error}"))
        })?;
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
    mut send: SendStream,
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

fn backend_error(error: impl std::fmt::Display) -> QuicTransferError {
    QuicTransferError::Backend(error.to_string())
}
fn protocol_frame(message: &str) -> QuicTransferError {
    QuicTransferError::Protocol(message.into())
}

async fn send_frame(send: &mut SendStream, frame: &TransferFrame) -> Result<(), QuicTransferError> {
    let encoded = frame.encode()?;
    send.write_all(&encoded).await?;
    Ok(())
}

async fn read_frame(recv: &mut RecvStream) -> Result<TransferFrame, QuicTransferError> {
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
        MountGeneration, ObjectEncoding, PlacementId, RouteGeneration, SessionGeneration,
        StorageVolumeId, TenantId, TransferEndpoint, TransferId, UnixMillis,
    };
    use neoengram_domain::CommitId;

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
        let mut unauthorized = transfer;
        unauthorized.allowed_objects.clear();
        assert!(validate_ticket_set(&unauthorized, &set).is_err());
    }
}
