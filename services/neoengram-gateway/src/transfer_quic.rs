//! QUIC data-plane boundary for object replication.
//!
//! Gateway owns the QUIC hop and relay policy, but never owns object bytes or a Volume mount.
//! The first frame on every stream is the binary `OpenTransfer` frame; the ticket is validated
//! before any object request is accepted.  The actual object source/sink remains an Agent
//! concern, so this module can be wired to either an in-process same-Gateway relay or a peer
//! Gateway connection without changing the domain protocol.

use std::{
    collections::BTreeMap,
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use neoengram_domain::protocol::{
    AgentId, EdgeClusterId, GatewayPoolId, SignedTransferTicket, TransferFrame, TransferFrameError,
    TransferTicket, MAX_TRANSFER_FRAME_BYTES, TRANSFER_ALPN,
};
use quinn::{Connection, Endpoint, Incoming, RecvStream, SendStream};
use tokio::{
    sync::{watch, Semaphore},
    task::JoinSet,
};
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicTransferError {
    #[error("QUIC connection could not be started: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("QUIC connection failed: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("QUIC stream failed: {0}")]
    Stream(#[from] quinn::ReadToEndError),
    #[error("QUIC frame read failed: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("QUIC stream write failed: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("QUIC send stream is already closed")]
    ClosedStream(#[from] quinn::ClosedStream),
    #[error("QUIC stream acknowledgement failed: {0}")]
    Stopped(#[from] quinn::StoppedError),
    #[error("QUIC peer stopped a relay stream before acknowledging all frames: {0:?}")]
    PeerStopped(quinn::VarInt),
    #[error("invalid transfer frame: {0}")]
    Frame(#[from] TransferFrameError),
    #[error("transfer ticket is expired")]
    Expired,
    #[error("transfer connection deadline elapsed")]
    Deadline,
    #[error("transfer ticket does not match Gateway ALPN")]
    Alpn,
    #[error("transfer connection did not provide a peer certificate")]
    MissingPeerIdentity,
    #[error("transfer ticket is fenced: {0}")]
    Fenced(&'static str),
    #[error("QUIC TLS configuration is invalid: {0}")]
    Tls(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
    #[error("failed to bind QUIC listener: {0}")]
    Bind(#[from] io::Error),
}

/// The endpoint identity and generation tuple that a Gateway validates on its incoming hop.
/// Requests travel from the target toward the source, so the target Gateway fences the target
/// tuple while the source Gateway fences the source tuple before either opens its next hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum TransferRelayRole {
    Target,
    Source,
}

/// Generation and identity fences applied before any object frame is accepted.  A listener has
/// no storage handle; the caller can update this value when Central replaces a route/session.
#[derive(Debug, Clone, Copy, Default)]
struct TransferGenerations {
    session: Option<u64>,
    mount: Option<u64>,
    route: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct TransferFenceState {
    /// Static deployments may configure one tuple before the Gateway has a Central route. It is a
    /// fallback only; a Central-granted Agent route always gets an exact per-Agent entry.
    default: Option<TransferGenerations>,
    agents: BTreeMap<AgentId, TransferGenerations>,
}

#[derive(Debug, Clone)]
pub(crate) struct QuicTransferFence {
    role: TransferRelayRole,
    gateway_pool_id: GatewayPoolId,
    edge_cluster_id: EdgeClusterId,
    state: Arc<RwLock<TransferFenceState>>,
}

impl QuicTransferFence {
    #[must_use]
    pub(crate) fn new(gateway_pool_id: GatewayPoolId, edge_cluster_id: EdgeClusterId) -> Self {
        Self::for_role(TransferRelayRole::Target, gateway_pool_id, edge_cluster_id)
    }

    #[must_use]
    pub(crate) fn for_role(
        role: TransferRelayRole,
        gateway_pool_id: GatewayPoolId,
        edge_cluster_id: EdgeClusterId,
    ) -> Self {
        Self {
            role,
            gateway_pool_id,
            edge_cluster_id,
            state: Arc::new(RwLock::new(TransferFenceState::default())),
        }
    }

    /// Installs the current generations for a route.  Until all three values are installed, the
    /// listener rejects transfers because no authenticated Agent route is active.
    #[must_use]
    pub(crate) fn with_generations(
        self,
        session_generation: u64,
        mount_generation: u64,
        route_generation: u64,
    ) -> Self {
        self.set_generations(session_generation, mount_generation, route_generation);
        self
    }

    /// Updates the route fence after the Gateway acquires or renews its Central route lease.
    /// The shared state is intentionally independent from the listener lifetime, so every
    /// accepted transfer observes the latest session/mount/route tuple after an Agent reconnect.
    pub(crate) fn set_generations(
        &self,
        session_generation: u64,
        mount_generation: u64,
        route_generation: u64,
    ) {
        if let Ok(mut state) = self.state.write() {
            state.agents.clear();
            state.default = Some(TransferGenerations {
                session: Some(session_generation),
                mount: Some(mount_generation),
                route: Some(route_generation),
            });
        }
    }

    /// Installs a complete route fence for one Agent. Routes for other Agents remain untouched.
    pub(crate) fn set_agent_generations(
        &self,
        agent_id: &AgentId,
        session_generation: u64,
        mount_generation: u64,
        route_generation: u64,
    ) {
        if let Ok(mut state) = self.state.write() {
            // A static fallback has no Agent identity and is only safe before Central has
            // published any dynamic route. Once one Agent is learned, unknown Agent IDs must
            // fail closed instead of borrowing that fallback tuple.
            state.default = None;
            let next = TransferGenerations {
                session: Some(session_generation),
                mount: Some(mount_generation),
                route: Some(route_generation),
            };
            match state.agents.get_mut(agent_id) {
                Some(current)
                    if current.route.is_some_and(|current_route| {
                        current_route > route_generation
                            || (current_route == route_generation
                                && current.session != Some(session_generation))
                    }) =>
                {
                    // A delayed Opened frame from a replaced stream cannot move the fence back
                    // to an older route, nor complete a conflicting session for one generation.
                }
                Some(current) => *current = next,
                None => {
                    state.agents.insert(agent_id.clone(), next);
                }
            }
        }
    }

    /// Updates the session and route part of the fence while retaining the mount generation
    /// supplied by the Agent's channel.opened response or a static deployment configuration.
    pub(crate) fn set_route_generations(&self, session_generation: u64, route_generation: u64) {
        if let Ok(mut state) = self.state.write() {
            if let Some(generations) = state.default.as_mut() {
                generations.session = Some(session_generation);
                generations.route = Some(route_generation);
            }
        }
    }

    /// Publishes one Agent route before its Opened frame is accepted. A same-session renewal keeps
    /// the authenticated mount; a replacement session clears it and therefore stays fail-closed
    /// until `set_agent_generations` installs the complete tuple.
    pub(crate) fn set_agent_route_generations(
        &self,
        agent_id: &AgentId,
        session_generation: u64,
        route_generation: u64,
    ) {
        if let Ok(mut state) = self.state.write() {
            // A dynamic route is authoritative even before its Opened frame arrives. Remove the
            // identity-free static fallback immediately so startup cannot expose that tuple to an
            // unrelated Agent while the new route is still being authenticated.
            state.default = None;
            let generations = state.agents.entry(agent_id.clone()).or_default();
            if generations
                .route
                .is_none_or(|current_route| route_generation >= current_route)
            {
                if generations.session != Some(session_generation) {
                    // A new session has a new mount identity until its authenticated Opened frame
                    // supplies the complete tuple. Retaining the previous mount would authorize
                    // a generation combination that Central never issued.
                    generations.mount = None;
                }
                generations.session = Some(session_generation);
                generations.route = Some(route_generation);
            }
        }
    }

    /// Revokes the transfer route while no authenticated Agent route is active.
    pub(crate) fn clear_generations(&self) {
        if let Ok(mut state) = self.state.write() {
            state.default = None;
            state.agents.clear();
        }
    }

    /// Revokes one Agent route without affecting other Agents sharing this Gateway listener.
    pub(crate) fn clear_agent_generations(&self, agent_id: &AgentId) {
        if let Ok(mut state) = self.state.write() {
            state.agents.remove(agent_id);
            // There is no Agent identity in the legacy static tuple. Clearing any named route
            // therefore revokes that compatibility fallback as well instead of leaving an
            // apparently removed route usable by an arbitrary ticket.
            state.default = None;
        }
    }

    /// Conditionally revokes one Agent route. The generation comparison is the linearization
    /// point for stream teardown, so a stale worker cannot clear a newer route for the same Agent.
    pub(crate) fn clear_agent_generations_if_current(
        &self,
        agent_id: &AgentId,
        session_generation: u64,
        route_generation: u64,
    ) -> bool {
        let Ok(mut state) = self.state.write() else {
            return false;
        };
        let current = state.agents.get(agent_id).copied();
        let matches = current.is_some_and(|generations| {
            generations.session == Some(session_generation)
                && generations.route == Some(route_generation)
        });
        let mut revoked = matches;
        if matches {
            state.agents.remove(agent_id);
        } else if state.agents.is_empty()
            && state.default.is_some_and(|generations| {
                generations.session == Some(session_generation)
                    && generations.route == Some(route_generation)
            })
        {
            // Compatibility-mode cleanup is still conditional on the exact tuple.
            state.default = None;
            revoked = true;
        }
        revoked
    }

    #[cfg(test)]
    pub(crate) fn agent_generations(&self, agent_id: &AgentId) -> Option<(u64, u64, u64)> {
        let state = self.state.read().ok()?;
        let generations = state.agents.get(agent_id)?;
        Some((generations.session?, generations.mount?, generations.route?))
    }

    fn validate(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        let (endpoint, session, mount, route, prefix) = match self.role {
            TransferRelayRole::Target => (
                &ticket.target,
                ticket.session_generation.get(),
                ticket.mount_generation.get(),
                ticket.route_generation.get(),
                "target",
            ),
            TransferRelayRole::Source => (
                &ticket.source,
                ticket.source_session_generation.get(),
                ticket.source_mount_generation.get(),
                ticket.source_route_generation.get(),
                "source",
            ),
        };
        if endpoint.gateway_pool_id != self.gateway_pool_id {
            return Err(QuicTransferError::Fenced(match self.role {
                TransferRelayRole::Target => "target_gateway_pool_id",
                TransferRelayRole::Source => "source_gateway_pool_id",
            }));
        }
        if endpoint.edge_cluster_id != self.edge_cluster_id {
            return Err(QuicTransferError::Fenced(match self.role {
                TransferRelayRole::Target => "target_edge_cluster_id",
                TransferRelayRole::Source => "source_edge_cluster_id",
            }));
        }
        let state = self
            .state
            .read()
            .map_err(|_| QuicTransferError::Fenced("transfer_route_unavailable"))?;
        let generations = state
            .agents
            .get(&endpoint.agent_id)
            .copied()
            .or_else(|| state.agents.is_empty().then_some(state.default).flatten())
            .ok_or(QuicTransferError::Fenced("transfer_route_unavailable"))?;
        let Some(expected_session) = generations.session else {
            return Err(QuicTransferError::Fenced("transfer_route_unavailable"));
        };
        if expected_session != session {
            return Err(QuicTransferError::Fenced(match prefix {
                "target" => "target_session_generation",
                _ => "source_session_generation",
            }));
        }
        let Some(expected_mount) = generations.mount else {
            return Err(QuicTransferError::Fenced("transfer_route_unavailable"));
        };
        if expected_mount != mount {
            return Err(QuicTransferError::Fenced(match prefix {
                "target" => "target_mount_generation",
                _ => "source_mount_generation",
            }));
        }
        let Some(expected_route) = generations.route else {
            return Err(QuicTransferError::Fenced("transfer_route_unavailable"));
        };
        if expected_route != route {
            return Err(QuicTransferError::Fenced(match prefix {
                "target" => "target_route_generation",
                _ => "source_route_generation",
            }));
        }
        Ok(())
    }
}

/// A Gateway-owned relay hook.  Implementations may connect to a same-Gateway Agent or to a
/// peer Gateway, but they never receive a Volume/ObjectStore handle from this module.
#[async_trait]
pub(crate) trait TransferRelay: fmt::Debug + Send + Sync {
    async fn relay(
        &self,
        ticket: SignedTransferTicket,
        send: SendStream,
        recv: RecvStream,
    ) -> Result<(), QuicTransferError>;
}

/// A connector used by [`ConnectedTransferRelay`] to establish the next single QUIC hop.
/// Route selection is deliberately outside this crate's listener and can be backed by the
/// Central-provided Gateway/Agent route lease without giving the Gateway a database handle.
#[async_trait]
pub(crate) trait TransferConnectionFactory: fmt::Debug + Send + Sync {
    async fn connect(&self, ticket: &TransferTicket) -> Result<Connection, QuicTransferError>;
}

/// A configured, storage-free QUIC next hop. Each Gateway owns one outbound endpoint and one
/// upstream address; the immutable ticket chooses no network address and therefore cannot turn
/// the relay into an open proxy.
pub(crate) struct QuinnTransferConnectionFactory {
    endpoint: Endpoint,
    upstream: SocketAddr,
    server_name: Arc<str>,
}

impl fmt::Debug for QuinnTransferConnectionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnTransferConnectionFactory")
            .field("upstream", &self.upstream)
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl QuinnTransferConnectionFactory {
    pub(crate) fn bind(
        upstream: SocketAddr,
        server_name: impl Into<Arc<str>>,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> Result<Self, QuicTransferError> {
        let bind_address = SocketAddr::new(
            match upstream.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            },
            0,
        );
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from((*tls_config).clone())?;
        let mut endpoint = Endpoint::client(bind_address)?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        Ok(Self {
            endpoint,
            upstream,
            server_name: server_name.into(),
        })
    }
}

#[async_trait]
impl TransferConnectionFactory for QuinnTransferConnectionFactory {
    async fn connect(&self, ticket: &TransferTicket) -> Result<Connection, QuicTransferError> {
        let remaining = ticket
            .deadline_unix_ms
            .get()
            .saturating_sub(unix_millis_now());
        if remaining == 0 {
            return Err(QuicTransferError::Expired);
        }
        let connecting = self
            .endpoint
            .connect(self.upstream, self.server_name.as_ref())?;
        tokio::time::timeout(Duration::from_millis(remaining), connecting)
            .await
            .map_err(|_| QuicTransferError::Deadline)?
            .map_err(QuicTransferError::Connection)
    }
}

/// Relay implementation that opens one next hop, sends the same immutable ticket, then forwards
/// bounded binary frames in both directions.  `read_frame`/`send_frame` are used for every frame,
/// so a peer cannot smuggle an unbounded payload through a raw byte copy.
pub(crate) struct ConnectedTransferRelay {
    connector: Arc<dyn TransferConnectionFactory>,
}

impl fmt::Debug for ConnectedTransferRelay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectedTransferRelay")
            .field("connector", &"configured")
            .finish()
    }
}

impl ConnectedTransferRelay {
    #[must_use]
    pub(crate) fn new(connector: Arc<dyn TransferConnectionFactory>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl TransferRelay for ConnectedTransferRelay {
    async fn relay(
        &self,
        ticket: SignedTransferTicket,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<(), QuicTransferError> {
        let connection = self.connector.connect(&ticket.ticket).await?;
        let (mut peer_send, mut peer_recv) = connection.open_bi().await?;
        send_frame(&mut peer_send, &TransferFrame::OpenTransferSigned(ticket)).await?;
        relay_frames(&mut recv, &mut send, &mut peer_recv, &mut peer_send).await?;
        finish_relay_streams(&mut send, &mut peer_send).await
    }
}

async fn finish_relay_streams(
    left_send: &mut SendStream,
    right_send: &mut SendStream,
) -> Result<(), QuicTransferError> {
    // Finish both directions before waiting for acknowledgements. A peer can stop one stream as
    // soon as it receives the terminal frame; retaining the second finish attempt ensures that a
    // fast stop on one hop does not leave the other hop half-open.
    let left_finish = left_send.finish().err();
    let right_finish = right_send.finish().err();
    let (left_stopped, right_stopped) =
        tokio::try_join!(left_send.stopped(), right_send.stopped())?;
    if let Some(code) = left_stopped.or(right_stopped) {
        return Err(QuicTransferError::PeerStopped(code));
    }
    if let Some(error) = left_finish.or(right_finish) {
        return Err(error.into());
    }
    Ok(())
}

/// Policy listener.  It intentionally contains no storage handle or Central client.  Without a
/// configured relay it remains fail-closed, which is useful while a route lease is being drained.
#[derive(Clone)]
pub(crate) struct QuicTransferListener {
    endpoint: Arc<Endpoint>,
    fence: Option<QuicTransferFence>,
    relay: Option<Arc<dyn TransferRelay>>,
}

impl fmt::Debug for QuicTransferListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicTransferListener")
            .field("endpoint", &self.endpoint)
            .field("fence", &self.fence)
            .field("relay", &self.relay.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl QuicTransferListener {
    /// Binds a QUIC endpoint with the supplied mTLS rustls configuration.  The ALPN is checked
    /// again after the handshake so a misconfigured client cannot use this UDP port for another
    /// protocol.
    pub(crate) fn bind(
        address: SocketAddr,
        tls_config: Arc<rustls::ServerConfig>,
        fence: QuicTransferFence,
    ) -> Result<Self, QuicTransferError> {
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        let transport = Arc::get_mut(&mut server_config.transport)
            .expect("new QUIC server config must have one transport owner");
        transport.max_concurrent_bidi_streams(64u32.into());
        transport.max_concurrent_uni_streams(0u32.into());
        let endpoint = Endpoint::server(server_config, address)?;
        Ok(Self {
            endpoint: Arc::new(endpoint),
            fence: Some(fence),
            relay: None,
        })
    }

    #[must_use]
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint: Arc::new(endpoint),
            fence: None,
            relay: None,
        }
    }

    #[must_use]
    pub(crate) fn with_relay(mut self, relay: Arc<dyn TransferRelay>) -> Self {
        self.relay = Some(relay);
        self
    }

    #[must_use]
    pub(crate) fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint.local_addr().ok()
    }

    /// Accepts one connection.  Callers must hand the returned connection to an Agent relay;
    /// this layer only performs framing and capability checks.
    pub(crate) async fn accept(&self) -> Option<quinn::Incoming> {
        self.endpoint.accept().await
    }

    #[must_use]
    pub(crate) fn open_connections(&self) -> usize {
        self.endpoint.open_connections()
    }

    #[must_use]
    pub(crate) fn fence(&self) -> Option<&QuicTransferFence> {
        self.fence.as_ref()
    }

    #[must_use]
    pub(crate) fn relay(&self) -> Option<&Arc<dyn TransferRelay>> {
        self.relay.as_ref()
    }
}

/// Reads and validates the mandatory first frame of a transfer control stream.
pub(crate) async fn open_transfer(
    connection: &Connection,
    recv: &mut RecvStream,
) -> Result<TransferTicket, QuicTransferError> {
    open_transfer_with_fence(connection, recv, None).await
}

/// Reads and validates the first frame with the listener's identity/generation fence.
pub(crate) async fn open_transfer_with_fence(
    connection: &Connection,
    recv: &mut RecvStream,
    fence: Option<&QuicTransferFence>,
) -> Result<TransferTicket, QuicTransferError> {
    let frame = read_frame(recv).await?;
    let TransferFrame::OpenTransfer(ticket) = frame else {
        return Err(TransferFrameError::InvalidField("first_frame").into());
    };
    validate_connection_handshake(connection, &ticket, fence)?;
    Ok(ticket)
}

/// Reads a Central-signed transfer envelope. Production listeners use this entry point so an
/// unsigned bearer ticket cannot be injected into a Gateway transfer stream.
pub(crate) async fn open_signed_transfer_with_fence(
    connection: &Connection,
    recv: &mut RecvStream,
    fence: Option<&QuicTransferFence>,
) -> Result<SignedTransferTicket, QuicTransferError> {
    let frame = read_frame(recv).await?;
    let TransferFrame::OpenTransferSigned(ticket) = frame else {
        return Err(TransferFrameError::InvalidField("signed_first_frame").into());
    };
    validate_connection_handshake(connection, &ticket.ticket, fence)?;
    Ok(ticket)
}

fn validate_connection_handshake(
    connection: &Connection,
    ticket: &TransferTicket,
    fence: Option<&QuicTransferFence>,
) -> Result<(), QuicTransferError> {
    let handshake = connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .ok_or(QuicTransferError::Alpn)?;
    if handshake.protocol.as_deref() != Some(alpn()) {
        return Err(QuicTransferError::Alpn);
    }
    if connection.peer_identity().is_none() {
        return Err(QuicTransferError::MissingPeerIdentity);
    }
    if ticket.deadline_unix_ms.get() <= unix_millis_now() {
        return Err(QuicTransferError::Expired);
    }
    if let Some(fence) = fence {
        fence.validate(ticket)?;
    }
    Ok(())
}

/// Reads one length-prefixed binary frame.  Reading only the declared payload keeps a control
/// stream usable for subsequent ObjectRequest/ObjectAck frames; `read_to_end` would wait forever
/// on a long-lived transfer stream.
pub(crate) async fn read_frame(recv: &mut RecvStream) -> Result<TransferFrame, QuicTransferError> {
    let mut prefix = [0_u8; 4];
    recv.read_exact(&mut prefix).await?;
    let payload_len = u32::from_be_bytes(prefix) as usize;
    let total_len = payload_len
        .checked_add(4)
        .ok_or(TransferFrameError::LimitExceeded {
            field: "frame",
            limit: MAX_TRANSFER_FRAME_BYTES,
            actual: usize::MAX,
        })?;
    if total_len > MAX_TRANSFER_FRAME_BYTES || payload_len == 0 {
        return Err(TransferFrameError::LimitExceeded {
            field: "frame",
            limit: MAX_TRANSFER_FRAME_BYTES,
            actual: total_len,
        }
        .into());
    }
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&prefix);
    encoded.resize(total_len, 0);
    recv.read_exact(&mut encoded[4..]).await?;
    Ok(TransferFrame::decode(&encoded)?)
}

async fn relay_direction(
    recv: &mut RecvStream,
    send: &mut SendStream,
) -> Result<(), QuicTransferError> {
    loop {
        let frame = read_frame(recv).await?;
        // Both frames are terminal application decisions. Continuing to read after a transfer
        // error can turn the peer's intentional failure into an unrelated EOF/reset and can keep
        // the opposite direction alive after the capability has already been rejected.
        let terminal = matches!(
            frame,
            TransferFrame::CloseTransfer(_) | TransferFrame::TransferError(_)
        );
        send_frame(send, &frame).await?;
        if terminal {
            return Ok(());
        }
    }
}

/// Relays one bounded control stream in both directions with QUIC backpressure.  The first side
/// to close terminates the relay and the caller is responsible for closing the connection; this
/// prevents a half-open source from keeping a target transfer alive indefinitely.
pub(crate) async fn relay_frames(
    left_recv: &mut RecvStream,
    left_send: &mut SendStream,
    right_recv: &mut RecvStream,
    right_send: &mut SendStream,
) -> Result<(), QuicTransferError> {
    let left_to_right = relay_direction(left_recv, right_send);
    let right_to_left = relay_direction(right_recv, left_send);
    tokio::select! {
        result = left_to_right => result,
        result = right_to_left => result,
    }
}

/// Runs the policy-only listener.  A future Agent relay can replace `handle_connection` without
/// changing endpoint construction or ticket fencing.  Until that relay is installed, a valid
/// OpenTransfer is acknowledged at the policy boundary and the connection is closed fail-closed;
/// no object bytes are persisted or forwarded by Gateway.
pub(crate) async fn serve(
    listener: QuicTransferListener,
    max_connections: usize,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let admission = Arc::new(Semaphore::new(max_connections.max(1)));
    info!(address = ?listener.local_addr(), alpn = TRANSFER_ALPN, "Gateway QUIC transfer listener started");
    let mut tasks: JoinSet<()> = JoinSet::new();
    loop {
        let incoming = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            incoming = listener.accept() => incoming,
        };
        let Some(incoming) = incoming else { break };
        let permit = match admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                incoming.refuse();
                continue;
            }
        };
        let fence = listener.fence().cloned();
        let relay = listener.relay().cloned();
        tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = handle_incoming(incoming, fence, relay).await {
                warn!(%error, "Gateway QUIC transfer rejected");
            }
        });
    }
    listener.endpoint.close(0u32.into(), b"gateway draining");
    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn handle_incoming(
    incoming: Incoming,
    fence: Option<QuicTransferFence>,
    relay: Option<Arc<dyn TransferRelay>>,
) -> Result<(), QuicTransferError> {
    let connection = incoming.await.map_err(QuicTransferError::Connection)?;
    let (send, mut recv) = connection.accept_bi().await?;
    let signed_ticket =
        open_signed_transfer_with_fence(&connection, &mut recv, fence.as_ref()).await?;
    if let Some(relay) = relay {
        relay.relay(signed_ticket, send, recv).await?;
    } else {
        // No route lease means no object bytes may enter the process.  Closing here is the
        // fail-closed behavior used during startup and drain.
        connection.close(0x100u32.into(), b"transfer relay unavailable");
    }
    Ok(())
}

/// Sends a bounded frame on a QUIC control stream.
pub(crate) async fn send_frame(
    send: &mut SendStream,
    frame: &TransferFrame,
) -> Result<(), QuicTransferError> {
    let encoded = frame.encode()?;
    send.write_all(&encoded).await?;
    Ok(())
}

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// The ALPN value used when constructing a Quinn client/server config.
#[must_use]
pub(crate) const fn alpn() -> &'static [u8] {
    TRANSFER_ALPN.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use neoengram_domain::protocol::{
        AgentId, CentralSignedPayload, CertificateGeneration, CloseTransfer, ContentDigest,
        DecimalU64, Ed25519Signature, Extensions, GatewayOpaqueBytes, MountGeneration, ObjectChunk,
        ObjectRequest, PlacementId, RouteGeneration, SessionGeneration, TenantId, TransferEndpoint,
        TransferId, UnixMillis,
    };
    use neoengram_domain::{CommitId, ObjectId};
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };
    use rustls::{server::WebPkiClientVerifier, ClientConfig, RootCertStore, ServerConfig};
    use rustls_pki_types::PrivatePkcs8KeyDer;

    fn ticket() -> TransferTicket {
        let target_pool = GatewayPoolId::new("gateway-target").unwrap();
        let target_cluster = EdgeClusterId::new("cluster-target").unwrap();
        TransferTicket {
            transfer_id: TransferId::new("transfer-test").unwrap(),
            tenant_id: TenantId::new("tenant-test").unwrap(),
            artifact_id: neoengram_domain::protocol::ArtifactId::new("artifact-test").unwrap(),
            commit_id: CommitId::from_bytes([1; 32]),
            object_set_digest: ContentDigest::from_bytes([2; 32]),
            source: TransferEndpoint {
                placement_id: PlacementId::new("placement-source").unwrap(),
                agent_id: AgentId::new("agent-source").unwrap(),
                gateway_pool_id: GatewayPoolId::new("gateway-source").unwrap(),
                edge_cluster_id: EdgeClusterId::new("cluster-source").unwrap(),
                storage_volume_id: None,
            },
            target: TransferEndpoint {
                placement_id: PlacementId::new("placement-target").unwrap(),
                agent_id: AgentId::new("agent-target").unwrap(),
                gateway_pool_id: target_pool,
                edge_cluster_id: target_cluster,
                storage_volume_id: None,
            },
            source_session_generation: SessionGeneration::new(4),
            source_mount_generation: MountGeneration::new(5),
            source_route_generation: RouteGeneration::new(6),
            session_generation: SessionGeneration::new(7),
            mount_generation: MountGeneration::new(8),
            route_generation: RouteGeneration::new(9),
            deadline_unix_ms: UnixMillis::new(unix_millis_now() + 60_000),
            max_bytes: DecimalU64::new(4096),
            allowed_objects: vec![ObjectId::from_bytes([3; 32])],
        }
    }

    fn signed_ticket() -> SignedTransferTicket {
        let ticket = ticket();
        let payload =
            GatewayOpaqueBytes::new(SignedTransferTicket::payload_bytes(&ticket).unwrap()).unwrap();
        SignedTransferTicket::new(
            ticket,
            CentralSignedPayload {
                key_id: "transfer-network-test".to_owned(),
                certificate_generation: CertificateGeneration::new(1),
                signed_at_unix_ms: UnixMillis::new(1),
                expires_at_unix_ms: UnixMillis::new(u64::MAX),
                payload_digest: ContentDigest::hash(payload.as_bytes()),
                payload,
                signature: Ed25519Signature::from_bytes([0; 64]),
                extensions: Extensions::new(),
            },
        )
        .unwrap()
    }

    fn transfer_tls() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_parameters.self_signed(&ca_key).unwrap();

        let workload_key = KeyPair::generate().unwrap();
        let mut workload_parameters = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        workload_parameters.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let workload = workload_parameters
            .signed_by(&workload_key, &ca, &ca_key)
            .unwrap();
        let chain = vec![workload.der().clone(), ca.der().clone()];
        let key = PrivatePkcs8KeyDer::from(workload_key.serialize_der());
        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();

        let mut client_roots = RootCertStore::empty();
        client_roots.add(ca.der().clone()).unwrap();
        let mut client = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(client_roots)
            .with_client_auth_cert(chain.clone(), key.clone_key().into())
            .unwrap();
        client.alpn_protocols = vec![alpn().to_vec()];
        client.resumption = rustls::client::Resumption::disabled();

        let mut server_roots = RootCertStore::empty();
        server_roots.add(ca.der().clone()).unwrap();
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(server_roots), provider.clone())
                .build()
                .unwrap();
        let mut server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key.into())
            .unwrap();
        server.alpn_protocols = vec![alpn().to_vec()];
        server.send_tls13_tickets = 0;
        server.max_tls13_tickets = 0;
        (Arc::new(server), Arc::new(client))
    }

    fn source_endpoint(tls: Arc<ServerConfig>) -> Endpoint {
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(crypto)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn fence_accepts_current_identity_and_generations() {
        let ticket = ticket();
        let fence = QuicTransferFence::new(
            ticket.target.gateway_pool_id.clone(),
            ticket.target.edge_cluster_id.clone(),
        )
        .with_generations(7, 8, 9);
        assert!(fence.validate(&ticket).is_ok());
        assert_eq!(alpn(), TRANSFER_ALPN.as_bytes());
    }

    #[test]
    fn fence_rejects_stale_route_generation_and_wrong_target() {
        let ticket = ticket();
        let stale = QuicTransferFence::new(
            ticket.target.gateway_pool_id.clone(),
            ticket.target.edge_cluster_id.clone(),
        )
        .with_generations(7, 8, 10);
        assert!(matches!(
            stale.validate(&ticket),
            Err(QuicTransferError::Fenced("target_route_generation"))
        ));

        let wrong_pool = QuicTransferFence::new(
            GatewayPoolId::new("gateway-other").unwrap(),
            ticket.target.edge_cluster_id.clone(),
        );
        assert!(matches!(
            wrong_pool.validate(&ticket),
            Err(QuicTransferError::Fenced("target_gateway_pool_id"))
        ));
    }

    #[test]
    fn fence_tracks_route_reconnects_and_revokes_without_restarting_listener() {
        let ticket = ticket();
        let fence = QuicTransferFence::new(
            ticket.target.gateway_pool_id.clone(),
            ticket.target.edge_cluster_id.clone(),
        );
        assert!(matches!(
            fence.validate(&ticket),
            Err(QuicTransferError::Fenced("transfer_route_unavailable"))
        ));

        fence.set_generations(7, 8, 9);
        assert!(fence.validate(&ticket).is_ok());
        fence.set_route_generations(11, 13);
        assert!(matches!(
            fence.validate(&ticket),
            Err(QuicTransferError::Fenced("target_session_generation"))
        ));
        fence.clear_generations();
        assert!(matches!(
            fence.validate(&ticket),
            Err(QuicTransferError::Fenced("transfer_route_unavailable"))
        ));
    }

    #[test]
    fn fence_keeps_multiple_agent_routes_isolated() {
        let first = ticket();
        let mut second = ticket();
        second.target.agent_id = AgentId::new("agent-target-2").unwrap();
        second.session_generation = SessionGeneration::new(17);
        second.mount_generation = MountGeneration::new(18);
        second.route_generation = RouteGeneration::new(19);
        let fence = QuicTransferFence::new(
            first.target.gateway_pool_id.clone(),
            first.target.edge_cluster_id.clone(),
        );

        fence.set_agent_generations(&first.target.agent_id, 7, 8, 9);
        fence.set_agent_generations(&second.target.agent_id, 17, 18, 19);
        assert!(fence.validate(&first).is_ok());
        assert!(fence.validate(&second).is_ok());

        fence.set_agent_route_generations(&first.target.agent_id, 11, 13);
        assert!(matches!(
            fence.validate(&first),
            Err(QuicTransferError::Fenced("target_session_generation"))
        ));
        assert!(fence.validate(&second).is_ok());
        // A late teardown for the old generation must not revoke the replacement route.
        assert!(!fence.clear_agent_generations_if_current(&first.target.agent_id, 7, 9,));
        assert!(matches!(
            fence.validate(&first),
            Err(QuicTransferError::Fenced("target_session_generation"))
        ));
        assert!(fence.clear_agent_generations_if_current(&first.target.agent_id, 11, 13,));
        assert!(matches!(
            fence.validate(&first),
            Err(QuicTransferError::Fenced("transfer_route_unavailable"))
        ));
        assert!(fence.validate(&second).is_ok());
    }

    #[test]
    fn source_fence_uses_the_source_identity_and_generation_tuple() {
        let ticket = ticket();
        let source = QuicTransferFence::for_role(
            TransferRelayRole::Source,
            ticket.source.gateway_pool_id.clone(),
            ticket.source.edge_cluster_id.clone(),
        )
        .with_generations(4, 5, 6);
        source.validate(&ticket).unwrap();

        let stale = QuicTransferFence::for_role(
            TransferRelayRole::Source,
            ticket.source.gateway_pool_id.clone(),
            ticket.source.edge_cluster_id.clone(),
        )
        .with_generations(4, 5, 7);
        assert!(matches!(
            stale.validate(&ticket),
            Err(QuicTransferError::Fenced("source_route_generation"))
        ));
    }

    #[tokio::test]
    async fn configured_relays_carry_frames_through_both_gateways_without_storage() {
        let (server_tls, client_tls) = transfer_tls();
        let source_agent = source_endpoint(server_tls.clone());
        let source_agent_address = source_agent.local_addr().unwrap();

        let signed = signed_ticket();
        let source_fence = QuicTransferFence::for_role(
            TransferRelayRole::Source,
            signed.ticket.source.gateway_pool_id.clone(),
            signed.ticket.source.edge_cluster_id.clone(),
        )
        .with_generations(4, 5, 6);
        let source_connector = QuinnTransferConnectionFactory::bind(
            source_agent_address,
            Arc::<str>::from("localhost"),
            client_tls.clone(),
        )
        .unwrap();
        let source_gateway = QuicTransferListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            server_tls.clone(),
            source_fence,
        )
        .unwrap()
        .with_relay(Arc::new(ConnectedTransferRelay::new(Arc::new(
            source_connector,
        ))));
        let source_gateway_address = source_gateway.local_addr().unwrap();

        let target_fence = QuicTransferFence::for_role(
            TransferRelayRole::Target,
            signed.ticket.target.gateway_pool_id.clone(),
            signed.ticket.target.edge_cluster_id.clone(),
        )
        .with_generations(7, 8, 9);
        let target_connector = QuinnTransferConnectionFactory::bind(
            source_gateway_address,
            Arc::<str>::from("localhost"),
            client_tls.clone(),
        )
        .unwrap();
        let target_gateway =
            QuicTransferListener::bind("127.0.0.1:0".parse().unwrap(), server_tls, target_fence)
                .unwrap()
                .with_relay(Arc::new(ConnectedTransferRelay::new(Arc::new(
                    target_connector,
                ))));
        let target_gateway_address = target_gateway.local_addr().unwrap();

        let (source_shutdown, source_shutdown_receiver) = watch::channel(false);
        let source_gateway_task =
            tokio::spawn(serve(source_gateway, 4, source_shutdown_receiver.clone()));
        let target_gateway_task = tokio::spawn(serve(target_gateway, 4, source_shutdown_receiver));
        let expected_ticket = signed.clone();
        let source_agent_server = source_agent.clone();
        let (release_source, release_source_receiver) = tokio::sync::oneshot::channel();
        let source_agent_task = tokio::spawn(async move {
            let incoming = source_agent_server.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            assert_eq!(
                read_frame(&mut recv).await.unwrap(),
                TransferFrame::OpenTransferSigned(expected_ticket)
            );
            let expected_request = ObjectRequest {
                object_id: ObjectId::from_bytes([3; 32]),
                offset: 0,
                length: 1,
            };
            assert_eq!(
                read_frame(&mut recv).await.unwrap(),
                TransferFrame::ObjectRequest(expected_request.clone())
            );
            send_frame(
                &mut send,
                &TransferFrame::ObjectChunk(
                    ObjectChunk::new(expected_request.object_id, 0, vec![42]).unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                read_frame(&mut recv).await.unwrap(),
                TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                    object_id: expected_request.object_id,
                    offset: 0,
                    length: 1,
                    accepted: true,
                })
            );
            assert_eq!(
                read_frame(&mut recv).await.unwrap(),
                TransferFrame::CloseTransfer(CloseTransfer { committed: true })
            );
            release_source_receiver.await.unwrap();
            drop(send);
            drop(connection);
        });

        let target_agent = QuinnTransferConnectionFactory::bind(
            target_gateway_address,
            Arc::<str>::from("localhost"),
            client_tls,
        )
        .unwrap();
        let connection = target_agent.connect(&signed.ticket).await.unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send_frame(
            &mut send,
            &TransferFrame::OpenTransferSigned(signed.clone()),
        )
        .await
        .unwrap();
        send_frame(
            &mut send,
            &TransferFrame::ObjectRequest(ObjectRequest {
                object_id: ObjectId::from_bytes([3; 32]),
                offset: 0,
                length: 1,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            read_frame(&mut recv).await.unwrap(),
            TransferFrame::ObjectChunk(
                ObjectChunk::new(ObjectId::from_bytes([3; 32]), 0, vec![42]).unwrap()
            )
        );
        send_frame(
            &mut send,
            &TransferFrame::ObjectAck(neoengram_domain::protocol::ObjectAck {
                object_id: ObjectId::from_bytes([3; 32]),
                offset: 0,
                length: 1,
                accepted: true,
            }),
        )
        .await
        .unwrap();
        send_frame(
            &mut send,
            &TransferFrame::CloseTransfer(CloseTransfer { committed: true }),
        )
        .await
        .unwrap();

        release_source.send(()).unwrap();
        source_agent_task.await.unwrap();
        source_shutdown.send(true).unwrap();
        source_gateway_task.await.unwrap().unwrap();
        target_gateway_task.await.unwrap().unwrap();
    }
}
