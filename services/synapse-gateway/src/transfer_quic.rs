//! QUIC data-plane boundary for object replication.
//!
//! Gateway owns the QUIC hop and relay policy, but never owns object bytes or a Volume mount.
//! The first frame on every stream is the binary `OpenTransfer` frame; the ticket is validated
//! before any object request is accepted.  The actual object source/sink remains an Agent
//! concern, so this module can be wired to either an in-process same-Gateway relay or a peer
//! Gateway connection without changing the domain protocol.

use std::{fmt, io, net::SocketAddr, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use neoengram_domain::protocol::{
    EdgeClusterId, GatewayPoolId, TransferFrame, TransferFrameError, TransferTicket,
    MAX_TRANSFER_FRAME_BYTES, TRANSFER_ALPN,
};
use quinn::{Connection, Endpoint, Incoming, RecvStream, SendStream};
use tokio::{
    sync::{watch, Semaphore},
    task::JoinSet,
};
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicTransferError {
    #[error("QUIC connection failed: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("QUIC stream failed: {0}")]
    Stream(#[from] quinn::ReadToEndError),
    #[error("QUIC frame read failed: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("QUIC stream write failed: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("invalid transfer frame: {0}")]
    Frame(#[from] TransferFrameError),
    #[error("transfer ticket is expired")]
    Expired,
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

/// Generation and identity fences applied before any object frame is accepted.  A listener has
/// no storage handle; the caller can update this value when Central replaces a route/session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuicTransferFence {
    gateway_pool_id: GatewayPoolId,
    edge_cluster_id: EdgeClusterId,
    session_generation: Option<u64>,
    mount_generation: Option<u64>,
    route_generation: Option<u64>,
}

impl QuicTransferFence {
    #[must_use]
    pub(crate) fn new(gateway_pool_id: GatewayPoolId, edge_cluster_id: EdgeClusterId) -> Self {
        Self {
            gateway_pool_id,
            edge_cluster_id,
            session_generation: None,
            mount_generation: None,
            route_generation: None,
        }
    }

    /// Installs the current generations for a route.  `None` means that the listener only checks
    /// the endpoint identity; callers that have an Agent route should always provide all three.
    #[must_use]
    pub(crate) fn with_generations(
        mut self,
        session_generation: u64,
        mount_generation: u64,
        route_generation: u64,
    ) -> Self {
        self.session_generation = Some(session_generation);
        self.mount_generation = Some(mount_generation);
        self.route_generation = Some(route_generation);
        self
    }

    fn validate(&self, ticket: &TransferTicket) -> Result<(), QuicTransferError> {
        if ticket.target.gateway_pool_id != self.gateway_pool_id {
            return Err(QuicTransferError::Fenced("target_gateway_pool_id"));
        }
        if ticket.target.edge_cluster_id != self.edge_cluster_id {
            return Err(QuicTransferError::Fenced("target_edge_cluster_id"));
        }
        if self
            .session_generation
            .is_some_and(|generation| generation != ticket.session_generation.get())
        {
            return Err(QuicTransferError::Fenced("session_generation"));
        }
        if self
            .mount_generation
            .is_some_and(|generation| generation != ticket.mount_generation.get())
        {
            return Err(QuicTransferError::Fenced("mount_generation"));
        }
        if self
            .route_generation
            .is_some_and(|generation| generation != ticket.route_generation.get())
        {
            return Err(QuicTransferError::Fenced("route_generation"));
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
        ticket: TransferTicket,
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
        ticket: TransferTicket,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<(), QuicTransferError> {
        let connection = self.connector.connect(&ticket).await?;
        let (mut peer_send, mut peer_recv) = connection.open_bi().await?;
        send_frame(&mut peer_send, &TransferFrame::OpenTransfer(ticket)).await?;
        relay_frames(&mut recv, &mut send, &mut peer_recv, &mut peer_send).await
    }
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
        fence.validate(&ticket)?;
    }
    Ok(ticket)
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
        let close = matches!(frame, TransferFrame::CloseTransfer(_));
        send_frame(send, &frame).await?;
        if close {
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
    let ticket = open_transfer_with_fence(&connection, &mut recv, fence.as_ref()).await?;
    if let Some(relay) = relay {
        relay.relay(ticket, send, recv).await?;
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
        AgentId, ContentDigest, DecimalU64, MountGeneration, PlacementId, RouteGeneration,
        SessionGeneration, TenantId, TransferEndpoint, TransferId, UnixMillis,
    };
    use neoengram_domain::{CommitId, ObjectId};

    fn ticket() -> TransferTicket {
        let target_pool = GatewayPoolId::new("gateway-target").unwrap();
        let target_cluster = EdgeClusterId::new("cluster-target").unwrap();
        TransferTicket {
            transfer_id: TransferId::new("transfer-test").unwrap(),
            tenant_id: TenantId::new("tenant-test").unwrap(),
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
            session_generation: SessionGeneration::new(7),
            mount_generation: MountGeneration::new(8),
            route_generation: RouteGeneration::new(9),
            deadline_unix_ms: UnixMillis::new(unix_millis_now() + 60_000),
            max_bytes: DecimalU64::new(4096),
            allowed_objects: vec![ObjectId::from_bytes([3; 32])],
        }
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
            Err(QuicTransferError::Fenced("route_generation"))
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
}
