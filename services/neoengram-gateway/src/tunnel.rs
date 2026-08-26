use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt::Write as _,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Version,
};
use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Body, Frame, SizeHint};
use neoengram_domain::protocol::{
    decode_bounded_unique_json, AgentChannelDownstreamFrame, AgentChannelDownstreamMessage,
    AgentChannelNdjsonDecoder, AgentChannelUpstreamFrame, AgentChannelUpstreamMessage,
    ContentDigest, GatewayAgentAction, GatewayAgentRequest, GatewayAgentResponse,
    GatewayAgentStreamData, GatewayAgentStreamEnd, GatewayAgentStreamOpen, GatewayBackpressure,
    GatewayConnectionId, GatewayControlError, GatewayControlFrame, GatewayControlMessage,
    GatewayControlNdjsonDecoder, GatewayDrain, GatewayErrorCode, GatewayOpaqueBytes,
    GatewayPeerDirectory, GatewayPeerForwardAccepted, GatewayPeerForwardRequest, GatewayPoolId,
    GatewayReplicaHeartbeat, GatewayReplicaHello, GatewayReplicaId, GatewayRouteLeaseGranted,
    GatewayRouteLeaseRequest, GatewayS3ReadRevocation, RequestId, RouteGeneration, SequenceNumber,
    SessionGeneration, TraceId, UnixMillis, AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS,
    AGENT_ROUTE_LEASE_TTL_MS, CURRENT_WIRE_VERSION, MAX_CONTROL_MESSAGE_BYTES,
    MAX_GATEWAY_STREAM_CHUNK_BYTES, MAX_METADATA_PAGE_BYTES,
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit},
    task::JoinSet,
    time::{interval, timeout, MissedTickBehavior},
};

use crate::transfer_quic::QuicTransferFence;

pub(crate) const JSON_CONTENT_TYPE: &str = "application/json";
pub(crate) const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";
pub(crate) const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";
pub(crate) const REQUEST_ID_HEADER: &str = "x-request-id";

const CONTROL_OUTPUT_BUFFER: usize = 256;
const DRAIN_ROUTE_RELEASE_BUDGET: Duration = Duration::from_secs(2);
const DRAIN_ROUTE_RELEASE_CONCURRENCY: usize = 16;
const STREAM_EVENT_BUFFER: usize = 64;
const STREAM_RESPONSE_BUFFER: usize = 64;
const CONTROL_FRAME_DEADLINE_MS: u64 = 10_000;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_PEER_FORWARD_REPLAYS: usize = 1024;
// A bounded tombstone set lets the control reader discard a response that raced a local timeout
// without turning that expected late frame into a fatal protocol error.
const MAX_LATE_CONTROL_RESPONSES: usize = 1024;
// Stream IDs are cryptographically random, but the admission-gate directory must still have a
// finite bound for a long-lived Gateway. Closed entries are eligible for eviction; request-ID
// binding checks below keep a stale worker harmless if it arrives after an eviction.
const MAX_STREAM_ADMISSION_GATES: usize = 4096;
const S3_READ_REVOCATION_BUFFER: usize = 256;

#[derive(Debug, Clone)]
pub(crate) struct GatewayIdentity {
    pub edge_cluster_id: neoengram_domain::protocol::EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub software_version: String,
}

pub(crate) struct StreamingBody {
    frames: mpsc::Receiver<Bytes>,
    _request_permit: Option<OwnedSemaphorePermit>,
}

impl StreamingBody {
    pub(crate) fn new(frames: mpsc::Receiver<Bytes>) -> Self {
        Self {
            frames,
            _request_permit: None,
        }
    }

    pub(crate) fn retain_request_permit(&mut self, permit: OwnedSemaphorePermit) {
        debug_assert!(self._request_permit.is_none());
        self._request_permit = Some(permit);
    }
}

impl Body for StreamingBody {
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

pub(crate) type GatewayBody = Either<Full<Bytes>, StreamingBody>;

#[derive(Clone)]
pub(crate) struct GatewayTunnel {
    identity: GatewayIdentity,
    state: Arc<TunnelState>,
    peer_forwarder: Arc<dyn PeerForwarder>,
}

impl std::fmt::Debug for GatewayTunnel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayTunnel")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

struct TunnelState {
    /// Serializes control-session replacement with teardown.  A reader from an old Central
    /// session must finish clearing its correlated state before a replacement session can be
    /// installed; otherwise the old reader could wipe maps belonging to the new session.
    lifecycle: Mutex<()>,
    link: Mutex<Option<CentralLink>>,
    /// Local shutdown fence. It is set before the Drain frame is queued so concurrent route
    /// mutations cannot extend a lease after the shutdown boundary.
    draining: AtomicBool,
    pending_unary: Mutex<BTreeMap<GatewayConnectionId, oneshot::Sender<GatewayAgentResponse>>>,
    late_unary: Mutex<BTreeSet<GatewayConnectionId>>,
    pending_routes: Mutex<
        BTreeMap<RequestId, oneshot::Sender<Result<GatewayRouteLeaseGranted, GatewayControlError>>>,
    >,
    late_routes: Mutex<BTreeSet<RequestId>>,
    streams: Mutex<BTreeMap<GatewayConnectionId, mpsc::Sender<StreamEvent>>>,
    closed_streams: Mutex<BTreeSet<GatewayConnectionId>>,
    /// Admission gates serialize Agent input frame enqueue with stream fencing. Inactive entries
    /// are retained within a bounded directory; request bindings fence stale workers after an
    /// entry is evicted or a transport ID is reused.
    stream_admission_gates: Mutex<BTreeMap<GatewayConnectionId, Arc<Mutex<()>>>>,
    /// A per-stream cancellation signal wakes an Agent input worker whose body is still pending
    /// after the Central/output side has fenced the stream.
    stream_cancellations: Mutex<BTreeMap<GatewayConnectionId, watch::Sender<bool>>>,
    stream_requests: Mutex<BTreeMap<RequestId, GatewayConnectionId>>,
    routes: Mutex<BTreeMap<GatewayConnectionId, ActiveRoute>>,
    /// Shared with the QUIC transfer listener. It is updated only from Central-granted Agent
    /// routes, so a reconnect cannot leave the data plane fenced to an old session generation.
    transfer_fence: Option<QuicTransferFence>,
    peer_forward_seen: Mutex<BTreeMap<RequestId, PeerForwardReplay>>,
    /// Central's current allow-list for peer TLS credentials. This is scoped to the active control
    /// connection and is cleared atomically when that connection is fenced or disconnected.
    peer_directory: Mutex<Option<GatewayPeerDirectory>>,
    /// Monotonic S3 authorization fences are fanned out to the dedicated binary read registry.
    /// A broadcast channel keeps the control transport independent from that data-plane module.
    s3_read_revocations: broadcast::Sender<GatewayS3ReadRevocation>,
}

struct CentralLink {
    connection_id: GatewayConnectionId,
    next_sequence: u64,
    /// The link is installed before the hello can be queued so duplicate Central requests are
    /// rejected during handshake.  Until this flips, no application frame may consume sequence 1.
    hello_sent: bool,
    output: mpsc::Sender<Bytes>,
}

#[derive(Debug)]
enum StreamEvent {
    Data(Bytes),
    End,
    Error(GatewayControlError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveRoute {
    agent_id: neoengram_domain::protocol::AgentId,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
    lease_expires_at_unix_ms: UnixMillis,
}

/// The immutable identity of a peer-forward request.  The payload digest is part of the
/// admission key so an attacker cannot reuse a request ID to deliver a different Agent frame.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerForwardReplayBinding {
    source_replica_id: GatewayReplicaId,
    target_replica_id: GatewayReplicaId,
    target_peer_endpoint: String,
    agent_id: neoengram_domain::protocol::AgentId,
    agent_connection_id: GatewayConnectionId,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
    frame_digest: ContentDigest,
}

impl PeerForwardReplayBinding {
    fn from_request(request: &GatewayPeerForwardRequest) -> Self {
        Self {
            source_replica_id: request.source_replica_id.clone(),
            target_replica_id: request.target_replica_id.clone(),
            target_peer_endpoint: request.target_peer_endpoint.clone(),
            agent_id: request.agent_id.clone(),
            agent_connection_id: request.agent_connection_id.clone(),
            session_generation: request.session_generation,
            route_generation: request.route_generation,
            frame_digest: ContentDigest::hash(request.frame.as_bytes()),
        }
    }
}

/// A request ID is reserved before the potentially blocking Agent delivery starts.  The watch
/// channel lets concurrent exact replays await and reuse the original result without delivering
/// the frame a second time.  Failed deliveries are removed after notifying waiters, so callers may
/// retry with the same ID once the transient route failure is gone.
struct PeerForwardReplay {
    binding: PeerForwardReplayBinding,
    /// Keep a successful request ID reserved for the full authenticated frame lifetime.  An
    /// earlier capacity-based eviction could let an old request ID be replayed after 1024 other
    /// forwards, which defeats the one-delivery guarantee while the original frame is still
    /// within its valid deadline window.
    expires_at_unix_ms: UnixMillis,
    reservation: Arc<()>,
    completion: watch::Receiver<Option<Result<GatewayPeerForwardAccepted, GatewayControlError>>>,
}

enum PeerForwardAdmission {
    Execute {
        reservation: Arc<()>,
        completion: watch::Sender<Option<Result<GatewayPeerForwardAccepted, GatewayControlError>>>,
    },
    Replay(watch::Receiver<Option<Result<GatewayPeerForwardAccepted, GatewayControlError>>>),
}

fn peer_forward_replay_complete(entry: &PeerForwardReplay) -> bool {
    entry.completion.borrow().is_some() || entry.completion.has_changed().is_err()
}

#[async_trait]
pub(crate) trait PeerForwarder: Send + Sync {
    async fn forward(
        &self,
        target_peer_endpoint: &str,
        frame: GatewayControlFrame,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError>;
}

#[derive(Debug)]
#[cfg(test)]
struct UnavailablePeerForwarder;

#[async_trait]
#[cfg(test)]
impl PeerForwarder for UnavailablePeerForwarder {
    async fn forward(
        &self,
        _target_peer_endpoint: &str,
        _frame: GatewayControlFrame,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        Err(route_unavailable(
            "owner Replica peer transport is unavailable",
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TunnelError {
    #[error("Gateway has no active Central control session")]
    Unavailable,
    #[error("Gateway control session is already active")]
    AlreadyConnected,
    #[error("Gateway request is invalid: {0}")]
    Invalid(&'static str),
    #[error("Gateway protocol rejected the request: {0}")]
    Protocol(#[from] neoengram_domain::protocol::ProtocolError),
    #[error("Gateway request exceeded its deadline")]
    Deadline,
    #[error("Gateway route was fenced")]
    Fenced,
    #[error("Gateway workload identity does not match the request")]
    Identity,
    #[error("Gateway request body failed")]
    Body,
    #[error("Gateway internal channel closed")]
    Closed,
}

impl TunnelError {
    fn status(&self) -> StatusCode {
        match self {
            Self::Unavailable | Self::Closed => StatusCode::SERVICE_UNAVAILABLE,
            Self::AlreadyConnected => StatusCode::CONFLICT,
            Self::Invalid(_) | Self::Protocol(_) | Self::Body => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Deadline => StatusCode::GATEWAY_TIMEOUT,
            Self::Fenced => StatusCode::CONFLICT,
            Self::Identity => StatusCode::FORBIDDEN,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Unavailable | Self::Closed => "ROUTE_UNAVAILABLE",
            Self::AlreadyConnected => "CONTROL_SESSION_EXISTS",
            Self::Invalid(_) | Self::Protocol(_) | Self::Body => "PROTOCOL_INVALID",
            Self::Deadline => "DEADLINE_EXCEEDED",
            Self::Fenced => "ROUTE_FENCED",
            Self::Identity => "IDENTITY_REJECTED",
        }
    }

    fn retryable(&self) -> bool {
        matches!(self, Self::Unavailable | Self::Closed)
    }
}

impl GatewayTunnel {
    #[cfg(test)]
    pub(crate) fn new(identity: GatewayIdentity) -> Self {
        Self::with_peer_forwarder(identity, Arc::new(UnavailablePeerForwarder))
    }

    #[cfg(test)]
    pub(crate) fn with_peer_forwarder(
        identity: GatewayIdentity,
        peer_forwarder: Arc<dyn PeerForwarder>,
    ) -> Self {
        Self::with_peer_forwarder_and_transfer_fence(identity, peer_forwarder, None)
    }

    pub(crate) fn with_peer_forwarder_and_transfer_fence(
        identity: GatewayIdentity,
        peer_forwarder: Arc<dyn PeerForwarder>,
        transfer_fence: Option<QuicTransferFence>,
    ) -> Self {
        let (s3_read_revocations, _) = broadcast::channel(S3_READ_REVOCATION_BUFFER);
        Self {
            identity,
            state: Arc::new(TunnelState {
                lifecycle: Mutex::new(()),
                link: Mutex::new(None),
                draining: AtomicBool::new(false),
                pending_unary: Mutex::new(BTreeMap::new()),
                late_unary: Mutex::new(BTreeSet::new()),
                pending_routes: Mutex::new(BTreeMap::new()),
                late_routes: Mutex::new(BTreeSet::new()),
                streams: Mutex::new(BTreeMap::new()),
                closed_streams: Mutex::new(BTreeSet::new()),
                stream_admission_gates: Mutex::new(BTreeMap::new()),
                stream_cancellations: Mutex::new(BTreeMap::new()),
                stream_requests: Mutex::new(BTreeMap::new()),
                routes: Mutex::new(BTreeMap::new()),
                transfer_fence,
                peer_forward_seen: Mutex::new(BTreeMap::new()),
                peer_directory: Mutex::new(None),
                s3_read_revocations,
            }),
            peer_forwarder,
        }
    }

    pub(crate) fn subscribe_s3_read_revocations(
        &self,
    ) -> broadcast::Receiver<GatewayS3ReadRevocation> {
        self.state.s3_read_revocations.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn publish_test_s3_read_revocation(&self, revocation: GatewayS3ReadRevocation) {
        let _ = self.state.s3_read_revocations.send(revocation);
    }

    pub(crate) async fn is_ready(&self) -> bool {
        !self.is_draining()
            && self
                .state
                .link
                .lock()
                .await
                .as_ref()
                .is_some_and(|link| link.hello_sent)
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.state.draining.load(Ordering::Acquire)
    }

    /// Applies one Central-issued peer credential directory to the active control session.
    /// Directly replaying an older directory would re-authorize a revoked certificate, so
    /// generations are strictly monotonic for the lifetime of a control connection.
    async fn install_peer_directory(
        &self,
        directory: GatewayPeerDirectory,
    ) -> Result<(), TunnelError> {
        directory
            .validate_at(now_unix_ms())
            .map_err(TunnelError::Protocol)?;
        let mut cached = self.state.peer_directory.lock().await;
        if cached
            .as_ref()
            .is_some_and(|previous| directory.directory_generation <= previous.directory_generation)
        {
            return Err(TunnelError::Invalid(
                "Central peer directory generation did not advance",
            ));
        }
        *cached = Some(directory);
        Ok(())
    }

    /// Checks the TLS leaf fingerprint against the latest non-expired Central directory. The
    /// caller has already authenticated the URI SAN; this second binding is what makes Registry
    /// certificate revocation and rotation effective for an otherwise long-lived mTLS peer.
    pub(crate) async fn authorize_peer_source(
        &self,
        source_replica_id: &GatewayReplicaId,
        certificate_fingerprint: &ContentDigest,
    ) -> Result<(), GatewayControlError> {
        let now = now_unix_ms();
        let cached = self.state.peer_directory.lock().await;
        let directory = cached.as_ref().ok_or_else(|| {
            identity_rejected("Central has not supplied a current peer credential directory")
        })?;
        if directory.validate_at(now).is_err() {
            return Err(identity_rejected(
                "Central peer credential directory is expired",
            ));
        }
        let entry = directory
            .replicas
            .iter()
            .find(|entry| entry.gateway_replica_id == *source_replica_id)
            .ok_or_else(|| {
                identity_rejected("source Replica is absent from the Central peer directory")
            })?;
        if &entry.certificate_fingerprint != certificate_fingerprint {
            return Err(identity_rejected(
                "source Replica certificate fingerprint is not current",
            ));
        }
        Ok(())
    }

    /// Announces a local shutdown to Central after fencing new control and route work locally.
    /// Central persists the Replica `Draining` transition and closes the control stream when it
    /// accepts this frame. If Central is already unavailable, the local fence still holds and this
    /// send remains bounded so Kubernetes can proceed to SIGTERM.
    pub(crate) async fn begin_drain(
        self: &Arc<Self>,
        deadline_unix_ms: UnixMillis,
        reason: impl Into<String>,
    ) -> Result<(), TunnelError> {
        if self.state.draining.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let active_routes = self
            .state
            .routes
            .lock()
            .await
            .iter()
            .map(|(connection_id, route)| (connection_id.clone(), route.clone()))
            .collect::<Vec<_>>();
        self.release_active_routes(active_routes).await;
        let message = GatewayControlMessage::Drain(GatewayDrain {
            deadline_unix_ms,
            reason: reason.into(),
        });
        let result = match timeout(
            Duration::from_millis(CONTROL_FRAME_DEADLINE_MS),
            self.send_control(fresh_request_id("drain")?, None, message),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(TunnelError::Deadline),
        };
        if result.is_err() {
            // A closed output queue can otherwise retain a stale link until SIGTERM. Clear it now
            // so stream waiters fail closed immediately.
            let connection_id = self
                .state
                .link
                .lock()
                .await
                .as_ref()
                .map(|link| link.connection_id.clone());
            if let Some(connection_id) = connection_id {
                self.disconnect(&connection_id).await;
            }
        }
        result
    }

    /// Releases the routes known by this Replica before announcing Drain. Releases are best
    /// effort and concurrent, with a small global budget; Central's Drain frame remains the
    /// authoritative fence when a route response races with shutdown.
    async fn release_active_routes(
        self: &Arc<Self>,
        routes: Vec<(GatewayConnectionId, ActiveRoute)>,
    ) {
        if routes.is_empty() {
            return;
        }
        let mut routes = routes.into_iter();
        let mut tasks = JoinSet::new();
        let deadline = Instant::now() + DRAIN_ROUTE_RELEASE_BUDGET;
        loop {
            while tasks.len() < DRAIN_ROUTE_RELEASE_CONCURRENCY {
                let Some((connection_id, route)) = routes.next() else {
                    break;
                };
                let tunnel = Arc::clone(self);
                let owner_replica_id = tunnel.identity.gateway_replica_id.clone();
                tasks.spawn(async move {
                    tunnel
                        .mutate_route(GatewayControlMessage::RouteRelease(
                            GatewayRouteLeaseRequest {
                                agent_id: route.agent_id,
                                owner_replica_id,
                                agent_connection_id: connection_id,
                                session_generation: route.session_generation,
                                route_generation: Some(route.route_generation),
                                requested_expires_at_unix_ms: route.lease_expires_at_unix_ms,
                            },
                        ))
                        .await
                });
            }
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                joined = tasks.join_next() => {
                    match joined {
                        Some(Ok(Ok(_))) => {}
                        Some(Ok(Err(error))) => {
                            tracing::warn!(%error, "Gateway route release during drain failed")
                        }
                        Some(Err(error)) => {
                            tracing::warn!(%error, "Gateway route release task panicked during drain")
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline.into()) => {
                    tracing::warn!(remaining = tasks.len() + routes.len(), "Gateway route release budget expired during drain");
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }

    pub(crate) fn identity(&self) -> &GatewayIdentity {
        &self.identity
    }

    /// Captures the exact Central-authoritative route fence for a new Agent S3 channel.
    pub(crate) async fn current_agent_route_fence(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
        session_generation: SessionGeneration,
    ) -> Option<(GatewayConnectionId, RouteGeneration)> {
        if self.is_draining() {
            return None;
        }
        let now = now_unix_ms();
        self.state
            .routes
            .lock()
            .await
            .iter()
            .find_map(|(connection_id, route)| {
                (&route.agent_id == agent_id
                    && route.session_generation == session_generation
                    && route.lease_expires_at_unix_ms.get() > now.get())
                .then(|| (connection_id.clone(), route.route_generation))
            })
    }

    /// Revalidates the full route fence captured when an Agent S3 channel was established.
    pub(crate) async fn agent_route_fence_is_current(
        &self,
        agent_id: &neoengram_domain::protocol::AgentId,
        session_generation: SessionGeneration,
        connection_id: &GatewayConnectionId,
        route_generation: RouteGeneration,
    ) -> bool {
        if self.is_draining() {
            return false;
        }
        let now = now_unix_ms();
        self.state
            .routes
            .lock()
            .await
            .get(connection_id)
            .is_some_and(|route| {
                &route.agent_id == agent_id
                    && route.session_generation == session_generation
                    && route.route_generation == route_generation
                    && route.lease_expires_at_unix_ms.get() > now.get()
            })
    }

    /// Matches every Central-authoritative route field embedded in an S3 read ticket. Unlike the
    /// Agent channel liveness check above, peer reads must bind the exact connection and route
    /// generation so a ticket issued before takeover cannot reach a newer session.
    pub(crate) async fn s3_read_route_is_current(
        &self,
        ticket: &neoengram_domain::protocol::S3ReadTicket,
    ) -> bool {
        if self.is_draining()
            || ticket.owner_replica_id != self.identity.gateway_replica_id
            || ticket.gateway_pool_id != self.identity.gateway_pool_id.as_str()
        {
            return false;
        }
        let now = now_unix_ms();
        self.state
            .routes
            .lock()
            .await
            .get(&ticket.agent_connection_id)
            .is_some_and(|route| {
                route.agent_id == ticket.agent_id
                    && route.session_generation == ticket.session_generation
                    && route.route_generation == ticket.route_generation
                    && route.lease_expires_at_unix_ms.get() > now.get()
            })
    }

    #[cfg(test)]
    pub(crate) async fn install_test_agent_route(
        &self,
        agent_id: neoengram_domain::protocol::AgentId,
        session_generation: SessionGeneration,
    ) -> GatewayConnectionId {
        let connection_id = GatewayConnectionId::new(format!(
            "s3-test-route-{}-{}",
            agent_id.as_str(),
            session_generation.get()
        ))
        .expect("test route ID must be valid");
        self.state.routes.lock().await.insert(
            connection_id.clone(),
            ActiveRoute {
                agent_id,
                session_generation,
                route_generation: RouteGeneration::new(1),
                lease_expires_at_unix_ms: UnixMillis::new(
                    now_unix_ms().get().saturating_add(60_000),
                ),
            },
        );
        connection_id
    }

    #[cfg(test)]
    pub(crate) async fn remove_test_agent_route(&self, connection_id: &GatewayConnectionId) {
        self.state.routes.lock().await.remove(connection_id);
    }

    #[cfg(test)]
    pub(crate) async fn replace_test_agent_route(
        &self,
        agent_id: neoengram_domain::protocol::AgentId,
        session_generation: SessionGeneration,
    ) -> GatewayConnectionId {
        let connection_id = GatewayConnectionId::new(format!(
            "s3-test-replacement-{}-{}",
            agent_id.as_str(),
            session_generation.get()
        ))
        .expect("test replacement route ID must be valid");
        let mut routes = self.state.routes.lock().await;
        routes.retain(|_, route| route.agent_id != agent_id);
        routes.insert(
            connection_id.clone(),
            ActiveRoute {
                agent_id,
                session_generation,
                route_generation: RouteGeneration::new(2),
                lease_expires_at_unix_ms: UnixMillis::new(
                    now_unix_ms().get().saturating_add(60_000),
                ),
            },
        );
        connection_id
    }

    pub(crate) async fn open_control<B>(
        self: &Arc<Self>,
        request: Request<B>,
    ) -> Result<Response<GatewayBody>, TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        if self.is_draining() {
            return Err(TunnelError::Unavailable);
        }
        if request.method() != Method::POST
            || request.version() != Version::HTTP_2
            || !has_content_type(request.headers(), NDJSON_CONTENT_TYPE)
        {
            return Err(TunnelError::Invalid(
                "control channel requires an HTTP/2 NDJSON POST",
            ));
        }
        let connection_id = fresh_connection_id("central")?;
        let (output, response_body) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        // Keep replacement and teardown mutually exclusive while publishing the link;
        // `disconnect` acquires the same guard before it starts clearing correlated stream state.
        // Release it before sending hello because the send failure path calls `disconnect`.
        {
            let _lifecycle_guard = self.state.lifecycle.lock().await;
            let mut link = self.state.link.lock().await;
            if self.is_draining() {
                return Err(TunnelError::Unavailable);
            }
            if link.is_some() {
                return Err(TunnelError::AlreadyConnected);
            }
            *link = Some(CentralLink {
                connection_id: connection_id.clone(),
                next_sequence: 1,
                hello_sent: false,
                output,
            });
        }

        if let Err(error) = self
            .send_control(
                fresh_request_id("hello")?,
                None,
                GatewayControlMessage::ReplicaHello(GatewayReplicaHello {
                    edge_cluster_id: self.identity.edge_cluster_id.clone(),
                    software_version: self.identity.software_version.clone(),
                    wire_version: CURRENT_WIRE_VERSION,
                    capabilities: neoengram_domain::protocol::gateway_capabilities_v1(),
                }),
            )
            .await
        {
            self.disconnect(&connection_id).await;
            return Err(error);
        }

        let tunnel = Arc::clone(self);
        let reader_connection_id = connection_id.clone();
        let body = request.into_body();
        tokio::spawn(async move {
            if let Err(error) = tunnel
                .read_control(reader_connection_id.clone(), body)
                .await
            {
                tracing::warn!(%error, "Central control stream closed");
            }
            tunnel.disconnect(&reader_connection_id).await;
        });
        let tunnel = Arc::clone(self);
        let heartbeat_connection_id = connection_id.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if tunnel.is_draining() {
                    return;
                }
                if !tunnel.is_connection(&heartbeat_connection_id).await {
                    return;
                }
                let connected_agents = tunnel.state.routes.lock().await.len();
                let active_streams = tunnel.state.streams.lock().await.len();
                let message = GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                    connected_agents: u32::try_from(connected_agents).unwrap_or(u32::MAX),
                    active_streams: u32::try_from(active_streams).unwrap_or(u32::MAX),
                    queue_depth: 0,
                });
                let Ok(request_id) = fresh_request_id("heartbeat") else {
                    return;
                };
                if tunnel
                    .send_control(request_id, None, message)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Response::builder()
            .status(StatusCode::OK)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .header(ACCEPT, NDJSON_CONTENT_TYPE)
            .body(Either::Right(StreamingBody::new(response_body)))
            .map_err(|_| TunnelError::Invalid("control response metadata is invalid"))
    }

    pub(crate) async fn forward_agent<B>(
        self: &Arc<Self>,
        request: Request<B>,
        peer_agent_id: Option<neoengram_domain::protocol::AgentId>,
        max_request_bytes: usize,
        request_deadline: Duration,
    ) -> Result<Response<GatewayBody>, TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        if self.is_draining() {
            return Err(TunnelError::Unavailable);
        }
        if request.method() != Method::POST {
            return Err(TunnelError::Invalid("Agent actions require POST"));
        }
        let action = GatewayAgentAction::from_path(request.uri().path())
            .ok_or(TunnelError::Invalid("unknown Agent action path"))?;
        if action == GatewayAgentAction::SessionChannelOpen {
            self.forward_agent_stream(request, peer_agent_id, request_deadline)
                .await
        } else {
            self.forward_agent_unary(
                request,
                action,
                peer_agent_id,
                max_request_bytes,
                request_deadline,
            )
            .await
        }
    }

    async fn forward_agent_unary<B>(
        &self,
        request: Request<B>,
        action: GatewayAgentAction,
        peer_agent_id: Option<neoengram_domain::protocol::AgentId>,
        configured_max_bytes: usize,
        request_deadline: Duration,
    ) -> Result<Response<GatewayBody>, TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        if !has_content_type(request.headers(), JSON_CONTENT_TYPE) {
            return Err(TunnelError::Invalid(
                "Agent unary actions require application/json",
            ));
        }
        // Enrollment bootstrap/status intentionally remain compatible with the small JSON
        // client used before an Agent has a workload certificate. Every authenticated action,
        // however, belongs to the H2 Gateway control protocol just like the streaming channel.
        if !matches!(
            action,
            GatewayAgentAction::EnrollmentBootstrap | GatewayAgentAction::EnrollmentStatusQuery
        ) && request.version() != Version::HTTP_2
        {
            return Err(TunnelError::Invalid(
                "authenticated Agent unary actions require HTTP/2",
            ));
        }
        let request_id = request_id(request.headers())?;
        let request_trace_id = trace_id(request.headers());
        let operation_limit = agent_action_limit(action).min(configured_max_bytes);
        if content_length_exceeds(request.headers(), operation_limit) {
            return Err(TunnelError::Invalid("Agent request body exceeds its limit"));
        }
        let body = timeout(
            request_deadline,
            collect_bounded(request.into_body(), operation_limit),
        )
        .await
        .map_err(|_| TunnelError::Deadline)??;
        if !matches!(
            action,
            GatewayAgentAction::EnrollmentBootstrap | GatewayAgentAction::EnrollmentStatusQuery
        ) {
            if let Some(expected_agent_id) = peer_agent_id {
                if request_agent_id(&body)? != expected_agent_id {
                    return Err(TunnelError::Identity);
                }
            }
        }
        let stream_id = fresh_connection_id("request")?;
        let (sender, receiver) = oneshot::channel();
        self.state
            .pending_unary
            .lock()
            .await
            .insert(stream_id.clone(), sender);
        let send_result = self
            .send_control(
                request_id.clone(),
                request_trace_id,
                GatewayControlMessage::AgentRequest(GatewayAgentRequest {
                    action,
                    stream_id: stream_id.clone(),
                    body: GatewayOpaqueBytes::new(body)?,
                }),
            )
            .await;
        if let Err(error) = send_result {
            self.expire_unary(&stream_id).await;
            return Err(error);
        }
        let response = match timeout(request_deadline, receiver).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.expire_unary(&stream_id).await;
                return Err(TunnelError::Closed);
            }
            Err(_) => {
                self.expire_unary(&stream_id).await;
                return Err(TunnelError::Deadline);
            }
        };
        let status = StatusCode::from_u16(response.status)
            .map_err(|_| TunnelError::Invalid("Central returned an invalid HTTP status"))?;
        let mut builder = Response::builder()
            .status(status)
            .header(CONTENT_TYPE, response.content_type)
            .header(REQUEST_ID_HEADER, request_id.as_str());
        if let Some(retry_after_ms) = response.retry_after_ms {
            let seconds = retry_after_ms.saturating_add(999) / 1_000;
            builder = builder.header(RETRY_AFTER, seconds.to_string());
        }
        builder
            .body(Either::Left(Full::new(Bytes::from(
                response.body.into_bytes(),
            ))))
            .map_err(|_| TunnelError::Invalid("Agent response metadata is invalid"))
    }

    async fn forward_agent_stream<B>(
        self: &Arc<Self>,
        request: Request<B>,
        peer_agent_id: Option<neoengram_domain::protocol::AgentId>,
        request_deadline: Duration,
    ) -> Result<Response<GatewayBody>, TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        if request.version() != Version::HTTP_2
            || !has_content_type(request.headers(), NDJSON_CONTENT_TYPE)
        {
            return Err(TunnelError::Invalid(
                "Agent control channel requires an HTTP/2 NDJSON POST",
            ));
        }
        let request_id = request_id(request.headers())?;
        let request_trace_id = trace_id(request.headers());
        let mut body = request.into_body();
        let (prefix, open) = timeout(request_deadline, read_agent_open(&mut body))
            .await
            .map_err(|_| TunnelError::Deadline)??;
        let agent_id = open.request.agent_id.clone();
        if peer_agent_id
            .as_ref()
            .is_some_and(|expected_agent_id| expected_agent_id != &agent_id)
        {
            return Err(TunnelError::Identity);
        }
        if !matches!(
            open.request.payload.message,
            AgentChannelUpstreamMessage::Open(_)
        ) {
            return Err(TunnelError::Invalid(
                "the first Agent stream frame must be channel.open",
            ));
        }

        let stream_id = fresh_connection_id("agent")?;
        let (event_sender, event_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        let (response_sender, response_receiver) = mpsc::channel(STREAM_RESPONSE_BUFFER);
        let (route_sender, route_receiver) = oneshot::channel();
        let gate = self.stream_admission_gate(&stream_id).await;
        let _gate_guard = gate.lock().await;
        self.reserve_stream_request(&request_id, &stream_id, route_sender)
            .await?;
        let mut closed_streams = self.state.closed_streams.lock().await;
        closed_streams.remove(&stream_id);
        self.state
            .stream_cancellations
            .lock()
            .await
            .insert(stream_id.clone(), watch::channel(false).0);
        self.state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), event_sender);
        drop(closed_streams);
        drop(_gate_guard);

        if let Err(error) = self
            .send_control(
                request_id.clone(),
                request_trace_id.clone(),
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: stream_id.clone(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            )
            .await
        {
            self.remove_stream(&request_id, &stream_id).await;
            return Err(error);
        }

        let upstream = Arc::clone(self);
        let upstream_request_id = request_id.clone();
        let upstream_stream_id = stream_id.clone();
        let upstream_trace_id = request_trace_id.clone();
        tokio::spawn(async move {
            let result = upstream
                .forward_agent_input(
                    upstream_request_id.clone(),
                    upstream_trace_id,
                    upstream_stream_id.clone(),
                    prefix,
                    body,
                )
                .await;
            if let Err(error) = result {
                tracing::warn!(%error, stream_id = %upstream_stream_id, "Agent stream input failed");
            }
        });

        let downstream = Arc::clone(self);
        let downstream_request_id = request_id.clone();
        let downstream_stream_id = stream_id.clone();
        tokio::spawn(async move {
            downstream
                .forward_agent_output(
                    downstream_request_id,
                    downstream_stream_id,
                    agent_id,
                    route_receiver,
                    event_receiver,
                    response_sender,
                )
                .await;
        });

        Response::builder()
            .status(StatusCode::OK)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .header(REQUEST_ID_HEADER, request_id.as_str())
            .body(Either::Right(StreamingBody::new(response_receiver)))
            .map_err(|_| TunnelError::Invalid("Agent stream response metadata is invalid"))
    }

    async fn reserve_stream_request(
        &self,
        request_id: &RequestId,
        stream_id: &GatewayConnectionId,
        route_sender: oneshot::Sender<Result<GatewayRouteLeaseGranted, GatewayControlError>>,
    ) -> Result<(), TunnelError> {
        {
            let mut stream_requests = self.state.stream_requests.lock().await;
            if stream_requests.contains_key(request_id) {
                return Err(TunnelError::Invalid(
                    "x-request-id is already bound to an active Agent stream",
                ));
            }
            stream_requests.insert(request_id.clone(), stream_id.clone());
        }
        let route_request_conflict = {
            let mut pending_routes = self.state.pending_routes.lock().await;
            if pending_routes.contains_key(request_id) {
                true
            } else {
                pending_routes.insert(request_id.clone(), route_sender);
                false
            }
        };
        if route_request_conflict {
            self.state.stream_requests.lock().await.remove(request_id);
            return Err(TunnelError::Invalid(
                "x-request-id conflicts with a pending Gateway operation",
            ));
        }
        Ok(())
    }

    async fn forward_agent_input<B>(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        stream_id: GatewayConnectionId,
        prefix: Vec<Bytes>,
        body: B,
    ) -> Result<(), TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        let cancellation = self.stream_cancellation_receiver(&stream_id).await;
        let result = self
            .forward_agent_input_inner(
                request_id.clone(),
                trace_id,
                stream_id.clone(),
                prefix,
                body,
                cancellation,
            )
            .await;
        if result.is_err() {
            // A body/read or Central-send failure can happen before AgentStreamEnd is emitted.
            // Close the correlated state here so the output worker cannot keep renewing a route
            // after the HTTP request has already failed.
            self.close_stream_state(&stream_id, Some(&request_id)).await;
        }
        result
    }

    async fn forward_agent_input_inner<B>(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        stream_id: GatewayConnectionId,
        prefix: Vec<Bytes>,
        mut body: B,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<(), TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        for chunk in prefix {
            self.send_agent_stream_chunk(&request_id, trace_id.clone(), &stream_id, chunk)
                .await?;
        }
        loop {
            if *cancellation.borrow() {
                return Err(TunnelError::Closed);
            }
            let frame = tokio::select! {
                changed = cancellation.changed() => {
                    if changed.is_err() || *cancellation.borrow() {
                        return Err(TunnelError::Closed);
                    }
                    continue;
                }
                frame = body.frame() => frame,
            };
            let Some(frame) = frame else {
                break;
            };
            let frame = frame.map_err(|_| TunnelError::Body)?;
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            self.send_agent_stream_chunk(&request_id, trace_id.clone(), &stream_id, chunk)
                .await?;
        }
        self.send_agent_stream_control(
            request_id,
            trace_id,
            &stream_id,
            GatewayControlMessage::AgentStreamEnd(GatewayAgentStreamEnd {
                stream_id: stream_id.clone(),
            }),
        )
        .await
    }

    async fn send_agent_stream_chunk(
        &self,
        request_id: &RequestId,
        trace_id: Option<TraceId>,
        stream_id: &GatewayConnectionId,
        chunk: Bytes,
    ) -> Result<(), TunnelError> {
        for part in chunk.chunks(MAX_GATEWAY_STREAM_CHUNK_BYTES) {
            self.send_agent_stream_control(
                request_id.clone(),
                trace_id.clone(),
                stream_id,
                GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                    stream_id: stream_id.clone(),
                    chunk: GatewayOpaqueBytes::new(part.to_vec())?,
                }),
            )
            .await?;
        }
        Ok(())
    }

    /// Serializes Agent input frame admission with stream fencing. The tombstone guard remains
    /// held while the frame enters the Central output queue; a close that wins the gate therefore
    /// cannot be followed by a stale Data/End frame. `send_control_impl` suppresses its usual
    /// recursive disconnect until these guards have been released.
    async fn send_agent_stream_control(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        stream_id: &GatewayConnectionId,
        message: GatewayControlMessage,
    ) -> Result<(), TunnelError> {
        let gate = self.stream_admission_gate(stream_id).await;
        let gate_guard = gate.lock().await;
        let closed = self.state.closed_streams.lock().await;
        let request_matches = self
            .state
            .stream_requests
            .lock()
            .await
            .get(&request_id)
            .is_some_and(|bound_stream_id| bound_stream_id == stream_id);
        if closed.contains(stream_id)
            || !request_matches
            || !self.state.streams.lock().await.contains_key(stream_id)
        {
            drop(closed);
            drop(gate_guard);
            return Err(TunnelError::Closed);
        }
        let connection_id = self
            .state
            .link
            .lock()
            .await
            .as_ref()
            .map(|link| link.connection_id.clone());
        let result = self
            .send_control_impl(request_id, trace_id, message, false)
            .await;
        drop(closed);
        drop(gate_guard);
        if result.is_err() {
            if let Some(connection_id) = connection_id {
                self.disconnect(&connection_id).await;
            }
        }
        result
    }

    async fn stream_admission_gate(&self, stream_id: &GatewayConnectionId) -> Arc<Mutex<()>> {
        let mut gates = self.state.stream_admission_gates.lock().await;
        if gates.len() >= MAX_STREAM_ADMISSION_GATES {
            // A gate may be handed to a worker before that worker publishes its stream/request
            // maps. Keep entries with another Arc owner, and inspect each correlated directory
            // before evicting an otherwise idle entry. The checks are deliberately performed one
            // lock at a time: the stream cleanup path has a different pending/routes/streams
            // order, and holding several of these locks here would introduce a lock cycle.
            let active_streams = self
                .state
                .streams
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let request_bindings = self.state.stream_requests.lock().await.clone();
            let active_routes = self
                .state
                .routes
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let pending_requests = self
                .state
                .pending_routes
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut evicted = None;
            for candidate in gates.keys().cloned().collect::<Vec<_>>() {
                let Some(gate) = gates.get(&candidate) else {
                    continue;
                };
                // The map owns the sole Arc for an idle entry. Any open/close/route worker that
                // already obtained the gate keeps an additional strong reference, so removing
                // this entry cannot split that worker from a replacement gate.
                if Arc::strong_count(gate) != 1 {
                    continue;
                }
                if active_streams.contains(&candidate) {
                    continue;
                }
                if request_bindings
                    .values()
                    .any(|bound_stream_id| bound_stream_id == &candidate)
                {
                    continue;
                }
                if active_routes.contains(&candidate) {
                    continue;
                }
                // Pending route requests are keyed by RequestId rather than stream ID. Only a
                // request that is still bound to this candidate protects it; an unrelated route
                // mutation must not disable bounded gate eviction for every other stream.
                if pending_requests
                    .iter()
                    .any(|request_id| request_bindings.get(request_id) == Some(&candidate))
                {
                    continue;
                }
                evicted = Some(candidate);
                break;
            }
            if let Some(evicted) = evicted {
                gates.remove(&evicted);
            }
        }
        gates
            .entry(stream_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn stream_cancellation_receiver(
        &self,
        stream_id: &GatewayConnectionId,
    ) -> watch::Receiver<bool> {
        // Use the same gate as frame admission so a receiver created after close observes the
        // already-fenced state instead of installing a fresh, unsignalled channel.
        let gate = self.stream_admission_gate(stream_id).await;
        let _gate_guard = gate.lock().await;
        let closed = self.state.closed_streams.lock().await;
        let initial = closed.contains(stream_id);
        let mut cancellations = self.state.stream_cancellations.lock().await;
        if initial && !cancellations.contains_key(stream_id) {
            // The stream was already fenced and its sender was removed by cleanup.  Return an
            // already-cancelled receiver without re-inserting a permanent tombstone into the
            // cancellation directory.
            return watch::channel(true).1;
        }
        let sender = cancellations
            .entry(stream_id.clone())
            .or_insert_with(|| watch::channel(initial).0);
        if initial {
            // `watch::Sender::send` fails without a live receiver and would leave a late worker
            // subscribed to the stale `false` value. `send_replace` updates the cell regardless
            // of receiver count.
            sender.send_replace(true);
        }
        sender.subscribe()
    }

    async fn forward_agent_output(
        &self,
        request_id: RequestId,
        stream_id: GatewayConnectionId,
        agent_id: neoengram_domain::protocol::AgentId,
        route_receiver: oneshot::Receiver<Result<GatewayRouteLeaseGranted, GatewayControlError>>,
        mut events: mpsc::Receiver<StreamEvent>,
        output: mpsc::Sender<Bytes>,
    ) {
        let granted = match timeout(
            Duration::from_millis(CONTROL_FRAME_DEADLINE_MS),
            route_receiver,
        )
        .await
        {
            Ok(Ok(Ok(granted)))
                if granted.agent_id == agent_id
                    && granted.owner_replica_id == self.identity.gateway_replica_id
                    && granted.agent_connection_id == stream_id
                    && granted.lease_expires_at_unix_ms.get() > now_unix_ms().get()
                    && granted.lease_expires_at_unix_ms.get() <= lease_expiry().get() =>
            {
                granted
            }
            Ok(Ok(Err(error))) => {
                tracing::warn!(code = ?error.code, detail = %error.detail, %stream_id, "Central rejected the atomic Agent route");
                self.remove_stream(&request_id, &stream_id).await;
                return;
            }
            _ => {
                tracing::warn!(%stream_id, "Central did not grant the atomic Agent route");
                self.remove_stream(&request_id, &stream_id).await;
                return;
            }
        };
        let mut decoder = AgentChannelNdjsonDecoder::new();
        let mut pending = Vec::new();
        let mut opened_verified = false;
        let mut route = ActiveRoute {
            agent_id: granted.agent_id,
            session_generation: granted.session_generation,
            route_generation: granted.route_generation,
            lease_expires_at_unix_ms: granted.lease_expires_at_unix_ms,
        };
        if !self.insert_route_if_open(&stream_id, route.clone()).await {
            self.remove_stream_if_route(&request_id, &stream_id, &route)
                .await;
            return;
        }
        let mut renew = interval(Duration::from_millis(AGENT_ROUTE_LEASE_RENEW_INTERVAL_MS));
        renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
        renew.tick().await;

        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Some(StreamEvent::Data(chunk)) if !opened_verified => {
                            pending.push(chunk.clone());
                            let lines = match decoder.push(&chunk) {
                                Ok(lines) => lines,
                                Err(error) => {
                                    tracing::warn!(%error, %stream_id, "Central returned invalid Agent NDJSON");
                                    break;
                                }
                            };
                            let Some(first) = lines.first() else {
                                continue;
                            };
                            let opened = match AgentChannelDownstreamFrame::decode_json(first) {
                                Ok(opened) => opened,
                                Err(error) => {
                                    tracing::warn!(%error, %stream_id, "Central returned an invalid Agent Opened frame");
                                    break;
                                }
                            };
                            let AgentChannelDownstreamMessage::Opened(opened_payload) = &opened.message else {
                                tracing::warn!(%stream_id, "Central did not open the Agent stream before sending work");
                                break;
                            };
                            if opened_payload.agent_id != agent_id
                                || opened_payload.session_generation != opened.session_generation
                            {
                                tracing::warn!(%stream_id, "Central Agent Opened identity does not match the signed Agent Open");
                                break;
                            }
                            if opened.session_generation != route.session_generation {
                                tracing::warn!(%stream_id, "Central Agent Opened generation differs from its atomic route");
                                break;
                            }
                            // RouteFence/teardown may have won while this Opened frame was
                            // queued. Never let a late worker resurrect a transfer fence after
                            // its stream and route have been removed.
                            let route_still_active = {
                                let closed = self.state.closed_streams.lock().await;
                                !closed.contains(&stream_id)
                                    && self
                                        .state
                                        .routes
                                        .lock()
                                        .await
                                        .get(&stream_id)
                                        .is_some_and(|current| current == &route)
                            };
                            if !route_still_active {
                                tracing::warn!(%stream_id, "Central Agent Opened arrived after its route was fenced");
                                break;
                            }
                            if let Some(fence) = &self.state.transfer_fence {
                                fence.set_agent_generations(
                                    &opened_payload.agent_id,
                                    opened_payload.session_generation.get(),
                                    opened_payload.mount_generation.get(),
                                    route.route_generation.get(),
                                );
                            }
                            opened_verified = true;
                            for bytes in pending.drain(..) {
                                if output.send(bytes).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Some(StreamEvent::Data(chunk)) => {
                            if output.send(chunk).await.is_err() {
                                break;
                            }
                        }
                        Some(StreamEvent::Error(error)) => {
                            tracing::warn!(code = ?error.code, detail = %error.detail, %stream_id, "Central closed the Agent stream");
                            break;
                        }
                        Some(StreamEvent::End) | None => break,
                    }
                }
                _ = renew.tick(), if opened_verified && !self.is_draining() => {
                    // Drain may race with select-arm readiness. Re-check immediately before the
                    // mutation so no renewal can be queued after the local fence.
                    if self.is_draining() {
                        continue;
                    }
                    let current = &route;
                    let renewal = GatewayControlMessage::RouteRenew(GatewayRouteLeaseRequest {
                        agent_id: current.agent_id.clone(),
                        owner_replica_id: self.identity.gateway_replica_id.clone(),
                        agent_connection_id: stream_id.clone(),
                        session_generation: current.session_generation,
                        route_generation: Some(current.route_generation),
                        requested_expires_at_unix_ms: lease_expiry(),
                    });
                    match self.mutate_route(renewal).await {
                        Ok(granted)
                            if granted.agent_id == route.agent_id
                                && granted.owner_replica_id == self.identity.gateway_replica_id
                                && granted.agent_connection_id == stream_id
                                && granted.session_generation == route.session_generation
                                && granted.route_generation == route.route_generation
                                && granted.lease_expires_at_unix_ms.get()
                                    > route.lease_expires_at_unix_ms.get()
                                && granted.lease_expires_at_unix_ms.get()
                                    <= lease_expiry().get() =>
                        {
                            route.route_generation = granted.route_generation;
                            route.lease_expires_at_unix_ms = granted.lease_expires_at_unix_ms;
                            if !self
                                .insert_route_if_open(&stream_id, route.clone())
                                .await
                            {
                                break;
                            }
                        }
                        Ok(_) => {
                            tracing::warn!(%stream_id, "Agent route renewal returned a different identity");
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(%error, %stream_id, "Agent route renewal failed closed");
                            break;
                        }
                    }
                }
            }
        }

        if opened_verified {
            let _ = self
                .mutate_route(GatewayControlMessage::RouteRelease(
                    GatewayRouteLeaseRequest {
                        agent_id: route.agent_id.clone(),
                        owner_replica_id: self.identity.gateway_replica_id.clone(),
                        agent_connection_id: stream_id.clone(),
                        session_generation: route.session_generation,
                        route_generation: Some(route.route_generation),
                        requested_expires_at_unix_ms: route.lease_expires_at_unix_ms,
                    },
                ))
                .await;
        }
        self.remove_stream_if_route(&request_id, &stream_id, &route)
            .await;
    }

    async fn mutate_route(
        &self,
        message: GatewayControlMessage,
    ) -> Result<GatewayRouteLeaseGranted, TunnelError> {
        self.mutate_route_with_timeout(message, Duration::from_millis(CONTROL_FRAME_DEADLINE_MS))
            .await
    }

    async fn mutate_route_with_timeout(
        &self,
        message: GatewayControlMessage,
        response_timeout: Duration,
    ) -> Result<GatewayRouteLeaseGranted, TunnelError> {
        if self.is_draining()
            && matches!(
                &message,
                GatewayControlMessage::RouteAcquire(_) | GatewayControlMessage::RouteRenew(_)
            )
        {
            return Err(TunnelError::Unavailable);
        }
        let request_id = fresh_request_id("route")?;
        let (sender, receiver) = oneshot::channel();
        self.state
            .pending_routes
            .lock()
            .await
            .insert(request_id.clone(), sender);
        if let Err(error) = self.send_control(request_id.clone(), None, message).await {
            self.expire_route(&request_id).await;
            return Err(error);
        }
        let outcome = match timeout(response_timeout, receiver).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => {
                self.expire_route(&request_id).await;
                return Err(TunnelError::Closed);
            }
            Err(_) => {
                self.expire_route(&request_id).await;
                return Err(TunnelError::Deadline);
            }
        };
        match outcome {
            Ok(granted) => Ok(granted),
            Err(error) if error.code == GatewayErrorCode::RouteFenced => Err(TunnelError::Fenced),
            Err(_) => Err(TunnelError::Unavailable),
        }
    }

    async fn read_control<B>(
        &self,
        connection_id: GatewayConnectionId,
        mut body: B,
    ) -> Result<(), TunnelError>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        let mut decoder = GatewayControlNdjsonDecoder::new();
        let mut last_sequence = 0_u64;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| TunnelError::Body)?;
            let Ok(bytes) = frame.into_data() else {
                continue;
            };
            for line in decoder.push(&bytes)? {
                let frame = GatewayControlFrame::decode_json(&line)?;
                frame.validate_at(now_unix_ms())?;
                if frame.gateway_pool_id != self.identity.gateway_pool_id
                    || frame.gateway_replica_id != self.identity.gateway_replica_id
                    || frame.connection_id != connection_id
                    || frame.hop_count != 0
                    || frame.sequence.get() != last_sequence.saturating_add(1)
                {
                    return Err(TunnelError::Invalid(
                        "Central frame identity, hop, or sequence is invalid",
                    ));
                }
                last_sequence = frame.sequence.get();
                self.dispatch_control(frame).await?;
            }
        }
        decoder.finish()?;
        Err(TunnelError::Closed)
    }

    async fn dispatch_control(&self, frame: GatewayControlFrame) -> Result<(), TunnelError> {
        let request_id = frame.request_id.clone();
        let trace_id = frame.trace_id.clone();
        match frame.message {
            GatewayControlMessage::AgentResponse(response) => {
                let mut pending = self.state.pending_unary.lock().await;
                if let Some(sender) = pending.remove(&response.stream_id) {
                    // A cancelled HTTP caller must not be able to tear down the shared Central
                    // control session when its response arrives after cancellation.
                    let _ = sender.send(response);
                    return Ok(());
                }
                let late = self
                    .state
                    .late_unary
                    .lock()
                    .await
                    .remove(&response.stream_id);
                if late {
                    Ok(())
                } else {
                    Err(TunnelError::Invalid("unknown Agent unary response"))
                }
            }
            GatewayControlMessage::AgentStreamData(data) => {
                self.dispatch_correlated_stream_event(
                    &request_id,
                    &data.stream_id,
                    StreamEvent::Data(Bytes::from(data.chunk.into_bytes())),
                    "unknown Agent stream response",
                )
                .await
            }
            GatewayControlMessage::AgentStreamEnd(end) => {
                self.dispatch_correlated_stream_event(
                    &request_id,
                    &end.stream_id,
                    StreamEvent::End,
                    "unknown Agent stream end",
                )
                .await
            }
            GatewayControlMessage::RouteGranted(granted) => {
                if self
                    .state
                    .closed_streams
                    .lock()
                    .await
                    .contains(&granted.agent_connection_id)
                {
                    self.expire_route(&frame.request_id).await;
                    return Ok(());
                }
                let mut pending = self.state.pending_routes.lock().await;
                if let Some(sender) = pending.remove(&frame.request_id) {
                    let _ = sender.send(Ok(granted));
                    return Ok(());
                }
                let late = self
                    .state
                    .late_routes
                    .lock()
                    .await
                    .remove(&frame.request_id);
                if late {
                    Ok(())
                } else {
                    Err(TunnelError::Invalid("unknown route grant"))
                }
            }
            GatewayControlMessage::RouteFence(fence) => {
                // A takeover fence is scoped to one Agent and generation.  A Replica can carry
                // several streams for other Agents (and, during reconnect races, more than one
                // generation for this Agent), so selecting only the first matching map entry can
                // leave an older stream alive and still able to consume Central frames.
                let stream_ids = self
                    .state
                    .routes
                    .lock()
                    .await
                    .iter()
                    .filter(|(_, route)| {
                        route.agent_id == fence.agent_id
                            && route.route_generation <= fence.route_generation
                    })
                    .map(|(stream_id, _)| stream_id.clone())
                    .collect::<Vec<_>>();
                for stream_id in stream_ids {
                    let _ = self
                        .dispatch_stream_event(
                            &stream_id,
                            StreamEvent::Error(GatewayControlError {
                                code: GatewayErrorCode::RouteFenced,
                                detail: fence.reason.clone(),
                                retryable: false,
                            }),
                            "unknown fenced Agent stream",
                        )
                        .await;
                    // Remove the route immediately after queueing the terminal error.  The stream
                    // task will perform its normal cleanup as well, but an immediate removal
                    // prevents peer forwarding or a concurrent renewal from observing stale
                    // ownership while the HTTP response body is unwinding.
                    self.close_stream_state(&stream_id, None).await;
                }
                Ok(())
            }
            GatewayControlMessage::S3ReadRevocation(revocation) => {
                // No receiver means this Replica has no S3 read registry and therefore no active
                // object streams to revoke. Lag handling is fail-closed in the registry itself.
                let _ = self.state.s3_read_revocations.send(revocation);
                Ok(())
            }
            GatewayControlMessage::PeerDirectory(directory) => {
                self.install_peer_directory(directory).await
            }
            GatewayControlMessage::PeerForward(request) => {
                self.forward_to_owner(request_id, trace_id, frame.deadline_unix_ms, request)
                    .await
            }
            GatewayControlMessage::Backpressure(GatewayBackpressure { retry_after_ms }) => {
                self.dispatch_error(
                    &frame.request_id,
                    GatewayControlError {
                        code: GatewayErrorCode::ResourceExhausted,
                        detail: format!("Central applied backpressure for {retry_after_ms} ms"),
                        retryable: true,
                    },
                )
                .await
            }
            GatewayControlMessage::Error(error) => {
                self.dispatch_error(&frame.request_id, error).await
            }
            GatewayControlMessage::Drain(_) => {
                self.disconnect(&frame.connection_id).await;
                Err(TunnelError::Unavailable)
            }
            GatewayControlMessage::ReplicaHello(_)
            | GatewayControlMessage::ReplicaHeartbeat(_)
            | GatewayControlMessage::AgentRequest(_)
            | GatewayControlMessage::AgentStreamOpen(_)
            | GatewayControlMessage::RouteAcquire(_)
            | GatewayControlMessage::RouteRenew(_)
            | GatewayControlMessage::RouteRelease(_)
            | GatewayControlMessage::PeerForwardAccepted(_) => Err(TunnelError::Invalid(
                "Central sent a Gateway-only control message",
            )),
        }
    }

    async fn forward_to_owner(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        deadline_unix_ms: UnixMillis,
        request: GatewayPeerForwardRequest,
    ) -> Result<(), TunnelError> {
        if request.source_replica_id != self.identity.gateway_replica_id {
            return Err(TunnelError::Identity);
        }
        let now = now_unix_ms();
        let remaining = deadline_unix_ms
            .get()
            .saturating_sub(now.get())
            .min(CONTROL_FRAME_DEADLINE_MS);
        if remaining == 0 {
            return self
                .send_control(
                    request_id,
                    trace_id,
                    GatewayControlMessage::Error(deadline_exceeded(
                        "peer forwarding deadline already elapsed",
                    )),
                )
                .await;
        }
        let peer_connection_id = fresh_connection_id("peer")?;
        let peer_frame = GatewayControlFrame {
            wire_version: CURRENT_WIRE_VERSION,
            gateway_pool_id: self.identity.gateway_pool_id.clone(),
            gateway_replica_id: self.identity.gateway_replica_id.clone(),
            connection_id: peer_connection_id,
            sequence: SequenceNumber::new(1),
            request_id: request_id.clone(),
            trace_id: trace_id.clone(),
            sent_at_unix_ms: now,
            deadline_unix_ms,
            hop_count: 1,
            message: GatewayControlMessage::PeerForward(request.clone()),
            extensions: neoengram_domain::protocol::Extensions::new(),
        };
        let result = timeout(
            Duration::from_millis(remaining),
            self.peer_forwarder
                .forward(&request.target_peer_endpoint, peer_frame),
        )
        .await
        .unwrap_or_else(|_| Err(deadline_exceeded("owner Replica forwarding timed out")));
        let message = match result {
            Ok(accepted)
                if accepted.source_replica_id == request.source_replica_id
                    && accepted.target_replica_id == request.target_replica_id
                    && accepted.agent_id == request.agent_id
                    && accepted.agent_connection_id == request.agent_connection_id
                    && accepted.session_generation == request.session_generation
                    && accepted.route_generation == request.route_generation =>
            {
                GatewayControlMessage::PeerForwardAccepted(accepted)
            }
            Ok(_) => GatewayControlMessage::Error(identity_rejected(
                "owner Replica acknowledgement does not match the requested route",
            )),
            Err(error) => GatewayControlMessage::Error(error),
        };
        self.send_control(request_id, trace_id, message).await
    }

    #[cfg(test)]
    pub(crate) async fn accept_peer_forward(
        &self,
        frame: GatewayControlFrame,
        authenticated_source: &GatewayReplicaId,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        self.accept_peer_forward_with_fingerprint(frame, authenticated_source, None)
            .await
    }

    /// Peer-listener entry point used by the TLS transport. `None` is retained only for explicit
    /// loopback development tests; production mTLS callers must provide the leaf fingerprint.
    pub(crate) async fn accept_peer_forward_with_fingerprint(
        &self,
        frame: GatewayControlFrame,
        authenticated_source: &GatewayReplicaId,
        source_certificate_fingerprint: Option<&ContentDigest>,
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        let now = now_unix_ms();
        match frame.validate_at(now) {
            Ok(()) => {}
            Err(neoengram_domain::protocol::ProtocolError::InvalidField {
                field: "deadline_unix_ms",
                ..
            }) => {
                return Err(deadline_exceeded("peer forward frame deadline has elapsed"));
            }
            Err(_) => {
                return Err(protocol_invalid("peer forward frame is invalid or expired"));
            }
        }
        if frame.gateway_pool_id != self.identity.gateway_pool_id
            || frame.gateway_replica_id != *authenticated_source
            || frame.hop_count != 1
            || frame.sequence.get() != 1
        {
            return Err(identity_rejected(
                "peer frame identity, pool, hop, or sequence is invalid",
            ));
        }
        let GatewayControlMessage::PeerForward(request) = frame.message else {
            return Err(protocol_invalid(
                "peer endpoint accepts only peer_forward messages",
            ));
        };
        if request.source_replica_id != *authenticated_source
            || request.target_replica_id != self.identity.gateway_replica_id
        {
            return Err(identity_rejected(
                "peer mTLS source or target Replica does not match the forward route",
            ));
        }
        if let Some(fingerprint) = source_certificate_fingerprint {
            self.authorize_peer_source(authenticated_source, fingerprint)
                .await?;
        }

        let encoded = request.frame.as_bytes();
        let downstream = AgentChannelDownstreamFrame::decode_json(&encoded[..encoded.len() - 1])
            .map_err(|_| protocol_invalid("forwarded Agent frame is invalid"))?;
        if downstream.session_generation != request.session_generation {
            return Err(route_fenced(
                "forwarded Agent frame has the wrong session generation",
            ));
        }
        if !matches!(
            downstream.message,
            AgentChannelDownstreamMessage::Assignment(_)
                | AgentChannelDownstreamMessage::Decision(_)
                | AgentChannelDownstreamMessage::LifecycleAssignment(_)
        ) {
            return Err(protocol_invalid(
                "peer forwarding accepts only Central Job or lifecycle command frames",
            ));
        }
        let request_id = frame.request_id.clone();
        let binding = PeerForwardReplayBinding::from_request(&request);
        let admission = self
            .admit_peer_forward(&request_id, binding.clone(), frame.deadline_unix_ms)
            .await?;
        match admission {
            PeerForwardAdmission::Replay(completion) => await_peer_forward_replay(completion).await,
            PeerForwardAdmission::Execute {
                reservation,
                completion,
            } => {
                let result = self
                    .deliver_peer_forward(&request, frame.deadline_unix_ms, encoded)
                    .await;
                let successful = result.is_ok();
                // The map owns a receiver, so this send normally cannot fail.  A dropped sender
                // is still handled by replay waiters as a retryable route-unavailable result.
                let _ = completion.send(Some(result.clone()));
                if !successful {
                    self.remove_peer_forward_replay(&request_id, &reservation)
                        .await;
                }
                result
            }
        }
    }

    async fn admit_peer_forward(
        &self,
        request_id: &RequestId,
        binding: PeerForwardReplayBinding,
        expires_at_unix_ms: UnixMillis,
    ) -> Result<PeerForwardAdmission, GatewayControlError> {
        let mut seen = self.state.peer_forward_seen.lock().await;
        let now = now_unix_ms();
        // A frame is rejected by validate_at once its deadline passes. Remove only completed
        // entries after that same boundary; retaining in-flight reservations prevents a slow or
        // cancelled delivery from being duplicated by an admission race.
        seen.retain(|_, entry| {
            !(entry.expires_at_unix_ms.get() <= now.get() && peer_forward_replay_complete(entry))
        });
        if let Some(previous) = seen.get(request_id) {
            if previous.binding != binding {
                return Err(identity_rejected(
                    "peer forwarding request ID was replayed with different payload or route fields",
                ));
            }
            // A cancelled owner leaves a closed watch channel behind.  Drop that stale
            // reservation so a later request can make progress instead of waiting forever.
            if previous.completion.borrow().is_none() && previous.completion.has_changed().is_err()
            {
                seen.remove(request_id);
            } else {
                return Ok(PeerForwardAdmission::Replay(previous.completion.clone()));
            }
        }

        while seen.len() >= MAX_PEER_FORWARD_REPLAYS {
            let evict = seen.iter().find_map(|(key, entry)| {
                (entry.expires_at_unix_ms.get() <= now.get() && peer_forward_replay_complete(entry))
                    .then(|| key.clone())
            });
            let Some(key) = evict else {
                return Err(resource_exhausted(
                    "peer forwarding replay admission is at capacity",
                ));
            };
            seen.remove(&key);
        }

        let (completion, receiver) = watch::channel(None);
        let reservation = Arc::new(());
        seen.insert(
            request_id.clone(),
            PeerForwardReplay {
                binding,
                expires_at_unix_ms,
                reservation: reservation.clone(),
                completion: receiver,
            },
        );
        Ok(PeerForwardAdmission::Execute {
            reservation,
            completion,
        })
    }

    async fn remove_peer_forward_replay(&self, request_id: &RequestId, reservation: &Arc<()>) {
        let mut seen = self.state.peer_forward_seen.lock().await;
        if seen
            .get(request_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.reservation, reservation))
        {
            seen.remove(request_id);
        }
    }

    async fn deliver_peer_forward(
        &self,
        request: &GatewayPeerForwardRequest,
        deadline_unix_ms: UnixMillis,
        encoded: &[u8],
    ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
        let now = now_unix_ms();
        let remaining_ms = deadline_unix_ms.get().saturating_sub(now.get());
        if remaining_ms == 0 {
            return Err(deadline_exceeded(
                "owner Agent stream delivery deadline elapsed",
            ));
        }
        let delivery_deadline =
            Instant::now() + Duration::from_millis(remaining_ms.min(CONTROL_FRAME_DEADLINE_MS));
        let mut event = StreamEvent::Data(Bytes::copy_from_slice(encoded));
        loop {
            // The tombstone is the stream fencing linearization point. Keep it locked through the
            // route check and non-blocking queue admission so a concurrent takeover either
            // observes this frame as admitted first or closes the stream first; a cloned sender
            // can never write after the close has won. The lock order matches
            // `close_stream_state` (closed -> streams -> routes).
            let attempt = {
                let closed = self.state.closed_streams.lock().await;
                if closed.contains(&request.agent_connection_id) {
                    return Err(route_fenced(
                        "owner Agent stream was fenced before peer delivery",
                    ));
                }
                let streams = self.state.streams.lock().await;
                let sender = streams
                    .get(&request.agent_connection_id)
                    .ok_or_else(|| route_unavailable("owner Agent stream is not active"))?;
                let routes = self.state.routes.lock().await;
                let route = routes.get(&request.agent_connection_id).ok_or_else(|| {
                    route_unavailable("owner Replica has no active Agent connection")
                })?;
                let now = now_unix_ms();
                if deadline_unix_ms.get() <= now.get() {
                    return Err(deadline_exceeded(
                        "owner Agent stream delivery deadline elapsed",
                    ));
                }
                if route.agent_id != request.agent_id
                    || route.session_generation != request.session_generation
                    || route.route_generation != request.route_generation
                    || route.lease_expires_at_unix_ms.get() <= now.get()
                {
                    return Err(route_fenced(
                        "owner Agent route generation or connection does not match",
                    ));
                }
                sender.try_send(event)
            };
            match attempt {
                Ok(()) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    return Err(route_unavailable(
                        "owner Agent stream closed before delivery",
                    ));
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                    event = returned;
                    if Instant::now() >= delivery_deadline {
                        return Err(deadline_exceeded("owner Agent stream delivery timed out"));
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        Ok(GatewayPeerForwardAccepted {
            source_replica_id: request.source_replica_id.clone(),
            target_replica_id: request.target_replica_id.clone(),
            agent_id: request.agent_id.clone(),
            agent_connection_id: request.agent_connection_id.clone(),
            session_generation: request.session_generation,
            route_generation: request.route_generation,
        })
    }

    async fn dispatch_error(
        &self,
        request_id: &RequestId,
        error: GatewayControlError,
    ) -> Result<(), TunnelError> {
        let mut pending_routes = self.state.pending_routes.lock().await;
        if let Some(sender) = pending_routes.remove(request_id) {
            let _ = sender.send(Err(error));
            drop(pending_routes);
            // The initial AgentStreamOpen uses the HTTP request ID as its route waiter key. Fence
            // that stream immediately after delivering the route error so its input worker cannot
            // emit Data/End while the output worker is still unwinding the failed waiter.
            let stream_id = {
                let stream_requests = self.state.stream_requests.lock().await;
                stream_requests.get(request_id).cloned()
            };
            if let Some(stream_id) = stream_id {
                self.close_stream_state(&stream_id, Some(request_id)).await;
            }
            return Ok(());
        }
        if self.state.late_routes.lock().await.remove(request_id) {
            return Ok(());
        }
        drop(pending_routes);
        let stream_id = self
            .state
            .stream_requests
            .lock()
            .await
            .get(request_id)
            .cloned()
            .ok_or(TunnelError::Invalid("Central error has no pending request"))?;
        self.dispatch_stream_event(
            &stream_id,
            StreamEvent::Error(error),
            "unknown Agent stream",
        )
        .await
    }

    /// Delivers a stream data/end frame only when its envelope request ID matches the request
    /// that opened the stream.  Stream IDs are transport-local, so accepting a frame under a
    /// different request ID would let a stale or misrouted Central frame write into the wrong
    /// Agent HTTP response.  A mismatch fences the affected stream before returning the protocol
    /// error; the control session will then fail closed through its normal reader path.
    async fn dispatch_correlated_stream_event(
        &self,
        request_id: &RequestId,
        stream_id: &GatewayConnectionId,
        event: StreamEvent,
        unknown_detail: &'static str,
    ) -> Result<(), TunnelError> {
        let bound_request_id = {
            let closed = self.state.closed_streams.lock().await;
            if closed.contains(stream_id) {
                // Preserve the existing tombstone behavior for frames that were already queued
                // when the stream closed. There is no active binding left to validate here.
                return Ok(());
            }
            self.state.stream_requests.lock().await.iter().find_map(
                |(bound_request_id, bound_stream_id)| {
                    (bound_stream_id == stream_id).then(|| bound_request_id.clone())
                },
            )
        };

        match bound_request_id {
            Some(bound_request_id) if bound_request_id.as_str() == request_id.as_str() => {
                self.dispatch_stream_event(stream_id, event, unknown_detail)
                    .await
            }
            Some(bound_request_id) => {
                self.close_stream_state(stream_id, Some(&bound_request_id))
                    .await;
                Err(TunnelError::Invalid(
                    "Agent stream frame request ID does not match its bound request",
                ))
            }
            None => {
                // A stream entry without a request binding is an internal protocol violation. If
                // the sender is still present, close it before reporting the error; otherwise
                // retain the ordinary unknown-stream diagnostic.
                let stream_exists = self.state.streams.lock().await.contains_key(stream_id);
                if stream_exists {
                    self.close_stream_state(stream_id, None).await;
                    Err(TunnelError::Invalid("Agent stream has no bound request ID"))
                } else {
                    Err(TunnelError::Invalid(unknown_detail))
                }
            }
        }
    }

    /// Delivers one Central frame without awaiting a slow Agent HTTP consumer. A full bounded
    /// response queue closes only that stream; the shared control reader remains free to process
    /// heartbeats, route grants and unrelated streams.
    async fn dispatch_stream_event(
        &self,
        stream_id: &GatewayConnectionId,
        event: StreamEvent,
        unknown_detail: &'static str,
    ) -> Result<(), TunnelError> {
        if matches!(&event, StreamEvent::End | StreamEvent::Error(_)) {
            return self
                .dispatch_terminal_stream_event(stream_id, event, unknown_detail)
                .await;
        }
        // Keep the tombstone guard through the non-blocking enqueue. A concurrent close either
        // wins first and makes this frame a harmless late delivery, or this lookup observes the
        // still-open sender and admits the frame before close can fence the stream. Cloning the
        // sender and sending after releasing `closed_streams` would let a stale frame enter the
        // queue after close had already removed the stream.
        let send_result = {
            let closed = self.state.closed_streams.lock().await;
            if closed.contains(stream_id) {
                return Ok(());
            }
            let streams = self.state.streams.lock().await;
            let sender = streams
                .get(stream_id)
                .ok_or(TunnelError::Invalid(unknown_detail))?;
            sender.try_send(event)
        };
        match send_result {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.close_stream_state(stream_id, None).await;
                Ok(())
            }
        }
    }

    /// Queues a Central terminal event and fences the stream under one admission gate. The
    /// response worker can consume the already-queued terminal frame after its sender is removed,
    /// while a concurrent Agent input worker cannot enter the gate and emit another frame.
    async fn dispatch_terminal_stream_event(
        &self,
        stream_id: &GatewayConnectionId,
        event: StreamEvent,
        unknown_detail: &'static str,
    ) -> Result<(), TunnelError> {
        let gate = self.stream_admission_gate(stream_id).await;
        let _gate_guard = gate.lock().await;
        let send_result = {
            let closed = self.state.closed_streams.lock().await;
            if closed.contains(stream_id) {
                return Ok(());
            }
            let streams = self.state.streams.lock().await;
            let sender = streams
                .get(stream_id)
                .ok_or(TunnelError::Invalid(unknown_detail))?;
            sender.try_send(event)
        };
        match send_result {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.close_stream_state_with_route(stream_id, None, None)
                    .await;
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.close_stream_state_with_route(stream_id, None, None)
                    .await;
                Ok(())
            }
        }
    }

    async fn send_control(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        message: GatewayControlMessage,
    ) -> Result<(), TunnelError> {
        self.send_control_impl(request_id, trace_id, message, true)
            .await
    }

    async fn send_control_impl(
        &self,
        request_id: RequestId,
        trace_id: Option<TraceId>,
        message: GatewayControlMessage,
        disconnect_on_error: bool,
    ) -> Result<(), TunnelError> {
        if self.is_draining()
            && matches!(
                &message,
                GatewayControlMessage::ReplicaHeartbeat(_)
                    | GatewayControlMessage::RouteAcquire(_)
                    | GatewayControlMessage::RouteRenew(_)
            )
        {
            return Err(TunnelError::Unavailable);
        }
        let now = now_unix_ms();
        // Keep sequence assignment and queue admission under one lock. The enclosing block is
        // intentional: the mutex guard must be released before `disconnect` tries to acquire it
        // again when a bounded output queue times out.
        let (connection_id, result) = {
            let mut link = self.state.link.lock().await;
            // The drain flag may have changed while this sender waited for the serialized control
            // link. Re-check under the same lock that orders sequence assignment and queue
            // admission; otherwise a heartbeat or route mutation could enter the bounded queue
            // after the local fencing boundary.
            if self.is_draining()
                && matches!(
                    &message,
                    GatewayControlMessage::ReplicaHeartbeat(_)
                        | GatewayControlMessage::RouteAcquire(_)
                        | GatewayControlMessage::RouteRenew(_)
                )
            {
                return Err(TunnelError::Unavailable);
            }
            let link = link.as_mut().ok_or(TunnelError::Unavailable)?;
            let is_hello = matches!(&message, GatewayControlMessage::ReplicaHello(_));
            if !link.hello_sent && !is_hello {
                // `open_control` publishes the link before it can enqueue hello.  Reject an Agent
                // or heartbeat frame in that small window instead of allowing it to become
                // sequence 1 and making Central reject the entire control session.
                return Err(TunnelError::Unavailable);
            }
            if link.hello_sent && is_hello {
                return Err(TunnelError::Invalid(
                    "Replica hello was already sent on this control link",
                ));
            }
            let sequence = link.next_sequence;
            link.next_sequence = sequence
                .checked_add(1)
                .ok_or(TunnelError::Invalid("Gateway sequence exhausted"))?;
            let frame = GatewayControlFrame {
                wire_version: CURRENT_WIRE_VERSION,
                gateway_pool_id: self.identity.gateway_pool_id.clone(),
                gateway_replica_id: self.identity.gateway_replica_id.clone(),
                connection_id: link.connection_id.clone(),
                sequence: SequenceNumber::new(sequence),
                request_id,
                trace_id,
                sent_at_unix_ms: now,
                deadline_unix_ms: UnixMillis::new(
                    now.get().saturating_add(CONTROL_FRAME_DEADLINE_MS),
                ),
                hop_count: 0,
                message,
                extensions: neoengram_domain::protocol::Extensions::new(),
            };
            let encoded = Bytes::from(frame.encode_ndjson()?);
            let connection_id = link.connection_id.clone();
            let result = match timeout(
                Duration::from_millis(CONTROL_FRAME_DEADLINE_MS),
                link.output.send(encoded),
            )
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(TunnelError::Closed),
                Err(_) => Err(TunnelError::Deadline),
            };
            if result.is_ok() && is_hello {
                link.hello_sent = true;
            }
            (connection_id, result)
        };
        if disconnect_on_error && result.is_err() {
            self.disconnect(&connection_id).await;
        }
        result
    }

    async fn is_connection(&self, connection_id: &GatewayConnectionId) -> bool {
        self.state
            .link
            .lock()
            .await
            .as_ref()
            .is_some_and(|link| link.connection_id == *connection_id)
    }

    async fn disconnect(&self, connection_id: &GatewayConnectionId) {
        // A stale reader may finish after a replacement control session has been installed.  Do
        // not let that reader clear the replacement's maps; serializing the whole teardown with
        // `open_control` makes the connection-id check and cleanup one lifecycle transaction.
        let _lifecycle_guard = self.state.lifecycle.lock().await;
        let mut link = self.state.link.lock().await;
        if !link
            .as_ref()
            .is_some_and(|link| link.connection_id == *connection_id)
        {
            return;
        }
        *link = None;
        drop(link);
        self.state.pending_unary.lock().await.clear();
        self.state.late_unary.lock().await.clear();
        self.state.pending_routes.lock().await.clear();
        self.state.late_routes.lock().await.clear();
        self.state.stream_requests.lock().await.clear();
        // Dropping the bounded senders closes every response body. Sending a synthetic error here
        // could itself await a full per-stream queue and deadlock session teardown.
        // Keep a bounded tombstone for every stream that was active at the disconnect boundary.
        // A body worker may start after this cleanup and must still observe cancellation instead
        // of creating an unsignalled receiver for the now-defunct session.
        let mut closed_streams = self.state.closed_streams.lock().await;
        let mut streams = self.state.streams.lock().await;
        for stream_id in streams.keys() {
            remember_late(&mut closed_streams, stream_id.clone());
        }
        let _streams = std::mem::take(&mut *streams);
        drop(streams);
        // Keep the tombstone guard while publishing cancellation and removing the senders. A
        // late worker therefore sees either the signalled sender or the already-closed tombstone,
        // never a fresh unsignalled channel in the gap between the two operations.
        let mut cancellations = self.state.stream_cancellations.lock().await;
        for sender in cancellations.values() {
            sender.send_replace(true);
        }
        cancellations.clear();
        drop(cancellations);
        drop(closed_streams);
        drop(_streams);
        self.state.routes.lock().await.clear();
        if let Some(fence) = &self.state.transfer_fence {
            fence.clear_generations();
        }
        // The directory is a lease of Central authority, not durable local configuration. Any
        // disconnect therefore revokes every cached peer credential immediately.
        self.state.peer_directory.lock().await.take();
    }

    async fn remove_stream(&self, request_id: &RequestId, stream_id: &GatewayConnectionId) {
        self.close_stream_state(stream_id, Some(request_id)).await;
    }

    async fn remove_stream_if_route(
        &self,
        request_id: &RequestId,
        stream_id: &GatewayConnectionId,
        expected_route: &ActiveRoute,
    ) {
        self.close_stream_state_if_route(stream_id, Some(request_id), expected_route)
            .await;
    }

    /// Closes one Agent stream with the tombstone as its linearization point. Holding the
    /// tombstone guard while removing all correlated state ensures a later control frame cannot
    /// observe a missing stream as an unknown protocol identity. The pending/late lock order is
    /// shared with route response dispatch.
    async fn close_stream_state(
        &self,
        stream_id: &GatewayConnectionId,
        known_request_id: Option<&RequestId>,
    ) {
        let gate = self.stream_admission_gate(stream_id).await;
        let _gate_guard = gate.lock().await;
        self.close_stream_state_with_route(stream_id, known_request_id, None)
            .await;
    }

    /// Closes a stream only while its route fence still identifies the worker performing the
    /// cleanup. A stale output worker must not tombstone or remove a newer route that reused the
    /// same transport stream ID.
    async fn close_stream_state_if_route(
        &self,
        stream_id: &GatewayConnectionId,
        known_request_id: Option<&RequestId>,
        expected_route: &ActiveRoute,
    ) {
        let gate = self.stream_admission_gate(stream_id).await;
        let _gate_guard = gate.lock().await;
        self.close_stream_state_with_route(stream_id, known_request_id, Some(expected_route))
            .await;
    }

    async fn close_stream_state_with_route(
        &self,
        stream_id: &GatewayConnectionId,
        known_request_id: Option<&RequestId>,
        expected_route: Option<&ActiveRoute>,
    ) {
        let mut closed_streams = self.state.closed_streams.lock().await;
        let mut stream_requests = self.state.stream_requests.lock().await;
        let bound_request_for_stream =
            stream_requests
                .iter()
                .find_map(|(request_id, mapped_stream_id)| {
                    (mapped_stream_id == stream_id).then(|| request_id.clone())
                });
        let stale_orphan = known_request_id
            .is_some_and(|request_id| stream_requests.get(request_id) != Some(stream_id));
        // If this stream ID has already been rebound to another request, a stale worker must not
        // touch the replacement. If it is orphaned (no reverse binding), it is still safe to
        // discard the old sender/route while leaving the newer request's maps untouched.
        if stale_orphan && bound_request_for_stream.is_some() {
            return;
        }
        if let Some(expected_route) = expected_route {
            // Keep both ownership checks under the tombstone guard. This prevents a newer worker
            // from replacing the request binding or route between validation and cleanup.
            let route_matches = self
                .state
                .routes
                .lock()
                .await
                .get(stream_id)
                .is_some_and(|current| current == expected_route);
            if !route_matches {
                return;
            }
        }
        if let Some(sender) = self
            .state
            .stream_cancellations
            .lock()
            .await
            .remove(stream_id)
        {
            sender.send_replace(true);
        }
        remember_late(&mut closed_streams, stream_id.clone());

        let mut request_ids = if stale_orphan {
            BTreeSet::new()
        } else {
            stream_requests
                .iter()
                .filter(|(_, mapped_stream_id)| *mapped_stream_id == stream_id)
                .map(|(request_id, _)| request_id.clone())
                .collect::<BTreeSet<_>>()
        };
        if !stale_orphan {
            if let Some(request_id) = known_request_id {
                request_ids.insert(request_id.clone());
            }
        }

        let mut pending_routes = self.state.pending_routes.lock().await;
        let mut late_routes = self.state.late_routes.lock().await;
        for request_id in &request_ids {
            pending_routes.remove(request_id);
            remember_late(&mut late_routes, request_id.clone());
            if stream_requests.get(request_id) == Some(stream_id) {
                stream_requests.remove(request_id);
            }
        }

        self.state.streams.lock().await.remove(stream_id);
        let mut routes = self.state.routes.lock().await;
        let removed_route = if let Some(expected_route) = expected_route {
            if routes
                .get(stream_id)
                .is_some_and(|current| current == expected_route)
            {
                routes.remove(stream_id)
            } else {
                None
            }
        } else {
            routes.remove(stream_id)
        };
        // A Gateway can carry several Agent streams. The transfer fence must follow only the
        // route being removed; selecting an arbitrary remaining route would mix another Agent's
        // session/mount/route generations into this Agent's ticket checks.
        let removed_fence = removed_route.as_ref().map(|route| {
            (
                route.agent_id.clone(),
                route.session_generation.get(),
                route.route_generation.get(),
            )
        });
        let replacement_route = removed_fence.as_ref().and_then(|(agent_id, _, _)| {
            routes
                .values()
                .filter(|route| &route.agent_id == agent_id)
                .max_by_key(|route| route.route_generation.get())
                .cloned()
        });
        drop(routes);
        if let Some(fence) = &self.state.transfer_fence {
            if let Some(route) = replacement_route {
                // The fence update is monotonic and preserves the mount for a same-session
                // renewal; a newer session intentionally clears mount until its Opened frame.
                fence.set_agent_route_generations(
                    &route.agent_id,
                    route.session_generation.get(),
                    route.route_generation.get(),
                );
            } else if let Some((agent_id, session_generation, route_generation)) = removed_fence {
                fence.clear_agent_generations_if_current(
                    &agent_id,
                    session_generation,
                    route_generation,
                );
            }
        }
    }

    /// Inserts an authoritative local route only while its Agent stream is still open. The lock
    /// order matches `close_stream_state`, preventing a late RouteGranted/renewal from recreating
    /// a route after backpressure has fenced that stream.
    async fn insert_route_if_open(
        &self,
        stream_id: &GatewayConnectionId,
        route: ActiveRoute,
    ) -> bool {
        // Route publication is part of the stream's admission lifecycle. Holding the same gate
        // prevents the bounded gate directory from evicting this stream while the route worker is
        // between its Central grant and local route insertion.
        let gate = self.stream_admission_gate(stream_id).await;
        let _gate_guard = gate.lock().await;
        let closed_streams = self.state.closed_streams.lock().await;
        if closed_streams.contains(stream_id) {
            return false;
        }
        let agent_id = route.agent_id.clone();
        let session_generation = route.session_generation.get();
        let route_generation = route.route_generation.get();
        let mut routes = self.state.routes.lock().await;
        let inserted = match routes.get(stream_id) {
            None => {
                routes.insert(stream_id.clone(), route);
                true
            }
            Some(current)
                if current.agent_id == route.agent_id
                    && current.session_generation == route.session_generation
                    && current.route_generation == route.route_generation
                    && route.lease_expires_at_unix_ms.get()
                        >= current.lease_expires_at_unix_ms.get() =>
            {
                routes.insert(stream_id.clone(), route);
                true
            }
            Some(_) => false,
        };
        drop(routes);
        if inserted {
            if let Some(fence) = &self.state.transfer_fence {
                fence.set_agent_route_generations(&agent_id, session_generation, route_generation);
            }
        }
        inserted
    }

    /// Removes a waiter and records its identity while holding the pending-map lock. The lock
    /// ordering is shared with `dispatch_control`, closing the race where a late frame could be
    /// observed between removal and tombstone insertion.
    async fn expire_unary(&self, stream_id: &GatewayConnectionId) {
        let mut pending = self.state.pending_unary.lock().await;
        pending.remove(stream_id);
        let mut late = self.state.late_unary.lock().await;
        remember_late(&mut late, stream_id.clone());
    }

    async fn expire_route(&self, request_id: &RequestId) {
        let mut pending = self.state.pending_routes.lock().await;
        pending.remove(request_id);
        let mut late = self.state.late_routes.lock().await;
        remember_late(&mut late, request_id.clone());
    }
}

fn remember_late<T>(set: &mut BTreeSet<T>, value: T)
where
    T: Ord + Clone,
{
    if set.len() >= MAX_LATE_CONTROL_RESPONSES {
        if let Some(oldest) = set.iter().next().cloned() {
            set.remove(&oldest);
        }
    }
    set.insert(value);
}

fn route_unavailable(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::RouteUnavailable,
        detail: detail.into(),
        retryable: true,
    }
}

fn resource_exhausted(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::ResourceExhausted,
        detail: detail.into(),
        retryable: true,
    }
}

fn route_fenced(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::RouteFenced,
        detail: detail.into(),
        retryable: false,
    }
}

fn identity_rejected(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::IdentityRejected,
        detail: detail.into(),
        retryable: false,
    }
}

fn protocol_invalid(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::ProtocolInvalid,
        detail: detail.into(),
        retryable: false,
    }
}

fn deadline_exceeded(detail: impl Into<String>) -> GatewayControlError {
    GatewayControlError {
        code: GatewayErrorCode::DeadlineExceeded,
        detail: detail.into(),
        retryable: false,
    }
}

async fn await_peer_forward_replay(
    mut completion: watch::Receiver<
        Option<Result<GatewayPeerForwardAccepted, GatewayControlError>>,
    >,
) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
    loop {
        if let Some(result) = completion.borrow().clone() {
            return result;
        }
        if completion.changed().await.is_err() {
            return Err(route_unavailable(
                "peer forwarding admission was cancelled before completion",
            ));
        }
    }
}

pub(crate) fn full_body(body: Bytes) -> GatewayBody {
    Either::Left(Full::new(body))
}

pub(crate) fn error_response(error: &TunnelError) -> Response<GatewayBody> {
    let mut response = json_response(
        error.status(),
        PROBLEM_CONTENT_TYPE,
        serde_json::json!({
            "type": format!("https://neoengram.dev/problems/{}", error.code().to_ascii_lowercase()),
            "title": "Gateway request failed",
            "status": error.status().as_u16(),
            "code": error.code(),
            "detail": error.to_string(),
            "retryable": error.retryable(),
        }),
    );
    if error.retryable() {
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

pub(crate) fn json_response(
    status: StatusCode,
    content_type: &'static str,
    document: serde_json::Value,
) -> Response<GatewayBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(full_body(Bytes::from(document.to_string())))
        .expect("static Gateway response metadata must be valid")
}

async fn read_agent_open<B>(
    body: &mut B,
) -> Result<(Vec<Bytes>, AgentChannelUpstreamFrame), TunnelError>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    let mut decoder = AgentChannelNdjsonDecoder::new();
    let mut prefix = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| TunnelError::Body)?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        let lines = decoder.push(&bytes)?;
        prefix.push(bytes);
        if let Some(first) = lines.first() {
            return Ok((prefix, AgentChannelUpstreamFrame::decode_json(first)?));
        }
    }
    decoder.finish()?;
    Err(TunnelError::Invalid(
        "Agent control channel ended before channel.open",
    ))
}

async fn collect_bounded<B>(mut body: B, limit: usize) -> Result<Vec<u8>, TunnelError>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| TunnelError::Body)?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(TunnelError::Invalid("request body exceeds its limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn request_agent_id(body: &[u8]) -> Result<neoengram_domain::protocol::AgentId, TunnelError> {
    let value: serde_json::Value = decode_bounded_unique_json(body, body.len())?;
    let agent_id = value
        .get("agent_id")
        .and_then(serde_json::Value::as_str)
        .ok_or(TunnelError::Invalid(
            "authenticated Agent request has no AgentId",
        ))?;
    neoengram_domain::protocol::AgentId::new(agent_id).map_err(TunnelError::Protocol)
}

fn agent_action_limit(action: GatewayAgentAction) -> usize {
    match action {
        GatewayAgentAction::JobMetadataPageStage => {
            MAX_METADATA_PAGE_BYTES.saturating_add(64 * 1024)
        }
        _ => MAX_CONTROL_MESSAGE_BYTES,
    }
}

fn content_length_exceeds(headers: &HeaderMap, limit: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > limit)
}

fn has_content_type(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn request_id(headers: &HeaderMap) -> Result<RequestId, TunnelError> {
    let mut values = headers.get_all(REQUEST_ID_HEADER).iter();
    let Some(value) = values.next() else {
        return fresh_request_id("agent-request");
    };
    if values.next().is_some() {
        return Err(TunnelError::Invalid(
            "x-request-id must appear exactly once",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| TunnelError::Invalid("x-request-id is not ASCII"))?;
    if value.len() > MAX_REQUEST_ID_BYTES {
        return Err(TunnelError::Invalid("x-request-id is too long"));
    }
    RequestId::new(value).map_err(TunnelError::Protocol)
}

fn trace_id(headers: &HeaderMap) -> Option<TraceId> {
    let value = headers.get("traceparent")?.to_str().ok()?;
    let trace = value.split('-').nth(1)?;
    TraceId::new(trace).ok()
}

fn fresh_connection_id(prefix: &str) -> Result<GatewayConnectionId, TunnelError> {
    GatewayConnectionId::new(fresh_id(prefix)?).map_err(TunnelError::Protocol)
}

fn fresh_request_id(prefix: &str) -> Result<RequestId, TunnelError> {
    RequestId::new(fresh_id(prefix)?).map_err(TunnelError::Protocol)
}

fn fresh_id(prefix: &str) -> Result<String, TunnelError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|_| TunnelError::Invalid("secure request identity generation failed"))?;
    let mut value = String::with_capacity(prefix.len() + 1 + random.len() * 2);
    value.push_str(prefix);
    value.push('-');
    for byte in random {
        write!(&mut value, "{byte:02x}")
            .map_err(|_| TunnelError::Invalid("request identity formatting failed"))?;
    }
    Ok(value)
}

fn now_unix_ms() -> UnixMillis {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    UnixMillis::new(u64::try_from(millis).unwrap_or(u64::MAX))
}

fn lease_expiry() -> UnixMillis {
    UnixMillis::new(now_unix_ms().get().saturating_add(AGENT_ROUTE_LEASE_TTL_MS))
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use http_body_util::Full;
    use neoengram_domain::protocol::{
        AgentId, AgentMountId, AgentResourceLifecycleAssignment, AgentResourceLifecycleScope,
        ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId, ContentDigest,
        DecisionGeneration, DeletionId, EdgeClusterId, Extensions, GatewayRouteFence,
        IndexRevision, JobDecision, JobId, JobState, LifecycleAssignmentId, LifecycleGeneration,
        MessageId, MountGeneration, OwnerGeneration, PlacementGeneration, ProjectId,
        PublishDecision, ResourceLifecycleAction, ResourceLifecycleAssignment, ResourceRef,
        StorageVolumeId, TenantId, VolumeMarkerId, WireIndexVersion,
    };

    struct InProcessPeerForwarder {
        owner: Arc<GatewayTunnel>,
        authenticated_source: GatewayReplicaId,
    }

    #[async_trait]
    impl PeerForwarder for InProcessPeerForwarder {
        async fn forward(
            &self,
            _target_peer_endpoint: &str,
            frame: GatewayControlFrame,
        ) -> Result<GatewayPeerForwardAccepted, GatewayControlError> {
            self.owner
                .accept_peer_forward(frame, &self.authenticated_source)
                .await
        }
    }

    fn identity(replica: &str) -> GatewayIdentity {
        GatewayIdentity {
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new(replica).unwrap(),
            software_version: "0.2.0".to_owned(),
        }
    }

    fn tunnel() -> Arc<GatewayTunnel> {
        Arc::new(GatewayTunnel::new(identity("replica-a")))
    }

    struct ErrorBody;

    impl Body for ErrorBody {
        type Data = Bytes;
        type Error = io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Err(io::Error::other("request body failed"))))
        }

        fn is_end_stream(&self) -> bool {
            false
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    fn decision_frame_bytes(session_generation: SessionGeneration) -> Bytes {
        let frame = AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(7),
            message_id: MessageId::new("central-decision-message-1").unwrap(),
            correlation_id: None,
            session_generation,
            sent_at_unix_ms: UnixMillis::new(300),
            central_signature: None,
            message: AgentChannelDownstreamMessage::Decision(JobDecision {
                job_id: JobId::new("central-decision-job-1").unwrap(),
                assignment_id: AssignmentId::new("central-decision-assignment-1").unwrap(),
                assignment_generation: AssignmentGeneration::new(2),
                decision_generation: DecisionGeneration::new(4),
                decision: PublishDecision::Publish {
                    published_index_version: WireIndexVersion {
                        revision: IndexRevision::new(5),
                        digest: ContentDigest::from_bytes([0x45; 32]),
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
                final_state: JobState::Succeeded,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        };
        Bytes::from(frame.encode_ndjson().unwrap())
    }

    fn lifecycle_frame_bytes(session_generation: SessionGeneration) -> Bytes {
        let now = now_unix_ms();
        let frame = AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(8),
            message_id: MessageId::new("central-lifecycle-message-1").unwrap(),
            correlation_id: None,
            session_generation,
            sent_at_unix_ms: now,
            central_signature: None,
            message: AgentChannelDownstreamMessage::LifecycleAssignment(Box::new(
                AgentResourceLifecycleAssignment {
                    assignment: ResourceLifecycleAssignment {
                        assignment_id: LifecycleAssignmentId::new("lifecycle-assignment-1")
                            .unwrap(),
                        tenant_id: TenantId::new("tenant-a").unwrap(),
                        deletion_id: DeletionId::new("deletion-a").unwrap(),
                        resource: ResourceRef::Artifact {
                            project_id: ProjectId::new("project-a").unwrap(),
                            artifact_id: ArtifactId::new("artifact-a").unwrap(),
                        },
                        action: ResourceLifecycleAction::Quarantine,
                        lifecycle_generation: LifecycleGeneration::new(3),
                        request_digest: ContentDigest::from_bytes([0x61; 32]),
                        deadline_unix_ms: UnixMillis::new(now.get().saturating_add(60_000)),
                    },
                    resource_scope: AgentResourceLifecycleScope::Artifact {
                        project_id: ProjectId::new("project-a").unwrap(),
                        artifact_id: ArtifactId::new("artifact-a").unwrap(),
                        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
                        artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                        placement_generation: PlacementGeneration::new(2),
                    },
                    agent_id: AgentId::new("agent-a").unwrap(),
                    edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
                    agent_mount_id: AgentMountId::new("mount-a").unwrap(),
                    volume_marker_id: VolumeMarkerId::new("volume-a").unwrap(),
                    session_generation,
                    mount_generation: MountGeneration::new(4),
                    owner_generation: OwnerGeneration::new(5),
                    extensions: Extensions::new(),
                },
            )),
            extensions: Extensions::new(),
        };
        Bytes::from(frame.encode_ndjson().unwrap())
    }

    fn peer_request(
        source: &str,
        target: &str,
        connection_id: GatewayConnectionId,
        session_generation: SessionGeneration,
        route_generation: RouteGeneration,
        frame: Bytes,
    ) -> GatewayPeerForwardRequest {
        GatewayPeerForwardRequest {
            source_replica_id: GatewayReplicaId::new(source).unwrap(),
            target_replica_id: GatewayReplicaId::new(target).unwrap(),
            target_peer_endpoint: format!("https://{target}.gateway.example"),
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            agent_connection_id: connection_id,
            session_generation,
            route_generation,
            frame: GatewayOpaqueBytes::new(frame.to_vec()).unwrap(),
        }
    }

    fn control_frame(
        replica: &str,
        connection_id: GatewayConnectionId,
        hop_count: u8,
        message: GatewayControlMessage,
    ) -> GatewayControlFrame {
        let now = now_unix_ms();
        GatewayControlFrame {
            wire_version: CURRENT_WIRE_VERSION,
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new(replica).unwrap(),
            connection_id,
            sequence: SequenceNumber::new(1),
            request_id: RequestId::new("peer-forward-request-a").unwrap(),
            trace_id: None,
            sent_at_unix_ms: now,
            deadline_unix_ms: UnixMillis::new(now.get().saturating_add(10_000)),
            hop_count,
            message,
            extensions: Extensions::new(),
        }
    }

    #[tokio::test]
    async fn readiness_tracks_exactly_one_control_session() {
        let tunnel = tunnel();
        assert!(!tunnel.is_ready().await);
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = tunnel.open_control(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(tunnel.is_ready().await);

        let duplicate = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert!(matches!(
            tunnel.open_control(duplicate).await,
            Err(TunnelError::AlreadyConnected)
        ));
    }

    #[tokio::test]
    async fn peer_directory_fences_rotated_credentials_and_clears_on_disconnect() {
        let tunnel = tunnel();
        let source = GatewayReplicaId::new("replica-b").unwrap();
        let fingerprint = ContentDigest::hash(b"replica-b-leaf-v2");
        let now = now_unix_ms();
        tunnel
            .install_peer_directory(GatewayPeerDirectory {
                directory_generation: neoengram_domain::protocol::Generation::new(2),
                issued_at_unix_ms: now,
                expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(5_000)),
                replicas: vec![neoengram_domain::protocol::GatewayPeerDirectoryEntry {
                    gateway_replica_id: source.clone(),
                    certificate_generation: neoengram_domain::protocol::CertificateGeneration::new(
                        2,
                    ),
                    certificate_fingerprint: fingerprint,
                }],
            })
            .await
            .unwrap();
        tunnel
            .authorize_peer_source(&source, &fingerprint)
            .await
            .unwrap();
        assert!(tunnel
            .authorize_peer_source(&source, &ContentDigest::hash(b"revoked-leaf-v1"))
            .await
            .is_err());

        let stale = GatewayPeerDirectory {
            directory_generation: neoengram_domain::protocol::Generation::new(1),
            issued_at_unix_ms: now,
            expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(5_000)),
            replicas: Vec::new(),
        };
        assert!(matches!(
            tunnel.install_peer_directory(stale).await,
            Err(TunnelError::Invalid(_))
        ));
        let duplicate_generation = GatewayPeerDirectory {
            directory_generation: neoengram_domain::protocol::Generation::new(2),
            issued_at_unix_ms: now,
            expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(5_000)),
            replicas: Vec::new(),
        };
        assert!(matches!(
            tunnel.install_peer_directory(duplicate_generation).await,
            Err(TunnelError::Invalid(_))
        ));

        let connection_id = GatewayConnectionId::new("directory-disconnect").unwrap();
        let (output, _receiver) = mpsc::channel(1);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        tunnel.disconnect(&connection_id).await;
        assert!(tunnel
            .authorize_peer_source(&source, &fingerprint)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn closed_stream_cancellation_receiver_does_not_reinsert_tombstone() {
        let tunnel = tunnel();
        let stream_id = GatewayConnectionId::new("closed-cancellation-stream").unwrap();

        tunnel.close_stream_state(&stream_id, None).await;
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
        assert!(!tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .contains_key(&stream_id));

        let receiver = tunnel.stream_cancellation_receiver(&stream_id).await;
        assert!(*receiver.borrow());
        assert!(!tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .contains_key(&stream_id));
    }

    #[tokio::test]
    async fn disconnect_preserves_closed_and_active_stream_cancellation_fences() {
        let tunnel = tunnel();
        let connection_id = GatewayConnectionId::new("disconnect-central-session").unwrap();
        let closed_stream_id = GatewayConnectionId::new("disconnect-closed-stream").unwrap();
        let active_stream_id = GatewayConnectionId::new("disconnect-active-stream").unwrap();
        let (output, _output_receiver) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        let (closed_sender, _closed_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        let (active_sender, _active_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(closed_stream_id.clone(), closed_sender);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(active_stream_id.clone(), active_sender);
        tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .insert(closed_stream_id.clone(), watch::channel(false).0);
        tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .insert(active_stream_id.clone(), watch::channel(false).0);

        // Cleanup removes the closed stream sender first; disconnect must retain its tombstone
        // for a worker that starts after the session has gone away.
        tunnel.close_stream_state(&closed_stream_id, None).await;

        tunnel.disconnect(&connection_id).await;

        let closed_receiver = tunnel.stream_cancellation_receiver(&closed_stream_id).await;
        assert!(*closed_receiver.borrow());
        let active_receiver = tunnel.stream_cancellation_receiver(&active_stream_id).await;
        assert!(*active_receiver.borrow());
        assert!(!tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .contains_key(&closed_stream_id));
        assert!(!tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .contains_key(&active_stream_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&closed_stream_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&active_stream_id));
    }

    #[tokio::test]
    async fn control_session_replacement_waits_for_stale_disconnect_cleanup() {
        let tunnel = tunnel();
        let old_connection = GatewayConnectionId::new("old-central-session").unwrap();
        let old_stream = GatewayConnectionId::new("old-central-stream").unwrap();
        let (old_output, _old_output_receiver) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: old_connection.clone(),
            next_sequence: 1,
            hello_sent: true,
            output: old_output,
        });
        let (old_stream_sender, _old_stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(old_stream.clone(), old_stream_sender);
        tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .insert(old_stream, watch::channel(false).0);

        // Queue teardown first, then a replacement open while the lifecycle guard is held.  The
        // replacement must wait; otherwise the stale teardown can clear its freshly published
        // link and stream maps.
        let lifecycle_guard = tunnel.state.lifecycle.lock().await;
        let disconnect_tunnel = Arc::clone(&tunnel);
        let disconnect_task = tokio::spawn(async move {
            disconnect_tunnel.disconnect(&old_connection).await;
        });
        tokio::task::yield_now().await;

        let (request_sender, request_body) = mpsc::channel(1);
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(StreamingBody::new(request_body))
            .unwrap();
        let open_tunnel = Arc::clone(&tunnel);
        let open_task = tokio::spawn(async move { open_tunnel.open_control(request).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!open_task.is_finished());

        drop(lifecycle_guard);
        disconnect_task.await.unwrap();
        let response = open_task.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(tunnel.is_ready().await);
        assert!(tunnel.state.streams.lock().await.is_empty());
        assert!(tunnel.state.routes.lock().await.is_empty());
        drop(request_sender);
    }

    #[tokio::test]
    async fn hello_encode_failure_can_reenter_disconnect_without_deadlock() {
        let mut identity = identity("replica-a");
        identity.software_version = "x".repeat(MAX_CONTROL_MESSAGE_BYTES);
        let tunnel = Arc::new(GatewayTunnel::new(identity));
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let result = timeout(Duration::from_secs(1), tunnel.open_control(request))
            .await
            .expect("hello failure must not deadlock lifecycle teardown");
        assert!(result.is_err());
        assert!(!tunnel.is_ready().await);
        assert!(tunnel.state.link.lock().await.is_none());
    }

    #[tokio::test]
    async fn control_link_only_allows_replica_hello_before_readiness() {
        let tunnel = tunnel();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        let connection_id = GatewayConnectionId::new("central-handshake-race").unwrap();
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: false,
            output,
        });

        let request = GatewayControlMessage::AgentRequest(GatewayAgentRequest {
            action: GatewayAgentAction::EnrollmentBootstrap,
            stream_id: GatewayConnectionId::new("request-before-hello").unwrap(),
            body: GatewayOpaqueBytes::new(b"{}".to_vec()).unwrap(),
        });
        assert!(matches!(
            tunnel
                .send_control(
                    RequestId::new("request-before-hello").unwrap(),
                    None,
                    request,
                )
                .await,
            Err(TunnelError::Unavailable)
        ));
        assert!(!tunnel.is_ready().await);
        assert!(frames.try_recv().is_err());

        tunnel
            .send_control(
                RequestId::new("replica-hello").unwrap(),
                None,
                GatewayControlMessage::ReplicaHello(GatewayReplicaHello {
                    edge_cluster_id: tunnel.identity.edge_cluster_id.clone(),
                    software_version: tunnel.identity.software_version.clone(),
                    wire_version: CURRENT_WIRE_VERSION,
                    capabilities: neoengram_domain::protocol::gateway_capabilities_v1(),
                }),
            )
            .await
            .unwrap();
        assert!(tunnel.is_ready().await);
        let encoded = frames.recv().await.unwrap();
        let frame = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(frame.connection_id, connection_id);
        assert_eq!(frame.sequence, SequenceNumber::new(1));
        assert!(matches!(
            frame.message,
            GatewayControlMessage::ReplicaHello(_)
        ));
    }

    #[tokio::test]
    async fn drain_announces_to_central_and_fences_route_renewal_without_sockets() {
        let tunnel = tunnel();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        let connection_id = GatewayConnectionId::new("central-drain-test").unwrap();
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });

        let deadline = UnixMillis::new(now_unix_ms().get().saturating_add(10_000));
        tunnel
            .begin_drain(deadline, "test shutdown")
            .await
            .expect("Drain must be queued on an active control link");
        let encoded = frames
            .recv()
            .await
            .expect("Central must receive a Drain frame");
        let frame = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        assert!(matches!(
            frame.message,
            GatewayControlMessage::Drain(GatewayDrain { .. })
        ));
        assert!(tunnel.is_draining());
        assert!(!tunnel.is_ready().await);

        let route = move |route_generation| GatewayRouteLeaseRequest {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-a-connection").unwrap(),
            session_generation: SessionGeneration::new(1),
            route_generation,
            requested_expires_at_unix_ms: deadline,
        };
        assert!(matches!(
            tunnel
                .mutate_route(GatewayControlMessage::RouteAcquire(route(None)))
                .await,
            Err(TunnelError::Unavailable)
        ));
        assert!(matches!(
            tunnel
                .mutate_route(GatewayControlMessage::RouteRenew(route(Some(
                    RouteGeneration::new(1)
                ))))
                .await,
            Err(TunnelError::Unavailable)
        ));

        // RouteRelease remains allowed after the local fence so active leases can be cleaned up
        // before Drain. The response is intentionally absent here; the test only verifies that a
        // release frame is attempted rather than rejected by the acquire/renew fence.
        let release = tokio::spawn({
            let tunnel = tunnel.clone();
            async move {
                tunnel
                    .mutate_route(GatewayControlMessage::RouteRelease(route(Some(
                        RouteGeneration::new(1),
                    ))))
                    .await
            }
        });
        let encoded = timeout(Duration::from_secs(1), frames.recv())
            .await
            .expect("RouteRelease must be emitted")
            .expect("control output must remain open");
        let frame = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        assert!(matches!(
            frame.message,
            GatewayControlMessage::RouteRelease(_)
        ));
        release.abort();
        let _ = release.await;
    }

    #[tokio::test]
    async fn drain_fence_is_rechecked_after_a_sender_waits_for_the_control_link() {
        let tunnel = tunnel();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: GatewayConnectionId::new("central-drain-race").unwrap(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });

        // Hold the serialized link while the sender passes its first (pre-lock) check. The
        // shutdown fence is then raised before the sender can assign a sequence or enqueue data.
        let link_guard = tunnel.state.link.lock().await;
        let sender_tunnel = tunnel.clone();
        let sender = tokio::spawn(async move {
            sender_tunnel
                .send_control(
                    RequestId::new("drain-race-renew").unwrap(),
                    None,
                    GatewayControlMessage::RouteRenew(GatewayRouteLeaseRequest {
                        agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                        owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
                        agent_connection_id: GatewayConnectionId::new("agent-a-connection")
                            .unwrap(),
                        session_generation: SessionGeneration::new(1),
                        route_generation: Some(RouteGeneration::new(1)),
                        requested_expires_at_unix_ms: lease_expiry(),
                    }),
                )
                .await
        });
        for _ in 0..100 {
            tokio::task::yield_now().await;
            if !sender.is_finished() {
                break;
            }
        }
        assert!(
            !sender.is_finished(),
            "sender should be waiting for the link lock"
        );
        tunnel.state.draining.store(true, Ordering::Release);
        drop(link_guard);

        assert!(matches!(
            sender.await.unwrap(),
            Err(TunnelError::Unavailable)
        ));
        assert!(
            frames.try_recv().is_err(),
            "fenced route renewal must not be queued"
        );
    }

    #[tokio::test]
    async fn duplicate_agent_request_ids_never_replace_the_original_waiter() {
        let tunnel = tunnel();
        let request_id = RequestId::new("agent-stream-request").unwrap();
        let first_stream = GatewayConnectionId::new("agent-stream-a").unwrap();
        let second_stream = GatewayConnectionId::new("agent-stream-b").unwrap();
        let (first_sender, _first_receiver) = oneshot::channel();
        tunnel
            .reserve_stream_request(&request_id, &first_stream, first_sender)
            .await
            .unwrap();
        let (second_sender, _second_receiver) = oneshot::channel();
        assert!(matches!(
            tunnel
                .reserve_stream_request(&request_id, &second_stream, second_sender)
                .await,
            Err(TunnelError::Invalid(_))
        ));
        assert_eq!(
            tunnel.state.stream_requests.lock().await.get(&request_id),
            Some(&first_stream)
        );
        assert!(tunnel
            .state
            .pending_routes
            .lock()
            .await
            .contains_key(&request_id));
    }

    #[tokio::test]
    async fn failed_agent_input_closes_stream_route_and_pending_request() {
        let tunnel = tunnel();
        let request_id = RequestId::new("failed-input-request").unwrap();
        let stream_id = GatewayConnectionId::new("failed-input-stream").unwrap();
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        tunnel.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation: SessionGeneration::new(1),
                route_generation: RouteGeneration::new(1),
                lease_expires_at_unix_ms: lease_expiry(),
            },
        );
        let (_route_sender, route_receiver) = oneshot::channel();
        tunnel
            .state
            .pending_routes
            .lock()
            .await
            .insert(request_id.clone(), _route_sender);

        let error = tunnel
            .forward_agent_input(
                request_id.clone(),
                None,
                stream_id.clone(),
                Vec::new(),
                ErrorBody,
            )
            .await
            .expect_err("body failure must be returned to the request task");
        assert!(matches!(error, TunnelError::Body));
        assert!(!tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert!(!tunnel.state.routes.lock().await.contains_key(&stream_id));
        assert!(!tunnel
            .state
            .stream_requests
            .lock()
            .await
            .contains_key(&request_id));
        assert!(!tunnel
            .state
            .pending_routes
            .lock()
            .await
            .contains_key(&request_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
        assert!(tunnel.state.late_routes.lock().await.contains(&request_id));
        drop(route_receiver);
    }

    #[tokio::test]
    async fn fenced_output_worker_blocks_late_agent_input_data_and_end() {
        let tunnel = tunnel();
        let request_id = RequestId::new("cancelled-input-request").unwrap();
        let stream_id = GatewayConnectionId::new("cancelled-input-stream").unwrap();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: GatewayConnectionId::new("cancelled-input-central").unwrap(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());

        let (body_sender, body_receiver) = mpsc::channel(2);
        let input_tunnel = tunnel.clone();
        let input_request_id = request_id.clone();
        let input_stream_id = stream_id.clone();
        let input_task = tokio::spawn(async move {
            input_tunnel
                .forward_agent_input(
                    input_request_id,
                    None,
                    input_stream_id,
                    Vec::new(),
                    StreamingBody::new(body_receiver),
                )
                .await
        });

        body_sender
            .send(Bytes::from_static(b"first"))
            .await
            .unwrap();
        let first = timeout(Duration::from_secs(1), frames.recv())
            .await
            .unwrap()
            .unwrap();
        let first = GatewayControlFrame::decode_json(&first[..first.len() - 1]).unwrap();
        assert!(matches!(
            first.message,
            GatewayControlMessage::AgentStreamData(_)
        ));

        tunnel
            .close_stream_state(&stream_id, Some(&request_id))
            .await;
        body_sender.send(Bytes::from_static(b"late")).await.unwrap();
        drop(body_sender);

        let result = timeout(Duration::from_secs(1), input_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TunnelError::Closed)));
        assert!(frames.try_recv().is_err());
    }

    #[tokio::test]
    async fn stream_close_cancels_an_agent_input_body_that_is_still_pending() {
        let tunnel = tunnel();
        let request_id = RequestId::new("pending-input-cancel-request").unwrap();
        let stream_id = GatewayConnectionId::new("pending-input-cancel-stream").unwrap();
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        let (body_sender, body_receiver) = mpsc::channel(1);
        let input_tunnel = tunnel.clone();
        let input_request_id = request_id.clone();
        let input_stream_id = stream_id.clone();
        let input_task = tokio::spawn(async move {
            input_tunnel
                .forward_agent_input(
                    input_request_id,
                    None,
                    input_stream_id,
                    Vec::new(),
                    StreamingBody::new(body_receiver),
                )
                .await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        tunnel
            .close_stream_state(&stream_id, Some(&request_id))
            .await;
        let result = timeout(Duration::from_secs(1), input_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TunnelError::Closed)));
        drop(body_sender);
    }

    #[tokio::test]
    async fn agent_input_admission_cannot_enter_after_close_wins_gate() {
        let tunnel = tunnel();
        let request_id = RequestId::new("close-first-input-request").unwrap();
        let stream_id = GatewayConnectionId::new("close-first-input-stream").unwrap();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: GatewayConnectionId::new("close-first-input-central").unwrap(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());

        let gate = tunnel.stream_admission_gate(&stream_id).await;
        let gate_guard = gate.lock().await;
        let (close_started, close_ready) = oneshot::channel();
        let close_tunnel = tunnel.clone();
        let close_stream_id = stream_id.clone();
        let close_request_id = request_id.clone();
        let close_task = tokio::spawn(async move {
            close_started.send(()).unwrap();
            close_tunnel
                .close_stream_state(&close_stream_id, Some(&close_request_id))
                .await;
        });
        close_ready.await.unwrap();
        tokio::task::yield_now().await;

        let send_tunnel = tunnel.clone();
        let send_stream_id = stream_id.clone();
        let send_request_id = request_id.clone();
        let send_task = tokio::spawn(async move {
            send_tunnel
                .send_agent_stream_control(
                    send_request_id,
                    None,
                    &send_stream_id,
                    GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                        stream_id: send_stream_id.clone(),
                        chunk: GatewayOpaqueBytes::new(b"late".to_vec()).unwrap(),
                    }),
                )
                .await
        });
        tokio::task::yield_now().await;
        drop(gate_guard);

        close_task.await.unwrap();
        assert!(matches!(send_task.await.unwrap(), Err(TunnelError::Closed)));
        assert!(frames.try_recv().is_err());
    }

    #[tokio::test]
    async fn stale_stream_cleanup_cannot_remove_a_reused_request_id() {
        let tunnel = tunnel();
        let request_id = RequestId::new("reused-stream-request").unwrap();
        let old_stream = GatewayConnectionId::new("old-stream").unwrap();
        let new_stream = GatewayConnectionId::new("new-stream").unwrap();
        let (old_sender, _old_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        let (new_sender, _new_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        {
            let mut streams = tunnel.state.streams.lock().await;
            streams.insert(old_stream.clone(), old_sender);
            streams.insert(new_stream.clone(), new_sender);
        }
        // The old worker has already lost its directory entry, while the same request ID now
        // belongs to a newly admitted stream with a live route waiter.
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), new_stream.clone());
        let (route_sender, _route_receiver) = oneshot::channel();
        tunnel
            .state
            .pending_routes
            .lock()
            .await
            .insert(request_id.clone(), route_sender);
        tunnel.state.routes.lock().await.insert(
            new_stream.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation: SessionGeneration::new(1),
                route_generation: RouteGeneration::new(1),
                lease_expires_at_unix_ms: lease_expiry(),
            },
        );

        tunnel
            .close_stream_state(&old_stream, Some(&request_id))
            .await;

        assert!(!tunnel.state.streams.lock().await.contains_key(&old_stream));
        assert!(tunnel.state.streams.lock().await.contains_key(&new_stream));
        assert_eq!(
            tunnel.state.stream_requests.lock().await.get(&request_id),
            Some(&new_stream)
        );
        assert!(tunnel
            .state
            .pending_routes
            .lock()
            .await
            .contains_key(&request_id));
        assert!(tunnel.state.routes.lock().await.contains_key(&new_stream));
    }

    #[tokio::test]
    async fn stale_worker_cannot_write_or_close_a_stream_rebound_to_another_request() {
        let tunnel = tunnel();
        let old_request = RequestId::new("old-reused-stream-request").unwrap();
        let current_request = RequestId::new("current-reused-stream-request").unwrap();
        let stream_id = GatewayConnectionId::new("reused-transport-stream").unwrap();
        let (control_output, mut control_frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: GatewayConnectionId::new("reused-stream-central").unwrap(),
            next_sequence: 1,
            hello_sent: true,
            output: control_output,
        });
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(current_request.clone(), stream_id.clone());
        tunnel
            .state
            .stream_cancellations
            .lock()
            .await
            .insert(stream_id.clone(), watch::channel(false).0);
        let current_route = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            route_generation: RouteGeneration::new(4),
            lease_expires_at_unix_ms: lease_expiry(),
        };
        tunnel
            .state
            .routes
            .lock()
            .await
            .insert(stream_id.clone(), current_route.clone());

        tunnel
            .close_stream_state(&stream_id, Some(&old_request))
            .await;
        let error = tunnel
            .send_agent_stream_control(
                old_request,
                None,
                &stream_id,
                GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                    stream_id: stream_id.clone(),
                    chunk: GatewayOpaqueBytes::new(b"stale".to_vec()).unwrap(),
                }),
            )
            .await
            .expect_err("a stale request binding must not enter the replacement stream");

        assert!(matches!(error, TunnelError::Closed));
        assert!(control_frames.try_recv().is_err());
        assert!(tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert_eq!(
            tunnel
                .state
                .stream_requests
                .lock()
                .await
                .get(&current_request),
            Some(&stream_id)
        );
        assert_eq!(
            tunnel.state.routes.lock().await.get(&stream_id),
            Some(&current_route)
        );
        assert!(!tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
    }

    #[tokio::test]
    async fn route_insert_rejects_identity_replacement_but_allows_same_fence_renewal() {
        let tunnel = tunnel();
        let stream_id = GatewayConnectionId::new("route-reuse-stream").unwrap();
        let now = now_unix_ms();
        let original = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            route_generation: RouteGeneration::new(4),
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(1_000)),
        };
        assert!(
            tunnel
                .insert_route_if_open(&stream_id, original.clone())
                .await
        );

        let replacement = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-b").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(5),
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(2_000)),
        };
        assert!(!tunnel.insert_route_if_open(&stream_id, replacement).await);
        assert_eq!(
            tunnel.state.routes.lock().await.get(&stream_id),
            Some(&original)
        );

        let renewed = ActiveRoute {
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(3_000)),
            ..original.clone()
        };
        assert!(
            tunnel
                .insert_route_if_open(&stream_id, renewed.clone())
                .await
        );
        assert_eq!(
            tunnel.state.routes.lock().await.get(&stream_id),
            Some(&renewed)
        );
    }

    #[tokio::test]
    async fn gate_eviction_skips_bound_routes_requests_inflight_and_pending_state() {
        let tunnel = tunnel();
        let request_stream = GatewayConnectionId::new("a-request-bound-stream").unwrap();
        let route_stream = GatewayConnectionId::new("b-route-bound-stream").unwrap();
        let inflight_stream = GatewayConnectionId::new("c-inflight-stream").unwrap();
        let request_id = RequestId::new("gate-eviction-request").unwrap();

        let request_gate = Arc::new(Mutex::new(()));
        let route_gate = Arc::new(Mutex::new(()));
        let inflight_gate = Arc::new(Mutex::new(()));
        tunnel
            .state
            .stream_admission_gates
            .lock()
            .await
            .insert(request_stream.clone(), request_gate);
        tunnel
            .state
            .stream_admission_gates
            .lock()
            .await
            .insert(route_stream.clone(), route_gate);
        tunnel
            .state
            .stream_admission_gates
            .lock()
            .await
            .insert(inflight_stream.clone(), inflight_gate);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id, request_stream.clone());
        tunnel.state.routes.lock().await.insert(
            route_stream.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("gate-eviction-agent").unwrap(),
                session_generation: SessionGeneration::new(1),
                route_generation: RouteGeneration::new(1),
                lease_expires_at_unix_ms: lease_expiry(),
            },
        );
        // Holding an Arc returned to a worker protects the entry even before its state maps are
        // published, which is the open-initialization window this directory must cover.
        let inflight_gate = tunnel.stream_admission_gate(&inflight_stream).await;

        {
            let mut gates = tunnel.state.stream_admission_gates.lock().await;
            for index in 0..MAX_STREAM_ADMISSION_GATES {
                let id = GatewayConnectionId::new(format!("idle-gate-{index:04}")).unwrap();
                gates.entry(id).or_insert_with(|| Arc::new(Mutex::new(())));
            }
        }
        let replacement = GatewayConnectionId::new("replacement-gate").unwrap();
        let _replacement_gate = tunnel.stream_admission_gate(&replacement).await;
        let gates = tunnel.state.stream_admission_gates.lock().await;
        assert!(gates.contains_key(&request_stream));
        assert!(gates.contains_key(&route_stream));
        assert!(gates.contains_key(&inflight_stream));
        drop(gates);
        drop(inflight_gate);

        // A pending route is keyed by RequestId, so its stream binding must also protect the
        // corresponding gate while the route grant is in flight.
        let pending_stream = GatewayConnectionId::new("d-pending-stream").unwrap();
        tunnel
            .state
            .stream_admission_gates
            .lock()
            .await
            .insert(pending_stream.clone(), Arc::new(Mutex::new(())));
        let pending_request = RequestId::new("gate-eviction-pending").unwrap();
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(pending_request.clone(), pending_stream.clone());
        let (pending_sender, _pending_receiver) = oneshot::channel();
        tunnel
            .state
            .pending_routes
            .lock()
            .await
            .insert(pending_request, pending_sender);
        let pending_replacement = GatewayConnectionId::new("pending-replacement-gate").unwrap();
        let _pending_gate = tunnel.stream_admission_gate(&pending_replacement).await;
        assert!(tunnel
            .state
            .stream_admission_gates
            .lock()
            .await
            .contains_key(&pending_stream));
    }

    #[tokio::test]
    async fn stale_route_cleanup_does_not_remove_a_reused_stream_route() {
        let tunnel = tunnel();
        let request_id = RequestId::new("stale-route-cleanup-request").unwrap();
        let stream_id = GatewayConnectionId::new("stale-route-cleanup-stream").unwrap();
        let (sender, _receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        let now = now_unix_ms();
        let stale_route = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(2),
            route_generation: RouteGeneration::new(4),
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(1_000)),
        };
        let current_route = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-b").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(5),
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(2_000)),
        };
        tunnel
            .state
            .routes
            .lock()
            .await
            .insert(stream_id.clone(), current_route.clone());

        // The old worker still owns the request binding but its route fence no longer matches.
        // Its conditional cleanup must leave the newer route and stream state untouched.
        tunnel
            .close_stream_state_if_route(&stream_id, Some(&request_id), &stale_route)
            .await;
        assert_eq!(
            tunnel.state.routes.lock().await.get(&stream_id),
            Some(&current_route)
        );
        assert!(tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert_eq!(
            tunnel.state.stream_requests.lock().await.get(&request_id),
            Some(&stream_id)
        );
        assert!(!tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));

        tunnel
            .close_stream_state_if_route(&stream_id, Some(&request_id), &current_route)
            .await;
        assert!(!tunnel.state.routes.lock().await.contains_key(&stream_id));
        assert!(!tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
    }

    #[tokio::test]
    async fn authenticated_agent_unary_actions_require_http2() {
        let tunnel = tunnel();
        let request = Request::builder()
            .method(Method::POST)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .body(Full::new(Bytes::from_static(b"{}")))
            .unwrap();
        let error = match tunnel
            .forward_agent_unary(
                request,
                GatewayAgentAction::SessionOpen,
                Some(neoengram_domain::protocol::AgentId::new("agent-a").unwrap()),
                1024,
                Duration::from_secs(1),
            )
            .await
        {
            Ok(_) => panic!("authenticated unary actions must reject HTTP/1"),
            Err(error) => error,
        };
        assert!(matches!(error, TunnelError::Invalid(message) if message.contains("HTTP/2")));
    }

    #[tokio::test]
    async fn late_unary_response_after_timeout_does_not_close_the_control_link() {
        let tunnel = tunnel();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        let connection_id = GatewayConnectionId::new("central-late-unary").unwrap();
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        let request = Request::builder()
            .method(Method::POST)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .body(Full::new(Bytes::from_static(b"{}")))
            .unwrap();
        assert!(matches!(
            tunnel
                .forward_agent_unary(
                    request,
                    GatewayAgentAction::EnrollmentBootstrap,
                    None,
                    1024,
                    Duration::from_millis(5),
                )
                .await,
            Err(TunnelError::Deadline)
        ));
        let encoded = frames.recv().await.unwrap();
        let outbound = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        let GatewayControlMessage::AgentRequest(request) = outbound.message else {
            panic!("expected Agent unary request")
        };
        assert!(tunnel.state.pending_unary.lock().await.is_empty());
        assert!(tunnel
            .state
            .late_unary
            .lock()
            .await
            .contains(&request.stream_id));

        let mut response = control_frame(
            "replica-a",
            connection_id,
            0,
            GatewayControlMessage::AgentResponse(GatewayAgentResponse {
                stream_id: request.stream_id.clone(),
                status: 200,
                content_type: JSON_CONTENT_TYPE.to_owned(),
                retry_after_ms: None,
                body: GatewayOpaqueBytes::new(b"{}".to_vec()).unwrap(),
            }),
        );
        response.request_id = outbound.request_id;
        tunnel.dispatch_control(response).await.unwrap();
        assert!(tunnel.state.late_unary.lock().await.is_empty());
        assert!(tunnel.state.link.lock().await.is_some());
    }

    #[tokio::test]
    async fn late_route_grant_after_timeout_does_not_close_the_control_link() {
        let tunnel = tunnel();
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        let connection_id = GatewayConnectionId::new("central-late-route").unwrap();
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: connection_id.clone(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });
        let now = now_unix_ms();
        let route = GatewayRouteLeaseRequest {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-late-route").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: None,
            requested_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
        };
        assert!(matches!(
            tunnel
                .mutate_route_with_timeout(
                    GatewayControlMessage::RouteAcquire(route.clone()),
                    Duration::from_millis(5),
                )
                .await,
            Err(TunnelError::Deadline)
        ));
        let encoded = frames.recv().await.unwrap();
        let outbound = GatewayControlFrame::decode_json(&encoded[..encoded.len() - 1]).unwrap();
        assert!(tunnel.state.pending_routes.lock().await.is_empty());
        assert!(tunnel
            .state
            .late_routes
            .lock()
            .await
            .contains(&outbound.request_id));

        let mut response = control_frame(
            "replica-a",
            connection_id,
            0,
            GatewayControlMessage::RouteGranted(GatewayRouteLeaseGranted {
                agent_id: route.agent_id,
                owner_replica_id: route.owner_replica_id,
                agent_connection_id: route.agent_connection_id,
                session_generation: route.session_generation,
                route_generation: RouteGeneration::new(1),
                lease_expires_at_unix_ms: route.requested_expires_at_unix_ms,
                replayed: false,
            }),
        );
        response.request_id = outbound.request_id;
        tunnel.dispatch_control(response).await.unwrap();
        assert!(tunnel.state.late_routes.lock().await.is_empty());
        assert!(tunnel.state.link.lock().await.is_some());
    }

    #[tokio::test]
    async fn full_agent_queue_fences_only_that_stream_and_discards_late_control_frames() {
        let tunnel = tunnel();
        let request_id = RequestId::new("full-agent-stream-request").unwrap();
        let stream_id = GatewayConnectionId::new("full-agent-stream").unwrap();
        let now = now_unix_ms();
        let route = ActiveRoute {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(7),
            lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
        };
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        for _ in 0..STREAM_EVENT_BUFFER {
            stream_sender.try_send(StreamEvent::End).unwrap();
        }
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .routes
            .lock()
            .await
            .insert(stream_id.clone(), route.clone());
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        let (route_sender, route_receiver) = oneshot::channel();
        tunnel
            .state
            .pending_routes
            .lock()
            .await
            .insert(request_id.clone(), route_sender);

        tunnel
            .dispatch_stream_event(
                &stream_id,
                StreamEvent::Data(Bytes::from_static(b"late")),
                "unknown Agent stream response",
            )
            .await
            .unwrap();
        assert!(route_receiver.await.is_err());
        assert!(tunnel.state.streams.lock().await.is_empty());
        assert!(tunnel.state.routes.lock().await.is_empty());
        assert!(tunnel.state.stream_requests.lock().await.is_empty());
        assert!(tunnel.state.pending_routes.lock().await.is_empty());
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
        assert!(tunnel.state.late_routes.lock().await.contains(&request_id));

        let connection_id = GatewayConnectionId::new("full-agent-control").unwrap();
        let mut grant = control_frame(
            "replica-a",
            connection_id.clone(),
            0,
            GatewayControlMessage::RouteGranted(GatewayRouteLeaseGranted {
                agent_id: route.agent_id,
                owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
                agent_connection_id: stream_id.clone(),
                session_generation: route.session_generation,
                route_generation: route.route_generation,
                lease_expires_at_unix_ms: route.lease_expires_at_unix_ms,
                replayed: false,
            }),
        );
        grant.request_id = request_id.clone();
        tunnel.dispatch_control(grant).await.unwrap();
        assert!(tunnel.state.routes.lock().await.is_empty());

        let mut data = control_frame(
            "replica-a",
            connection_id.clone(),
            0,
            GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                stream_id: stream_id.clone(),
                chunk: GatewayOpaqueBytes::new(b"late data".to_vec()).unwrap(),
            }),
        );
        data.request_id = request_id.clone();
        tunnel.dispatch_control(data).await.unwrap();

        let mut end = control_frame(
            "replica-a",
            connection_id.clone(),
            0,
            GatewayControlMessage::AgentStreamEnd(GatewayAgentStreamEnd {
                stream_id: stream_id.clone(),
            }),
        );
        end.request_id = request_id.clone();
        tunnel.dispatch_control(end).await.unwrap();

        let mut error = control_frame(
            "replica-a",
            connection_id,
            0,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::ResourceExhausted,
                detail: "late stream backpressure".to_owned(),
                retryable: true,
            }),
        );
        error.request_id = request_id.clone();
        tunnel.dispatch_control(error).await.unwrap();
        assert!(!tunnel.state.late_routes.lock().await.contains(&request_id));
    }

    #[tokio::test]
    async fn central_stream_delivery_cannot_enter_after_close_wins_the_fence() {
        let tunnel = tunnel();
        let request_id = RequestId::new("central-stream-fence-request").unwrap();
        let stream_id = GatewayConnectionId::new("central-stream-fence").unwrap();
        let (sender, mut receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());

        // Hold the tombstone mutex while both operations queue behind it. The close task is
        // started first, so releasing the guard deterministically lets close establish the fence
        // before dispatch can clone or enqueue through the old sender.
        let closed_guard = tunnel.state.closed_streams.lock().await;
        let (close_started, close_ready) = oneshot::channel();
        let close_tunnel = tunnel.clone();
        let close_stream_id = stream_id.clone();
        let close_request_id = request_id.clone();
        let close_task = tokio::spawn(async move {
            close_started.send(()).unwrap();
            close_tunnel
                .close_stream_state(&close_stream_id, Some(&close_request_id))
                .await;
        });
        close_ready.await.unwrap();
        tokio::task::yield_now().await;

        let dispatch_tunnel = tunnel.clone();
        let dispatch_stream_id = stream_id.clone();
        let dispatch_task = tokio::spawn(async move {
            dispatch_tunnel
                .dispatch_stream_event(
                    &dispatch_stream_id,
                    StreamEvent::Data(Bytes::from_static(b"late")),
                    "unknown Agent stream response",
                )
                .await
        });
        tokio::task::yield_now().await;
        drop(closed_guard);

        close_task.await.unwrap();
        assert!(matches!(dispatch_task.await.unwrap(), Ok(())));
        assert!(receiver.recv().await.is_none());
        assert!(!tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert!(!tunnel
            .state
            .stream_requests
            .lock()
            .await
            .contains_key(&request_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
    }

    #[tokio::test]
    async fn central_terminal_event_fences_agent_input_before_worker_consumes_it() {
        let tunnel = tunnel();
        let request_id = RequestId::new("central-terminal-fence-request").unwrap();
        let stream_id = GatewayConnectionId::new("central-terminal-fence-stream").unwrap();
        let (stream_sender, mut stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        let (output, mut frames) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        *tunnel.state.link.lock().await = Some(CentralLink {
            connection_id: GatewayConnectionId::new("central-terminal-fence-link").unwrap(),
            next_sequence: 1,
            hello_sent: true,
            output,
        });

        let gate = tunnel.stream_admission_gate(&stream_id).await;
        let gate_guard = gate.lock().await;
        let (terminal_started, terminal_ready) = oneshot::channel();
        let terminal_tunnel = tunnel.clone();
        let terminal_stream_id = stream_id.clone();
        let terminal_task = tokio::spawn(async move {
            terminal_started.send(()).unwrap();
            terminal_tunnel
                .dispatch_stream_event(
                    &terminal_stream_id,
                    StreamEvent::End,
                    "unknown Agent stream end",
                )
                .await
        });
        terminal_ready.await.unwrap();
        tokio::task::yield_now().await;

        let send_tunnel = tunnel.clone();
        let send_stream_id = stream_id.clone();
        let send_request_id = request_id.clone();
        let send_task = tokio::spawn(async move {
            send_tunnel
                .send_agent_stream_control(
                    send_request_id,
                    None,
                    &send_stream_id,
                    GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                        stream_id: send_stream_id.clone(),
                        chunk: GatewayOpaqueBytes::new(b"after-end".to_vec()).unwrap(),
                    }),
                )
                .await
        });
        tokio::task::yield_now().await;
        drop(gate_guard);

        assert!(matches!(terminal_task.await.unwrap(), Ok(())));
        assert!(matches!(send_task.await.unwrap(), Err(TunnelError::Closed)));
        assert!(matches!(
            stream_receiver.recv().await,
            Some(StreamEvent::End)
        ));
        assert!(stream_receiver.recv().await.is_none());
        assert!(frames.try_recv().is_err());
    }

    #[tokio::test]
    async fn route_error_fences_initial_agent_stream_before_late_input() {
        let tunnel = tunnel();
        let request_id = RequestId::new("route-error-fence-request").unwrap();
        let stream_id = GatewayConnectionId::new("route-error-fence-stream").unwrap();
        let (stream_sender, _stream_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), stream_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(request_id.clone(), stream_id.clone());
        let (route_sender, route_receiver) = oneshot::channel();
        tunnel
            .state
            .pending_routes
            .lock()
            .await
            .insert(request_id.clone(), route_sender);

        tunnel
            .dispatch_error(
                &request_id,
                GatewayControlError {
                    code: GatewayErrorCode::RouteUnavailable,
                    detail: "route rejected".to_owned(),
                    retryable: true,
                },
            )
            .await
            .unwrap();
        assert!(route_receiver.await.unwrap().is_err());
        assert!(!tunnel.state.streams.lock().await.contains_key(&stream_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&stream_id));
        assert!(matches!(
            tunnel
                .send_agent_stream_control(
                    request_id,
                    None,
                    &stream_id,
                    GatewayControlMessage::AgentStreamEnd(GatewayAgentStreamEnd {
                        stream_id: stream_id.clone(),
                    }),
                )
                .await,
            Err(TunnelError::Closed)
        ));
    }

    #[tokio::test]
    async fn agent_stream_data_and_end_require_the_open_request_id() {
        let tunnel = tunnel();
        let connection_id = GatewayConnectionId::new("request-binding-control").unwrap();
        let bound_request_id = RequestId::new("request-binding-bound").unwrap();
        let wrong_request_id = RequestId::new("request-binding-wrong").unwrap();

        let data_stream_id = GatewayConnectionId::new("request-binding-data").unwrap();
        let (data_sender, _data_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(data_stream_id.clone(), data_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(bound_request_id.clone(), data_stream_id.clone());

        let mut data = control_frame(
            "replica-a",
            connection_id.clone(),
            0,
            GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                stream_id: data_stream_id.clone(),
                chunk: GatewayOpaqueBytes::new(b"mismatched data".to_vec()).unwrap(),
            }),
        );
        data.request_id = wrong_request_id.clone();
        let data_error = tunnel
            .dispatch_control(data)
            .await
            .expect_err("mismatched Agent data request ID must fail");
        assert!(matches!(
            data_error,
            TunnelError::Invalid("Agent stream frame request ID does not match its bound request")
        ));
        assert!(!tunnel
            .state
            .streams
            .lock()
            .await
            .contains_key(&data_stream_id));
        assert!(!tunnel
            .state
            .stream_requests
            .lock()
            .await
            .contains_key(&bound_request_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&data_stream_id));

        let end_stream_id = GatewayConnectionId::new("request-binding-end").unwrap();
        let end_bound_request_id = RequestId::new("request-binding-end-bound").unwrap();
        let (end_sender, _end_receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        tunnel
            .state
            .streams
            .lock()
            .await
            .insert(end_stream_id.clone(), end_sender);
        tunnel
            .state
            .stream_requests
            .lock()
            .await
            .insert(end_bound_request_id.clone(), end_stream_id.clone());

        let mut end = control_frame(
            "replica-a",
            connection_id,
            0,
            GatewayControlMessage::AgentStreamEnd(GatewayAgentStreamEnd {
                stream_id: end_stream_id.clone(),
            }),
        );
        end.request_id = wrong_request_id;
        let end_error = tunnel
            .dispatch_control(end)
            .await
            .expect_err("mismatched Agent end request ID must fail");
        assert!(matches!(
            end_error,
            TunnelError::Invalid("Agent stream frame request ID does not match its bound request")
        ));
        assert!(!tunnel
            .state
            .streams
            .lock()
            .await
            .contains_key(&end_stream_id));
        assert!(!tunnel
            .state
            .stream_requests
            .lock()
            .await
            .contains_key(&end_bound_request_id));
        assert!(tunnel
            .state
            .closed_streams
            .lock()
            .await
            .contains(&end_stream_id));
    }

    #[tokio::test]
    async fn route_fence_closes_all_matching_streams_without_affecting_newer_or_other_agents() {
        let tunnel = tunnel();
        let now = now_unix_ms();
        let fenced_generation = RouteGeneration::new(5);
        let entries = [
            ("agent-a-old-1", "agent-a", 3_u64, true),
            ("agent-a-old-2", "agent-a", 5_u64, true),
            ("agent-a-current", "agent-a", 6_u64, false),
            ("agent-b-other", "agent-b", 1_u64, false),
        ];
        let mut receivers = BTreeMap::new();
        for (stream_name, agent_name, generation, should_fence) in entries {
            let stream_id = GatewayConnectionId::new(stream_name).unwrap();
            let (sender, receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
            tunnel
                .state
                .streams
                .lock()
                .await
                .insert(stream_id.clone(), sender);
            tunnel.state.routes.lock().await.insert(
                stream_id.clone(),
                ActiveRoute {
                    agent_id: neoengram_domain::protocol::AgentId::new(agent_name).unwrap(),
                    session_generation: SessionGeneration::new(3),
                    route_generation: RouteGeneration::new(generation),
                    lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
                },
            );
            receivers.insert(stream_id, (should_fence, receiver));
        }

        tunnel
            .dispatch_control(control_frame(
                "replica-a",
                GatewayConnectionId::new("central-route-fence").unwrap(),
                0,
                GatewayControlMessage::RouteFence(GatewayRouteFence {
                    agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                    route_generation: fenced_generation,
                    reason: "takeover".to_owned(),
                }),
            ))
            .await
            .unwrap();

        for (_stream_id, (should_fence, mut receiver)) in receivers {
            if should_fence {
                let Some(StreamEvent::Error(error)) = receiver.recv().await else {
                    panic!("fenced stream must receive a terminal route error")
                };
                assert_eq!(error.code, GatewayErrorCode::RouteFenced);
                assert_eq!(error.detail, "takeover");
                assert!(receiver.recv().await.is_none());
            } else {
                assert!(receiver.try_recv().is_err());
            }
        }

        let routes = tunnel.state.routes.lock().await;
        assert_eq!(routes.len(), 2);
        assert!(routes.contains_key(&GatewayConnectionId::new("agent-a-current").unwrap()));
        assert!(routes.contains_key(&GatewayConnectionId::new("agent-b-other").unwrap()));
        drop(routes);
        let closed = tunnel.state.closed_streams.lock().await;
        assert!(closed.contains(&GatewayConnectionId::new("agent-a-old-1").unwrap()));
        assert!(closed.contains(&GatewayConnectionId::new("agent-a-old-2").unwrap()));
        assert!(!closed.contains(&GatewayConnectionId::new("agent-a-current").unwrap()));
        assert!(!closed.contains(&GatewayConnectionId::new("agent-b-other").unwrap()));
    }

    #[tokio::test]
    async fn transfer_route_removal_does_not_mix_agent_generations() {
        let identity = identity("replica-transfer-fence");
        let fence = QuicTransferFence::new(
            identity.gateway_pool_id.clone(),
            identity.edge_cluster_id.clone(),
        );
        let tunnel = Arc::new(GatewayTunnel::with_peer_forwarder_and_transfer_fence(
            identity,
            Arc::new(UnavailablePeerForwarder),
            Some(fence.clone()),
        ));
        let agent_a = AgentId::new("transfer-agent-a").unwrap();
        let agent_b = AgentId::new("transfer-agent-b").unwrap();
        let old_stream = GatewayConnectionId::new("transfer-agent-a-old").unwrap();
        let current_stream = GatewayConnectionId::new("transfer-agent-a-current").unwrap();
        let other_stream = GatewayConnectionId::new("transfer-agent-b-current").unwrap();
        let old_route = ActiveRoute {
            agent_id: agent_a.clone(),
            session_generation: SessionGeneration::new(4),
            route_generation: RouteGeneration::new(7),
            lease_expires_at_unix_ms: lease_expiry(),
        };
        let current_route = ActiveRoute {
            agent_id: agent_a.clone(),
            session_generation: SessionGeneration::new(5),
            route_generation: RouteGeneration::new(8),
            lease_expires_at_unix_ms: lease_expiry(),
        };
        let other_route = ActiveRoute {
            agent_id: agent_b.clone(),
            session_generation: SessionGeneration::new(9),
            route_generation: RouteGeneration::new(3),
            lease_expires_at_unix_ms: lease_expiry(),
        };
        tunnel.state.routes.lock().await.extend([
            (old_stream.clone(), old_route.clone()),
            (current_stream.clone(), current_route.clone()),
            (other_stream, other_route.clone()),
        ]);
        fence.set_agent_generations(&agent_a, 5, 11, 8);
        fence.set_agent_generations(&agent_b, 9, 12, 3);

        tunnel
            .close_stream_state_if_route(&old_stream, None, &old_route)
            .await;
        assert_eq!(fence.agent_generations(&agent_a), Some((5, 11, 8)));
        assert_eq!(fence.agent_generations(&agent_b), Some((9, 12, 3)));

        tunnel
            .close_stream_state_if_route(&old_stream, None, &old_route)
            .await;
        assert_eq!(fence.agent_generations(&agent_a), Some((5, 11, 8)));
        assert_eq!(fence.agent_generations(&agent_b), Some((9, 12, 3)));

        tunnel
            .close_stream_state_if_route(&current_stream, None, &current_route)
            .await;
        assert_eq!(fence.agent_generations(&agent_a), None);
        assert_eq!(fence.agent_generations(&agent_b), Some((9, 12, 3)));
    }

    #[tokio::test]
    async fn first_gateway_frame_is_a_direct_hello() {
        let tunnel = tunnel();
        let (request_sender, request_body) = mpsc::channel(1);
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(StreamingBody::new(request_body))
            .unwrap();
        let mut response = tunnel.open_control(request).await.unwrap();
        let bytes = response
            .body_mut()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let frame = GatewayControlFrame::decode_json(&bytes[..bytes.len() - 1]).unwrap();
        assert_eq!(frame.sequence, SequenceNumber::new(1));
        assert_eq!(frame.hop_count, 0);
        assert!(matches!(
            frame.message,
            GatewayControlMessage::ReplicaHello(_)
        ));
        drop(request_sender);
    }

    #[tokio::test]
    async fn two_replicas_forward_exactly_one_hop_to_the_fenced_owner() {
        let owner = Arc::new(GatewayTunnel::new(identity("replica-b")));
        let stream_id = GatewayConnectionId::new("agent-owner-connection").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (agent_sender, mut agent_receiver) = mpsc::channel(1);
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), agent_sender);

        let source = Arc::new(GatewayTunnel::with_peer_forwarder(
            identity("replica-a"),
            Arc::new(InProcessPeerForwarder {
                owner: owner.clone(),
                authenticated_source: GatewayReplicaId::new("replica-a").unwrap(),
            }),
        ));
        let (central_sender, central_body) = mpsc::channel(1);
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, NDJSON_CONTENT_TYPE)
            .body(StreamingBody::new(central_body))
            .unwrap();
        let mut response = source.open_control(request).await.unwrap();
        let hello = response
            .body_mut()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let hello = GatewayControlFrame::decode_json(&hello[..hello.len() - 1]).unwrap();
        let downstream = decision_frame_bytes(session_generation);
        let forward = peer_request(
            "replica-a",
            "replica-b",
            stream_id.clone(),
            session_generation,
            route_generation,
            downstream.clone(),
        );
        let central_frame = control_frame(
            "replica-a",
            hello.connection_id,
            0,
            GatewayControlMessage::PeerForward(forward),
        );
        central_sender
            .send(Bytes::from(central_frame.encode_ndjson().unwrap()))
            .await
            .unwrap();

        let delivered = timeout(Duration::from_secs(1), agent_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let StreamEvent::Data(delivered) = delivered else {
            panic!("expected forwarded Agent data")
        };
        assert_eq!(delivered, downstream);
        let acknowledgement = timeout(Duration::from_secs(1), response.body_mut().frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let acknowledgement =
            GatewayControlFrame::decode_json(&acknowledgement[..acknowledgement.len() - 1])
                .unwrap();
        assert_eq!(acknowledgement.hop_count, 0);
        let GatewayControlMessage::PeerForwardAccepted(accepted) = acknowledgement.message else {
            panic!("expected peer forwarding acknowledgement")
        };
        assert_eq!(
            accepted.target_replica_id,
            GatewayReplicaId::new("replica-b").unwrap()
        );
        assert!(source.state.routes.lock().await.is_empty());
        assert_eq!(owner.state.routes.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn peer_forwarding_delivers_a_fenced_lifecycle_assignment() {
        let owner = GatewayTunnel::new(identity("replica-b"));
        let stream_id = GatewayConnectionId::new("agent-lifecycle-owner-connection").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (sender, mut receiver) = mpsc::channel(1);
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);

        let downstream = lifecycle_frame_bytes(session_generation);
        let request = peer_request(
            "replica-a",
            "replica-b",
            stream_id,
            session_generation,
            route_generation,
            downstream.clone(),
        );
        let frame = control_frame(
            "replica-a",
            GatewayConnectionId::new("peer-lifecycle-connection").unwrap(),
            1,
            GatewayControlMessage::PeerForward(request),
        );

        owner
            .accept_peer_forward(frame, &GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap();
        let Some(StreamEvent::Data(delivered)) = receiver.recv().await else {
            panic!("expected a forwarded lifecycle assignment")
        };
        assert_eq!(delivered, downstream);
        let decoded = AgentChannelDownstreamFrame::decode_json(
            &delivered[..delivered.len().saturating_sub(1)],
        )
        .unwrap();
        assert!(matches!(
            decoded.message,
            AgentChannelDownstreamMessage::LifecycleAssignment(_)
        ));
    }

    #[tokio::test]
    async fn peer_delivery_cannot_enter_a_stream_after_close_wins_the_fence() {
        let owner = tunnel();
        let stream_id = GatewayConnectionId::new("peer-fence-race-stream").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (sender, mut receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);

        let request = peer_request(
            "replica-b",
            "replica-a",
            stream_id.clone(),
            session_generation,
            route_generation,
            decision_frame_bytes(session_generation),
        );
        // The direct delivery helper is exercised here so the test can hold the tombstone mutex
        // and deterministically queue close before delivery. Wire-level validation is covered by
        // the peer forwarding tests above; this race concerns only local owner state.
        let encoded = Bytes::from(request.frame.as_bytes().to_vec());
        let deadline = UnixMillis::new(now.get().saturating_add(10_000));
        let closed_guard = owner.state.closed_streams.lock().await;
        let (close_started, close_ready) = oneshot::channel();
        let close_owner = owner.clone();
        let close_stream_id = stream_id.clone();
        let close_task = tokio::spawn(async move {
            close_started.send(()).unwrap();
            close_owner.close_stream_state(&close_stream_id, None).await;
        });
        close_ready.await.unwrap();
        tokio::task::yield_now().await;

        let deliver_owner = owner.clone();
        let deliver_request = request.clone();
        let deliver_task = tokio::spawn(async move {
            deliver_owner
                .deliver_peer_forward(&deliver_request, deadline, &encoded)
                .await
        });
        tokio::task::yield_now().await;
        drop(closed_guard);

        close_task.await.unwrap();
        let result = deliver_task.await.unwrap();
        assert!(matches!(
            result,
            Err(GatewayControlError {
                code: GatewayErrorCode::RouteFenced,
                ..
            })
        ));
        assert!(receiver.recv().await.is_none());
        assert!(!owner.state.routes.lock().await.contains_key(&stream_id));
        assert!(!owner.state.streams.lock().await.contains_key(&stream_id));
    }

    #[tokio::test]
    async fn concurrent_exact_peer_forward_replays_deliver_only_once() {
        let owner = Arc::new(GatewayTunnel::new(identity("replica-b")));
        let stream_id = GatewayConnectionId::new("agent-owner-concurrent-replay").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(StreamEvent::End).await.unwrap();
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);

        let downstream = decision_frame_bytes(session_generation);
        let request = peer_request(
            "replica-a",
            "replica-b",
            stream_id,
            session_generation,
            route_generation,
            downstream.clone(),
        );
        let frame = control_frame(
            "replica-a",
            GatewayConnectionId::new("peer-concurrent-replay").unwrap(),
            1,
            GatewayControlMessage::PeerForward(request),
        );
        let request_id = frame.request_id.clone();
        let first_owner = owner.clone();
        let first_frame = frame.clone();
        let first = tokio::spawn(async move {
            first_owner
                .accept_peer_forward(first_frame, &GatewayReplicaId::new("replica-a").unwrap())
                .await
        });
        timeout(Duration::from_secs(1), async {
            loop {
                if owner
                    .state
                    .peer_forward_seen
                    .lock()
                    .await
                    .contains_key(&request_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let replay_owner = owner.clone();
        let mut replay = tokio::spawn(async move {
            replay_owner
                .accept_peer_forward(frame, &GatewayReplicaId::new("replica-a").unwrap())
                .await
        });
        assert!(timeout(Duration::from_millis(25), &mut replay)
            .await
            .is_err());
        assert!(matches!(receiver.recv().await, Some(StreamEvent::End)));
        let first = first.await.unwrap().unwrap();
        let replay = replay.await.unwrap().unwrap();
        assert_eq!(first, replay);
        let Some(StreamEvent::Data(delivered)) = receiver.recv().await else {
            panic!("expected exactly one forwarded Agent frame")
        };
        assert_eq!(delivered, downstream);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn concurrent_peer_forward_replay_with_different_payload_is_rejected() {
        let owner = Arc::new(GatewayTunnel::new(identity("replica-b")));
        let stream_id = GatewayConnectionId::new("agent-owner-payload-replay").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(StreamEvent::End).await.unwrap();
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);

        let downstream = decision_frame_bytes(session_generation);
        let request = peer_request(
            "replica-a",
            "replica-b",
            stream_id.clone(),
            session_generation,
            route_generation,
            downstream.clone(),
        );
        let frame = control_frame(
            "replica-a",
            GatewayConnectionId::new("peer-payload-replay").unwrap(),
            1,
            GatewayControlMessage::PeerForward(request),
        );
        let request_id = frame.request_id.clone();
        let first_owner = owner.clone();
        let first = tokio::spawn(async move {
            first_owner
                .accept_peer_forward(frame, &GatewayReplicaId::new("replica-a").unwrap())
                .await
        });
        timeout(Duration::from_secs(1), async {
            loop {
                if owner
                    .state
                    .peer_forward_seen
                    .lock()
                    .await
                    .contains_key(&request_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut altered =
            AgentChannelDownstreamFrame::decode_json(&downstream[..downstream.len() - 1]).unwrap();
        altered.message_id = MessageId::new("central-decision-message-2").unwrap();
        let altered = Bytes::from(altered.encode_ndjson().unwrap());
        let different = control_frame(
            "replica-a",
            GatewayConnectionId::new("peer-payload-replay-altered").unwrap(),
            1,
            GatewayControlMessage::PeerForward(peer_request(
                "replica-a",
                "replica-b",
                stream_id,
                session_generation,
                route_generation,
                altered,
            )),
        );
        let error = timeout(
            Duration::from_secs(1),
            owner.accept_peer_forward(different, &GatewayReplicaId::new("replica-a").unwrap()),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, GatewayErrorCode::IdentityRejected);

        assert!(matches!(receiver.recv().await, Some(StreamEvent::End)));
        first.await.unwrap().unwrap();
        let Some(StreamEvent::Data(delivered)) = receiver.recv().await else {
            panic!("expected the originally admitted Agent frame")
        };
        assert_eq!(delivered, downstream);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn expired_peer_forward_replay_entries_are_pruned_before_admission() {
        let owner = GatewayTunnel::new(identity("replica-b"));
        let connection_id = GatewayConnectionId::new("peer-replay-expired-connection").unwrap();
        let request = peer_request(
            "replica-a",
            "replica-b",
            connection_id,
            SessionGeneration::new(3),
            RouteGeneration::new(5),
            decision_frame_bytes(SessionGeneration::new(3)),
        );
        let binding = PeerForwardReplayBinding::from_request(&request);
        let expired_request_id = RequestId::new("peer-replay-expired").unwrap();
        let (completion, receiver) = watch::channel(None);
        drop(completion);
        owner.state.peer_forward_seen.lock().await.insert(
            expired_request_id.clone(),
            PeerForwardReplay {
                binding: binding.clone(),
                expires_at_unix_ms: UnixMillis::new(now_unix_ms().get().saturating_sub(1)),
                reservation: Arc::new(()),
                completion: receiver,
            },
        );

        let replacement_request_id = RequestId::new("peer-replay-replacement").unwrap();
        let admission = owner
            .admit_peer_forward(
                &replacement_request_id,
                binding,
                UnixMillis::new(now_unix_ms().get().saturating_add(10_000)),
            )
            .await
            .expect("an expired completed replay entry must not consume admission capacity");
        assert!(matches!(admission, PeerForwardAdmission::Execute { .. }));
        let seen = owner.state.peer_forward_seen.lock().await;
        assert!(!seen.contains_key(&expired_request_id));
        assert!(seen.contains_key(&replacement_request_id));
    }

    #[tokio::test]
    async fn peer_forwarding_rejects_second_hop_wrong_san_and_stale_generation() {
        let owner = GatewayTunnel::new(identity("replica-b"));
        let stream_id = GatewayConnectionId::new("agent-owner-connection").unwrap();
        let session_generation = SessionGeneration::new(3);
        let route_generation = RouteGeneration::new(5);
        let now = now_unix_ms();
        owner.state.routes.lock().await.insert(
            stream_id.clone(),
            ActiveRoute {
                agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                session_generation,
                route_generation,
                lease_expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(30_000)),
            },
        );
        let (sender, mut receiver) = mpsc::channel(1);
        owner
            .state
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), sender);
        let request = peer_request(
            "replica-a",
            "replica-b",
            stream_id.clone(),
            session_generation,
            route_generation,
            decision_frame_bytes(session_generation),
        );
        let connection_id = GatewayConnectionId::new("peer-connection-a").unwrap();

        let mut second_hop = control_frame(
            "replica-a",
            connection_id.clone(),
            1,
            GatewayControlMessage::PeerForward(request.clone()),
        );
        second_hop.hop_count = 2;
        let error = owner
            .accept_peer_forward(second_hop, &GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.code, GatewayErrorCode::ProtocolInvalid);

        let wrong_san = control_frame(
            "replica-a",
            connection_id.clone(),
            1,
            GatewayControlMessage::PeerForward(request.clone()),
        );
        let error = owner
            .accept_peer_forward(wrong_san, &GatewayReplicaId::new("replica-c").unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.code, GatewayErrorCode::IdentityRejected);

        let mut stale = request.clone();
        stale.route_generation = RouteGeneration::new(4);
        let stale = control_frame(
            "replica-a",
            connection_id,
            1,
            GatewayControlMessage::PeerForward(stale),
        );
        let error = owner
            .accept_peer_forward(stale, &GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.code, GatewayErrorCode::RouteFenced);

        let valid = control_frame(
            "replica-a",
            GatewayConnectionId::new("peer-connection-valid").unwrap(),
            1,
            GatewayControlMessage::PeerForward(request),
        );
        let mut different_route = valid.clone();
        let GatewayControlMessage::PeerForward(different_route_request) =
            &mut different_route.message
        else {
            panic!("expected peer forward request")
        };
        different_route_request.route_generation = RouteGeneration::new(6);
        let first = owner
            .accept_peer_forward(valid.clone(), &GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap();
        let replayed = owner
            .accept_peer_forward(valid, &GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap();
        assert_eq!(first, replayed);
        let error = owner
            .accept_peer_forward(
                different_route,
                &GatewayReplicaId::new("replica-a").unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, GatewayErrorCode::IdentityRejected);
        assert!(matches!(receiver.recv().await, Some(StreamEvent::Data(_))));
        assert!(receiver.try_recv().is_err());
    }
}
