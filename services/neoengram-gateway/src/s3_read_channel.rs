//! Dedicated full-duplex Agent/Gateway S3 object channel.
//!
//! The Agent opens one workload-mTLS HTTP/2 POST. Gateway-to-Agent `ReadOpen`/`ReadCancel`
//! frames travel in the response body, while `ReadHead`/`ReadData`/`ReadEnd`/`ReadError` travel in
//! the request body. Object bytes never enter the JSON/NDJSON control tunnel.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use http::{
    header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE},
    Method, Request, Response, StatusCode, Version,
};
use http_body_util::{BodyExt as _, Either, Full};
use hyper::body::Body;
use neoengram_domain::protocol::{
    AgentId, ContentDigest, GatewayConnectionId, GatewayPoolId, GatewayReplicaId,
    GatewayS3ReadRevocation, LifecycleGeneration, ResourceVersion, RouteGeneration, S3ReadCancel,
    S3ReadChannelDecoder, S3ReadChannelFrame, S3ReadChannelHello, S3ReadChannelReady, S3ReadData,
    S3ReadEnd, S3ReadError, S3ReadFrame, S3ReadHead, S3ReadOpen, S3ReadTicket, SessionGeneration,
    UnixMillis, S3_READ_CHANNEL_CONTENT_TYPE, S3_READ_FRAME_MAX_BYTES,
};
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::{
    public_listener::{BoxError, S3ControlError},
    s3_backend::{S3ObjectRead, S3ObjectReader},
    tunnel::{GatewayBody, GatewayTunnel, StreamingBody},
};

const CHANNEL_OUTPUT_BUFFER: usize = 64;
const OBJECT_OUTPUT_BUFFER: usize = 8;
const CHANNEL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const ROUTE_FENCE_CHECK_INTERVAL: Duration = Duration::from_millis(250);
const MAX_CANCELLED_STREAMS: usize = 1024;

#[derive(Clone)]
pub(crate) struct S3ReadChannelRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    gateway_pool_id: GatewayPoolId,
    tunnel: Arc<GatewayTunnel>,
    peer_reader: Arc<dyn S3PeerReader>,
    sessions: Mutex<BTreeMap<(AgentId, SessionGeneration), Arc<AgentReadSession>>>,
    revocations: Mutex<S3RevocationState>,
    revocation_receiver: Mutex<Option<broadcast::Receiver<GatewayS3ReadRevocation>>>,
    revocation_listener_started: AtomicBool,
    next_connection_id: AtomicU64,
}

#[derive(Default)]
struct S3RevocationState {
    snapshot_minimums: BTreeMap<(String, String), LifecycleGeneration>,
    access_point_minimums: BTreeMap<(String, String), ResourceVersion>,
    fail_closed: bool,
}

impl S3RevocationState {
    fn apply(&mut self, revocation: &GatewayS3ReadRevocation) {
        let tenant_id = revocation.tenant_id.to_string();
        let snapshot_key = (tenant_id.clone(), revocation.snapshot_id.to_string());
        let snapshot_minimum = self
            .snapshot_minimums
            .entry(snapshot_key)
            .or_insert(revocation.minimum_snapshot_lifecycle_generation);
        if snapshot_minimum.get() < revocation.minimum_snapshot_lifecycle_generation.get() {
            *snapshot_minimum = revocation.minimum_snapshot_lifecycle_generation;
        }
        let access_point_key = (tenant_id, revocation.bucket.clone());
        let policy_minimum = self
            .access_point_minimums
            .entry(access_point_key)
            .or_insert(revocation.minimum_access_point_policy_generation);
        if policy_minimum.get() < revocation.minimum_access_point_policy_generation.get() {
            *policy_minimum = revocation.minimum_access_point_policy_generation;
        }
    }

    fn rejects(&self, scope: &S3ReadRevocationScope) -> bool {
        self.fail_closed
            || self
                .snapshot_minimums
                .get(&(scope.tenant_id.clone(), scope.snapshot_id.clone()))
                .is_some_and(|minimum| scope.snapshot_lifecycle_generation.get() < minimum.get())
            || self
                .access_point_minimums
                .get(&(scope.tenant_id.clone(), scope.bucket.clone()))
                .is_some_and(|minimum| scope.access_point_policy_generation.get() < minimum.get())
    }
}

#[async_trait::async_trait]
pub(crate) trait S3PeerReader: Send + Sync {
    async fn open_peer(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError>;
}

#[cfg(test)]
struct UnavailableS3PeerReader;

#[cfg(test)]
#[async_trait::async_trait]
impl S3PeerReader for UnavailableS3PeerReader {
    async fn open_peer(&self, _ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
        Err(S3ControlError::Unavailable)
    }
}

impl std::fmt::Debug for S3ReadChannelRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ReadChannelRegistry")
            .field("gateway_pool_id", &self.inner.gateway_pool_id)
            .finish_non_exhaustive()
    }
}

struct AgentReadSession {
    connection_id: u64,
    agent_id: AgentId,
    session_generation: SessionGeneration,
    agent_connection_id: GatewayConnectionId,
    route_generation: RouteGeneration,
    tunnel: Arc<GatewayTunnel>,
    outgoing: Mutex<Option<mpsc::Sender<Bytes>>>,
    streams: Mutex<BTreeMap<String, ActiveRead>>,
    cancelled_streams: Mutex<BTreeSet<String>>,
    alive: AtomicBool,
}

struct ActiveRead {
    sender: mpsc::Sender<Result<Bytes, BoxError>>,
    revocation_scope: S3ReadRevocationScope,
    expected_offset: u64,
    end_exclusive: u64,
    size_bytes: u64,
    etag: neoengram_domain::protocol::ContentDigest,
    head_received: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3ReadRevocationScope {
    tenant_id: String,
    snapshot_id: String,
    snapshot_lifecycle_generation: LifecycleGeneration,
    bucket: String,
    access_point_policy_generation: ResourceVersion,
}

impl From<&S3ReadTicket> for S3ReadRevocationScope {
    fn from(ticket: &S3ReadTicket) -> Self {
        Self {
            tenant_id: ticket.tenant_id.clone(),
            snapshot_id: ticket.snapshot_id.clone(),
            snapshot_lifecycle_generation: ticket.snapshot_lifecycle_generation,
            bucket: ticket.bucket.clone(),
            access_point_policy_generation: ticket.access_point_policy_generation,
        }
    }
}

impl AgentReadSession {
    fn new(
        connection_id: u64,
        hello: &S3ReadChannelHello,
        agent_connection_id: GatewayConnectionId,
        route_generation: RouteGeneration,
        outgoing: mpsc::Sender<Bytes>,
        tunnel: Arc<GatewayTunnel>,
    ) -> Self {
        Self {
            connection_id,
            agent_id: hello.agent_id.clone(),
            session_generation: hello.session_generation,
            agent_connection_id,
            route_generation,
            tunnel,
            outgoing: Mutex::new(Some(outgoing)),
            streams: Mutex::new(BTreeMap::new()),
            cancelled_streams: Mutex::new(BTreeSet::new()),
            alive: AtomicBool::new(true),
        }
    }

    async fn send(&self, frame: S3ReadChannelFrame) -> Result<(), ()> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(());
        }
        let bytes = frame.encode().map(Bytes::from).map_err(|_| ())?;
        let sender = self.outgoing.lock().await.clone().ok_or(())?;
        sender.send(bytes).await.map_err(|_| ())
    }

    async fn try_send(&self, frame: S3ReadChannelFrame) -> Result<(), ()> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(());
        }
        let bytes = frame.encode().map(Bytes::from).map_err(|_| ())?;
        let sender = self.outgoing.lock().await.clone().ok_or(())?;
        sender.try_send(bytes).map_err(|_| ())
    }

    async fn close(&self, detail: &'static str) {
        if !self.alive.swap(false, Ordering::AcqRel) {
            return;
        }
        self.outgoing.lock().await.take();
        let streams = std::mem::take(&mut *self.streams.lock().await);
        for (_, stream) in streams {
            let _ = stream.sender.try_send(Err(stream_error(detail)));
        }
    }

    async fn remember_cancelled(&self, stream_id: String) {
        let mut cancelled = self.cancelled_streams.lock().await;
        while cancelled.len() >= MAX_CANCELLED_STREAMS {
            let Some(oldest) = cancelled.first().cloned() else {
                break;
            };
            cancelled.remove(&oldest);
        }
        cancelled.insert(stream_id);
    }

    async fn was_cancelled(&self, stream_id: &str) -> bool {
        self.cancelled_streams.lock().await.contains(stream_id)
    }

    async fn route_is_current(&self) -> bool {
        self.tunnel
            .agent_route_fence_is_current(
                &self.agent_id,
                self.session_generation,
                &self.agent_connection_id,
                self.route_generation,
            )
            .await
    }

    async fn try_send_cancel(&self, stream_id: String) {
        let Ok(bytes) = S3ReadChannelFrame::Read(S3ReadFrame::Cancel(S3ReadCancel { stream_id }))
            .encode()
            .map(Bytes::from)
        else {
            return;
        };
        if let Some(sender) = self.outgoing.lock().await.as_ref() {
            let _ = sender.try_send(bytes);
        }
    }
}

impl S3ReadChannelRegistry {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(tunnel: Arc<GatewayTunnel>) -> Self {
        Self::with_peer_reader(tunnel, Arc::new(UnavailableS3PeerReader))
    }

    #[must_use]
    pub(crate) fn with_peer_reader(
        tunnel: Arc<GatewayTunnel>,
        peer_reader: Arc<dyn S3PeerReader>,
    ) -> Self {
        let revocations = tunnel.subscribe_s3_read_revocations();
        let registry = Self {
            inner: Arc::new(RegistryInner {
                gateway_pool_id: tunnel.identity().gateway_pool_id.clone(),
                tunnel,
                peer_reader,
                sessions: Mutex::new(BTreeMap::new()),
                revocations: Mutex::new(S3RevocationState::default()),
                revocation_receiver: Mutex::new(Some(revocations)),
                revocation_listener_started: AtomicBool::new(false),
                next_connection_id: AtomicU64::new(1),
            }),
        };
        registry
    }

    async fn ensure_revocation_listener(&self) {
        if self
            .inner
            .revocation_listener_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let Some(mut receiver) = self.inner.revocation_receiver.lock().await.take() else {
            self.fail_closed_revocations().await;
            return;
        };
        let inner = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(revocation) => {
                        let Some(inner) = inner.upgrade() else {
                            return;
                        };
                        S3ReadChannelRegistry { inner }
                            .apply_revocation(revocation)
                            .await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let Some(inner) = inner.upgrade() else {
                            return;
                        };
                        tracing::error!(
                            skipped,
                            "S3 read revocation receiver lagged; fencing all reads"
                        );
                        S3ReadChannelRegistry { inner }
                            .fail_closed_revocations()
                            .await;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
    }

    async fn apply_revocation(&self, revocation: GatewayS3ReadRevocation) {
        let cancelled = {
            let mut state = self.inner.revocations.lock().await;
            state.apply(&revocation);
            self.remove_revoked_streams(&state).await
        };
        cancel_revoked_streams(cancelled, "S3 read authorization was revoked").await;
    }

    async fn fail_closed_revocations(&self) {
        let cancelled = {
            let mut state = self.inner.revocations.lock().await;
            state.fail_closed = true;
            self.remove_revoked_streams(&state).await
        };
        cancel_revoked_streams(cancelled, "S3 read revocation state is incomplete").await;
    }

    async fn remove_revoked_streams(
        &self,
        state: &S3RevocationState,
    ) -> Vec<(Arc<AgentReadSession>, String, ActiveRead)> {
        let sessions = self
            .inner
            .sessions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut cancelled = Vec::new();
        for session in sessions {
            let mut streams = session.streams.lock().await;
            let stream_ids = streams
                .iter()
                .filter(|(_, stream)| state.rejects(&stream.revocation_scope))
                .map(|(stream_id, _)| stream_id.clone())
                .collect::<Vec<_>>();
            for stream_id in stream_ids {
                if let Some(stream) = streams.remove(&stream_id) {
                    cancelled.push((session.clone(), stream_id, stream));
                }
            }
        }
        cancelled
    }

    /// Accepts one Agent-initiated binary H2 channel after the listener has authenticated the
    /// workload certificate as `peer_agent_id`.
    pub(crate) async fn open_channel<B>(
        &self,
        request: Request<B>,
        peer_agent_id: Option<AgentId>,
    ) -> Response<GatewayBody>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        self.ensure_revocation_listener().await;
        if request.method() != Method::POST
            || request.version() != Version::HTTP_2
            || !request
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| has_content_type(value, S3_READ_CHANNEL_CONTENT_TYPE))
        {
            return channel_problem(
                StatusCode::BAD_REQUEST,
                "S3 read channel requires an HTTP/2 binary POST",
            );
        }

        let mut body = request.into_body();
        let handshake =
            tokio::time::timeout(CHANNEL_HANDSHAKE_TIMEOUT, read_hello(&mut body)).await;
        let (hello, decoder, pending) = match handshake {
            Ok(Ok(value)) => value,
            Ok(Err(detail)) => return channel_problem(StatusCode::BAD_REQUEST, detail),
            Err(_) => {
                return channel_problem(
                    StatusCode::GATEWAY_TIMEOUT,
                    "S3 read channel handshake timed out",
                );
            }
        };
        if peer_agent_id
            .as_ref()
            .is_some_and(|peer_agent_id| &hello.agent_id != peer_agent_id)
            || hello.session_generation.get() == 0
        {
            return channel_problem(
                StatusCode::FORBIDDEN,
                "S3 read channel identity does not match the workload certificate",
            );
        }
        let Some((agent_connection_id, route_generation)) = self
            .inner
            .tunnel
            .current_agent_route_fence(&hello.agent_id, hello.session_generation)
            .await
        else {
            return channel_problem(
                StatusCode::CONFLICT,
                "S3 read channel session is not the current Agent route",
            );
        };

        let (outgoing, response_body) = mpsc::channel(CHANNEL_OUTPUT_BUFFER);
        let ready = S3ReadChannelFrame::Ready(S3ReadChannelReady {
            agent_id: hello.agent_id.clone(),
            session_generation: hello.session_generation,
        });
        let ready = match ready.encode() {
            Ok(frame) => Bytes::from(frame),
            Err(_) => {
                return channel_problem(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "S3 read channel acknowledgement failed",
                );
            }
        };
        if outgoing.try_send(ready).is_err() {
            return channel_problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "S3 read channel output is unavailable",
            );
        }
        let connection_id = self
            .inner
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(AgentReadSession::new(
            connection_id,
            &hello,
            agent_connection_id,
            route_generation,
            outgoing,
            self.inner.tunnel.clone(),
        ));
        self.install(session.clone()).await;

        let registry = self.clone();
        tokio::spawn(async move {
            let result = registry
                .read_agent_frames(session.clone(), body, decoder, pending)
                .await;
            if let Err(detail) = result {
                tracing::warn!(
                    agent_id = %session.agent_id,
                    session_generation = session.session_generation.get(),
                    %detail,
                    "Agent S3 read channel closed"
                );
            }
            registry.remove(&session).await;
        });

        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, S3_READ_CHANNEL_CONTENT_TYPE)
            .header(ACCEPT, S3_READ_CHANNEL_CONTENT_TYPE)
            .body(Either::Right(StreamingBody::new(response_body)))
            .expect("static S3 read channel response metadata must be valid")
    }

    /// Accepts one binary object stream from a same-pool ingress Replica. The peer endpoint is
    /// deliberately owner-only: it never invokes the peer reader again, which enforces the
    /// one-hop limit even if a stale or malicious ticket names another Replica.
    pub(crate) async fn open_peer_stream<B>(
        &self,
        request: Request<B>,
        source_replica_id: Option<GatewayReplicaId>,
        source_certificate_fingerprint: Option<ContentDigest>,
    ) -> Response<GatewayBody>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        self.ensure_revocation_listener().await;
        if request.method() != Method::POST
            || request.version() != Version::HTTP_2
            || !request
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| has_content_type(value, S3_READ_CHANNEL_CONTENT_TYPE))
        {
            return channel_problem(
                StatusCode::BAD_REQUEST,
                "S3 peer read requires an HTTP/2 binary POST",
            );
        }
        let Some(source_replica_id) = source_replica_id else {
            return channel_problem(
                StatusCode::FORBIDDEN,
                "S3 peer read requires a Gateway Replica identity",
            );
        };
        if source_replica_id == self.inner.tunnel.identity().gateway_replica_id {
            return channel_problem(
                StatusCode::FORBIDDEN,
                "S3 peer read source must be a distinct ingress Replica",
            );
        }
        if let Some(fingerprint) = source_certificate_fingerprint.as_ref() {
            if self
                .inner
                .tunnel
                .authorize_peer_source(&source_replica_id, fingerprint)
                .await
                .is_err()
            {
                return channel_problem(
                    StatusCode::FORBIDDEN,
                    "S3 peer read certificate is not current",
                );
            }
        }

        let mut body = request.into_body();
        let open = match tokio::time::timeout(CHANNEL_HANDSHAKE_TIMEOUT, read_peer_open(&mut body))
            .await
        {
            Ok(Ok(open)) => open,
            Ok(Err(detail)) => return channel_problem(StatusCode::BAD_REQUEST, detail),
            Err(_) => {
                return channel_problem(
                    StatusCode::GATEWAY_TIMEOUT,
                    "S3 peer read handshake timed out",
                );
            }
        };
        if open.start != open.ticket.allowed_start
            || open.end_exclusive != open.ticket.allowed_end_exclusive
            || open.stream_id.is_empty()
            || open.ticket.owner_replica_id != self.inner.tunnel.identity().gateway_replica_id
            || open.ticket.validate_at(now_unix_ms()).is_err()
        {
            return channel_problem(
                StatusCode::FORBIDDEN,
                "S3 peer read ticket or range is invalid",
            );
        }
        let expected_length = open.end_exclusive.saturating_sub(open.start);
        let read = match self.open_owner(open.ticket).await {
            Ok(read) => read,
            Err(_) => {
                return channel_problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "S3 peer owner route is unavailable",
                );
            }
        };
        peer_stream_response(read, expected_length)
    }

    async fn install(&self, session: Arc<AgentReadSession>) {
        let key = (session.agent_id.clone(), session.session_generation);
        let previous = {
            let mut sessions = self.inner.sessions.lock().await;
            let stale = sessions
                .keys()
                .filter(|(agent_id, _)| agent_id == &session.agent_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut previous = Vec::new();
            for key in stale {
                if let Some(old) = sessions.remove(&key) {
                    previous.push(old);
                }
            }
            sessions.insert(key, session);
            previous
        };
        for old in previous {
            old.close("Agent S3 read channel was replaced").await;
        }
    }

    async fn remove(&self, session: &Arc<AgentReadSession>) {
        let key = (session.agent_id.clone(), session.session_generation);
        let removed = {
            let mut sessions = self.inner.sessions.lock().await;
            if sessions
                .get(&key)
                .is_some_and(|current| current.connection_id == session.connection_id)
            {
                sessions.remove(&key)
            } else {
                None
            }
        };
        session.close("Agent S3 read channel disconnected").await;
        if let Some(current) = removed {
            if current.connection_id != session.connection_id {
                current.close("Agent S3 read channel was fenced").await;
            }
        }
    }

    async fn read_agent_frames<B>(
        &self,
        session: Arc<AgentReadSession>,
        mut body: B,
        mut decoder: S3ReadChannelDecoder,
        pending: Vec<S3ReadChannelFrame>,
    ) -> Result<(), &'static str>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        for frame in pending {
            self.dispatch_agent_frame(&session, frame).await?;
        }
        let mut route_check = tokio::time::interval(ROUTE_FENCE_CHECK_INTERVAL);
        route_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        route_check.tick().await;
        loop {
            let frame = tokio::select! {
                frame = body.frame() => frame,
                _ = route_check.tick() => {
                    if !session.route_is_current().await {
                        return Err("Agent S3 route was fenced");
                    }
                    continue;
                }
            };
            let Some(frame) = frame else {
                break;
            };
            let frame = frame.map_err(|_| "Agent S3 read channel body failed")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let frames = decoder.push(&data)?;
            for frame in frames {
                if !session.alive.load(Ordering::Acquire) {
                    return Err("Agent S3 read channel was fenced");
                }
                if !session.route_is_current().await {
                    return Err("Agent S3 route was fenced");
                }
                self.dispatch_agent_frame(&session, frame).await?;
            }
        }
        decoder.finish()?;
        Err("Agent S3 read channel ended")
    }

    async fn dispatch_agent_frame(
        &self,
        session: &Arc<AgentReadSession>,
        frame: S3ReadChannelFrame,
    ) -> Result<(), &'static str> {
        match frame {
            S3ReadChannelFrame::Read(S3ReadFrame::Head(head)) => dispatch_head(session, head).await,
            S3ReadChannelFrame::Read(S3ReadFrame::Data(data)) => dispatch_data(session, data).await,
            S3ReadChannelFrame::Read(S3ReadFrame::End(end)) => dispatch_end(session, end).await,
            S3ReadChannelFrame::Read(S3ReadFrame::Error(error)) => {
                dispatch_error(session, error).await
            }
            S3ReadChannelFrame::Hello(_)
            | S3ReadChannelFrame::Ready(_)
            | S3ReadChannelFrame::Read(S3ReadFrame::Open(_))
            | S3ReadChannelFrame::Read(S3ReadFrame::Cancel(_)) => {
                Err("Agent sent an invalid S3 read channel frame")
            }
        }
    }

    async fn cancel_stream(
        &self,
        key: (AgentId, SessionGeneration),
        connection_id: u64,
        stream_id: String,
    ) {
        let session = self.inner.sessions.lock().await.get(&key).cloned();
        let Some(session) = session.filter(|session| session.connection_id == connection_id) else {
            return;
        };
        let removed = session.streams.lock().await.remove(&stream_id).is_some();
        if !removed {
            return;
        }
        session.remember_cancelled(stream_id.clone()).await;
        let _ = session
            .send(S3ReadChannelFrame::Read(S3ReadFrame::Cancel(
                S3ReadCancel { stream_id },
            )))
            .await;
    }

    async fn open_owner(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
        if !self.inner.tunnel.s3_read_route_is_current(&ticket).await {
            return Err(S3ControlError::Unavailable);
        }
        let revocation_scope = S3ReadRevocationScope::from(&ticket);
        // This gate is held through stream registration and ReadOpen enqueue. A concurrent
        // revocation either rejects the ticket first or removes a registered stream and queues
        // ReadCancel after ReadOpen; it can never miss the stream or reorder those frames.
        let revocations = self.inner.revocations.lock().await;
        if revocations.rejects(&revocation_scope) {
            return Err(S3ControlError::AccessDenied);
        }
        let key = (ticket.agent_id.clone(), ticket.session_generation);
        let session = self.inner.sessions.lock().await.get(&key).cloned();
        let Some(session) = session.filter(|session| session.alive.load(Ordering::Acquire)) else {
            return Err(S3ControlError::Unavailable);
        };
        if session.agent_connection_id != ticket.agent_connection_id
            || session.route_generation != ticket.route_generation
        {
            return Err(S3ControlError::Unavailable);
        }
        let stream_id = fresh_stream_id().map_err(|_| S3ControlError::Internal)?;
        let (sender, receiver) = mpsc::channel(OBJECT_OUTPUT_BUFFER);
        let active = ActiveRead {
            sender,
            revocation_scope,
            expected_offset: ticket.allowed_start,
            end_exclusive: ticket.allowed_end_exclusive,
            size_bytes: ticket.size_bytes,
            etag: ticket.manifest_id,
            head_received: false,
        };
        if session
            .streams
            .lock()
            .await
            .insert(stream_id.clone(), active)
            .is_some()
        {
            return Err(S3ControlError::Internal);
        }
        let open = S3ReadChannelFrame::Read(S3ReadFrame::Open(Box::new(S3ReadOpen {
            stream_id: stream_id.clone(),
            start: ticket.allowed_start,
            end_exclusive: ticket.allowed_end_exclusive,
            ticket,
        })));
        if session.try_send(open).await.is_err() {
            session.streams.lock().await.remove(&stream_id);
            return Err(S3ControlError::Unavailable);
        }
        drop(revocations);

        let registry = self.clone();
        let connection_id = session.connection_id;
        let cancel_key = key;
        let runtime = tokio::runtime::Handle::current();
        S3ObjectRead::new(receiver, move || {
            runtime.spawn(async move {
                registry
                    .cancel_stream(cancel_key, connection_id, stream_id)
                    .await;
            });
        })
    }
}

#[async_trait::async_trait]
impl S3ObjectReader for S3ReadChannelRegistry {
    async fn open(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
        self.ensure_revocation_listener().await;
        if ticket.gateway_pool_id != self.inner.gateway_pool_id.as_str()
            || ticket.validate_at(now_unix_ms()).is_err()
        {
            return Err(S3ControlError::AccessDenied);
        }
        if self
            .inner
            .revocations
            .lock()
            .await
            .rejects(&S3ReadRevocationScope::from(&ticket))
        {
            return Err(S3ControlError::AccessDenied);
        }
        if ticket.owner_replica_id == self.inner.tunnel.identity().gateway_replica_id {
            self.open_owner(ticket).await
        } else {
            self.inner.peer_reader.open_peer(ticket).await
        }
    }
}

async fn cancel_revoked_streams(
    streams: Vec<(Arc<AgentReadSession>, String, ActiveRead)>,
    detail: &'static str,
) {
    for (session, stream_id, stream) in streams {
        session.remember_cancelled(stream_id.clone()).await;
        let _ = stream.sender.try_send(Err(stream_error(detail)));
        session.try_send_cancel(stream_id).await;
    }
}

async fn read_peer_open<B>(body: &mut B) -> Result<S3ReadOpen, &'static str>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    let mut decoder = S3ReadChannelDecoder::new();
    let mut open = None;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| "S3 peer read body failed")?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        for frame in decoder.push(&data)? {
            let S3ReadChannelFrame::Read(S3ReadFrame::Open(candidate)) = frame else {
                return Err("S3 peer read accepts exactly one ReadOpen frame");
            };
            if open.replace(*candidate).is_some() {
                return Err("S3 peer read contains more than one ReadOpen frame");
            }
        }
    }
    decoder.finish()?;
    open.ok_or("S3 peer read ended before ReadOpen")
}

fn peer_stream_response(read: S3ObjectRead, expected_length: u64) -> Response<GatewayBody> {
    let (mut incoming, mut cancellation) = read.into_parts();
    let (outgoing, response_body) = mpsc::channel(OBJECT_OUTPUT_BUFFER);
    tokio::spawn(async move {
        let mut sent = 0_u64;
        loop {
            let frame = tokio::select! {
                _ = outgoing.closed() => return,
                frame = incoming.recv() => frame,
            };
            match frame {
                Some(Ok(bytes)) => {
                    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                    if bytes.is_empty()
                        || bytes.len() > S3_READ_FRAME_MAX_BYTES
                        || sent.saturating_add(length) > expected_length
                    {
                        tracing::warn!("owner Agent returned an invalid S3 peer stream length");
                        return;
                    }
                    sent += length;
                    if outgoing.send(bytes).await.is_err() {
                        return;
                    }
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "owner Agent S3 peer stream failed");
                    return;
                }
                None if sent == expected_length => {
                    cancellation.disarm();
                    return;
                }
                None => {
                    tracing::warn!(
                        sent,
                        expected_length,
                        "owner Agent S3 peer stream ended early"
                    );
                    return;
                }
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, expected_length.to_string())
        .body(Either::Right(StreamingBody::new(response_body)))
        .expect("S3 peer response metadata must be valid")
}

async fn read_hello<B>(
    body: &mut B,
) -> Result<
    (
        S3ReadChannelHello,
        S3ReadChannelDecoder,
        Vec<S3ReadChannelFrame>,
    ),
    &'static str,
>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    let mut decoder = S3ReadChannelDecoder::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| "S3 read channel handshake body failed")?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let mut frames = decoder.push(&data)?;
        if frames.is_empty() {
            continue;
        }
        let first = frames.remove(0);
        let S3ReadChannelFrame::Hello(hello) = first else {
            return Err("S3 read channel must start with Hello");
        };
        return Ok((hello, decoder, frames));
    }
    Err("S3 read channel ended before Hello")
}

async fn dispatch_head(session: &AgentReadSession, head: S3ReadHead) -> Result<(), &'static str> {
    let mut streams = session.streams.lock().await;
    let Some(stream) = streams.get_mut(&head.stream_id) else {
        return if session.was_cancelled(&head.stream_id).await {
            Ok(())
        } else {
            Err("Agent returned Head for an unknown S3 stream")
        };
    };
    if stream.head_received || head.size_bytes != stream.size_bytes || head.etag != stream.etag {
        return Err("Agent S3 Head does not match the Central ticket");
    }
    stream.head_received = true;
    Ok(())
}

async fn dispatch_data(session: &AgentReadSession, data: S3ReadData) -> Result<(), &'static str> {
    let stream_id = data.stream_id.clone();
    if data.bytes.is_empty() || data.bytes.len() > S3_READ_FRAME_MAX_BYTES {
        return Err("Agent S3 Data frame has an invalid length");
    }
    let data_len = u64::try_from(data.bytes.len()).map_err(|_| "S3 Data length overflow")?;
    let payload = Bytes::from(data.bytes);
    let next = data
        .offset
        .checked_add(data_len)
        .ok_or("S3 Data offset overflow")?;
    let sender = {
        let streams = session.streams.lock().await;
        let Some(stream) = streams.get(&data.stream_id) else {
            drop(streams);
            return if session.was_cancelled(&data.stream_id).await {
                Ok(())
            } else {
                Err("Agent returned Data for an unknown S3 stream")
            };
        };
        if !stream.head_received
            || data.offset != stream.expected_offset
            || next > stream.end_exclusive
        {
            return Err("Agent S3 Data violates the authorized range");
        }
        stream.sender.clone()
    };

    // Waiting for a per-object queue credit deliberately stops polling the Agent H2 request
    // body. H2 flow control then propagates the slow consumer back to the Agent's own bounded
    // reader instead of treating ordinary backpressure as a browser disconnect.
    let mut route_check = tokio::time::interval(ROUTE_FENCE_CHECK_INTERVAL);
    route_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    route_check.tick().await;
    let permit = loop {
        tokio::select! {
            permit = sender.reserve() => match permit {
                Ok(permit) => break permit,
                Err(_) => {
                    // A closed receiver is an actual browser/peer cancellation, unlike a full
                    // receiver. Scope the cancellation to this object stream.
                    let removed = session.streams.lock().await.remove(&stream_id).is_some();
                    if removed {
                        session.remember_cancelled(stream_id.clone()).await;
                        session.try_send_cancel(stream_id).await;
                    }
                    return Ok(());
                }
            },
            _ = route_check.tick() => {
                if !session.alive.load(Ordering::Acquire) || !session.route_is_current().await {
                    return Err("Agent S3 route was fenced");
                }
            }
        }
    };

    // A credit can become available at the same instant that the RouteLease is replaced. Check
    // the exact fence again before making the bytes visible to the response stream.
    if !session.alive.load(Ordering::Acquire) || !session.route_is_current().await {
        return Err("Agent S3 route was fenced");
    }
    {
        let mut streams = session.streams.lock().await;
        let Some(stream) = streams.get_mut(&stream_id) else {
            drop(streams);
            return if session.was_cancelled(&stream_id).await {
                Ok(())
            } else {
                Err("Agent returned Data for an unknown S3 stream")
            };
        };
        if !sender.same_channel(&stream.sender)
            || !stream.head_received
            || data.offset != stream.expected_offset
            || next > stream.end_exclusive
        {
            return Err("Agent S3 Data violates the authorized range");
        }
        stream.expected_offset = next;
    }
    permit.send(Ok(payload));
    Ok(())
}

async fn dispatch_end(session: &AgentReadSession, end: S3ReadEnd) -> Result<(), &'static str> {
    let stream = session.streams.lock().await.remove(&end.stream_id);
    let Some(stream) = stream else {
        return if session.was_cancelled(&end.stream_id).await {
            Ok(())
        } else {
            Err("Agent returned End for an unknown S3 stream")
        };
    };
    session.remember_cancelled(end.stream_id.clone()).await;
    if !stream.head_received || stream.expected_offset != stream.end_exclusive {
        let _ = stream
            .sender
            .send(Err(stream_error("Agent ended an incomplete S3 read")))
            .await;
        return Err("Agent ended an incomplete S3 read");
    }
    drop(stream);
    Ok(())
}

async fn dispatch_error(
    session: &AgentReadSession,
    error: S3ReadError,
) -> Result<(), &'static str> {
    let stream = session.streams.lock().await.remove(&error.stream_id);
    let Some(stream) = stream else {
        return if session.was_cancelled(&error.stream_id).await {
            Ok(())
        } else {
            Err("Agent returned Error for an unknown S3 stream")
        };
    };
    session.remember_cancelled(error.stream_id.clone()).await;
    let _ = stream
        .sender
        .send(Err(stream_error("Agent rejected the immutable S3 read")))
        .await;
    Ok(())
}

fn channel_problem(status: StatusCode, detail: &'static str) -> Response<GatewayBody> {
    let body = serde_json::json!({
        "type": "https://neoengram.dev/problems/s3-read-channel",
        "title": "S3 read channel rejected",
        "status": status.as_u16(),
        "code": "S3_READ_CHANNEL_REJECTED",
        "detail": detail,
    });
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/problem+json")
        .body(Either::Left(Full::new(Bytes::from(body.to_string()))))
        .expect("static S3 read channel problem metadata must be valid")
}

fn stream_error(detail: &'static str) -> BoxError {
    Box::new(std::io::Error::other(detail))
}

fn has_content_type(actual: &str, expected: &str) -> bool {
    actual
        .split(';')
        .next()
        .map(str::trim)
        .is_some_and(|value| value.eq_ignore_ascii_case(expected))
}

fn fresh_stream_id() -> Result<String, ()> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|_| ())?;
    let mut value = String::with_capacity(40);
    value.push_str("s3-read-");
    for byte in random {
        write!(&mut value, "{byte:02x}").map_err(|_| ())?;
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

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Poll},
    };

    use hyper::body::{Frame, SizeHint};
    use neoengram_domain::protocol::{
        ContentDigest, EdgeClusterId, GatewayConnectionId, GatewayOpaqueBytes, GatewayReplicaId,
        MountGeneration, OwnerGeneration, RouteGeneration,
    };

    use crate::tunnel::GatewayIdentity;

    use super::*;

    struct PendingBody;

    impl Body for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    fn tunnel() -> Arc<GatewayTunnel> {
        Arc::new(GatewayTunnel::new(GatewayIdentity {
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            software_version: "test".to_owned(),
        }))
    }

    fn hello(generation: u64) -> S3ReadChannelHello {
        S3ReadChannelHello {
            agent_id: AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(generation),
        }
    }

    fn ticket(generation: u64) -> S3ReadTicket {
        let now = now_unix_ms();
        S3ReadTicket {
            ticket_id: "ticket-a".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            snapshot_id: "snapshot-a".to_owned(),
            snapshot_lifecycle_generation: neoengram_domain::protocol::LifecycleGeneration::new(1),
            commit_id: ContentDigest::hash(b"commit"),
            index_digest: ContentDigest::hash(b"index"),
            bucket: "bucket-a".to_owned(),
            access_point_policy_generation: neoengram_domain::protocol::ResourceVersion::new(1),
            logical_path: "folder/object.bin".to_owned(),
            manifest_id: ContentDigest::hash(b"manifest"),
            size_bytes: 4,
            allowed_start: 0,
            allowed_end_exclusive: 4,
            gateway_pool_id: "pool-a".to_owned(),
            owner_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            owner_peer_endpoint: "https://replica-a.peer.example".to_owned(),
            agent_connection_id: GatewayConnectionId::new(format!(
                "s3-test-route-agent-a-{generation}"
            ))
            .unwrap(),
            route_generation: RouteGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            owner_generation: OwnerGeneration::new(2),
            mount_generation: MountGeneration::new(3),
            session_generation: SessionGeneration::new(generation),
            issued_at_unix_ms: UnixMillis::new(now.get().saturating_sub(1)),
            expires_at_unix_ms: UnixMillis::new(now.get().saturating_add(60_000)),
            signature: GatewayOpaqueBytes::new(vec![1]).unwrap(),
        }
    }

    fn revocation_scope() -> S3ReadRevocationScope {
        S3ReadRevocationScope::from(&ticket(7))
    }

    fn revocation(snapshot_generation: u64, policy_generation: u64) -> GatewayS3ReadRevocation {
        GatewayS3ReadRevocation {
            tenant_id: neoengram_domain::protocol::TenantId::new("tenant-a").unwrap(),
            snapshot_id: neoengram_domain::protocol::SnapshotId::new("snapshot-a").unwrap(),
            minimum_snapshot_lifecycle_generation: LifecycleGeneration::new(snapshot_generation),
            bucket: "bucket-a".to_owned(),
            minimum_access_point_policy_generation: ResourceVersion::new(policy_generation),
            reason: "resource generation changed".to_owned(),
        }
    }

    #[tokio::test]
    async fn disconnected_browser_cancels_only_its_stream() {
        let (outgoing, mut outgoing_rx) = mpsc::channel(8);
        let read_tunnel = tunnel();
        let route = read_tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let session = AgentReadSession::new(
            1,
            &hello(7),
            route,
            RouteGeneration::new(1),
            outgoing,
            read_tunnel,
        );
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        session.streams.lock().await.insert(
            "download-a".to_owned(),
            ActiveRead {
                sender,
                revocation_scope: revocation_scope(),
                expected_offset: 0,
                end_exclusive: 4,
                size_bytes: 4,
                etag: ContentDigest::hash(b"manifest"),
                head_received: true,
            },
        );

        dispatch_data(
            &session,
            S3ReadData {
                stream_id: "download-a".to_owned(),
                offset: 0,
                bytes: b"data".to_vec(),
            },
        )
        .await
        .unwrap();

        assert!(session.alive.load(Ordering::Acquire));
        assert!(session.streams.lock().await.is_empty());
        assert!(session.was_cancelled("download-a").await);
        let cancel = outgoing_rx.recv().await.unwrap();
        assert!(matches!(
            S3ReadChannelFrame::decode(&cancel).unwrap(),
            S3ReadChannelFrame::Read(S3ReadFrame::Cancel(S3ReadCancel { ref stream_id }))
                if stream_id == "download-a"
        ));

        let (sender, mut receiver) = mpsc::channel(1);
        session.streams.lock().await.insert(
            "download-b".to_owned(),
            ActiveRead {
                sender,
                revocation_scope: revocation_scope(),
                expected_offset: 0,
                end_exclusive: 1,
                size_bytes: 1,
                etag: ContentDigest::hash(b"manifest-b"),
                head_received: true,
            },
        );
        dispatch_data(
            &session,
            S3ReadData {
                stream_id: "download-b".to_owned(),
                offset: 0,
                bytes: vec![b'b'],
            },
        )
        .await
        .unwrap();
        assert_eq!(
            receiver.recv().await.unwrap().unwrap(),
            Bytes::from_static(b"b")
        );
    }

    #[tokio::test]
    async fn slow_browser_backpressures_a_large_download_without_exceeding_the_object_queue() {
        const FRAME_COUNT: usize = OBJECT_OUTPUT_BUFFER + 4;
        const TOTAL_BYTES: usize = FRAME_COUNT * S3_READ_FRAME_MAX_BYTES;

        const { assert!(TOTAL_BYTES > 2 * 1024 * 1024) };
        let (outgoing, mut outgoing_rx) = mpsc::channel(8);
        let read_tunnel = tunnel();
        let route = read_tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let session = Arc::new(AgentReadSession::new(
            1,
            &hello(7),
            route,
            RouteGeneration::new(1),
            outgoing,
            read_tunnel,
        ));
        let (slow_sender, mut slow_receiver) = mpsc::channel(OBJECT_OUTPUT_BUFFER);
        session.streams.lock().await.insert(
            "slow-download".to_owned(),
            ActiveRead {
                sender: slow_sender,
                revocation_scope: revocation_scope(),
                expected_offset: 0,
                end_exclusive: TOTAL_BYTES as u64,
                size_bytes: TOTAL_BYTES as u64,
                etag: ContentDigest::hash(b"slow-manifest"),
                head_received: true,
            },
        );

        let completed_frames = Arc::new(AtomicUsize::new(0));
        let producer_session = session.clone();
        let producer_completed_frames = completed_frames.clone();
        let producer = tokio::spawn(async move {
            for frame_index in 0..FRAME_COUNT {
                let offset = frame_index * S3_READ_FRAME_MAX_BYTES;
                dispatch_data(
                    &producer_session,
                    S3ReadData {
                        stream_id: "slow-download".to_owned(),
                        offset: offset as u64,
                        bytes: vec![frame_index as u8; S3_READ_FRAME_MAX_BYTES],
                    },
                )
                .await
                .unwrap();
                producer_completed_frames.fetch_add(1, Ordering::Release);
            }
            dispatch_end(
                &producer_session,
                S3ReadEnd {
                    stream_id: "slow-download".to_owned(),
                },
            )
            .await
            .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while completed_frames.load(Ordering::Acquire) < OBJECT_OUTPUT_BUFFER {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the producer must fill the bounded object queue");
        tokio::task::yield_now().await;
        assert_eq!(slow_receiver.max_capacity(), OBJECT_OUTPUT_BUFFER);
        assert_eq!(slow_receiver.len(), OBJECT_OUTPUT_BUFFER);
        assert_eq!(
            completed_frames.load(Ordering::Acquire),
            OBJECT_OUTPUT_BUFFER,
            "the ninth frame must wait for a consumer credit"
        );
        assert!(!producer.is_finished());

        let mut received_bytes = 0_usize;
        for frame_index in 0..FRAME_COUNT {
            let bytes = tokio::time::timeout(Duration::from_secs(1), slow_receiver.recv())
                .await
                .expect("a credited frame must arrive")
                .expect("the object stream must remain open")
                .expect("the Agent frame must remain successful");
            assert_eq!(bytes.len(), S3_READ_FRAME_MAX_BYTES);
            assert!(bytes.iter().all(|byte| *byte == frame_index as u8));
            received_bytes += bytes.len();
            assert!(slow_receiver.len() <= OBJECT_OUTPUT_BUFFER);
            tokio::task::yield_now().await;
        }
        assert_eq!(received_bytes, TOTAL_BYTES);
        tokio::time::timeout(Duration::from_secs(1), producer)
            .await
            .expect("the backpressured producer must finish after credits are returned")
            .unwrap();
        assert!(slow_receiver.recv().await.is_none());
        assert!(
            outgoing_rx.try_recv().is_err(),
            "backpressure is not a cancel"
        );
    }

    #[tokio::test]
    async fn backpressured_download_still_observes_route_replacement() {
        let (outgoing, _outgoing_rx) = mpsc::channel(8);
        let read_tunnel = tunnel();
        let route = read_tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let session = Arc::new(AgentReadSession::new(
            1,
            &hello(7),
            route.clone(),
            RouteGeneration::new(1),
            outgoing,
            read_tunnel.clone(),
        ));
        let (sender, _receiver) = mpsc::channel(1);
        sender
            .try_send(Ok(Bytes::from_static(b"already-buffered")))
            .unwrap();
        session.streams.lock().await.insert(
            "fenced-download".to_owned(),
            ActiveRead {
                sender,
                revocation_scope: revocation_scope(),
                expected_offset: 0,
                end_exclusive: 1,
                size_bytes: 1,
                etag: ContentDigest::hash(b"fenced-manifest"),
                head_received: true,
            },
        );

        let producer_session = session.clone();
        let producer = tokio::spawn(async move {
            dispatch_data(
                &producer_session,
                S3ReadData {
                    stream_id: "fenced-download".to_owned(),
                    offset: 0,
                    bytes: vec![b'f'],
                },
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!producer.is_finished());

        read_tunnel.remove_test_agent_route(&route).await;
        let result = tokio::time::timeout(Duration::from_secs(1), producer)
            .await
            .expect("a backpressured read must recheck its route fence")
            .unwrap();
        assert_eq!(result, Err("Agent S3 route was fenced"));
    }

    #[tokio::test]
    async fn object_open_rejects_stale_generation_pool_and_route() {
        let tunnel = tunnel();
        let route = tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let registry = S3ReadChannelRegistry::new(tunnel.clone());
        let (outgoing, mut outgoing_rx) = mpsc::channel(8);
        registry
            .install(Arc::new(AgentReadSession::new(
                1,
                &hello(7),
                route.clone(),
                RouteGeneration::new(1),
                outgoing,
                tunnel.clone(),
            )))
            .await;

        let stale_error = match S3ObjectReader::open(&registry, ticket(6)).await {
            Err(error) => error,
            Ok(_) => panic!("stale session generation unexpectedly opened"),
        };
        assert_eq!(stale_error, S3ControlError::Unavailable);
        let mut wrong_pool = ticket(7);
        wrong_pool.gateway_pool_id = "pool-b".to_owned();
        let pool_error = match S3ObjectReader::open(&registry, wrong_pool).await {
            Err(error) => error,
            Ok(_) => panic!("wrong gateway pool unexpectedly opened"),
        };
        assert_eq!(pool_error, S3ControlError::AccessDenied);

        let mut stale_route_generation = ticket(7);
        stale_route_generation.route_generation = RouteGeneration::new(2);
        assert!(matches!(
            S3ObjectReader::open(&registry, stale_route_generation).await,
            Err(S3ControlError::Unavailable)
        ));
        let mut wrong_connection = ticket(7);
        wrong_connection.agent_connection_id =
            GatewayConnectionId::new("another-agent-connection").unwrap();
        assert!(matches!(
            S3ObjectReader::open(&registry, wrong_connection).await,
            Err(S3ControlError::Unavailable)
        ));

        let read = S3ObjectReader::open(&registry, ticket(7)).await.unwrap();
        assert!(matches!(
            S3ReadChannelFrame::decode(&outgoing_rx.recv().await.unwrap()).unwrap(),
            S3ReadChannelFrame::Read(S3ReadFrame::Open(_))
        ));
        drop(read);

        tunnel.remove_test_agent_route(&route).await;
        let fenced_error = match S3ObjectReader::open(&registry, ticket(7)).await {
            Err(error) => error,
            Ok(_) => panic!("fenced route unexpectedly opened"),
        };
        assert_eq!(fenced_error, S3ControlError::Unavailable);
    }

    #[tokio::test]
    async fn revocation_cancels_only_older_generations_and_fences_late_opens() {
        let tunnel = tunnel();
        let route = tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let registry = S3ReadChannelRegistry::new(tunnel.clone());
        let (outgoing, mut outgoing_rx) = mpsc::channel(16);
        let session = Arc::new(AgentReadSession::new(
            1,
            &hello(7),
            route,
            RouteGeneration::new(1),
            outgoing,
            tunnel.clone(),
        ));
        registry.install(session.clone()).await;

        let mut old_snapshot = ticket(7);
        old_snapshot.access_point_policy_generation = ResourceVersion::new(2);
        let (mut old_snapshot_rx, _old_snapshot_cancel) =
            S3ObjectReader::open(&registry, old_snapshot)
                .await
                .unwrap()
                .into_parts();

        let mut old_policy = ticket(7);
        old_policy.snapshot_lifecycle_generation = LifecycleGeneration::new(2);
        let (mut old_policy_rx, _old_policy_cancel) = S3ObjectReader::open(&registry, old_policy)
            .await
            .unwrap()
            .into_parts();

        let mut current = ticket(7);
        current.snapshot_lifecycle_generation = LifecycleGeneration::new(2);
        current.access_point_policy_generation = ResourceVersion::new(2);
        let (mut current_rx, _current_cancel) = S3ObjectReader::open(&registry, current.clone())
            .await
            .unwrap()
            .into_parts();

        for _ in 0..3 {
            assert!(matches!(
                S3ReadChannelFrame::decode(&outgoing_rx.recv().await.unwrap()).unwrap(),
                S3ReadChannelFrame::Read(S3ReadFrame::Open(_))
            ));
        }

        tunnel.publish_test_s3_read_revocation(revocation(2, 2));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), old_snapshot_rx.recv())
                .await
                .unwrap()
                .is_some_and(|result| result.is_err())
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), old_policy_rx.recv())
                .await
                .unwrap()
                .is_some_and(|result| result.is_err())
        );
        assert!(matches!(
            current_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(session.streams.lock().await.len(), 1);

        let mut cancelled = BTreeSet::new();
        for _ in 0..2 {
            let frame = S3ReadChannelFrame::decode(&outgoing_rx.recv().await.unwrap()).unwrap();
            let S3ReadChannelFrame::Read(S3ReadFrame::Cancel(cancel)) = frame else {
                panic!("expected revoked stream cancellation");
            };
            cancelled.insert(cancel.stream_id);
        }
        assert_eq!(cancelled.len(), 2);

        assert!(matches!(
            S3ObjectReader::open(&registry, ticket(7)).await,
            Err(S3ControlError::AccessDenied)
        ));

        registry.apply_revocation(revocation(2, 2)).await;
        assert_eq!(session.streams.lock().await.len(), 1);
        assert!(outgoing_rx.try_recv().is_err());

        let additional_current = S3ObjectReader::open(&registry, current).await.unwrap();
        assert!(matches!(
            S3ReadChannelFrame::decode(&outgoing_rx.recv().await.unwrap()).unwrap(),
            S3ReadChannelFrame::Read(S3ReadFrame::Open(_))
        ));
        drop(additional_current);
    }

    #[tokio::test]
    async fn active_channel_detects_exact_route_replacement_without_waiting_for_data() {
        let tunnel = tunnel();
        let route = tunnel
            .install_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        let registry = S3ReadChannelRegistry::new(tunnel.clone());
        let (outgoing, _response) = mpsc::channel(8);
        let session = Arc::new(AgentReadSession::new(
            1,
            &hello(7),
            route.clone(),
            RouteGeneration::new(1),
            outgoing,
            tunnel.clone(),
        ));
        let replacement = tunnel
            .replace_test_agent_route(AgentId::new("agent-a").unwrap(), SessionGeneration::new(7))
            .await;
        assert_ne!(replacement, route);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            registry.read_agent_frames(
                session,
                PendingBody,
                S3ReadChannelDecoder::new(),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result, Err("Agent S3 route was fenced"));
    }

    struct RecordingPeerReader {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl S3PeerReader for RecordingPeerReader {
        async fn open_peer(&self, ticket: S3ReadTicket) -> Result<S3ObjectRead, S3ControlError> {
            assert_eq!(ticket.owner_replica_id.as_str(), "replica-b");
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (_sender, receiver) = mpsc::channel(1);
            S3ObjectRead::new(receiver, || {})
        }
    }

    #[tokio::test]
    async fn non_owner_delegates_exactly_once_but_owner_endpoint_never_forwards_again() {
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = S3ReadChannelRegistry::with_peer_reader(
            tunnel(),
            Arc::new(RecordingPeerReader {
                calls: calls.clone(),
            }),
        );
        let mut remote = ticket(7);
        remote.owner_replica_id = GatewayReplicaId::new("replica-b").unwrap();
        remote.owner_peer_endpoint = "https://replica-b.peer.example".to_owned();
        remote.agent_connection_id = GatewayConnectionId::new("remote-agent-connection").unwrap();

        let read = S3ObjectReader::open(&registry, remote.clone())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(read);

        let encoded = S3ReadChannelFrame::Read(S3ReadFrame::Open(Box::new(S3ReadOpen {
            stream_id: "peer-open".to_owned(),
            start: remote.allowed_start,
            end_exclusive: remote.allowed_end_exclusive,
            ticket: remote,
        })))
        .encode()
        .unwrap();
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .header(CONTENT_TYPE, S3_READ_CHANNEL_CONTENT_TYPE)
            .body(Full::new(Bytes::from(encoded)))
            .unwrap();
        let response = registry
            .open_peer_stream(
                request,
                Some(GatewayReplicaId::new("replica-source").unwrap()),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_peer_response_propagates_cancellation_to_the_owner_read() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let (_input, receiver) = mpsc::channel(1);
        let cancelled_for_callback = cancelled.clone();
        let read = S3ObjectRead::new(receiver, move || {
            cancelled_for_callback.store(true, Ordering::Release);
        })
        .unwrap();
        let response = peer_stream_response(read, 4);
        drop(response);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
