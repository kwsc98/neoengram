use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::Duration,
};

use crate::{
    AgentRouteLease, AgentRouteLeaseListRequest, CentralError, CentralErrorCode, Clock,
    GatewayCredentialState, GatewayPoolState, GatewayRegistryRepository, GatewayReplicaListRequest,
    GatewayReplicaState, ReleaseAgentRouteLeaseRequest, RenewAgentRouteLeaseRequest,
    GATEWAY_REGISTRY_MAX_PAGE_SIZE,
};
use bytes::Bytes;
use neoengram_domain::protocol::{
    CertificateGeneration, GatewayAgentAction, GatewayAgentRequest, GatewayAgentResponse,
    GatewayAgentStreamData, GatewayAgentStreamEnd, GatewayAgentStreamOpen, GatewayBackpressure,
    GatewayConnectionId, GatewayControlError, GatewayControlFrame, GatewayControlMessage,
    GatewayErrorCode, GatewayOpaqueBytes, GatewayPeerDirectory, GatewayPeerDirectoryEntry,
    GatewayPeerForwardAccepted, GatewayPeerForwardRequest, GatewayPoolId, GatewayReplicaId,
    GatewayRouteFence, GatewayRouteLeaseGranted, GatewayRouteLeaseRequest, GatewayS3ReadRevocation,
    Generation, RequestId, SequenceNumber, UnixMillis, AGENT_ROUTE_LEASE_TTL_MS,
    CURRENT_WIRE_VERSION, GATEWAY_PEER_DIRECTORY_TTL_MS, MAX_AGENT_CHANNEL_FRAME_BYTES,
    MAX_GATEWAY_STREAM_CHUNK_BYTES,
};
use serde::Serialize;
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, RwLock as AsyncRwLock},
    time::{timeout, timeout_at},
};

use crate::agent_transport::{
    AgentAction, AgentApiHandler, AgentControlInput, AgentControlInputTrySendError,
    AgentControlInputWriter, AgentHttpError, GatewayAgentRouteContext,
};

const CONTROL_OUTPUT_BUFFER: usize = 256;
const AGENT_STREAM_INPUT_BUFFER: usize = 16;
const MAX_ACTIVE_AGENT_STREAMS: usize = 256;
const MAX_ACTIVE_AGENT_REQUESTS: usize = 256;
const MAX_LATE_PEER_FORWARDS: usize = 1_024;
const CONTROL_RESPONSE_DEADLINE_MS: u64 = 10_000;
const LATE_PEER_FORWARD_RETENTION_MS: u64 = CONTROL_RESPONSE_DEADLINE_MS * 2;
// Disconnect cleanup is best effort. A stalled authority must not prevent a replacement Gateway
// control session from being admitted; the route lease remains fenced by its exact generation and
// expires through the normal short lease window if this cleanup cannot complete.
const ROUTE_CLEANUP_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const ROUTE_CLEANUP_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const JSON_CONTENT_TYPE: &str = "application/json";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json";

/// Identity extracted from a verified Gateway workload certificate URI SAN.
///
/// The transport adapter must construct this value from the mTLS peer certificate. Frame fields
/// are never accepted as proof of workload identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedGatewayReplica {
    pub edge_cluster_id: neoengram_domain::protocol::EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub certificate_generation: CertificateGeneration,
}

/// Central-side protocol boundary shared by concrete H2 connector implementations.
#[derive(Clone)]
pub struct CentralGatewayControl {
    registry: Arc<dyn GatewayRegistryRepository>,
    agent_handler: Arc<dyn AgentApiHandler>,
    clock: Arc<dyn Clock>,
    sessions: Arc<AsyncMutex<BTreeMap<GatewayReplicaId, Weak<CentralGatewaySession>>>>,
}

impl CentralGatewayControl {
    #[must_use]
    pub fn new(
        registry: Arc<dyn GatewayRegistryRepository>,
        agent_handler: Arc<dyn AgentApiHandler>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            registry,
            agent_handler,
            clock,
            sessions: Arc::new(AsyncMutex::new(BTreeMap::new())),
        }
    }

    /// Delivers one already-encoded Agent downstream frame through an explicitly selected,
    /// connected non-owner Replica. The authoritative owner route and peer endpoint are always
    /// reloaded from Central's Registry; callers cannot supply or override either value.
    pub async fn forward_agent_frame_via(
        &self,
        via_replica_id: &GatewayReplicaId,
        agent_id: &neoengram_domain::protocol::AgentId,
        encoded_frame: Bytes,
    ) -> Result<(), GatewaySessionError> {
        forward_agent_frame_via_authority(
            &self.registry,
            &self.sessions,
            self.clock.as_ref(),
            via_replica_id,
            agent_id,
            None,
            encoded_frame,
        )
        .await
    }

    /// Broadcasts one monotonic S3 ticket fence to every connected Replica in the selected pool.
    /// A failed session is fenced by its send path; other Replicas still receive the revocation.
    pub async fn broadcast_s3_read_revocation(
        &self,
        gateway_pool_id: &GatewayPoolId,
        revocation: GatewayS3ReadRevocation,
    ) -> usize {
        let sessions = {
            let mut directory = self.sessions.lock().await;
            let mut selected = Vec::new();
            directory.retain(|_, session| {
                let Some(session) = session.upgrade() else {
                    return false;
                };
                if session.identity.gateway_pool_id == *gateway_pool_id {
                    selected.push(session);
                }
                true
            });
            selected
        };
        let mut delivered = 0;
        for session in sessions {
            let request_id = match fresh_control_request_id("s3-read-revocation") {
                Ok(request_id) => request_id,
                Err(error) => {
                    tracing::error!(%error, "failed to allocate S3 read revocation identity");
                    continue;
                }
            };
            match session
                .send(
                    request_id,
                    None,
                    GatewayControlMessage::S3ReadRevocation(revocation.clone()),
                )
                .await
            {
                Ok(()) => delivered += 1,
                Err(error) => tracing::warn!(
                    gateway_replica_id = %session.identity.gateway_replica_id,
                    %error,
                    "failed to deliver S3 read revocation"
                ),
            }
        }
        delivered
    }

    /// Authenticates the first Replica hello and creates one fail-closed control session.
    pub async fn open(
        &self,
        identity: AuthenticatedGatewayReplica,
        hello: GatewayControlFrame,
    ) -> Result<
        (
            Arc<CentralGatewaySession>,
            mpsc::Receiver<GatewayControlFrame>,
        ),
        GatewaySessionError,
    > {
        hello.validate_at(self.clock.now())?;
        if hello.sequence.get() != 1 || hello.hop_count != 0 {
            return Err(GatewaySessionError::Protocol(
                "the first Gateway frame must be a direct sequence-1 hello",
            ));
        }
        if hello.gateway_pool_id != identity.gateway_pool_id
            || hello.gateway_replica_id != identity.gateway_replica_id
        {
            return Err(GatewaySessionError::Identity);
        }
        let GatewayControlMessage::ReplicaHello(hello_message) = &hello.message else {
            return Err(GatewaySessionError::Protocol(
                "the first Gateway frame must be replica_hello",
            ));
        };
        if hello_message.edge_cluster_id != identity.edge_cluster_id {
            return Err(GatewaySessionError::Identity);
        }
        let replica = self
            .registry
            .get_replica(&identity.gateway_replica_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        let certificate_generation = replica
            .credential
            .certificate_generation
            .ok_or(GatewaySessionError::Identity)?;
        let certificate_not_after = replica
            .credential
            .certificate_not_after_unix_ms
            .ok_or(GatewaySessionError::Identity)?;
        let identity_matches = replica.gateway_pool_id == identity.gateway_pool_id
            && replica.edge_cluster_id == identity.edge_cluster_id
            && replica.state == GatewayReplicaState::Active
            && replica.credential.state == GatewayCredentialState::Active
            && certificate_generation == identity.certificate_generation
            && certificate_not_after.get() > self.clock.now().get()
            && replica.wire_version == CURRENT_WIRE_VERSION
            && hello_message.software_version == replica.software_version
            && hello_message.wire_version == replica.wire_version
            && hello_message.capabilities == replica.capabilities;
        if !identity_matches {
            tracing::warn!(
                gateway_replica_id = %identity.gateway_replica_id,
                identity_pool = %identity.gateway_pool_id,
                registry_pool = %replica.gateway_pool_id,
                identity_edge_cluster = %identity.edge_cluster_id,
                registry_edge_cluster = %replica.edge_cluster_id,
                registry_state = ?replica.state,
                registry_credential_state = ?replica.credential.state,
                identity_certificate_generation = ?identity.certificate_generation,
                registry_certificate_generation = ?certificate_generation,
                certificate_not_after = certificate_not_after.get(),
                now = self.clock.now().get(),
                hello_software_version = %hello_message.software_version,
                registry_software_version = %replica.software_version,
                hello_wire_version = ?hello_message.wire_version,
                registry_wire_version = ?replica.wire_version,
                hello_capabilities = ?hello_message.capabilities,
                registry_capabilities = ?replica.capabilities,
                "Gateway workload identity mismatch details"
            );
            return Err(GatewaySessionError::Identity);
        }
        if replica.gateway_pool_id != identity.gateway_pool_id
            || replica.edge_cluster_id != identity.edge_cluster_id
            || replica.state != GatewayReplicaState::Active
            || replica.credential.state != GatewayCredentialState::Active
            || certificate_generation != identity.certificate_generation
            || certificate_not_after.get() <= self.clock.now().get()
            || replica.wire_version != CURRENT_WIRE_VERSION
            || hello_message.software_version != replica.software_version
            || hello_message.wire_version != replica.wire_version
            || hello_message.capabilities != replica.capabilities
        {
            return Err(GatewaySessionError::Identity);
        }
        let pool = self
            .registry
            .get_pool(&identity.gateway_pool_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        if pool.edge_cluster_id != identity.edge_cluster_id || pool.state != GatewayPoolState::Ready
        {
            return Err(GatewaySessionError::Identity);
        }

        let (output, receiver) = mpsc::channel(CONTROL_OUTPUT_BUFFER);
        let session = Arc::new(CentralGatewaySession {
            identity,
            connection_id: hello.connection_id,
            registry: self.registry.clone(),
            agent_handler: self.agent_handler.clone(),
            clock: self.clock.clone(),
            sessions: self.sessions.clone(),
            inbound: StdMutex::new(InboundState {
                last_sequence: 1,
                streams: BTreeMap::new(),
            }),
            owned_routes: StdMutex::new(BTreeMap::new()),
            accept_gate: AsyncMutex::new(()),
            admission: AsyncRwLock::new(()),
            cleanup_gate: StdMutex::new(()),
            unary_tasks: StdMutex::new(BTreeMap::new()),
            outbound: AsyncMutex::new(OutboundState {
                next_sequence: 1,
                sender: output,
            }),
            pending_forwards: StdMutex::new(BTreeMap::new()),
            late_forwards: StdMutex::new(BTreeMap::new()),
            fenced: AtomicBool::new(false),
            route_release: StdMutex::new(RouteReleaseState { tasks: Vec::new() }),
            aborted_stream_tasks: StdMutex::new(Vec::new()),
            fence_signal: watch::channel(false).0,
            peer_directory_generation: AtomicU64::new(1),
        });
        // Keep a weak directory so a newly acquired route can fence an old owner even when that
        // owner is connected through another Gateway Replica. The directory never owns a session
        // and stale entries are harmless: they are replaced on the next successful handshake.
        let previous = self
            .sessions
            .lock()
            .await
            .insert(
                session.identity.gateway_replica_id.clone(),
                Arc::downgrade(&session),
            )
            .and_then(|session| session.upgrade());
        if let Some(previous) = previous {
            previous.fence_and_wait().await;
        }
        Ok((session, receiver))
    }
}

#[async_trait::async_trait]
impl crate::service::S3ReadRevocationPublisher for CentralGatewayControl {
    async fn publish_s3_read_revocation(
        &self,
        gateway_pool_id: &GatewayPoolId,
        revocation: GatewayS3ReadRevocation,
    ) {
        self.broadcast_s3_read_revocation(gateway_pool_id, revocation)
            .await;
    }
}

async fn forward_agent_frame_via_authority(
    registry: &Arc<dyn GatewayRegistryRepository>,
    sessions: &Arc<AsyncMutex<BTreeMap<GatewayReplicaId, Weak<CentralGatewaySession>>>>,
    clock: &dyn Clock,
    via_replica_id: &GatewayReplicaId,
    agent_id: &neoengram_domain::protocol::AgentId,
    expected_route: Option<&AgentRouteLease>,
    encoded_frame: Bytes,
) -> Result<(), GatewaySessionError> {
    let now = clock.now();
    let route = registry
        .get_agent_route(agent_id)
        .await?
        .filter(|route| route.is_active_at(now))
        .ok_or(GatewaySessionError::RouteUnavailable(
            "Agent has no active owner route",
        ))?;
    if expected_route.is_some_and(|expected| !same_route_fence(expected, &route)) {
        return Err(GatewaySessionError::RouteUnavailable(
            "Agent owner route changed before fallback delivery",
        ));
    }
    let pool = registry.get_pool(&route.gateway_pool_id).await?.ok_or(
        GatewaySessionError::RouteUnavailable("owner GatewayPool is not registered"),
    )?;
    if pool.state != GatewayPoolState::Ready || pool.edge_cluster_id != route.edge_cluster_id {
        return Err(GatewaySessionError::RouteUnavailable(
            "owner GatewayPool is not ready in the Agent route scope",
        ));
    }
    if &route.gateway_replica_id == via_replica_id {
        return Err(GatewaySessionError::Protocol(
            "owner forwarding requires a distinct ingress Replica",
        ));
    }
    let owner = registry
        .get_replica(&route.gateway_replica_id)
        .await?
        .ok_or(GatewaySessionError::RouteUnavailable(
            "owner Replica is not registered",
        ))?;
    if owner.state != GatewayReplicaState::Active
        || owner.credential.state != GatewayCredentialState::Active
        || owner
            .credential
            .certificate_not_after_unix_ms
            .is_none_or(|not_after| not_after.get() <= now.get())
        || owner.gateway_pool_id != route.gateway_pool_id
        || owner.edge_cluster_id != route.edge_cluster_id
    {
        return Err(GatewaySessionError::RouteUnavailable(
            "owner Replica is not active in the Agent route scope",
        ));
    }
    let via_record = registry.get_replica(via_replica_id).await?.ok_or(
        GatewaySessionError::RouteUnavailable("ingress Replica is not registered"),
    )?;
    if via_record.state != GatewayReplicaState::Active
        || via_record.credential.state != GatewayCredentialState::Active
        || via_record
            .credential
            .certificate_not_after_unix_ms
            .is_none_or(|not_after| not_after.get() <= now.get())
        || via_record.gateway_pool_id != route.gateway_pool_id
        || via_record.edge_cluster_id != route.edge_cluster_id
    {
        return Err(GatewaySessionError::RouteUnavailable(
            "ingress Replica cannot perform same-pool peer forwarding",
        ));
    }
    let via = sessions
        .lock()
        .await
        .get(via_replica_id)
        .and_then(Weak::upgrade)
        .ok_or(GatewaySessionError::RouteUnavailable(
            "ingress Replica control session is unavailable",
        ))?;
    if via.identity.gateway_pool_id != route.gateway_pool_id
        || via.identity.edge_cluster_id != route.edge_cluster_id
        || via_record.credential.certificate_generation != Some(via.identity.certificate_generation)
    {
        return Err(GatewaySessionError::Identity);
    }
    if encoded_frame.len() < 2
        || encoded_frame.len() > MAX_AGENT_CHANNEL_FRAME_BYTES.saturating_add(1)
        || encoded_frame.last() != Some(&b'\n')
    {
        return Err(GatewaySessionError::Protocol(
            "forwarded Agent frame must be one bounded LF-terminated frame",
        ));
    }
    let downstream = neoengram_domain::protocol::AgentChannelDownstreamFrame::decode_json(
        &encoded_frame[..encoded_frame.len() - 1],
    )?;
    if !is_peer_forwardable_message(&downstream.message) {
        return Err(GatewaySessionError::Protocol(
            "peer forwarding accepts only Agent Job or lifecycle command frames",
        ));
    }
    if downstream.session_generation != route.session_generation {
        return Err(GatewaySessionError::RouteUnavailable(
            "Agent frame session generation does not match the owner route",
        ));
    }
    let request_id = fresh_forward_request_id()?;
    via.send_peer_forward(
        request_id,
        GatewayPeerForwardRequest {
            source_replica_id: via.identity.gateway_replica_id.clone(),
            target_replica_id: route.gateway_replica_id,
            target_peer_endpoint: owner.peer_endpoint,
            agent_id: route.agent_id,
            agent_connection_id: route.connection_id,
            session_generation: route.session_generation,
            route_generation: route.route_generation,
            frame: GatewayOpaqueBytes::new(encoded_frame.to_vec())?,
        },
    )
    .await
}

fn same_route_fence(expected: &AgentRouteLease, current: &AgentRouteLease) -> bool {
    expected.agent_id == current.agent_id
        && expected.edge_cluster_id == current.edge_cluster_id
        && expected.gateway_pool_id == current.gateway_pool_id
        && expected.gateway_replica_id == current.gateway_replica_id
        && expected.connection_id == current.connection_id
        && expected.session_generation == current.session_generation
        && expected.route_generation == current.route_generation
}

fn prune_late_peer_forwards(late: &mut BTreeMap<RequestId, LatePeerForward>, now: UnixMillis) {
    late.retain(|_, tombstone| tombstone.expires_at_unix_ms.get() > now.get());
}

fn remember_late_peer_forward(
    late: &mut BTreeMap<RequestId, LatePeerForward>,
    request_id: RequestId,
    expected: GatewayPeerForwardAccepted,
    now: UnixMillis,
) {
    prune_late_peer_forwards(late, now);
    while late.len() >= MAX_LATE_PEER_FORWARDS {
        let Some(oldest) = late.keys().next().cloned() else {
            break;
        };
        late.remove(&oldest);
    }
    late.insert(
        request_id,
        LatePeerForward {
            expected,
            expires_at_unix_ms: UnixMillis::new(
                now.get().saturating_add(LATE_PEER_FORWARD_RETENTION_MS),
            ),
        },
    );
}

fn forwarded_command_generation(
    encoded_frame: &[u8],
) -> Option<neoengram_domain::protocol::SessionGeneration> {
    if encoded_frame.len() < 2
        || encoded_frame.len() > MAX_AGENT_CHANNEL_FRAME_BYTES.saturating_add(1)
        || encoded_frame.last() != Some(&b'\n')
    {
        return None;
    }
    let downstream = neoengram_domain::protocol::AgentChannelDownstreamFrame::decode_json(
        &encoded_frame[..encoded_frame.len() - 1],
    )
    .ok()?;
    is_peer_forwardable_message(&downstream.message).then_some(downstream.session_generation)
}

/// Only current Job and resource-lifecycle commands may cross a Gateway peer hop. The former
/// whole-Commit replication assignment is intentionally excluded from this transport boundary.
fn is_peer_forwardable_message(
    message: &neoengram_domain::protocol::AgentChannelDownstreamMessage,
) -> bool {
    matches!(
        message,
        neoengram_domain::protocol::AgentChannelDownstreamMessage::Assignment(_)
            | neoengram_domain::protocol::AgentChannelDownstreamMessage::Decision(_)
            | neoengram_domain::protocol::AgentChannelDownstreamMessage::LifecycleAssignment(_)
    )
}

#[cfg(test)]
fn is_peer_forwardable_type(message_type: &str) -> bool {
    matches!(
        message_type,
        "job.assignment" | "job.decision" | "resource.lifecycle.assignment"
    )
}

/// One authenticated Central-to-Replica H2 session.
pub struct CentralGatewaySession {
    identity: AuthenticatedGatewayReplica,
    connection_id: GatewayConnectionId,
    registry: Arc<dyn GatewayRegistryRepository>,
    agent_handler: Arc<dyn AgentApiHandler>,
    clock: Arc<dyn Clock>,
    sessions: Arc<AsyncMutex<BTreeMap<GatewayReplicaId, Weak<CentralGatewaySession>>>>,
    // The synchronous maps are drained by the fence path, while `admission` linearizes async
    // frame dispatch against replacement fencing. Synchronous draining keeps the connector's Drop
    // guard abort-safe during task cancellation.
    inbound: StdMutex<InboundState>,
    /// Central route leases outlive the H2 stream worker that acquired them. Keep ownership
    /// independently of `inbound.streams` so a worker that exits after its final outbound frame
    /// cannot make a later disconnect forget the lease. Entries are reserved when the stream is
    /// admitted, then filled with the authoritative lease once the atomic session/route open
    /// commits; the reservation closes the fence-vs-acquire race during task cancellation.
    owned_routes: StdMutex<BTreeMap<GatewayConnectionId, OwnedAgentRoute>>,
    accept_gate: AsyncMutex<()>,
    admission: AsyncRwLock<()>,
    /// Linearizes stream-worker exit cleanup with replacement fencing. A worker removes its
    /// inbound entry before scheduling the durable route release; keeping both operations under
    /// this gate prevents a replacement from observing the gap between them.
    cleanup_gate: StdMutex<()>,
    unary_tasks: StdMutex<BTreeMap<GatewayConnectionId, tokio::task::AbortHandle>>,
    outbound: AsyncMutex<OutboundState>,
    pending_forwards: StdMutex<BTreeMap<RequestId, PendingForward>>,
    late_forwards: StdMutex<BTreeMap<RequestId, LatePeerForward>>,
    fenced: AtomicBool,
    /// Serializes disconnected-route cleanup tasks with replacement fencing. The replacement
    /// boundary must observe every task scheduled by an exiting stream worker.
    route_release: StdMutex<RouteReleaseState>,
    /// Stream workers are spawned independently from the H2 frame reader. Keep their join
    /// handles after fencing aborts them so replacement admission can wait for each worker's
    /// `Drop` guard and its exact route-release task to finish.
    aborted_stream_tasks: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
    fence_signal: watch::Sender<bool>,
    /// Monotonic version for peer-directory snapshots on this control session.
    peer_directory_generation: AtomicU64,
}

struct PendingForward {
    expected: GatewayPeerForwardAccepted,
    sender: oneshot::Sender<Result<(), GatewayControlError>>,
}

/// Retains the exact binding of a peer forward after its waiter has timed out or the local
/// control link failed. The target Replica may already have accepted the frame, so a late matching
/// acknowledgement must be absorbed rather than interpreted as an unknown protocol frame that
/// fences the whole Central session. Entries are bounded and short-lived; an acknowledgement with
/// a different binding still fails closed.
struct LatePeerForward {
    expected: GatewayPeerForwardAccepted,
    expires_at_unix_ms: UnixMillis,
}

struct InboundState {
    last_sequence: u64,
    streams: BTreeMap<GatewayConnectionId, InboundStream>,
}

struct InboundStream {
    request_id: RequestId,
    writer: AgentControlInputWriter,
    task: tokio::task::JoinHandle<()>,
}

struct OwnedAgentRoute {
    request_id: RequestId,
}

/// Removes a stream directory entry when its worker exits for any reason, including a rejected
/// route grant or task cancellation. The request ID check prevents a late worker from deleting a
/// newer stream that happens to reuse the same transport stream ID.
struct AgentStreamCleanup {
    session: Arc<CentralGatewaySession>,
    stream_id: GatewayConnectionId,
    request_id: RequestId,
}

impl Drop for AgentStreamCleanup {
    fn drop(&mut self) {
        let _cleanup_gate = self
            .session
            .cleanup_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inbound = self
            .session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inbound
            .streams
            .get(&self.stream_id)
            .is_some_and(|stream| stream.request_id == self.request_id)
        {
            inbound.streams.remove(&self.stream_id);
        }
        // The worker can exit after Central has acquired the durable route but before Gateway
        // receives a grant or can emit its normal RouteRelease.  Schedule an exact, idempotent
        // authority cleanup from Drop so an individual stream failure does not pin the Agent
        // route until the lease TTL.  The release helper re-reads the route generation before
        // mutating it, so a replacement owner is never touched by this stale cleanup.
        drop(inbound);
        let owner = RouteOwner {
            stream_id: self.stream_id.clone(),
            request_id: self.request_id.clone(),
        };
        // Always schedule the exact cleanup, even when a synchronous session fence already
        // drained the ownership reservation. The worker may have committed the route after that
        // fence's local scan but before its cancellation was observed; the request/stream binding
        // below makes this late release harmless when a replacement owner has taken over.
        self.session.take_owned_route(&owner);
        self.session.schedule_route_release(vec![owner]);
    }
}

struct OutboundState {
    next_sequence: u64,
    sender: mpsc::Sender<GatewayControlFrame>,
}

struct RouteReleaseState {
    /// Every cleanup request owns a task handle. Individual Agent stream workers can exit
    /// independently, so a one-shot session-wide marker would lose later route IDs. Duplicate
    /// releases are harmless because the authority mutation is bound to the exact route fence.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

struct RouteOwner {
    stream_id: GatewayConnectionId,
    request_id: RequestId,
}

impl CentralGatewaySession {
    /// Applies one ordered Replica frame. Any identity, generation, sequence, or deadline error
    /// is terminal for the concrete connector and must close the underlying H2 connection.
    pub async fn accept(
        self: &Arc<Self>,
        frame: GatewayControlFrame,
    ) -> Result<(), GatewaySessionError> {
        self.accept_with_dispatch(frame, false).await
    }

    /// Applies one connector frame while dispatching independent unary Agent work in bounded
    /// background tasks. Sequence, identity and route mutations remain ordered here, but a slow
    /// enrollment/status/report handler cannot stop the connector from reading Replica heartbeats.
    pub(crate) async fn accept_from_connector(
        self: &Arc<Self>,
        frame: GatewayControlFrame,
    ) -> Result<(), GatewaySessionError> {
        self.accept_with_dispatch(frame, true).await
    }

    async fn accept_with_dispatch(
        self: &Arc<Self>,
        frame: GatewayControlFrame,
        background_unary: bool,
    ) -> Result<(), GatewaySessionError> {
        // Hold the admission read lease across the authoritative dispatch. A replacement session
        // takes the write lease before returning from `open`, so an old frame that already passed
        // identity checks is allowed to finish, while no new frame can begin after fencing.
        let _admission = self.admission.read().await;
        let _gate = self.accept_gate.lock().await;
        let result = self.accept_inner(frame, background_unary).await;
        if result.is_err() {
            self.fence_local_state();
        }
        result
    }

    async fn accept_inner(
        self: &Arc<Self>,
        frame: GatewayControlFrame,
        background_unary: bool,
    ) -> Result<(), GatewaySessionError> {
        frame.validate_at(self.clock.now())?;
        if frame.gateway_pool_id != self.identity.gateway_pool_id
            || frame.gateway_replica_id != self.identity.gateway_replica_id
            || frame.connection_id != self.connection_id
            || frame.hop_count != 0
        {
            return Err(GatewaySessionError::Identity);
        }
        self.ensure_current_replica().await?;
        {
            let mut inbound = self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let expected = inbound.last_sequence.saturating_add(1);
            if frame.sequence.get() != expected {
                return Err(GatewaySessionError::Protocol(
                    "Gateway frame sequence is duplicated or out of order",
                ));
            }
            inbound.last_sequence = frame.sequence.get();
        }

        let request_id = frame.request_id.clone();
        let trace_id = frame.trace_id.clone();
        match frame.message {
            GatewayControlMessage::ReplicaHeartbeat(_) => self.record_heartbeat().await,
            GatewayControlMessage::Drain(_) => self.record_drain().await,
            GatewayControlMessage::AgentRequest(request) if background_unary => {
                self.spawn_agent_request(request_id, trace_id, request)
                    .await
            }
            GatewayControlMessage::AgentRequest(request) => {
                self.handle_agent_request(request_id, trace_id, request)
                    .await
            }
            GatewayControlMessage::AgentStreamOpen(open) => {
                self.open_agent_stream(request_id, trace_id, open).await
            }
            GatewayControlMessage::AgentStreamData(data) => {
                self.write_agent_stream(request_id, trace_id, data).await
            }
            GatewayControlMessage::AgentStreamEnd(end) => {
                self.end_agent_stream(request_id, end).await
            }
            // A route is only valid when it is established together with the authenticated
            // Agent session opened by `AgentStreamOpen`; standalone route acquisition is rejected
            // before any authority mutation.
            GatewayControlMessage::RouteAcquire(_) => Err(GatewaySessionError::Protocol(
                "standalone route acquire is not supported; open an Agent stream to acquire the route atomically",
            )),
            GatewayControlMessage::RouteRenew(request) => {
                self.renew_route(request_id, trace_id, request).await
            }
            GatewayControlMessage::RouteRelease(request) => {
                self.release_route(request_id, trace_id, request).await
            }
            GatewayControlMessage::PeerForwardAccepted(accepted) => {
                self.accept_peer_forward_ack(request_id, accepted).await
            }
            GatewayControlMessage::Error(error) => {
                self.accept_peer_forward_error(&request_id, error).await
            }
            GatewayControlMessage::Backpressure(_) => Ok(()),
            GatewayControlMessage::ReplicaHello(_)
            | GatewayControlMessage::AgentResponse(_)
            | GatewayControlMessage::RouteGranted(_)
            | GatewayControlMessage::RouteFence(_)
            | GatewayControlMessage::S3ReadRevocation(_)
            | GatewayControlMessage::PeerDirectory(_)
            | GatewayControlMessage::PeerForward(_) => Err(GatewaySessionError::Protocol(
                "Gateway sent a Central-only control message",
            )),
        }
    }

    /// Re-reads Central authority before processing or emitting anything on this connection.
    ///
    /// Replica state and certificate generation are mutable Registry facts, so the identity
    /// captured during the TLS handshake is not sufficient to authorize a long-lived session.
    /// The session directory check also fences a superseded connection for the same Replica.
    async fn ensure_current_replica(&self) -> Result<(), GatewaySessionError> {
        if self.fenced.load(Ordering::Acquire) {
            self.fence_local_state();
            return Err(GatewaySessionError::Identity);
        }
        let result = self.check_current_replica().await;
        if result.is_err() {
            self.fence_local_state();
        }
        result
    }

    pub(crate) fn subscribe_fence(&self) -> watch::Receiver<bool> {
        self.fence_signal.subscribe()
    }

    pub(crate) fn close(&self) {
        self.fence_local_state();
        // `close` is also used by the connector's synchronous Drop guard. Start the exact route
        // cleanup here so a normal H2 EOF does not rely on a later replacement connection to
        // release its Agent leases. `fence_and_wait` will adopt and await these same handles when
        // a replacement arrives.
        self.schedule_all_route_releases();
    }

    /// Fences this session and waits for every already-admitted Central frame to finish. This is
    /// used when a replacement connection is installed; the synchronous local marker remains
    /// available for error paths and Drop guards that cannot await.
    async fn fence_and_wait(&self) {
        // One deadline covers admission, worker joins, and authority cleanup. A worker retained
        // after a normal AgentStreamEnd is not necessarily finished; waiting on it without this
        // bound would make a replacement connection depend on an untrusted Agent handler.
        let cleanup_deadline = tokio::time::Instant::now() + ROUTE_CLEANUP_WAIT_TIMEOUT;
        self.fenced.store(true, Ordering::Release);
        // Abort background stream/unary tasks before waiting for the write lease. An Agent stream
        // opener may be holding an admission read lease while it awaits its first upstream line;
        // draining first lets cancellation release that lease instead of making replacement wait
        // on an unbounded handler.
        self.fence_local_state_inner();
        let admission = timeout_at(cleanup_deadline, self.admission.write())
            .await
            .ok();
        // Wait for any worker Drop guard that is currently publishing its cleanup marker. The
        // guard is synchronous and short-lived; taking this lock after admission guarantees a
        // replacement cannot race the stream-table removal/release scheduling pair.
        let _cleanup_gate = self
            .cleanup_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Route acquisition is performed while holding an admission read lease.  Wait until all
        // such work has drained before taking ownership of the pending cleanup scan; otherwise a
        // route that commits just after the scan would remain pinned until its TTL.
        let stream_tasks = self
            .aborted_stream_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect::<Vec<_>>();
        // `AgentStreamEnd` removes a stream from `inbound` before its worker necessarily exits,
        // so those handles are already in the retained list rather than the map just drained by
        // `fence_local_state_inner`. Abort every retained worker before joining it, including
        // workers from a previous normal End path.
        for stream_task in &stream_tasks {
            stream_task.abort();
        }
        drop(_cleanup_gate);
        drop(admission);
        // Abort is cooperative at the Tokio task boundary. Await every worker before scanning
        // the authoritative route table so a late `AgentStreamCleanup::Drop` cannot race a new
        // connection's route acquisition.
        for stream_task in stream_tasks {
            if timeout_at(cleanup_deadline, stream_task).await.is_err() {
                tracing::warn!(
                    "disconnected Gateway stream cleanup exceeded its bounded wait; replacement admission will continue"
                );
                break;
            }
        }
        self.schedule_all_route_releases();
        // A worker may run its Drop guard while the aborts above are being observed. Drain all
        // tasks that were already scheduled, then check once more for a late Drop-triggered task
        // before returning the replacement admission boundary. Use one shared deadline so a
        // broken authority cannot make replacement admission wait once per route.
        loop {
            let now = tokio::time::Instant::now();
            if now >= cleanup_deadline {
                self.route_release
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .tasks
                    .drain(..)
                    .for_each(|task| task.abort());
                tracing::warn!(
                    "disconnected Gateway route cleanup exceeded its bounded wait; replacement admission will continue"
                );
                break;
            }
            let route_releases = self
                .route_release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tasks
                .drain(..)
                .collect::<Vec<_>>();
            if route_releases.is_empty() {
                break;
            }
            for route_release in route_releases {
                match timeout_at(cleanup_deadline, route_release).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::debug!(
                        %error,
                        "disconnected Gateway route cleanup task failed"
                    ),
                    Err(_) => {
                        tracing::warn!(
                            "disconnected Gateway route cleanup exceeded its bounded wait; replacement admission will continue"
                        );
                        break;
                    }
                }
            }
        }
    }

    async fn check_current_replica(&self) -> Result<(), GatewaySessionError> {
        let replica = self
            .registry
            .get_replica(&self.identity.gateway_replica_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        let pool = self
            .registry
            .get_pool(&self.identity.gateway_pool_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        let now = self.clock.now();
        if replica.gateway_pool_id != self.identity.gateway_pool_id
            || replica.edge_cluster_id != self.identity.edge_cluster_id
            || replica.state != GatewayReplicaState::Active
            || replica.credential.state != GatewayCredentialState::Active
            || replica.credential.certificate_generation
                != Some(self.identity.certificate_generation)
            || replica
                .credential
                .certificate_not_after_unix_ms
                .is_none_or(|expires_at| expires_at.get() <= now.get())
            || pool.edge_cluster_id != self.identity.edge_cluster_id
            || pool.state != GatewayPoolState::Ready
        {
            return Err(GatewaySessionError::Identity);
        }
        let is_current = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(&self.identity.gateway_replica_id)
                .is_some_and(|session| std::ptr::eq(session.as_ptr(), self))
        };
        if !is_current || self.fenced.load(Ordering::Acquire) {
            return Err(GatewaySessionError::Identity);
        }
        Ok(())
    }

    fn fence_local_state(&self) {
        self.fence_local_state_inner();
    }

    fn fence_local_state_inner(&self) {
        let _cleanup_gate = self
            .cleanup_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let streams = {
            let mut inbound = self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.fenced.store(true, Ordering::Release);
            std::mem::take(&mut inbound.streams)
        };
        self.fence_signal.send_replace(true);
        let mut aborted_stream_tasks = self
            .aborted_stream_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in streams.into_values() {
            stream.task.abort();
            aborted_stream_tasks.push(stream.task);
        }
        // A stream may already have received AgentStreamEnd and therefore be retained only in
        // this join-handle list. Fence those workers as well before waiting for the admission
        // writer; otherwise one such worker could keep a read lease until its handler returns.
        for task in aborted_stream_tasks.iter() {
            task.abort();
        }
        drop(aborted_stream_tasks);
        let unary_tasks = {
            let mut unary_tasks = self
                .unary_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *unary_tasks)
        };
        for task in unary_tasks.into_values() {
            task.abort();
        }
        let pending = {
            let mut pending = self
                .pending_forwards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *pending)
        };
        drop(pending);
        self.late_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Releases route leases owned by streams on this fenced control session. The route table is
    /// authoritative and outlives the H2 socket, so merely clearing the in-memory stream map
    /// would force a reconnect to wait for the full lease TTL. Each release is bound to the old
    /// stream ID and route generation; if a replacement session won the route first, Central
    /// rejects this stale release without touching the replacement lease.
    fn schedule_all_route_releases(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // All production fencing occurs on a Tokio task. Keep the synchronous fence path
            // usable for embedded callers that construct a session outside a runtime.
            tracing::debug!("skipping disconnected Gateway route release without a Tokio runtime");
            return;
        };
        let owners = {
            let mut owned_routes = self
                .owned_routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *owned_routes)
                .into_iter()
                .map(|(stream_id, owned)| RouteOwner {
                    stream_id,
                    request_id: owned.request_id,
                })
                .collect::<Vec<_>>()
        };
        let mut route_release = self
            .route_release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.spawn_route_release_tasks(&mut route_release, handle, owners);
    }

    fn register_owned_route(&self, stream_id: GatewayConnectionId, request_id: RequestId) {
        self.owned_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(stream_id, OwnedAgentRoute { request_id });
    }

    fn track_stream_task(&self, task: tokio::task::JoinHandle<()>) {
        let mut tasks = self
            .aborted_stream_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Completed workers have already run their Drop cleanup. Reaping them before retaining
        // the next handle keeps a long-lived Gateway session from accumulating one handle per
        // short-lived Agent stream while still preserving unfinished workers for replacement
        // fencing.
        tasks.retain(|task| !task.is_finished());
        if !task.is_finished() {
            tasks.push(task);
        }
    }

    fn take_owned_route(&self, owner: &RouteOwner) -> bool {
        let mut routes = self
            .owned_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        routes
            .get(&owner.stream_id)
            .is_some_and(|route| route.request_id == owner.request_id)
            .then(|| routes.remove(&owner.stream_id))
            .flatten()
            .is_some()
    }

    fn schedule_route_release(&self, owners: Vec<RouteOwner>) {
        if owners.is_empty() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("skipping disconnected Gateway route release without a Tokio runtime");
            return;
        };
        let mut route_release = self
            .route_release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.spawn_route_release_tasks(&mut route_release, handle, owners);
    }

    fn spawn_route_release_tasks(
        &self,
        route_release: &mut RouteReleaseState,
        handle: tokio::runtime::Handle,
        owners: Vec<RouteOwner>,
    ) {
        if owners.is_empty() {
            return;
        }
        route_release.tasks.retain(|task| !task.is_finished());
        let registry = Arc::clone(&self.registry);
        let pool_id = self.identity.gateway_pool_id.clone();
        let replica_id = self.identity.gateway_replica_id.clone();
        let clock = Arc::clone(&self.clock);
        route_release.tasks.push(handle.spawn(async move {
            release_disconnected_gateway_routes(registry, clock, pool_id, replica_id, owners).await;
        }));
    }

    async fn send_agent_route_fence(
        &self,
        route: &AgentRouteLease,
        reason: &'static str,
    ) -> Result<(), GatewaySessionError> {
        self.send(
            fresh_route_fence_request_id()?,
            None,
            GatewayControlMessage::RouteFence(GatewayRouteFence {
                agent_id: route.agent_id.clone(),
                route_generation: route.route_generation,
                reason: reason.to_owned(),
            }),
        )
        .await
    }

    async fn send_route_fenced_error(
        &self,
        request_id: RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        error: CentralError,
    ) -> Result<(), GatewaySessionError> {
        debug_assert_eq!(error.code(), CentralErrorCode::GatewayRouteFenced);
        self.send(
            request_id,
            trace_id,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::RouteFenced,
                detail: error.message().to_owned(),
                retryable: false,
            }),
        )
        .await
    }

    async fn notify_fenced_route_owner(&self, route: &AgentRouteLease) {
        let owner = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(&route.gateway_replica_id)
                .and_then(Weak::upgrade)
                .filter(|session| {
                    session.identity.gateway_pool_id == route.gateway_pool_id
                        && session.identity.edge_cluster_id == route.edge_cluster_id
                        && session.owns_agent_route_stream(route)
                })
        };
        if let Some(owner) = owner {
            if let Err(error) = owner
                .send_agent_route_fence(route, "Agent route ownership changed")
                .await
            {
                tracing::warn!(
                    agent_id = %route.agent_id,
                    owner_replica_id = %route.gateway_replica_id,
                    route_generation = route.route_generation.get(),
                    %error,
                    "Failed to notify the previous Gateway route owner"
                );
            }
        }
    }

    /// Confirms that this concrete control session still carries the exact Agent stream that
    /// acquired a route. `AgentRouteLease::connection_id` is the Agent stream ID, not the Gateway
    /// control connection ID, so both it and the acquisition RequestId must match the session's
    /// inbound directory before a takeover fence is delivered.
    fn owns_agent_route_stream(&self, route: &AgentRouteLease) -> bool {
        !self.fenced.load(Ordering::Acquire)
            && self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .streams
                .get(&route.connection_id)
                .is_some_and(|stream| stream.request_id == route.acquire_request_id)
    }

    async fn accept_peer_forward_ack(
        &self,
        request_id: RequestId,
        accepted: GatewayPeerForwardAccepted,
    ) -> Result<(), GatewaySessionError> {
        let pending = {
            // Keep the pending lock while consulting the late tombstones. A timeout removes the
            // waiter and records its binding under this same lock order, so an acknowledgement
            // cannot slip through the gap and fence the session before the tombstone is visible.
            let mut pending = self
                .pending_forwards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = pending.remove(&request_id) {
                Some(pending)
            } else {
                let mut late = self
                    .late_forwards
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                prune_late_peer_forwards(&mut late, self.clock.now());
                let Some(tombstone) = late.remove(&request_id) else {
                    return Err(GatewaySessionError::Protocol(
                        "Gateway acknowledged an unknown peer forward request",
                    ));
                };
                if tombstone.expected != accepted {
                    return Err(GatewaySessionError::Identity);
                }
                // The request already completed or timed out. A matching duplicate/late ACK is
                // harmless and must not tear down unrelated Replica traffic.
                return Ok(());
            }
        };
        let pending = pending.expect("pending peer forward was removed above");
        if pending.expected != accepted {
            return Err(GatewaySessionError::Identity);
        }
        pending
            .sender
            .send(Ok(()))
            .map_err(|_| GatewaySessionError::Closed)
    }

    async fn accept_peer_forward_error(
        &self,
        request_id: &RequestId,
        error: GatewayControlError,
    ) -> Result<(), GatewaySessionError> {
        let pending = {
            // Use the same pending -> late lock order as ACK handling and timeout cleanup.
            let mut pending = self
                .pending_forwards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = pending.remove(request_id) {
                Some(pending)
            } else {
                let mut late = self
                    .late_forwards
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                prune_late_peer_forwards(&mut late, self.clock.now());
                if late.remove(request_id).is_some() {
                    // A late error is already bound by the request ID tombstone. Its detailed
                    // payload is intentionally opaque at this point because no waiter remains.
                    return Ok(());
                }
                return Err(GatewaySessionError::Protocol(
                    "Gateway returned an error for an unknown peer forward request",
                ));
            }
        };
        let pending = pending.expect("pending peer forward was removed above");
        pending
            .sender
            .send(Err(error))
            .map_err(|_| GatewaySessionError::Closed)
    }

    async fn send_peer_forward(
        &self,
        request_id: RequestId,
        request: GatewayPeerForwardRequest,
    ) -> Result<(), GatewaySessionError> {
        self.send_peer_forward_with_timeout(
            request_id,
            request,
            Duration::from_millis(CONTROL_RESPONSE_DEADLINE_MS),
        )
        .await
    }

    async fn send_peer_forward_with_timeout(
        &self,
        request_id: RequestId,
        request: GatewayPeerForwardRequest,
        response_timeout: Duration,
    ) -> Result<(), GatewaySessionError> {
        let expected = GatewayPeerForwardAccepted {
            source_replica_id: request.source_replica_id.clone(),
            target_replica_id: request.target_replica_id.clone(),
            agent_id: request.agent_id.clone(),
            agent_connection_id: request.agent_connection_id.clone(),
            session_generation: request.session_generation,
            route_generation: request.route_generation,
        };
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self
                .pending_forwards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut late = self
                .late_forwards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prune_late_peer_forwards(&mut late, self.clock.now());
            if self.fenced.load(Ordering::Acquire) {
                return Err(GatewaySessionError::Identity);
            }
            if late.contains_key(&request_id) {
                return Err(GatewaySessionError::Protocol(
                    "peer forward request ID is still in its late-response window",
                ));
            }
            match pending.entry(request_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(PendingForward {
                        expected: expected.clone(),
                        sender,
                    });
                }
                Entry::Occupied(_) => {
                    return Err(GatewaySessionError::Protocol(
                        "peer forward request ID is already pending",
                    ));
                }
            }
        }
        if let Err(error) = self
            .send(
                request_id.clone(),
                None,
                GatewayControlMessage::PeerForward(request),
            )
            .await
        {
            self.move_pending_peer_forward_to_late(&request_id, expected.clone());
            return Err(error);
        }
        let result = match timeout(response_timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.move_pending_peer_forward_to_late(&request_id, expected.clone());
                return Err(GatewaySessionError::Closed);
            }
            Err(_) => {
                self.move_pending_peer_forward_to_late(&request_id, expected.clone());
                return Err(GatewaySessionError::RouteUnavailable(
                    "owner forwarding acknowledgement timed out",
                ));
            }
        };
        match result {
            Ok(()) => {
                self.pending_forwards
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&request_id);
                Ok(())
            }
            Err(error) if error.code == GatewayErrorCode::RouteFenced => Err({
                self.move_pending_peer_forward_to_late(&request_id, expected.clone());
                GatewaySessionError::RouteUnavailable("owner route was fenced before delivery")
            }),
            Err(_) => {
                self.move_pending_peer_forward_to_late(&request_id, expected);
                Err(GatewaySessionError::RouteUnavailable(
                    "owner Replica rejected peer forwarding",
                ))
            }
        }
    }

    /// Atomically replaces a pending peer-forward waiter with a bounded late-response tombstone.
    /// Keeping both maps under the pending -> late lock order closes the timeout/ACK race: an
    /// incoming ACK cannot observe neither the waiter nor its tombstone.
    fn move_pending_peer_forward_to_late(
        &self,
        request_id: &RequestId,
        expected: GatewayPeerForwardAccepted,
    ) {
        let mut pending = self
            .pending_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pending.remove(request_id);
        let mut late = self
            .late_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        remember_late_peer_forward(&mut late, request_id.clone(), expected, self.clock.now());
    }

    /// Builds and emits the current Central-authoritative peer credential allow-list. Endpoint
    /// discovery remains outside this payload: peer forwarding still uses the persisted target
    /// endpoint from the route decision, while this directory only answers whether the TLS leaf
    /// is the currently active credential for its source Replica.
    /// Publishes the initial credential allow-list for a newly established H2 connector.
    ///
    /// The network connector calls this immediately after `CentralGatewayControl::open` returns,
    /// before it starts forwarding the outbound queue. Keeping the transport bootstrap step in
    /// the connector avoids making the domain session API perform an implicit Registry page walk
    /// for in-process callers while preserving the wire ordering (directory before heartbeat or
    /// application traffic).
    pub(crate) async fn send_peer_directory(&self) -> Result<(), GatewaySessionError> {
        let directory = self.build_peer_directory().await?;
        self.send(
            fresh_control_request_id("peer-directory")?,
            None,
            GatewayControlMessage::PeerDirectory(directory),
        )
        .await
    }

    async fn build_peer_directory(&self) -> Result<GatewayPeerDirectory, GatewaySessionError> {
        let now = self.clock.now();
        let mut entries = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .registry
                .list_replicas(&GatewayReplicaListRequest {
                    gateway_pool_id: self.identity.gateway_pool_id.clone(),
                    state: Some(GatewayReplicaState::Active),
                    after: after.clone(),
                    limit: GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                })
                .await?;
            let page_len = page.len();
            let page_after = page
                .last()
                .map(|replica| replica.gateway_replica_id.clone());
            for replica in page {
                if replica.gateway_pool_id != self.identity.gateway_pool_id
                    || replica.edge_cluster_id != self.identity.edge_cluster_id
                    || replica.state != GatewayReplicaState::Active
                    || replica.credential.state != GatewayCredentialState::Active
                {
                    continue;
                }
                let (Some(certificate_generation), Some(certificate_fingerprint)) = (
                    replica.credential.certificate_generation,
                    replica.credential.certificate_fingerprint,
                ) else {
                    // Pending/partially prepared credentials are never peer-authoritative.
                    continue;
                };
                if replica
                    .credential
                    .certificate_not_after_unix_ms
                    .is_none_or(|expires_at| expires_at.get() <= now.get())
                {
                    continue;
                }
                entries.push(GatewayPeerDirectoryEntry {
                    gateway_replica_id: replica.gateway_replica_id,
                    certificate_generation,
                    certificate_fingerprint,
                });
            }
            if page_len < GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                break;
            }
            // Registry pagination is ordered by Replica ID. A full page without a final cursor
            // would otherwise loop forever; treat that malformed repository response as a hard
            // authority failure.
            if page_after.is_none() || page_after == after {
                return Err(GatewaySessionError::Protocol(
                    "Gateway Registry returned a full peer directory page without a cursor",
                ));
            }
            after = page_after;
        }
        let generation = self
            .peer_directory_generation
            .fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            return Err(GatewaySessionError::Protocol(
                "Gateway peer directory generation overflowed",
            ));
        }
        let expires_at =
            UnixMillis::new(now.get().checked_add(GATEWAY_PEER_DIRECTORY_TTL_MS).ok_or(
                GatewaySessionError::Protocol("Gateway peer directory expiry overflowed"),
            )?);
        let directory = GatewayPeerDirectory {
            directory_generation: Generation::new(generation),
            issued_at_unix_ms: now,
            expires_at_unix_ms: expires_at,
            replicas: entries,
        };
        directory.validate().map_err(GatewaySessionError::Wire)?;
        Ok(directory)
    }

    async fn record_heartbeat(&self) -> Result<(), GatewaySessionError> {
        let mut replica = self
            .registry
            .get_replica(&self.identity.gateway_replica_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        if replica.state != GatewayReplicaState::Active
            || replica.credential.state != GatewayCredentialState::Active
            || replica.credential.certificate_generation
                != Some(self.identity.certificate_generation)
        {
            return Err(GatewaySessionError::Identity);
        }
        let pool = self
            .registry
            .get_pool(&self.identity.gateway_pool_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        if pool.edge_cluster_id != self.identity.edge_cluster_id
            || pool.state != GatewayPoolState::Ready
        {
            return Err(GatewaySessionError::Identity);
        }
        let expected = replica.resource_version.get();
        let now = self.clock.now();
        replica.last_heartbeat_at_unix_ms = Some(now);
        replica.updated_at_unix_ms = now;
        replica.resource_version = neoengram_domain::protocol::ResourceVersion::new(
            expected
                .checked_add(1)
                .ok_or(GatewaySessionError::Protocol("resource version overflow"))?,
        );
        self.registry.replace_replica(expected, replica).await?;
        self.send_peer_directory().await
    }

    async fn record_drain(&self) -> Result<(), GatewaySessionError> {
        let mut replica = self
            .registry
            .get_replica(&self.identity.gateway_replica_id)
            .await?
            .ok_or(GatewaySessionError::Identity)?;
        if replica.state == GatewayReplicaState::Draining {
            self.fence_local_state();
            return Err(GatewaySessionError::Closed);
        }
        if replica.state != GatewayReplicaState::Active
            || replica.credential.state != GatewayCredentialState::Active
            || replica.credential.certificate_generation
                != Some(self.identity.certificate_generation)
        {
            return Err(GatewaySessionError::Identity);
        }
        let expected = replica.resource_version.get();
        replica.state = GatewayReplicaState::Draining;
        replica.updated_at_unix_ms = self.clock.now();
        replica.resource_version = neoengram_domain::protocol::ResourceVersion::new(
            expected
                .checked_add(1)
                .ok_or(GatewaySessionError::Protocol("resource version overflow"))?,
        );
        self.registry.replace_replica(expected, replica).await?;
        self.fence_local_state();
        Err(GatewaySessionError::Closed)
    }

    async fn spawn_agent_request(
        self: &Arc<Self>,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        request: GatewayAgentRequest,
    ) -> Result<(), GatewaySessionError> {
        if request.action == GatewayAgentAction::SessionChannelOpen
            || AgentAction::from_path(request.action.path()).is_none()
        {
            return Err(GatewaySessionError::Protocol(
                "Gateway requested an unsupported Agent action",
            ));
        }
        let stream_id = request.stream_id.clone();
        let rejection_stream_id = stream_id.clone();
        let rejection_request_id = request_id.clone();
        let rejection_trace_id = trace_id.clone();
        let weak_session = Arc::downgrade(self);
        let task_stream_id = stream_id.clone();
        let (start, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            if started.await.is_err() {
                return;
            }
            let Some(session) = weak_session.upgrade() else {
                return;
            };
            // Unary handlers may mutate Central state and outlive the frame admission call. Keep
            // them inside the session admission lease so a replacement fence waits for an
            // already-started handler instead of allowing it to commit afterward.
            let _admission = session.admission.read().await;
            if session.fenced.load(Ordering::Acquire) {
                session
                    .unary_tasks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&task_stream_id);
                return;
            }
            let result = session
                .handle_agent_request(request_id, trace_id, request)
                .await;
            session
                .unary_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&task_stream_id);
            if result.is_err() {
                session.fence_local_state();
            }
        });
        let task_abort = task.abort_handle();
        let registration = {
            let mut tasks = self
                .unary_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.fenced.load(Ordering::Acquire) {
                Err(GatewaySessionError::Identity)
            } else if tasks.len() >= MAX_ACTIVE_AGENT_REQUESTS {
                Err(GatewaySessionError::RouteUnavailable(
                    "Gateway unary request admission is full",
                ))
            } else {
                match tasks.entry(stream_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(task_abort.clone());
                        Ok(())
                    }
                    Entry::Occupied(_) => Err(GatewaySessionError::Protocol(
                        "Gateway Agent unary stream ID is already active",
                    )),
                }
            }
        };
        match registration {
            Ok(()) => start.send(()).map_err(|_| GatewaySessionError::Closed),
            Err(GatewaySessionError::RouteUnavailable(_)) => {
                task_abort.abort();
                let response = agent_error_response(
                    rejection_stream_id,
                    AgentHttpError::new(
                        http::StatusCode::TOO_MANY_REQUESTS,
                        "RESOURCE_EXHAUSTED",
                        "Gateway unary request admission is full",
                        true,
                    )
                    .with_retry_after_ms(1_000),
                )?;
                self.send(
                    rejection_request_id,
                    rejection_trace_id,
                    GatewayControlMessage::AgentResponse(response),
                )
                .await
            }
            Err(error) => {
                task_abort.abort();
                Err(error)
            }
        }
    }

    async fn handle_agent_request(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        request: GatewayAgentRequest,
    ) -> Result<(), GatewaySessionError> {
        if request.action == GatewayAgentAction::SessionChannelOpen {
            return Err(GatewaySessionError::Protocol(
                "the Agent control channel must use Gateway stream frames",
            ));
        }
        let action = AgentAction::from_path(request.action.path()).ok_or(
            GatewaySessionError::Protocol("Gateway requested an unsupported Agent action"),
        )?;
        let response = if request.body.as_bytes().len() > action.max_body_bytes() {
            agent_error_response(
                request.stream_id,
                AgentHttpError::new(
                    http::StatusCode::PAYLOAD_TOO_LARGE,
                    "PROTOCOL_LIMIT_EXCEEDED",
                    "Agent request body exceeds the operation limit",
                    false,
                ),
            )?
        } else {
            match timeout(
                Duration::from_millis(CONTROL_RESPONSE_DEADLINE_MS),
                self.agent_handler.handle(action, request.body.as_bytes()),
            )
            .await
            {
                Ok(Ok(body)) => GatewayAgentResponse {
                    stream_id: request.stream_id,
                    status: http::StatusCode::OK.as_u16(),
                    content_type: JSON_CONTENT_TYPE.to_owned(),
                    retry_after_ms: None,
                    body: GatewayOpaqueBytes::new(body)?,
                },
                Ok(Err(error)) => agent_error_response(request.stream_id, error)?,
                Err(_) => agent_error_response(
                    request.stream_id,
                    AgentHttpError::new(
                        http::StatusCode::GATEWAY_TIMEOUT,
                        "DEADLINE_EXCEEDED",
                        "Agent request exceeded its bounded deadline",
                        true,
                    ),
                )?,
            }
        };
        self.send(
            request_id,
            trace_id,
            GatewayControlMessage::AgentResponse(response),
        )
        .await
    }

    async fn deliver_agent_frame(
        &self,
        request_id: RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        established_route: &AgentRouteLease,
        stream_id: &GatewayConnectionId,
        encoded_frame: Bytes,
    ) -> Result<(), GatewaySessionError> {
        if established_route.gateway_pool_id != self.identity.gateway_pool_id
            || established_route.gateway_replica_id != self.identity.gateway_replica_id
            || established_route.edge_cluster_id != self.identity.edge_cluster_id
            || established_route.connection_id != *stream_id
        {
            self.fence_local_state();
            return Err(GatewaySessionError::Identity);
        }

        let current_route = match self
            .registry
            .get_agent_route(&established_route.agent_id)
            .await
        {
            Ok(route) => route,
            Err(error) => {
                let _ = self
                    .send_agent_route_fence(
                        established_route,
                        "Agent route authority is unavailable",
                    )
                    .await;
                return Err(GatewaySessionError::Registry(error));
            }
        };
        if current_route.as_ref().is_none_or(|current| {
            !current.is_active_at(self.clock.now()) || !same_route_fence(established_route, current)
        }) {
            let _ = self
                .send_agent_route_fence(
                    established_route,
                    "Agent route ownership changed or expired",
                )
                .await;
            return Err(GatewaySessionError::RouteUnavailable(
                "Agent owner route changed before fallback delivery",
            ));
        }

        // A single Gateway stream-data message is the only point where failure is atomic: either
        // the owner control queue accepted the entire Agent frame or no Agent bytes were queued.
        // Once accepted, a later transport failure is ambiguous and must be recovered by the
        // durable outbox on the Agent's next session; peer replay here could duplicate a frame
        // that the owner already delivered. Multi-chunk frames likewise stay on the owner path
        // because forwarding after a partial send could corrupt Agent NDJSON ordering.
        if encoded_frame.len() <= MAX_GATEWAY_STREAM_CHUNK_BYTES {
            let forwardable_command = forwarded_command_generation(&encoded_frame)
                .is_some_and(|generation| generation == established_route.session_generation);
            let message = GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                stream_id: stream_id.clone(),
                chunk: GatewayOpaqueBytes::new(encoded_frame.to_vec())?,
            });
            match self.send_inner(request_id, trace_id, message).await {
                Ok(()) => return Ok(()),
                Err(GatewaySessionError::Closed) if forwardable_command => {
                    let result = self
                        .forward_agent_command_after_owner_closed(established_route, encoded_frame)
                        .await;
                    self.fence_local_state();
                    return result;
                }
                Err(error) => {
                    self.fence_local_state();
                    return Err(error);
                }
            }
        }

        for chunk in encoded_frame.chunks(MAX_GATEWAY_STREAM_CHUNK_BYTES) {
            let chunk = GatewayOpaqueBytes::new(chunk.to_vec())?;
            self.send(
                request_id.clone(),
                trace_id.clone(),
                GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                    stream_id: stream_id.clone(),
                    chunk,
                }),
            )
            .await?;
        }
        Ok(())
    }

    /// Re-checks the Central-authoritative Agent route immediately before the initial grant.
    ///
    /// Acquiring the route and writing the grant are separate operations: a replacement Gateway
    /// Replica can take over while the Agent handler is still preparing its channel.  Treat the
    /// grant as valid only when every route fence field still matches; later stream delivery also
    /// performs this check, but rejecting the stale grant keeps the Gateway's pending stream from
    /// briefly becoming an apparently active route.
    async fn ensure_current_agent_route(
        &self,
        expected: &AgentRouteLease,
    ) -> Result<(), GatewaySessionError> {
        if self.fenced.load(Ordering::Acquire)
            || expected.gateway_pool_id != self.identity.gateway_pool_id
            || expected.gateway_replica_id != self.identity.gateway_replica_id
            || expected.edge_cluster_id != self.identity.edge_cluster_id
        {
            return Err(GatewaySessionError::Identity);
        }
        let current = self.registry.get_agent_route(&expected.agent_id).await?;
        if current.as_ref().is_none_or(|route| {
            !route.is_active_at(self.clock.now()) || !same_route_fence(expected, route)
        }) {
            return Err(GatewaySessionError::RouteUnavailable(
                "Agent route ownership changed before route grant",
            ));
        }
        Ok(())
    }

    async fn reject_agent_route_grant(
        &self,
        request_id: RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        error: &GatewaySessionError,
    ) {
        let (code, retryable, detail) = match error {
            GatewaySessionError::RouteUnavailable(_) => (
                GatewayErrorCode::RouteUnavailable,
                true,
                "Agent route ownership changed before route grant",
            ),
            GatewaySessionError::Registry(_) => (
                GatewayErrorCode::RouteUnavailable,
                true,
                "Agent route authority is temporarily unavailable",
            ),
            _ => return,
        };
        let _ = self
            .send(
                request_id,
                trace_id,
                GatewayControlMessage::Error(GatewayControlError {
                    code,
                    detail: detail.to_owned(),
                    retryable,
                }),
            )
            .await;
    }

    async fn forward_agent_command_after_owner_closed(
        &self,
        established_route: &AgentRouteLease,
        encoded_frame: Bytes,
    ) -> Result<(), GatewaySessionError> {
        let via_replica_id = {
            let sessions = self.sessions.lock().await;
            sessions.iter().find_map(|(replica_id, session)| {
                if replica_id == &established_route.gateway_replica_id {
                    return None;
                }
                let session = session.upgrade()?;
                (!session.fenced.load(Ordering::Acquire)
                    && session.identity.gateway_pool_id == established_route.gateway_pool_id
                    && session.identity.edge_cluster_id == established_route.edge_cluster_id)
                    .then(|| replica_id.clone())
            })
        }
        .ok_or(GatewaySessionError::RouteUnavailable(
            "no non-owner Gateway ingress session is available",
        ))?;

        // Select exactly one ingress. Retrying another Replica after an acknowledgement timeout
        // could duplicate a command that the owner already accepted, so failure is returned to the
        // durable delivery loop instead of broadcasting or trying a second path.
        forward_agent_frame_via_authority(
            &self.registry,
            &self.sessions,
            self.clock.as_ref(),
            &via_replica_id,
            &established_route.agent_id,
            Some(established_route),
            encoded_frame,
        )
        .await
    }

    async fn open_agent_stream(
        self: &Arc<Self>,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        open: GatewayAgentStreamOpen,
    ) -> Result<(), GatewaySessionError> {
        if open.action != GatewayAgentAction::SessionChannelOpen {
            return Err(GatewaySessionError::Protocol(
                "Gateway stream action is not the Agent control channel",
            ));
        }
        let (writer, input) = AgentControlInput::channel(AGENT_STREAM_INPUT_BUFFER);
        // The transport connection is the session owner. A background Agent task must not keep
        // the session and its input writer alive after that connection disappears.
        let weak_session = Arc::downgrade(self);
        let (start, started) = oneshot::channel();
        let task_request_id = request_id.clone();
        let task_trace_id = trace_id.clone();
        let task_open = open.clone();
        let task = tokio::spawn(async move {
            if started.await.is_err() {
                return;
            }
            let request_id = task_request_id;
            let trace_id = task_trace_id;
            let open = task_open;
            let Some(session) = weak_session.upgrade() else {
                return;
            };
            let _cleanup = AgentStreamCleanup {
                session: session.clone(),
                stream_id: open.stream_id.clone(),
                request_id: request_id.clone(),
            };
            let observed_at_unix_ms = session.clock.now();
            let handler = session.agent_handler.clone();
            let route = GatewayAgentRouteContext {
                route_request_id: request_id.clone(),
                gateway_pool_id: session.identity.gateway_pool_id.clone(),
                gateway_replica_id: session.identity.gateway_replica_id.clone(),
                connection_id: open.stream_id.clone(),
                observed_at_unix_ms,
                lease_expires_at_unix_ms: UnixMillis::new(
                    observed_at_unix_ms
                        .get()
                        .saturating_add(AGENT_ROUTE_LEASE_TTL_MS),
                ),
                heartbeat_timeout_ms: AGENT_ROUTE_LEASE_TTL_MS,
            };
            // Route acquisition is a Central registry mutation. Keep it inside the admission
            // read lease so replacement fencing waits for an already-started opener; the fence
            // path aborts this task before taking its write lease if the opener is still waiting
            // on the Agent's first line.
            let result = {
                let _admission = session.admission.read().await;
                if session.fenced.load(Ordering::Acquire) {
                    return;
                }
                handler.open_routed_control_channel(input, route).await
            };
            drop(session);
            match result {
                Ok(mut routed) => {
                    let established_route = routed.route.clone();
                    let Some(session) = weak_session.upgrade() else {
                        return;
                    };
                    if let Err(error) = session.ensure_current_agent_route(&established_route).await
                    {
                        session
                            .reject_agent_route_grant(request_id.clone(), trace_id.clone(), &error)
                            .await;
                        return;
                    }
                    if let Some(fenced) = routed.fenced.as_ref() {
                        session.notify_fenced_route_owner(fenced).await;
                    }
                    // The takeover notification is asynchronous. Re-read after it as well so a
                    // second takeover cannot turn the grant below into another stale authority.
                    if let Err(error) = session.ensure_current_agent_route(&established_route).await
                    {
                        session
                            .reject_agent_route_grant(request_id.clone(), trace_id.clone(), &error)
                            .await;
                        return;
                    }
                    if session
                        .send_route_granted(
                            request_id.clone(),
                            trace_id.clone(),
                            routed.route,
                            routed.replayed,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                    drop(session);
                    let channel = &mut routed.channel;
                    while let Some(frame) = channel.next_frame().await {
                        let Some(session) = weak_session.upgrade() else {
                            return;
                        };
                        let _admission = session.admission.read().await;
                        if session.fenced.load(Ordering::Acquire) {
                            return;
                        }
                        let result = session
                            .deliver_agent_frame(
                                request_id.clone(),
                                trace_id.clone(),
                                &established_route,
                                &open.stream_id,
                                frame,
                            )
                            .await;
                        drop(_admission);
                        if result.is_err() {
                            return;
                        }
                    }
                    let Some(session) = weak_session.upgrade() else {
                        return;
                    };
                    let _admission = session.admission.read().await;
                    if session.fenced.load(Ordering::Acquire) {
                        return;
                    }
                    let _ = session
                        .send(
                            request_id,
                            trace_id,
                            GatewayControlMessage::AgentStreamEnd(GatewayAgentStreamEnd {
                                stream_id: open.stream_id,
                            }),
                        )
                        .await;
                }
                Err(error) => {
                    let Some(session) = weak_session.upgrade() else {
                        return;
                    };
                    let _ = session
                        .send(
                            request_id,
                            trace_id,
                            GatewayControlMessage::Error(gateway_error(&error)),
                        )
                        .await;
                }
            }
        });
        let task_abort = task.abort_handle();
        let mut task = Some(task);
        let stream_id = open.stream_id.clone();
        let registration = {
            let mut inbound = self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.fenced.load(Ordering::Acquire) {
                Err(GatewaySessionError::Identity)
            } else if inbound.streams.len() >= MAX_ACTIVE_AGENT_STREAMS {
                Ok(true)
            } else {
                match inbound.streams.entry(stream_id.clone()) {
                    Entry::Vacant(stream) => {
                        stream.insert(InboundStream {
                            request_id: request_id.clone(),
                            writer,
                            task: task
                                .take()
                                .expect("stream worker handle must be installed once"),
                        });
                        Ok(false)
                    }
                    Entry::Occupied(_) => Err(GatewaySessionError::Protocol(
                        "Gateway Agent stream ID is already active",
                    )),
                }
            }
        };
        if matches!(registration, Ok(false)) {
            self.register_owned_route(stream_id, request_id.clone());
        }
        match registration {
            Ok(false) => start.send(()).map_err(|_| GatewaySessionError::Closed),
            Ok(true) => {
                task_abort.abort();
                if let Some(task) = task.take() {
                    self.track_stream_task(task);
                }
                self.send(
                    request_id,
                    trace_id,
                    GatewayControlMessage::Backpressure(GatewayBackpressure {
                        retry_after_ms: 1_000,
                    }),
                )
                .await
            }
            Err(error) => {
                task_abort.abort();
                if let Some(task) = task.take() {
                    self.track_stream_task(task);
                }
                Err(error)
            }
        }
    }

    async fn write_agent_stream(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        data: GatewayAgentStreamData,
    ) -> Result<(), GatewaySessionError> {
        let writer = {
            let mut inbound = self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.fenced.load(Ordering::Acquire) {
                return Err(GatewaySessionError::Identity);
            }
            let stream =
                inbound
                    .streams
                    .get(&data.stream_id)
                    .ok_or(GatewaySessionError::Protocol(
                        "Gateway Agent stream is not active",
                    ))?;
            if stream.request_id != request_id {
                let stream = inbound.streams.remove(&data.stream_id);
                if let Some(stream) = stream {
                    stream.task.abort();
                    drop(stream.writer);
                    self.track_stream_task(stream.task);
                }
                return Err(GatewaySessionError::Protocol(
                    "Gateway Agent stream request ID does not match its bound request",
                ));
            }
            stream.writer.clone()
        };
        if self.fenced.load(Ordering::Acquire) {
            return Err(GatewaySessionError::Identity);
        }
        match writer.try_send(Bytes::copy_from_slice(data.chunk.as_bytes())) {
            Ok(()) => Ok(()),
            Err(AgentControlInputTrySendError::Closed) => {
                // The Agent task already ended; dropping the registry entry keeps this frame
                // from affecting unrelated streams while allowing the control session to proceed.
                self.inbound
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .streams
                    .remove(&data.stream_id);
                Ok(())
            }
            Err(AgentControlInputTrySendError::Full) => {
                let stream = self
                    .inbound
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .streams
                    .remove(&data.stream_id);
                if let Some(stream) = stream {
                    stream.task.abort();
                    self.track_stream_task(stream.task);
                }
                self.send(
                    request_id,
                    trace_id,
                    GatewayControlMessage::Error(GatewayControlError {
                        code: GatewayErrorCode::ResourceExhausted,
                        detail: "Agent control input queue is full; stream was closed".to_owned(),
                        retryable: true,
                    }),
                )
                .await
            }
        }
    }

    async fn end_agent_stream(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        end: GatewayAgentStreamEnd,
    ) -> Result<(), GatewaySessionError> {
        let stream = {
            let mut inbound = self
                .inbound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.fenced.load(Ordering::Acquire) {
                return Err(GatewaySessionError::Identity);
            }
            let stream =
                inbound
                    .streams
                    .get(&end.stream_id)
                    .ok_or(GatewaySessionError::Protocol(
                        "Gateway Agent stream is not active",
                    ))?;
            if stream.request_id != request_id {
                let stream = inbound.streams.remove(&end.stream_id);
                if let Some(stream) = stream {
                    stream.task.abort();
                    drop(stream.writer);
                    self.track_stream_task(stream.task);
                }
                return Err(GatewaySessionError::Protocol(
                    "Gateway Agent stream request ID does not match its bound request",
                ));
            }
            inbound
                .streams
                .remove(&end.stream_id)
                .expect("stream was present while holding the inbound lock")
        };
        drop(stream.writer);
        self.track_stream_task(stream.task);
        Ok(())
    }

    async fn renew_route(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        request: GatewayRouteLeaseRequest,
    ) -> Result<(), GatewaySessionError> {
        self.validate_route_request(&request, true)?;
        let route_generation = request
            .route_generation
            .ok_or(GatewaySessionError::Protocol(
                "route renew requires route_generation",
            ))?;
        let outcome = match self
            .registry
            .renew_agent_route(RenewAgentRouteLeaseRequest {
                request_id: request_id.clone(),
                agent_id: request.agent_id.clone(),
                gateway_replica_id: self.identity.gateway_replica_id.clone(),
                connection_id: request.agent_connection_id.clone(),
                session_generation: request.session_generation,
                route_generation,
                renewed_at_unix_ms: self.clock.now(),
                lease_expires_at_unix_ms: request.requested_expires_at_unix_ms,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if error.code() == CentralErrorCode::GatewayRouteFenced => {
                // A takeover is expected during normal Replica failover. Fence only this Agent
                // route and answer the correlated mutation; do not tear down unrelated Agents
                // sharing the same Gateway control session. The correlated RouteFenced error
                // drives Gateway stream teardown. Retain the Central entry until its normal
                // AgentStreamEnd absorbs a concurrently queued end frame.
                return self
                    .send_route_fenced_error(request_id, trace_id, error)
                    .await;
            }
            Err(error) => return Err(GatewaySessionError::Registry(error)),
        };
        self.send_route_granted(request_id, trace_id, outcome.lease, outcome.replayed)
            .await
    }

    async fn release_route(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        request: GatewayRouteLeaseRequest,
    ) -> Result<(), GatewaySessionError> {
        self.validate_route_request(&request, true)?;
        let route_generation = request
            .route_generation
            .ok_or(GatewaySessionError::Protocol(
                "route release requires route_generation",
            ))?;
        let outcome = match self
            .registry
            .release_agent_route(ReleaseAgentRouteLeaseRequest {
                request_id: request_id.clone(),
                agent_id: request.agent_id.clone(),
                gateway_replica_id: self.identity.gateway_replica_id.clone(),
                connection_id: request.agent_connection_id.clone(),
                session_generation: request.session_generation,
                route_generation,
                released_at_unix_ms: self.clock.now(),
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if error.code() == CentralErrorCode::GatewayRouteFenced => {
                // Route release is commonly the final frame from an already-fenced stream. Treat
                // that expected race as a per-Agent result so another Agent on this Replica keeps
                // its control session and heartbeat alive. The correlated error lets Gateway
                // close this route while retaining the Central entry for a queued end frame.
                return self
                    .send_route_fenced_error(request_id, trace_id, error)
                    .await;
            }
            Err(error) => return Err(GatewaySessionError::Registry(error)),
        };
        self.send_route_granted(request_id, trace_id, outcome.lease, outcome.replayed)
            .await
    }

    fn validate_route_request(
        &self,
        request: &GatewayRouteLeaseRequest,
        generation_required: bool,
    ) -> Result<(), GatewaySessionError> {
        let now = self.clock.now().get();
        if request.owner_replica_id != self.identity.gateway_replica_id
            || generation_required != request.route_generation.is_some()
            || request.requested_expires_at_unix_ms.get() <= now
            || request.requested_expires_at_unix_ms.get()
                > now.saturating_add(AGENT_ROUTE_LEASE_TTL_MS)
        {
            return Err(GatewaySessionError::Protocol(
                "Gateway route request scope, generation, or TTL is invalid",
            ));
        }
        Ok(())
    }

    async fn send_route_granted(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        lease: AgentRouteLease,
        replayed: bool,
    ) -> Result<(), GatewaySessionError> {
        self.send(
            request_id,
            trace_id,
            GatewayControlMessage::RouteGranted(GatewayRouteLeaseGranted {
                agent_id: lease.agent_id,
                owner_replica_id: lease.gateway_replica_id,
                agent_connection_id: lease.connection_id,
                session_generation: lease.session_generation,
                route_generation: lease.route_generation,
                lease_expires_at_unix_ms: lease.lease_expires_at_unix_ms,
                replayed,
            }),
        )
        .await
    }

    async fn send(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        message: GatewayControlMessage,
    ) -> Result<(), GatewaySessionError> {
        let result = self.send_inner(request_id, trace_id, message).await;
        if result.is_err() {
            self.fence_local_state();
        }
        result
    }

    async fn send_inner(
        &self,
        request_id: neoengram_domain::protocol::RequestId,
        trace_id: Option<neoengram_domain::protocol::TraceId>,
        message: GatewayControlMessage,
    ) -> Result<(), GatewaySessionError> {
        self.ensure_current_replica().await?;
        let now = self.clock.now();
        let mut outbound = self.outbound.lock().await;
        if self.fenced.load(Ordering::Acquire) {
            return Err(GatewaySessionError::Identity);
        }
        let sequence = outbound.next_sequence;
        outbound.next_sequence = sequence
            .checked_add(1)
            .ok_or(GatewaySessionError::Protocol("Gateway sequence overflow"))?;
        let frame = GatewayControlFrame {
            wire_version: CURRENT_WIRE_VERSION,
            gateway_pool_id: self.identity.gateway_pool_id.clone(),
            gateway_replica_id: self.identity.gateway_replica_id.clone(),
            connection_id: self.connection_id.clone(),
            sequence: SequenceNumber::new(sequence),
            request_id,
            trace_id,
            sent_at_unix_ms: now,
            deadline_unix_ms: UnixMillis::new(
                now.get().saturating_add(CONTROL_RESPONSE_DEADLINE_MS),
            ),
            hop_count: 0,
            message,
            extensions: neoengram_domain::protocol::Extensions::new(),
        };
        frame.validate()?;
        match timeout(
            Duration::from_millis(CONTROL_RESPONSE_DEADLINE_MS),
            outbound.sender.send(frame),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(GatewaySessionError::Closed),
        }
    }
}

async fn release_disconnected_gateway_routes(
    registry: Arc<dyn GatewayRegistryRepository>,
    clock: Arc<dyn Clock>,
    pool_id: GatewayPoolId,
    replica_id: GatewayReplicaId,
    owners: Vec<RouteOwner>,
) {
    let owners = owners
        .into_iter()
        .map(|owner| (owner.stream_id, owner.request_id))
        .collect::<BTreeMap<_, _>>();
    let mut after = None;
    loop {
        let page = match timeout(
            ROUTE_CLEANUP_RPC_TIMEOUT,
            registry.list_agent_routes(&AgentRouteLeaseListRequest {
                gateway_pool_id: pool_id.clone(),
                gateway_replica_id: Some(replica_id.clone()),
                active_at_unix_ms: None,
                after: after.clone(),
                limit: GATEWAY_REGISTRY_MAX_PAGE_SIZE,
            }),
        )
        .await
        {
            Ok(Ok(page)) => page,
            Ok(Err(error)) => {
                tracing::warn!(
                    gateway_replica_id = %replica_id,
                    %error,
                    "failed to list Agent routes while fencing a disconnected Gateway"
                );
                return;
            }
            Err(_) => {
                tracing::warn!(
                    gateway_replica_id = %replica_id,
                    "timed out listing Agent routes while fencing a disconnected Gateway"
                );
                return;
            }
        };
        let page_len = page.len();
        for route in page.iter().filter(|route| {
            route.gateway_pool_id == pool_id
                && route.gateway_replica_id == replica_id
                && owners
                    .get(&route.connection_id)
                    .is_some_and(|request_id| request_id == &route.acquire_request_id)
        }) {
            let released_at =
                UnixMillis::new(clock.now().get().max(route.renewed_at_unix_ms.get()));
            let Ok(request_id) = fresh_control_request_id("disconnect-route-release") else {
                tracing::warn!(
                    agent_id = %route.agent_id,
                    "failed to allocate a disconnected Gateway route release identity"
                );
                continue;
            };
            match timeout(
                ROUTE_CLEANUP_RPC_TIMEOUT,
                registry.release_agent_route(ReleaseAgentRouteLeaseRequest {
                    request_id,
                    agent_id: route.agent_id.clone(),
                    gateway_replica_id: route.gateway_replica_id.clone(),
                    connection_id: route.connection_id.clone(),
                    session_generation: route.session_generation,
                    route_generation: route.route_generation,
                    released_at_unix_ms: released_at,
                }),
            )
            .await
            {
                Ok(Ok(_)) => tracing::debug!(
                    agent_id = %route.agent_id,
                    connection_id = %route.connection_id,
                    "released Agent route owned by disconnected Gateway"
                ),
                Ok(Err(error)) => tracing::debug!(
                    agent_id = %route.agent_id,
                    connection_id = %route.connection_id,
                    %error,
                    "disconnected Gateway route release was superseded or unavailable"
                ),
                Err(_) => tracing::warn!(
                    agent_id = %route.agent_id,
                    connection_id = %route.connection_id,
                    "timed out releasing Agent route owned by disconnected Gateway"
                ),
            }
        }
        if page_len < GATEWAY_REGISTRY_MAX_PAGE_SIZE {
            return;
        }
        let Some(next_after) = page.last().map(|route| route.agent_id.clone()) else {
            tracing::warn!(
                gateway_replica_id = %replica_id,
                "Gateway route listing returned a full page without a cursor"
            );
            return;
        };
        if after.as_ref() == Some(&next_after) {
            tracing::warn!(
                gateway_replica_id = %replica_id,
                "Gateway route listing cursor did not advance during disconnect cleanup"
            );
            return;
        }
        after = Some(next_after);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewaySessionError {
    #[error("Gateway protocol rejected: {0}")]
    Protocol(&'static str),
    #[error("Gateway workload identity was rejected")]
    Identity,
    #[error("Gateway control session is closed")]
    Closed,
    #[error("Gateway owner route is unavailable: {0}")]
    RouteUnavailable(&'static str),
    #[error("Gateway protocol rejected: {0}")]
    Wire(#[from] neoengram_domain::protocol::ProtocolError),
    #[error("Gateway registry rejected the operation: {0}")]
    Registry(#[from] crate::CentralError),
}

fn fresh_forward_request_id() -> Result<RequestId, GatewaySessionError> {
    fresh_control_request_id("peer-forward")
}

fn fresh_route_fence_request_id() -> Result<RequestId, GatewaySessionError> {
    fresh_control_request_id("route-fence")
}

fn fresh_control_request_id(prefix: &str) -> Result<RequestId, GatewaySessionError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|_| GatewaySessionError::Protocol("secure request identity generation failed"))?;
    let mut value = format!("{prefix}-");
    use std::fmt::Write as _;
    for byte in random {
        write!(&mut value, "{byte:02x}")
            .map_err(|_| GatewaySessionError::Protocol("request identity formatting failed"))?;
    }
    RequestId::new(value).map_err(GatewaySessionError::Wire)
}

#[derive(Serialize)]
struct AgentProblem<'a> {
    code: &'a str,
    detail: &'a str,
    retryable: bool,
}

fn agent_error_response(
    stream_id: GatewayConnectionId,
    error: AgentHttpError,
) -> Result<GatewayAgentResponse, GatewaySessionError> {
    let body = serde_json::to_vec(&AgentProblem {
        code: error.code(),
        detail: error.detail(),
        retryable: error.retryable(),
    })
    .map_err(|_| GatewaySessionError::Protocol("failed to encode Agent problem"))?;
    Ok(GatewayAgentResponse {
        stream_id,
        status: error.status().as_u16(),
        content_type: PROBLEM_CONTENT_TYPE.to_owned(),
        retry_after_ms: error.retry_after_ms(),
        body: GatewayOpaqueBytes::new(body)?,
    })
}

fn gateway_error(error: &AgentHttpError) -> GatewayControlError {
    // HTTP 409 carries both ephemeral CAS contention and authoritative session fencing. Preserve
    // the retry decision when adapting it to the stricter Gateway error classes so a reconnecting
    // Agent retries only the former.
    let code = match (error.status(), error.retryable()) {
        (http::StatusCode::CONFLICT, true) => GatewayErrorCode::RouteUnavailable,
        (http::StatusCode::CONFLICT, false) => GatewayErrorCode::RouteFenced,
        (http::StatusCode::GATEWAY_TIMEOUT, _) => GatewayErrorCode::DeadlineExceeded,
        (http::StatusCode::TOO_MANY_REQUESTS, _) => GatewayErrorCode::ResourceExhausted,
        (http::StatusCode::SERVICE_UNAVAILABLE, _) => GatewayErrorCode::RouteUnavailable,
        (status, _) if status.is_client_error() => GatewayErrorCode::ProtocolInvalid,
        _ => GatewayErrorCode::Internal,
    };
    GatewayControlError {
        code,
        detail: error.detail().to_owned(),
        retryable: error.retryable() && code.permits_retry(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use crate::{
        AcquireAgentRouteLeaseRequest, AcquireAgentSessionRouteRequest,
        AgentRouteLeaseAcquireOutcome, AgentRouteLeaseListRequest, AgentRouteLeaseMutationOutcome,
        AgentSessionRouteAcquireOutcome, CentralError, CentralErrorCode, CentralResult,
        GatewayInsertOutcome, GatewayPoolListRequest, GatewayPoolRecord, GatewayPoolState,
        GatewayReplicaCertificateRecord, GatewayReplicaCredential, GatewayReplicaListRequest,
        GatewayReplicaRecord, InMemoryClock, InMemoryGatewayRegistry,
    };
    use async_trait::async_trait;
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        AgentChannelAck, AgentChannelDownstreamFrame, AgentChannelDownstreamMessage,
        AssignmentGeneration, AssignmentId, ControlError, DecisionGeneration, Ed25519PublicKeySpki,
        EdgeClusterId, ErrorCode, Extensions, GatewayDrain, GatewayOpaqueBytes,
        GatewayReplicaHeartbeat, GatewayS3ReadRevocation, Generation, JobDecision, JobId, JobState,
        LifecycleGeneration, MessageId, PrincipalId, PrincipalKind, PrincipalRef, PublishDecision,
        RequestId, ResourceVersion, RouteGeneration, SessionGeneration, SnapshotId,
        TaskExecutionFence, TaskId, TenantId, TraceId, CURRENT_WIRE_VERSION,
    };

    use super::*;

    struct ForwardAuthorityRegistry {
        state: Mutex<ForwardAuthorityState>,
        standalone_acquire_calls: AtomicUsize,
        route_release_calls: AtomicUsize,
    }

    struct ForwardAuthorityState {
        pool: GatewayPoolRecord,
        replicas: BTreeMap<GatewayReplicaId, GatewayReplicaRecord>,
        route: Option<AgentRouteLease>,
    }

    impl ForwardAuthorityRegistry {
        fn new() -> Self {
            let mut pool = pool_record();
            pool.state = GatewayPoolState::Ready;
            pool.config_generation = Generation::new(2);
            pool.resource_version = ResourceVersion::new(2);
            pool.updated_at_unix_ms = UnixMillis::new(200);

            let owner = forwarding_replica_record(
                replica_id(),
                "https://replica-a.control.example",
                "https://replica-a.peer.authority.example",
            );
            let ingress = forwarding_replica_record(
                ingress_replica_id(),
                "https://replica-b.control.example",
                "https://replica-b.peer.example",
            );
            let route = authoritative_agent_route();

            Self {
                state: Mutex::new(ForwardAuthorityState {
                    pool,
                    replicas: BTreeMap::from([
                        (owner.gateway_replica_id.clone(), owner),
                        (ingress.gateway_replica_id.clone(), ingress),
                    ]),
                    route: Some(route),
                }),
                standalone_acquire_calls: AtomicUsize::new(0),
                route_release_calls: AtomicUsize::new(0),
            }
        }

        fn standalone_acquire_calls(&self) -> usize {
            self.standalone_acquire_calls.load(Ordering::SeqCst)
        }

        fn route_release_calls(&self) -> usize {
            self.route_release_calls.load(Ordering::SeqCst)
        }

        fn route(&self) -> AgentRouteLease {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .route
                .clone()
                .expect("forwarding route must be present")
        }

        fn update_route(&self, update: impl FnOnce(&mut AgentRouteLease)) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            update(
                state
                    .route
                    .as_mut()
                    .expect("forwarding route must be present"),
            );
        }

        fn update_replica(
            &self,
            replica_id: &GatewayReplicaId,
            update: impl FnOnce(&mut GatewayReplicaRecord),
        ) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            update(
                state
                    .replicas
                    .get_mut(replica_id)
                    .expect("forwarding Replica must be present"),
            );
        }
    }

    #[async_trait]
    impl GatewayRegistryRepository for ForwardAuthorityRegistry {
        async fn get_pool(
            &self,
            gateway_pool_id: &GatewayPoolId,
        ) -> CentralResult<Option<GatewayPoolRecord>> {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok((state.pool.gateway_pool_id == *gateway_pool_id).then(|| state.pool.clone()))
        }

        async fn get_pool_by_edge_cluster(
            &self,
            edge_cluster_id: &EdgeClusterId,
        ) -> CentralResult<Option<GatewayPoolRecord>> {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok((state.pool.edge_cluster_id == *edge_cluster_id).then(|| state.pool.clone()))
        }

        async fn list_pools(
            &self,
            _request: &GatewayPoolListRequest,
        ) -> CentralResult<Vec<GatewayPoolRecord>> {
            unreachable!("forwarding tests only use authoritative point reads")
        }

        async fn insert_pool(
            &self,
            _record: GatewayPoolRecord,
        ) -> CentralResult<GatewayInsertOutcome<GatewayPoolRecord>> {
            unreachable!("forwarding tests use an immutable authority fixture")
        }

        async fn replace_pool(
            &self,
            _expected_resource_version: u64,
            _record: GatewayPoolRecord,
        ) -> CentralResult<GatewayPoolRecord> {
            unreachable!("forwarding tests use an immutable authority fixture")
        }

        async fn get_replica(
            &self,
            gateway_replica_id: &GatewayReplicaId,
        ) -> CentralResult<Option<GatewayReplicaRecord>> {
            Ok(self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replicas
                .get(gateway_replica_id)
                .cloned())
        }

        async fn get_replica_by_activation_token_digest(
            &self,
            _token_digest: &ContentDigest,
        ) -> CentralResult<Option<GatewayReplicaRecord>> {
            unreachable!("forwarding tests only use Replica identity reads")
        }

        async fn list_replicas(
            &self,
            request: &GatewayReplicaListRequest,
        ) -> CentralResult<Vec<GatewayReplicaRecord>> {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(state
                .replicas
                .values()
                .filter(|replica| {
                    replica.gateway_pool_id == request.gateway_pool_id
                        && request.state.is_none_or(|state| replica.state == state)
                        && request
                            .after
                            .as_ref()
                            .is_none_or(|after| replica.gateway_replica_id > *after)
                })
                .take(request.limit)
                .cloned()
                .collect())
        }

        async fn insert_replica(
            &self,
            _record: GatewayReplicaRecord,
        ) -> CentralResult<GatewayInsertOutcome<GatewayReplicaRecord>> {
            unreachable!("forwarding tests use an immutable authority fixture")
        }

        async fn replace_replica(
            &self,
            _expected_resource_version: u64,
            _record: GatewayReplicaRecord,
        ) -> CentralResult<GatewayReplicaRecord> {
            unreachable!("forwarding tests use an immutable authority fixture")
        }

        async fn get_agent_route(
            &self,
            agent_id: &neoengram_domain::protocol::AgentId,
        ) -> CentralResult<Option<AgentRouteLease>> {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(state
                .route
                .as_ref()
                .filter(|route| route.agent_id == *agent_id)
                .cloned())
        }

        async fn list_agent_routes(
            &self,
            request: &AgentRouteLeaseListRequest,
        ) -> CentralResult<Vec<AgentRouteLease>> {
            let route = self.route();
            Ok((route.gateway_pool_id == request.gateway_pool_id
                && request
                    .gateway_replica_id
                    .as_ref()
                    .is_none_or(|replica| &route.gateway_replica_id == replica)
                && request
                    .after
                    .as_ref()
                    .is_none_or(|after| route.agent_id > *after))
            .then_some(route)
            .into_iter()
            .collect())
        }

        async fn acquire_agent_route(
            &self,
            _request: AcquireAgentRouteLeaseRequest,
        ) -> CentralResult<AgentRouteLeaseAcquireOutcome> {
            self.standalone_acquire_calls.fetch_add(1, Ordering::SeqCst);
            Ok(AgentRouteLeaseAcquireOutcome {
                lease: self.route(),
                replayed: false,
                fenced: None,
            })
        }

        async fn acquire_agent_session_route(
            &self,
            _request: AcquireAgentSessionRouteRequest,
        ) -> CentralResult<AgentSessionRouteAcquireOutcome> {
            unreachable!("forwarding tests use a pre-established route")
        }

        async fn renew_agent_route(
            &self,
            request: RenewAgentRouteLeaseRequest,
        ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
            let route = self.route();
            if route.agent_id != request.agent_id
                || route.gateway_replica_id != request.gateway_replica_id
                || route.connection_id != request.connection_id
                || route.session_generation != request.session_generation
                || route.route_generation != request.route_generation
            {
                return Err(CentralError::new(
                    CentralErrorCode::GatewayRouteFenced,
                    "forwarding test route was replaced",
                )
                .with_retryable(false));
            }
            Ok(AgentRouteLeaseMutationOutcome {
                lease: route,
                replayed: false,
            })
        }

        async fn release_agent_route(
            &self,
            request: ReleaseAgentRouteLeaseRequest,
        ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
            self.route_release_calls.fetch_add(1, Ordering::SeqCst);
            let route = self.route();
            if route.agent_id != request.agent_id
                || route.gateway_replica_id != request.gateway_replica_id
                || route.connection_id != request.connection_id
                || route.session_generation != request.session_generation
                || route.route_generation != request.route_generation
            {
                return Err(CentralError::new(
                    CentralErrorCode::GatewayRouteFenced,
                    "forwarding test route was replaced",
                )
                .with_retryable(false));
            }
            Ok(AgentRouteLeaseMutationOutcome {
                lease: route,
                replayed: false,
            })
        }
    }

    #[derive(Default)]
    struct CountingHandler {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl AgentApiHandler for CountingHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(body.to_vec())
        }
    }

    #[test]
    fn gateway_error_distinguishes_retryable_and_fenced_conflicts() {
        let route_race = AgentHttpError::new(
            http::StatusCode::CONFLICT,
            "AGENT_ACTION_CONFLICT",
            "Agent action conflicts with authoritative state",
            true,
        );
        let mapped = gateway_error(&route_race);
        assert_eq!(mapped.code, GatewayErrorCode::RouteUnavailable);
        assert!(mapped.retryable);
        mapped.validate().unwrap();

        let active_session = AgentHttpError::new(
            http::StatusCode::CONFLICT,
            "AGENT_ACTION_CONFLICT",
            "Agent action conflicts with authoritative state",
            false,
        );
        let mapped = gateway_error(&active_session);
        assert_eq!(mapped.code, GatewayErrorCode::RouteFenced);
        assert!(!mapped.retryable);
        mapped.validate().unwrap();

        let stale_session = AgentHttpError::new(
            http::StatusCode::CONFLICT,
            "AGENT_SESSION_FENCED",
            "Agent request belongs to a stale or closed session",
            false,
        );
        let mapped = gateway_error(&stale_session);
        assert_eq!(mapped.code, GatewayErrorCode::RouteFenced);
        assert!(!mapped.retryable);
        mapped.validate().unwrap();

        // A route fence emitted by Gateway's route-owner path keeps its dedicated code so an
        // already-established Agent channel can reconnect after the old lease is replaced.
        let gateway_route_fence = AgentHttpError::new(
            http::StatusCode::CONFLICT,
            "GATEWAY_ROUTE_FENCED",
            "the previous owner lease is still active",
            false,
        );
        let mapped = gateway_error(&gateway_route_fence);
        assert_eq!(mapped.code, GatewayErrorCode::RouteFenced);
        assert!(!mapped.retryable);
        mapped.validate().unwrap();
    }

    #[derive(Default)]
    struct SlowUnaryHandler {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl AgentApiHandler for SlowUnaryHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(body.to_vec())
        }
    }

    #[derive(Default)]
    struct HoldingRoutedHandler {
        entered: tokio::sync::Notify,
        input_closed: tokio::sync::Notify,
    }

    struct NotifyOnDrop<'a>(&'a tokio::sync::Notify);

    impl Drop for NotifyOnDrop<'_> {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    #[async_trait]
    impl AgentApiHandler for HoldingRoutedHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            Ok(body.to_vec())
        }

        async fn open_routed_control_channel(
            &self,
            mut input: AgentControlInput,
            _route: GatewayAgentRouteContext,
        ) -> Result<crate::agent_transport::RoutedAgentControlChannel, AgentHttpError> {
            let _exit = NotifyOnDrop(&self.input_closed);
            self.entered.notify_one();
            while input.next_line().await?.is_some() {}
            Err(AgentHttpError::unavailable())
        }
    }

    #[derive(Default)]
    struct BufferedInputHandler {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        exited: tokio::sync::Notify,
        consumed: AtomicUsize,
    }

    #[async_trait]
    impl AgentApiHandler for BufferedInputHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            Ok(body.to_vec())
        }

        async fn open_routed_control_channel(
            &self,
            mut input: AgentControlInput,
            _route: GatewayAgentRouteContext,
        ) -> Result<crate::agent_transport::RoutedAgentControlChannel, AgentHttpError> {
            let _exit = NotifyOnDrop(&self.exited);
            self.entered.notify_one();
            self.release.notified().await;
            while input.next_line().await?.is_some() {
                self.consumed.fetch_add(1, Ordering::SeqCst);
            }
            Err(AgentHttpError::unavailable())
        }
    }

    #[derive(Default)]
    struct EchoHandler;

    #[async_trait]
    impl AgentApiHandler for EchoHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            Ok(body.to_vec())
        }
    }

    #[derive(Default)]
    struct RoutedHandler {
        atomic_route_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct GatedRoutedHandler {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl AgentApiHandler for RoutedHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            Ok(body.to_vec())
        }

        async fn open_routed_control_channel(
            &self,
            _input: AgentControlInput,
            route: GatewayAgentRouteContext,
        ) -> Result<crate::agent_transport::RoutedAgentControlChannel, AgentHttpError> {
            self.atomic_route_calls.fetch_add(1, Ordering::SeqCst);
            let (sender, receiver) = mpsc::channel(1);
            sender
                .try_send(Bytes::from_static(b"channel.opened\n"))
                .unwrap();
            drop(sender);
            Ok(crate::agent_transport::RoutedAgentControlChannel {
                channel: crate::agent_transport::AgentControlChannel::new(receiver),
                route: synthetic_route(&route),
                replayed: false,
                fenced: None,
            })
        }
    }

    #[async_trait]
    impl AgentApiHandler for GatedRoutedHandler {
        async fn handle(
            &self,
            _action: AgentAction,
            body: &[u8],
        ) -> Result<Vec<u8>, AgentHttpError> {
            Ok(body.to_vec())
        }

        async fn open_routed_control_channel(
            &self,
            _input: AgentControlInput,
            route: GatewayAgentRouteContext,
        ) -> Result<crate::agent_transport::RoutedAgentControlChannel, AgentHttpError> {
            self.entered.notify_one();
            self.release.notified().await;
            let (sender, receiver) = mpsc::channel(1);
            sender
                .try_send(Bytes::from_static(b"channel.opened\n"))
                .unwrap();
            drop(sender);
            Ok(crate::agent_transport::RoutedAgentControlChannel {
                channel: crate::agent_transport::AgentControlChannel::new(receiver),
                route: synthetic_route(&route),
                replayed: false,
                fenced: None,
            })
        }
    }

    #[tokio::test]
    async fn hello_binds_the_persisted_mtls_identity_and_unary_payload_stays_opaque() {
        let (control, identity) = control_fixture().await;
        let hello = frame(1, GatewayControlMessage::ReplicaHello(hello_message()));
        let (session, mut output) = control.open(identity.clone(), hello).await.unwrap();
        let body = br#"{"signed":"agent-payload"}"#.to_vec();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentRequest(GatewayAgentRequest {
                    action: GatewayAgentAction::EnrollmentBootstrap,
                    stream_id: GatewayConnectionId::new("agent-request-a").unwrap(),
                    body: GatewayOpaqueBytes::new(body.clone()).unwrap(),
                }),
            ))
            .await
            .unwrap();

        let response = output.recv().await.unwrap();
        assert_eq!(response.sequence, SequenceNumber::new(1));
        let GatewayControlMessage::AgentResponse(response) = response.message else {
            panic!("expected Agent response")
        };
        assert_eq!(response.status, 200);
        assert_eq!(response.body.as_bytes(), body);

        let mut wrong = identity;
        wrong.certificate_generation = CertificateGeneration::new(2);
        let result = control
            .open(
                wrong,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await;
        assert!(matches!(result, Err(GatewaySessionError::Identity)));
    }

    #[tokio::test]
    async fn hello_must_match_the_registered_version_and_capabilities() {
        let (control, identity) = control_fixture().await;

        let mut wrong_version = hello_message();
        wrong_version.software_version = "0.2.1".to_owned();
        assert!(matches!(
            control
                .open(
                    identity.clone(),
                    frame(1, GatewayControlMessage::ReplicaHello(wrong_version)),
                )
                .await,
            Err(GatewaySessionError::Identity)
        ));

        let mut wrong_capabilities = hello_message();
        wrong_capabilities
            .capabilities
            .insert("unregistered-capability-v1".to_owned());
        assert!(matches!(
            control
                .open(
                    identity,
                    frame(1, GatewayControlMessage::ReplicaHello(wrong_capabilities),),
                )
                .await,
            Err(GatewaySessionError::Identity)
        ));
    }

    #[tokio::test]
    async fn standalone_route_acquire_is_rejected_before_registry_access() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let identity = AuthenticatedGatewayReplica {
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            certificate_generation: CertificateGeneration::new(1),
        };
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request = GatewayRouteLeaseRequest {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            owner_replica_id: replica_id(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            session_generation: SessionGeneration::new(4),
            route_generation: None,
            requested_expires_at_unix_ms: UnixMillis::new(31_000),
        };
        let rejected = session
            .accept(frame(
                2,
                GatewayControlMessage::RouteAcquire(request.clone()),
            ))
            .await;
        assert!(matches!(
            rejected,
            Err(GatewaySessionError::Protocol(
                "standalone route acquire is not supported; open an Agent stream to acquire the route atomically"
            ))
        ));
        assert_eq!(registry.standalone_acquire_calls(), 0);
        assert!(output.try_recv().is_err());

        let duplicate = session
            .accept(frame(2, GatewayControlMessage::RouteAcquire(request)))
            .await;
        assert!(matches!(duplicate, Err(GatewaySessionError::Identity)));
    }

    #[tokio::test]
    async fn routed_agent_stream_uses_atomic_route_path_before_channel_data() {
        let handler = Arc::new(RoutedHandler::default());
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let stream_id = GatewayConnectionId::new("agent-stream-a").unwrap();
        // The handler fixture returns this route through the atomic session-open result.  Keep
        // the repository point-read in sync so the Central delivery fence validates the same
        // connection/generation rather than treating the synthetic stream as stale.
        registry.update_route(|route| {
            route.connection_id = stream_id.clone();
            route.route_generation = RouteGeneration::new(1);
        });
        let control = CentralGatewayControl::new(
            registry,
            handler.clone(),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let identity = AuthenticatedGatewayReplica {
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            certificate_generation: CertificateGeneration::new(1),
        };
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: stream_id.clone(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();

        let first = output.recv().await.unwrap();
        let GatewayControlMessage::RouteGranted(granted) = first.message else {
            panic!("the route grant must precede Agent channel output")
        };
        assert_eq!(first.request_id, RequestId::new("request-2").unwrap());
        assert_eq!(granted.agent_connection_id, stream_id);
        assert_eq!(granted.route_generation, RouteGeneration::new(1));
        assert!(!granted.replayed);
        assert_eq!(handler.atomic_route_calls.load(Ordering::SeqCst), 1);

        let second = output.recv().await.unwrap();
        let GatewayControlMessage::AgentStreamData(data) = second.message else {
            panic!("expected Agent channel data after the route grant")
        };
        assert_eq!(data.chunk.as_bytes(), b"channel.opened\n");
        assert!(matches!(
            output.recv().await.unwrap().message,
            GatewayControlMessage::AgentStreamEnd(_)
        ));
    }

    #[tokio::test]
    async fn stale_route_grant_is_rejected_after_takeover_before_grant_send() {
        let handler = Arc::new(GatedRoutedHandler::default());
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let stream_id = GatewayConnectionId::new("gated-agent-stream").unwrap();
        registry.update_route(|route| {
            route.connection_id = stream_id.clone();
            route.route_generation = RouteGeneration::new(1);
        });
        let control = CentralGatewayControl::new(
            registry.clone(),
            handler.clone(),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let identity = AuthenticatedGatewayReplica {
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            certificate_generation: CertificateGeneration::new(1),
        };
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = handler.entered.notified();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id,
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered)
            .await
            .expect("the Agent handler must reach the grant race gate");

        registry.update_route(|route| {
            route.gateway_replica_id = ingress_replica_id();
            route.connection_id = GatewayConnectionId::new("replacement-agent-stream").unwrap();
            route.session_generation = SessionGeneration::new(5);
            route.route_generation = RouteGeneration::new(2);
        });
        handler.release.notify_one();

        let rejected = timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("stale route must receive a bounded rejection")
            .expect("Central output must remain open");
        assert_eq!(rejected.request_id, RequestId::new("request-2").unwrap());
        let GatewayControlMessage::Error(error) = rejected.message else {
            panic!("stale route must not receive RouteGranted")
        };
        assert_eq!(error.code, GatewayErrorCode::RouteUnavailable);
        assert!(error.retryable);
        assert!(!session.fenced.load(Ordering::Acquire));
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn agent_stream_cleanup_is_scoped_to_the_worker_request() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let stream_id = GatewayConnectionId::new("cleanup-stream").unwrap();
        let request_id = RequestId::new("cleanup-request").unwrap();
        let (writer, _input) = AgentControlInput::channel(1);
        let worker = tokio::spawn(std::future::pending::<()>());
        let worker_abort = worker.abort_handle();
        session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .insert(
                stream_id.clone(),
                InboundStream {
                    request_id: request_id.clone(),
                    writer,
                    task: worker,
                },
            );

        drop(AgentStreamCleanup {
            session: session.clone(),
            stream_id: stream_id.clone(),
            request_id: RequestId::new("different-worker").unwrap(),
        });
        assert!(session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .contains_key(&stream_id));

        drop(AgentStreamCleanup {
            session: session.clone(),
            stream_id: stream_id.clone(),
            request_id,
        });
        assert!(!session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .contains_key(&stream_id));

        worker_abort.abort();
    }

    #[tokio::test]
    async fn agent_stream_data_and_end_require_the_open_request_id() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();

        let data_stream_id = GatewayConnectionId::new("request-bound-data-stream").unwrap();
        let data_request_id = RequestId::new("request-bound-data").unwrap();
        let wrong_request_id = RequestId::new("request-bound-wrong").unwrap();
        let (data_writer, _data_input) = AgentControlInput::channel(1);
        let data_task = tokio::spawn(std::future::pending::<()>());
        let data_task_abort = data_task.abort_handle();
        session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .insert(
                data_stream_id.clone(),
                InboundStream {
                    request_id: data_request_id,
                    writer: data_writer,
                    task: data_task,
                },
            );

        let data_result = session
            .write_agent_stream(
                wrong_request_id.clone(),
                None,
                GatewayAgentStreamData {
                    stream_id: data_stream_id.clone(),
                    chunk: GatewayOpaqueBytes::new(b"mismatched".to_vec()).unwrap(),
                },
            )
            .await;
        assert!(matches!(
            data_result,
            Err(GatewaySessionError::Protocol(
                "Gateway Agent stream request ID does not match its bound request"
            ))
        ));
        assert!(!session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .contains_key(&data_stream_id));
        tokio::task::yield_now().await;
        assert!(data_task_abort.is_finished());

        let end_stream_id = GatewayConnectionId::new("request-bound-end-stream").unwrap();
        let end_request_id = RequestId::new("request-bound-end").unwrap();
        let (end_writer, _end_input) = AgentControlInput::channel(1);
        let end_task = tokio::spawn(std::future::pending::<()>());
        let end_task_abort = end_task.abort_handle();
        session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .insert(
                end_stream_id.clone(),
                InboundStream {
                    request_id: end_request_id,
                    writer: end_writer,
                    task: end_task,
                },
            );

        let end_result = session
            .end_agent_stream(
                wrong_request_id,
                GatewayAgentStreamEnd {
                    stream_id: end_stream_id.clone(),
                },
            )
            .await;
        assert!(matches!(
            end_result,
            Err(GatewaySessionError::Protocol(
                "Gateway Agent stream request ID does not match its bound request"
            ))
        ));
        assert!(!session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .contains_key(&end_stream_id));
        tokio::task::yield_now().await;
        assert!(end_task_abort.is_finished());
    }

    #[tokio::test]
    async fn peer_forwarding_waits_for_an_exact_bounded_acknowledgement() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request_id = RequestId::new("peer-forward-request-a").unwrap();
        let request = GatewayPeerForwardRequest {
            source_replica_id: replica_id(),
            target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
            target_peer_endpoint: "https://replica-b.peer.example".to_owned(),
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            session_generation: SessionGeneration::new(4),
            route_generation: RouteGeneration::new(7),
            frame: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
        };
        let expected = GatewayPeerForwardAccepted {
            source_replica_id: request.source_replica_id.clone(),
            target_replica_id: request.target_replica_id.clone(),
            agent_id: request.agent_id.clone(),
            agent_connection_id: request.agent_connection_id.clone(),
            session_generation: request.session_generation,
            route_generation: request.route_generation,
        };
        let delivery = session.clone();
        let delivery_request_id = request_id.clone();
        let task = tokio::spawn(async move {
            delivery
                .send_peer_forward(delivery_request_id, request)
                .await
        });

        let outbound = output.recv().await.unwrap();
        assert_eq!(outbound.hop_count, 0);
        assert_eq!(outbound.request_id, request_id);
        let GatewayControlMessage::PeerForward(forward) = outbound.message else {
            panic!("expected Central peer forwarding request")
        };
        assert_eq!(forward.target_replica_id, expected.target_replica_id);
        assert_eq!(
            forward.target_peer_endpoint,
            "https://replica-b.peer.example"
        );

        let mut acknowledgement = frame(
            2,
            GatewayControlMessage::PeerForwardAccepted(expected.clone()),
        );
        acknowledgement.request_id = request_id;
        session.accept(acknowledgement).await.unwrap();
        task.await.unwrap().unwrap();

        let mut wrong = frame(3, GatewayControlMessage::PeerForwardAccepted(expected));
        wrong.request_id = RequestId::new("unknown-forward-request").unwrap();
        assert!(matches!(
            session.accept(wrong).await,
            Err(GatewaySessionError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn late_peer_forward_ack_and_error_are_absorbed_only_for_the_exact_binding() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();

        let request_id = RequestId::new("late-peer-forward-ack").unwrap();
        let request = peer_forward_request();
        let expected = peer_forward_accepted(&request);
        let (sender, _receiver) = oneshot::channel();
        session
            .pending_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                request_id.clone(),
                PendingForward {
                    expected: expected.clone(),
                    sender,
                },
            );
        session.move_pending_peer_forward_to_late(&request_id, expected.clone());

        let mut late_ack = frame(
            2,
            GatewayControlMessage::PeerForwardAccepted(expected.clone()),
        );
        late_ack.request_id = request_id.clone();
        session.accept(late_ack).await.unwrap();
        assert!(session
            .late_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());

        let error_request_id = RequestId::new("late-peer-forward-error").unwrap();
        let error_request = peer_forward_request();
        let error_expected = peer_forward_accepted(&error_request);
        let (sender, _receiver) = oneshot::channel();
        session
            .pending_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                error_request_id.clone(),
                PendingForward {
                    expected: error_expected.clone(),
                    sender,
                },
            );
        session.move_pending_peer_forward_to_late(&error_request_id, error_expected);

        let mut late_error = frame(
            3,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::RouteUnavailable,
                detail: "late owner rejection".to_owned(),
                retryable: true,
            }),
        );
        late_error.request_id = error_request_id;
        session.accept(late_error).await.unwrap();
        assert!(session
            .late_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
        assert!(!session.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn timed_out_peer_forward_retains_a_late_ack_tombstone() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request_id = RequestId::new("timed-out-peer-forward").unwrap();
        let request = peer_forward_request();
        let expected = peer_forward_accepted(&request);
        let delivery = session.clone();
        let delivery_request_id = request_id.clone();
        let task = tokio::spawn(async move {
            delivery
                .send_peer_forward_with_timeout(
                    delivery_request_id,
                    request,
                    Duration::from_millis(1),
                )
                .await
        });
        let outbound = output.recv().await.unwrap();
        assert!(matches!(
            outbound.message,
            GatewayControlMessage::PeerForward(_)
        ));
        assert!(matches!(
            task.await.unwrap(),
            Err(GatewaySessionError::RouteUnavailable(
                "owner forwarding acknowledgement timed out"
            ))
        ));
        assert!(session
            .late_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&request_id));

        let mut late_ack = frame(2, GatewayControlMessage::PeerForwardAccepted(expected));
        late_ack.request_id = request_id;
        session.accept(late_ack).await.unwrap();
        assert!(!session.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn late_peer_forward_ack_with_wrong_binding_fences_the_session() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request_id = RequestId::new("late-peer-forward-wrong-binding").unwrap();
        let request = peer_forward_request();
        let expected = peer_forward_accepted(&request);
        let (sender, _receiver) = oneshot::channel();
        session
            .pending_forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                request_id.clone(),
                PendingForward {
                    expected: expected.clone(),
                    sender,
                },
            );
        session.move_pending_peer_forward_to_late(&request_id, expected.clone());

        let mut wrong = expected;
        wrong.route_generation =
            RouteGeneration::new(wrong.route_generation.get().saturating_add(1));
        let mut frame = frame(2, GatewayControlMessage::PeerForwardAccepted(wrong));
        frame.request_id = request_id;
        assert!(matches!(
            session.accept(frame).await,
            Err(GatewaySessionError::Identity)
        ));
        assert!(session.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn forward_agent_frame_via_uses_the_authoritative_owner_route_and_endpoint() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let clock = Arc::new(InMemoryClock::new(1_000));
        let control = CentralGatewayControl::new(registry.clone(), Arc::new(EchoHandler), clock);
        let ingress_id = ingress_replica_id();
        let ingress_connection = GatewayConnectionId::new("ingress-control-connection").unwrap();
        let identity = AuthenticatedGatewayReplica {
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: ingress_id.clone(),
            certificate_generation: CertificateGeneration::new(1),
        };
        let (session, mut output) = control
            .open(
                identity,
                frame_for_replica(
                    ingress_id.clone(),
                    ingress_connection.clone(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let agent_id = neoengram_domain::protocol::AgentId::new("agent-a").unwrap();
        let encoded_frame = forwarding_frame(SessionGeneration::new(4));

        let delivery = control.clone();
        let ingress_for_delivery = ingress_id.clone();
        let agent_for_delivery = agent_id.clone();
        let first = tokio::spawn(async move {
            delivery
                .forward_agent_frame_via(
                    &ingress_for_delivery,
                    &agent_for_delivery,
                    encoded_frame.clone(),
                )
                .await
        });

        let outbound = timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("forwarding must emit a peer frame")
            .expect("ingress session output must remain open");
        let request_id = outbound.request_id.clone();
        let GatewayControlMessage::PeerForward(request) = outbound.message else {
            panic!("expected a peer-forward request")
        };
        let route = registry.route();
        assert_eq!(request.source_replica_id, ingress_id);
        assert_eq!(request.target_replica_id, route.gateway_replica_id);
        assert_eq!(request.agent_id, route.agent_id);
        assert_eq!(request.agent_connection_id, route.connection_id);
        assert_eq!(request.session_generation, route.session_generation);
        assert_eq!(request.route_generation, route.route_generation);
        assert_eq!(
            request.target_peer_endpoint,
            "https://replica-a.peer.authority.example"
        );
        assert_eq!(
            request.frame.as_bytes(),
            forwarding_frame(route.session_generation)
        );

        let mut acknowledgement = frame_for_replica(
            ingress_id.clone(),
            ingress_connection,
            2,
            GatewayControlMessage::PeerForwardAccepted(peer_forward_accepted(&request)),
        );
        acknowledgement.request_id = request_id;
        session.accept(acknowledgement).await.unwrap();
        timeout(Duration::from_secs(1), first)
            .await
            .expect("forwarding should complete after the exact ack")
            .unwrap()
            .unwrap();

        // The endpoint is read from Central on each call. A caller cannot pin a stale peer
        // endpoint by reusing the first forwarding request.
        registry.update_replica(&route.gateway_replica_id, |replica| {
            replica.peer_endpoint = "https://replica-a.peer.rotated.example".to_owned();
        });
        let second_delivery = control.clone();
        let ingress_for_second = ingress_id.clone();
        let agent_for_second = agent_id.clone();
        let second = tokio::spawn(async move {
            second_delivery
                .forward_agent_frame_via(
                    &ingress_for_second,
                    &agent_for_second,
                    forwarding_frame(route.session_generation),
                )
                .await
        });
        let second_outbound = timeout(Duration::from_secs(1), output.recv())
            .await
            .expect("second forwarding must emit a peer frame")
            .expect("ingress session output must remain open");
        let second_request_id = second_outbound.request_id.clone();
        let GatewayControlMessage::PeerForward(second_request) = second_outbound.message else {
            panic!("expected a second peer-forward request")
        };
        assert_eq!(
            second_request.target_peer_endpoint,
            "https://replica-a.peer.rotated.example"
        );
        let mut second_ack = frame_for_replica(
            ingress_id.clone(),
            GatewayConnectionId::new("ingress-control-connection").unwrap(),
            3,
            GatewayControlMessage::PeerForwardAccepted(peer_forward_accepted(&second_request)),
        );
        second_ack.request_id = second_request_id;
        session.accept(second_ack).await.unwrap();
        timeout(Duration::from_secs(1), second)
            .await
            .expect("second forwarding should complete after the exact ack")
            .unwrap()
            .unwrap();

        // A session whose persisted credential generation no longer matches the TLS identity is
        // rejected by the authority helper before it can enqueue another peer frame.
        registry.update_replica(&ingress_id, |replica| {
            replica.credential.certificate_generation = Some(CertificateGeneration::new(2));
        });
        let error = control
            .forward_agent_frame_via(
                &ingress_id,
                &agent_id,
                forwarding_frame(route.session_generation),
            )
            .await
            .expect_err("stale ingress certificate generation must fail closed");
        assert!(matches!(error, GatewaySessionError::Identity));
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn forward_agent_frame_via_fails_closed_for_stale_or_invalid_authority() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let agent_id = neoengram_domain::protocol::AgentId::new("agent-a").unwrap();
        let frame = forwarding_frame(SessionGeneration::new(4));

        registry.update_route(|route| {
            route.lease_expires_at_unix_ms = UnixMillis::new(1_000);
        });
        assert!(matches!(
            control
                .forward_agent_frame_via(&ingress_replica_id(), &agent_id, frame.clone())
                .await,
            Err(GatewaySessionError::RouteUnavailable(
                "Agent has no active owner route"
            ))
        ));

        registry.update_route(|route| {
            route.lease_expires_at_unix_ms = UnixMillis::new(31_000);
            route.gateway_replica_id = ingress_replica_id();
        });
        assert!(matches!(
            control
                .forward_agent_frame_via(&ingress_replica_id(), &agent_id, frame.clone())
                .await,
            Err(GatewaySessionError::Protocol(
                "owner forwarding requires a distinct ingress Replica"
            ))
        ));

        registry.update_route(|route| {
            route.gateway_replica_id = replica_id();
        });
        registry.update_replica(&ingress_replica_id(), |replica| {
            replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(1_000));
        });
        assert!(matches!(
            control
                .forward_agent_frame_via(&ingress_replica_id(), &agent_id, frame.clone())
                .await,
            Err(GatewaySessionError::RouteUnavailable(
                "ingress Replica cannot perform same-pool peer forwarding"
            ))
        ));
        registry.update_replica(&ingress_replica_id(), |replica| {
            replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(21_600_000));
        });
        registry.update_replica(&replica_id(), |replica| {
            replica.state = GatewayReplicaState::Draining;
        });
        assert!(matches!(
            control
                .forward_agent_frame_via(&ingress_replica_id(), &agent_id, frame)
                .await,
            Err(GatewaySessionError::RouteUnavailable(
                "owner Replica is not active in the Agent route scope"
            ))
        ));
    }

    #[tokio::test]
    async fn owner_stream_close_falls_back_once_through_a_connected_non_owner_replica() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let owner_connection = GatewayConnectionId::new("owner-control-connection").unwrap();
        let (owner, owner_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    owner_connection,
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let ingress_connection = GatewayConnectionId::new("ingress-control-connection").unwrap();
        let (ingress, mut ingress_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: ingress_replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    ingress_replica_id(),
                    ingress_connection.clone(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        // Closing the receiver makes the owner send fail before any stream-data frame is queued.
        drop(owner_output);

        let route = registry.route();
        let encoded = forwarding_frame(route.session_generation);
        let owner_for_delivery = owner.clone();
        let route_for_delivery = route.clone();
        let encoded_for_delivery = encoded.clone();
        let delivery = tokio::spawn(async move {
            owner_for_delivery
                .deliver_agent_frame(
                    RequestId::new("owner-command-delivery").unwrap(),
                    None,
                    &route_for_delivery,
                    &route_for_delivery.connection_id,
                    encoded_for_delivery,
                )
                .await
        });

        let outbound = timeout(Duration::from_secs(1), ingress_output.recv())
            .await
            .expect("closed owner must use the connected non-owner session")
            .expect("ingress session must remain open");
        let forward_request_id = outbound.request_id.clone();
        let GatewayControlMessage::PeerForward(forward) = outbound.message else {
            panic!("expected peer-forward fallback")
        };
        assert_eq!(forward.source_replica_id, ingress_replica_id());
        assert_eq!(forward.target_replica_id, replica_id());
        assert_eq!(forward.agent_id, route.agent_id);
        assert_eq!(forward.agent_connection_id, route.connection_id);
        assert_eq!(forward.session_generation, route.session_generation);
        assert_eq!(forward.route_generation, route.route_generation);
        assert_eq!(forward.frame.as_bytes(), encoded);

        let mut acknowledgement = frame_for_replica(
            ingress_replica_id(),
            ingress_connection,
            2,
            GatewayControlMessage::PeerForwardAccepted(peer_forward_accepted(&forward)),
        );
        acknowledgement.request_id = forward_request_id;
        ingress.accept(acknowledgement).await.unwrap();
        timeout(Duration::from_secs(1), delivery)
            .await
            .expect("fallback must complete after the exact owner acknowledgement")
            .unwrap()
            .unwrap();
        assert!(owner.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn accepted_owner_command_is_not_peer_replayed_after_the_session_closes() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (owner, mut owner_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    GatewayConnectionId::new("owner-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let (_ingress, mut ingress_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: ingress_replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    ingress_replica_id(),
                    GatewayConnectionId::new("ingress-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();

        let route = registry.route();
        let encoded = forwarding_frame(route.session_generation);
        owner
            .deliver_agent_frame(
                RequestId::new("accepted-owner-command").unwrap(),
                None,
                &route,
                &route.connection_id,
                encoded.clone(),
            )
            .await
            .expect("the complete command should enter the owner queue atomically");

        let queued = owner_output
            .recv()
            .await
            .expect("the owner queue must contain the accepted command");
        let GatewayControlMessage::AgentStreamData(data) = queued.message else {
            panic!("expected direct owner stream data")
        };
        assert_eq!(data.stream_id, route.connection_id);
        assert_eq!(data.chunk.as_bytes(), encoded);

        // Losing the connection after queue acceptance is not proof that the Agent missed the
        // command. The next Agent session will re-poll durable state; this session must not also
        // issue a peer copy with the same downstream sequence.
        owner.close();
        tokio::task::yield_now().await;
        assert!(ingress_output.try_recv().is_err());
    }

    #[tokio::test]
    async fn multipart_owner_delivery_never_falls_back_after_partial_queue_write() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (owner, owner_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    GatewayConnectionId::new("owner-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let (_ingress, mut ingress_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: ingress_replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    ingress_replica_id(),
                    GatewayConnectionId::new("ingress-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();

        // Leave exactly one queue slot. The first chunk below occupies it and the second chunk
        // waits; closing the owner receiver then proves that a partially queued frame is never
        // replayed through the peer path.
        for sequence in 0..(CONTROL_OUTPUT_BUFFER - 1) {
            owner
                .send_inner(
                    RequestId::new(format!("prefill-{sequence}")).unwrap(),
                    None,
                    GatewayControlMessage::Backpressure(GatewayBackpressure { retry_after_ms: 1 }),
                )
                .await
                .unwrap();
        }
        let route = registry.route();
        let large_frame = Bytes::from(vec![b'x'; MAX_GATEWAY_STREAM_CHUNK_BYTES + 1]);
        let owner_for_delivery = owner.clone();
        let route_for_delivery = route.clone();
        let mut delivery = tokio::spawn(async move {
            owner_for_delivery
                .deliver_agent_frame(
                    RequestId::new("multipart-owner-delivery").unwrap(),
                    None,
                    &route_for_delivery,
                    &route_for_delivery.connection_id,
                    large_frame,
                )
                .await
        });
        assert!(
            timeout(Duration::from_millis(100), &mut delivery)
                .await
                .is_err(),
            "second chunk should remain blocked behind the bounded owner queue"
        );
        drop(owner_output);
        let error = timeout(Duration::from_secs(1), delivery)
            .await
            .expect("owner close should release the blocked second chunk")
            .unwrap()
            .expect_err("multipart owner delivery must fail closed");
        assert!(matches!(error, GatewaySessionError::Closed));
        assert!(ingress_output.try_recv().is_err());
    }

    #[tokio::test]
    async fn owner_stream_fallback_rejects_a_changed_authoritative_route() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (owner, owner_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    GatewayConnectionId::new("owner-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let (_ingress, mut ingress_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: ingress_replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    ingress_replica_id(),
                    GatewayConnectionId::new("ingress-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        drop(owner_output);
        let established_route = registry.route();
        registry.update_route(|route| {
            route.route_generation = RouteGeneration::new(route.route_generation.get() + 1);
            route.connection_id = GatewayConnectionId::new("replacement-agent-connection").unwrap();
        });

        let error = owner
            .deliver_agent_frame(
                RequestId::new("stale-owner-command-delivery").unwrap(),
                None,
                &established_route,
                &established_route.connection_id,
                forwarding_frame(established_route.session_generation),
            )
            .await
            .expect_err("an old stream must not target the replacement route");
        assert!(matches!(
            error,
            GatewaySessionError::RouteUnavailable(
                "Agent owner route changed before fallback delivery"
            )
        ));
        assert!(ingress_output.try_recv().is_err());
        assert!(owner.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn route_takeover_fences_old_owner_before_direct_queue_admission() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (owner, mut owner_output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    GatewayConnectionId::new("owner-control-before-takeover").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let established_route = registry.route();
        registry.update_route(|route| {
            route.gateway_replica_id = ingress_replica_id();
            route.connection_id = GatewayConnectionId::new("replacement-agent-connection").unwrap();
            route.session_generation =
                SessionGeneration::new(route.session_generation.get().saturating_add(1));
            route.route_generation =
                RouteGeneration::new(route.route_generation.get().saturating_add(1));
        });

        let error = owner
            .deliver_agent_frame(
                RequestId::new("old-owner-after-takeover").unwrap(),
                None,
                &established_route,
                &established_route.connection_id,
                forwarding_frame(established_route.session_generation),
            )
            .await
            .expect_err("a stale owner must be fenced before queue admission");

        assert!(matches!(
            error,
            GatewaySessionError::RouteUnavailable(
                "Agent owner route changed before fallback delivery"
            )
        ));
        let fence = owner_output
            .recv()
            .await
            .expect("stale route must receive a per-Agent fence");
        let GatewayControlMessage::RouteFence(fence) = fence.message else {
            panic!("stale route must emit RouteFence instead of fencing the whole session")
        };
        assert_eq!(fence.agent_id, established_route.agent_id);
        assert_eq!(fence.route_generation, established_route.route_generation);
        assert_eq!(fence.reason, "Agent route ownership changed or expired");
        assert!(!owner.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn takeover_notification_targets_the_exact_owner_stream_not_control_connection() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (owner, mut output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame_for_replica(
                    replica_id(),
                    GatewayConnectionId::new("owner-control-connection").unwrap(),
                    1,
                    GatewayControlMessage::ReplicaHello(hello_message()),
                ),
            )
            .await
            .unwrap();
        let route = registry.route();
        assert_ne!(owner.connection_id, route.connection_id);

        let (writer, _input) = AgentControlInput::channel(1);
        let task = tokio::spawn(std::future::pending::<()>());
        owner
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .insert(
                route.connection_id.clone(),
                InboundStream {
                    request_id: RequestId::new("different-route-acquire").unwrap(),
                    writer,
                    task,
                },
            );

        owner.notify_fenced_route_owner(&route).await;
        assert!(output.try_recv().is_err());

        owner
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .get_mut(&route.connection_id)
            .unwrap()
            .request_id = route.acquire_request_id.clone();
        owner.notify_fenced_route_owner(&route).await;

        let frame = output
            .recv()
            .await
            .expect("the exact route owner stream must receive a takeover fence");
        let GatewayControlMessage::RouteFence(fence) = frame.message else {
            panic!("takeover notification must be a route fence")
        };
        assert_eq!(fence.agent_id, route.agent_id);
        assert_eq!(fence.route_generation, route.route_generation);
        assert!(!owner.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn stale_route_mutations_fence_only_the_target_route_and_keep_session_alive() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (session, mut output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let stale = registry.route();
        registry.update_route(|route| {
            route.gateway_replica_id = ingress_replica_id();
            route.connection_id = GatewayConnectionId::new("replacement-route-connection").unwrap();
            route.session_generation =
                SessionGeneration::new(stale.session_generation.get().saturating_add(1));
            route.route_generation =
                RouteGeneration::new(stale.route_generation.get().saturating_add(1));
        });

        let renew = GatewayRouteLeaseRequest {
            agent_id: stale.agent_id.clone(),
            owner_replica_id: replica_id(),
            agent_connection_id: stale.connection_id.clone(),
            session_generation: stale.session_generation,
            route_generation: Some(stale.route_generation),
            requested_expires_at_unix_ms: UnixMillis::new(31_000),
        };
        session
            .accept(frame_for_replica(
                replica_id(),
                GatewayConnectionId::new("control-connection-a").unwrap(),
                2,
                GatewayControlMessage::RouteRenew(renew.clone()),
            ))
            .await
            .expect("a stale renew is a route-scoped response, not a session failure");
        let error = output
            .recv()
            .await
            .expect("stale renew must receive a correlated error");
        assert!(matches!(
            error.message,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::RouteFenced,
                ..
            })
        ));

        session
            .accept(frame_for_replica(
                replica_id(),
                GatewayConnectionId::new("control-connection-a").unwrap(),
                3,
                GatewayControlMessage::RouteRelease(renew),
            ))
            .await
            .expect("a stale release is also route-scoped");
        let error = output
            .recv()
            .await
            .expect("stale release must receive a correlated error");
        assert!(matches!(
            error.message,
            GatewayControlMessage::Error(GatewayControlError {
                code: GatewayErrorCode::RouteFenced,
                ..
            })
        ));

        // A healthy control session can continue serving unrelated Agents after one route loses
        // ownership. Backpressure is an inbound no-op, so this assertion does not need a mutable
        // Replica fixture just to prove the session was not globally fenced.
        session
            .accept(frame_for_replica(
                replica_id(),
                GatewayConnectionId::new("control-connection-a").unwrap(),
                4,
                GatewayControlMessage::Backpressure(GatewayBackpressure {
                    retry_after_ms: 1_000,
                }),
            ))
            .await
            .expect("stale route mutations must not fence the Gateway session");
    }

    #[test]
    fn peer_fallback_admits_only_complete_assignment_or_decision_frames() {
        let generation = SessionGeneration::new(4);
        assert!(is_peer_forwardable_type("job.assignment"));
        assert!(is_peer_forwardable_type("job.decision"));
        assert!(is_peer_forwardable_type("resource.lifecycle.assignment"));
        assert!(!is_peer_forwardable_type("replication.assignment"));
        assert_eq!(
            forwarded_command_generation(&forwarding_frame(generation)),
            Some(generation)
        );
        let acknowledgement = AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(2),
            message_id: MessageId::new("non-forwarded-ack").unwrap(),
            correlation_id: Some(MessageId::new("non-forwarded-correlation").unwrap()),
            session_generation: generation,
            sent_at_unix_ms: UnixMillis::new(1_000),
            central_signature: None,
            message: AgentChannelDownstreamMessage::Ack(AgentChannelAck {
                acknowledged_sequence: SequenceNumber::new(1),
                resource_version: ResourceVersion::new(1),
                replayed: false,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        }
        .encode_ndjson()
        .unwrap();
        assert_eq!(forwarded_command_generation(&acknowledgement), None);
        assert_eq!(
            forwarded_command_generation(&forwarding_frame(generation)[..10]),
            None
        );
    }

    #[tokio::test]
    async fn heartbeat_updates_the_registered_replica_without_trusting_frame_identity() {
        let (control, identity) = control_fixture().await;
        let repository = control.registry.clone();
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        session
            .accept(frame(
                2,
                GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                    connected_agents: 1,
                    active_streams: 1,
                    queue_depth: 0,
                }),
            ))
            .await
            .unwrap();
        let replica = repository
            .get_replica(&replica_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            replica.last_heartbeat_at_unix_ms,
            Some(UnixMillis::new(1_000))
        );
        assert_eq!(replica.resource_version, ResourceVersion::new(4));
    }

    #[tokio::test]
    async fn peer_directory_snapshot_binds_current_certificate_and_excludes_revoked_replica() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();

        session.send_peer_directory().await.unwrap();
        let first = output.recv().await.expect("initial peer directory");
        let GatewayControlMessage::PeerDirectory(first) = first.message else {
            panic!("expected initial peer directory")
        };
        assert_eq!(first.directory_generation, Generation::new(1));
        assert_eq!(first.replicas.len(), 1);
        assert_eq!(
            first.replicas[0].certificate_fingerprint,
            ContentDigest::hash(b"replica-cert")
        );
        assert_eq!(
            first.expires_at_unix_ms.get() - first.issued_at_unix_ms.get(),
            neoengram_domain::protocol::GATEWAY_PEER_DIRECTORY_TTL_MS
        );

        rotate_replica_certificate(&control).await;
        let rotated = session.build_peer_directory().await.unwrap();
        assert_eq!(rotated.directory_generation, Generation::new(2));
        assert_eq!(
            rotated.replicas[0].certificate_generation,
            CertificateGeneration::new(2)
        );
        assert_eq!(
            rotated.replicas[0].certificate_fingerprint,
            ContentDigest::hash(b"rotated-replica-cert")
        );

        mutate_replica(&control, |replica| {
            replica.state = GatewayReplicaState::Revoked;
            replica.credential.state = GatewayCredentialState::Revoked;
        })
        .await;
        let revoked = session.build_peer_directory().await.unwrap();
        assert!(revoked.replicas.is_empty());
    }

    #[tokio::test]
    async fn s3_read_revocation_is_broadcast_to_connected_pool_replicas() {
        let (control, identity) = control_fixture().await;
        let (_session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let revocation = GatewayS3ReadRevocation {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            snapshot_id: SnapshotId::new("snapshot-a").unwrap(),
            minimum_snapshot_lifecycle_generation: LifecycleGeneration::new(4),
            bucket: "dataset-a".to_owned(),
            minimum_access_point_policy_generation: ResourceVersion::new(7),
            reason: "snapshot deletion requested".to_owned(),
        };

        assert_eq!(
            control
                .broadcast_s3_read_revocation(&pool_id(), revocation.clone())
                .await,
            1
        );
        let delivered = output.recv().await.expect("S3 read revocation");
        assert_eq!(
            delivered.message,
            GatewayControlMessage::S3ReadRevocation(revocation)
        );

        assert_eq!(
            control
                .broadcast_s3_read_revocation(
                    &GatewayPoolId::new("another-pool").unwrap(),
                    GatewayS3ReadRevocation {
                        tenant_id: TenantId::new("tenant-a").unwrap(),
                        snapshot_id: SnapshotId::new("snapshot-a").unwrap(),
                        minimum_snapshot_lifecycle_generation: LifecycleGeneration::new(5),
                        bucket: "dataset-a".to_owned(),
                        minimum_access_point_policy_generation: ResourceVersion::new(8),
                        reason: "unrelated pool".to_owned(),
                    },
                )
                .await,
            0
        );
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn connector_dispatch_does_not_head_of_line_block_on_a_slow_unary_agent_handler() {
        let handler = Arc::new(SlowUnaryHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request = frame(
            2,
            GatewayControlMessage::AgentRequest(GatewayAgentRequest {
                action: GatewayAgentAction::EnrollmentBootstrap,
                stream_id: GatewayConnectionId::new("slow-unary-request").unwrap(),
                body: GatewayOpaqueBytes::new(b"{}".to_vec()).unwrap(),
            }),
        );
        tokio::time::timeout(
            Duration::from_millis(100),
            session.accept_from_connector(request),
        )
        .await
        .expect("slow Agent work must be admitted without waiting for completion")
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handler.started.notified())
            .await
            .unwrap();

        session
            .accept_from_connector(frame(
                3,
                GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                    connected_agents: 0,
                    active_streams: 0,
                    queue_depth: 0,
                }),
            ))
            .await
            .unwrap();
        handler.release.notify_one();
        // Heartbeat processing refreshes the Central peer credential directory before the
        // background unary task can publish its response. The connector treats that control
        // frame as an out-of-band update; this test is interested in the eventual response.
        loop {
            let message = output.recv().await.unwrap().message;
            if matches!(message, GatewayControlMessage::AgentResponse(_)) {
                break;
            }
            assert!(matches!(message, GatewayControlMessage::PeerDirectory(_)));
        }
    }

    #[tokio::test]
    async fn registry_drain_fences_unary_requests_before_agent_dispatch() {
        let handler = Arc::new(CountingHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        mutate_replica(&control, |replica| {
            replica.state = GatewayReplicaState::Draining;
        })
        .await;

        let result = session
            .accept(frame(
                2,
                GatewayControlMessage::AgentRequest(GatewayAgentRequest {
                    action: GatewayAgentAction::EnrollmentBootstrap,
                    stream_id: GatewayConnectionId::new("drained-agent-request").unwrap(),
                    body: GatewayOpaqueBytes::new(b"{}".to_vec()).unwrap(),
                }),
            ))
            .await;

        assert!(matches!(result, Err(GatewaySessionError::Identity)));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn certificate_rotation_fences_route_renewal_from_the_old_session() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        rotate_replica_certificate(&control).await;

        let result = session
            .accept(frame(
                2,
                GatewayControlMessage::RouteRenew(GatewayRouteLeaseRequest {
                    agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                    owner_replica_id: replica_id(),
                    agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
                    session_generation: SessionGeneration::new(4),
                    route_generation: Some(RouteGeneration::new(1)),
                    requested_expires_at_unix_ms: UnixMillis::new(31_000),
                }),
            ))
            .await;

        assert!(matches!(result, Err(GatewaySessionError::Identity)));
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn registry_revoke_stops_existing_agent_stream_input_and_output() {
        let handler = Arc::new(HoldingRoutedHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = handler.entered.notified();
        let stream_id = GatewayConnectionId::new("revoked-agent-stream").unwrap();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: stream_id.clone(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered).await.unwrap();
        let input_closed = handler.input_closed.notified();
        mutate_replica(&control, |replica| {
            replica.state = GatewayReplicaState::Revoked;
            replica.credential.state = GatewayCredentialState::Revoked;
            replica.credential.certificate_generation = Some(CertificateGeneration::new(2));
        })
        .await;

        let inbound = session
            .accept(frame(
                3,
                GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                    stream_id: stream_id.clone(),
                    chunk: GatewayOpaqueBytes::new(b"agent-frame\n".to_vec()).unwrap(),
                }),
            ))
            .await;
        assert!(matches!(inbound, Err(GatewaySessionError::Identity)));
        timeout(Duration::from_secs(1), input_closed).await.unwrap();

        let outbound = session
            .send(
                RequestId::new("revoked-stream-output").unwrap(),
                None,
                GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                    stream_id,
                    chunk: GatewayOpaqueBytes::new(b"central-frame\n".to_vec()).unwrap(),
                }),
            )
            .await;
        assert!(matches!(outbound, Err(GatewaySessionError::Identity)));
        tokio::task::yield_now().await;
        assert!(output.try_recv().is_err());
    }

    #[tokio::test]
    async fn replacement_connection_immediately_closes_the_previous_stream_input() {
        let handler = Arc::new(HoldingRoutedHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (previous, mut previous_output) = control
            .open(
                identity.clone(),
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = handler.entered.notified();
        previous
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: GatewayConnectionId::new("replacement-agent-stream").unwrap(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered).await.unwrap();
        let input_closed = handler.input_closed.notified();

        let (_current, _current_output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();

        timeout(Duration::from_secs(1), input_closed).await.unwrap();
        assert!(matches!(
            previous
                .accept(frame(
                    3,
                    GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                        connected_agents: 0,
                        active_streams: 0,
                        queue_depth: 0,
                    }),
                ))
                .await,
            Err(GatewaySessionError::Identity)
        ));
        tokio::task::yield_now().await;
        assert!(previous_output.try_recv().is_err());
    }

    #[tokio::test]
    async fn registry_revoke_immediately_closes_a_pending_peer_forward() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request_id = RequestId::new("revoked-peer-forward").unwrap();
        let delivery = session.clone();
        let task = tokio::spawn(async move {
            delivery
                .send_peer_forward(
                    request_id,
                    GatewayPeerForwardRequest {
                        source_replica_id: replica_id(),
                        target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
                        target_peer_endpoint: "https://replica-b.peer.example".to_owned(),
                        agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
                        agent_connection_id: GatewayConnectionId::new("agent-connection-a")
                            .unwrap(),
                        session_generation: SessionGeneration::new(4),
                        route_generation: RouteGeneration::new(7),
                        frame: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
                    },
                )
                .await
        });
        assert!(matches!(
            output.recv().await.unwrap().message,
            GatewayControlMessage::PeerForward(_)
        ));
        mutate_replica(&control, |replica| {
            replica.state = GatewayReplicaState::Revoked;
            replica.credential.state = GatewayCredentialState::Revoked;
            replica.credential.certificate_generation = Some(CertificateGeneration::new(2));
        })
        .await;

        assert!(matches!(
            session
                .send(
                    RequestId::new("revoke-fence-trigger").unwrap(),
                    None,
                    GatewayControlMessage::Backpressure(GatewayBackpressure {
                        retry_after_ms: 1_000,
                    }),
                )
                .await,
            Err(GatewaySessionError::Identity)
        ));
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("pending forward must not wait for its 10 second deadline")
            .unwrap();
        assert!(matches!(result, Err(GatewaySessionError::Closed)));
    }

    #[tokio::test]
    async fn drain_frame_closes_stream_pending_forward_and_connector_signal() {
        let handler = Arc::new(HoldingRoutedHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let mut fenced = session.subscribe_fence();
        let entered = handler.entered.notified();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: GatewayConnectionId::new("draining-agent-stream").unwrap(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered).await.unwrap();
        let stream_exited = handler.input_closed.notified();

        let pending_session = session.clone();
        let pending = tokio::spawn(async move {
            pending_session
                .send_peer_forward(
                    RequestId::new("draining-peer-forward").unwrap(),
                    peer_forward_request(),
                )
                .await
        });
        assert!(matches!(
            output.recv().await.unwrap().message,
            GatewayControlMessage::PeerForward(_)
        ));

        let result = session
            .accept(frame(
                3,
                GatewayControlMessage::Drain(GatewayDrain {
                    deadline_unix_ms: UnixMillis::new(31_000),
                    reason: "maintenance".to_owned(),
                }),
            ))
            .await;

        assert!(matches!(result, Err(GatewaySessionError::Closed)));
        timeout(Duration::from_secs(1), stream_exited)
            .await
            .unwrap();
        let pending = timeout(Duration::from_secs(1), pending)
            .await
            .expect("drain must close pending forwarding immediately")
            .unwrap();
        assert!(matches!(pending, Err(GatewaySessionError::Closed)));
        timeout(Duration::from_secs(1), fenced.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(*fenced.borrow());
        assert_eq!(
            control
                .registry
                .get_replica(&replica_id())
                .await
                .unwrap()
                .unwrap()
                .state,
            GatewayReplicaState::Draining
        );
    }

    #[tokio::test]
    async fn protocol_error_closes_existing_stream_and_pending_forward() {
        let handler = Arc::new(HoldingRoutedHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = handler.entered.notified();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: GatewayConnectionId::new("protocol-error-agent-stream").unwrap(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered).await.unwrap();
        let stream_exited = handler.input_closed.notified();
        let pending_session = session.clone();
        let pending = tokio::spawn(async move {
            pending_session
                .send_peer_forward(
                    RequestId::new("protocol-error-peer-forward").unwrap(),
                    peer_forward_request(),
                )
                .await
        });
        assert!(matches!(
            output.recv().await.unwrap().message,
            GatewayControlMessage::PeerForward(_)
        ));

        let result = session
            .accept(frame(
                2,
                GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                    connected_agents: 1,
                    active_streams: 1,
                    queue_depth: 0,
                }),
            ))
            .await;

        assert!(matches!(result, Err(GatewaySessionError::Protocol(_))));
        timeout(Duration::from_secs(1), stream_exited)
            .await
            .unwrap();
        let pending = timeout(Duration::from_secs(1), pending)
            .await
            .expect("terminal protocol errors must close pending forwarding")
            .unwrap();
        assert!(matches!(pending, Err(GatewaySessionError::Closed)));
    }

    #[tokio::test]
    async fn revoke_discards_agent_input_queued_before_fencing() {
        let handler = Arc::new(BufferedInputHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = handler.entered.notified();
        let stream_id = GatewayConnectionId::new("buffered-agent-stream").unwrap();
        session
            .accept(frame(
                2,
                GatewayControlMessage::AgentStreamOpen(GatewayAgentStreamOpen {
                    stream_id: stream_id.clone(),
                    action: GatewayAgentAction::SessionChannelOpen,
                }),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), entered).await.unwrap();
        let mut data_frame = frame(
            3,
            GatewayControlMessage::AgentStreamData(GatewayAgentStreamData {
                stream_id,
                chunk: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
            }),
        );
        data_frame.request_id = RequestId::new("request-2").unwrap();
        session.accept(data_frame).await.unwrap();
        let exited = handler.exited.notified();
        mutate_replica(&control, |replica| {
            replica.state = GatewayReplicaState::Revoked;
            replica.credential.state = GatewayCredentialState::Revoked;
            replica.credential.certificate_generation = Some(CertificateGeneration::new(2));
        })
        .await;

        assert!(matches!(
            session
                .accept(frame(
                    4,
                    GatewayControlMessage::ReplicaHeartbeat(GatewayReplicaHeartbeat {
                        connected_agents: 1,
                        active_streams: 1,
                        queue_depth: 0,
                    }),
                ))
                .await,
            Err(GatewaySessionError::Identity)
        ));
        timeout(Duration::from_secs(1), exited).await.unwrap();
        handler.release.notify_waiters();
        assert_eq!(handler.consumed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn duplicate_peer_request_id_does_not_replace_the_first_waiter() {
        let (control, identity) = control_fixture().await;
        let (session, mut output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request_id = RequestId::new("duplicate-peer-forward").unwrap();
        let request = peer_forward_request();
        let expected = peer_forward_accepted(&request);
        let first_session = session.clone();
        let first_request_id = request_id.clone();
        let first = tokio::spawn(async move {
            first_session
                .send_peer_forward(first_request_id, request)
                .await
        });
        assert!(matches!(
            output.recv().await.unwrap().message,
            GatewayControlMessage::PeerForward(_)
        ));

        assert!(matches!(
            session
                .send_peer_forward(request_id.clone(), peer_forward_request())
                .await,
            Err(GatewaySessionError::Protocol(_))
        ));
        let mut acknowledgement = frame(2, GatewayControlMessage::PeerForwardAccepted(expected));
        acknowledgement.request_id = request_id;
        session.accept(acknowledgement).await.unwrap();
        timeout(Duration::from_secs(1), first)
            .await
            .expect("the original pending waiter must remain registered")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn replacement_connection_fences_the_previous_session() {
        let handler = Arc::new(CountingHandler::default());
        let (control, identity) = control_fixture_with_handler(handler.clone()).await;
        let (previous, mut previous_output) = control
            .open(
                identity.clone(),
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let (current, mut current_output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let request = || {
            frame(
                2,
                GatewayControlMessage::AgentRequest(GatewayAgentRequest {
                    action: GatewayAgentAction::EnrollmentBootstrap,
                    stream_id: GatewayConnectionId::new("replacement-agent-request").unwrap(),
                    body: GatewayOpaqueBytes::new(b"{}".to_vec()).unwrap(),
                }),
            )
        };

        assert!(matches!(
            previous.accept(request()).await,
            Err(GatewaySessionError::Identity)
        ));
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
        assert!(previous_output.try_recv().is_err());

        current.accept(request()).await.unwrap();
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            current_output.recv().await.unwrap().message,
            GatewayControlMessage::AgentResponse(_)
        ));
    }

    #[tokio::test]
    async fn replacement_fence_waits_for_an_admitted_dispatch() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let admission = session.admission.read().await;
        let replacing = session.clone();
        let mut fence = tokio::spawn(async move {
            replacing.fence_and_wait().await;
        });
        assert!(timeout(Duration::from_millis(5), &mut fence).await.is_err());
        drop(admission);
        timeout(Duration::from_secs(1), fence)
            .await
            .expect("replacement fence must complete after admitted work drains")
            .unwrap();
        assert!(session.fenced.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn replacement_fence_waits_for_aborted_stream_cleanup_before_route_release() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (session, _output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let route = registry.route();
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let worker = tokio::spawn({
            let entered = Arc::clone(&entered);
            let dropped = Arc::clone(&dropped);
            async move {
                entered.notify_one();
                let _cleanup = NotifyOnDrop(&dropped);
                std::future::pending::<()>().await;
            }
        });
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the stream worker must be running before fencing");
        let (writer, _input) = AgentControlInput::channel(1);
        session
            .inbound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .streams
            .insert(
                route.connection_id.clone(),
                InboundStream {
                    request_id: route.acquire_request_id.clone(),
                    writer,
                    task: worker,
                },
            );
        session.register_owned_route(
            route.connection_id.clone(),
            route.acquire_request_id.clone(),
        );

        session.fence_and_wait().await;
        timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("replacement fencing must await the aborted worker Drop guard");
        assert_eq!(registry.route_release_calls(), 1);
    }

    #[tokio::test]
    async fn replacement_fence_aborts_a_worker_retained_after_agent_stream_end() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let worker = tokio::spawn({
            let entered = Arc::clone(&entered);
            let dropped = Arc::clone(&dropped);
            async move {
                entered.notify_one();
                let _cleanup = NotifyOnDrop(&dropped);
                std::future::pending::<()>().await;
            }
        });
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the retained stream worker must be running before replacement");

        // AgentStreamEnd removes the worker from `inbound` and retains only its join handle.
        // Replacement fencing must still abort it rather than awaiting the handler forever.
        session.track_stream_task(worker);
        timeout(Duration::from_secs(1), session.fence_and_wait())
            .await
            .expect("replacement fencing must abort a retained stream worker");
        timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("the retained stream worker Drop guard must run");
    }

    #[tokio::test]
    async fn completed_stream_and_route_cleanup_handles_are_reaped() {
        let (control, identity) = control_fixture().await;
        let (session, _output) = control
            .open(
                identity,
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();

        let completed_stream = tokio::spawn(async {});
        timeout(Duration::from_secs(1), async {
            while !completed_stream.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed stream worker must become observable");
        session.track_stream_task(completed_stream);
        assert!(session
            .aborted_stream_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());

        let completed_release = tokio::spawn(async {});
        timeout(Duration::from_secs(1), async {
            while !completed_release.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed route cleanup must become observable");
        session
            .route_release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .push(completed_release);
        session.schedule_route_release(vec![RouteOwner {
            stream_id: GatewayConnectionId::new("reap-cleanup-stream").unwrap(),
            request_id: RequestId::new("reap-cleanup-request").unwrap(),
        }]);
        assert_eq!(
            session
                .route_release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tasks
                .len(),
            1
        );
        session.fence_and_wait().await;
    }

    #[tokio::test]
    async fn synchronous_close_starts_route_cleanup_for_a_disconnected_session() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let control = CentralGatewayControl::new(
            registry.clone(),
            Arc::new(EchoHandler),
            Arc::new(InMemoryClock::new(1_000)),
        );
        let (session, _output) = control
            .open(
                AuthenticatedGatewayReplica {
                    edge_cluster_id: cluster_id(),
                    gateway_pool_id: pool_id(),
                    gateway_replica_id: replica_id(),
                    certificate_generation: CertificateGeneration::new(1),
                },
                frame(1, GatewayControlMessage::ReplicaHello(hello_message())),
            )
            .await
            .unwrap();
        let route = registry.route();
        session.register_owned_route(
            route.connection_id.clone(),
            route.acquire_request_id.clone(),
        );

        session.close();
        timeout(Duration::from_secs(1), async {
            while registry.route_release_calls() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a disconnected session must release its owned route");
        assert_eq!(registry.route_release_calls(), 1);
    }

    #[tokio::test]
    async fn disconnected_gateway_route_release_is_bound_to_the_old_stream() {
        let registry = Arc::new(ForwardAuthorityRegistry::new());
        let route = registry.route();
        let clock: Arc<dyn Clock> = Arc::new(InMemoryClock::new(1_000));

        release_disconnected_gateway_routes(
            registry.clone(),
            clock.clone(),
            route.gateway_pool_id.clone(),
            route.gateway_replica_id.clone(),
            vec![RouteOwner {
                stream_id: route.connection_id.clone(),
                request_id: route.acquire_request_id.clone(),
            }],
        )
        .await;
        assert_eq!(registry.route_release_calls(), 1);

        release_disconnected_gateway_routes(
            registry.clone(),
            clock,
            route.gateway_pool_id,
            route.gateway_replica_id,
            vec![RouteOwner {
                stream_id: GatewayConnectionId::new("different-stream").unwrap(),
                request_id: route.acquire_request_id,
            }],
        )
        .await;
        assert_eq!(registry.route_release_calls(), 1);
    }

    async fn control_fixture() -> (CentralGatewayControl, AuthenticatedGatewayReplica) {
        control_fixture_with_handler(Arc::new(EchoHandler)).await
    }

    fn forwarding_replica_record(
        gateway_replica_id: GatewayReplicaId,
        control_endpoint: &str,
        peer_endpoint: &str,
    ) -> GatewayReplicaRecord {
        let mut replica = replica_record();
        replica.gateway_replica_id = gateway_replica_id;
        replica.control_endpoint = control_endpoint.to_owned();
        replica.peer_endpoint = peer_endpoint.to_owned();
        replica.bootstrap_endpoint = format!("{control_endpoint}/bootstrap");
        replica.state = GatewayReplicaState::Active;
        replica.credential.state = GatewayCredentialState::Active;
        replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(200));
        replica.credential.certificate_generation = Some(CertificateGeneration::new(1));
        replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(21_600_000));
        replica
    }

    fn authoritative_agent_route() -> AgentRouteLease {
        AgentRouteLease {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            connection_id: GatewayConnectionId::new("agent-owner-connection").unwrap(),
            session_generation: SessionGeneration::new(4),
            route_generation: RouteGeneration::new(7),
            acquire_request_id: RequestId::new("route-acquire-authority").unwrap(),
            last_renew_request_id: None,
            release_request_id: None,
            acquired_at_unix_ms: UnixMillis::new(500),
            acquired_lease_expires_at_unix_ms: UnixMillis::new(31_000),
            renewed_at_unix_ms: UnixMillis::new(500),
            lease_expires_at_unix_ms: UnixMillis::new(31_000),
            released_at_unix_ms: None,
        }
    }

    fn synthetic_route(route: &GatewayAgentRouteContext) -> AgentRouteLease {
        AgentRouteLease {
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            edge_cluster_id: cluster_id(),
            gateway_pool_id: route.gateway_pool_id.clone(),
            gateway_replica_id: route.gateway_replica_id.clone(),
            connection_id: route.connection_id.clone(),
            session_generation: SessionGeneration::new(4),
            route_generation: RouteGeneration::new(1),
            acquire_request_id: route.route_request_id.clone(),
            last_renew_request_id: None,
            release_request_id: None,
            acquired_at_unix_ms: route.observed_at_unix_ms,
            acquired_lease_expires_at_unix_ms: route.lease_expires_at_unix_ms,
            renewed_at_unix_ms: route.observed_at_unix_ms,
            lease_expires_at_unix_ms: route.lease_expires_at_unix_ms,
            released_at_unix_ms: None,
        }
    }

    fn forwarding_frame(session_generation: SessionGeneration) -> Bytes {
        Bytes::from(
            AgentChannelDownstreamFrame {
                wire_version: CURRENT_WIRE_VERSION,
                sequence: SequenceNumber::new(2),
                message_id: MessageId::new("forwarded-message-a").unwrap(),
                correlation_id: None,
                session_generation,
                sent_at_unix_ms: UnixMillis::new(1_000),
                central_signature: None,
                message: AgentChannelDownstreamMessage::Decision(JobDecision {
                    job_id: JobId::new("forwarded-job-a").unwrap(),
                    task_fence: TaskExecutionFence::new(
                        TaskId::new("task-forwarded-job-a").unwrap(),
                        Generation::new(1),
                        "materialize",
                        Generation::new(1),
                        Generation::new(1),
                    ),
                    assignment_id: AssignmentId::new("forwarded-assignment-a").unwrap(),
                    assignment_generation: AssignmentGeneration::new(1),
                    decision_generation: DecisionGeneration::new(1),
                    decision: PublishDecision::Reject {
                        error: ControlError {
                            code: ErrorCode::new("FORWARDED_TEST_REJECTED").unwrap(),
                            message: "forwarded test decision".to_owned(),
                            retryable: false,
                            retry_after_ms: None,
                            extensions: Extensions::new(),
                        },
                        extensions: Extensions::new(),
                    },
                    final_state: JobState::Rejected,
                    extensions: Extensions::new(),
                }),
                extensions: Extensions::new(),
            }
            .encode_ndjson()
            .unwrap(),
        )
    }

    fn frame_for_replica(
        gateway_replica_id: GatewayReplicaId,
        connection_id: GatewayConnectionId,
        sequence: u64,
        message: GatewayControlMessage,
    ) -> GatewayControlFrame {
        let mut frame = frame(sequence, message);
        frame.gateway_replica_id = gateway_replica_id;
        frame.connection_id = connection_id;
        frame
    }

    fn peer_forward_request() -> GatewayPeerForwardRequest {
        GatewayPeerForwardRequest {
            source_replica_id: replica_id(),
            target_replica_id: GatewayReplicaId::new("replica-b").unwrap(),
            target_peer_endpoint: "https://replica-b.peer.example".to_owned(),
            agent_id: neoengram_domain::protocol::AgentId::new("agent-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-a").unwrap(),
            session_generation: SessionGeneration::new(4),
            route_generation: RouteGeneration::new(7),
            frame: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
        }
    }

    fn peer_forward_accepted(request: &GatewayPeerForwardRequest) -> GatewayPeerForwardAccepted {
        GatewayPeerForwardAccepted {
            source_replica_id: request.source_replica_id.clone(),
            target_replica_id: request.target_replica_id.clone(),
            agent_id: request.agent_id.clone(),
            agent_connection_id: request.agent_connection_id.clone(),
            session_generation: request.session_generation,
            route_generation: request.route_generation,
        }
    }

    async fn mutate_replica(
        control: &CentralGatewayControl,
        update: impl FnOnce(&mut GatewayReplicaRecord),
    ) -> GatewayReplicaRecord {
        let mut replica = control
            .registry
            .get_replica(&replica_id())
            .await
            .unwrap()
            .unwrap();
        let expected = replica.resource_version.get();
        update(&mut replica);
        replica.resource_version = ResourceVersion::new(expected + 1);
        replica.updated_at_unix_ms = control.clock.now();
        control
            .registry
            .replace_replica(expected, replica)
            .await
            .unwrap()
    }

    async fn rotate_replica_certificate(control: &CentralGatewayControl) {
        mutate_replica(control, |replica| {
            let previous = replica.credential.certificate.as_ref().unwrap();
            let public_key = previous.public_key_spki.clone();
            let not_after_unix_ms =
                UnixMillis::new(previous.not_after_unix_ms.get().checked_add(1).unwrap());
            let leaf_certificate =
                GatewayOpaqueBytes::new(b"rotated-replica-cert".to_vec()).unwrap();
            replica.credential.certificate_generation = Some(CertificateGeneration::new(2));
            replica.credential.certificate_fingerprint =
                Some(ContentDigest::hash(leaf_certificate.as_bytes()));
            replica.credential.certificate_not_after_unix_ms = Some(not_after_unix_ms);
            replica.credential.certificate = Some(GatewayReplicaCertificateRecord {
                request_id: RequestId::new("gateway-rotation-request").unwrap(),
                public_key_spki: public_key,
                certificate_generation: CertificateGeneration::new(2),
                not_before_unix_ms: UnixMillis::new(1_000),
                not_after_unix_ms,
                server_names: BTreeSet::new(),
                leaf_certificate_der: leaf_certificate,
                issuer_chain_der: vec![
                    GatewayOpaqueBytes::new(b"rotated-replica-issuer".to_vec()).unwrap()
                ],
            });
        })
        .await;
    }

    async fn control_fixture_with_handler(
        handler: Arc<dyn AgentApiHandler>,
    ) -> (CentralGatewayControl, AuthenticatedGatewayReplica) {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        let mut pool = pool_record();
        assert!(matches!(
            repository.insert_pool(pool.clone()).await.unwrap(),
            GatewayInsertOutcome::Inserted(_)
        ));
        pool.state = GatewayPoolState::Ready;
        pool.config_generation = Generation::new(2);
        pool.resource_version = ResourceVersion::new(2);
        pool.updated_at_unix_ms = UnixMillis::new(200);
        repository.replace_pool(1, pool).await.unwrap();

        let mut replica = replica_record();
        repository.insert_replica(replica.clone()).await.unwrap();
        let public_key = Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
        let leaf_certificate = GatewayOpaqueBytes::new(b"replica-cert".to_vec()).unwrap();
        replica.credential.state = GatewayCredentialState::PendingCertificateDelivery;
        replica.credential.public_key_fingerprint = Some(public_key.fingerprint());
        replica.credential.certificate_generation = Some(CertificateGeneration::new(1));
        replica.credential.certificate_fingerprint =
            Some(ContentDigest::hash(leaf_certificate.as_bytes()));
        replica.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(21_600_000));
        replica.credential.certificate = Some(GatewayReplicaCertificateRecord {
            request_id: RequestId::new("gateway-activation-request").unwrap(),
            public_key_spki: public_key,
            certificate_generation: CertificateGeneration::new(1),
            not_before_unix_ms: UnixMillis::new(150),
            not_after_unix_ms: UnixMillis::new(21_600_000),
            server_names: BTreeSet::new(),
            leaf_certificate_der: leaf_certificate,
            issuer_chain_der: vec![GatewayOpaqueBytes::new(b"replica-issuer".to_vec()).unwrap()],
        });
        replica.resource_version = ResourceVersion::new(2);
        replica.updated_at_unix_ms = UnixMillis::new(150);
        replica = repository.replace_replica(1, replica).await.unwrap();

        replica.state = GatewayReplicaState::Active;
        replica.credential.state = GatewayCredentialState::Active;
        replica.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(200));
        replica.resource_version = ResourceVersion::new(3);
        replica.updated_at_unix_ms = UnixMillis::new(200);
        repository.replace_replica(2, replica).await.unwrap();

        let clock = Arc::new(InMemoryClock::new(1_000));
        let control = CentralGatewayControl::new(repository, handler, clock);
        let identity = AuthenticatedGatewayReplica {
            edge_cluster_id: cluster_id(),
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            certificate_generation: CertificateGeneration::new(1),
        };
        (control, identity)
    }

    fn pool_record() -> GatewayPoolRecord {
        GatewayPoolRecord {
            gateway_pool_id: pool_id(),
            edge_cluster_id: cluster_id(),
            display_name: "Test Gateway".to_owned(),
            agent_endpoint: "https://gateway.example".to_owned(),
            s3_endpoint: None,
            desired_replicas: 2,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Provisioning,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
            created_by: principal(),
            updated_by: principal(),
        }
    }

    fn replica_record() -> GatewayReplicaRecord {
        GatewayReplicaRecord {
            gateway_replica_id: replica_id(),
            gateway_pool_id: pool_id(),
            edge_cluster_id: cluster_id(),
            control_endpoint: "https://replica.control.example".to_owned(),
            peer_endpoint: "https://replica.peer.example".to_owned(),
            bootstrap_endpoint: "https://replica.bootstrap.example".to_owned(),
            software_version: "0.2.0".to_owned(),
            wire_version: CURRENT_WIRE_VERSION,
            capabilities: neoengram_domain::protocol::gateway_capabilities_v1(),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(b"activation-token"),
                activation_created_at_unix_ms: UnixMillis::new(100),
                activation_expires_at_unix_ms: UnixMillis::new(900_100),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        }
    }

    fn frame(sequence: u64, message: GatewayControlMessage) -> GatewayControlFrame {
        GatewayControlFrame {
            wire_version: CURRENT_WIRE_VERSION,
            gateway_pool_id: pool_id(),
            gateway_replica_id: replica_id(),
            connection_id: GatewayConnectionId::new("control-connection-a").unwrap(),
            sequence: SequenceNumber::new(sequence),
            request_id: RequestId::new(format!("request-{sequence}")).unwrap(),
            trace_id: Some(TraceId::new("trace-a").unwrap()),
            sent_at_unix_ms: UnixMillis::new(900),
            deadline_unix_ms: UnixMillis::new(2_000),
            hop_count: 0,
            message,
            extensions: Extensions::new(),
        }
    }

    fn hello_message() -> neoengram_domain::protocol::GatewayReplicaHello {
        neoengram_domain::protocol::GatewayReplicaHello {
            edge_cluster_id: cluster_id(),
            software_version: "0.2.0".to_owned(),
            wire_version: CURRENT_WIRE_VERSION,
            capabilities: neoengram_domain::protocol::gateway_capabilities_v1(),
        }
    }

    fn principal() -> PrincipalRef {
        PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("gateway-test").unwrap(),
            extensions: Extensions::new(),
        }
    }

    fn pool_id() -> GatewayPoolId {
        GatewayPoolId::new("pool-a").unwrap()
    }

    fn replica_id() -> GatewayReplicaId {
        GatewayReplicaId::new("replica-a").unwrap()
    }

    fn ingress_replica_id() -> GatewayReplicaId {
        GatewayReplicaId::new("replica-b").unwrap()
    }

    fn cluster_id() -> EdgeClusterId {
        EdgeClusterId::new("cluster-a").unwrap()
    }
}
