use std::{collections::BTreeSet, fmt};

use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentEnrollmentState, AgentId, CertificateGeneration, Ed25519PublicKeySpki, EdgeClusterId,
    GatewayConnectionId, GatewayOpaqueBytes, GatewayPoolId, GatewayReplicaId, Generation,
    PrincipalRef, ProtocolVersion, RequestId, ResourceVersion, RouteGeneration, SessionGeneration,
    UnixMillis, CURRENT_WIRE_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::{
    AgentInstanceState, AgentRegistryRecord, CentralError, CentralErrorCode, CentralResult,
    OpenAgentSessionRequest, OpenAgentSessionResult,
};

pub const GATEWAY_REGISTRY_MAX_PAGE_SIZE: usize = 256;
pub const GATEWAY_ENDPOINT_MAX_CHARS: usize = 2048;
pub const GATEWAY_TEXT_MAX_CHARS: usize = 256;
pub const GATEWAY_ACTIVATION_TOKEN_MAX_TTL_MS: u64 = 15 * 60 * 1000;
pub const AGENT_ROUTE_LEASE_MAX_TTL_MS: u64 = 30_000;
const GATEWAY_CERTIFICATE_MAX_BYTES: usize = 64 * 1024;
const GATEWAY_CERTIFICATE_CHAIN_MAX_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayPoolState {
    Provisioning,
    Ready,
    Draining,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayPoolRecord {
    pub gateway_pool_id: GatewayPoolId,
    pub edge_cluster_id: EdgeClusterId,
    pub display_name: String,
    pub agent_endpoint: String,
    pub s3_endpoint: Option<String>,
    pub desired_replicas: u16,
    pub minimum_ready_replicas: u16,
    pub state: GatewayPoolState,
    pub config_generation: Generation,
    pub resource_version: ResourceVersion,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    pub created_by: PrincipalRef,
    pub updated_by: PrincipalRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayReplicaState {
    Pending,
    Active,
    Draining,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayCredentialState {
    PendingActivation,
    PendingCertificateDelivery,
    Active,
    Expired,
    Revoked,
}

/// Public certificate material retained while a Replica activation is being delivered.
///
/// The activation token and Replica private key are never stored here.  Keeping the exact CA
/// response in the Registry makes certificate delivery an at-least-once operation: a Central
/// restart or a transport timeout can retry delivery without issuing a second certificate.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayReplicaCertificateRecord {
    pub request_id: RequestId,
    pub public_key_spki: Ed25519PublicKeySpki,
    pub certificate_generation: CertificateGeneration,
    pub not_before_unix_ms: UnixMillis,
    pub not_after_unix_ms: UnixMillis,
    /// Exact DNS/IP SAN set requested for the Gateway listener certificate.
    #[serde(default)]
    pub server_names: BTreeSet<String>,
    pub leaf_certificate_der: GatewayOpaqueBytes,
    pub issuer_chain_der: Vec<GatewayOpaqueBytes>,
}

impl fmt::Debug for GatewayReplicaCertificateRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayReplicaCertificateRecord")
            .field("request_id", &self.request_id)
            .field("public_key_spki", &self.public_key_spki)
            .field("certificate_generation", &self.certificate_generation)
            .field("not_before_unix_ms", &self.not_before_unix_ms)
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .field("server_names", &self.server_names)
            .field("leaf_certificate_der", &self.leaf_certificate_der)
            .field("issuer_chain_der", &self.issuer_chain_der)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayReplicaCredential {
    pub activation_token_digest: ContentDigest,
    pub activation_created_at_unix_ms: UnixMillis,
    pub activation_expires_at_unix_ms: UnixMillis,
    pub activation_consumed_at_unix_ms: Option<UnixMillis>,
    pub public_key_fingerprint: Option<ContentDigest>,
    pub certificate_generation: Option<CertificateGeneration>,
    pub certificate_fingerprint: Option<ContentDigest>,
    pub certificate_not_after_unix_ms: Option<UnixMillis>,
    /// Exact public certificate response. It is present after prepare and retained after commit.
    #[serde(default)]
    pub certificate: Option<GatewayReplicaCertificateRecord>,
    pub state: GatewayCredentialState,
}

impl fmt::Debug for GatewayReplicaCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayReplicaCredential")
            .field("activation_token_digest", &"[REDACTED]")
            .field(
                "activation_created_at_unix_ms",
                &self.activation_created_at_unix_ms,
            )
            .field(
                "activation_expires_at_unix_ms",
                &self.activation_expires_at_unix_ms,
            )
            .field(
                "activation_consumed_at_unix_ms",
                &self.activation_consumed_at_unix_ms,
            )
            .field("public_key_fingerprint", &self.public_key_fingerprint)
            .field("certificate_generation", &self.certificate_generation)
            .field("certificate_fingerprint", &self.certificate_fingerprint)
            .field(
                "certificate_not_after_unix_ms",
                &self.certificate_not_after_unix_ms,
            )
            .field("certificate", &self.certificate)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayReplicaRecord {
    pub gateway_replica_id: GatewayReplicaId,
    pub gateway_pool_id: GatewayPoolId,
    pub edge_cluster_id: EdgeClusterId,
    pub control_endpoint: String,
    pub peer_endpoint: String,
    pub bootstrap_endpoint: String,
    pub software_version: String,
    pub wire_version: ProtocolVersion,
    pub capabilities: BTreeSet<String>,
    pub last_heartbeat_at_unix_ms: Option<UnixMillis>,
    pub state: GatewayReplicaState,
    pub credential: GatewayReplicaCredential,
    pub resource_version: ResourceVersion,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRouteLease {
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
    pub acquire_request_id: RequestId,
    pub last_renew_request_id: Option<RequestId>,
    pub release_request_id: Option<RequestId>,
    pub acquired_at_unix_ms: UnixMillis,
    pub acquired_lease_expires_at_unix_ms: UnixMillis,
    pub renewed_at_unix_ms: UnixMillis,
    pub lease_expires_at_unix_ms: UnixMillis,
    pub released_at_unix_ms: Option<UnixMillis>,
}

impl AgentRouteLease {
    #[must_use]
    pub fn is_active_at(&self, now_unix_ms: UnixMillis) -> bool {
        self.released_at_unix_ms.is_none()
            && self.lease_expires_at_unix_ms.get() > now_unix_ms.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPoolListRequest {
    pub edge_cluster_id: Option<EdgeClusterId>,
    pub state: Option<GatewayPoolState>,
    pub after: Option<GatewayPoolId>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayReplicaListRequest {
    pub gateway_pool_id: GatewayPoolId,
    pub state: Option<GatewayReplicaState>,
    pub after: Option<GatewayReplicaId>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRouteLeaseListRequest {
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: Option<GatewayReplicaId>,
    pub active_at_unix_ms: Option<UnixMillis>,
    pub after: Option<AgentId>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireAgentRouteLeaseRequest {
    pub request_id: RequestId,
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub acquired_at_unix_ms: UnixMillis,
    pub lease_expires_at_unix_ms: UnixMillis,
}

/// Atomically establishes the authoritative Agent session and acquires its Gateway route.
///
/// `observed_at_unix_ms` is shared by the session-open transition and route acquisition so neither
/// half can become visible without the other. The repository derives the session generation and
/// EdgeCluster from the authoritative Agent aggregate rather than trusting Gateway input.
#[derive(Debug, Clone, PartialEq)]
pub struct AcquireAgentSessionRouteRequest {
    pub route_request_id: RequestId,
    pub session: OpenAgentSessionRequest,
    pub gateway_pool_id: GatewayPoolId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub observed_at_unix_ms: UnixMillis,
    pub lease_expires_at_unix_ms: UnixMillis,
    pub heartbeat_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewAgentRouteLeaseRequest {
    pub request_id: RequestId,
    pub agent_id: AgentId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
    pub renewed_at_unix_ms: UnixMillis,
    pub lease_expires_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseAgentRouteLeaseRequest {
    pub request_id: RequestId,
    pub agent_id: AgentId,
    pub gateway_replica_id: GatewayReplicaId,
    pub connection_id: GatewayConnectionId,
    pub session_generation: SessionGeneration,
    pub route_generation: RouteGeneration,
    pub released_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayInsertOutcome<T> {
    Inserted(T),
    Existing(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRouteLeaseAcquireOutcome {
    pub lease: AgentRouteLease,
    pub replayed: bool,
    pub fenced: Option<AgentRouteLease>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRouteLeaseMutationOutcome {
    pub lease: AgentRouteLease,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentSessionRouteAcquireOutcome {
    pub session: OpenAgentSessionResult,
    pub route: AgentRouteLeaseAcquireOutcome,
}

impl AcquireAgentSessionRouteRequest {
    pub(crate) fn route_request(
        &self,
        session: &OpenAgentSessionResult,
    ) -> AcquireAgentRouteLeaseRequest {
        AcquireAgentRouteLeaseRequest {
            request_id: self.route_request_id.clone(),
            agent_id: self.session.agent_id.clone(),
            edge_cluster_id: session.record.enrollment.edge_cluster_id.clone(),
            gateway_pool_id: self.gateway_pool_id.clone(),
            gateway_replica_id: self.gateway_replica_id.clone(),
            connection_id: self.connection_id.clone(),
            session_generation: session.session_generation,
            acquired_at_unix_ms: self.observed_at_unix_ms,
            lease_expires_at_unix_ms: self.lease_expires_at_unix_ms,
        }
    }
}

pub(crate) fn validate_agent_route_target(
    record: &AgentRegistryRecord,
    request: &AcquireAgentRouteLeaseRequest,
) -> CentralResult<()> {
    if record.enrollment.reserved_agent_id != request.agent_id
        || record.enrollment.edge_cluster_id != request.edge_cluster_id
    {
        return route_unavailable("Agent and Gateway route belong to different EdgeClusters");
    }
    validate_agent_route_session(record, request.agent_id.clone(), request.session_generation)
}

/// Re-checks the Agent aggregate for every route mutation after the initial acquire.
///
/// A route lease is not an independent credential: approval, instance lifecycle and the active
/// session generation remain authoritative in the Agent Registry. In particular, a connection
/// must not renew a lease after the Agent has been revoked or fenced by a newer boot.
pub(crate) fn validate_agent_route_session(
    record: &AgentRegistryRecord,
    agent_id: AgentId,
    session_generation: SessionGeneration,
) -> CentralResult<()> {
    if record.enrollment.reserved_agent_id != agent_id {
        return route_unavailable("Agent route identity is not registered");
    }
    let instance = record.instance.as_ref().ok_or_else(|| {
        CentralError::new(
            CentralErrorCode::GatewayRouteUnavailable,
            "Agent route requires an approved Agent instance",
        )
    })?;
    if record.enrollment.state != AgentEnrollmentState::Approved
        || instance.state != AgentInstanceState::Active
    {
        return route_unavailable("Agent route requires an approved Active Agent");
    }
    if instance.active_boot_id.is_none()
        || instance.active_session_id.is_none()
        || instance.session_generation != Some(session_generation)
    {
        return route_fenced("Agent route does not match the authoritative active session");
    }
    Ok(())
}

/// Verifies that the persisted owner is still eligible to acquire or renew an Agent route.
///
/// Route leases are deliberately short, but they must not be extendable merely because the
/// Agent session fence still matches. Replica revocation, pool draining, and workload leaf expiry
/// independently remove the owner's authority to keep the route alive.
pub(crate) fn validate_agent_route_owner(
    pool: &GatewayPoolRecord,
    replica: &GatewayReplicaRecord,
    edge_cluster_id: &EdgeClusterId,
    gateway_pool_id: &GatewayPoolId,
    gateway_replica_id: &GatewayReplicaId,
    observed_at_unix_ms: UnixMillis,
) -> CentralResult<()> {
    let credential_is_current = replica.credential.state == GatewayCredentialState::Active
        && replica.credential.certificate_generation.is_some()
        && replica
            .credential
            .certificate_not_after_unix_ms
            .is_some_and(|not_after| not_after.get() > observed_at_unix_ms.get());
    if &pool.gateway_pool_id != gateway_pool_id
        || &pool.edge_cluster_id != edge_cluster_id
        || pool.state != GatewayPoolState::Ready
        || &replica.gateway_replica_id != gateway_replica_id
        || &replica.gateway_pool_id != gateway_pool_id
        || &replica.edge_cluster_id != edge_cluster_id
        || replica.state != GatewayReplicaState::Active
        || !credential_is_current
    {
        return route_unavailable(
            "Agent route owner is not an Active credentialed Replica in a Ready Pool",
        );
    }
    Ok(())
}

pub(crate) fn validate_gateway_pool(record: &GatewayPoolRecord) -> CentralResult<()> {
    validate_text(
        "GatewayPool display_name",
        &record.display_name,
        GATEWAY_TEXT_MAX_CHARS,
    )?;
    validate_endpoint("GatewayPool agent_endpoint", &record.agent_endpoint)?;
    if let Some(endpoint) = &record.s3_endpoint {
        validate_endpoint("GatewayPool s3_endpoint", endpoint)?;
    }
    if record.desired_replicas == 0
        || record.minimum_ready_replicas == 0
        || record.minimum_ready_replicas > record.desired_replicas
    {
        return invalid("GatewayPool replica counts are inconsistent");
    }
    if record.config_generation.get() == 0 || record.resource_version.get() == 0 {
        return invalid("GatewayPool generations must be positive");
    }
    validate_timestamps(
        "GatewayPool",
        record.created_at_unix_ms,
        record.updated_at_unix_ms,
    )
}

pub(crate) fn validate_gateway_pool_replace(
    stored: &GatewayPoolRecord,
    expected_resource_version: u64,
    next: &GatewayPoolRecord,
) -> CentralResult<()> {
    validate_gateway_pool(next)?;
    if stored.resource_version.get() != expected_resource_version
        || next.resource_version.get() != expected_resource_version.checked_add(1).unwrap_or(0)
    {
        return concurrent("GatewayPool ResourceVersion changed");
    }
    if stored.gateway_pool_id != next.gateway_pool_id
        || stored.edge_cluster_id != next.edge_cluster_id
        || stored.created_at_unix_ms != next.created_at_unix_ms
        || stored.created_by != next.created_by
    {
        return invalid("GatewayPool immutable identity changed");
    }
    // Replica workload certificates include every Pool listener host in their DNS/IP SAN set.
    // Endpoint rotation needs a dedicated issue/deliver/restart protocol; accepting a mutation
    // here would make already-active Replicas fail TLS hostname validation immediately.
    if stored.agent_endpoint != next.agent_endpoint || stored.s3_endpoint != next.s3_endpoint {
        return invalid(
            "GatewayPool endpoints are immutable until workload certificate rotation is supported",
        );
    }
    if next.updated_at_unix_ms.get() < stored.updated_at_unix_ms.get() {
        return invalid("GatewayPool update time moved backwards");
    }
    let generation = next.config_generation.get();
    let previous_generation = stored.config_generation.get();
    if generation < previous_generation || generation > previous_generation.saturating_add(1) {
        return invalid("GatewayPool config generation must remain stable or advance once");
    }
    if !valid_pool_transition(stored.state, next.state) {
        return invalid("GatewayPool state transition is not allowed");
    }
    Ok(())
}

pub(crate) fn validate_gateway_replica(record: &GatewayReplicaRecord) -> CentralResult<()> {
    validate_endpoint("GatewayReplica control_endpoint", &record.control_endpoint)?;
    validate_endpoint("GatewayReplica peer_endpoint", &record.peer_endpoint)?;
    validate_endpoint(
        "GatewayReplica bootstrap_endpoint",
        &record.bootstrap_endpoint,
    )?;
    if record.control_endpoint == record.peer_endpoint
        || record.control_endpoint == record.bootstrap_endpoint
        || record.peer_endpoint == record.bootstrap_endpoint
    {
        return invalid("GatewayReplica endpoints must be pairwise distinct");
    }
    validate_text(
        "GatewayReplica software_version",
        &record.software_version,
        GATEWAY_TEXT_MAX_CHARS,
    )?;
    if record.wire_version != CURRENT_WIRE_VERSION || record.capabilities.len() > 128 {
        return invalid("GatewayReplica must advertise exactly the current wire version");
    }
    for capability in &record.capabilities {
        validate_text("GatewayReplica capability", capability, 128)?;
    }
    if record.resource_version.get() == 0 {
        return invalid("GatewayReplica ResourceVersion must be positive");
    }
    validate_timestamps(
        "GatewayReplica",
        record.created_at_unix_ms,
        record.updated_at_unix_ms,
    )?;
    validate_gateway_credential(&record.credential)?;
    match (record.state, record.credential.state) {
        (GatewayReplicaState::Pending, GatewayCredentialState::PendingActivation)
        | (GatewayReplicaState::Pending, GatewayCredentialState::PendingCertificateDelivery)
        | (GatewayReplicaState::Pending, GatewayCredentialState::Expired)
        | (GatewayReplicaState::Active, GatewayCredentialState::Active)
        | (GatewayReplicaState::Draining, GatewayCredentialState::Active)
        | (GatewayReplicaState::Revoked, GatewayCredentialState::Revoked) => Ok(()),
        _ => invalid("GatewayReplica and credential states are inconsistent"),
    }
}

pub(crate) fn validate_gateway_replica_replace(
    stored: &GatewayReplicaRecord,
    expected_resource_version: u64,
    next: &GatewayReplicaRecord,
) -> CentralResult<()> {
    validate_gateway_replica(next)?;
    if stored.resource_version.get() != expected_resource_version
        || next.resource_version.get() != expected_resource_version.checked_add(1).unwrap_or(0)
    {
        return concurrent("GatewayReplica ResourceVersion changed");
    }
    if stored.gateway_replica_id != next.gateway_replica_id
        || stored.gateway_pool_id != next.gateway_pool_id
        || stored.edge_cluster_id != next.edge_cluster_id
        || stored.created_at_unix_ms != next.created_at_unix_ms
        || stored.credential.activation_token_digest != next.credential.activation_token_digest
        || stored.credential.activation_created_at_unix_ms
            != next.credential.activation_created_at_unix_ms
        || stored.credential.activation_expires_at_unix_ms
            != next.credential.activation_expires_at_unix_ms
    {
        return invalid("GatewayReplica immutable identity changed");
    }
    // The bootstrap/control/peer hosts are copied into the workload certificate SAN set when
    // certificate preparation starts.  There is no safe in-place endpoint rotation until a new
    // certificate generation has been issued, delivered, installed, and acknowledged.  Keep the
    // endpoints mutable only while the activation credential is still completely unissued; this
    // lets a provisioner correct a pending registration without allowing a SAN/endpoint split
    // once Central has begun issuing or delivering identity material.
    let endpoint_changed = stored.control_endpoint != next.control_endpoint
        || stored.peer_endpoint != next.peer_endpoint
        || stored.bootstrap_endpoint != next.bootstrap_endpoint;
    let certificate_material_started = stored.credential.activation_consumed_at_unix_ms.is_some()
        || stored.credential.public_key_fingerprint.is_some()
        || stored.credential.certificate_generation.is_some()
        || stored.credential.certificate_fingerprint.is_some()
        || stored.credential.certificate_not_after_unix_ms.is_some()
        || stored.credential.certificate.is_some();
    if endpoint_changed && certificate_material_started {
        return invalid(
            "GatewayReplica endpoints are immutable until workload certificate rotation is supported",
        );
    }
    if stored
        .credential
        .activation_consumed_at_unix_ms
        .is_some_and(|value| next.credential.activation_consumed_at_unix_ms != Some(value))
        || stored
            .credential
            .public_key_fingerprint
            .is_some_and(|value| next.credential.public_key_fingerprint != Some(value))
    {
        return invalid("GatewayReplica activated identity changed");
    }
    if next.updated_at_unix_ms.get() < stored.updated_at_unix_ms.get()
        // Once Central has observed a heartbeat, a replacement may only advance it.  Treating
        // `None` as a valid successor would allow a stale writer to erase the liveness evidence.
        || stored
            .last_heartbeat_at_unix_ms
            .is_some_and(|stored| next.last_heartbeat_at_unix_ms.is_none_or(|next| next.get() < stored.get()))
    {
        return invalid("GatewayReplica observation time moved backwards");
    }
    if !valid_replica_transition(stored.state, next.state)
        || !valid_credential_transition(stored.credential.state, next.credential.state)
    {
        return invalid("GatewayReplica state transition is not allowed");
    }
    match (
        stored.credential.certificate_generation,
        next.credential.certificate_generation,
    ) {
        (None, None) => {}
        (None, Some(next_generation)) if next_generation.get() > 0 => {}
        (Some(stored_generation), Some(next_generation))
            if next_generation == stored_generation
                && next.credential.certificate_fingerprint
                    == stored.credential.certificate_fingerprint
                && next.credential.certificate_not_after_unix_ms
                    == stored.credential.certificate_not_after_unix_ms => {}
        (Some(stored_generation), Some(next_generation))
            if next_generation.get() == stored_generation.get().saturating_add(1)
                && ((next.state == GatewayReplicaState::Revoked
                    && next.credential.certificate_fingerprint
                        == stored.credential.certificate_fingerprint
                    && next.credential.certificate_not_after_unix_ms
                        == stored.credential.certificate_not_after_unix_ms)
                    || (next.credential.certificate_fingerprint
                        != stored.credential.certificate_fingerprint
                        && next
                            .credential
                            .certificate_not_after_unix_ms
                            .zip(stored.credential.certificate_not_after_unix_ms)
                            .is_some_and(|(next_expiry, stored_expiry)| {
                                next_expiry.get() > stored_expiry.get()
                            }))) => {}
        _ => return invalid("certificate generation must remain stable or advance once"),
    }
    if let Some(stored_certificate) = stored.credential.certificate.as_ref() {
        let Some(next_certificate) = next.credential.certificate.as_ref() else {
            return invalid("Gateway certificate payload cannot be cleared");
        };
        let same_generation =
            stored.credential.certificate_generation == next.credential.certificate_generation;
        if same_generation && stored_certificate != next_certificate {
            return invalid("Gateway certificate payload changed without advancing generation");
        }
    }
    Ok(())
}

/// Applies the parent Pool lifecycle fence to an existing Replica mutation.
///
/// A Draining or Disabled Pool must not admit new identities or ordinary metadata/certificate
/// changes, but already persisted Replicas still need a monotonic termination path: operators must
/// be able to drain/revoke them and the credential expiry reconciler must be able to fence expired
/// leaves.
/// Keep this check separate from the generic Replica transition validator so a terminal mutation
/// cannot smuggle endpoint, capability, or credential identity changes through a disabled scope.
pub(crate) fn validate_gateway_replica_parent_replace(
    pool: &GatewayPoolRecord,
    stored: &GatewayReplicaRecord,
    next: &GatewayReplicaRecord,
) -> CentralResult<()> {
    if pool.edge_cluster_id != next.edge_cluster_id || pool.gateway_pool_id != next.gateway_pool_id
    {
        return identity_conflict("GatewayReplica scope differs from its GatewayPool");
    }
    if !matches!(
        pool.state,
        GatewayPoolState::Draining | GatewayPoolState::Disabled
    ) {
        return Ok(());
    }

    let immutable_runtime_fields_match = stored.control_endpoint == next.control_endpoint
        && stored.peer_endpoint == next.peer_endpoint
        && stored.bootstrap_endpoint == next.bootstrap_endpoint
        && stored.software_version == next.software_version
        && stored.wire_version == next.wire_version
        && stored.capabilities == next.capabilities
        && stored.last_heartbeat_at_unix_ms == next.last_heartbeat_at_unix_ms;
    if !immutable_runtime_fields_match {
        return identity_conflict(
            "Draining or Disabled GatewayPool only permits terminal Replica fencing without metadata changes",
        );
    }

    let terminal_transition = match (
        stored.state,
        stored.credential.state,
        next.state,
        next.credential.state,
    ) {
        // A drain is an explicit monotonic shutdown step. A repeated Draining record is retained
        // as an idempotent terminal no-op for callers that race the management action.
        (GatewayReplicaState::Active, GatewayCredentialState::Active,
            GatewayReplicaState::Draining, GatewayCredentialState::Active)
        | (GatewayReplicaState::Draining, GatewayCredentialState::Active,
            GatewayReplicaState::Draining, GatewayCredentialState::Active)
        // Revocation is allowed from every persisted Replica/credential state accepted by the
        // generic validator, including a pending activation and an already expired token.
        | (_, _, GatewayReplicaState::Revoked, GatewayCredentialState::Revoked)
        // Expiring an unconsumed activation token is terminal even though the Replica remains
        // Pending; it must remain possible after a Pool is disabled.
        | (GatewayReplicaState::Pending, GatewayCredentialState::PendingActivation,
            GatewayReplicaState::Pending, GatewayCredentialState::Expired)
        | (GatewayReplicaState::Pending, GatewayCredentialState::Expired,
            GatewayReplicaState::Pending, GatewayCredentialState::Expired) => true,
        _ => false,
    };
    if !terminal_transition {
        return identity_conflict(
            "Draining or Disabled GatewayPool only permits Replica drain, revoke, or credential expiry",
        );
    }

    // Terminal mutations may advance the revocation generation by exactly one, but may not alter
    // any certificate identity/material. The generic validator already checks the allowed
    // generation transition; this comparison closes the revoked-state exception there.
    let generation_changed =
        stored.credential.certificate_generation != next.credential.certificate_generation;
    let allowed_generation_fence = next.state == GatewayReplicaState::Revoked
        && stored
            .credential
            .certificate_generation
            .zip(next.credential.certificate_generation)
            .is_some_and(|(stored, next)| next.get() == stored.get().saturating_add(1));
    if stored.credential.activation_consumed_at_unix_ms
        != next.credential.activation_consumed_at_unix_ms
        || stored.credential.public_key_fingerprint != next.credential.public_key_fingerprint
        || stored.credential.certificate_fingerprint != next.credential.certificate_fingerprint
        || stored.credential.certificate_not_after_unix_ms
            != next.credential.certificate_not_after_unix_ms
        || stored.credential.certificate != next.credential.certificate
        || (generation_changed && !allowed_generation_fence)
    {
        return identity_conflict(
            "Disabled GatewayPool terminal Replica mutation changed credential identity or material",
        );
    }
    Ok(())
}

pub(crate) fn validate_pool_list_request(request: &GatewayPoolListRequest) -> CentralResult<()> {
    validate_limit(request.limit)
}

pub(crate) fn validate_replica_list_request(
    request: &GatewayReplicaListRequest,
) -> CentralResult<()> {
    validate_limit(request.limit)
}

pub(crate) fn validate_route_list_request(
    request: &AgentRouteLeaseListRequest,
) -> CentralResult<()> {
    validate_limit(request.limit)?;
    if let Some(active_at) = request.active_at_unix_ms {
        validate_sqlite_timestamp(active_at, "Agent route list timestamp")?;
    }
    Ok(())
}

pub(crate) fn validate_acquire_request(
    request: &AcquireAgentRouteLeaseRequest,
) -> CentralResult<()> {
    validate_positive_generation(
        request.session_generation.get(),
        "Agent route session generation",
    )?;
    validate_lease_window(
        request.acquired_at_unix_ms,
        request.lease_expires_at_unix_ms,
    )
}

pub(crate) fn validate_renew_request(request: &RenewAgentRouteLeaseRequest) -> CentralResult<()> {
    validate_positive_generation(
        request.session_generation.get(),
        "Agent route session generation",
    )?;
    validate_positive_generation(request.route_generation.get(), "Agent route generation")?;
    validate_lease_window(request.renewed_at_unix_ms, request.lease_expires_at_unix_ms)
}

pub(crate) fn validate_release_request(
    request: &ReleaseAgentRouteLeaseRequest,
) -> CentralResult<()> {
    validate_positive_generation(
        request.session_generation.get(),
        "Agent route session generation",
    )?;
    validate_positive_generation(request.route_generation.get(), "Agent route generation")?;
    validate_sqlite_timestamp(request.released_at_unix_ms, "Agent route release timestamp")
}

pub(crate) fn build_acquired_route(
    request: &AcquireAgentRouteLeaseRequest,
    route_generation: u64,
) -> CentralResult<AgentRouteLease> {
    validate_acquire_request(request)?;
    validate_positive_generation(route_generation, "Agent route generation")?;
    Ok(AgentRouteLease {
        agent_id: request.agent_id.clone(),
        edge_cluster_id: request.edge_cluster_id.clone(),
        gateway_pool_id: request.gateway_pool_id.clone(),
        gateway_replica_id: request.gateway_replica_id.clone(),
        connection_id: request.connection_id.clone(),
        session_generation: request.session_generation,
        route_generation: RouteGeneration::new(route_generation),
        acquire_request_id: request.request_id.clone(),
        last_renew_request_id: None,
        release_request_id: None,
        acquired_at_unix_ms: request.acquired_at_unix_ms,
        acquired_lease_expires_at_unix_ms: request.lease_expires_at_unix_ms,
        renewed_at_unix_ms: request.acquired_at_unix_ms,
        lease_expires_at_unix_ms: request.lease_expires_at_unix_ms,
        released_at_unix_ms: None,
    })
}

pub(crate) fn acquire_route_against(
    stored: Option<&AgentRouteLease>,
    request: &AcquireAgentRouteLeaseRequest,
) -> CentralResult<AgentRouteLeaseAcquireOutcome> {
    validate_acquire_request(request)?;
    if let Some(stored) = stored {
        if stored.acquire_request_id == request.request_id {
            if acquire_matches(stored, request) {
                return Ok(AgentRouteLeaseAcquireOutcome {
                    lease: stored.clone(),
                    replayed: true,
                    fenced: None,
                });
            }
            return concurrent("Agent route acquire RequestId was reused");
        }
        if stored.last_renew_request_id.as_ref() == Some(&request.request_id)
            || stored.release_request_id.as_ref() == Some(&request.request_id)
        {
            return concurrent("Agent route RequestId was reused for another mutation");
        }
        if request.session_generation.get() < stored.session_generation.get() {
            return route_fenced("Agent route session generation is stale");
        }
        if stored.is_active_at(request.acquired_at_unix_ms)
            && request.session_generation.get() <= stored.session_generation.get()
        {
            return route_unavailable("another connection owns the active Agent route");
        }
        let next_generation = stored
            .route_generation
            .get()
            .checked_add(1)
            .ok_or_else(|| internal("Agent route generation exhausted"))?;
        return Ok(AgentRouteLeaseAcquireOutcome {
            lease: build_acquired_route(request, next_generation)?,
            replayed: false,
            fenced: Some(stored.clone()),
        });
    }
    Ok(AgentRouteLeaseAcquireOutcome {
        lease: build_acquired_route(request, 1)?,
        replayed: false,
        fenced: None,
    })
}

pub(crate) fn renew_route_against(
    stored: &AgentRouteLease,
    request: &RenewAgentRouteLeaseRequest,
) -> CentralResult<AgentRouteLeaseMutationOutcome> {
    validate_renew_request(request)?;
    validate_route_fence(
        stored,
        &request.agent_id,
        &request.gateway_replica_id,
        &request.connection_id,
        request.session_generation,
        request.route_generation,
    )?;
    if stored.last_renew_request_id.as_ref() == Some(&request.request_id) {
        if stored.renewed_at_unix_ms == request.renewed_at_unix_ms
            && stored.lease_expires_at_unix_ms == request.lease_expires_at_unix_ms
        {
            return Ok(AgentRouteLeaseMutationOutcome {
                lease: stored.clone(),
                replayed: true,
            });
        }
        return concurrent("Agent route renew RequestId was reused");
    }
    if stored.acquire_request_id == request.request_id
        || stored.release_request_id.as_ref() == Some(&request.request_id)
    {
        return concurrent("Agent route RequestId was reused for another mutation");
    }
    if !stored.is_active_at(request.renewed_at_unix_ms) {
        return route_fenced("Agent route lease already expired or was released");
    }
    if request.lease_expires_at_unix_ms.get() <= stored.lease_expires_at_unix_ms.get()
        || request.renewed_at_unix_ms.get() < stored.renewed_at_unix_ms.get()
    {
        return invalid("Agent route renewal must extend the lease without moving time backwards");
    }
    let mut lease = stored.clone();
    lease.last_renew_request_id = Some(request.request_id.clone());
    lease.renewed_at_unix_ms = request.renewed_at_unix_ms;
    lease.lease_expires_at_unix_ms = request.lease_expires_at_unix_ms;
    Ok(AgentRouteLeaseMutationOutcome {
        lease,
        replayed: false,
    })
}

pub(crate) fn release_route_against(
    stored: &AgentRouteLease,
    request: &ReleaseAgentRouteLeaseRequest,
) -> CentralResult<AgentRouteLeaseMutationOutcome> {
    validate_release_request(request)?;
    validate_route_fence(
        stored,
        &request.agent_id,
        &request.gateway_replica_id,
        &request.connection_id,
        request.session_generation,
        request.route_generation,
    )?;
    if let Some(released_at) = stored.released_at_unix_ms {
        if stored.release_request_id.as_ref() == Some(&request.request_id)
            && released_at == request.released_at_unix_ms
        {
            return Ok(AgentRouteLeaseMutationOutcome {
                lease: stored.clone(),
                replayed: true,
            });
        }
        return route_fenced("Agent route lease was already released");
    }
    if stored.acquire_request_id == request.request_id
        || stored.last_renew_request_id.as_ref() == Some(&request.request_id)
    {
        return concurrent("Agent route RequestId was reused for another mutation");
    }
    if request.released_at_unix_ms.get() < stored.renewed_at_unix_ms.get() {
        return invalid("Agent route release precedes its latest acquisition or renewal");
    }
    let mut lease = stored.clone();
    lease.release_request_id = Some(request.request_id.clone());
    lease.released_at_unix_ms = Some(request.released_at_unix_ms);
    if request.released_at_unix_ms.get() < lease.lease_expires_at_unix_ms.get() {
        lease.lease_expires_at_unix_ms = request.released_at_unix_ms;
    }
    Ok(AgentRouteLeaseMutationOutcome {
        lease,
        replayed: false,
    })
}

fn validate_gateway_credential(credential: &GatewayReplicaCredential) -> CentralResult<()> {
    let activation_ttl = credential
        .activation_expires_at_unix_ms
        .get()
        .checked_sub(credential.activation_created_at_unix_ms.get())
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "Gateway activation expiry precedes creation",
            )
            .with_retryable(false)
        })?;
    if activation_ttl == 0 || activation_ttl > GATEWAY_ACTIVATION_TOKEN_MAX_TTL_MS {
        return invalid("Gateway activation token lifetime exceeds 15 minutes");
    }
    let issuance_presence = [
        credential.activation_consumed_at_unix_ms.is_some(),
        credential.public_key_fingerprint.is_some(),
        credential.certificate_generation.is_some(),
        credential.certificate_fingerprint.is_some(),
        credential.certificate_not_after_unix_ms.is_some(),
    ];
    let metadata_issued = issuance_presence.iter().all(|present| *present);
    let metadata_unissued = issuance_presence.iter().all(|present| !*present);
    let certificate = credential.certificate.as_ref();
    let issued = metadata_issued && certificate.is_some();
    // During prepare the token remains unconsumed, but all other issuance metadata and the exact
    // certificate response are durable. This is the recoverable delivery state.
    let prepared = !issuance_presence[0]
        && issuance_presence[1..].iter().all(|present| *present)
        && certificate.is_some();
    let unissued = metadata_unissued && certificate.is_none();
    if !issued && !prepared && !unissued {
        return invalid("Gateway credential issuance material is only partially present");
    }
    if let Some(certificate) = certificate {
        validate_gateway_certificate(certificate)?;
        if credential.public_key_fingerprint != Some(certificate.public_key_spki.fingerprint())
            || credential.certificate_fingerprint
                != Some(ContentDigest::hash(
                    certificate.leaf_certificate_der.as_bytes(),
                ))
            || credential.certificate_not_after_unix_ms != Some(certificate.not_after_unix_ms)
            || credential.certificate_generation != Some(certificate.certificate_generation)
                && credential.state != GatewayCredentialState::Revoked
        {
            return invalid("Gateway credential certificate metadata disagrees with payload");
        }
    }
    match credential.state {
        GatewayCredentialState::PendingActivation if unissued => Ok(()),
        GatewayCredentialState::PendingCertificateDelivery if prepared => Ok(()),
        GatewayCredentialState::Active if issued => Ok(()),
        GatewayCredentialState::Expired if unissued => Ok(()),
        GatewayCredentialState::Revoked if issued || prepared || unissued => Ok(()),
        _ => invalid("Gateway credential material is inconsistent with its state"),
    }?;
    if credential
        .activation_consumed_at_unix_ms
        .is_some_and(|consumed| consumed.get() < credential.activation_created_at_unix_ms.get())
        || credential
            .certificate_not_after_unix_ms
            .zip(credential.activation_consumed_at_unix_ms)
            .is_some_and(|(not_after, consumed)| not_after.get() <= consumed.get())
    {
        return invalid("Gateway credential timestamps are inconsistent");
    }
    Ok(())
}

fn validate_gateway_certificate(
    certificate: &GatewayReplicaCertificateRecord,
) -> CentralResult<()> {
    if certificate.certificate_generation.get() == 0
        || certificate.not_before_unix_ms.get() == 0
        || certificate.not_after_unix_ms.get() <= certificate.not_before_unix_ms.get()
        || certificate.leaf_certificate_der.as_bytes().is_empty()
        || certificate.leaf_certificate_der.as_bytes().len() > GATEWAY_CERTIFICATE_MAX_BYTES
        || certificate.issuer_chain_der.is_empty()
        || certificate.issuer_chain_der.len() > GATEWAY_CERTIFICATE_CHAIN_MAX_DEPTH
        || certificate.issuer_chain_der.iter().any(|certificate| {
            certificate.as_bytes().is_empty()
                || certificate.as_bytes().len() > GATEWAY_CERTIFICATE_MAX_BYTES
        })
    {
        return invalid("Gateway certificate material is outside its limits");
    }
    Ok(())
}

fn validate_route_fence(
    stored: &AgentRouteLease,
    agent_id: &AgentId,
    gateway_replica_id: &GatewayReplicaId,
    connection_id: &GatewayConnectionId,
    session_generation: SessionGeneration,
    route_generation: RouteGeneration,
) -> CentralResult<()> {
    if &stored.agent_id == agent_id
        && &stored.gateway_replica_id == gateway_replica_id
        && &stored.connection_id == connection_id
        && stored.session_generation == session_generation
        && stored.route_generation == route_generation
    {
        Ok(())
    } else {
        route_fenced("Agent route fence does not match the current owner")
    }
}

fn acquire_matches(stored: &AgentRouteLease, request: &AcquireAgentRouteLeaseRequest) -> bool {
    stored.agent_id == request.agent_id
        && stored.edge_cluster_id == request.edge_cluster_id
        && stored.gateway_pool_id == request.gateway_pool_id
        && stored.gateway_replica_id == request.gateway_replica_id
        && stored.connection_id == request.connection_id
        && stored.session_generation == request.session_generation
        && stored.acquired_at_unix_ms == request.acquired_at_unix_ms
        && stored.acquired_lease_expires_at_unix_ms == request.lease_expires_at_unix_ms
}

fn valid_pool_transition(from: GatewayPoolState, to: GatewayPoolState) -> bool {
    from == to
        || matches!(
            (from, to),
            (GatewayPoolState::Provisioning, GatewayPoolState::Ready)
                | (GatewayPoolState::Provisioning, GatewayPoolState::Draining)
                | (GatewayPoolState::Provisioning, GatewayPoolState::Disabled)
                | (GatewayPoolState::Ready, GatewayPoolState::Draining)
                | (GatewayPoolState::Ready, GatewayPoolState::Disabled)
                | (GatewayPoolState::Draining, GatewayPoolState::Disabled)
        )
}

fn valid_replica_transition(from: GatewayReplicaState, to: GatewayReplicaState) -> bool {
    from == to
        || matches!(
            (from, to),
            (GatewayReplicaState::Pending, GatewayReplicaState::Active)
                | (GatewayReplicaState::Pending, GatewayReplicaState::Revoked)
                | (GatewayReplicaState::Active, GatewayReplicaState::Draining)
                | (GatewayReplicaState::Active, GatewayReplicaState::Revoked)
                | (GatewayReplicaState::Draining, GatewayReplicaState::Revoked)
        )
}

fn valid_credential_transition(from: GatewayCredentialState, to: GatewayCredentialState) -> bool {
    from == to
        || matches!(
            (from, to),
            (
                GatewayCredentialState::PendingActivation,
                GatewayCredentialState::PendingCertificateDelivery
            ) | (
                GatewayCredentialState::PendingCertificateDelivery,
                GatewayCredentialState::Active
            ) | (
                GatewayCredentialState::PendingActivation,
                GatewayCredentialState::Expired
            ) | (
                GatewayCredentialState::PendingActivation,
                GatewayCredentialState::Revoked
            ) | (
                GatewayCredentialState::PendingCertificateDelivery,
                GatewayCredentialState::Revoked
            ) | (
                GatewayCredentialState::Active,
                GatewayCredentialState::Revoked
            ) | (
                GatewayCredentialState::Expired,
                GatewayCredentialState::Revoked
            )
        )
}

fn validate_endpoint(field: &str, endpoint: &str) -> CentralResult<()> {
    validate_text(field, endpoint, GATEWAY_ENDPOINT_MAX_CHARS)?;
    let parsed = url::Url::parse(endpoint).map_err(|_| {
        CentralError::new(
            CentralErrorCode::InvalidState,
            format!(
                "{field} must be a canonical HTTPS origin (or loopback HTTP origin in development)"
            ),
        )
        .with_retryable(false)
    })?;
    let canonical = parsed.as_str().strip_suffix('/').unwrap_or(parsed.as_str());
    let invalid_origin_shape = parsed.cannot_be_a_base()
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || canonical != endpoint;
    if invalid_origin_shape {
        return invalid(format!(
            "{field} must be a canonical origin without credentials, path, query, or fragment"
        ));
    }
    if parsed.scheme() == "https" {
        return Ok(());
    }
    let loopback = parsed.scheme() == "http"
        && parsed.host().is_some_and(|host| match host {
            url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
            url::Host::Ipv4(address) => address.is_loopback(),
            url::Host::Ipv6(address) => address.is_loopback(),
        });
    if !loopback {
        return invalid(format!(
            "{field} must use HTTPS unless it is a literal loopback HTTP origin"
        ));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str, max_chars: usize) -> CentralResult<()> {
    let length = value.chars().count();
    if length == 0 || length > max_chars || value.chars().any(char::is_control) {
        return invalid(format!(
            "{field} must contain 1..={max_chars} non-control characters"
        ));
    }
    Ok(())
}

fn validate_timestamps(
    resource: &str,
    created_at: UnixMillis,
    updated_at: UnixMillis,
) -> CentralResult<()> {
    if updated_at.get() < created_at.get() {
        invalid(format!("{resource} update time precedes creation"))
    } else {
        Ok(())
    }
}

fn validate_limit(limit: usize) -> CentralResult<()> {
    if limit == 0 || limit > GATEWAY_REGISTRY_MAX_PAGE_SIZE {
        invalid(format!(
            "Gateway registry list limit must be in 1..={GATEWAY_REGISTRY_MAX_PAGE_SIZE}"
        ))
    } else {
        Ok(())
    }
}

fn validate_positive_generation(value: u64, field: &str) -> CentralResult<()> {
    if value == 0 {
        invalid(format!("{field} must be positive"))
    } else {
        Ok(())
    }
}

fn validate_lease_window(start: UnixMillis, expires: UnixMillis) -> CentralResult<()> {
    validate_sqlite_timestamp(start, "Agent route lease start timestamp")?;
    validate_sqlite_timestamp(expires, "Agent route lease expiry timestamp")?;
    let ttl = expires.get().checked_sub(start.get()).ok_or_else(|| {
        CentralError::new(
            CentralErrorCode::InvalidState,
            "lease expiry precedes its start",
        )
        .with_retryable(false)
    })?;
    if ttl == 0 || ttl > AGENT_ROUTE_LEASE_MAX_TTL_MS {
        return invalid(format!(
            "lease lifetime must be in 1..={AGENT_ROUTE_LEASE_MAX_TTL_MS} milliseconds"
        ));
    }
    Ok(())
}

fn validate_sqlite_timestamp(value: UnixMillis, field: &str) -> CentralResult<()> {
    if value.get() > i64::MAX as u64 {
        invalid(format!(
            "{field} exceeds the SQLite INTEGER timestamp range"
        ))
    } else {
        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false))
}

fn concurrent<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(
        CentralErrorCode::ConcurrentUpdate,
        message,
    ))
}

fn route_unavailable<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(
        CentralErrorCode::GatewayRouteUnavailable,
        message,
    ))
}

fn route_fenced<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::GatewayRouteFenced, message).with_retryable(false))
}

fn identity_conflict<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::GatewayIdentityConflict, message).with_retryable(false))
}

fn internal(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::validate_endpoint;

    #[test]
    fn endpoint_policy_allows_only_canonical_loopback_http_for_development() {
        for endpoint in [
            "http://127.0.0.1:8081",
            "http://127.0.0.1:8080",
            "http://[::1]:8081",
            "http://localhost:8081",
            "https://gateway.example:8443",
        ] {
            validate_endpoint("gateway endpoint", endpoint).unwrap_or_else(|error| {
                panic!("expected endpoint to validate: {endpoint}: {error}")
            });
        }
        for endpoint in [
            "http://10.0.0.1:8081",
            "http://gateway.internal:8081",
            "http://127.0.0.1:8081/",
            "https://gateway.example:8443/path",
        ] {
            assert!(
                validate_endpoint("gateway endpoint", endpoint).is_err(),
                "endpoint must be rejected: {endpoint}"
            );
        }
    }
}
