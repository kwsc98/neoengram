//! Placement-first control actions.
//!
//! The authority owns the logical request and its fencing identity.  Byte movement is delegated
//! to Agent/Gateway data-plane workers; this service never reads or stores object payloads.

use std::collections::{BTreeMap, BTreeSet};

use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::materialization::{
    AvailabilityStatus, CoverageState, MaterializationJob, MaterializationJobState,
    NamespaceObjectSet, ObjectPlacement as ObjectPlacementV2, ObjectPlacementState, ViewReadiness,
    VolumeCommitCoverage,
};
use neoengram_domain::protocol::{
    ArtifactId, BackendId, CommitPlacementSet, DecimalU64, EdgeClusterId, GatewayPoolId,
    Generation, MaterializationId, MountGeneration, ObjectNamespaceId, PlacementGeneration,
    PlacementId, PlacementSetId, PlacementState, ProjectId, ReplicationId, ReplicationState,
    RequestId, RouteGeneration, SessionGeneration, StorageVolumeId, TenantId, TransferEndpoint,
    TransferId, TransferRouteId, TransferTicket, UnixMillis, WorkspaceId, WorkspaceLifecycle,
};
use neoengram_domain::CommitId;

use crate::{
    dto::{
        CancelCommitMaterializationRequest, CancelCommitMaterializationResponse,
        CancelCommitReplicationRequest, CancelCommitReplicationResponse, CommitAvailabilityV2View,
        CommitAvailabilityView, CommitPlacementView, CreateCommitMaterializationRequest,
        CreateCommitMaterializationResponse, CreateCommitReplicationRequest,
        CreateCommitReplicationResponse, CreateWorkspaceRequest, CreateWorkspaceResponse,
        MaterializationView, MissingObjectView, QueryCommitAvailabilityRequest,
        QueryCommitAvailabilityResponse, QueryCommitAvailabilityV2Request,
        QueryCommitAvailabilityV2Response, QueryCommitCoverageRequest, QueryCommitCoverageResponse,
        QueryCommitMaterializationListRequest, QueryCommitMaterializationListResponse,
        QueryCommitMaterializationRequest, QueryCommitMaterializationResponse,
        QueryCommitPlacementListRequest, QueryCommitPlacementListResponse,
        QueryCommitReplicationListRequest, QueryCommitReplicationListResponse,
        QueryCommitReplicationRequest, QueryCommitReplicationResponse,
        QueryCommitReplicationTicketRequest, QueryCommitReplicationTicketResponse, ReplicationView,
        RetryCommitMaterializationRequest, RetryCommitMaterializationResponse,
        RetryCommitReplicationRequest, RetryCommitReplicationResponse, VolumeCommitCoverageView,
        WorkspaceView,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission},
    RefreshReplicationRoutesRequest, ReplicationRouteBinding,
};

use super::CatalogService;

fn not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_volume(value: String) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value)
        .map_err(|error| invalid_request(format!("target_storage_volume_id: {error}")))
}

fn parse_artifact(value: String) -> Result<ArtifactId, Error> {
    ArtifactId::new(value).map_err(|error| invalid_request(format!("artifact_id: {error}")))
}

fn parse_commit(value: String) -> Result<ContentDigest, Error> {
    value
        .parse()
        .map_err(|_| invalid_request("commit_id must be a 64-character digest"))
}

fn parse_attempt(value: String) -> Result<u64, Error> {
    value
        .parse::<u64>()
        .map_err(|_| invalid_request("expected_attempt must be a canonical unsigned integer"))
}

fn parse_namespace(value: String) -> Result<ObjectNamespaceId, Error> {
    ObjectNamespaceId::new(value)
        .map_err(|error| invalid_request(format!("object_namespace_id: {error}")))
}

fn parse_materialization_id(value: String) -> Result<MaterializationId, Error> {
    MaterializationId::new(value)
        .map_err(|error| invalid_request(format!("materialization_id: {error}")))
}

fn parse_generation(value: String, field: &'static str) -> Result<Generation, Error> {
    value
        .parse::<u64>()
        .map(Generation::new)
        .map_err(|_| invalid_request(format!("{field} must be a canonical unsigned integer")))
}

fn materialization_state_name(state: MaterializationJobState) -> &'static str {
    match state {
        MaterializationJobState::Queued => "queued",
        MaterializationJobState::Planning => "planning",
        MaterializationJobState::WaitingForSources => "waiting_for_sources",
        MaterializationJobState::Materializing => "materializing",
        MaterializationJobState::Verifying => "verifying",
        MaterializationJobState::Complete => "complete",
        MaterializationJobState::Stalled => "stalled",
        MaterializationJobState::Failed => "failed",
        MaterializationJobState::Cancelled => "cancelled",
    }
}

fn coverage_state_name(state: CoverageState) -> &'static str {
    match state {
        CoverageState::Partial => "partial",
        CoverageState::Complete => "complete",
        CoverageState::Retiring => "retiring",
        CoverageState::Deleted => "deleted",
    }
}

fn availability_status_name(status: AvailabilityStatus) -> &'static str {
    match status {
        AvailabilityStatus::Available => "available",
        AvailabilityStatus::Degraded => "degraded",
        AvailabilityStatus::Unavailable => "unavailable",
        AvailabilityStatus::Unknown => "unknown",
    }
}

fn view_readiness_name(readiness: ViewReadiness) -> &'static str {
    match readiness {
        ViewReadiness::Ready => "ready",
        ViewReadiness::NotReady => "not_ready",
    }
}

fn availability_status_for_counts(total: usize, present: usize) -> AvailabilityStatus {
    if present == 0 {
        AvailabilityStatus::Unavailable
    } else if present == total {
        AvailabilityStatus::Available
    } else {
        AvailabilityStatus::Degraded
    }
}

fn object_encoding_name(encoding: neoengram_domain::protocol::ObjectEncoding) -> &'static str {
    match encoding {
        neoengram_domain::protocol::ObjectEncoding::Raw => "raw",
        neoengram_domain::protocol::ObjectEncoding::Zstd => "zstd",
    }
}

fn coverage_view(coverage: &VolumeCommitCoverage) -> VolumeCommitCoverageView {
    VolumeCommitCoverageView {
        object_namespace_id: coverage.object_namespace_id.to_string(),
        commit_id: coverage.commit_id.to_string(),
        storage_volume_id: coverage.storage_volume_id.to_string(),
        placement_generation: coverage.placement_generation.to_string(),
        object_set_digest: coverage.object_set_digest.to_string(),
        total_objects: coverage.object_count.to_string(),
        verified_objects: coverage.verified_object_count.to_string(),
        total_bytes: coverage.total_bytes.to_string(),
        verified_bytes: coverage.verified_bytes.to_string(),
        missing_objects: coverage
            .object_count
            .get()
            .saturating_sub(coverage.verified_object_count.get())
            .to_string(),
        missing_bytes: coverage
            .total_bytes
            .get()
            .saturating_sub(coverage.verified_bytes.get())
            .to_string(),
        state: coverage_state_name(coverage.state).to_owned(),
    }
}

fn materialization_issue(job: &MaterializationJob) -> Option<crate::dto::ResourceIssueSummary> {
    job.issue
        .as_ref()
        .map(|message| crate::dto::ResourceIssueSummary {
            code: "MATERIALIZATION_ISSUE".to_owned(),
            message: message.clone(),
            retryable: matches!(
                job.state,
                MaterializationJobState::Stalled | MaterializationJobState::WaitingForSources
            ),
            occurred_at_unix_ms: Some(job.updated_at_unix_ms.to_string()),
        })
}

fn materialization_view(
    job: &MaterializationJob,
    object_set_digest: ContentDigest,
) -> MaterializationView {
    MaterializationView {
        materialization_id: job.materialization_id.to_string(),
        tenant_id: job.key.tenant_id.to_string(),
        artifact_id: Some(job.artifact_id.to_string()),
        object_namespace_id: job.key.object_namespace_id.to_string(),
        commit_id: job.key.commit_id.to_string(),
        target_storage_volume_id: job.key.target_storage_volume_id.to_string(),
        plan_revision: job.plan_revision.to_string(),
        coverage_goal: job.key.coverage_goal,
        state: materialization_state_name(job.state).to_owned(),
        object_set_digest: object_set_digest.to_string(),
        verified_objects: job.verified_object_count.to_string(),
        total_objects: job.object_count.to_string(),
        verified_bytes: job.verified_bytes.to_string(),
        total_bytes: job.total_bytes.to_string(),
        missing_objects: job.missing_object_count.to_string(),
        missing_bytes: job.missing_bytes.to_string(),
        source_count: job.source_count.to_string(),
        issue: materialization_issue(job),
    }
}

/// Convert the legacy authority ObjectSet envelope into the namespace-scoped v2 contract.
/// Namespace binding is checked here at every service boundary; no v1 row is trusted implicitly.
pub(crate) fn namespace_object_set(
    tenant_id: &TenantId,
    namespace_id: &ObjectNamespaceId,
    commit_digest: ContentDigest,
    stored: &neoengram_domain::protocol::CommitObjectSet,
) -> Result<NamespaceObjectSet, Error> {
    if stored.tenant_id != *tenant_id || stored.commit_id.digest() != commit_digest {
        return Err(application_error(
            ErrorCategory::Conflict,
            "commit_object_set_mismatch",
            "COMMIT_OBJECT_SET_MISMATCH",
            "Commit authority and Placement authority disagree on the namespace ObjectSet",
            false,
        ));
    }
    NamespaceObjectSet::from_object_set(
        tenant_id.clone(),
        namespace_id.clone(),
        stored.commit_id,
        &stored.object_set,
    )
    .map_err(|error| invalid_request(format!("commit object set: {error}")))
}

fn parse_offset(value: Option<&str>) -> Result<usize, Error> {
    let Some(value) = value else { return Ok(0) };
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return Err(invalid_request(
            "cursor must be a canonical unsigned integer",
        ));
    }
    value
        .parse::<usize>()
        .map_err(|_| invalid_request("cursor must be a canonical unsigned integer"))
}

fn replication_id(request_id: &str) -> Result<ReplicationId, Error> {
    let digest = blake3::hash(format!("neoengram-replication\0{request_id}").as_bytes());
    ReplicationId::new(format!("replication-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("replication_id: {error}")))
}

fn workspace_id(request_id: &str) -> Result<WorkspaceId, Error> {
    let digest = blake3::hash(format!("neoengram-workspace\0{request_id}").as_bytes());
    WorkspaceId::new(format!("workspace-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("workspace_id: {error}")))
}

fn replication_state_name(state: ReplicationState) -> &'static str {
    match state {
        ReplicationState::Queued => "queued",
        ReplicationState::Planning => "planning",
        ReplicationState::Transferring => "transferring",
        ReplicationState::Verifying => "verifying",
        ReplicationState::Published => "published",
        ReplicationState::Failed => "failed",
        ReplicationState::Cancelled => "cancelled",
    }
}

fn workspace_lifecycle_name(state: WorkspaceLifecycle) -> &'static str {
    match state {
        WorkspaceLifecycle::Provisioning => "provisioning",
        WorkspaceLifecycle::Active => "active",
        WorkspaceLifecycle::Unavailable => "unavailable",
        WorkspaceLifecycle::Deleting => "deleting",
        WorkspaceLifecycle::Deleted => "deleted",
    }
}

fn idempotency_conflict(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::Conflict,
        "request_id_reused",
        "REQUEST_ID_REUSED",
        format!("{resource} request_id is already bound to another payload"),
        false,
    )
}

fn replication_view(record: &crate::ReplicationRecord) -> ReplicationView {
    ReplicationView {
        replication_id: record.replication_id.to_string(),
        tenant_id: record.tenant_id.to_string(),
        artifact_id: record.artifact_id.as_ref().map(ToString::to_string),
        commit_id: record.commit_id.to_string(),
        target_storage_volume_id: record.target_storage_volume_id.to_string(),
        attempt: record.attempt.to_string(),
        source_placement_set_id: record
            .source_placement_set_id
            .as_ref()
            .map(ToString::to_string),
        source_storage_volume_id: record
            .source_storage_volume_id
            .as_ref()
            .map(ToString::to_string),
        source_edge_cluster_id: record
            .source_edge_cluster_id
            .as_ref()
            .map(ToString::to_string),
        source_gateway_pool_id: record
            .source_gateway_pool_id
            .as_ref()
            .map(ToString::to_string),
        source_agent_id: record.source_agent_id.as_ref().map(ToString::to_string),
        source_session_generation: record
            .source_session_generation
            .map(|value| value.get().to_string()),
        source_mount_generation: record
            .source_mount_generation
            .map(|value| value.get().to_string()),
        source_route_generation: record
            .source_route_generation
            .map(|value| value.get().to_string()),
        target_edge_cluster_id: record
            .target_edge_cluster_id
            .as_ref()
            .map(ToString::to_string),
        target_gateway_pool_id: record
            .target_gateway_pool_id
            .as_ref()
            .map(ToString::to_string),
        target_agent_id: record.target_agent_id.as_ref().map(ToString::to_string),
        target_session_generation: record
            .target_session_generation
            .map(|value| value.get().to_string()),
        target_mount_generation: record
            .target_mount_generation
            .map(|value| value.get().to_string()),
        target_route_generation: record
            .target_route_generation
            .map(|value| value.get().to_string()),
        transfer_route_id: record.transfer_route_id.as_ref().map(ToString::to_string),
        transfer_id: record.transfer_id.as_ref().map(ToString::to_string),
        target_placement_set_id: record
            .target_placement_set_id
            .as_ref()
            .map(ToString::to_string),
        staging_id: record.staging_id.clone(),
        state: replication_state_name(record.state).to_owned(),
        object_set_digest: record.object_set_digest.to_string(),
        completed_objects: record.completed_objects.to_string(),
        total_objects: record.total_objects.to_string(),
        completed_bytes: record.completed_bytes.to_string(),
        total_bytes: record.total_bytes.to_string(),
        issue: record
            .issue_code
            .as_ref()
            .map(|code| crate::dto::ResourceIssueSummary {
                code: code.clone(),
                message: record.issue_message.clone().unwrap_or_default(),
                retryable: false,
                occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
            }),
    }
}

fn placement_state_name(
    state: neoengram_domain::protocol::CommitPlacementSetState,
) -> &'static str {
    match state {
        neoengram_domain::protocol::CommitPlacementSetState::Staged => "staged",
        neoengram_domain::protocol::CommitPlacementSetState::Published => "published",
        neoengram_domain::protocol::CommitPlacementSetState::Retiring => "retiring",
        neoengram_domain::protocol::CommitPlacementSetState::Deleted => "deleted",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadyReplicationRoute {
    pub(crate) edge_cluster_id: EdgeClusterId,
    pub(crate) gateway_pool_id: GatewayPoolId,
    pub(crate) agent_id: neoengram_domain::protocol::AgentId,
    /// Current owner fence for the Volume. Materialization targets must use this generation even
    /// when the Volume has no object placements yet; otherwise an old owner can be mistaken for a
    /// fresh empty target and accept receipts after an ownership change.
    pub(crate) placement_generation: PlacementGeneration,
    pub(crate) mount_generation: MountGeneration,
    pub(crate) session_generation: SessionGeneration,
    pub(crate) route_generation: RouteGeneration,
}

fn stored_replication_route_binding(
    record: &crate::ReplicationRecord,
    source: bool,
) -> Option<ReplicationRouteBinding> {
    let (
        edge_cluster_id,
        gateway_pool_id,
        agent_id,
        session_generation,
        mount_generation,
        route_generation,
    ) = if source {
        (
            record.source_edge_cluster_id.clone(),
            record.source_gateway_pool_id.clone(),
            record.source_agent_id.clone(),
            record.source_session_generation,
            record.source_mount_generation,
            record.source_route_generation,
        )
    } else {
        (
            record.target_edge_cluster_id.clone(),
            record.target_gateway_pool_id.clone(),
            record.target_agent_id.clone(),
            record.target_session_generation,
            record.target_mount_generation,
            record.target_route_generation,
        )
    };
    Some(ReplicationRouteBinding {
        edge_cluster_id: edge_cluster_id?,
        gateway_pool_id: gateway_pool_id?,
        agent_id: agent_id?,
        session_generation: session_generation?,
        mount_generation: mount_generation?,
        route_generation: route_generation?,
    })
}

fn replication_route_unavailable(role: &str) -> Error {
    application_error(
        ErrorCategory::Unavailable,
        "replication_route_unavailable",
        "REPLICATION_ROUTE_UNAVAILABLE",
        format!("{role} StorageVolume has no current Agent/Gateway route"),
        true,
    )
}

fn replication_prerequisites_unmet(role: &str) -> Error {
    application_error(
        ErrorCategory::Conflict,
        "replication_prerequisites_unmet",
        "REPLICATION_PREREQUISITES_UNMET",
        format!(
            "{role} StorageVolume Agent is ready for control traffic but has not passed the Commit replication QUIC preflight"
        ),
        false,
    )
}

fn ensure_replication_capacity(
    total_bytes: u64,
    reserve_bytes: u64,
    available_bytes: u64,
) -> Result<(), Error> {
    let required_bytes = total_bytes
        .checked_add(reserve_bytes)
        .ok_or_else(|| invalid_request("replication capacity requirement overflow"))?;
    if available_bytes < required_bytes {
        return Err(application_error(
            ErrorCategory::ResourceExhausted,
            "replication_capacity_insufficient",
            "REPLICATION_CAPACITY_INSUFFICIENT",
            format!(
                "target StorageVolume requires {required_bytes} available bytes including reserve, but reports {available_bytes}"
            ),
            true,
        ));
    }
    Ok(())
}

impl CatalogService {
    /// Builds the immutable, route-fenced capability for one replication attempt. Ticket
    /// issuance is deliberately separate from task creation so a reconnect can receive a fresh
    /// TTL without changing the frozen Commit, ObjectSet, source, target, or placement identity.
    async fn transfer_ticket_for_replication(
        &self,
        record: &crate::ReplicationRecord,
    ) -> Result<TransferTicket, Error> {
        let mut record = record.clone();
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let object_set = repository
            .get_commit_object_set(&record.tenant_id, &record.commit_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        if object_set.object_set.object_set_digest != record.object_set_digest {
            return Err(application_error(
                ErrorCategory::Conflict,
                "object_set_changed",
                "OBJECT_SET_CHANGED",
                "the frozen Commit ObjectSet no longer matches the replication",
                false,
            ));
        }
        let source_volume_id = record
            .source_storage_volume_id
            .clone()
            .ok_or_else(|| replication_route_unavailable("source"))?;
        let target_volume = self
            .repository
            .get_storage_volume(&record.tenant_id, &record.target_storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("storage volume"))?;
        let source_volume = self
            .repository
            .get_storage_volume(&record.tenant_id, &source_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("source storage volume"))?;
        let source_route = self
            .ready_replication_route(&record.tenant_id, &source_volume, "source")
            .await?;
        let target_route = self
            .ready_replication_route(&record.tenant_id, &target_volume, "target")
            .await?;
        let expected_source = stored_replication_route_binding(&record, true);
        let expected_target = stored_replication_route_binding(&record, false);
        let refreshed_source = ReplicationRouteBinding {
            edge_cluster_id: source_route.edge_cluster_id.clone(),
            gateway_pool_id: source_route.gateway_pool_id.clone(),
            agent_id: source_route.agent_id.clone(),
            session_generation: source_route.session_generation,
            mount_generation: source_route.mount_generation,
            route_generation: source_route.route_generation,
        };
        let refreshed_target = ReplicationRouteBinding {
            edge_cluster_id: target_route.edge_cluster_id.clone(),
            gateway_pool_id: target_route.gateway_pool_id.clone(),
            agent_id: target_route.agent_id.clone(),
            session_generation: target_route.session_generation,
            mount_generation: target_route.mount_generation,
            route_generation: target_route.route_generation,
        };
        // A live route may advance after a Gateway/Agent reconnect. Persist that rebind with a
        // two-sided CAS before issuing a ticket so later queries and channel delivery converge on
        // the same fence. Legacy rows missing route fields retain the existing read-only path.
        if let (Some(expected_source), Some(expected_target)) = (expected_source, expected_target) {
            if expected_source != refreshed_source || expected_target != refreshed_target {
                record = repository
                    .refresh_replication_routes(RefreshReplicationRoutesRequest {
                        tenant_id: record.tenant_id.clone(),
                        replication_id: record.replication_id.clone(),
                        expected_attempt: record.attempt,
                        expected_source,
                        expected_target,
                        source: refreshed_source,
                        target: refreshed_target,
                        updated_at_unix_ms: self.clock.now(),
                    })
                    .await
                    .map_err(map_central_error)?;
            }
        }
        if record
            .source_edge_cluster_id
            .as_ref()
            .is_some_and(|value| value != &source_route.edge_cluster_id)
            || record
                .source_gateway_pool_id
                .as_ref()
                .is_some_and(|value| value != &source_route.gateway_pool_id)
            || record
                .target_edge_cluster_id
                .as_ref()
                .is_some_and(|value| value != &target_route.edge_cluster_id)
            || record
                .target_gateway_pool_id
                .as_ref()
                .is_some_and(|value| value != &target_route.gateway_pool_id)
            || record
                .source_agent_id
                .as_ref()
                .is_some_and(|value| value != &source_route.agent_id)
            || record
                .source_session_generation
                .is_some_and(|value| value != source_route.session_generation)
            || record
                .source_mount_generation
                .is_some_and(|value| value != source_route.mount_generation)
            || record
                .source_route_generation
                .is_some_and(|value| value != source_route.route_generation)
            || record
                .target_agent_id
                .as_ref()
                .is_some_and(|value| value != &target_route.agent_id)
            || record
                .target_session_generation
                .is_some_and(|value| value != target_route.session_generation)
            || record
                .target_mount_generation
                .is_some_and(|value| value != target_route.mount_generation)
            || record
                .target_route_generation
                .is_some_and(|value| value != target_route.route_generation)
        {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "replication_route_changed",
                "REPLICATION_ROUTE_CHANGED",
                "the frozen replication route is no longer active",
                true,
            ));
        }
        let source_placement_id = PlacementId::new(
            record
                .source_placement_set_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("source-{}", record.replication_id)),
        )
        .map_err(|error| invalid_request(format!("source placement ID: {error}")))?;
        let target_placement_id = PlacementId::new(
            record
                .target_placement_set_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("target-{}", record.replication_id)),
        )
        .map_err(|error| invalid_request(format!("target placement ID: {error}")))?;
        let transfer_id = record
            .transfer_id
            .clone()
            .ok_or_else(|| invalid_request("replication transfer ID is missing"))?;
        let deadline = self
            .clock
            .now()
            .get()
            .checked_add(crate::service::DEFAULT_CENTRAL_COMMAND_TTL_MS)
            .ok_or_else(|| invalid_request("replication ticket deadline overflowed"))?;
        let mut allowed_objects = object_set
            .object_set
            .objects
            .iter()
            .map(|object| object.object_id)
            .collect::<Vec<_>>();
        allowed_objects.sort_unstable();
        let artifact_id = record
            .artifact_id
            .clone()
            .ok_or_else(|| invalid_request("replication artifact scope is missing"))?;
        Ok(TransferTicket {
            transfer_id,
            tenant_id: record.tenant_id.clone(),
            artifact_id,
            commit_id: CommitId::from_digest(record.commit_id),
            object_set_digest: record.object_set_digest,
            source: TransferEndpoint {
                placement_id: source_placement_id,
                agent_id: source_route.agent_id,
                gateway_pool_id: source_route.gateway_pool_id.clone(),
                edge_cluster_id: source_route.edge_cluster_id.clone(),
                storage_volume_id: Some(source_volume_id.clone()),
            },
            target: TransferEndpoint {
                placement_id: target_placement_id,
                agent_id: target_route.agent_id,
                gateway_pool_id: target_route.gateway_pool_id.clone(),
                edge_cluster_id: target_route.edge_cluster_id.clone(),
                storage_volume_id: Some(record.target_storage_volume_id.clone()),
            },
            source_session_generation: source_route.session_generation,
            source_mount_generation: source_route.mount_generation,
            source_route_generation: source_route.route_generation,
            session_generation: target_route.session_generation,
            mount_generation: target_route.mount_generation,
            route_generation: target_route.route_generation,
            deadline_unix_ms: UnixMillis::new(deadline),
            max_bytes: DecimalU64::new(record.total_bytes),
            allowed_objects,
        })
    }

    pub async fn query_commit_replication_ticket(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitReplicationTicketRequest,
    ) -> Result<QueryCommitReplicationTicketResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let replication_id = ReplicationId::new(request.replication_id)
            .map_err(|error| invalid_request(format!("replication_id: {error}")))?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let record = repository
            .get_replication(&tenant_id, &replication_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("replication"))?;
        if matches!(
            record.state,
            ReplicationState::Published | ReplicationState::Failed | ReplicationState::Cancelled
        ) {
            return Err(application_error(
                ErrorCategory::Conflict,
                "replication_not_active",
                "REPLICATION_NOT_ACTIVE",
                "a terminal replication cannot receive a transfer ticket",
                false,
            ));
        }
        let ticket = self.transfer_ticket_for_replication(&record).await?;
        let signed_ticket = match &self.replication_ticket_keyring {
            Some(keyring) => Some(
                keyring
                    .sign_transfer_ticket(
                        ticket.clone(),
                        self.clock.now(),
                        crate::service::DEFAULT_CENTRAL_COMMAND_TTL_MS,
                    )
                    .await
                    .map_err(|error| {
                        application_error(
                            ErrorCategory::Unavailable,
                            "replication_ticket_signing_unavailable",
                            "REPLICATION_TICKET_SIGNING_UNAVAILABLE",
                            error.to_string(),
                            true,
                        )
                    })?,
            ),
            None => None,
        };
        Ok(QueryCommitReplicationTicketResponse {
            ticket,
            signed_ticket,
        })
    }

    pub(crate) async fn ready_replication_route(
        &self,
        tenant_id: &TenantId,
        volume: &crate::StorageVolumeRecord,
        role: &str,
    ) -> Result<ReadyReplicationRoute, Error> {
        let route = self.ready_transfer_route(tenant_id, volume, role).await?;
        let agent_registry = self
            .agent_registry
            .as_ref()
            .ok_or_else(|| replication_prerequisites_unmet(role))?;
        if !agent_registry
            .current_ready_volume_supports_replication(tenant_id, &volume.storage_volume_id)
            .await
            .map_err(map_central_error)?
        {
            return Err(replication_prerequisites_unmet(role));
        }
        Ok(route)
    }

    /// Resolves the route, current Volume owner, mount/session fences, and Gateway readiness that
    /// both transfer protocols require. Protocol-specific capability checks belong in the v1/v2
    /// wrappers so a v2-only Agent is never rejected for not advertising the legacy capability.
    async fn ready_transfer_route(
        &self,
        tenant_id: &TenantId,
        volume: &crate::StorageVolumeRecord,
        role: &str,
    ) -> Result<ReadyReplicationRoute, Error> {
        let placement_provider = self
            .s3_placement
            .as_ref()
            .ok_or_else(|| replication_route_unavailable(role))?;
        let registry = self
            .gateway_registry
            .as_ref()
            .ok_or_else(|| replication_route_unavailable(role))?;
        let placement = placement_provider
            .current_placement(tenant_id, &volume.storage_volume_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| replication_route_unavailable(role))?;
        let route = registry
            .get_agent_route(&placement.agent_id)
            .await
            .map_err(map_central_error)?
            .filter(|route| {
                route.is_active_at(self.clock.now())
                    && route.session_generation == placement.session_generation
                    && route.edge_cluster_id == volume.edge_cluster_id
            })
            .ok_or_else(|| replication_route_unavailable(role))?;
        let pool = registry
            .get_pool(&route.gateway_pool_id)
            .await
            .map_err(map_central_error)?
            .filter(|pool| {
                pool.state == crate::GatewayPoolState::Ready
                    && pool.edge_cluster_id == volume.edge_cluster_id
            })
            .ok_or_else(|| replication_route_unavailable(role))?;

        // A Ready pool is not sufficient if every replica is stale, draining, or missing an
        // active certificate. Match the same heartbeat/certificate fence used by S3 routes.
        let now = self.clock.now();
        let _route_replica = registry
            .get_replica(&route.gateway_replica_id)
            .await
            .map_err(map_central_error)?
            .filter(|replica| {
                replica.gateway_pool_id == pool.gateway_pool_id
                    && replica.edge_cluster_id == pool.edge_cluster_id
                    && replica.state == crate::GatewayReplicaState::Active
                    && replica.credential.state == crate::GatewayCredentialState::Active
                    && replica.credential.certificate_generation.is_some()
                    && replica
                        .credential
                        .certificate_not_after_unix_ms
                        .is_some_and(|not_after| not_after.get() > now.get())
                    && replica.last_heartbeat_at_unix_ms.is_some_and(|heartbeat| {
                        heartbeat.get() <= now.get()
                            && now.get() - heartbeat.get() <= crate::AGENT_ROUTE_LEASE_MAX_TTL_MS
                    })
            })
            .ok_or_else(|| replication_route_unavailable(role))?;
        let mut ready_replicas = 0_usize;
        let mut after = None;
        loop {
            let replicas = registry
                .list_replicas(&crate::GatewayReplicaListRequest {
                    gateway_pool_id: pool.gateway_pool_id.clone(),
                    state: Some(crate::GatewayReplicaState::Active),
                    after: after.clone(),
                    limit: crate::GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                })
                .await
                .map_err(map_central_error)?;
            ready_replicas += replicas
                .iter()
                .filter(|replica| {
                    replica.gateway_pool_id == pool.gateway_pool_id
                        && replica.edge_cluster_id == pool.edge_cluster_id
                        && replica.state == crate::GatewayReplicaState::Active
                        && replica.credential.state == crate::GatewayCredentialState::Active
                        && replica.credential.certificate_generation.is_some()
                        && replica
                            .credential
                            .certificate_not_after_unix_ms
                            .is_some_and(|not_after| not_after.get() > now.get())
                        && replica.last_heartbeat_at_unix_ms.is_some_and(|heartbeat| {
                            heartbeat.get() <= now.get()
                                && now.get() - heartbeat.get()
                                    <= crate::AGENT_ROUTE_LEASE_MAX_TTL_MS
                        })
                })
                .count();
            if ready_replicas >= usize::from(pool.minimum_ready_replicas.max(1)) {
                return Ok(ReadyReplicationRoute {
                    edge_cluster_id: volume.edge_cluster_id.clone(),
                    gateway_pool_id: pool.gateway_pool_id,
                    agent_id: placement.agent_id,
                    placement_generation: PlacementGeneration::new(
                        placement.owner_generation.get(),
                    ),
                    mount_generation: placement.mount_generation,
                    session_generation: placement.session_generation,
                    route_generation: route.route_generation,
                });
            }
            if replicas.len() < crate::GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                return Err(replication_route_unavailable(role));
            }
            after = replicas
                .last()
                .map(|replica| replica.gateway_replica_id.clone());
        }
    }

    /// Returns a route that is eligible for the clean-slate v2 object materialization protocol.
    ///
    /// `ready_replication_route` intentionally remains available to the internal v1 recovery
    /// path while old assignments are being drained. A v2 planner must never treat that legacy
    /// capability as equivalent: otherwise an Agent that can only execute whole-Commit
    /// replication could receive a namespace-scoped Batch ticket. Reuse the common route,
    /// identity and certificate checks, then apply the stricter v2 capability gate here.
    pub(crate) async fn ready_materialization_route(
        &self,
        tenant_id: &TenantId,
        volume: &crate::StorageVolumeRecord,
        role: &str,
    ) -> Result<ReadyReplicationRoute, Error> {
        let route = self.ready_transfer_route(tenant_id, volume, role).await?;
        let agent_registry = self
            .agent_registry
            .as_ref()
            .ok_or_else(|| replication_prerequisites_unmet(role))?;
        if !agent_registry
            .current_ready_volume_supports_materialization(tenant_id, &volume.storage_volume_id)
            .await
            .map_err(map_central_error)?
        {
            return Err(replication_prerequisites_unmet(role));
        }
        Ok(route)
    }

    async fn source_placement_for_replication(
        &self,
        tenant_id: &TenantId,
        commit_id: &ContentDigest,
        object_set: &neoengram_domain::protocol::CommitObjectSet,
        target_volume_id: &StorageVolumeId,
        placement_repository: &dyn crate::PlacementRepository,
    ) -> Result<
        (
            CommitPlacementSet,
            crate::StorageVolumeRecord,
            ReadyReplicationRoute,
        ),
        Error,
    > {
        let candidates = placement_repository
            .published_placement_sets(tenant_id, commit_id)
            .await
            .map_err(map_central_error)?;
        for candidate in candidates {
            let Some(source_volume_id) = candidate.storage_volume_id.clone() else {
                continue;
            };
            if source_volume_id == *target_volume_id
                || candidate.object_set_digest != object_set.object_set.object_set_digest
                || candidate.object_count.get() != object_set.object_set.object_count() as u64
                || candidate.verified_object_count != candidate.object_count
                || candidate.state != neoengram_domain::protocol::CommitPlacementSetState::Published
            {
                continue;
            }
            let Some(source_volume) = self
                .repository
                .get_storage_volume(tenant_id, &source_volume_id)
                .await
                .map_err(map_central_error)?
            else {
                continue;
            };
            if !source_volume.lifecycle.is_active()
                || source_volume.state != crate::StorageVolumeState::Ready
            {
                continue;
            }
            if let Some(provider) = &self.storage_availability {
                if provider
                    .current_volume_state(tenant_id, &source_volume_id)
                    .await
                    .map_err(map_central_error)?
                    != crate::DerivedVolumeState::Ready
                {
                    continue;
                }
            }
            let Ok(route) = self
                .ready_replication_route(tenant_id, &source_volume, "source")
                .await
            else {
                continue;
            };
            let mut complete = true;
            for object in &object_set.object_set.objects {
                let placements = placement_repository
                    .object_placements(tenant_id, &object.object_id)
                    .await
                    .map_err(map_central_error)?;
                if !placements.iter().any(|placement| {
                    placement.backend_id == candidate.backend_id
                        && placement.storage_volume_id.as_ref() == Some(&source_volume_id)
                        && placement.archive_id == candidate.archive_id
                        && placement.placement_generation == candidate.placement_generation
                        && placement.state == PlacementState::Verified
                        && placement.verified_size == object.size
                        && placement.verified_digest == object.object_id.digest()
                        && placement
                            .edge_cluster_id
                            .as_ref()
                            .is_none_or(|cluster| cluster == &route.edge_cluster_id)
                        && placement
                            .gateway_pool_id
                            .as_ref()
                            .is_none_or(|pool| pool == &route.gateway_pool_id)
                }) {
                    complete = false;
                    break;
                }
            }
            if !complete {
                continue;
            }
            return Ok((candidate, source_volume, route));
        }
        Err(application_error(
            ErrorCategory::Unavailable,
            "source_unavailable",
            "SOURCE_UNAVAILABLE",
            "no complete verified source PlacementSet has a live transfer route",
            true,
        ))
    }
}

impl CatalogService {
    async fn require_replication_read(
        &self,
        identity: &AuthenticatedIdentity,
        tenant_id: &TenantId,
    ) -> Result<(), Error> {
        let permission = if self.policy.is_allowed(
            identity.principal(),
            Permission::ArtifactCommitReplicate,
            tenant_id,
        ) {
            Permission::ArtifactCommitReplicate
        } else {
            Permission::SnapshotRead
        };
        self.require_tenant(identity, permission, tenant_id).await
    }

    async fn materialization_view_for_job(
        &self,
        repository: &dyn crate::PlacementRepository,
        job: &MaterializationJob,
    ) -> Result<MaterializationView, Error> {
        let stored = repository
            .get_commit_object_set(&job.key.tenant_id, &job.key.commit_id.digest())
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        Ok(materialization_view(
            job,
            stored.object_set.object_set_digest,
        ))
    }

    pub async fn materialize_commit_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateCommitMaterializationRequest,
    ) -> Result<CreateCommitMaterializationResponse, Error> {
        // Keep this compatibility symbol pointed at the v2 aggregate publisher.  The old
        // implementation inserted the parent Job and Object rows in separate calls, which could
        // expose a partially planned materialization after a process crash.  All creation now
        // goes through `service::materialization::materialize_commit`, whose repository boundary
        // publishes Job, Batch, Object, Lease, and Coverage rows atomically.
        self.materialize_commit(identity, request).await
    }

    pub async fn query_commit_materialization_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitMaterializationRequest,
    ) -> Result<QueryCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let job = repository
            .get_materialization(&tenant_id, &namespace_id, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        Ok(QueryCommitMaterializationResponse {
            materialization: self
                .materialization_view_for_job(repository.as_ref(), &job)
                .await?,
        })
    }

    pub async fn query_commit_materialization_list_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitMaterializationListRequest,
    ) -> Result<QueryCommitMaterializationListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume_id = request
            .target_storage_volume_id
            .map(parse_volume)
            .transpose()?;
        let offset = parse_offset(request.cursor.as_deref())?;
        let limit = usize::from(request.page_size.unwrap_or(50));
        if !(1..=100).contains(&limit) {
            return Err(invalid_request("page_size must be between 1 and 100"));
        }
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let jobs = repository
            .list_materializations(
                &tenant_id,
                &namespace_id,
                &commit_digest,
                target_volume_id.as_ref(),
            )
            .await
            .map_err(map_central_error)?;
        let mut jobs = jobs;
        jobs.sort_by_key(|job| job.materialization_id.clone());
        let page = jobs
            .into_iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        let items = page
            .iter()
            .take(limit)
            .map(|job| materialization_view(job, stored.object_set.object_set_digest))
            .collect();
        let next_cursor = has_more.then(|| offset.saturating_add(limit).to_string());
        Ok(QueryCommitMaterializationListResponse {
            materializations: items,
            next_cursor,
        })
    }

    pub async fn retry_commit_materialization_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: RetryCommitMaterializationRequest,
    ) -> Result<RetryCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let expected_plan_revision =
            parse_generation(request.expected_plan_revision, "expected_plan_revision")?;
        let _request_id = RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let current = repository
            .get_materialization(&tenant_id, &namespace_id, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        if current.plan_revision != expected_plan_revision {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_plan_revision_conflict",
                "MATERIALIZATION_PLAN_REVISION_CONFLICT",
                "materialization plan revision changed",
                false,
            ));
        }
        if current.state == MaterializationJobState::Complete {
            return Ok(RetryCommitMaterializationResponse {
                materialization: self
                    .materialization_view_for_job(repository.as_ref(), &current)
                    .await?,
                replayed: true,
            });
        }
        let next_revision = expected_plan_revision
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid_request("materialization plan revision overflow"))?;
        let mut next = current.clone();
        next.plan_revision = Generation::new(next_revision);
        next.state = MaterializationJobState::Queued;
        next.issue = None;
        next.updated_at_unix_ms = self.clock.now();
        let stored = repository
            .replace_materialization(
                &tenant_id,
                &materialization_id,
                expected_plan_revision,
                next,
            )
            .await
            .map_err(map_central_error)?;
        Ok(RetryCommitMaterializationResponse {
            materialization: self
                .materialization_view_for_job(repository.as_ref(), &stored)
                .await?,
            replayed: false,
        })
    }

    pub async fn cancel_commit_materialization_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelCommitMaterializationRequest,
    ) -> Result<CancelCommitMaterializationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let materialization_id = parse_materialization_id(request.materialization_id)?;
        let expected_plan_revision =
            parse_generation(request.expected_plan_revision, "expected_plan_revision")?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let current = repository
            .get_materialization(&tenant_id, &namespace_id, &materialization_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("materialization"))?;
        if current.plan_revision != expected_plan_revision {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_plan_revision_conflict",
                "MATERIALIZATION_PLAN_REVISION_CONFLICT",
                "materialization plan revision changed",
                false,
            ));
        }
        if current.state == MaterializationJobState::Cancelled {
            return Ok(CancelCommitMaterializationResponse {
                materialization: self
                    .materialization_view_for_job(repository.as_ref(), &current)
                    .await?,
            });
        }
        if current.state == MaterializationJobState::Complete {
            return Err(application_error(
                ErrorCategory::Conflict,
                "materialization_already_complete",
                "MATERIALIZATION_ALREADY_COMPLETE",
                "a complete materialization cannot be cancelled",
                false,
            ));
        }
        let next_revision = expected_plan_revision
            .get()
            .checked_add(1)
            .ok_or_else(|| invalid_request("materialization plan revision overflow"))?;
        let mut next = current;
        next.plan_revision = Generation::new(next_revision);
        next.state = MaterializationJobState::Cancelled;
        next.updated_at_unix_ms = self.clock.now();
        let stored = repository
            .replace_materialization(
                &tenant_id,
                &materialization_id,
                expected_plan_revision,
                next,
            )
            .await
            .map_err(map_central_error)?;
        Ok(CancelCommitMaterializationResponse {
            materialization: self
                .materialization_view_for_job(repository.as_ref(), &stored)
                .await?,
        })
    }

    pub async fn query_commit_coverage_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitCoverageRequest,
    ) -> Result<QueryCommitCoverageResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume_id = request.storage_volume_id.map(parse_volume).transpose()?;
        let offset = parse_offset(request.cursor.as_deref())?;
        let limit = usize::from(request.page_size.unwrap_or(50));
        if !(1..=100).contains(&limit) {
            return Err(invalid_request("page_size must be between 1 and 100"));
        }
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let namespace_set =
            namespace_object_set(&tenant_id, &namespace_id, commit_digest, &stored)?;
        let mut coverages = repository
            .volume_commit_coverages(&tenant_id, &namespace_id, &commit_digest)
            .await
            .map_err(map_central_error)?;

        // Older rows may not have a materialized summary yet.  Derive missing summaries from the
        // v2 object evidence so this read path never treats an aggregate counter as authoritative.
        let mut grouped =
            BTreeMap::<(StorageVolumeId, PlacementGeneration), Vec<ObjectPlacementV2>>::new();
        for object in &namespace_set.objects {
            let placements = repository
                .object_placements_v2(&tenant_id, &namespace_id, &object.object_id)
                .await
                .map_err(map_central_error)?;
            for placement in placements.into_iter().filter(|placement| {
                placement.state == ObjectPlacementState::Verified
                    && placement.tenant_id == tenant_id
                    && placement.object_namespace_id == namespace_id
                    && placement.matches_ref(object)
                    && placement.storage_volume_id.is_some()
            }) {
                let volume = placement
                    .storage_volume_id
                    .clone()
                    .expect("filtered v2 placement must have a volume");
                grouped
                    .entry((volume, placement.placement_generation))
                    .or_default()
                    .push(placement);
            }
        }
        let existing_keys = coverages
            .iter()
            .map(|coverage| {
                (
                    coverage.storage_volume_id.clone(),
                    coverage.placement_generation,
                )
            })
            .collect::<BTreeSet<_>>();
        for ((volume, generation), placements) in grouped {
            if existing_keys.contains(&(volume.clone(), generation)) {
                continue;
            }
            let coverage = VolumeCommitCoverage::from_placements(
                tenant_id.clone(),
                namespace_id.clone(),
                namespace_set.commit_id,
                volume,
                generation,
                &stored.object_set,
                &placements,
            )
            .map_err(|error| invalid_request(format!("coverage: {error}")))?;
            coverages.push(coverage);
        }
        coverages.sort_by_key(|coverage| {
            (
                coverage.storage_volume_id.clone(),
                coverage.placement_generation,
            )
        });
        if let Some(target) = &target_volume_id {
            coverages.retain(|coverage| &coverage.storage_volume_id == target);
        }
        let page = coverages
            .into_iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        let coverage = page.iter().take(limit).map(coverage_view).collect();
        Ok(QueryCommitCoverageResponse {
            coverage,
            next_cursor: has_more.then(|| offset.saturating_add(limit).to_string()),
        })
    }

    pub async fn query_commit_availability_legacy_v2(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitAvailabilityV2Request,
    ) -> Result<QueryCommitAvailabilityV2Response, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let namespace_id = parse_namespace(request.object_namespace_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume_id = request
            .target_storage_volume_id
            .map(parse_volume)
            .transpose()?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let stored = repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let namespace_set =
            namespace_object_set(&tenant_id, &namespace_id, commit_digest, &stored)?;
        let total = namespace_set.objects.len();
        let mut content_present = 0_usize;
        let mut target_present = 0_usize;
        let mut missing_objects = Vec::new();
        let mut verified_volume_ids = BTreeSet::new();
        let mut volume_generation_objects =
            BTreeMap::<(StorageVolumeId, PlacementGeneration), BTreeSet<ObjectId>>::new();

        for object in &namespace_set.objects {
            let placements = repository
                .object_placements_v2(&tenant_id, &namespace_id, &object.object_id)
                .await
                .map_err(map_central_error)?;
            let matching = placements
                .iter()
                .filter(|placement| {
                    placement.state == ObjectPlacementState::Verified
                        && placement.tenant_id == tenant_id
                        && placement.object_namespace_id == namespace_id
                        && placement.matches_ref(object)
                })
                .collect::<Vec<_>>();
            if matching.is_empty() {
                if target_volume_id.is_none() {
                    missing_objects.push(MissingObjectView {
                        object_id: object.object_id.to_string(),
                        size: object.size.to_string(),
                        encoding: object_encoding_name(object.encoding).to_owned(),
                    });
                }
            } else {
                content_present += 1;
            }
            let mut target_match = false;
            for placement in matching {
                let Some(volume) = placement.storage_volume_id.clone() else {
                    continue;
                };
                verified_volume_ids.insert(volume.to_string());
                volume_generation_objects
                    .entry((volume.clone(), placement.placement_generation))
                    .or_default()
                    .insert(placement.object_id);
                if target_volume_id.as_ref() == Some(&volume) {
                    target_match = true;
                }
            }
            if target_volume_id.is_some() && !target_match {
                missing_objects.push(MissingObjectView {
                    object_id: object.object_id.to_string(),
                    size: object.size.to_string(),
                    encoding: object_encoding_name(object.encoding).to_owned(),
                });
            }
            if target_match {
                target_present += 1;
            }
        }

        let required_ids = namespace_set
            .objects
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        let complete_volume_count = volume_generation_objects
            .iter()
            .filter(|((volume, _generation), objects)| {
                target_volume_id
                    .as_ref()
                    .is_none_or(|target| *target == *volume)
                    && objects.len() == required_ids.len()
                    && objects.is_superset(&required_ids)
            })
            .map(|((volume, _), _)| volume)
            .collect::<BTreeSet<_>>()
            .len();
        let target_coverage = if target_volume_id.is_some() {
            if target_present == total {
                CoverageState::Complete
            } else {
                CoverageState::Partial
            }
        } else if complete_volume_count > 0 {
            CoverageState::Complete
        } else {
            CoverageState::Partial
        };
        let content_status = availability_status_for_counts(total, content_present);
        let view_readiness = if target_volume_id.is_some() && target_coverage.complete() {
            ViewReadiness::Ready
        } else {
            ViewReadiness::NotReady
        };
        let availability = CommitAvailabilityV2View {
            object_namespace_id: namespace_id.to_string(),
            commit_id: commit_digest.to_string(),
            object_count: total.to_string(),
            content_presence: availability_status_name(content_status).to_owned(),
            source_serving: availability_status_name(content_status).to_owned(),
            durability: availability_status_name(content_status).to_owned(),
            target_coverage: coverage_state_name(target_coverage).to_owned(),
            view_readiness: view_readiness_name(view_readiness).to_owned(),
            complete_volume_count: complete_volume_count.to_string(),
            missing_objects,
            verified_storage_volume_ids: verified_volume_ids.into_iter().collect(),
        };
        Ok(QueryCommitAvailabilityV2Response { availability })
    }

    pub async fn create_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateCommitReplicationRequest,
    ) -> Result<CreateCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let project_id = ProjectId::new(request.project_id)
            .map_err(|error| invalid_request(format!("project_id: {error}")))?;
        let artifact_id = parse_artifact(request.artifact_id)?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume = parse_volume(request.target_storage_volume_id)?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let replication_id = replication_id(&request.request_id)?;
        let Some(placement_repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };
        let precommits = self.precommits.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "precommit_unavailable",
                "PRECOMMIT_UNAVAILABLE",
                "Commit authority is not configured",
                true,
            )
        })?;
        let commit = precommits
            .get_commit(
                &tenant_id,
                &project_id,
                &artifact_id,
                CommitId::from_digest(commit_digest),
            )
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit"))?;
        let object_set = placement_repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        if commit.object_set_digest != object_set.object_set.object_set_digest {
            return Err(application_error(
                ErrorCategory::Conflict,
                "commit_object_set_mismatch",
                "COMMIT_OBJECT_SET_MISMATCH",
                "Commit authority and Placement authority disagree on the ObjectSet digest",
                false,
            ));
        }
        let total_bytes = object_set
            .object_set
            .total_bytes()
            .map_err(|error| invalid_request(format!("commit object total size: {error}")))?;

        // Resolve the idempotency key before checking mutable target-volume state. A retry must
        // return the original authority row even when the target has since gone offline.
        if let Some(existing) = placement_repository
            .get_replication_by_request_id(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.commit_id != commit_digest
                || existing.artifact_id.as_ref() != Some(&artifact_id)
                || existing.target_storage_volume_id != target_volume
            {
                return Err(idempotency_conflict("replication"));
            }
            return Ok(CreateCommitReplicationResponse {
                replication: replication_view(&existing),
                replayed: true,
            });
        }
        if placement_repository
            .list_replications_for_commit(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .iter()
            .any(|replication| {
                replication.target_storage_volume_id == target_volume
                    && matches!(
                        replication.state,
                        ReplicationState::Queued
                            | ReplicationState::Planning
                            | ReplicationState::Transferring
                            | ReplicationState::Verifying
                    )
            })
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "replication_already_active",
                "REPLICATION_ALREADY_ACTIVE",
                "an active replication already targets this Commit and StorageVolume",
                false,
            ));
        }

        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target_volume)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("storage volume"))?;
        // A replication target must be writable and fully ready. A degraded Volume may still
        // serve existing reads, but accepting it as a destination would publish a copy onto an
        // already unhealthy failure domain and make the resulting PlacementSet misleading.
        if !volume.lifecycle.is_active() || volume.state != crate::StorageVolumeState::Ready {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "target StorageVolume is not ready",
                true,
            ));
        }
        if matches!(volume.access_mode, crate::StorageAccessMode::ReadOnlyMany) {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_writable",
                "STORAGE_VOLUME_NOT_WRITABLE",
                "target StorageVolume is read-only",
                false,
            ));
        }
        let target_route = self
            .ready_replication_route(&tenant_id, &volume, "target")
            .await?;
        let capacity = self.storage_availability.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "replication_capacity_unavailable",
                "REPLICATION_CAPACITY_UNAVAILABLE",
                "target StorageVolume capacity authority is not configured",
                true,
            )
        })?;
        let available_bytes = capacity
            .current_available_bytes(&tenant_id, &target_volume)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| {
                application_error(
                    ErrorCategory::Unavailable,
                    "replication_capacity_unavailable",
                    "REPLICATION_CAPACITY_UNAVAILABLE",
                    "target StorageVolume has no current free-space observation",
                    true,
                )
            })?;
        ensure_replication_capacity(
            total_bytes,
            volume.copy_reserve_bytes.get(),
            available_bytes,
        )?;
        let availability = placement_repository
            .commit_availability(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?;
        if matches!(
            availability.data_health,
            neoengram_domain::protocol::DataHealth::Unavailable
        ) {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "source_unavailable",
                "SOURCE_UNAVAILABLE",
                "no verified PlacementSet can provide every Commit object",
                true,
            ));
        }
        let target_backend_id = BackendId::new(target_volume.to_string())
            .map_err(|error| invalid_request(format!("target backend_id: {error}")))?;
        if placement_repository
            .get_placement_set(&tenant_id, &commit_digest, &target_backend_id)
            .await
            .map_err(map_central_error)?
            .is_some()
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "target_commit_placement_exists",
                "TARGET_COMMIT_PLACEMENT_EXISTS",
                "target StorageVolume already has a Commit PlacementSet; retire it before creating another copy",
                false,
            ));
        }
        let (source_placement, _source_volume, source_route) = self
            .source_placement_for_replication(
                &tenant_id,
                &commit_digest,
                &object_set,
                &target_volume,
                placement_repository.as_ref(),
            )
            .await?;
        let target_placement_generation = PlacementGeneration::new(1);
        // These identities are frozen with the request. A resumed Ticket may receive a new TTL,
        // but it must always refer to the same route, transfer, staging root, and target fence.
        let transfer_id = TransferId::new(format!("transfer-{replication_id}"))
            .map_err(|error| invalid_request(format!("transfer_id: {error}")))?;
        let transfer_route_id = TransferRouteId::new(format!("route-{replication_id}"))
            .map_err(|error| invalid_request(format!("transfer_route_id: {error}")))?;
        let target_placement_set_id =
            PlacementSetId::new(format!("placement-set-{replication_id}"))
                .map_err(|error| invalid_request(format!("target_placement_set_id: {error}")))?;
        let staging_id = format!("staging-{replication_id}");
        let object_set_digest = object_set.object_set.object_set_digest;
        let total_objects = u64::try_from(object_set.object_set.objects.len())
            .map_err(|_| invalid_request("commit object count exceeds supported range"))?;
        let now = self.clock.now();
        let record = crate::ReplicationRecord {
            tenant_id: tenant_id.clone(),
            replication_id,
            artifact_id: Some(artifact_id),
            commit_id: commit_digest,
            target_backend_id: target_backend_id.to_string(),
            target_storage_volume_id: target_volume,
            source_placement_set_id: Some(source_placement.placement_set_id),
            source_backend_id: Some(source_placement.backend_id),
            source_storage_volume_id: source_placement.storage_volume_id,
            source_edge_cluster_id: Some(source_route.edge_cluster_id),
            source_gateway_pool_id: Some(source_route.gateway_pool_id),
            source_placement_generation: Some(source_placement.placement_generation),
            source_agent_id: Some(source_route.agent_id.clone()),
            source_session_generation: Some(source_route.session_generation),
            source_mount_generation: Some(source_route.mount_generation),
            source_route_generation: Some(source_route.route_generation),
            target_edge_cluster_id: Some(target_route.edge_cluster_id),
            target_gateway_pool_id: Some(target_route.gateway_pool_id),
            target_placement_generation: Some(target_placement_generation),
            target_agent_id: Some(target_route.agent_id.clone()),
            target_session_generation: Some(target_route.session_generation),
            target_mount_generation: Some(target_route.mount_generation),
            target_route_generation: Some(target_route.route_generation),
            transfer_route_id: Some(transfer_route_id),
            transfer_id: Some(transfer_id),
            target_placement_set_id: Some(target_placement_set_id),
            staging_id: Some(staging_id),
            object_set_digest,
            state: ReplicationState::Queued,
            request_id,
            attempt: 1,
            completed_objects: 0,
            total_objects,
            completed_bytes: 0,
            total_bytes,
            issue_code: None,
            issue_message: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let stored = placement_repository
            .insert_replication(record.clone())
            .await
            .map_err(map_central_error)?;
        let replayed = stored != record;
        let record = stored;
        Ok(CreateCommitReplicationResponse {
            replication: replication_view(&record),
            replayed,
        })
    }

    pub async fn query_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitReplicationRequest,
    ) -> Result<QueryCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let replication_id = ReplicationId::new(request.replication_id)
            .map_err(|error| invalid_request(format!("replication_id: {error}")))?;
        let Some(repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };
        let record = repository
            .get_replication(&tenant_id, &replication_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("replication"))?;
        Ok(QueryCommitReplicationResponse {
            replication: replication_view(&record),
        })
    }

    pub async fn query_commit_replication_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitReplicationListRequest,
    ) -> Result<QueryCommitReplicationListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let commit_id = parse_commit(request.commit_id)?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let records = repository
            .list_replications_for_commit(&tenant_id, &commit_id)
            .await
            .map_err(map_central_error)?;
        Ok(QueryCommitReplicationListResponse {
            replications: records.iter().map(replication_view).collect(),
        })
    }

    pub async fn query_commit_placement_list(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitPlacementListRequest,
    ) -> Result<QueryCommitPlacementListResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let commit_id = parse_commit(request.commit_id)?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let placements = repository
            .commit_placement_sets(&tenant_id, &commit_id)
            .await
            .map_err(map_central_error)?
            .into_iter()
            .map(|placement| CommitPlacementView {
                placement_set_id: placement.placement_set_id.to_string(),
                commit_id: placement.commit_id.to_string(),
                backend_id: placement.backend_id.to_string(),
                storage_volume_id: placement.storage_volume_id.map(|id| id.to_string()),
                object_set_digest: placement.object_set_digest.to_string(),
                object_count: placement.object_count.to_string(),
                verified_object_count: placement.verified_object_count.to_string(),
                placement_generation: placement.placement_generation.to_string(),
                state: placement_state_name(placement.state).to_owned(),
            })
            .collect();
        Ok(QueryCommitPlacementListResponse { placements })
    }

    pub async fn retry_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: RetryCommitReplicationRequest,
    ) -> Result<RetryCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let replication_id = ReplicationId::new(request.replication_id)
            .map_err(|error| invalid_request(format!("replication_id: {error}")))?;
        let expected_attempt = parse_attempt(request.expected_attempt)?;
        let request_id = RequestId::new(request.request_id)
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let record = repository
            .retry_replication(crate::RetryReplicationRequest {
                tenant_id,
                replication_id,
                expected_attempt,
                request_id,
                updated_at_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_central_error)?;
        Ok(RetryCommitReplicationResponse {
            replication: replication_view(&record.replication),
            replayed: record.replayed,
        })
    }

    pub async fn cancel_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: CancelCommitReplicationRequest,
    ) -> Result<CancelCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::ArtifactCommitReplicate, &tenant_id)
            .await?;
        let replication_id = ReplicationId::new(request.replication_id)
            .map_err(|error| invalid_request(format!("replication_id: {error}")))?;
        let expected_attempt = parse_attempt(request.expected_attempt)?;
        let repository = self.placement.as_ref().ok_or_else(|| {
            application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            )
        })?;
        let record = repository
            .cancel_replication(crate::CancelReplicationRequest {
                tenant_id,
                replication_id,
                expected_attempt,
                updated_at_unix_ms: self.clock.now(),
            })
            .await
            .map_err(map_central_error)?;
        Ok(CancelCommitReplicationResponse {
            replication: replication_view(&record),
        })
    }

    pub async fn query_commit_availability(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitAvailabilityRequest,
    ) -> Result<QueryCommitAvailabilityResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_replication_read(identity, &tenant_id).await?;
        let commit_id = parse_commit(request.commit_id)?;
        let availability = if let Some(repository) = &self.placement {
            repository
                .commit_availability(&tenant_id, &commit_id)
                .await
                .map_err(map_central_error)?
        } else {
            crate::CommitAvailabilityRecord {
                tenant_id: tenant_id.clone(),
                commit_id,
                data_health: neoengram_domain::protocol::DataHealth::Unavailable,
                verified_placements: 0,
                missing_objects: 0,
                verified_storage_volume_ids: Vec::new(),
            }
        };
        Ok(QueryCommitAvailabilityResponse {
            availability: CommitAvailabilityView {
                commit_id: availability.commit_id.to_string(),
                data_health: format!("{:?}", availability.data_health).to_lowercase(),
                verified_placements: availability.verified_placements.to_string(),
                missing_objects: availability.missing_objects.to_string(),
                verified_storage_volume_ids: availability
                    .verified_storage_volume_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
        })
    }

    pub async fn create_workspace(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateWorkspaceRequest,
    ) -> Result<CreateWorkspaceResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let project_id = ProjectId::new(request.project_id)
            .map_err(|error| invalid_request(format!("project_id: {error}")))?;
        let artifact_id = ArtifactId::new(request.artifact_id)
            .map_err(|error| invalid_request(format!("artifact_id: {error}")))?;
        let target = parse_volume(request.target_storage_volume_id)?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let Some(placement_repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };
        if let Some(existing) = placement_repository
            .get_workspace_by_request_id(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            let base_commit_id = request
                .base_commit_id
                .as_deref()
                .map(|value| parse_commit(value.to_owned()))
                .transpose()?;
            if existing.project_id != project_id
                || existing.artifact_id != artifact_id
                || existing.base_commit_id != base_commit_id
                || existing.target_storage_volume_id != target
            {
                return Err(idempotency_conflict("workspace"));
            }
            return Ok(CreateWorkspaceResponse {
                workspace: WorkspaceView {
                    workspace_id: existing.workspace_id.to_string(),
                    tenant_id: existing.tenant_id.to_string(),
                    project_id: existing.project_id.to_string(),
                    artifact_id: existing.artifact_id.to_string(),
                    base_commit_id: existing.base_commit_id.map(|value| value.to_string()),
                    target_storage_volume_id: existing.target_storage_volume_id.to_string(),
                    lifecycle: workspace_lifecycle_name(existing.lifecycle).to_owned(),
                },
                replayed: true,
            });
        }
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("storage volume"))?;
        if !volume.lifecycle.is_active()
            || !matches!(volume.state, crate::StorageVolumeState::Ready)
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "target StorageVolume is not ready",
                true,
            ));
        }
        let id = workspace_id(&request.request_id)?;
        let base_commit_id = request
            .base_commit_id
            .as_deref()
            .map(|value| parse_commit(value.to_owned()))
            .transpose()?;
        if let Some(commit_id) = base_commit_id {
            // Workspace hydration is an object-level v2 read.  Legacy complete PlacementSets
            // are deliberately ignored: a Commit is usable only when every required object has
            // a matching verified ObjectPlacement in its Artifact namespace.
            let namespace = ObjectNamespaceId::new(artifact_id.to_string())
                .map_err(|error| invalid_request(format!("object namespace: {error}")))?;
            let data_health = self
                .v2_commit_data_health_for_digest(&tenant_id, &namespace, &commit_id)
                .await?;
            if data_health.is_none_or(|health| {
                matches!(health, neoengram_domain::protocol::DataHealth::Unavailable)
            }) {
                return Err(application_error(
                    ErrorCategory::Unavailable,
                    "data_unavailable",
                    "DATA_UNAVAILABLE",
                    "the requested base Commit has no readable verified v2 ObjectPlacement",
                    true,
                ));
            }
        }
        let now = self.clock.now();
        let workspace = crate::WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            workspace_id: id,
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            base_commit_id,
            target_storage_volume_id: target.clone(),
            request_id,
            lifecycle: WorkspaceLifecycle::Provisioning,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let stored = placement_repository
            .insert_workspace(workspace.clone())
            .await
            .map_err(map_central_error)?;
        let replayed = stored != workspace;
        let workspace = stored;
        Ok(CreateWorkspaceResponse {
            workspace: WorkspaceView {
                workspace_id: workspace.workspace_id.to_string(),
                tenant_id: workspace.tenant_id.to_string(),
                project_id: workspace.project_id.to_string(),
                artifact_id: workspace.artifact_id.to_string(),
                base_commit_id: workspace.base_commit_id.map(|value| value.to_string()),
                target_storage_volume_id: workspace.target_storage_volume_id.to_string(),
                lifecycle: workspace_lifecycle_name(workspace.lifecycle).to_owned(),
            },
            replayed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_replication_capacity;

    #[test]
    fn replication_capacity_includes_staging_reserve() {
        assert!(ensure_replication_capacity(80, 20, 100).is_ok());
        let error = ensure_replication_capacity(80, 21, 100).unwrap_err();
        assert_eq!(error.code().as_str(), "replication_capacity_insufficient");
    }

    #[test]
    fn replication_capacity_rejects_overflow() {
        let error = ensure_replication_capacity(u64::MAX, 1, u64::MAX).unwrap_err();
        assert_eq!(error.code().as_str(), "protocol_invalid");
    }
}
