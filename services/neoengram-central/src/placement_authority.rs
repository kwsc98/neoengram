use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    AgentId, ArtifactId, BackendId, CommitObjectSet, CommitPlacementSet, CommitPlacementSetState,
    DataHealth, EdgeClusterId, GatewayPoolId, MountGeneration, ObjectPlacement,
    PlacementGeneration, PlacementSetId, PlacementState, ReplicationId, ReplicationObjectState,
    ReplicationState, RequestId, RouteGeneration, SessionGeneration, StorageVolumeId, TenantId,
    TransferId, TransferRouteId, UnixMillis, WorkspaceId, WorkspaceLifecycle,
};
use serde::{Deserialize, Serialize};

use crate::{CentralError, CentralErrorCode, CentralResult};

/// Durable control-plane identity for one explicit Commit replication request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationRecord {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    /// Artifact namespace for the physical Volume CAS. Legacy rows may omit this value; such
    /// records are deliberately rejected when Central builds a data-plane assignment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    pub commit_id: ContentDigest,
    pub target_backend_id: String,
    pub target_storage_volume_id: StorageVolumeId,
    /// The complete source PlacementSet selected when the transfer was planned. These fields are
    /// optional for records created before route-aware replication was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_placement_set_id: Option<PlacementSetId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_backend_id: Option<BackendId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_storage_volume_id: Option<StorageVolumeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_edge_cluster_id: Option<EdgeClusterId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_gateway_pool_id: Option<GatewayPoolId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_placement_generation: Option<PlacementGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_session_generation: Option<SessionGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_mount_generation: Option<MountGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_route_generation: Option<RouteGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_edge_cluster_id: Option<EdgeClusterId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_gateway_pool_id: Option<GatewayPoolId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_placement_generation: Option<PlacementGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session_generation: Option<SessionGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_mount_generation: Option<MountGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_route_generation: Option<RouteGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_route_id: Option<TransferRouteId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<TransferId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_placement_set_id: Option<PlacementSetId>,
    /// The staged target root is an opaque Agent-owned identity, never a physical path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_id: Option<String>,
    pub object_set_digest: ContentDigest,
    pub state: ReplicationState,
    pub request_id: RequestId,
    /// Monotonic execution attempt. Retrying a failed transfer increments this value and fences
    /// reports or finalization from the previous attempt.
    #[serde(default = "initial_replication_attempt")]
    pub attempt: u64,
    pub completed_objects: u64,
    pub total_objects: u64,
    #[serde(default)]
    pub completed_bytes: u64,
    #[serde(default)]
    pub total_bytes: u64,
    pub issue_code: Option<String>,
    pub issue_message: Option<String>,
    pub created_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    pub updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
}

const fn initial_replication_attempt() -> u64 {
    1
}

/// Compare-and-swap request for a non-terminal Replication state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationStateTransitionRequest {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub expected_state: ReplicationState,
    pub expected_attempt: u64,
    pub next_state: ReplicationState,
    pub completed_objects: u64,
    pub completed_bytes: u64,
    pub issue_code: Option<String>,
    pub issue_message: Option<String>,
    pub updated_at_unix_ms: UnixMillis,
}

/// Compare-and-swap request that starts a fresh attempt without discarding durable checkpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryReplicationRequest {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub expected_attempt: u64,
    pub updated_at_unix_ms: UnixMillis,
}

/// Idempotent cancellation request for one active Replication attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelReplicationRequest {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub expected_attempt: u64,
    pub updated_at_unix_ms: UnixMillis,
}

/// Atomic publication request after the target Agent has verified and durably committed every
/// object. No target Placement is externally readable until this request commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeReplicationRequest {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub expected_attempt: u64,
    pub placements: Vec<ObjectPlacement>,
    pub placement_set: CommitPlacementSet,
    pub finalized_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeReplicationResult {
    pub replication: ReplicationRecord,
    pub placement_set: CommitPlacementSet,
    pub replayed: bool,
}

#[must_use]
pub(crate) const fn valid_replication_transition(
    current: ReplicationState,
    next: ReplicationState,
) -> bool {
    matches!(
        (current, next),
        (ReplicationState::Queued, ReplicationState::Queued)
            | (ReplicationState::Planning, ReplicationState::Planning)
            | (
                ReplicationState::Transferring,
                ReplicationState::Transferring
            )
            | (ReplicationState::Verifying, ReplicationState::Verifying)
            | (ReplicationState::Queued, ReplicationState::Planning)
            | (ReplicationState::Planning, ReplicationState::Transferring)
            | (ReplicationState::Transferring, ReplicationState::Verifying)
            | (
                ReplicationState::Queued
                    | ReplicationState::Planning
                    | ReplicationState::Transferring
                    | ReplicationState::Verifying,
                ReplicationState::Failed | ReplicationState::Cancelled
            )
    )
}

pub(crate) fn validate_replication_record(record: &ReplicationRecord) -> CentralResult<()> {
    if record.attempt == 0 {
        return Err(replication_invalid(
            "replication attempt must be greater than zero",
        ));
    }
    if record.completed_objects > record.total_objects {
        return Err(replication_invalid(
            "replication completed object count exceeds total object count",
        ));
    }
    if record.completed_bytes > record.total_bytes {
        return Err(replication_invalid(
            "replication completed byte count exceeds total byte count",
        ));
    }
    if record.created_at_unix_ms > record.updated_at_unix_ms {
        return Err(replication_invalid(
            "replication timestamps are out of order",
        ));
    }
    let source_fields = [
        record.source_placement_set_id.is_some(),
        record.source_backend_id.is_some(),
        record.source_storage_volume_id.is_some(),
        record.source_placement_generation.is_some(),
        record.source_agent_id.is_some(),
        record.source_session_generation.is_some(),
        record.source_mount_generation.is_some(),
        record.source_route_generation.is_some(),
    ];
    if source_fields.iter().any(|present| *present) && source_fields.iter().any(|present| !*present)
    {
        return Err(replication_invalid(
            "replication source PlacementSet binding must be complete",
        ));
    }
    if record.source_edge_cluster_id.is_some() != record.source_gateway_pool_id.is_some()
        || record.target_edge_cluster_id.is_some() != record.target_gateway_pool_id.is_some()
    {
        return Err(replication_invalid(
            "replication EdgeCluster and GatewayPool bindings must be paired",
        ));
    }
    let target_fields = [
        record.target_edge_cluster_id.is_some(),
        record.target_gateway_pool_id.is_some(),
        record.target_placement_generation.is_some(),
        record.target_agent_id.is_some(),
        record.target_session_generation.is_some(),
        record.target_mount_generation.is_some(),
        record.target_route_generation.is_some(),
    ];
    if target_fields.iter().any(|present| *present) && target_fields.iter().any(|present| !*present)
    {
        return Err(replication_invalid(
            "replication target route binding must be complete",
        ));
    }
    if matches!(record.state, ReplicationState::Published)
        && record.target_placement_set_id.is_none()
    {
        return Err(replication_invalid(
            "published replication requires a target PlacementSet",
        ));
    }
    if record.staging_id.as_ref().is_some_and(|value| {
        value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    }) {
        return Err(replication_invalid(
            "replication staging ID must be an opaque resource identifier",
        ));
    }
    Ok(())
}

pub(crate) fn validate_replication_publication(
    record: &ReplicationRecord,
    object_set: &CommitObjectSet,
    placements: &[ObjectPlacement],
    placement_set: &CommitPlacementSet,
) -> CentralResult<()> {
    validate_replication_record(record)?;
    object_set.validate().map_err(CentralError::from)?;
    placement_set.validate().map_err(CentralError::from)?;
    let object_count = u64::try_from(object_set.object_set.object_count())
        .map_err(|_| replication_invalid("replication object count exceeds u64"))?;
    let total_bytes = object_set
        .object_set
        .total_bytes()
        .map_err(CentralError::from)?;
    if record.tenant_id != object_set.tenant_id
        || record.commit_id != object_set.commit_id.digest()
        || record.object_set_digest != object_set.object_set.object_set_digest
        || record.total_objects != object_count
        || record.total_bytes != total_bytes
        || placement_set.tenant_id != record.tenant_id
        || placement_set.commit_id.digest() != record.commit_id
        || placement_set.backend_id.as_str() != record.target_backend_id
        || placement_set.storage_volume_id.as_ref() != Some(&record.target_storage_volume_id)
        || placement_set.archive_id.is_some()
        || placement_set.object_set_digest != record.object_set_digest
        || placement_set.object_count.get() != record.total_objects
        || placement_set.verified_object_count.get() != record.total_objects
        || !matches!(placement_set.state, CommitPlacementSetState::Published)
        || record
            .target_placement_generation
            .is_some_and(|generation| generation != placement_set.placement_generation)
    {
        return Err(replication_invalid(
            "replication publication does not match its frozen Commit and target",
        ));
    }
    let by_object = placements
        .iter()
        .map(|placement| (placement.object_id, placement))
        .collect::<std::collections::BTreeMap<_, _>>();
    if by_object.len() != placements.len()
        || by_object.len() != object_set.object_set.object_count()
    {
        return Err(replication_invalid(
            "replication publication requires exactly one target copy of every object",
        ));
    }
    for object in &object_set.object_set.objects {
        let placement = by_object.get(&object.object_id).ok_or_else(|| {
            replication_invalid("replication publication is missing a Commit object")
        })?;
        placement.validate().map_err(CentralError::from)?;
        if placement.tenant_id != record.tenant_id
            || placement.backend_id != placement_set.backend_id
            || placement.storage_volume_id != placement_set.storage_volume_id
            || placement.archive_id.is_some()
            || placement.placement_generation != placement_set.placement_generation
            || !matches!(placement.state, PlacementState::Verified)
            || placement.verified_size != object.size
            || placement.verified_digest != object.object_id.digest()
            || record
                .target_edge_cluster_id
                .as_ref()
                .is_some_and(|cluster| placement.edge_cluster_id.as_ref() != Some(cluster))
            || record
                .target_gateway_pool_id
                .as_ref()
                .is_some_and(|pool| placement.gateway_pool_id.as_ref() != Some(pool))
        {
            return Err(replication_invalid(
                "replication object Placement does not match its publication fence",
            ));
        }
    }
    Ok(())
}

/// Verifies the durable Agent checkpoints before the publication transaction. Progress counters
/// are useful for UI, but they are not an authority for publication: every immutable ObjectSet
/// member must have its own fsync-confirmed, digest-verified checkpoint.
pub(crate) fn validate_replication_checkpoints(
    record: &ReplicationRecord,
    object_set: &CommitObjectSet,
    checkpoints: &[ReplicationObjectRecord],
) -> CentralResult<()> {
    if checkpoints.len() != object_set.object_set.object_count() {
        return Err(replication_invalid(
            "replication checkpoints do not cover the complete ObjectSet",
        ));
    }
    let by_object = checkpoints
        .iter()
        .map(|checkpoint| (checkpoint.object_id, checkpoint))
        .collect::<std::collections::BTreeMap<_, _>>();
    if by_object.len() != checkpoints.len() {
        return Err(replication_invalid(
            "replication checkpoints contain duplicate objects",
        ));
    }
    for object in &object_set.object_set.objects {
        let checkpoint = by_object.get(&object.object_id).ok_or_else(|| {
            replication_invalid("replication checkpoint is missing a Commit object")
        })?;
        if checkpoint.tenant_id != record.tenant_id
            || checkpoint.replication_id != record.replication_id
            || checkpoint.state != ReplicationObjectState::Verified
            || checkpoint.offset != object.size.get()
        {
            return Err(replication_invalid(
                "replication checkpoint is not a verified, complete object boundary",
            ));
        }
    }
    Ok(())
}

fn replication_invalid(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false)
}

/// Durable object-level checkpoint for a Replication. The offset is the last fsync-confirmed
/// byte boundary in the target staging area, so a reconnect never needs to restart earlier
/// objects or ranges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationObjectRecord {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub object_id: ObjectId,
    pub offset: u64,
    pub state: ReplicationObjectState,
    pub retry_count: u64,
    pub updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
}

/// Durable writable Workspace identity. Object hydration remains an Agent-side concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub tenant_id: TenantId,
    pub workspace_id: WorkspaceId,
    pub project_id: neoengram_domain::protocol::ProjectId,
    pub artifact_id: neoengram_domain::protocol::ArtifactId,
    pub base_commit_id: Option<ContentDigest>,
    pub target_storage_volume_id: StorageVolumeId,
    pub request_id: RequestId,
    pub lifecycle: WorkspaceLifecycle,
    pub created_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    pub updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
}

/// Placement-derived Commit availability snapshot returned by the authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAvailabilityRecord {
    pub tenant_id: TenantId,
    pub commit_id: ContentDigest,
    pub data_health: DataHealth,
    pub verified_placements: u64,
    pub missing_objects: u64,
    pub verified_storage_volume_ids: Vec<StorageVolumeId>,
}
