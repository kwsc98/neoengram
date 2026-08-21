use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    DataHealth, ReplicationId, ReplicationObjectState, ReplicationState, RequestId,
    StorageVolumeId, TenantId, WorkspaceId, WorkspaceLifecycle,
};
use serde::{Deserialize, Serialize};

/// Durable control-plane identity for one explicit Commit replication request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationRecord {
    pub tenant_id: TenantId,
    pub replication_id: ReplicationId,
    pub commit_id: ContentDigest,
    pub target_backend_id: String,
    pub target_storage_volume_id: StorageVolumeId,
    pub object_set_digest: ContentDigest,
    pub state: ReplicationState,
    pub request_id: RequestId,
    pub completed_objects: u64,
    pub total_objects: u64,
    pub issue_code: Option<String>,
    pub issue_message: Option<String>,
    pub created_at_unix_ms: neoengram_domain::protocol::UnixMillis,
    pub updated_at_unix_ms: neoengram_domain::protocol::UnixMillis,
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
