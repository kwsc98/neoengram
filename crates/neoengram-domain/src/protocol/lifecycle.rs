//! Storage-resource lifecycle and safe-deletion contracts.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AgentId, ArtifactId, ContentDigest, DecimalU64, DeletionId, DeletionProofId,
    LifecycleAssignmentId, LifecycleEventId, LifecycleGeneration, ProjectId, RequestId,
    ResourceVersion, RetentionHoldId, SnapshotId, StorageVolumeId, TenantId, UnixMillis,
    WorkspaceId,
};

/// Fixed recovery window used by lifecycle v1.
pub const DELETION_RECOVERY_WINDOW_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// Maximum lifetime of a dry-run impact digest.
pub const DELETION_IMPACT_TTL_MILLIS: u64 = 5 * 60 * 1_000;

/// Access lifecycle, deliberately independent from resource health or delivery state.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleState {
    Active,
    PendingDelete,
    Deleting,
    Restoring,
    Deleted,
}

/// Fully scoped resource identity. Tenant scope is carried by the containing request/record.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRef {
    Project {
        project_id: ProjectId,
    },
    StorageVolume {
        storage_volume_id: StorageVolumeId,
    },
    Artifact {
        project_id: ProjectId,
        artifact_id: ArtifactId,
    },
    #[serde(rename = "workspace")]
    Workspace {
        project_id: ProjectId,
        artifact_id: ArtifactId,
        #[serde(rename = "workspace_id")]
        workspace_id: WorkspaceId,
    },
    Snapshot {
        snapshot_id: SnapshotId,
    },
}

/// Persisted lifecycle fields shared by Artifact, StorageVolume, Workspace, and Snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceLifecycle {
    pub state: ResourceLifecycleState,
    pub generation: LifecycleGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_deletion_id: Option<DeletionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_requested_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purge_after_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix_ms: Option<UnixMillis>,
}

impl ResourceLifecycle {
    #[must_use]
    pub const fn active() -> Self {
        Self {
            state: ResourceLifecycleState::Active,
            generation: LifecycleGeneration::new(1),
            active_deletion_id: None,
            delete_requested_at_unix_ms: None,
            purge_after_unix_ms: None,
            deleted_at_unix_ms: None,
        }
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self.state, ResourceLifecycleState::Active)
    }
}

impl Default for ResourceLifecycle {
    fn default() -> Self {
        Self::active()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeletionOperationState {
    Requested,
    Quiescing,
    Quarantining,
    Recoverable,
    Restoring,
    Purging,
    Finalizing,
    Completed,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeletionCompletion {
    Restored,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeletionTarget {
    pub resource: ResourceRef,
    pub resource_version: ResourceVersion,
    pub lifecycle_generation: LifecycleGeneration,
    pub requires_agent_cleanup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeletionBlocker {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceRef>,
    pub message: String,
}

/// Immutable five-minute view used to confirm a destructive request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeletionImpact {
    pub tenant_id: TenantId,
    pub root: ResourceRef,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub targets: Vec<DeletionTarget>,
    pub active_job_count: DecimalU64,
    pub active_s3_credential_count: DecimalU64,
    pub estimated_file_count: DecimalU64,
    pub estimated_bytes: DecimalU64,
    pub blockers: Vec<DeletionBlocker>,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionOperation {
    pub deletion_id: DeletionId,
    pub tenant_id: TenantId,
    pub root: ResourceRef,
    pub state: DeletionOperationState,
    pub resource_version: ResourceVersion,
    pub targets: Vec<DeletionTarget>,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub impact_digest: ContentDigest,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub purge_after_unix_ms: UnixMillis,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<DeletionCompletion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Durable continuation point captured when the saga enters `blocked` or `failed`.
    ///
    /// The phase also preserves intent: `restoring` resumes restoration, while the deletion
    /// phases resume deletion. It must be absent outside an error state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_state: Option<DeletionOperationState>,
    pub retry_count: DecimalU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeletionMutationKind {
    Create,
    Restore,
    Retry,
    RetentionHoldCreate,
    RetentionHoldRelease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionMutation {
    pub tenant_id: TenantId,
    pub request_id: RequestId,
    pub kind: DeletionMutationKind,
    pub request_digest: ContentDigest,
    pub deletion_id: DeletionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_hold_id: Option<RetentionHoldId>,
    pub created_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetentionHoldState {
    Active,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionHold {
    pub retention_hold_id: RetentionHoldId,
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub reason: String,
    pub state: RetentionHoldState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<UnixMillis>,
    pub created_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_at_unix_ms: Option<UnixMillis>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventKind {
    DeletionRequested,
    StateChanged,
    RestoreRequested,
    RetryRequested,
    HoldCreated,
    HoldReleased,
    ProofAccepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleEvent {
    pub event_id: LifecycleEventId,
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub kind: LifecycleEventKind,
    pub occurred_at_unix_ms: UnixMillis,
    pub payload_digest: ContentDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeletionProofResult {
    Complete,
    Partial,
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionProof {
    pub proof_id: DeletionProofId,
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub resource: ResourceRef,
    pub lifecycle_generation: LifecycleGeneration,
    pub agent_id: AgentId,
    pub result: DeletionProofResult,
    pub file_count: DecimalU64,
    pub object_count: DecimalU64,
    pub byte_count: DecimalU64,
    pub object_set_digest: ContentDigest,
    pub report_digest: ContentDigest,
    pub completed_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleAction {
    Quarantine,
    Restore,
    Purge,
    CancelJobs,
}

/// Durable outbox item. Agent-specific fencing fields are added by the delivery coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceLifecycleAssignment {
    pub assignment_id: LifecycleAssignmentId,
    pub tenant_id: TenantId,
    pub deletion_id: DeletionId,
    pub resource: ResourceRef,
    pub action: ResourceLifecycleAction,
    pub lifecycle_generation: LifecycleGeneration,
    #[schemars(with = "String")]
    pub request_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_ref_is_explicitly_tagged() {
        let resource = ResourceRef::Snapshot {
            snapshot_id: SnapshotId::new("snapshot-1").unwrap(),
        };
        assert_eq!(
            serde_json::to_value(resource).unwrap(),
            serde_json::json!({"type": "snapshot", "snapshot_id": "snapshot-1"})
        );
    }

    #[test]
    fn active_lifecycle_has_first_generation() {
        let lifecycle = ResourceLifecycle::active();
        assert!(lifecycle.is_active());
        assert_eq!(lifecycle.generation.get(), 1);
    }
}
