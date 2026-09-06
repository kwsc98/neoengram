//! Immutable Snapshot delivery contracts.
//!
//! A Snapshot identifies the immutable Artifact/Commit/Volume tuple.  A delivery is the
//! independently managed read-only projection of that Snapshot onto an Agent Volume.

use crate::core::{ChunkingStrategy, LogicalPath};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    AgentId, AgentMountId, AssignmentGeneration, AssignmentId, ContentDigest, DecimalU64,
    DeliveryGeneration, Extensions, JobId, MountGeneration, OwnerGeneration, PlacementGeneration,
    PrincipalRef, ProjectId, ProtocolError, ProtocolResult, SnapshotDeliveryId, SnapshotId,
    StorageVolumeId, TaskExecutionFence, TenantId, UnixMillis, WireChunkingStrategy,
};

/// The physical strategy used to expose one immutable Snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotDeliveryMode {
    Fuse,
    Copy,
    Hardlink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotDeliveryAction {
    Materialize,
    Delete,
}

/// Durable state of a SnapshotDelivery operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotDeliveryState {
    Requested,
    Validating,
    Materializing,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

/// Storage-side policy that is evaluated before a Delivery assignment is issued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeliveryPolicy {
    pub allowed_modes: Vec<SnapshotDeliveryMode>,
    pub hardlink_policy: HardlinkPolicy,
    pub max_whole_file_bytes: DecimalU64,
    pub copy_reserve_bytes: DecimalU64,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HardlinkPolicy {
    Disabled,
    SealedAcl,
    TrustedLocal,
}

impl SnapshotDeliveryPolicy {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.allowed_modes.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "allowed_modes",
                reason: "must contain at least one delivery mode".to_owned(),
            });
        }
        if self.allowed_modes.contains(&SnapshotDeliveryMode::Hardlink)
            && matches!(self.hardlink_policy, HardlinkPolicy::Disabled)
        {
            return Err(ProtocolError::InvalidField {
                field: "hardlink_policy",
                reason: "hardlink mode cannot be enabled while hardlink policy is disabled"
                    .to_owned(),
            });
        }
        if !self.allowed_modes.contains(&SnapshotDeliveryMode::Hardlink)
            && !matches!(self.hardlink_policy, HardlinkPolicy::Disabled)
        {
            return Err(ProtocolError::InvalidField {
                field: "hardlink_policy",
                reason: "a non-disabled hardlink policy requires hardlink mode to be allowed"
                    .to_owned(),
            });
        }
        crate::validation::validate_extension_keys(
            &self.extensions,
            &[
                "allowed_modes",
                "hardlink_policy",
                "max_whole_file_bytes",
                "copy_reserve_bytes",
            ],
        )
    }

    /// Returns whether a delivery mode is permitted by this Volume policy.
    #[must_use]
    pub fn allows(&self, mode: SnapshotDeliveryMode) -> bool {
        self.allowed_modes.contains(&mode)
    }
}

/// Canonical user operation signed by Central before an Agent assignment is emitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeliveryOperation {
    pub job_id: JobId,
    pub action: SnapshotDeliveryAction,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: crate::ArtifactId,
    pub snapshot_id: SnapshotId,
    pub delivery_id: SnapshotDeliveryId,
    #[schemars(with = "String", length(equal = 64), regex(pattern = crate::validation::CONTENT_DIGEST_PATTERN))]
    pub commit_id: ContentDigest,
    pub storage_volume_id: StorageVolumeId,
    pub mode: SnapshotDeliveryMode,
    /// Logical bytes that the Agent must materialize for Copy delivery.
    pub snapshot_size_bytes: DecimalU64,
    /// Volume free-space reserve that must remain after Copy materialization.
    pub copy_reserve_bytes: DecimalU64,
    /// Volume hardlink policy frozen into the signed operation.
    pub hardlink_policy: HardlinkPolicy,
    /// Server-derived path relative to the approved Volume mount.
    #[schemars(with = "String")]
    pub target_relative_root: LogicalPath,
    #[schemars(with = "String", length(equal = 64), regex(pattern = crate::validation::CONTENT_DIGEST_PATTERN))]
    pub source_index_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl SnapshotDeliveryOperation {
    pub fn canonical_target_relative_root(
        project_id: &ProjectId,
        artifact_id: &crate::ArtifactId,
        snapshot_id: &SnapshotId,
        delivery_id: &SnapshotDeliveryId,
    ) -> ProtocolResult<LogicalPath> {
        LogicalPath::parse(format!(
            "snapshots/{project_id}/{artifact_id}/{snapshot_id}/deliveries/{delivery_id}"
        ))
        .map_err(|error| ProtocolError::InvalidField {
            field: "target_relative_root",
            reason: error.to_string(),
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.principal.validate()?;
        if self.mode == SnapshotDeliveryMode::Hardlink
            && matches!(self.hardlink_policy, HardlinkPolicy::Disabled)
        {
            return Err(ProtocolError::InvalidField {
                field: "hardlink_policy",
                reason: "Hardlink Delivery requires a non-disabled Volume policy".to_owned(),
            });
        }
        let expected = Self::canonical_target_relative_root(
            &self.project_id,
            &self.artifact_id,
            &self.snapshot_id,
            &self.delivery_id,
        )?;
        if self.target_relative_root != expected {
            return Err(ProtocolError::InvalidField {
                field: "target_relative_root",
                reason: format!("must equal the server-derived Delivery path {expected}"),
            });
        }
        crate::validation::validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "action",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "snapshot_id",
                "delivery_id",
                "commit_id",
                "storage_volume_id",
                "mode",
                "snapshot_size_bytes",
                "copy_reserve_bytes",
                "hardlink_policy",
                "target_relative_root",
                "source_index_digest",
                "deadline_unix_ms",
            ],
        )
    }

    pub fn request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.validate()?;
        crate::jcs_blake3(self)
    }
}

/// Complete placement/fencing data required by an Agent to materialize a Delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotDeliveryAssignment {
    pub job_id: JobId,
    /// Root operation/stage fence for this Agent delivery.
    #[serde(flatten)]
    pub task_fence: TaskExecutionFence,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub agent_id: AgentId,
    pub principal: PrincipalRef,
    pub action: SnapshotDeliveryAction,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: crate::ArtifactId,
    pub snapshot_id: SnapshotId,
    pub delivery_id: SnapshotDeliveryId,
    #[schemars(with = "String", length(equal = 64), regex(pattern = crate::validation::CONTENT_DIGEST_PATTERN))]
    pub commit_id: ContentDigest,
    pub storage_volume_id: StorageVolumeId,
    pub snapshot_size_bytes: DecimalU64,
    pub copy_reserve_bytes: DecimalU64,
    pub hardlink_policy: HardlinkPolicy,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    pub placement_generation: PlacementGeneration,
    pub mode: SnapshotDeliveryMode,
    #[schemars(with = "String")]
    pub target_relative_root: LogicalPath,
    #[schemars(with = "String", length(equal = 64), regex(pattern = crate::validation::CONTENT_DIGEST_PATTERN))]
    pub source_index_digest: ContentDigest,
    #[schemars(with = "String", length(equal = 64), regex(pattern = crate::validation::CONTENT_DIGEST_PATTERN))]
    pub request_digest: ContentDigest,
    pub delivery_generation: DeliveryGeneration,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl SnapshotDeliveryAssignment {
    pub fn operation(&self) -> SnapshotDeliveryOperation {
        SnapshotDeliveryOperation {
            job_id: self.job_id.clone(),
            action: self.action,
            principal: self.principal.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            delivery_id: self.delivery_id.clone(),
            commit_id: self.commit_id,
            storage_volume_id: self.storage_volume_id.clone(),
            mode: self.mode,
            snapshot_size_bytes: self.snapshot_size_bytes,
            copy_reserve_bytes: self.copy_reserve_bytes,
            hardlink_policy: self.hardlink_policy,
            target_relative_root: self.target_relative_root.clone(),
            source_index_digest: self.source_index_digest,
            deadline_unix_ms: self.deadline_unix_ms,
            extensions: self.extensions.clone(),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.task_fence.validate()?;
        for (field, value) in [
            ("assignment_generation", self.assignment_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("owner_generation", self.owner_generation.get()),
            ("placement_generation", self.placement_generation.get()),
            ("delivery_generation", self.delivery_generation.get()),
        ] {
            if value == 0 {
                return Err(ProtocolError::InvalidField {
                    field,
                    reason: "must be greater than zero".to_owned(),
                });
            }
        }
        self.principal.validate()?;
        let operation = self.operation();
        operation.validate()?;
        let digest = operation.request_digest()?;
        if self.request_digest != digest {
            return Err(ProtocolError::InvalidField {
                field: "request_digest",
                reason: "does not match the canonical delivery operation".to_owned(),
            });
        }
        crate::validation::validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "task_id",
                "attempt",
                "stage_key",
                "stage_attempt",
                "plan_revision",
                "assignment_id",
                "assignment_generation",
                "agent_id",
                "principal",
                "action",
                "tenant_id",
                "project_id",
                "artifact_id",
                "snapshot_id",
                "delivery_id",
                "commit_id",
                "storage_volume_id",
                "snapshot_size_bytes",
                "copy_reserve_bytes",
                "hardlink_policy",
                "agent_mount_id",
                "mount_generation",
                "owner_generation",
                "placement_generation",
                "mode",
                "target_relative_root",
                "source_index_digest",
                "request_digest",
                "delivery_generation",
                "deadline_unix_ms",
            ],
        )
    }

    #[must_use]
    pub fn supports_hardlink(&self) -> bool {
        self.mode == SnapshotDeliveryMode::Hardlink
    }
}

/// Returns the error code used when a hardlink request cannot be safely materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotDeliveryErrorCode {
    HardlinkRequiresWholeFile,
    HardlinkCrossFilesystem,
    HardlinkUnsafeVolume,
    HardlinkObjectNotSealed,
    DeliveryTargetConflict,
}

impl SnapshotDeliveryErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HardlinkRequiresWholeFile => "HARDLINK_REQUIRES_WHOLE_FILE",
            Self::HardlinkCrossFilesystem => "HARDLINK_CROSS_FILESYSTEM",
            Self::HardlinkUnsafeVolume => "HARDLINK_UNSAFE_VOLUME",
            Self::HardlinkObjectNotSealed => "HARDLINK_OBJECT_NOT_SEALED",
            Self::DeliveryTargetConflict => "DELIVERY_TARGET_CONFLICT",
        }
    }
}

/// The layout frozen for one Commit and all of its manifests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommitDataLayout {
    #[default]
    FastCdc,
    WholeFile,
}

impl From<WireChunkingStrategy> for CommitDataLayout {
    fn from(value: WireChunkingStrategy) -> Self {
        match value {
            WireChunkingStrategy::FastCdc => Self::FastCdc,
            WireChunkingStrategy::WholeFile => Self::WholeFile,
        }
    }
}

impl From<ChunkingStrategy> for CommitDataLayout {
    fn from(value: ChunkingStrategy) -> Self {
        match value {
            ChunkingStrategy::FastCdc => Self::FastCdc,
            ChunkingStrategy::WholeFile => Self::WholeFile,
        }
    }
}

impl From<CommitDataLayout> for WireChunkingStrategy {
    fn from(value: CommitDataLayout) -> Self {
        match value {
            CommitDataLayout::FastCdc => Self::FastCdc,
            CommitDataLayout::WholeFile => Self::WholeFile,
        }
    }
}

impl From<CommitDataLayout> for ChunkingStrategy {
    fn from(value: CommitDataLayout) -> Self {
        match value {
            CommitDataLayout::FastCdc => Self::FastCdc,
            CommitDataLayout::WholeFile => Self::WholeFile,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_layout_round_trips_wire_and_core_chunking() {
        assert_eq!(
            CommitDataLayout::from(WireChunkingStrategy::WholeFile),
            CommitDataLayout::WholeFile
        );
        assert_eq!(
            WireChunkingStrategy::from(CommitDataLayout::FastCdc),
            WireChunkingStrategy::FastCdc
        );
        assert_eq!(
            ChunkingStrategy::from(CommitDataLayout::WholeFile),
            ChunkingStrategy::WholeFile
        );
    }

    #[test]
    fn hardlink_policy_requires_explicit_allowed_mode() {
        let policy = SnapshotDeliveryPolicy {
            allowed_modes: vec![SnapshotDeliveryMode::Fuse],
            hardlink_policy: HardlinkPolicy::SealedAcl,
            max_whole_file_bytes: DecimalU64::new(10),
            copy_reserve_bytes: DecimalU64::new(10),
            extensions: Extensions::new(),
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn canonical_delivery_root_is_scoped_to_project_and_artifact() {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = crate::ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let delivery_id = SnapshotDeliveryId::new("delivery-a").unwrap();

        let root = SnapshotDeliveryOperation::canonical_target_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
            &delivery_id,
        )
        .unwrap();

        assert_eq!(
            root.as_str(),
            "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-a"
        );
    }
}
