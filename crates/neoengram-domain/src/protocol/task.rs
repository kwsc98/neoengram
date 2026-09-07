//! Unified write-operation tasks and audit contracts.
//!
//! A task is the stable identity of one write request.  Domain executors keep their own detail
//! records (for example `JobRecord` or `MaterializationJob`), but every executor reports the same
//! coarse lifecycle and appends the same audit events through this contract.  The types in this
//! module are deliberately transport-neutral: persistence and scheduling live in Central.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    validation::{validate_collection_limit, validate_nonempty_limited, validate_positive},
    AgentId, ArtifactId, ContentDigest, DecimalU64, Generation, ObjectNamespaceId, PrincipalRef,
    ProjectId, ProtocolError, ProtocolResult, RequestId, ResourceVersion, SequenceNumber,
    SnapshotId, StorageVolumeId, TaskAttemptId, TaskEventId, TaskId, TenantId, UnixMillis,
    WorkspaceId,
};
use crate::core::CommitId;

/// Version of the persisted operation-task contract.
pub const OPERATION_TASK_PROTOCOL_VERSION: u16 = 2;
/// Capability required by every current Agent session that participates in task-fenced work.
///
/// This is intentionally a separate capability from the materialization data-plane capability:
/// Central must be able to reject an Agent that understands object transfer but cannot persist
/// the unified task/attempt identity required by the v21 control contract.
pub const OPERATION_TASK_CAPABILITY_V2: &str = "operation_task_v2";
/// Maximum task state, error, and actor metadata text accepted by the contract.
pub const MAX_TASK_TEXT_BYTES: usize = 512;
/// Maximum task detail reference text accepted by the contract.
pub const MAX_TASK_DETAIL_BYTES: usize = 256;
/// Maximum number of resource links attached to one task.
pub const MAX_TASK_RESOURCE_LINKS: usize = 128;
/// Maximum number of relations supplied to the cycle checker in one batch.
pub const MAX_TASK_RELATIONS: usize = 16_384;
/// Maximum number of event message/detail bytes retained in the audit log.
pub const MAX_TASK_EVENT_DETAIL_BYTES: usize = 4_096;
/// Maximum number of stages in one operation plan.
pub const MAX_TASK_STAGES: usize = 128;
/// Maximum dependencies declared by one stage.
pub const MAX_STAGE_DEPENDENCIES: usize = 64;

fn increment_resource_version(
    current: ResourceVersion,
    field: &'static str,
) -> ProtocolResult<ResourceVersion> {
    let next = current
        .get()
        .checked_add(1)
        .ok_or_else(|| ProtocolError::InvalidField {
            field,
            reason: "resource version counter overflow".to_owned(),
        })?;
    Ok(ResourceVersion::new(next))
}

/// The user-visible intent represented by an [`OperationTask`].
///
/// Values intentionally use dotted stable names because they are also used as API filters and
/// database/reporting dimensions. Unknown values are rejected by Serde rather than silently
/// falling back to a generic operation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    Default,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskIntent {
    #[serde(rename = "project.create")]
    #[default]
    ProjectCreate,
    #[serde(rename = "project.delete")]
    ProjectDelete,
    #[serde(rename = "project.restore")]
    ProjectRestore,
    #[serde(rename = "artifact.create")]
    ArtifactCreate,
    #[serde(rename = "artifact.delete")]
    ArtifactDelete,
    #[serde(rename = "artifact.restore")]
    ArtifactRestore,
    #[serde(rename = "workspace.create")]
    WorkspaceCreate,
    #[serde(rename = "workspace.delete")]
    WorkspaceDelete,
    #[serde(rename = "workspace.restore")]
    WorkspaceRestore,
    #[serde(rename = "snapshot.create")]
    SnapshotCreate,
    #[serde(rename = "snapshot.delete")]
    SnapshotDelete,
    #[serde(rename = "snapshot.restore")]
    SnapshotRestore,
    #[serde(rename = "storage_volume.create")]
    StorageVolumeCreate,
    #[serde(rename = "storage_volume.delete")]
    StorageVolumeDelete,
    #[serde(rename = "storage_volume.restore")]
    StorageVolumeRestore,
    #[serde(rename = "s3_access_point.create")]
    S3AccessPointCreate,
    #[serde(rename = "s3_access_point.delete")]
    S3AccessPointDelete,
    #[serde(rename = "s3_access_point.enable")]
    S3AccessPointEnable,
    #[serde(rename = "s3_access_point.disable")]
    S3AccessPointDisable,
    #[serde(rename = "commit.validate")]
    CommitValidate,
    #[serde(rename = "commit.create")]
    CommitCreate,
    #[serde(rename = "commit.materialize")]
    CommitMaterialize,
    #[serde(rename = "agent_enrollment.create")]
    AgentEnrollmentCreate,
    #[serde(rename = "agent_enrollment.approve")]
    AgentEnrollmentApprove,
    #[serde(rename = "agent_enrollment.reject")]
    AgentEnrollmentReject,
    #[serde(rename = "agent_enrollment.recover")]
    AgentEnrollmentRecover,
    #[serde(rename = "agent_enrollment.delete")]
    AgentEnrollmentDelete,
    #[serde(rename = "gateway_pool.create")]
    GatewayPoolCreate,
    #[serde(rename = "gateway_pool.update")]
    GatewayPoolUpdate,
    #[serde(rename = "gateway_pool.drain")]
    GatewayPoolDrain,
    #[serde(rename = "gateway_pool.delete")]
    GatewayPoolDelete,
    #[serde(rename = "gateway_replica.create")]
    GatewayReplicaCreate,
    #[serde(rename = "gateway_replica.activate")]
    GatewayReplicaActivate,
    #[serde(rename = "gateway_replica.drain")]
    GatewayReplicaDrain,
    #[serde(rename = "gateway_replica.revoke")]
    GatewayReplicaRevoke,
    #[serde(rename = "gateway_replica.delete")]
    GatewayReplicaDelete,
    #[serde(rename = "s3_credential.create")]
    S3CredentialCreate,
    #[serde(rename = "s3_credential.revoke")]
    S3CredentialRevoke,
}

impl TaskIntent {
    /// Returns the stable wire/API value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCreate => "project.create",
            Self::ProjectDelete => "project.delete",
            Self::ProjectRestore => "project.restore",
            Self::ArtifactCreate => "artifact.create",
            Self::ArtifactDelete => "artifact.delete",
            Self::ArtifactRestore => "artifact.restore",
            Self::WorkspaceCreate => "workspace.create",
            Self::WorkspaceDelete => "workspace.delete",
            Self::WorkspaceRestore => "workspace.restore",
            Self::CommitValidate => "commit.validate",
            Self::CommitCreate => "commit.create",
            Self::SnapshotCreate => "snapshot.create",
            Self::SnapshotDelete => "snapshot.delete",
            Self::SnapshotRestore => "snapshot.restore",
            Self::StorageVolumeCreate => "storage_volume.create",
            Self::StorageVolumeDelete => "storage_volume.delete",
            Self::StorageVolumeRestore => "storage_volume.restore",
            Self::S3AccessPointCreate => "s3_access_point.create",
            Self::S3AccessPointDelete => "s3_access_point.delete",
            Self::S3AccessPointEnable => "s3_access_point.enable",
            Self::S3AccessPointDisable => "s3_access_point.disable",
            Self::CommitMaterialize => "commit.materialize",
            Self::AgentEnrollmentCreate => "agent_enrollment.create",
            Self::AgentEnrollmentApprove => "agent_enrollment.approve",
            Self::AgentEnrollmentReject => "agent_enrollment.reject",
            Self::AgentEnrollmentRecover => "agent_enrollment.recover",
            Self::AgentEnrollmentDelete => "agent_enrollment.delete",
            Self::GatewayPoolCreate => "gateway_pool.create",
            Self::GatewayPoolUpdate => "gateway_pool.update",
            Self::GatewayPoolDrain => "gateway_pool.drain",
            Self::GatewayPoolDelete => "gateway_pool.delete",
            Self::GatewayReplicaCreate => "gateway_replica.create",
            Self::GatewayReplicaActivate => "gateway_replica.activate",
            Self::GatewayReplicaDrain => "gateway_replica.drain",
            Self::GatewayReplicaRevoke => "gateway_replica.revoke",
            Self::GatewayReplicaDelete => "gateway_replica.delete",
            Self::S3CredentialCreate => "s3_credential.create",
            Self::S3CredentialRevoke => "s3_credential.revoke",
        }
    }

    #[must_use]
    pub const fn is_materialization(self) -> bool {
        matches!(self, Self::CommitMaterialize)
    }

    #[must_use]
    pub const fn is_delete(self) -> bool {
        matches!(
            self,
            Self::ProjectDelete
                | Self::ArtifactDelete
                | Self::WorkspaceDelete
                | Self::SnapshotDelete
                | Self::StorageVolumeDelete
                | Self::S3AccessPointDelete
                | Self::AgentEnrollmentReject
                | Self::AgentEnrollmentDelete
                | Self::GatewayPoolDrain
                | Self::GatewayPoolDelete
                | Self::GatewayReplicaDrain
                | Self::GatewayReplicaRevoke
                | Self::GatewayReplicaDelete
                | Self::S3CredentialRevoke
        )
    }

    /// Returns the stable execution stage keys for a user intent.  These are coarse,
    /// user-visible stages; object batches, shards and individual objects remain detail records.
    #[must_use]
    pub const fn default_stage_keys(self) -> &'static [&'static str] {
        match self {
            Self::CommitCreate => &[
                "validate",
                "freeze_workspace",
                "scan_changes",
                "build_manifest",
                "publish_commit",
                "finalize",
            ],
            Self::CommitValidate => &["validate", "scan_changes", "finalize"],
            Self::CommitMaterialize => &[
                "validate",
                "plan",
                "transfer",
                "verify",
                "publish_coverage",
                "finalize",
            ],
            Self::WorkspaceCreate => &["validate", "persist", "materialize", "verify", "publish"],
            Self::SnapshotCreate => &[
                "validate",
                "persist",
                "delivery_materialize",
                "verify",
                "publish",
            ],
            Self::ProjectDelete
            | Self::ArtifactDelete
            | Self::WorkspaceDelete
            | Self::SnapshotDelete
            | Self::StorageVolumeDelete
            | Self::S3AccessPointDelete
            | Self::AgentEnrollmentReject
            | Self::AgentEnrollmentDelete
            | Self::GatewayPoolDrain
            | Self::GatewayPoolDelete
            | Self::GatewayReplicaDrain
            | Self::GatewayReplicaRevoke
            | Self::GatewayReplicaDelete
            | Self::S3CredentialRevoke => {
                &["validate", "impact", "quiesce", "quarantine", "finalize"]
            }
            Self::ProjectCreate
            | Self::ProjectRestore
            | Self::ArtifactCreate
            | Self::ArtifactRestore
            | Self::WorkspaceRestore
            | Self::SnapshotRestore
            | Self::StorageVolumeCreate
            | Self::StorageVolumeRestore
            | Self::S3AccessPointCreate
            | Self::S3AccessPointEnable
            | Self::S3AccessPointDisable
            | Self::AgentEnrollmentCreate
            | Self::AgentEnrollmentApprove
            | Self::AgentEnrollmentRecover
            | Self::GatewayPoolCreate
            | Self::GatewayPoolUpdate
            | Self::GatewayReplicaCreate
            | Self::GatewayReplicaActivate
            | Self::S3CredentialCreate => &["validate", "persist", "publish"],
        }
    }
}

impl fmt::Display for TaskIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TaskIntent {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "project.create" => Ok(Self::ProjectCreate),
            "project.delete" => Ok(Self::ProjectDelete),
            "project.restore" => Ok(Self::ProjectRestore),
            "artifact.create" => Ok(Self::ArtifactCreate),
            "artifact.delete" => Ok(Self::ArtifactDelete),
            "artifact.restore" => Ok(Self::ArtifactRestore),
            "workspace.create" => Ok(Self::WorkspaceCreate),
            "workspace.delete" => Ok(Self::WorkspaceDelete),
            "workspace.restore" => Ok(Self::WorkspaceRestore),
            "commit.validate" => Ok(Self::CommitValidate),
            "commit.create" => Ok(Self::CommitCreate),
            "snapshot.create" => Ok(Self::SnapshotCreate),
            "snapshot.delete" => Ok(Self::SnapshotDelete),
            "snapshot.restore" => Ok(Self::SnapshotRestore),
            "storage_volume.create" => Ok(Self::StorageVolumeCreate),
            "storage_volume.delete" => Ok(Self::StorageVolumeDelete),
            "storage_volume.restore" => Ok(Self::StorageVolumeRestore),
            "s3_access_point.create" => Ok(Self::S3AccessPointCreate),
            "s3_access_point.delete" => Ok(Self::S3AccessPointDelete),
            "s3_access_point.enable" => Ok(Self::S3AccessPointEnable),
            "s3_access_point.disable" => Ok(Self::S3AccessPointDisable),
            "commit.materialize" => Ok(Self::CommitMaterialize),
            "agent_enrollment.create" => Ok(Self::AgentEnrollmentCreate),
            "agent_enrollment.approve" => Ok(Self::AgentEnrollmentApprove),
            "agent_enrollment.reject" => Ok(Self::AgentEnrollmentReject),
            "agent_enrollment.recover" => Ok(Self::AgentEnrollmentRecover),
            "agent_enrollment.delete" => Ok(Self::AgentEnrollmentDelete),
            "gateway_pool.create" => Ok(Self::GatewayPoolCreate),
            "gateway_pool.update" => Ok(Self::GatewayPoolUpdate),
            "gateway_pool.drain" => Ok(Self::GatewayPoolDrain),
            "gateway_pool.delete" => Ok(Self::GatewayPoolDelete),
            "gateway_replica.create" => Ok(Self::GatewayReplicaCreate),
            "gateway_replica.activate" => Ok(Self::GatewayReplicaActivate),
            "gateway_replica.drain" => Ok(Self::GatewayReplicaDrain),
            "gateway_replica.revoke" => Ok(Self::GatewayReplicaRevoke),
            "gateway_replica.delete" => Ok(Self::GatewayReplicaDelete),
            "s3_credential.create" => Ok(Self::S3CredentialCreate),
            "s3_credential.revoke" => Ok(Self::S3CredentialRevoke),
            _ => Err(ProtocolError::UnsupportedMessageType(value.to_owned())),
        }
    }
}

/// Optional semantic purpose for an intent that can reuse an existing execution plan.
///
/// At present only commit materialization uses a purpose.  The value is intentionally separate
/// from [`TaskIntent`] so `commit.copy` and `commit.repair` share one execution implementation and
/// differ only in their idempotency/fencing inputs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskPurpose {
    Copy,
    Repair,
}

impl TaskPurpose {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Repair => "repair",
        }
    }
}

impl fmt::Display for TaskPurpose {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TaskPurpose {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "copy" => Ok(Self::Copy),
            "repair" => Ok(Self::Repair),
            _ => Err(ProtocolError::UnsupportedMessageType(value.to_owned())),
        }
    }
}

/// Exact execution identity copied into every Central-to-Agent assignment and Agent report.
///
/// The root task and its current stage are separate from domain detail identities (Job, Batch,
/// Delivery, or Replication).  Keeping this fence as one flattened value object prevents one
/// protocol variant from accidentally omitting a generation while still leaving the wire fields
/// at the top level for strict v2 payloads.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionFence {
    pub task_id: TaskId,
    pub attempt: Generation,
    pub stage_key: String,
    pub stage_attempt: Generation,
    pub plan_revision: Generation,
}

impl TaskExecutionFence {
    #[must_use]
    pub fn new(
        task_id: TaskId,
        attempt: Generation,
        stage_key: impl Into<String>,
        stage_attempt: Generation,
        plan_revision: Generation,
    ) -> Self {
        Self {
            task_id,
            attempt,
            stage_key: stage_key.into(),
            stage_attempt,
            plan_revision,
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.task_id.as_str().is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "task_id",
                reason: "must not be empty".to_owned(),
            });
        }
        validate_positive("attempt", self.attempt.get())?;
        validate_nonempty_limited("stage_key", &self.stage_key, MAX_TASK_DETAIL_BYTES)?;
        validate_positive("stage_attempt", self.stage_attempt.get())?;
        validate_positive("plan_revision", self.plan_revision.get())
    }
}

/// Coarse lifecycle shared by all write operations.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskState {
    Queued,
    Running,
    Waiting,
    Verifying,
    Succeeded,
    Stalled,
    Failed,
    Cancelling,
    Cancelled,
}

impl TaskState {
    #[must_use]
    pub const fn state_name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Verifying => "verifying",
            Self::Succeeded => "succeeded",
            Self::Stalled => "stalled",
            Self::Failed => "failed",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub const fn is_failure_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled)
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Running | Self::Waiting | Self::Verifying | Self::Stalled | Self::Cancelling
        )
    }

    /// Returns whether a state change is structurally valid.  Retryability of `failed -> queued`
    /// additionally depends on the task's [`TaskIssue::retryable`] flag and is checked by
    /// [`Self::validate_transition`].
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Queued,
                Self::Running | Self::Waiting | Self::Stalled | Self::Failed | Self::Cancelling
            ) | (
                Self::Running,
                Self::Waiting
                    | Self::Verifying
                    | Self::Succeeded
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelling
            ) | (
                Self::Waiting,
                Self::Queued
                    | Self::Running
                    | Self::Verifying
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelling
            ) | (
                Self::Verifying,
                Self::Running | Self::Succeeded | Self::Stalled | Self::Failed | Self::Cancelling
            ) | (
                Self::Stalled,
                Self::Queued | Self::Running | Self::Failed | Self::Cancelling
            ) | (Self::Failed, Self::Queued | Self::Cancelling)
                | (Self::Cancelling, Self::Cancelled)
        )
    }

    /// Validates a state transition with the retryability rule applied.
    pub fn validate_transition(self, next: Self, retryable: bool) -> ProtocolResult<()> {
        if !self.can_transition_to(next) {
            return Err(ProtocolError::InvalidField {
                field: "state",
                reason: format!("cannot transition from {self:?} to {next:?}"),
            });
        }
        if self == Self::Failed && next == Self::Queued && !retryable {
            return Err(ProtocolError::InvalidField {
                field: "state",
                reason: "a non-retryable failure cannot be retried".to_owned(),
            });
        }
        Ok(())
    }
}

/// Lifecycle of one stage in a task plan.  A stage is the smallest user-visible execution unit;
/// object batches, shards and individual objects remain detail records owned by the domain flow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StageState {
    Pending,
    Ready,
    Running,
    Waiting,
    Verifying,
    Succeeded,
    Skipped,
    NoOp,
    Stalled,
    Failed,
    Cancelling,
    Cancelled,
}

impl StageState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Skipped | Self::NoOp | Self::Failed | Self::Cancelled
        )
    }

    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Succeeded | Self::Skipped | Self::NoOp)
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::Ready | Self::Running | Self::Skipped | Self::NoOp | Self::Cancelling
            ) | (
                Self::Ready,
                Self::Running | Self::Skipped | Self::NoOp | Self::Cancelling
            ) | (
                Self::Running,
                Self::Waiting
                    | Self::Verifying
                    | Self::Succeeded
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelling
            ) | (
                Self::Waiting,
                Self::Running | Self::Verifying | Self::Stalled | Self::Failed | Self::Cancelling
            ) | (
                Self::Verifying,
                Self::Running | Self::Succeeded | Self::Stalled | Self::Failed | Self::Cancelling
            ) | (
                Self::Stalled,
                Self::Ready | Self::Running | Self::Failed | Self::Cancelling
            ) | (Self::Failed, Self::Ready | Self::Cancelling)
                | (Self::Cancelling, Self::Cancelled)
        )
    }

    pub fn validate_transition(self, next: Self, retryable: bool) -> ProtocolResult<()> {
        if !self.can_transition_to(next) {
            return Err(ProtocolError::InvalidField {
                field: "stage.state",
                reason: format!("cannot transition from {self:?} to {next:?}"),
            });
        }
        if self == Self::Failed && next == Self::Ready && !retryable {
            return Err(ProtocolError::InvalidField {
                field: "stage.state",
                reason: "a non-retryable stage failure cannot be retried".to_owned(),
            });
        }
        Ok(())
    }
}

/// Terminal disposition of a stage.  `Reused` records that a compatible prior validation or
/// execution was adopted without creating another task; it is still a successful stage outcome.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StageOutcome {
    Succeeded,
    Skipped,
    NoOp,
    Reused,
    Failed,
    Cancelled,
}

impl StageOutcome {
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Skipped | Self::NoOp | Self::Reused
        )
    }
}

/// The resource targeted by a task.  This is deliberately generic so a task can address a Commit,
/// Workspace, S3 access point, or infrastructure object without adding nullable columns for every
/// resource type.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct TaskResourceRef {
    pub resource_kind: TaskResourceKind,
    pub resource_id: String,
}

impl TaskResourceRef {
    #[must_use]
    pub fn new(resource_kind: TaskResourceKind, resource_id: impl Into<String>) -> Self {
        Self {
            resource_kind,
            resource_id: resource_id.into(),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_nonempty_limited(
            "primary_resource.resource_id",
            &self.resource_id,
            MAX_TASK_DETAIL_BYTES,
        )
    }
}

/// One stage in a task's execution DAG.  Stages are persisted as separate rows so a retry can
/// retain prior attempts while the root task keeps one stable identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskStage {
    pub task_id: TaskId,
    pub stage_key: String,
    pub stage_kind: String,
    pub ordinal: DecimalU64,
    pub dependencies: Vec<String>,
    pub state: StageState,
    pub stage_attempt: Generation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<StageOutcome>,
    pub progress: TaskProgressSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssue>,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<UnixMillis>,
    pub resource_version: ResourceVersion,
}

impl TaskStage {
    /// Builds the initial DAG for an intent.  Dependencies are linear by default; callers may
    /// replace them with independent branches when a flow can execute stages in parallel.
    #[must_use]
    pub fn plan_for_intent(task_id: TaskId, intent: TaskIntent, now: UnixMillis) -> Vec<Self> {
        intent
            .default_stage_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let dependencies = index
                    .checked_sub(1)
                    .and_then(|previous| intent.default_stage_keys().get(previous))
                    .map(|key| vec![(*key).to_owned()])
                    .unwrap_or_default();
                Self::new(
                    task_id.clone(),
                    *key,
                    *key,
                    index as u64 + 1,
                    dependencies,
                    now,
                )
            })
            .collect()
    }

    #[must_use]
    pub fn new(
        task_id: TaskId,
        stage_key: impl Into<String>,
        stage_kind: impl Into<String>,
        ordinal: u64,
        dependencies: Vec<String>,
        now: UnixMillis,
    ) -> Self {
        Self {
            task_id,
            stage_key: stage_key.into(),
            stage_kind: stage_kind.into(),
            ordinal: DecimalU64::new(ordinal),
            dependencies,
            state: StageState::Pending,
            stage_attempt: Generation::new(1),
            outcome: None,
            progress: TaskProgressSummary::default(),
            detail_kind: None,
            detail_id: None,
            issue: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            resource_version: ResourceVersion::new(1),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_nonempty_limited("stage_key", &self.stage_key, MAX_TASK_DETAIL_BYTES)?;
        validate_nonempty_limited("stage_kind", &self.stage_kind, MAX_TASK_DETAIL_BYTES)?;
        validate_positive("stage.ordinal", self.ordinal.get())?;
        validate_collection_limit(
            "stage.dependencies",
            self.dependencies.len(),
            MAX_STAGE_DEPENDENCIES,
        )?;
        let mut dependencies = BTreeSet::new();
        for dependency in &self.dependencies {
            validate_nonempty_limited("stage.dependencies", dependency, MAX_TASK_DETAIL_BYTES)?;
            if dependency == &self.stage_key {
                return Err(ProtocolError::InvalidField {
                    field: "stage.dependencies",
                    reason: "a stage cannot depend on itself".to_owned(),
                });
            }
            if !dependencies.insert(dependency) {
                return Err(ProtocolError::InvalidField {
                    field: "stage.dependencies",
                    reason: format!("duplicate dependency {dependency:?}"),
                });
            }
        }
        validate_positive("stage_attempt", self.stage_attempt.get())?;
        if self.created_at_unix_ms.get() == 0
            || self.updated_at_unix_ms.get() < self.created_at_unix_ms.get()
        {
            return Err(ProtocolError::InvalidField {
                field: "stage.timestamps",
                reason: "created and updated timestamps are inconsistent".to_owned(),
            });
        }
        if let Some(started) = self.started_at_unix_ms {
            if started < self.created_at_unix_ms || started > self.updated_at_unix_ms {
                return Err(ProtocolError::InvalidField {
                    field: "stage.started_at_unix_ms",
                    reason: "started timestamp must be within stage lifetime".to_owned(),
                });
            }
        }
        if let Some(finished) = self.finished_at_unix_ms {
            if finished < self.created_at_unix_ms || finished > self.updated_at_unix_ms {
                return Err(ProtocolError::InvalidField {
                    field: "stage.finished_at_unix_ms",
                    reason: "finished timestamp must be within stage lifetime".to_owned(),
                });
            }
            if !self.state.is_terminal() {
                return Err(ProtocolError::InvalidField {
                    field: "stage.finished_at_unix_ms",
                    reason: "only terminal stages may have a finished timestamp".to_owned(),
                });
            }
        }
        if let Some(issue) = &self.issue {
            issue.validate()?;
        }
        self.progress.validate()?;
        if self.state.is_terminal() && self.outcome.is_none() {
            return Err(ProtocolError::InvalidField {
                field: "stage.outcome",
                reason: "a terminal stage must record an outcome".to_owned(),
            });
        }
        if let Some(outcome) = self.outcome {
            if !self.state.is_terminal() {
                return Err(ProtocolError::InvalidField {
                    field: "stage.outcome",
                    reason: "an outcome requires a terminal stage state".to_owned(),
                });
            }
            if outcome.is_success() != self.state.is_success() {
                return Err(ProtocolError::InvalidField {
                    field: "stage.outcome",
                    reason: "stage outcome does not match stage state".to_owned(),
                });
            }
        }
        Ok(())
    }

    pub fn transition_to(&mut self, next: StageState, now: UnixMillis) -> ProtocolResult<()> {
        let retryable = self.issue.as_ref().is_some_and(|issue| issue.retryable);
        self.state.validate_transition(next, retryable)?;
        if now < self.updated_at_unix_ms {
            return Err(ProtocolError::InvalidField {
                field: "stage.updated_at_unix_ms",
                reason: "stage timestamps cannot move backwards".to_owned(),
            });
        }
        let next_resource_version =
            increment_resource_version(self.resource_version, "stage.resource_version")?;
        self.state = next;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        if matches!(
            next,
            StageState::Running | StageState::Waiting | StageState::Verifying
        ) && self.started_at_unix_ms.is_none()
        {
            self.started_at_unix_ms = Some(now);
        }
        if next.is_terminal() {
            self.finished_at_unix_ms = Some(now);
            self.outcome = Some(match next {
                StageState::Succeeded => StageOutcome::Succeeded,
                StageState::Skipped => StageOutcome::Skipped,
                StageState::NoOp => StageOutcome::NoOp,
                StageState::Failed => StageOutcome::Failed,
                StageState::Cancelled => StageOutcome::Cancelled,
                _ => unreachable!("terminal stage state covered above"),
            });
        } else {
            self.finished_at_unix_ms = None;
            self.outcome = None;
        }
        if !matches!(next, StageState::Failed | StageState::Stalled) {
            self.issue = None;
        }
        Ok(())
    }

    /// Marks a successful stage as reused without creating a child task.
    pub fn mark_reused(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        if !matches!(
            self.state,
            StageState::Pending
                | StageState::Ready
                | StageState::Running
                | StageState::Waiting
                | StageState::Verifying
        ) {
            return Err(ProtocolError::InvalidField {
                field: "stage.state",
                reason: "only an uncompleted stage can be marked reused".to_owned(),
            });
        }
        if now < self.updated_at_unix_ms {
            return Err(ProtocolError::InvalidField {
                field: "stage.updated_at_unix_ms",
                reason: "stage timestamps cannot move backwards".to_owned(),
            });
        }
        let next_resource_version =
            increment_resource_version(self.resource_version, "stage.resource_version")?;
        self.state = StageState::Succeeded;
        self.started_at_unix_ms.get_or_insert(now);
        self.finished_at_unix_ms = Some(now);
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        self.outcome = Some(StageOutcome::Reused);
        Ok(())
    }

    pub fn retry(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        let retryable = self.state == StageState::Stalled
            || self.issue.as_ref().is_some_and(|issue| issue.retryable);
        if !retryable {
            return Err(ProtocolError::InvalidField {
                field: "stage.state",
                reason: "stage is not eligible for retry".to_owned(),
            });
        }
        self.state.validate_transition(StageState::Ready, true)?;
        let next_stage_attempt =
            Generation::new(self.stage_attempt.get().checked_add(1).ok_or_else(|| {
                ProtocolError::InvalidField {
                    field: "stage_attempt",
                    reason: "stage attempt counter overflow".to_owned(),
                }
            })?);
        let next_resource_version =
            increment_resource_version(self.resource_version, "stage.resource_version")?;
        self.stage_attempt = next_stage_attempt;
        self.state = StageState::Ready;
        self.outcome = None;
        self.issue = None;
        self.finished_at_unix_ms = None;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        Ok(())
    }

    /// Starts a fresh execution attempt for this stage while retaining its stable stage key.
    /// The current row is reset to `pending`; callers persist the prior state in their audit
    /// history before replacing it. This is used when a root task is retried so a previously
    /// successful stage cannot make the new attempt complete prematurely.
    pub fn reset_for_retry(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        if now < self.updated_at_unix_ms {
            return Err(ProtocolError::InvalidField {
                field: "stage.updated_at_unix_ms",
                reason: "stage timestamps cannot move backwards".to_owned(),
            });
        }
        let next_stage_attempt =
            Generation::new(self.stage_attempt.get().checked_add(1).ok_or_else(|| {
                ProtocolError::InvalidField {
                    field: "stage_attempt",
                    reason: "stage attempt counter overflow".to_owned(),
                }
            })?);
        let next_resource_version =
            increment_resource_version(self.resource_version, "stage.resource_version")?;
        self.stage_attempt = next_stage_attempt;
        self.state = StageState::Pending;
        self.outcome = None;
        self.progress = TaskProgressSummary::default();
        self.issue = None;
        self.started_at_unix_ms = None;
        self.finished_at_unix_ms = None;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        Ok(())
    }
}

/// Validates one task's stage dependency graph and stage-local invariants.
pub fn validate_task_stages(stages: &[TaskStage]) -> ProtocolResult<()> {
    validate_collection_limit("task_stages", stages.len(), MAX_TASK_STAGES)?;
    let mut by_key = BTreeMap::new();
    let mut task_id: Option<&TaskId> = None;
    for stage in stages {
        stage.validate()?;
        if let Some(existing) = task_id {
            if existing != &stage.task_id {
                return Err(ProtocolError::InvalidField {
                    field: "task_stages.task_id",
                    reason: "all stages must belong to the same task".to_owned(),
                });
            }
        } else {
            task_id = Some(&stage.task_id);
        }
        if by_key.insert(stage.stage_key.clone(), stage).is_some() {
            return Err(ProtocolError::InvalidField {
                field: "task_stages.stage_key",
                reason: format!("duplicate stage key {:?}", stage.stage_key),
            });
        }
    }
    for stage in stages {
        for dependency in &stage.dependencies {
            if !by_key.contains_key(dependency) {
                return Err(ProtocolError::InvalidField {
                    field: "task_stages.dependencies",
                    reason: format!(
                        "stage {:?} references unknown dependency {:?}",
                        stage.stage_key, dependency
                    ),
                });
            }
        }
    }

    fn visit(
        key: &str,
        by_key: &BTreeMap<String, &TaskStage>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if visiting.contains(key) {
            return false;
        }
        if visited.contains(key) {
            return true;
        }
        visiting.insert(key.to_owned());
        if by_key.get(key).is_some_and(|stage| {
            stage
                .dependencies
                .iter()
                .any(|dependency| !visit(dependency, by_key, visiting, visited))
        }) {
            return false;
        }
        visiting.remove(key);
        visited.insert(key.to_owned());
        true
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    if by_key
        .keys()
        .any(|key| !visit(key, &by_key, &mut visiting, &mut visited))
    {
        return Err(ProtocolError::InvalidField {
            field: "task_stages.dependencies",
            reason: "stage dependency graph contains a cycle".to_owned(),
        });
    }
    Ok(())
}

/// Alias emphasizing that the stage dependencies form a DAG.
pub fn validate_stage_dag(stages: &[TaskStage]) -> ProtocolResult<()> {
    validate_task_stages(stages)
}

/// Origin of a task record.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOrigin {
    User,
    System,
}

/// One actor visible in audit records. A principal is used for both user and system operations;
/// an Agent actor is useful for events emitted after a control-plane assignment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskActor {
    Principal(PrincipalRef),
    Agent { agent_id: AgentId },
}

impl TaskActor {
    pub(crate) fn validate(&self) -> ProtocolResult<()> {
        if let Self::Principal(principal) = self {
            principal.validate()?;
        }
        Ok(())
    }
}

/// Optional resource scope used by list/query APIs and task indexes.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct TaskScope {
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_namespace_id: Option<ObjectNamespaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<CommitId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<WorkspaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<SnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
}

impl TaskScope {
    #[must_use]
    pub fn new(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            project_id: None,
            artifact_id: None,
            object_namespace_id: None,
            commit_id: None,
            workspace_id: None,
            snapshot_id: None,
            storage_volume_id: None,
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        // The IDs perform their own lexical validation. Relationship validation is intentionally
        // left to the owning resource aggregate because a Commit can be addressed without an
        // Artifact in inventory/recovery queries.
        if self.object_namespace_id.is_some() && self.artifact_id.is_none() {
            // A namespace can be independently managed in v2, so this is valid.
        }
        Ok(())
    }
}

/// Progress summary kept on the task row. Per-object or per-byte updates belong in the domain
/// detail tables and should not produce one audit event each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskProgressSummary {
    pub completed: DecimalU64,
    pub total: DecimalU64,
    pub completed_bytes: DecimalU64,
    pub total_bytes: DecimalU64,
}

/// Short alias used by execution adapters and API view mappers.
pub type TaskProgress = TaskProgressSummary;

impl TaskProgressSummary {
    #[must_use]
    pub const fn new(completed: u64, total: u64, completed_bytes: u64, total_bytes: u64) -> Self {
        Self {
            completed: DecimalU64::new(completed),
            total: DecimalU64::new(total),
            completed_bytes: DecimalU64::new(completed_bytes),
            total_bytes: DecimalU64::new(total_bytes),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.completed.get() > self.total.get() {
            return Err(ProtocolError::InvalidField {
                field: "progress.completed",
                reason: "completed cannot exceed total".to_owned(),
            });
        }
        if self.completed_bytes.get() > self.total_bytes.get() {
            return Err(ProtocolError::InvalidField {
                field: "progress.completed_bytes",
                reason: "completed bytes cannot exceed total bytes".to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for TaskProgressSummary {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

/// Stable failure information. The retryable bit is part of the authority decision and cannot be
/// inferred from a free-form error message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskIssue {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Short alias used by error/reporting adapters.
pub type TaskError = TaskIssue;

impl TaskIssue {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_nonempty_limited("issue.code", &self.code, MAX_TASK_DETAIL_BYTES)?;
        validate_nonempty_limited("issue.message", &self.message, MAX_TASK_TEXT_BYTES)?;
        if let Some(detail) = &self.detail {
            validate_nonempty_limited("issue.detail", detail, MAX_TASK_EVENT_DETAIL_BYTES)?;
        }
        Ok(())
    }
}

/// Unified task row. Domain-specific execution data is referenced by task stages and resource
/// links; a user operation always has one stable task identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperationTask {
    pub task_id: TaskId,
    pub intent_kind: TaskIntent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<TaskPurpose>,
    pub primary_resource: TaskResourceRef,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_links: Vec<TaskResourceLink>,
    pub execution_id: String,
    pub execution_key_digest: ContentDigest,
    pub state: TaskState,
    pub current_stage_key: String,
    pub tenant_id: TenantId,
    #[serde(skip)]
    #[schemars(skip)]
    pub workspace_id: Option<WorkspaceId>,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub actor: TaskActor,
    pub attempt: Generation,
    /// Aggregate progress for the root task.
    pub progress: TaskProgressSummary,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssue>,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<UnixMillis>,
    pub resource_version: ResourceVersion,
    pub origin: TaskOrigin,
    pub executable: bool,
    /// Persisted by the task-stage repository and attached on reads for service/view mapping.
    /// It is deliberately omitted from the OperationTask wire payload because stages have their
    /// own protocol record and optimistic-concurrency history.
    #[serde(skip)]
    #[schemars(skip)]
    pub stages: Vec<TaskStage>,
    /// In-process projection set when a request resolves to an existing semantic execution.
    /// This is intentionally not serialized: the canonical task row remains independent of the
    /// request that happened to discover it.
    #[serde(skip)]
    #[schemars(skip)]
    pub execution_reused: bool,
    /// In-process result projection set when the submitted request identity was already known.
    /// It is kept separate from `execution_reused` so a replay of an alias can report both facts
    /// without changing the canonical task row.
    #[serde(skip)]
    #[schemars(skip)]
    pub request_replayed: bool,
    /// These resource dimensions are local query projections. The public task target is carried
    /// by `primary_resource` and `resource_links`; they are omitted from the wire schema while
    /// the Central index uses the typed resource columns.
    #[serde(skip)]
    #[schemars(skip)]
    pub project_id: Option<ProjectId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub artifact_id: Option<ArtifactId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub object_namespace_id: Option<ObjectNamespaceId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub commit_id: Option<CommitId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub snapshot_id: Option<SnapshotId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub storage_volume_id: Option<StorageVolumeId>,
    #[serde(skip)]
    #[schemars(skip)]
    pub detail_kind: Option<String>,
    #[serde(skip)]
    #[schemars(skip)]
    pub detail_id: Option<String>,
}

impl OperationTask {
    /// Builds a new user task with an initial queued attempt and empty progress summary.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        intent_kind: TaskIntent,
        purpose: Option<TaskPurpose>,
        primary_resource: TaskResourceRef,
        execution_id: impl Into<String>,
        execution_key_digest: ContentDigest,
        scope: TaskScope,
        request_id: RequestId,
        request_digest: ContentDigest,
        actor: TaskActor,
        created_at_unix_ms: UnixMillis,
        deadline_unix_ms: UnixMillis,
    ) -> Self {
        Self {
            task_id,
            intent_kind,
            purpose,
            primary_resource,
            resource_links: Vec::new(),
            execution_id: execution_id.into(),
            execution_key_digest,
            state: TaskState::Queued,
            current_stage_key: "validate".to_owned(),
            tenant_id: scope.tenant_id,
            request_id,
            request_digest,
            actor,
            attempt: Generation::new(1),
            progress: TaskProgressSummary::default(),
            deadline_unix_ms,
            issue: None,
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            resource_version: ResourceVersion::new(1),
            origin: TaskOrigin::User,
            executable: true,
            stages: Vec::new(),
            execution_reused: false,
            request_replayed: false,
            project_id: scope.project_id,
            artifact_id: scope.artifact_id,
            object_namespace_id: scope.object_namespace_id,
            commit_id: scope.commit_id,
            workspace_id: scope.workspace_id,
            snapshot_id: scope.snapshot_id,
            storage_volume_id: scope.storage_volume_id,
            detail_kind: None,
            detail_id: None,
        }
    }

    #[must_use]
    pub fn scope(&self) -> TaskScope {
        TaskScope {
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            object_namespace_id: self.object_namespace_id.clone(),
            commit_id: self.commit_id,
            workspace_id: self.workspace_id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            storage_volume_id: self.storage_volume_id.clone(),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.actor.validate()?;
        self.scope().validate()?;
        if self.intent_kind == TaskIntent::CommitMaterialize && self.purpose.is_none() {
            return Err(ProtocolError::InvalidField {
                field: "purpose",
                reason: "commit.materialize tasks require copy or repair purpose".to_owned(),
            });
        }
        if self.intent_kind != TaskIntent::CommitMaterialize && self.purpose.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "purpose",
                reason: "purpose is only valid for commit.materialize tasks".to_owned(),
            });
        }
        self.primary_resource.validate()?;
        validate_collection_limit(
            "resource_links",
            self.resource_links.len(),
            MAX_TASK_RESOURCE_LINKS,
        )?;
        for link in &self.resource_links {
            link.validate()?;
            if link.task_id != self.task_id {
                return Err(ProtocolError::InvalidField {
                    field: "resource_links.task_id",
                    reason: "resource link belongs to a different task".to_owned(),
                });
            }
        }
        validate_nonempty_limited("execution_id", &self.execution_id, MAX_TASK_DETAIL_BYTES)?;
        validate_nonempty_limited(
            "current_stage_key",
            &self.current_stage_key,
            MAX_TASK_DETAIL_BYTES,
        )?;
        validate_positive("attempt", self.attempt.get())?;
        if self.created_at_unix_ms.get() == 0
            || self.updated_at_unix_ms.get() < self.created_at_unix_ms.get()
            || self.deadline_unix_ms.get() <= self.created_at_unix_ms.get()
        {
            return Err(ProtocolError::InvalidField {
                field: "timestamps",
                reason: "created, updated, and deadline timestamps are inconsistent".to_owned(),
            });
        }
        if let Some(started) = self.started_at_unix_ms {
            if started.get() < self.created_at_unix_ms.get()
                || started.get() > self.updated_at_unix_ms.get()
            {
                return Err(ProtocolError::InvalidField {
                    field: "started_at_unix_ms",
                    reason: "started timestamp must be within task lifetime".to_owned(),
                });
            }
        }
        if let Some(finished) = self.finished_at_unix_ms {
            if finished.get() < self.created_at_unix_ms.get()
                || finished.get() > self.updated_at_unix_ms.get()
            {
                return Err(ProtocolError::InvalidField {
                    field: "finished_at_unix_ms",
                    reason: "finished timestamp must be within task lifetime".to_owned(),
                });
            }
            if self.started_at_unix_ms.is_none() && !matches!(self.state, TaskState::Queued) {
                return Err(ProtocolError::InvalidField {
                    field: "started_at_unix_ms",
                    reason: "a non-queued finished task must have a start timestamp".to_owned(),
                });
            }
        }
        if self.state == TaskState::Failed {
            if let Some(issue) = &self.issue {
                issue.validate()?;
            }
        } else if let Some(issue) = &self.issue {
            issue.validate()?;
        }
        self.progress.validate()?;
        if !self.stages.is_empty() {
            validate_task_stages(&self.stages)?;
            if !self
                .stages
                .iter()
                .any(|stage| stage.stage_key == self.current_stage_key)
            {
                return Err(ProtocolError::InvalidField {
                    field: "current_stage_key",
                    reason: "current stage key is not present in task stages".to_owned(),
                });
            }
            if self.state == TaskState::Succeeded
                && self.stages.iter().any(|stage| !stage.state.is_success())
            {
                return Err(ProtocolError::InvalidField {
                    field: "state",
                    reason: "a succeeded task must have all stages in a successful state"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Applies a lifecycle transition and updates the optimistic-concurrency version.
    pub fn transition_to(&mut self, next: TaskState, now: UnixMillis) -> ProtocolResult<()> {
        let retryable = self.issue.as_ref().is_some_and(|issue| issue.retryable);
        self.state.validate_transition(next, retryable)?;
        if now.get() < self.updated_at_unix_ms.get() {
            return Err(ProtocolError::InvalidField {
                field: "updated_at_unix_ms",
                reason: "task timestamps cannot move backwards".to_owned(),
            });
        }
        let next_resource_version =
            increment_resource_version(self.resource_version, "resource_version")?;
        self.state = next;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        // A cancellation request is itself an execution boundary.  Mark queued work as started
        // when it enters the convergence state so a later `cancelled` terminal state has a
        // complete lifetime even when the original operation never reached `running`.
        if matches!(
            next,
            TaskState::Running | TaskState::Waiting | TaskState::Verifying | TaskState::Cancelling
        ) && self.started_at_unix_ms.is_none()
        {
            self.started_at_unix_ms = Some(now);
        }
        if next.is_terminal() {
            self.finished_at_unix_ms = Some(now);
        } else {
            self.finished_at_unix_ms = None;
        }
        // Stalled is a retryable, observable state. Keep its issue on the task row so operators
        // can see why work stopped until an explicit retry clears it; a successful/active
        // transition still clears the previous diagnosis.
        if !matches!(next, TaskState::Failed | TaskState::Stalled) {
            self.issue = None;
        }
        Ok(())
    }

    /// Applies a retry transition, preserving the task identity and incrementing its attempt.
    pub fn retry(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        let retryable = self.state == TaskState::Stalled
            || self.issue.as_ref().is_some_and(|issue| issue.retryable);
        if !retryable {
            return Err(ProtocolError::InvalidField {
                field: "state",
                reason: "task is not eligible for retry".to_owned(),
            });
        }
        self.state.validate_transition(TaskState::Queued, true)?;
        let next_attempt = Generation::new(self.attempt.get().checked_add(1).ok_or_else(|| {
            ProtocolError::InvalidField {
                field: "attempt",
                reason: "attempt counter overflow".to_owned(),
            }
        })?);
        let next_resource_version =
            increment_resource_version(self.resource_version, "resource_version")?;
        self.attempt = next_attempt;
        self.state = TaskState::Queued;
        self.current_stage_key = "validate".to_owned();
        self.issue = None;
        self.finished_at_unix_ms = None;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        Ok(())
    }

    /// Starts cancellation. The task reaches `cancelled` only after agents and leases converge.
    pub fn cancel(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        if matches!(self.state, TaskState::Cancelling | TaskState::Cancelled) {
            return Ok(());
        }
        self.transition_to(TaskState::Cancelling, now)
    }

    /// Completes a previously requested cancellation after in-flight work has drained.
    pub fn complete_cancellation(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        if self.state == TaskState::Cancelled {
            return Ok(());
        }
        self.transition_to(TaskState::Cancelled, now)
    }

    /// Completes a task only when every stage is successful, skipped, no-op, or reused.
    pub fn complete_with_stages(
        &mut self,
        stages: &[TaskStage],
        now: UnixMillis,
    ) -> ProtocolResult<()> {
        validate_task_stages(stages)?;
        if stages.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "task.stages",
                reason: "a task must define at least one stage before completing".to_owned(),
            });
        }
        if stages.iter().any(|stage| !stage.state.is_success()) {
            return Err(ProtocolError::InvalidField {
                field: "task.stages",
                reason: "a task cannot succeed while a stage is incomplete or failed".to_owned(),
            });
        }
        self.transition_to(TaskState::Succeeded, now)
    }
}

/// One execution attempt. A retry keeps the root [`TaskId`] while creating a new attempt ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskAttempt {
    pub attempt_id: TaskAttemptId,
    pub task_id: TaskId,
    pub attempt: Generation,
    pub state: TaskState,
    pub current_stage_key: String,
    pub created_at_unix_ms: UnixMillis,
    pub updated_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssue>,
    pub resource_version: ResourceVersion,
}

impl TaskAttempt {
    #[must_use]
    pub fn new(
        task_id: TaskId,
        attempt_id: TaskAttemptId,
        attempt: Generation,
        now: UnixMillis,
    ) -> Self {
        Self {
            attempt_id,
            task_id,
            attempt,
            state: TaskState::Queued,
            current_stage_key: "validate".to_owned(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            issue: None,
            resource_version: ResourceVersion::new(1),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("attempt", self.attempt.get())?;
        validate_nonempty_limited(
            "current_stage_key",
            &self.current_stage_key,
            MAX_TASK_DETAIL_BYTES,
        )?;
        if self.created_at_unix_ms.get() == 0
            || self.updated_at_unix_ms.get() < self.created_at_unix_ms.get()
        {
            return Err(ProtocolError::InvalidField {
                field: "timestamps",
                reason: "attempt timestamps are inconsistent".to_owned(),
            });
        }
        if let Some(issue) = &self.issue {
            issue.validate()?;
        }
        Ok(())
    }

    pub fn transition_to(&mut self, next: TaskState, now: UnixMillis) -> ProtocolResult<()> {
        let retryable = self.issue.as_ref().is_some_and(|issue| issue.retryable);
        self.state.validate_transition(next, retryable)?;
        if now.get() < self.updated_at_unix_ms.get() {
            return Err(ProtocolError::InvalidField {
                field: "updated_at_unix_ms",
                reason: "attempt timestamps cannot move backwards".to_owned(),
            });
        }
        let next_resource_version =
            increment_resource_version(self.resource_version, "resource_version")?;
        self.state = next;
        self.updated_at_unix_ms = now;
        self.resource_version = next_resource_version;
        if matches!(
            next,
            TaskState::Running | TaskState::Waiting | TaskState::Verifying
        ) && self.started_at_unix_ms.is_none()
        {
            self.started_at_unix_ms = Some(now);
        }
        if next.is_terminal() {
            self.finished_at_unix_ms = Some(now);
        }
        Ok(())
    }
}

/// Audit event category. Events are append-only and ordered by `sequence` per task.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskEventKind {
    Created,
    StateChanged,
    AttemptStarted,
    AttemptFinished,
    Retried,
    CancelRequested,
    Cancelled,
    Assigned,
    Reported,
    ProgressUpdated,
    ResourceLinked,
    ResourcePublished,
    Failed,
}

/// Immutable audit event. `from_state`/`to_state` are populated for a state-change event; the
/// optional fields keep the event schema useful for assignment/report/resource events without
/// embedding domain-specific payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskEvent {
    pub event_id: TaskEventId,
    pub task_id: TaskId,
    pub sequence: SequenceNumber,
    pub attempt: Generation,
    pub kind: TaskEventKind,
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_state: Option<TaskState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_state: Option<TaskState>,
    pub actor: TaskActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<TaskProgressSummary>,
    pub occurred_at_unix_ms: UnixMillis,
    pub resource_version: ResourceVersion,
}

impl TaskEvent {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("event.sequence", self.sequence.get())?;
        validate_positive("event.attempt", self.attempt.get())?;
        self.actor.validate()?;
        if self.occurred_at_unix_ms.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "occurred_at_unix_ms",
                reason: "event timestamp must be greater than zero".to_owned(),
            });
        }
        if let Some(message) = &self.message {
            validate_nonempty_limited("event.message", message, MAX_TASK_EVENT_DETAIL_BYTES)?;
        }
        if let Some(issue) = &self.issue {
            issue.validate()?;
        }
        if let Some(progress) = &self.progress {
            progress.validate()?;
        }
        if self.kind == TaskEventKind::StateChanged
            && (self.from_state.is_none() || self.to_state.is_none())
        {
            return Err(ProtocolError::InvalidField {
                field: "event.state",
                reason: "state change events require from_state and to_state".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn state_change(
        event_id: TaskEventId,
        task_id: TaskId,
        sequence: SequenceNumber,
        attempt: Generation,
        actor: TaskActor,
        from_state: TaskState,
        to_state: TaskState,
        occurred_at_unix_ms: UnixMillis,
        resource_version: ResourceVersion,
    ) -> Self {
        Self {
            event_id,
            task_id,
            sequence,
            attempt,
            kind: TaskEventKind::StateChanged,
            state: to_state,
            from_state: Some(from_state),
            to_state: Some(to_state),
            actor,
            message: None,
            issue: None,
            progress: None,
            occurred_at_unix_ms,
            resource_version,
        }
    }
}

/// Resource dimension used to navigate from a task to an asset or infrastructure object.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskResourceKind {
    Tenant,
    Project,
    Artifact,
    ObjectNamespace,
    Commit,
    Workspace,
    Snapshot,
    SnapshotDelivery,
    StorageVolume,
    StorageEnrollment,
    Agent,
    Gateway,
    S3AccessPoint,
    S3Credential,
    Deletion,
    RetentionHold,
    Materialization,
    MaterializationBatch,
    Precommit,
    ControlJob,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskResourceRole {
    Primary,
    Source,
    Target,
    Related,
}

/// Link from a task to a resource. The typed scope on [`OperationTask`] is the common query path;
/// links cover secondary resources such as a source Volume or a SnapshotDelivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskResourceLink {
    pub task_id: TaskId,
    pub resource_kind: TaskResourceKind,
    pub resource_id: String,
    pub role: TaskResourceRole,
}

impl TaskResourceLink {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_nonempty_limited("resource_id", &self.resource_id, MAX_TASK_DETAIL_BYTES)
    }

    #[must_use]
    pub fn new(
        task_id: TaskId,
        resource_kind: TaskResourceKind,
        resource_id: impl Into<String>,
        role: TaskResourceRole,
    ) -> Self {
        Self {
            task_id,
            resource_kind,
            resource_id: resource_id.into(),
            role,
        }
    }
}

/// Explicit causal relationship between two task records. A task has no implicit parent; all
/// cross-operation edges are represented explicitly and can be many-to-many.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskRelationKind {
    CausedBy,
    TriggeredBy,
    Supersedes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskRelation {
    pub task_id: TaskId,
    pub related_task_id: TaskId,
    pub relation: TaskRelationKind,
}

impl TaskRelation {
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.task_id == self.related_task_id {
            return Err(ProtocolError::InvalidField {
                field: "related_task_id",
                reason: "a task cannot relate to itself".to_owned(),
            });
        }
        Ok(())
    }
}

/// Checks parent/causal task relations for cycles before they are committed. Supersedes edges are
/// also checked because a cycle would make audit traversal non-terminating.
pub fn validate_task_relations(relations: &[TaskRelation]) -> ProtocolResult<()> {
    validate_collection_limit("task_relations", relations.len(), MAX_TASK_RELATIONS)?;
    let mut graph: BTreeMap<TaskId, Vec<TaskId>> = BTreeMap::new();
    for relation in relations {
        relation.validate()?;
        graph
            .entry(relation.task_id.clone())
            .or_default()
            .push(relation.related_task_id.clone());
    }

    fn visit(
        node: &TaskId,
        graph: &BTreeMap<TaskId, Vec<TaskId>>,
        visiting: &mut BTreeSet<TaskId>,
        visited: &mut BTreeSet<TaskId>,
    ) -> bool {
        if visiting.contains(node) {
            return false;
        }
        if visited.contains(node) {
            return true;
        }
        visiting.insert(node.clone());
        if graph.get(node).is_some_and(|children| {
            children
                .iter()
                .any(|child| !visit(child, graph, visiting, visited))
        }) {
            return false;
        }
        visiting.remove(node);
        visited.insert(node.clone());
        true
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    if graph
        .keys()
        .any(|node| !visit(node, &graph, &mut visiting, &mut visited))
    {
        return Err(ProtocolError::InvalidField {
            field: "task_relations",
            reason: "task relation graph contains a cycle".to_owned(),
        });
    }
    Ok(())
}

/// Root containing every public operation-task contract for schema generation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum TaskProtocolSchema {
    OperationTask(OperationTask),
    TaskIntent(TaskIntent),
    TaskPurpose(TaskPurpose),
    TaskResourceRef(TaskResourceRef),
    TaskScope(TaskScope),
    TaskStage(TaskStage),
    StageState(StageState),
    StageOutcome(StageOutcome),
    TaskAttempt(TaskAttempt),
    TaskEvent(TaskEvent),
    TaskResourceLink(TaskResourceLink),
    TaskRelation(TaskRelation),
    TaskIssue(TaskIssue),
    TaskProgressSummary(TaskProgressSummary),
}

/// Alias retained for schema callers that use the shorter task name.
pub type OperationTaskSchema = TaskProtocolSchema;

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> TaskActor {
        TaskActor::Principal(PrincipalRef {
            kind: super::super::control::PrincipalKind::System,
            id: super::super::PrincipalId::new("system").unwrap(),
            extensions: super::super::super::Extensions::new(),
        })
    }

    fn task(state: TaskState) -> OperationTask {
        let mut task = OperationTask::new(
            TaskId::new("task-1").unwrap(),
            TaskIntent::CommitMaterialize,
            Some(TaskPurpose::Copy),
            TaskResourceRef::new(TaskResourceKind::Commit, "commit-1"),
            "execution-1",
            ContentDigest::from_bytes([0x12; 32]),
            TaskScope::new(TenantId::new("tenant-1").unwrap()),
            RequestId::new("request-1").unwrap(),
            ContentDigest::from_bytes([0x11; 32]),
            actor(),
            UnixMillis::new(1),
            UnixMillis::new(100),
        );
        task.state = state;
        task
    }

    #[test]
    fn task_intent_uses_stable_dotted_values() {
        let json = serde_json::to_string(&TaskIntent::CommitMaterialize).unwrap();
        assert_eq!(json, r#""commit.materialize""#);
        assert_eq!(
            "commit.materialize".parse::<TaskIntent>().unwrap(),
            TaskIntent::CommitMaterialize
        );
        assert!("unknown.operation".parse::<TaskIntent>().is_err());
    }

    #[test]
    fn state_machine_enforces_retryable_failures() {
        assert!(TaskState::Queued.can_transition_to(TaskState::Running));
        assert!(!TaskState::Succeeded.can_transition_to(TaskState::Running));
        assert!(TaskState::Failed
            .validate_transition(TaskState::Queued, true)
            .is_ok());
        assert!(TaskState::Failed
            .validate_transition(TaskState::Queued, false)
            .is_err());
    }

    #[test]
    fn stalled_transition_retains_issue_for_recovery() {
        let mut task = task(TaskState::Running);
        let issue = TaskIssue {
            code: "route_unavailable".to_owned(),
            message: "source Gateway disconnected".to_owned(),
            retryable: true,
            detail: None,
        };
        task.issue = Some(issue.clone());
        task.transition_to(TaskState::Stalled, UnixMillis::new(2))
            .unwrap();
        assert_eq!(task.issue, Some(issue));
    }

    #[test]
    fn retry_preserves_identity_and_increments_attempt() {
        let mut task = task(TaskState::Failed);
        task.issue = Some(TaskIssue {
            code: "timeout".to_owned(),
            message: "source offline".to_owned(),
            retryable: true,
            detail: None,
        });
        task.retry(UnixMillis::new(2)).unwrap();
        assert_eq!(task.task_id, TaskId::new("task-1").unwrap());
        assert_eq!(task.attempt.get(), 2);
        assert_eq!(task.state, TaskState::Queued);
    }

    #[test]
    fn waiting_task_with_retryable_issue_can_retry() {
        let mut task = task(TaskState::Waiting);
        task.issue = Some(TaskIssue {
            code: "source_unavailable".to_owned(),
            message: "source route is not ready".to_owned(),
            retryable: true,
            detail: None,
        });
        task.retry(UnixMillis::new(2)).unwrap();
        assert_eq!(task.state, TaskState::Queued);
        assert_eq!(task.attempt.get(), 2);
        assert!(task.issue.is_none());
    }

    #[test]
    fn relation_checker_rejects_cycles() {
        let a = TaskId::new("a").unwrap();
        let b = TaskId::new("b").unwrap();
        let c = TaskId::new("c").unwrap();
        let relations = vec![
            TaskRelation {
                task_id: a.clone(),
                related_task_id: b.clone(),
                relation: TaskRelationKind::CausedBy,
            },
            TaskRelation {
                task_id: b.clone(),
                related_task_id: c.clone(),
                relation: TaskRelationKind::CausedBy,
            },
            TaskRelation {
                task_id: c,
                related_task_id: a,
                relation: TaskRelationKind::TriggeredBy,
            },
        ];
        assert!(validate_task_relations(&relations).is_err());
    }

    #[test]
    fn operation_task_round_trips_and_validates() {
        let task = task(TaskState::Queued);
        task.validate().unwrap();
        let encoded = serde_json::to_vec(&task).unwrap();
        let decoded: OperationTask = serde_json::from_slice(&encoded).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.task_id, task.task_id);
        assert_eq!(decoded.intent_kind, task.intent_kind);
        assert_eq!(decoded.primary_resource, task.primary_resource);
        assert_eq!(decoded.execution_key_digest, task.execution_key_digest);
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        for removed in ["task_kind", "phase", "parent_task_id"] {
            assert!(!value.as_object().unwrap().contains_key(removed));
        }
    }

    #[test]
    fn stage_dag_rejects_missing_dependencies_and_cycles() {
        let task_id = TaskId::new("task-stages").unwrap();
        let mut validate = TaskStage::new(
            task_id.clone(),
            "validate",
            "validate",
            1,
            Vec::new(),
            UnixMillis::new(1),
        );
        let mut publish = TaskStage::new(
            task_id.clone(),
            "publish",
            "publish",
            2,
            vec!["validate".to_owned()],
            UnixMillis::new(1),
        );
        assert!(validate_task_stages(&[validate.clone(), publish.clone()]).is_ok());

        publish.dependencies = vec!["missing".to_owned()];
        assert!(validate_task_stages(&[validate.clone(), publish.clone()]).is_err());

        validate.dependencies = vec!["publish".to_owned()];
        publish.dependencies = vec!["validate".to_owned()];
        assert!(validate_stage_dag(&[validate, publish]).is_err());
    }

    #[test]
    fn task_cancellation_waits_for_convergence() {
        let mut task = task(TaskState::Running);
        task.cancel(UnixMillis::new(2)).unwrap();
        assert_eq!(task.state, TaskState::Cancelling);
        task.complete_cancellation(UnixMillis::new(3)).unwrap();
        assert_eq!(task.state, TaskState::Cancelled);
    }

    #[test]
    fn task_completion_requires_all_stages_to_succeed() {
        let mut task = task(TaskState::Running);
        let task_id = task.task_id.clone();
        let mut stage = TaskStage::new(
            task_id,
            "validate",
            "validate",
            1,
            Vec::new(),
            UnixMillis::new(1),
        );
        assert!(task
            .complete_with_stages(&[stage.clone()], UnixMillis::new(2))
            .is_err());
        stage
            .transition_to(StageState::Ready, UnixMillis::new(2))
            .unwrap();
        stage
            .transition_to(StageState::Running, UnixMillis::new(3))
            .unwrap();
        stage
            .transition_to(StageState::Succeeded, UnixMillis::new(4))
            .unwrap();
        task.complete_with_stages(&[stage], UnixMillis::new(5))
            .unwrap();
        assert_eq!(task.state, TaskState::Succeeded);
    }

    #[test]
    fn pending_stage_can_reuse_a_prior_execution() {
        let mut stage = TaskStage::new(
            TaskId::new("task-reuse").unwrap(),
            "scan_changes",
            "scan_changes",
            1,
            Vec::new(),
            UnixMillis::new(1),
        );
        stage.mark_reused(UnixMillis::new(2)).unwrap();
        assert_eq!(stage.state, StageState::Succeeded);
        assert_eq!(stage.outcome, Some(StageOutcome::Reused));
        stage.validate().unwrap();
    }
}
