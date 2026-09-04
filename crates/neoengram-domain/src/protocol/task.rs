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
    AgentId, ArtifactId, ContentDigest, DecimalU64, Generation, ObjectNamespaceId, PlaygroundId,
    PrincipalRef, ProjectId, ProtocolError, ProtocolResult, RequestId, ResourceVersion,
    SequenceNumber, SnapshotId, StorageVolumeId, TaskAttemptId, TaskEventId, TaskId, TenantId,
    UnixMillis,
};
use crate::core::CommitId;

/// Version of the persisted operation-task contract.
pub const OPERATION_TASK_PROTOCOL_VERSION: u16 = 1;
/// Capability required by every current Agent session that participates in task-fenced work.
///
/// This is intentionally a separate capability from the materialization data-plane capability:
/// Central must be able to reject an Agent that understands object transfer but cannot persist
/// the unified task/attempt identity required by the v20 control contract.
pub const OPERATION_TASK_CAPABILITY_V1: &str = "operation_task_v1";
/// Maximum task phase, error, and actor metadata text accepted by the contract.
pub const MAX_TASK_TEXT_BYTES: usize = 512;
/// Maximum task detail reference text accepted by the contract.
pub const MAX_TASK_DETAIL_BYTES: usize = 256;
/// Maximum number of resource links attached to one task.
pub const MAX_TASK_RESOURCE_LINKS: usize = 128;
/// Maximum number of relations supplied to the cycle checker in one batch.
pub const MAX_TASK_RELATIONS: usize = 16_384;
/// Maximum number of event message/detail bytes retained in the audit log.
pub const MAX_TASK_EVENT_DETAIL_BYTES: usize = 4_096;

/// The write operation represented by an [`OperationTask`].
///
/// Values intentionally use dotted stable names because they are also used as API filters and
/// database/reporting dimensions. Unknown values are rejected by Serde rather than silently
/// falling back to a generic operation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    #[serde(rename = "workspace.create")]
    WorkspaceCreate,
    #[serde(rename = "workspace.materialize")]
    WorkspaceMaterialize,
    #[serde(rename = "precommit.check")]
    PrecommitCheck,
    #[serde(rename = "add.scan")]
    AddScan,
    #[serde(rename = "commit.create")]
    CommitCreate,
    #[serde(rename = "snapshot.create")]
    SnapshotCreate,
    #[serde(rename = "snapshot.delivery.materialize")]
    SnapshotDeliveryMaterialize,
    #[serde(rename = "commit.materialize")]
    CommitMaterialize,
    #[serde(rename = "integrity.scan")]
    IntegrityScan,
    #[serde(rename = "resource.repair")]
    ResourceRepair,
    #[serde(rename = "catalog.lifecycle")]
    CatalogLifecycle,
    #[serde(rename = "storage.lifecycle")]
    StorageLifecycle,
    #[serde(rename = "gateway.lifecycle")]
    GatewayLifecycle,
    #[serde(rename = "s3.lifecycle")]
    S3Lifecycle,
}

impl TaskKind {
    /// Returns the stable wire/API value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceCreate => "workspace.create",
            Self::WorkspaceMaterialize => "workspace.materialize",
            Self::PrecommitCheck => "precommit.check",
            Self::AddScan => "add.scan",
            Self::CommitCreate => "commit.create",
            Self::SnapshotCreate => "snapshot.create",
            Self::SnapshotDeliveryMaterialize => "snapshot.delivery.materialize",
            Self::CommitMaterialize => "commit.materialize",
            Self::IntegrityScan => "integrity.scan",
            Self::ResourceRepair => "resource.repair",
            Self::CatalogLifecycle => "catalog.lifecycle",
            Self::StorageLifecycle => "storage.lifecycle",
            Self::GatewayLifecycle => "gateway.lifecycle",
            Self::S3Lifecycle => "s3.lifecycle",
        }
    }

    #[must_use]
    pub const fn is_materialization(self) -> bool {
        matches!(
            self,
            Self::WorkspaceMaterialize
                | Self::SnapshotDeliveryMaterialize
                | Self::CommitMaterialize
        )
    }
}

impl fmt::Display for TaskKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TaskKind {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "workspace.create" => Ok(Self::WorkspaceCreate),
            "workspace.materialize" => Ok(Self::WorkspaceMaterialize),
            "precommit.check" => Ok(Self::PrecommitCheck),
            "add.scan" => Ok(Self::AddScan),
            "commit.create" => Ok(Self::CommitCreate),
            "snapshot.create" => Ok(Self::SnapshotCreate),
            "snapshot.delivery.materialize" => Ok(Self::SnapshotDeliveryMaterialize),
            "commit.materialize" => Ok(Self::CommitMaterialize),
            "integrity.scan" => Ok(Self::IntegrityScan),
            "resource.repair" => Ok(Self::ResourceRepair),
            "catalog.lifecycle" => Ok(Self::CatalogLifecycle),
            "storage.lifecycle" => Ok(Self::StorageLifecycle),
            "gateway.lifecycle" => Ok(Self::GatewayLifecycle),
            "s3.lifecycle" => Ok(Self::S3Lifecycle),
            _ => Err(ProtocolError::UnsupportedMessageType(value.to_owned())),
        }
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
    Cancelled,
}

impl TaskState {
    #[must_use]
    pub const fn phase_name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Verifying => "verifying",
            Self::Succeeded => "succeeded",
            Self::Stalled => "stalled",
            Self::Failed => "failed",
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
            Self::Running | Self::Waiting | Self::Verifying | Self::Stalled
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
                Self::Running | Self::Waiting | Self::Stalled | Self::Failed | Self::Cancelled
            ) | (
                Self::Running,
                Self::Waiting
                    | Self::Verifying
                    | Self::Succeeded
                    | Self::Stalled
                    | Self::Failed
                    | Self::Cancelled
            ) | (
                Self::Waiting,
                Self::Running | Self::Verifying | Self::Stalled | Self::Failed | Self::Cancelled
            ) | (
                Self::Verifying,
                Self::Running | Self::Succeeded | Self::Stalled | Self::Failed | Self::Cancelled
            ) | (
                Self::Stalled,
                Self::Queued | Self::Running | Self::Failed | Self::Cancelled
            ) | (Self::Failed, Self::Queued)
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

/// Origin of a task record. Legacy records are queryable but must never be scheduled again.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOrigin {
    User,
    System,
    Legacy,
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
    pub playground_id: Option<PlaygroundId>,
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
            playground_id: None,
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
                field: "progress_summary.completed",
                reason: "completed cannot exceed total".to_owned(),
            });
        }
        if self.completed_bytes.get() > self.total_bytes.get() {
            return Err(ProtocolError::InvalidField {
                field: "progress_summary.completed_bytes",
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

/// Unified task row. Domain-specific execution data is referenced by `detail_kind/detail_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OperationTask {
    pub task_id: TaskId,
    pub task_kind: TaskKind,
    pub state: TaskState,
    pub phase: String,
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
    pub playground_id: Option<PlaygroundId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<SnapshotId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_volume_id: Option<StorageVolumeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<TaskId>,
    pub request_id: RequestId,
    pub request_digest: ContentDigest,
    pub actor: TaskActor,
    pub attempt: Generation,
    pub progress_summary: TaskProgressSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail_id: Option<String>,
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
}

impl OperationTask {
    /// Builds a new user task with an initial queued attempt and empty progress summary.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        task_kind: TaskKind,
        scope: TaskScope,
        request_id: RequestId,
        request_digest: ContentDigest,
        actor: TaskActor,
        created_at_unix_ms: UnixMillis,
        deadline_unix_ms: UnixMillis,
    ) -> Self {
        Self {
            task_id,
            task_kind,
            state: TaskState::Queued,
            phase: "queued".to_owned(),
            tenant_id: scope.tenant_id,
            project_id: scope.project_id,
            artifact_id: scope.artifact_id,
            object_namespace_id: scope.object_namespace_id,
            commit_id: scope.commit_id,
            playground_id: scope.playground_id,
            snapshot_id: scope.snapshot_id,
            storage_volume_id: scope.storage_volume_id,
            parent_task_id: None,
            request_id,
            request_digest,
            actor,
            attempt: Generation::new(1),
            progress_summary: TaskProgressSummary::default(),
            detail_kind: None,
            detail_id: None,
            deadline_unix_ms,
            issue: None,
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            resource_version: ResourceVersion::new(1),
            origin: TaskOrigin::User,
            executable: true,
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
            playground_id: self.playground_id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            storage_volume_id: self.storage_volume_id.clone(),
        }
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.actor.validate()?;
        self.scope().validate()?;
        validate_nonempty_limited("phase", &self.phase, MAX_TASK_TEXT_BYTES)?;
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
        if self.origin == TaskOrigin::Legacy && self.executable {
            return Err(ProtocolError::InvalidField {
                field: "executable",
                reason: "legacy tasks cannot be executable".to_owned(),
            });
        }
        if self.state == TaskState::Failed {
            if let Some(issue) = &self.issue {
                issue.validate()?;
            }
        } else if let Some(issue) = &self.issue {
            issue.validate()?;
        }
        self.progress_summary.validate()
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
        self.state = next;
        self.phase = next.phase_name().to_owned();
        self.updated_at_unix_ms = now;
        self.resource_version = ResourceVersion::new(self.resource_version.get().saturating_add(1));
        if matches!(
            next,
            TaskState::Running | TaskState::Waiting | TaskState::Verifying
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
        self.attempt = Generation::new(self.attempt.get().checked_add(1).ok_or_else(|| {
            ProtocolError::InvalidField {
                field: "attempt",
                reason: "attempt counter overflow".to_owned(),
            }
        })?);
        self.state = TaskState::Queued;
        self.phase = TaskState::Queued.phase_name().to_owned();
        self.issue = None;
        self.finished_at_unix_ms = None;
        self.updated_at_unix_ms = now;
        self.resource_version = ResourceVersion::new(self.resource_version.get().saturating_add(1));
        Ok(())
    }

    /// Cancels an active task. Replaying cancellation is idempotent.
    pub fn cancel(&mut self, now: UnixMillis) -> ProtocolResult<()> {
        if self.state == TaskState::Cancelled {
            return Ok(());
        }
        self.transition_to(TaskState::Cancelled, now)
    }
}

/// One execution attempt. A retry keeps the parent [`TaskId`] while creating a new attempt ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskAttempt {
    pub attempt_id: TaskAttemptId,
    pub task_id: TaskId,
    pub attempt: Generation,
    pub state: TaskState,
    pub phase: String,
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
            phase: "queued".to_owned(),
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
        validate_nonempty_limited("phase", &self.phase, MAX_TASK_TEXT_BYTES)?;
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
        self.state = next;
        self.phase = next.phase_name().to_owned();
        self.updated_at_unix_ms = now;
        self.resource_version = ResourceVersion::new(self.resource_version.get().saturating_add(1));
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
    Playground,
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

/// Explicit relationship between two task records. Parent/causal edges are queried independently
/// from `parent_task_id` so one task can be caused by several prior tasks.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskRelationKind {
    Parent,
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
    TaskScope(TaskScope),
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
            TaskKind::CommitMaterialize,
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
    fn task_kind_uses_stable_dotted_values() {
        let json = serde_json::to_string(&TaskKind::SnapshotDeliveryMaterialize).unwrap();
        assert_eq!(json, r#""snapshot.delivery.materialize""#);
        assert_eq!(
            "commit.materialize".parse::<TaskKind>().unwrap(),
            TaskKind::CommitMaterialize
        );
        assert!("unknown.operation".parse::<TaskKind>().is_err());
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
    fn relation_checker_rejects_cycles() {
        let a = TaskId::new("a").unwrap();
        let b = TaskId::new("b").unwrap();
        let c = TaskId::new("c").unwrap();
        let relations = vec![
            TaskRelation {
                task_id: a.clone(),
                related_task_id: b.clone(),
                relation: TaskRelationKind::Parent,
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
        assert_eq!(decoded, task);
    }
}
