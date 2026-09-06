//! Public DTOs for the unified operation-task and audit API.

use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub object_namespace_id: Option<String>,
    #[serde(default)]
    pub commit_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub storage_volume_id: Option<String>,
    #[serde(default)]
    pub intent_kind: Vec<String>,
    #[serde(default)]
    pub purpose: Option<String>,
    #[serde(default)]
    pub state: Vec<String>,
    #[serde(default)]
    pub created_after_unix_ms: Option<String>,
    #[serde(default)]
    pub created_before_unix_ms: Option<String>,
    #[serde(default)]
    pub updated_after_unix_ms: Option<String>,
    #[serde(default)]
    pub updated_before_unix_ms: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskRequest {
    pub tenant_id: String,
    pub task_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskEventListRequest {
    pub tenant_id: String,
    pub task_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskSummaryRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub object_namespace_id: Option<String>,
    #[serde(default)]
    pub commit_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub storage_volume_id: Option<String>,
    #[serde(default)]
    pub intent_kind: Vec<String>,
    #[serde(default)]
    pub purpose: Option<String>,
    #[serde(default)]
    pub state: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetryTaskRequest {
    pub tenant_id: String,
    pub task_id: String,
    #[serde(default)]
    pub expected_resource_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CancelTaskRequest {
    pub tenant_id: String,
    pub task_id: String,
    #[serde(default)]
    pub expected_resource_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskProgressView {
    pub completed: String,
    pub total: String,
    pub completed_bytes: String,
    pub total_bytes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskIssueView {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskResourceRefView {
    pub resource_kind: String,
    pub resource_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskResourceLinkView {
    pub resource_kind: String,
    pub resource_id: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskStageView {
    pub stage_key: String,
    pub stage_kind: String,
    pub ordinal: String,
    pub dependencies: Vec<String>,
    pub state: String,
    pub stage_attempt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    pub progress: TaskProgressView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssueView>,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<String>,
    pub resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskCompletionView {
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskView {
    pub task_id: String,
    pub intent_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    pub state: String,
    pub tenant_id: String,
    pub primary_resource: TaskResourceRefView,
    pub resource_links: Vec<TaskResourceLinkView>,
    pub execution_id: String,
    pub execution_key_digest: String,
    pub execution_reused: bool,
    pub current_stage: TaskStageView,
    pub stages: Vec<TaskStageView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<TaskCompletionView>,
    pub request_id: String,
    pub request_digest: String,
    pub actor: String,
    pub attempt: String,
    pub progress: TaskProgressView,
    pub deadline_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssueView>,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<String>,
    pub resource_version: String,
    pub origin: String,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskAttemptView {
    pub attempt_id: String,
    pub task_id: String,
    pub attempt: String,
    pub state: String,
    pub current_stage_key: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssueView>,
    pub resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskEventView {
    pub event_id: String,
    pub task_id: String,
    pub sequence: String,
    pub attempt: String,
    pub kind: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_state: Option<String>,
    pub actor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<TaskIssueView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<TaskProgressView>,
    pub occurred_at_unix_ms: String,
    pub resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskListResponse {
    pub items: Vec<TaskView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskResponse {
    pub task: TaskView,
    pub attempts: Vec<TaskAttemptView>,
    pub events: Vec<TaskEventView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskEventListResponse {
    pub items: Vec<TaskEventView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskSummaryView {
    pub total: String,
    pub queued: String,
    pub running: String,
    pub waiting: String,
    pub verifying: String,
    pub succeeded: String,
    pub stalled: String,
    pub failed: String,
    pub cancelling: String,
    pub cancelled: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTaskSummaryResponse {
    pub summary: TaskSummaryView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TaskMutationResponse {
    pub task: TaskView,
    pub request_replayed: bool,
    pub execution_reused: bool,
}
