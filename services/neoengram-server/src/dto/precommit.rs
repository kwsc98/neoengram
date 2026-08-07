use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{IndexVersionBody, JsonExtensions, PlaygroundView};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct StartPreCommitRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub precommit_request_id: String,
    pub expected_index_version: IndexVersionBody,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct StartPreCommitResponse {
    pub precommit: PreCommitView,
    pub playground: PlaygroundView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPreCommitRequest {
    pub tenant_id: String,
    pub precommit_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPreCommitResponse {
    pub precommit: PreCommitView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct RestartPreCommitRequest {
    pub tenant_id: String,
    pub precommit_id: String,
    pub restart_request_id: String,
    pub expected_index_version: IndexVersionBody,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RestartPreCommitResponse {
    pub precommit: PreCommitView,
    pub playground: PlaygroundView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct CancelPreCommitRequest {
    pub tenant_id: String,
    pub precommit_id: String,
    pub cancel_request_id: String,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CancelPreCommitResponse {
    pub precommit: PreCommitView,
    pub playground: PlaygroundView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PreCommitView {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub precommit_id: String,
    pub precommit_request_id: String,
    pub attempt: u32,
    pub state: String,
    pub phase: String,
    pub progress: PreCommitProgressView,
    pub checks: Vec<PreCommitCheckView>,
    pub warnings: Vec<PreCommitNoticeView>,
    pub blockers: Vec<PreCommitNoticeView>,
    pub source_index_version: IndexVersionBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_index_version: Option<IndexVersionBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<PreCommitDiffSummaryView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_commit_id: Option<String>,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PreCommitProgressView {
    pub percent: u8,
    pub files_completed: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_total: Option<String>,
    pub bytes_completed: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_total: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PreCommitCheckView {
    pub check_id: String,
    pub status: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PreCommitNoticeView {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PreCommitDiffSummaryView {
    pub files_added: String,
    pub files_modified: String,
    pub files_deleted: String,
    pub files_renamed: String,
    pub bytes_added: String,
    pub bytes_removed: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ResourceIssueSummary {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub occurred_at_unix_ms: Option<String>,
}
