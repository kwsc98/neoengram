use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{IndexVersionBody, PlaygroundView, PreCommitView};

/// Immutable Commit data layout. The value is frozen when a Pre-commit starts and is part of
/// the resulting Commit identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(rename_all = "snake_case")]
#[sensitive(opaque)]
pub enum DataLayout {
    FastCdc,
    WholeFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactCommitGraphRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactCommitGraphResponse {
    pub graph: CommitGraphView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactCommitDiffRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub commit_id: String,
    #[serde(default)]
    pub base_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactCommitDiffResponse {
    pub diff: CommitDiffView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitDiffView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<CommitNodeView>,
    pub target_commit: CommitNodeView,
    pub summary: CommitDiffSummary,
    pub changes: Vec<CommitDiffEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitDiffSummary {
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
pub struct CommitDiffEntry {
    pub change_type: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_size_bytes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitGraphView {
    pub graph_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<String>,
    pub nodes: Vec<CommitNodeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitPlaygroundRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub commit_request_id: String,
    pub precommit_id: String,
    pub expected_candidate_index_version: IndexVersionBody,
    pub data_layout: DataLayout,
    pub message: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tag_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitNodeView {
    pub commit_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_commit_id: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub tag_names: Vec<String>,
    pub created_at_unix_ms: String,
    pub data_layout: DataLayout,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitPlaygroundResponse {
    pub commit: CommitNodeView,
    pub playground: PlaygroundView,
    pub consumed_precommit: PreCommitView,
    pub replayed: bool,
}
