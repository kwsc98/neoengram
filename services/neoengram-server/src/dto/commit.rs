use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{IndexVersionBody, JsonExtensions, PlaygroundView, PreCommitView};

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
pub struct CommitGraphView {
    pub graph_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<String>,
    pub nodes: Vec<CommitNodeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct CommitPlaygroundRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub commit_request_id: String,
    pub precommit_id: String,
    pub expected_candidate_index_version: IndexVersionBody,
    pub message: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tag_names: Vec<String>,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
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
