use serde::{Deserialize, Serialize};

use super::IndexVersionBody;

/// Common identity for a tenant-scoped Playground query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PlaygroundScopeRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundFileListRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct LogicalFileEntry {
    pub path: String,
    pub entry_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundFileListResponse {
    pub index_version: IndexVersionBody,
    pub items: Vec<LogicalFileEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundChangeListRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    #[serde(default)]
    pub precommit_id: Option<String>,
    #[serde(default)]
    pub change_type: Option<String>,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PlaygroundChangeEntry {
    pub change_type: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PlaygroundChangeSummary {
    pub files_added: String,
    pub files_modified: String,
    pub files_deleted: String,
    pub files_renamed: String,
    pub bytes_added: String,
    pub bytes_removed: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundChangeListResponse {
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precommit_id: Option<String>,
    pub index_version: IndexVersionBody,
    pub summary: PlaygroundChangeSummary,
    pub items: Vec<PlaygroundChangeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundFileMetadataRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct FileMetadataView {
    pub path: String,
    pub size_bytes: String,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundFileMetadataResponse {
    pub index_version: IndexVersionBody,
    pub metadata: FileMetadataView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundDatasetProfileRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DatasetProfileSummary {
    pub format_count: u32,
    pub logical_file_count: String,
    pub logical_size_bytes: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DatasetProfileView {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<DatasetProfileSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryPlaygroundDatasetProfileResponse {
    pub index_version: IndexVersionBody,
    pub profile: DatasetProfileView,
}
