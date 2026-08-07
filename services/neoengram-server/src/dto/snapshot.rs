use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{JsonExtensions, ResourceIssueSummary};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub commit_id: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub storage_volume_id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotListResponse {
    pub items: Vec<SnapshotView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotRequest {
    pub tenant_id: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotResponse {
    pub snapshot: SnapshotView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateSnapshotRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub commit_id: String,
    pub storage_volume_id: String,
    pub snapshot_request_id: String,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateSnapshotResponse {
    pub snapshot: SnapshotView,
    pub replayed: bool,
    pub placement_reused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct SnapshotView {
    pub snapshot_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub commit_id: String,
    pub storage_volume_id: String,
    pub region: String,
    pub message: String,
    pub tag_names: Vec<String>,
    pub state: String,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
    pub integrity: SnapshotIntegritySummary,
    pub logical_file_count: String,
    pub logical_size_bytes: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct SnapshotIntegritySummary {
    pub state: String,
    pub files_verified: String,
    pub bytes_verified: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_at_unix_ms: Option<String>,
}
