use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::ResourceIssueSummary;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(rename_all = "snake_case")]
#[sensitive(opaque)]
pub enum SnapshotDeliveryMode {
    Fuse,
    Copy,
    Hardlink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(rename_all = "snake_case")]
#[sensitive(opaque)]
pub enum SnapshotDeliveryState {
    Requested,
    Validating,
    Materializing,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateSnapshotDeliveryRequest {
    pub tenant_id: String,
    pub snapshot_id: String,
    pub target_storage_volume_id: String,
    pub mode: SnapshotDeliveryMode,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct SnapshotDeliveryView {
    pub delivery_id: String,
    pub snapshot_id: String,
    pub commit_id: String,
    pub storage_volume_id: String,
    pub mode: SnapshotDeliveryMode,
    pub target_relative_root: String,
    pub state: SnapshotDeliveryState,
    pub source_index_digest: String,
    pub delivery_generation: String,
    pub file_count: String,
    pub size_bytes: String,
    pub object_set_digest: String,
    pub resource_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateSnapshotDeliveryResponse {
    pub delivery: SnapshotDeliveryView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotDeliveryRequest {
    pub tenant_id: String,
    pub delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotDeliveryResponse {
    pub delivery: SnapshotDeliveryView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotDeliveryListRequest {
    pub tenant_id: String,
    pub snapshot_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QuerySnapshotDeliveryListResponse {
    pub items: Vec<SnapshotDeliveryView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetrySnapshotDeliveryRequest {
    pub tenant_id: String,
    pub delivery_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetrySnapshotDeliveryResponse {
    pub delivery: SnapshotDeliveryView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeleteSnapshotDeliveryRequest {
    pub tenant_id: String,
    pub delivery_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeleteSnapshotDeliveryResponse {
    pub delivery: SnapshotDeliveryView,
    pub replayed: bool,
}
