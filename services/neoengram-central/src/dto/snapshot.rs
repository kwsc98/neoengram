use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{
    snapshot_delivery::SnapshotDeliveryMode, DataLayout, ResourceIssueSummary,
    ResourceLifecycleView, TaskView,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
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
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateSnapshotRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub commit_id: String,
    /// The EdgeCluster and StorageVolume are selected before the immutable Snapshot is created.
    pub target_edge_cluster_id: String,
    pub target_storage_volume_id: String,
    pub delivery_mode: SnapshotDeliveryMode,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateSnapshotResponse {
    pub snapshot: SnapshotView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
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
    pub delivery_id: String,
    pub edge_cluster_id: String,
    pub storage_volume_id: String,
    pub delivery_mode: SnapshotDeliveryMode,
    /// Denormalized Commit layout for the read-only console; Commit remains authoritative.
    pub data_layout: DataLayout,
    pub message: String,
    pub tag_names: Vec<String>,
    pub state: String,
    /// Dynamic health of the Commit's verified PlacementSet copies.  This is independent from
    /// the logical Snapshot lifecycle/state and therefore changes when a Volume is lost or
    /// another replica is published.
    pub data_health: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
    pub integrity: SnapshotIntegritySummary,
    pub resource_version: String,
    pub lifecycle: ResourceLifecycleView,
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
