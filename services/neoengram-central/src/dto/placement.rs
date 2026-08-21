use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::ResourceIssueSummary;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateCommitReplicationRequest {
    pub tenant_id: String,
    pub commit_id: String,
    pub target_storage_volume_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitReplicationRequest {
    pub tenant_id: String,
    pub replication_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitAvailabilityRequest {
    pub tenant_id: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateWorkspaceRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    #[serde(default)]
    pub base_commit_id: Option<String>,
    pub target_storage_volume_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ReplicationView {
    pub replication_id: String,
    pub tenant_id: String,
    pub commit_id: String,
    pub target_storage_volume_id: String,
    pub state: String,
    pub object_set_digest: String,
    pub completed_objects: String,
    pub total_objects: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateCommitReplicationResponse {
    pub replication: ReplicationView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitReplicationResponse {
    pub replication: ReplicationView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitReplicationListRequest {
    pub tenant_id: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitAvailabilityView {
    pub commit_id: String,
    pub data_health: String,
    pub verified_placements: String,
    pub missing_objects: String,
    pub verified_storage_volume_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitAvailabilityResponse {
    pub availability: CommitAvailabilityView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct WorkspaceView {
    pub workspace_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub base_commit_id: Option<String>,
    pub target_storage_volume_id: String,
    pub lifecycle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateWorkspaceResponse {
    pub workspace: WorkspaceView,
    pub replayed: bool,
}
