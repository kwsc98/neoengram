use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::ResourceIssueSummary;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateCommitReplicationRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    pub commit_id: String,
    pub target_storage_volume_id: String,
    /// Monotonic attempt fence required by Retry/Cancel mutations.
    pub attempt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_placement_set_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_storage_volume_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_edge_cluster_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_gateway_pool_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_session_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_mount_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_route_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_edge_cluster_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_gateway_pool_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_session_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_mount_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_route_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_route_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_placement_set_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_id: Option<String>,
    pub state: String,
    pub object_set_digest: String,
    pub completed_objects: String,
    pub total_objects: String,
    pub completed_bytes: String,
    pub total_bytes: String,
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
pub struct QueryCommitReplicationTicketRequest {
    pub tenant_id: String,
    pub replication_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitReplicationTicketResponse {
    /// The unsigned form is retained for local deterministic adapters. Production data-plane
    /// clients must use `signed_ticket` and reject a response that has no signature.
    pub ticket: neoengram_domain::protocol::TransferTicket,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_ticket: Option<neoengram_domain::protocol::SignedTransferTicket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetryCommitReplicationRequest {
    pub tenant_id: String,
    pub replication_id: String,
    pub expected_attempt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetryCommitReplicationResponse {
    pub replication: ReplicationView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CancelCommitReplicationRequest {
    pub tenant_id: String,
    pub replication_id: String,
    pub expected_attempt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CancelCommitReplicationResponse {
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
pub struct QueryCommitReplicationListResponse {
    pub replications: Vec<ReplicationView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitPlacementListRequest {
    pub tenant_id: String,
    pub commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CommitPlacementView {
    pub placement_set_id: String,
    pub commit_id: String,
    pub backend_id: String,
    pub storage_volume_id: Option<String>,
    pub object_set_digest: String,
    pub object_count: String,
    pub verified_object_count: String,
    pub placement_generation: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryCommitPlacementListResponse {
    pub placements: Vec<CommitPlacementView>,
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
