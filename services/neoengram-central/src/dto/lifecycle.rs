use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ResourceLifecycleView {
    pub state: String,
    pub generation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deletion_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_requested_at_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_after_unix_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at_unix_ms: Option<String>,
}

/// Public tagged resource identity used by lifecycle mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[sensitive(kind = "enum")]
pub enum ResourceRefBody {
    StorageVolume {
        storage_volume_id: String,
    },
    Artifact {
        project_id: String,
        artifact_id: String,
    },
    Playground {
        project_id: String,
        artifact_id: String,
        playground_id: String,
    },
    Snapshot {
        snapshot_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeletionTargetView {
    pub resource: ResourceRefBody,
    pub resource_version: String,
    pub lifecycle_generation: String,
    pub requires_agent_cleanup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeletionBlockerView {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceRefBody>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeletionImpactView {
    pub tenant_id: String,
    pub root: ResourceRefBody,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub targets: Vec<DeletionTargetView>,
    pub active_job_count: String,
    pub active_s3_credential_count: String,
    pub estimated_file_count: String,
    pub estimated_bytes: String,
    pub blockers: Vec<DeletionBlockerView>,
    pub issued_at_unix_ms: String,
    pub expires_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeletionOperationView {
    pub deletion_id: String,
    pub tenant_id: String,
    pub root: ResourceRefBody,
    pub state: String,
    pub resource_version: String,
    pub targets: Vec<DeletionTargetView>,
    pub request_id: String,
    pub request_digest: String,
    pub impact_digest: String,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub purge_after_unix_ms: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_state: Option<String>,
    pub retry_count: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RetentionHoldView {
    pub retention_hold_id: String,
    pub tenant_id: String,
    pub deletion_id: String,
    pub reason: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<String>,
    pub created_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub released_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionImpactRequest {
    pub tenant_id: String,
    pub resource: ResourceRefBody,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub expected_resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionImpactResponse {
    pub impact: DeletionImpactView,
    pub impact_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateDeletionRequest {
    pub tenant_id: String,
    pub resource: ResourceRefBody,
    pub cascade: bool,
    pub confirm_managed_data_erase: bool,
    pub expected_resource_version: String,
    pub impact_digest: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionRequest {
    pub tenant_id: String,
    pub deletion_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionResponse {
    pub deletion: DeletionOperationView,
    pub retention_holds: Vec<RetentionHoldView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub states: Option<Vec<String>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryDeletionListResponse {
    pub items: Vec<DeletionOperationView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct UpdateDeletionRequest {
    pub tenant_id: String,
    pub deletion_id: String,
    pub request_id: String,
    pub expected_resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateRetentionHoldRequest {
    pub tenant_id: String,
    pub deletion_id: String,
    pub request_id: String,
    pub expected_resource_version: String,
    pub reason: String,
    #[serde(default)]
    pub expires_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ReleaseRetentionHoldRequest {
    pub tenant_id: String,
    pub deletion_id: String,
    pub retention_hold_id: String,
    pub request_id: String,
    pub expected_resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DeletionMutationResponse {
    pub deletion: DeletionOperationView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateRetentionHoldResponse {
    pub deletion: DeletionOperationView,
    pub retention_hold: RetentionHoldView,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ReleaseRetentionHoldResponse {
    pub deletion: DeletionOperationView,
    pub retention_hold: RetentionHoldView,
    pub replayed: bool,
}
