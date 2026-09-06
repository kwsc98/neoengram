use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

use super::{
    IndexVersionBody, PvcReference, ResourceIssueSummary, ResourceLifecycleView, StorageVolumeView,
    TaskView,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTenantListRequest {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTenantListResponse {
    pub items: Vec<TenantView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub can_create_tenant: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTenantRequest {
    pub tenant_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryTenantResponse {
    pub tenant: TenantView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateTenantRequest {
    pub tenant_id: String,
    pub display_name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateTenantResponse {
    pub tenant: TenantView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct TenantView {
    pub tenant_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub resource_version: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryProjectListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryProjectListResponse {
    pub items: Vec<ProjectView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateProjectRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub display_name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateProjectResponse {
    pub project: ProjectView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ProjectView {
    pub tenant_id: String,
    pub project_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub resource_version: String,
    pub lifecycle: ResourceLifecycleView,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageVolumeListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub backend_type: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageVolumeListResponse {
    pub items: Vec<StorageVolumeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageVolumeRequest {
    pub tenant_id: String,
    pub storage_volume_id: String,
    /// Optional Snapshot scope for a read-only caller. When present, Central only permits
    /// querying the immutable StorageVolume bound to that Snapshot and returns the same
    /// sanitized projection as the volume list action.
    #[serde(default)]
    pub snapshot_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageVolumeResponse {
    pub storage_volume: StorageVolumeView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateStorageVolumeRequest {
    pub tenant_id: String,
    pub storage_volume_id: String,
    pub display_name: String,
    pub edge_cluster_id: String,
    pub region: String,
    pub backend_type: String,
    pub access_mode: String,
    #[serde(default)]
    pub allowed_delivery_modes: Vec<String>,
    #[serde(default)]
    pub hardlink_policy: Option<String>,
    #[serde(default)]
    pub max_whole_file_bytes: Option<String>,
    #[serde(default)]
    pub copy_reserve_bytes: Option<String>,
    #[serde(default)]
    pub pvc_reference: Option<PvcReference>,
    #[serde(default)]
    pub nfs_reference: Option<NfsReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct NfsReference {
    pub server: String,
    pub export_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateStorageVolumeResponse {
    pub storage_volume: StorageVolumeView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactListResponse {
    pub items: Vec<ArtifactView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryArtifactResponse {
    pub artifact: ArtifactView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateArtifactRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub display_name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub initialization: ArtifactInitialization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[sensitive(kind = "enum")]
pub enum ArtifactInitialization {
    Empty,
    Derived {
        source_project_id: String,
        source_artifact_id: String,
        source_commit_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateArtifactResponse {
    pub artifact: ArtifactView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ArtifactView {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub initialization: ArtifactInitialization,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<String>,
    pub resource_version: String,
    pub lifecycle: ResourceLifecycleView,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryWorkspaceListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryWorkspaceListResponse {
    pub items: Vec<WorkspaceView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryWorkspaceRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    #[serde(rename = "workspace_id")]
    pub workspace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryWorkspaceResponse {
    #[serde(rename = "workspace")]
    pub workspace: WorkspaceView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateWorkspaceRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    #[serde(rename = "workspace_id")]
    pub workspace_id: String,
    pub storage_volume_id: String,
    pub display_name: String,
    #[serde(default)]
    pub base_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateWorkspaceResponse {
    #[serde(rename = "workspace")]
    pub workspace: WorkspaceView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct WorkspaceView {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    #[serde(rename = "workspace_id")]
    pub workspace_id: String,
    pub storage_volume_id: String,
    pub region: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<String>,
    pub index_version: IndexVersionBody,
    pub state: String,
    pub storage_availability: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_precommit_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<ResourceIssueSummary>,
    pub resource_version: String,
    pub lifecycle: ResourceLifecycleView,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}
