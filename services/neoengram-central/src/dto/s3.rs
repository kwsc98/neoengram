use serde::{Deserialize, Serialize};

use neoengram_domain::protocol::{S3AuthorizeRequest, S3AuthorizeResponse};

use super::TaskView;

/// Transport wrapper used only by the private Gateway authorization interface. `transparent`
/// preserves the shared protocol wire shape while keeping Fusen-specific metadata out of the
/// transport-neutral protocol crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(transparent)]
#[sensitive(opaque)]
pub struct InternalS3AuthorizeRequest(pub S3AuthorizeRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(transparent)]
#[sensitive(opaque)]
pub struct InternalS3AuthorizeResponse(pub S3AuthorizeResponse);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateS3AccessPointRequest {
    pub tenant_id: String,
    pub snapshot_id: String,
    pub bucket_name: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3AccessPointListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3AccessPointRequest {
    pub tenant_id: String,
    pub access_point_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct UpdateS3AccessPointRequest {
    pub tenant_id: String,
    pub access_point_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateS3CredentialRequest {
    pub tenant_id: String,
    pub access_point_id: String,
    pub request_id: String,
    #[serde(default)]
    pub expires_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3CredentialListRequest {
    pub tenant_id: String,
    pub access_point_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct RevokeS3CredentialRequest {
    pub tenant_id: String,
    pub access_point_id: String,
    pub credential_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3ObjectListRequest {
    pub tenant_id: String,
    pub access_point_id: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateS3DownloadUrlRequest {
    pub tenant_id: String,
    pub access_point_id: String,
    pub key: String,
    #[serde(default)]
    pub expires_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct S3AccessPointView {
    pub access_point_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub snapshot_id: String,
    pub commit_id: String,
    pub delivery_id: String,
    pub storage_volume_id: String,
    pub edge_cluster_id: String,
    pub bucket_name: String,
    pub endpoint: String,
    pub region: String,
    pub state: String,
    pub policy_generation: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct S3CredentialView {
    pub credential_id: String,
    pub access_point_id: String,
    pub access_key_id: String,
    pub state: String,
    pub expires_at_unix_ms: String,
    pub created_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct S3ObjectEntryView {
    pub key: String,
    pub entry_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified_unix_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3AccessPointListResponse {
    pub items: Vec<S3AccessPointView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3AccessPointResponse {
    pub access_point: S3AccessPointView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateS3AccessPointResponse {
    pub access_point: S3AccessPointView,
    pub access_key_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secret_access_key: String,
    pub credential_expires_at_unix_ms: String,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct UpdateS3AccessPointResponse {
    pub access_point: S3AccessPointView,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateS3CredentialResponse {
    pub credential: S3CredentialView,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secret_access_key: String,
    pub request_replayed: bool,
    pub execution_reused: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3CredentialListResponse {
    pub items: Vec<S3CredentialView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct QueryS3ObjectListResponse {
    pub items: Vec<S3ObjectEntryView>,
    #[serde(default)]
    pub common_prefixes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, fusen_rs::SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateS3DownloadUrlResponse {
    pub url: String,
    pub expires_at_unix_ms: String,
}
