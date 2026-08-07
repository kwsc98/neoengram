use std::collections::BTreeMap;

use fusen_rs::{SensitiveFields, SensitiveShape};
use serde::{Deserialize, Serialize};

mod catalog;
pub use catalog::*;
mod commit;
pub use commit::*;
mod precommit;
pub use precommit::*;
mod playground;
pub use playground::*;
mod snapshot;
pub use snapshot::*;

/// Unknown root-level JSON members retained for forward-compatible digesting.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonExtensions(pub BTreeMap<String, serde_json::Value>);

impl SensitiveFields for JsonExtensions {
    fn sensitive_shape() -> SensitiveShape {
        SensitiveShape::Opaque
    }
}

impl JsonExtensions {
    /// Returns the first extension that attempts to shadow a server-owned field.
    pub fn first_reserved<'a>(&self, reserved: &'a [&str]) -> Option<&'a str> {
        reserved
            .iter()
            .copied()
            .find(|field| self.0.contains_key(*field))
    }

    /// Consumes the wrapper into protocol extensions.
    pub fn into_protocol(self) -> neoengram_protocol::Extensions {
        self.0
    }
}

/// Empty JSON request object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct EmptyRequest {}

/// Public index version representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct IndexVersionBody {
    #[sensitive(kind = "public")]
    pub revision: String,
    #[sensitive(kind = "identifier")]
    pub digest: String,
}

/// Request for creating a managed Add job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct CreateAddJobRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub job_id: String,
    pub expected_index_version: IndexVersionBody,
    pub deadline_unix_ms: String,
    pub paths: Vec<String>,
    pub all: bool,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

/// Request for querying a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct QueryJobRequest {
    #[sensitive(kind = "identifier")]
    pub tenant_id: String,
    #[sensitive(kind = "identifier")]
    pub job_id: String,
}

/// Request for finalizing a managed Add job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct FinalizeAddJobRequest {
    #[sensitive(kind = "identifier")]
    pub tenant_id: String,
    #[sensitive(kind = "identifier")]
    pub job_id: String,
}

/// Supported public API and agent protocol versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct ApiVersionResponse {
    #[sensitive(kind = "public")]
    pub api_versions: Vec<u32>,
    #[sensitive(kind = "public")]
    pub agent_protocol_versions: Vec<u32>,
    #[sensitive(kind = "public")]
    pub capabilities: Vec<String>,
}

/// Health probe response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct HealthStatus {
    #[sensitive(kind = "public")]
    pub status: String,
}

/// Create response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct CreateAddJobResponse {
    pub job: JobView,
    #[sensitive(kind = "public")]
    pub replayed: bool,
}

/// Query response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct QueryJobResponse {
    pub job: JobView,
}

/// Finalize response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct FinalizeAddJobResponse {
    pub job: JobView,
    pub decision: PublicJobDecision,
    #[sensitive(kind = "public")]
    pub finalized_at_unix_ms: String,
    #[sensitive(kind = "public")]
    pub replayed: bool,
}

/// Deliberately small public projection of an authoritative job record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(opaque)]
pub struct JobView {
    pub operation: String,
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub playground_id: String,
    pub job_id: String,
    pub state: String,
    pub resource_version: String,
    pub deadline_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<PublicJobProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<PublicJobDecision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<PublicJobFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_at_unix_ms: Option<String>,
    #[serde(default, flatten)]
    pub extensions: JsonExtensions,
}

/// Public progress projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
pub struct PublicJobProgress {
    #[sensitive(kind = "public")]
    pub state: String,
    #[sensitive(kind = "public")]
    pub phase: String,
    #[sensitive(kind = "public")]
    pub files_completed: String,
    #[sensitive(kind = "public")]
    pub bytes_completed: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sensitive(kind = "public")]
    pub retry_after_ms: Option<String>,
}

/// Stable public publication decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SensitiveFields)]
#[sensitive(kind = "enum")]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PublicJobDecision {
    Publish {
        final_state: String,
        published_index_version: IndexVersionBody,
    },
    Conflict {
        final_state: String,
        current_index_version: IndexVersionBody,
    },
    Reject {
        final_state: String,
        error: JobErrorView,
    },
}

/// Public terminal failure projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct PublicJobFailure {
    #[sensitive(kind = "public")]
    pub final_state: String,
    #[sensitive(kind = "public")]
    pub failed_at_unix_ms: String,
    #[sensitive(kind = "public")]
    pub stage: String,
    pub error: JobErrorView,
}

/// Public job error projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
pub struct JobErrorView {
    #[sensitive(kind = "public")]
    pub code: String,
    #[sensitive(kind = "public")]
    pub message: String,
    #[sensitive(kind = "public")]
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sensitive(kind = "public")]
    pub retry_after_ms: Option<String>,
}

/// Public request for freezing one PVC descriptor and issuing a bootstrap token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateStorageEnrollmentTokenRequest {
    pub tenant_id: String,
    pub token_request_id: String,
    pub storage_volume_id: String,
    pub display_name: String,
    pub edge_cluster_id: String,
    pub region: String,
    pub access_mode: String,
    pub pvc_reference: PvcReference,
}

/// PVC identity safe for public administrative views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct PvcReference {
    pub namespace: String,
    pub claim_name: String,
}

/// Token material returned only by the create operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateStorageEnrollmentTokenResponse {
    pub token_id: String,
    pub bootstrap_token: String,
    pub volume_descriptor_digest: String,
    pub expires_at_unix_ms: String,
    pub replayed: bool,
}

/// Tenant-scoped enrollment list request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageEnrollmentListRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub registration_kind: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
    #[serde(default)]
    pub query: Option<String>,
}

/// Enrollment list response with an opaque keyset cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageEnrollmentListResponse {
    pub items: Vec<StorageEnrollmentView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Tenant-scoped enrollment lookup request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageEnrollmentRequest {
    pub tenant_id: String,
    pub storage_enrollment_id: String,
}

/// Enrollment lookup response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryStorageEnrollmentResponse {
    pub enrollment: StorageEnrollmentView,
}

/// Public approval mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ApproveStorageEnrollmentRequest {
    pub tenant_id: String,
    pub storage_enrollment_id: String,
    pub approval_request_id: String,
    pub expected_resource_version: String,
    pub confirm_replacement: bool,
}

/// Approval result. Approval never implies an Agent session or Ready Volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ApproveStorageEnrollmentResponse {
    pub enrollment: StorageEnrollmentView,
    pub storage_volume: StorageVolumeView,
    pub replayed: bool,
}

/// Public rejection mutation. The optional reason is never included in a public response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RejectStorageEnrollmentRequest {
    pub tenant_id: String,
    pub storage_enrollment_id: String,
    pub rejection_request_id: String,
    pub expected_resource_version: String,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Rejection result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct RejectStorageEnrollmentResponse {
    pub enrollment: StorageEnrollmentView,
    pub replayed: bool,
}

/// Deliberately whitelisted public enrollment projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct StorageEnrollmentView {
    pub tenant_id: String,
    pub storage_enrollment_id: String,
    pub storage_volume_id: String,
    pub display_name: String,
    pub edge_cluster_id: String,
    pub region: String,
    pub access_mode: String,
    pub pvc_reference: PvcReference,
    pub registration_kind: String,
    pub state: String,
    pub agent_version: String,
    pub identity_fingerprint: String,
    pub proof_of_possession_status: String,
    pub probe: StorageEnrollmentProbeSummary,
    pub resource_version: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
    pub expires_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reviewed_at_unix_ms: Option<String>,
}

/// Sanitized bootstrap probe summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct StorageEnrollmentProbeSummary {
    pub observed_access_mode: String,
    pub descriptor_matches: bool,
    pub protocol_compatible: bool,
    pub observed_at_unix_ms: String,
}

/// Public Volume projection returned by enrollment approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct StorageVolumeView {
    pub tenant_id: String,
    pub storage_volume_id: String,
    pub display_name: String,
    pub edge_cluster_id: String,
    pub region: String,
    pub backend_type: String,
    pub access_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pvc_reference: Option<PvcReference>,
    pub state: String,
    pub resource_version: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_request_retains_extensions_at_the_root() {
        let value = serde_json::json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "artifact_id": "artifact-a",
            "playground_id": "playground-a",
            "job_id": "job-a",
            "expected_index_version": { "revision": "0", "digest": "0".repeat(64) },
            "deadline_unix_ms": "2000000000000",
            "paths": ["dataset/images"],
            "all": false,
            "future_option": { "mode": "strict" }
        });
        let request: CreateAddJobRequest = serde_json::from_value(value).unwrap();
        assert_eq!(
            request.extensions.0.get("future_option"),
            Some(&serde_json::json!({ "mode": "strict" }))
        );
        let encoded = serde_json::to_value(request).unwrap();
        assert!(encoded.get("future_option").is_some());
        assert!(encoded.get("extensions").is_none());
    }

    #[test]
    fn closed_requests_reject_unknown_members() {
        let result = serde_json::from_value::<QueryJobRequest>(serde_json::json!({
            "tenant_id": "tenant-a",
            "job_id": "job-a",
            "principal": "forged"
        }));
        assert!(result.is_err());
    }
}
