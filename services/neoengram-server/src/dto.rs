use fusen_rs::{SensitiveFields, SensitiveShape};
use serde::{Deserialize, Serialize};

// ── Extension wrapper ───────────────────────────────────────────────────

/// JSON extension fields captured as an opaque object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonExtensions {
    #[serde(flatten)]
    fields: serde_json::Value,
}

impl SensitiveFields for JsonExtensions {
    fn sensitive_shape() -> SensitiveShape {
        SensitiveShape::Opaque
    }
}

impl Default for JsonExtensions {
    fn default() -> Self {
        Self {
            fields: serde_json::Value::Object(Default::default()),
        }
    }
}

impl JsonExtensions {
    /// Returns the extension entries that are not in the reserved field set.
    pub fn non_reserved_entries(
        &self,
        reserved: &[&str],
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        let mut result = std::collections::BTreeMap::new();
        if let serde_json::Value::Object(map) = &self.fields {
            for (k, v) in map.iter() {
                if !reserved.contains(&k.as_str()) {
                    result.insert(k.clone(), v.clone());
                }
            }
        }
        result
    }

    /// Returns true if any reserved field name is present.
    pub fn contains_any_reserved(&self, reserved: &[&str]) -> Option<String> {
        if let serde_json::Value::Object(map) = &self.fields {
            for key in reserved {
                if map.contains_key(*key) {
                    return Some((*key).to_string());
                }
            }
        }
        None
    }
}

// ── Request DTOs ───────────────────────────────────────────────────────

/// Empty POST body for `/api/system/version/query`.
#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct EmptyRequest {}

/// POST body for `/api/job/add/create`.
#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
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
    #[serde(default)]
    pub extensions: JsonExtensions,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct IndexVersionBody {
    pub revision: String,
    pub digest: String,
}

/// POST body for `/api/job/query`.
#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct QueryJobRequest {
    pub tenant_id: String,
    pub job_id: String,
}

/// POST body for `/api/job/add/finalize`.
#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct FinalizeAddJobRequest {
    pub tenant_id: String,
    pub job_id: String,
}

// ── Response DTOs ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct ApiVersionResponse {
    pub api_versions: Vec<u32>,
    pub agent_protocol_versions: Vec<u32>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct CreateAddJobResponseBody {
    pub job: JobView,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct QueryJobResponseBody {
    pub job: JobView,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct FinalizeAddJobResponseBody {
    pub job: JobView,
    pub decision: PublicJobDecision,
    pub finalized_at_unix_ms: String,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct PublicJobProgress {
    pub state: String,
    pub phase: String,
    pub files_completed: String,
    pub bytes_completed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
#[sensitive(kind = "enum")]
#[serde(tag = "outcome")]
pub enum PublicJobDecision {
    #[serde(rename = "publish")]
    Publish {
        final_state: String,
        published_index_version: IndexVersionResponse,
    },
    #[serde(rename = "conflict")]
    Conflict {
        final_state: String,
        current_index_version: IndexVersionResponse,
    },
    #[serde(rename = "reject")]
    Reject {
        final_state: String,
        error: JobErrorView,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct IndexVersionResponse {
    pub revision: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct PublicJobFailure {
    pub final_state: String,
    pub failed_at_unix_ms: String,
    pub stage: String,
    pub error: JobErrorView,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct JobErrorView {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, SensitiveFields)]
pub struct HealthStatus {
    pub status: String,
}
