use std::{str::FromStr, sync::Arc};

use neoengram_core::{ContentDigest, LogicalPath};
use neoengram_protocol::{JobFailureStage, JobState, PublishDecision, WireIndexVersion};
use neoengramd::{ControlPlane, JobRecord};

use crate::{
    dto::{
        CreateAddJobRequest, CreateAddJobResponse, FinalizeAddJobRequest, FinalizeAddJobResponse,
        IndexVersionBody, JobErrorView, JobView, PublicJobDecision, PublicJobFailure,
        PublicJobProgress, QueryJobRequest, QueryJobResponse,
    },
    error::{invalid_request, map_central_error},
    identity::AuthenticatedIdentity,
    service::JobCoordinator,
};

const RESERVED_ADD_EXTENSIONS: &[&str] = &["actor", "principal", "request_digest"];

const JOB_VIEW_BLOCKED_EXTENSIONS: &[&str] = &[
    "accepted",
    "actor",
    "agent_id",
    "agent_mount_id",
    "artifact_placement_id",
    "assignment",
    "assignment_generation",
    "assignment_id",
    "assignment_target",
    "artifact_id",
    "decision",
    "decision_generation",
    "deadline_unix_ms",
    "edge_cluster_id",
    "failure",
    "fencing",
    "fencing_token",
    "finalized_ack",
    "finalized_at_unix_ms",
    "generation",
    "index_delta",
    "job_id",
    "lease",
    "manifest",
    "manifests",
    "mount_generation",
    "mutations",
    "operation",
    "owner_generation",
    "placement_generation",
    "playground_id",
    "prepared",
    "principal",
    "progress",
    "project_id",
    "publication_candidate",
    "request_digest",
    "resource_version",
    "resume_publication",
    "state",
    "storage_volume_id",
    "tenant_id",
];

/// Managed Add application service that maps public DTOs to the control plane.
pub struct JobService {
    control: Arc<ControlPlane>,
    coordinator: Option<Arc<JobCoordinator>>,
}

impl JobService {
    /// Creates the service from explicit domain and authorization ports.
    pub fn new(control: Arc<ControlPlane>) -> Self {
        Self {
            control,
            coordinator: None,
        }
    }

    #[must_use]
    pub fn with_coordinator(mut self, coordinator: Arc<JobCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
    }

    /// Creates or idempotently loads a managed Add job.
    pub async fn create_add_job(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateAddJobRequest,
    ) -> Result<CreateAddJobResponse, fusen_rs::Error> {
        let spec = build_add_job_spec(request, identity.principal())?;
        if let Some(coordinator) = &self.coordinator {
            coordinator
                .validate_spec(&spec)
                .await
                .map_err(map_central_error)?;
        }
        let result = self
            .control
            .create_add_job(neoengramd::CreateAddJobRequest {
                actor: identity.principal().clone(),
                spec,
            })
            .await
            .map_err(map_central_error)?;
        let mut job = result.job;
        if let Some(coordinator) = &self.coordinator {
            match coordinator.schedule(&job).await {
                Ok(Some(assigned)) => job = assigned,
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    job_id = %job.spec.job_id,
                    error = %error,
                    "immediate Job scheduling failed; recovery loop will retry"
                ),
            }
        }
        Ok(CreateAddJobResponse {
            job: job_record_to_view(&job),
            replayed: result.replayed,
        })
    }

    /// Loads a job through the control plane's non-disclosing authorization boundary.
    pub async fn query_job(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryJobRequest,
    ) -> Result<QueryJobResponse, fusen_rs::Error> {
        let tenant_id = neoengram_protocol::TenantId::new(request.tenant_id)
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let job_id = neoengram_protocol::JobId::new(request.job_id)
            .map_err(|error| invalid_request(format!("job_id: {error}")))?;
        let result = self
            .control
            .query_job(neoengramd::QueryJobRequest {
                actor: identity.principal().clone(),
                tenant_id,
                job_id,
            })
            .await
            .map_err(map_central_error)?;
        Ok(QueryJobResponse {
            job: job_record_to_view(&result.job),
        })
    }

    /// Finalizes a prepared managed Add job.
    pub async fn finalize_add_job(
        &self,
        identity: &AuthenticatedIdentity,
        request: FinalizeAddJobRequest,
    ) -> Result<FinalizeAddJobResponse, fusen_rs::Error> {
        let tenant_id = neoengram_protocol::TenantId::new(request.tenant_id)
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let job_id = neoengram_protocol::JobId::new(request.job_id)
            .map_err(|error| invalid_request(format!("job_id: {error}")))?;
        let result = self
            .control
            .finalize_add(neoengramd::FinalizeAddRequest {
                actor: identity.principal().clone(),
                tenant_id,
                job_id,
            })
            .await
            .map_err(map_central_error)?;
        let decision = map_decision(&result.decision.decision, result.decision.final_state);
        Ok(FinalizeAddJobResponse {
            job: job_record_to_view(&result.job),
            decision,
            finalized_at_unix_ms: result.finalized.finalized_at_unix_ms.to_string(),
            replayed: result.replayed,
        })
    }
}

fn build_add_job_spec(
    request: CreateAddJobRequest,
    principal: &neoengram_protocol::PrincipalRef,
) -> Result<neoengramd::AddJobSpec, fusen_rs::Error> {
    if let Some(field) = request.extensions.first_reserved(RESERVED_ADD_EXTENSIONS) {
        return Err(invalid_request(format!(
            "request body must not contain server-owned field {field:?}"
        )));
    }
    let tenant_id = neoengram_protocol::TenantId::new(request.tenant_id)
        .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
    let project_id = neoengram_protocol::ProjectId::new(request.project_id)
        .map_err(|error| invalid_request(format!("project_id: {error}")))?;
    let artifact_id = neoengram_protocol::ArtifactId::new(request.artifact_id)
        .map_err(|error| invalid_request(format!("artifact_id: {error}")))?;
    let playground_id = neoengram_protocol::PlaygroundId::new(request.playground_id)
        .map_err(|error| invalid_request(format!("playground_id: {error}")))?;
    let job_id = neoengram_protocol::JobId::new(request.job_id)
        .map_err(|error| invalid_request(format!("job_id: {error}")))?;
    let revision = parse_canonical_u64(
        "expected_index_version.revision",
        &request.expected_index_version.revision,
    )?;
    let digest = ContentDigest::from_str(&request.expected_index_version.digest).map_err(|_| {
        invalid_request("expected_index_version.digest must be a BLAKE3 hex digest")
    })?;
    let expected_index_version = WireIndexVersion {
        revision: neoengram_protocol::IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    };
    let deadline_unix_ms = neoengram_protocol::UnixMillis::new(parse_canonical_u64(
        "deadline_unix_ms",
        &request.deadline_unix_ms,
    )?);
    let paths = request
        .paths
        .iter()
        .map(|path| {
            LogicalPath::parse(path)
                .map_err(|error| invalid_request(format!("invalid path {path:?}: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let extensions = request.extensions.into_protocol();
    let operation = neoengram_protocol::AddOperation {
        job_id: job_id.clone(),
        principal: principal.clone(),
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
        expected_index_version: expected_index_version.clone(),
        deadline_unix_ms,
        paths: paths.clone(),
        all: request.all,
        extensions: extensions.clone(),
    };
    let request_digest = operation
        .request_digest()
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(neoengramd::AddJobSpec {
        job_id,
        principal: principal.clone(),
        tenant_id,
        project_id,
        artifact_id,
        playground_id,
        expected_index_version,
        request_digest,
        deadline_unix_ms,
        paths,
        all: request.all,
        extensions,
    })
}

fn parse_canonical_u64(field: &'static str, value: &str) -> Result<u64, fusen_rs::Error> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| invalid_request(format!("{field} must be a canonical unsigned integer")))?;
    if parsed.to_string() != value {
        return Err(invalid_request(format!(
            "{field} must be a canonical unsigned integer"
        )));
    }
    Ok(parsed)
}

fn job_record_to_view(job: &JobRecord) -> JobView {
    JobView {
        operation: "add".to_owned(),
        tenant_id: job.spec.tenant_id.to_string(),
        project_id: job.spec.project_id.to_string(),
        artifact_id: job.spec.artifact_id.to_string(),
        playground_id: job.spec.playground_id.to_string(),
        job_id: job.spec.job_id.to_string(),
        state: job_state(job.state),
        resource_version: job.resource_version.get().to_string(),
        deadline_unix_ms: job.spec.deadline_unix_ms.to_string(),
        progress: job.progress.as_ref().map(|progress| PublicJobProgress {
            state: job_state(progress.state),
            phase: progress.phase.clone(),
            files_completed: progress.files_completed.to_string(),
            bytes_completed: progress.bytes_completed.to_string(),
            retry_after_ms: progress.retry_after_ms.as_ref().map(ToString::to_string),
        }),
        decision: job
            .decision
            .as_ref()
            .map(|decision| map_decision(&decision.decision, decision.final_state)),
        failure: job.failure.as_ref().map(|failure| PublicJobFailure {
            final_state: job_state(failure.final_state),
            failed_at_unix_ms: failure.failed_at_unix_ms.to_string(),
            stage: job_failure_stage(failure.stage).to_owned(),
            error: JobErrorView {
                code: failure.error.code.as_str().to_owned(),
                message: failure.error.message.clone(),
                retryable: failure.error.retryable,
                retry_after_ms: failure
                    .error
                    .retry_after_ms
                    .as_ref()
                    .map(ToString::to_string),
            },
        }),
        finalized_at_unix_ms: job
            .finalized
            .as_ref()
            .map(|finalized| finalized.finalized_at_unix_ms.to_string()),
        extensions: public_job_extensions(&job.spec.extensions),
    }
}

fn public_job_extensions(
    extensions: &neoengram_protocol::Extensions,
) -> crate::dto::JsonExtensions {
    let values = extensions
        .iter()
        .filter(|(name, _)| !JOB_VIEW_BLOCKED_EXTENSIONS.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    crate::dto::JsonExtensions(values)
}

fn job_state(state: JobState) -> String {
    match state {
        JobState::Queued => "queued",
        JobState::Assigned => "assigned",
        JobState::Accepted => "accepted",
        JobState::Running => "running",
        JobState::Prepared => "prepared",
        JobState::Publishing => "publishing",
        JobState::CancelRequested => "cancel_requested",
        JobState::Succeeded => "succeeded",
        JobState::Conflicted => "conflicted",
        JobState::Rejected => "rejected",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::TimedOut => "timed_out",
        JobState::RecoveryRequired => "recovery_required",
        JobState::Unknown => "unknown",
    }
    .to_owned()
}

fn job_failure_stage(stage: JobFailureStage) -> &'static str {
    match stage {
        JobFailureStage::Execution => "execution",
        JobFailureStage::ObjectTransfer => "object_transfer",
        JobFailureStage::Reporting => "reporting",
        JobFailureStage::Finalization => "finalization",
    }
}

fn map_decision(decision: &PublishDecision, final_state: JobState) -> PublicJobDecision {
    match decision {
        PublishDecision::Publish {
            published_index_version,
            ..
        } => PublicJobDecision::Publish {
            final_state: job_state(final_state),
            published_index_version: IndexVersionBody {
                revision: published_index_version.revision.get().to_string(),
                digest: published_index_version.digest.to_string(),
            },
        },
        PublishDecision::Conflict {
            current_index_version,
            ..
        } => PublicJobDecision::Conflict {
            final_state: job_state(final_state),
            current_index_version: IndexVersionBody {
                revision: current_index_version.revision.get().to_string(),
                digest: current_index_version.digest.to_string(),
            },
        },
        PublishDecision::Reject { error, .. } => PublicJobDecision::Reject {
            final_state: job_state(final_state),
            error: JobErrorView {
                code: error.code.as_str().to_owned(),
                message: error.message.clone(),
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms.as_ref().map(ToString::to_string),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AuthenticatedIdentity;
    use neoengram_protocol::{PrincipalKind, PrincipalRef};
    use std::collections::BTreeMap;

    fn principal() -> PrincipalRef {
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject")
            .unwrap()
            .principal()
            .clone()
    }

    fn request(extension_value: &str) -> CreateAddJobRequest {
        CreateAddJobRequest {
            tenant_id: "tenant-a".into(),
            project_id: "project-a".into(),
            artifact_id: "artifact-a".into(),
            playground_id: "playground-a".into(),
            job_id: "job-a".into(),
            expected_index_version: IndexVersionBody {
                revision: "0".into(),
                digest: "0".repeat(64),
            },
            deadline_unix_ms: "2000000000000".into(),
            paths: vec!["dataset/images".into()],
            all: false,
            extensions: crate::dto::JsonExtensions(BTreeMap::from([(
                "future_option".into(),
                serde_json::Value::String(extension_value.into()),
            )])),
        }
    }

    #[test]
    fn root_extensions_are_retained_and_bound_by_digest() {
        let first = build_add_job_spec(request("one"), &principal()).unwrap();
        let second = build_add_job_spec(request("two"), &principal()).unwrap();
        assert_eq!(
            first.extensions.get("future_option"),
            Some(&serde_json::Value::String("one".into()))
        );
        assert_ne!(first.request_digest, second.request_digest);
    }

    #[test]
    fn canonical_numbers_reject_leading_zeroes() {
        assert!(parse_canonical_u64("revision", "00").is_err());
        assert_eq!(parse_canonical_u64("revision", "0").unwrap(), 0);
    }

    #[test]
    fn multi_word_wire_enums_use_snake_case() {
        assert_eq!(job_state(JobState::TimedOut), "timed_out");
        assert_eq!(job_state(JobState::RecoveryRequired), "recovery_required");
        assert_eq!(
            job_failure_stage(JobFailureStage::ObjectTransfer),
            "object_transfer"
        );
    }

    #[test]
    fn public_job_extensions_keep_safe_fields_and_filter_server_fields() {
        let extensions = BTreeMap::from([
            ("future_option".to_owned(), serde_json::json!("visible")),
            ("assignment".to_owned(), serde_json::json!({"forged": true})),
            ("state".to_owned(), serde_json::json!("forged")),
        ]);
        let public = public_job_extensions(&extensions);
        assert_eq!(
            public.0.get("future_option"),
            Some(&serde_json::json!("visible"))
        );
        assert!(!public.0.contains_key("assignment"));
        assert!(!public.0.contains_key("state"));
    }
}
