use std::{str::FromStr, sync::Arc};

use crate::{
    AuthorityStore, CentralError, CentralErrorCode, CentralResult, ControlCatalogRepository,
    ControlPlane, IndexPublisher, JobRecord, JobRepository, PreCommitRepository,
};
use neoengram_domain::core::{ContentDigest, LogicalPath};
use neoengram_domain::protocol::{JobFailureStage, JobState, PublishDecision, WireIndexVersion};

use crate::{
    dto::{
        CreateAddJobRequest, CreateAddJobResponse, FinalizeAddJobRequest, FinalizeAddJobResponse,
        IndexVersionBody, JobErrorView, JobView, PublicJobDecision, PublicJobFailure,
        PublicJobProgress, QueryJobRequest, QueryJobResponse,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::AuthenticatedIdentity,
    service::{coordinator::validate_job_spec, JobCoordinator},
};

/// Managed Add application service that maps public DTOs to the control plane.
pub struct JobService {
    control: Arc<ControlPlane>,
    jobs: Arc<dyn JobRepository>,
    catalog: Arc<dyn ControlCatalogRepository>,
    indexes: Arc<dyn IndexPublisher>,
    precommits: Option<Arc<dyn PreCommitRepository>>,
    coordinator: Option<Arc<JobCoordinator>>,
}

impl JobService {
    /// Creates a service whose public create path always validates authority scope.
    pub fn from_authority(
        control: Arc<ControlPlane>,
        authority: &AuthorityStore,
    ) -> CentralResult<Self> {
        let catalog = authority.control_catalog().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no control catalog composition",
            )
        })?;
        Ok(Self {
            control,
            jobs: authority.jobs(),
            catalog,
            indexes: authority.publisher(),
            precommits: authority.precommits(),
            coordinator: None,
        })
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
        self.control
            .preauthorize_create_add_job(identity.principal(), &spec)
            .await
            .map_err(map_central_error)?;
        let existing = self
            .jobs
            .get(&crate::JobKey::new(
                spec.tenant_id.clone(),
                spec.job_id.clone(),
            ))
            .await
            .map_err(map_central_error)?;
        if existing.is_none() {
            if let Some(precommits) = &self.precommits {
                if precommits
                    .get_active(
                        &spec.tenant_id,
                        &spec.project_id,
                        &spec.artifact_id,
                        &spec.playground_id,
                    )
                    .await
                    .map_err(map_central_error)?
                    .is_some()
                {
                    return Err(application_error(
                        fusen_rs::ErrorCategory::Conflict,
                        "precommit_already_active",
                        "PRECOMMIT_ALREADY_ACTIVE",
                        "the Playground has an active Pre-commit",
                        false,
                    ));
                }
            }
        }
        match &self.coordinator {
            Some(coordinator) => coordinator
                .validate_spec(&spec)
                .await
                .map_err(map_central_error)?,
            None => validate_job_spec(
                self.jobs.as_ref(),
                self.catalog.as_ref(),
                self.indexes.as_ref(),
                &spec,
            )
            .await
            .map_err(map_central_error)?,
        }
        let result = self
            .control
            .create_add_job(crate::CreateAddJobRequest {
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
        let tenant_id = neoengram_domain::protocol::TenantId::new(request.tenant_id)
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let job_id = neoengram_domain::protocol::JobId::new(request.job_id)
            .map_err(|error| invalid_request(format!("job_id: {error}")))?;
        let result = self
            .control
            .query_job(crate::QueryJobRequest {
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
        let tenant_id = neoengram_domain::protocol::TenantId::new(request.tenant_id)
            .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
        let job_id = neoengram_domain::protocol::JobId::new(request.job_id)
            .map_err(|error| invalid_request(format!("job_id: {error}")))?;
        let result = self
            .control
            .finalize_add(crate::FinalizeAddRequest {
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
    principal: &neoengram_domain::protocol::PrincipalRef,
) -> Result<crate::AddJobSpec, fusen_rs::Error> {
    let tenant_id = neoengram_domain::protocol::TenantId::new(request.tenant_id)
        .map_err(|error| invalid_request(format!("tenant_id: {error}")))?;
    let project_id = neoengram_domain::protocol::ProjectId::new(request.project_id)
        .map_err(|error| invalid_request(format!("project_id: {error}")))?;
    let artifact_id = neoengram_domain::protocol::ArtifactId::new(request.artifact_id)
        .map_err(|error| invalid_request(format!("artifact_id: {error}")))?;
    let playground_id = neoengram_domain::protocol::PlaygroundId::new(request.playground_id)
        .map_err(|error| invalid_request(format!("playground_id: {error}")))?;
    let job_id = neoengram_domain::protocol::JobId::new(request.job_id)
        .map_err(|error| invalid_request(format!("job_id: {error}")))?;
    let revision = parse_canonical_u64(
        "expected_index_version.revision",
        &request.expected_index_version.revision,
    )?;
    let digest = ContentDigest::from_str(&request.expected_index_version.digest).map_err(|_| {
        invalid_request("expected_index_version.digest must be a BLAKE3 hex digest")
    })?;
    let expected_index_version = WireIndexVersion {
        revision: neoengram_domain::protocol::IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    };
    let deadline_unix_ms = neoengram_domain::protocol::UnixMillis::new(parse_canonical_u64(
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
    let operation = neoengram_domain::protocol::AddOperation {
        job_id: job_id.clone(),
        principal: principal.clone(),
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
        expected_index_version: expected_index_version.clone(),
        data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
        deadline_unix_ms,
        paths: paths.clone(),
        all: request.all,
        extensions: neoengram_domain::protocol::Extensions::new(),
    };
    let request_digest = operation
        .request_digest()
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(crate::AddJobSpec {
        job_id,
        principal: principal.clone(),
        tenant_id,
        project_id,
        artifact_id,
        playground_id,
        expected_index_version,
        data_layout: neoengram_domain::protocol::CommitDataLayout::FastCdc,
        request_digest,
        deadline_unix_ms,
        paths,
        all: request.all,
        extensions: neoengram_domain::protocol::Extensions::new(),
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
    }
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

    fn request() -> CreateAddJobRequest {
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
        }
    }

    #[test]
    fn unknown_root_fields_are_rejected() {
        let mut value = serde_json::to_value(request()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future_option".to_owned(), serde_json::json!("one"));
        assert!(serde_json::from_value::<CreateAddJobRequest>(value).is_err());
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
}
