use std::{str::FromStr, sync::Arc};

use fusen_rs::{interface, Error, ErrorCategory, Response};
use neoengram_core::{ContentDigest, LogicalPath};
use neoengram_protocol::{Extensions, JobState, PublishDecision, WireIndexVersion};

use crate::{
    app_state::AppState,
    dto::{
        CreateAddJobRequest, CreateAddJobResponseBody, FinalizeAddJobRequest,
        FinalizeAddJobResponseBody, IndexVersionResponse, JobErrorView, JobView, PublicJobDecision,
        PublicJobFailure, PublicJobProgress, QueryJobRequest, QueryJobResponseBody,
    },
    error::map_central_error,
};

const RESERVED_REQUEST_FIELDS: &[&str] = &["actor", "principal", "request_digest"];

/// Managed Add job endpoints.
#[interface(name = "neoengram.job", group = "api", version = "1")]
pub trait JobApi {
    /// Creates a new managed Add job and persists it in the Queued state.
    #[fusen_rs::method(method = "POST", path = "/api/job/add/create")]
    async fn create_add_job(
        &self,
        #[param(body)] request: CreateAddJobRequest,
    ) -> Result<Response<CreateAddJobResponseBody>, Error>;

    /// Returns the current public view of a managed Add job.
    #[fusen_rs::method(method = "POST", path = "/api/job/query")]
    async fn query_job(
        &self,
        #[param(body)] request: QueryJobRequest,
    ) -> Result<Response<QueryJobResponseBody>, Error>;

    /// Finalizes a prepared job with a CAS index publication.
    #[fusen_rs::method(method = "POST", path = "/api/job/add/finalize")]
    async fn finalize_add_job(
        &self,
        #[param(body)] request: FinalizeAddJobRequest,
    ) -> Result<Response<FinalizeAddJobResponseBody>, Error>;
}

pub struct JobApiImpl {
    pub state: Arc<AppState>,
}

impl JobApi for JobApiImpl {
    async fn create_add_job(
        &self,
        request: CreateAddJobRequest,
    ) -> Result<Response<CreateAddJobResponseBody>, Error> {
        validate_extensions(&request.extensions)?;
        let spec = build_add_job_spec(&request, &self.state.dev_principal)?;
        let add_request = neoengramd::CreateAddJobRequest {
            actor: self.state.dev_principal.clone(),
            spec,
        };
        let result = self
            .state
            .control
            .create_add_job(add_request)
            .await
            .map_err(map_central_error)?;
        Ok(Response::new(CreateAddJobResponseBody {
            job: job_record_to_view(&result.job),
            replayed: result.replayed,
        }))
    }

    async fn query_job(
        &self,
        request: QueryJobRequest,
    ) -> Result<Response<QueryJobResponseBody>, Error> {
        let tenant_id = neoengram_protocol::TenantId::new(request.tenant_id)
            .map_err(|e| invalid_arg(format!("tenant_id: {e}")))?;
        let job_id = neoengram_protocol::JobId::new(request.job_id)
            .map_err(|e| invalid_arg(format!("job_id: {e}")))?;
        let query_request = neoengramd::QueryJobRequest {
            actor: self.state.dev_principal.clone(),
            tenant_id,
            job_id,
        };
        let result = self
            .state
            .control
            .query_job(query_request)
            .await
            .map_err(map_central_error)?;
        Ok(Response::new(QueryJobResponseBody {
            job: job_record_to_view(&result.job),
        }))
    }

    async fn finalize_add_job(
        &self,
        request: FinalizeAddJobRequest,
    ) -> Result<Response<FinalizeAddJobResponseBody>, Error> {
        let tenant_id = neoengram_protocol::TenantId::new(request.tenant_id)
            .map_err(|e| invalid_arg(format!("tenant_id: {e}")))?;
        let job_id = neoengram_protocol::JobId::new(request.job_id)
            .map_err(|e| invalid_arg(format!("job_id: {e}")))?;
        let finalize_request = neoengramd::FinalizeAddRequest {
            actor: self.state.dev_principal.clone(),
            tenant_id,
            job_id,
        };
        let result = self
            .state
            .control
            .finalize_add(finalize_request)
            .await
            .map_err(map_central_error)?;
        let decision = map_decision(&result.decision.decision, result.decision.final_state);
        Ok(Response::new(FinalizeAddJobResponseBody {
            job: job_record_to_view(&result.job),
            decision,
            finalized_at_unix_ms: result.finalized.finalized_at_unix_ms.to_string(),
            replayed: result.replayed,
        }))
    }
}

// ── Request parsing helpers ────────────────────────────────────────────

fn validate_extensions(extensions: &crate::dto::JsonExtensions) -> Result<(), Error> {
    if let Some(reserved) = extensions.contains_any_reserved(RESERVED_REQUEST_FIELDS) {
        return Err(invalid_arg(format!(
            "request body must not contain the reserved field '{reserved}'"
        )));
    }
    Ok(())
}

fn invalid_arg(message: impl Into<String>) -> Error {
    Error::application(ErrorCategory::InvalidArgument, "invalid_argument", message).unwrap_or_else(
        |_| {
            Error::application(
                ErrorCategory::InvalidArgument,
                "invalid_argument",
                "invalid request argument",
            )
            .unwrap()
        },
    )
}

fn build_add_job_spec(
    body: &CreateAddJobRequest,
    principal: &neoengram_protocol::PrincipalRef,
) -> Result<neoengramd::AddJobSpec, Error> {
    let tenant_id = neoengram_protocol::TenantId::new(body.tenant_id.as_str())
        .map_err(|e| invalid_arg(format!("tenant_id: {e}")))?;
    let project_id = neoengram_protocol::ProjectId::new(body.project_id.as_str())
        .map_err(|e| invalid_arg(format!("project_id: {e}")))?;
    let artifact_id = neoengram_protocol::ArtifactId::new(body.artifact_id.as_str())
        .map_err(|e| invalid_arg(format!("artifact_id: {e}")))?;
    let playground_id = neoengram_protocol::PlaygroundId::new(body.playground_id.as_str())
        .map_err(|e| invalid_arg(format!("playground_id: {e}")))?;
    let job_id = neoengram_protocol::JobId::new(body.job_id.as_str())
        .map_err(|e| invalid_arg(format!("job_id: {e}")))?;

    let revision = body
        .expected_index_version
        .revision
        .parse::<u64>()
        .map_err(|_| {
            invalid_arg("expected_index_version.revision must be a non-negative integer")
        })?;
    let digest = ContentDigest::from_str(&body.expected_index_version.digest).map_err(|_| {
        invalid_arg("expected_index_version.digest is not a valid BLAKE3 hex digest")
    })?;
    let expected_index_version = WireIndexVersion {
        revision: neoengram_protocol::IndexRevision::new(revision),
        digest,
        extensions: Default::default(),
    };

    let deadline_raw = body
        .deadline_unix_ms
        .parse::<u64>()
        .map_err(|_| invalid_arg("deadline_unix_ms must be a non-negative integer"))?;
    let deadline_unix_ms = neoengram_protocol::UnixMillis::new(deadline_raw);

    let paths: Vec<LogicalPath> = body
        .paths
        .iter()
        .map(|p| LogicalPath::parse(p).map_err(|e| invalid_arg(format!("invalid path '{p}': {e}"))))
        .collect::<Result<_, _>>()?;

    let all_extensions = body
        .extensions
        .non_reserved_entries(RESERVED_REQUEST_FIELDS);
    let extensions = Extensions::from(all_extensions);

    // Build the operation and compute canonical request digest.
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
        all: body.all,
        extensions,
    };
    let request_digest = operation
        .request_digest()
        .map_err(|e| invalid_arg(format!("failed to compute request digest: {e}")))?;

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
        all: body.all,
        extensions: Default::default(),
    })
}

// ── Response mapping helpers ────────────────────────────────────────────

fn job_record_to_view(job: &neoengramd::JobRecord) -> JobView {
    let progress = job.progress.as_ref().map(|p| PublicJobProgress {
        state: job_state_str(p.state),
        phase: p.phase.clone(),
        files_completed: p.files_completed.to_string(),
        bytes_completed: p.bytes_completed.to_string(),
    });

    let decision = job
        .decision
        .as_ref()
        .map(|d| map_decision(&d.decision, d.final_state));

    let failure = job.failure.as_ref().map(|f| PublicJobFailure {
        final_state: job_state_str(f.final_state),
        failed_at_unix_ms: f.failed_at_unix_ms.to_string(),
        stage: format!("{:?}", f.stage).to_lowercase(),
        error: JobErrorView {
            code: f.error.code.as_str().to_owned(),
            message: f.error.message.clone(),
            retryable: f.error.retryable,
        },
    });

    let finalized_at = job
        .finalized
        .as_ref()
        .map(|f| f.finalized_at_unix_ms.to_string());

    JobView {
        operation: "add".to_owned(),
        tenant_id: job.spec.tenant_id.to_string(),
        project_id: job.spec.project_id.to_string(),
        artifact_id: job.spec.artifact_id.to_string(),
        playground_id: job.spec.playground_id.to_string(),
        job_id: job.spec.job_id.to_string(),
        state: job_state_str(job.state),
        resource_version: job.resource_version.get().to_string(),
        deadline_unix_ms: job.spec.deadline_unix_ms.to_string(),
        progress,
        decision,
        failure,
        finalized_at_unix_ms: finalized_at,
    }
}

fn job_state_str(state: JobState) -> String {
    format!("{:?}", state).to_lowercase()
}

fn map_decision(decision: &PublishDecision, final_state: JobState) -> PublicJobDecision {
    match decision {
        PublishDecision::Publish {
            published_index_version,
            ..
        } => PublicJobDecision::Publish {
            final_state: job_state_str(final_state),
            published_index_version: IndexVersionResponse {
                revision: published_index_version.revision.get().to_string(),
                digest: published_index_version.digest.to_string(),
            },
        },
        PublishDecision::Conflict {
            current_index_version,
            ..
        } => PublicJobDecision::Conflict {
            final_state: job_state_str(final_state),
            current_index_version: IndexVersionResponse {
                revision: current_index_version.revision.get().to_string(),
                digest: current_index_version.digest.to_string(),
            },
        },
        PublishDecision::Reject { error, .. } => PublicJobDecision::Reject {
            final_state: job_state_str(final_state),
            error: JobErrorView {
                code: error.code.as_str().to_owned(),
                message: error.message.clone(),
                retryable: error.retryable,
            },
        },
    }
}
