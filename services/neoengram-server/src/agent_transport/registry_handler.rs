use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use http::StatusCode;
use neoengram_protocol::{
    decode_bounded_unique_json, AgentActionAcceptedResponse, AgentAuthenticatedRequest,
    AgentBootstrapRequest, AgentBootstrapStatusRequest, AgentHeartbeatReportPayload,
    AgentHeartbeatReportResponse, AgentIndexPageQueryPayload, AgentIndexPageQueryResponse,
    AgentJobReportCreatePayload, AgentMessageListQueryPayload, AgentMessageListQueryResponse,
    AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload, AgentMetadataStageResponse,
    AgentObjectMissingQueryPayload, AgentObjectMissingQueryResponse, AgentObjectUploadPayload,
    AgentObjectUploadResponse, AgentSessionClosePayload, AgentSessionCloseResponse,
    AgentSessionOpenPayload, AgentSessionOpenResponse, ControlMessage, DecimalU64, Extensions,
    ProtocolVersion, UnixMillis, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_METADATA_BATCH_STAGE_PATH, AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    AGENT_JOB_OBJECT_MISSING_QUERY_PATH, AGENT_JOB_OBJECT_UPLOAD_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CLOSE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH,
};
use neoengramd::{
    AgentRegistryService, BootstrapStorageEnrollmentRequest, CentralError, CentralErrorCode,
    CloseAgentSessionRequest, ControlPlane, MetadataBatchSubmission, OpenAgentSessionRequest,
    ReceiveReportRequest, StageMetadataBatchRequest,
};

use super::{AgentAction, AgentApiHandler, AgentHttpError, AGENT_MAX_REQUEST_BODY_BYTES};

const AGENT_ACTION_MAX_CLOCK_SKEW_MS: u64 = 60_000;

/// Extension point for the filesystem-backed Index and object data plane.
///
/// The registry handler authenticates and fences every request before invoking this port.
#[async_trait]
pub trait AgentDataPlaneHandler: Send + Sync + 'static {
    async fn query_index_page(
        &self,
        request: AgentAuthenticatedRequest<AgentIndexPageQueryPayload>,
    ) -> Result<AgentIndexPageQueryResponse, AgentHttpError>;

    async fn query_missing_objects(
        &self,
        request: AgentAuthenticatedRequest<AgentObjectMissingQueryPayload>,
    ) -> Result<AgentObjectMissingQueryResponse, AgentHttpError>;

    async fn upload_object(
        &self,
        request: AgentAuthenticatedRequest<AgentObjectUploadPayload>,
    ) -> Result<AgentObjectUploadResponse, AgentHttpError>;
}

/// Hyper adapter that shares the process-wide registry and control-plane services.
pub struct RegistryAgentApiHandler {
    registry: Arc<AgentRegistryService>,
    control: Option<Arc<ControlPlane>>,
    data_plane: Option<Arc<dyn AgentDataPlaneHandler>>,
    accepting: Arc<AtomicBool>,
}

/// Compatibility name retained while the composition root migrates to `RegistryAgentApiHandler`.
pub type RegistryAgentEnrollmentHandler = RegistryAgentApiHandler;

impl RegistryAgentApiHandler {
    #[must_use]
    pub fn new(registry: Arc<AgentRegistryService>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }

    #[must_use]
    pub fn with_readiness(registry: Arc<AgentRegistryService>, accepting: Arc<AtomicBool>) -> Self {
        Self {
            registry,
            control: None,
            data_plane: None,
            accepting,
        }
    }

    #[must_use]
    pub fn with_transport(
        registry: Arc<AgentRegistryService>,
        control: Arc<ControlPlane>,
        data_plane: Arc<dyn AgentDataPlaneHandler>,
        accepting: Arc<AtomicBool>,
    ) -> Self {
        Self {
            registry,
            control: Some(control),
            data_plane: Some(data_plane),
            accepting,
        }
    }

    fn require_control(&self) -> Result<&ControlPlane, AgentHttpError> {
        self.control
            .as_deref()
            .ok_or_else(AgentHttpError::unavailable)
    }

    fn require_data_plane(&self) -> Result<&dyn AgentDataPlaneHandler, AgentHttpError> {
        self.data_plane
            .as_deref()
            .ok_or_else(AgentHttpError::unavailable)
    }

    async fn authenticate<T: serde::Serialize>(
        &self,
        request: &AgentAuthenticatedRequest<T>,
        path: &'static str,
    ) -> Result<neoengramd::AgentRegistryRecord, AgentHttpError> {
        self.registry
            .authenticate_agent_action(
                request,
                path,
                path != AGENT_SESSION_OPEN_PATH,
                AGENT_ACTION_MAX_CLOCK_SKEW_MS,
            )
            .await
            .map_err(map_registry_error)
    }

    async fn bootstrap(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request = AgentBootstrapRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .registry
            .bootstrap_storage_enrollment(BootstrapStorageEnrollmentRequest { request })
            .await
            .map_err(map_registry_error)?;
        encode(&result.accepted)
    }

    async fn bootstrap_status(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request = AgentBootstrapStatusRequest::decode_json(body)
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let response = self
            .registry
            .bootstrap_status_with_clock_skew(request, AGENT_ACTION_MAX_CLOCK_SKEW_MS)
            .await
            .map_err(map_registry_error)?;
        encode(&response)
    }

    async fn session_open(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentSessionOpenPayload> = decode(body)?;
        let authenticated = self.authenticate(&request, AGENT_SESSION_OPEN_PATH).await?;
        let result = self
            .registry
            .open_session(OpenAgentSessionRequest {
                agent_id: request.agent_id.clone(),
                installation_id: request.installation_id.clone(),
                boot_id: request.boot_id.clone(),
                mount_identity_digest: request.payload.mount_identity_digest,
                expected_resource_version: request.payload.expected_resource_version,
            })
            .await
            .map_err(map_registry_error)?;
        let opened_at = result
            .record
            .instance
            .as_ref()
            .and_then(|instance| instance.session_opened_at_unix_ms)
            .ok_or_else(AgentHttpError::unavailable)?;
        debug_assert_eq!(
            authenticated.enrollment.reserved_agent_id,
            result.record.enrollment.reserved_agent_id
        );
        encode(&AgentSessionOpenResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            agent_id: request.agent_id,
            session_id: result.session_id,
            session_generation: result.session_generation,
            agent_mount_id: result.record.mount.agent_mount_id.clone(),
            mount_generation: result.record.mount.mount_generation,
            owner_generation: result.record.owner.owner_generation,
            resource_version: result.record.resource_version,
            opened_at_unix_ms: opened_at,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn heartbeat_report(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentHeartbeatReportPayload> = decode(body)?;
        request
            .payload
            .heartbeat
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_SESSION_HEARTBEAT_REPORT_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let report = &request.payload.mount_report;
        if request.payload.heartbeat.agent_id != request.agent_id
            || report.agent_id != request.agent_id
            || report.installation_id != request.installation_id
            || report.boot_id != request.boot_id
            || report.session_generation != generation
            || report.sequence != request.payload.heartbeat.sequence
        {
            return Err(AgentHttpError::session_fenced());
        }
        let result = self
            .registry
            .report_mount(request.payload.mount_report)
            .await
            .map_err(map_registry_error)?;
        let received_at = result
            .record
            .instance
            .as_ref()
            .and_then(|instance| instance.last_heartbeat_at_unix_ms)
            .ok_or_else(AgentHttpError::unavailable)?;
        encode(&AgentHeartbeatReportResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: result.record.resource_version,
            received_at_unix_ms: received_at,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn message_list_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMessageListQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_SESSION_MESSAGE_LIST_QUERY_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let messages = self
            .require_control()?
            .poll_agent_messages(
                &request.agent_id,
                generation,
                usize::from(request.payload.max_messages),
            )
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMessageListQueryResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            messages,
            retry_after_ms: DecimalU64::new(1_000),
            extensions: Extensions::new(),
        })
    }

    async fn job_report_create(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentJobReportCreatePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_REPORT_CREATE_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        request
            .payload
            .report
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        if request.payload.report.session_generation != generation {
            return Err(AgentHttpError::session_fenced());
        }
        let report = match request.payload.report.message {
            ControlMessage::Accepted(value) => neoengramd::AgentReport::Accepted(value),
            ControlMessage::Progress(value) => neoengramd::AgentReport::Progress(value),
            ControlMessage::Prepared(value) => neoengramd::AgentReport::Prepared(value),
            ControlMessage::Failed(value) => neoengramd::AgentReport::Failed(value),
            ControlMessage::Finalized(value) => neoengramd::AgentReport::Finalized(value),
            _ => return Err(AgentHttpError::protocol_invalid()),
        };
        let result = self
            .require_control()?
            .receive_report(ReceiveReportRequest {
                tenant_id: request.payload.tenant_id,
                agent_id: request.agent_id,
                report,
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentActionAcceptedResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: result.job.resource_version,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn metadata_batch_stage(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMetadataBatchStagePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_METADATA_BATCH_STAGE_PATH)
            .await?;
        request
            .payload
            .descriptor
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .require_control()?
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: request.payload.tenant_id,
                job_id: request.payload.job_id,
                agent_id: request.agent_id,
                submission: MetadataBatchSubmission::Descriptor(request.payload.descriptor),
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMetadataStageResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            batch_id: result.batch_id,
            complete: result.complete,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn metadata_page_stage(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentMetadataPageStagePayload> = decode(body)?;
        self.authenticate(&request, AGENT_JOB_METADATA_PAGE_STAGE_PATH)
            .await?;
        request
            .payload
            .page
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        let result = self
            .require_control()?
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: request.payload.tenant_id,
                job_id: request.payload.job_id,
                agent_id: request.agent_id,
                submission: MetadataBatchSubmission::Page(request.payload.page),
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentMetadataStageResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            batch_id: result.batch_id,
            complete: result.complete,
            replayed: result.replayed,
            extensions: Extensions::new(),
        })
    }

    async fn index_page_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentIndexPageQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_JOB_INDEX_PAGE_QUERY_PATH)
            .await?;
        encode(&self.require_data_plane()?.query_index_page(request).await?)
    }

    async fn object_missing_query(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentObjectMissingQueryPayload> = decode(body)?;
        request
            .payload
            .validate()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_JOB_OBJECT_MISSING_QUERY_PATH)
            .await?;
        encode(
            &self
                .require_data_plane()?
                .query_missing_objects(request)
                .await?,
        )
    }

    async fn object_upload(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentObjectUploadPayload> = decode(body)?;
        request
            .payload
            .decode_content()
            .map_err(|_| AgentHttpError::protocol_invalid())?;
        self.authenticate(&request, AGENT_JOB_OBJECT_UPLOAD_PATH)
            .await?;
        encode(&self.require_data_plane()?.upload_object(request).await?)
    }

    async fn session_close(&self, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        let request: AgentAuthenticatedRequest<AgentSessionClosePayload> = decode(body)?;
        self.authenticate(&request, AGENT_SESSION_CLOSE_PATH)
            .await?;
        let generation = request
            .session_generation
            .ok_or_else(AgentHttpError::protocol_invalid)?;
        let record = self
            .registry
            .close_session(CloseAgentSessionRequest {
                agent_id: request.agent_id,
                boot_id: request.boot_id,
                session_generation: generation,
                expected_resource_version: request.payload.expected_resource_version,
            })
            .await
            .map_err(map_registry_error)?;
        encode(&AgentSessionCloseResponse {
            protocol_version: ProtocolVersion::V1,
            request_id: request.request_id,
            resource_version: record.resource_version,
            closed_at_unix_ms: record
                .storage_enrollment
                .updated_at_unix_ms
                .unwrap_or_else(|| UnixMillis::new(0)),
            extensions: Extensions::new(),
        })
    }
}

#[async_trait]
impl AgentApiHandler for RegistryAgentApiHandler {
    async fn handle(&self, action: AgentAction, body: &[u8]) -> Result<Vec<u8>, AgentHttpError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(AgentHttpError::unavailable());
        }
        match action {
            AgentAction::EnrollmentBootstrap => self.bootstrap(body).await,
            AgentAction::EnrollmentStatusQuery => self.bootstrap_status(body).await,
            AgentAction::SessionOpen => self.session_open(body).await,
            AgentAction::SessionHeartbeatReport => self.heartbeat_report(body).await,
            AgentAction::SessionMessageListQuery => self.message_list_query(body).await,
            AgentAction::JobReportCreate => self.job_report_create(body).await,
            AgentAction::JobMetadataBatchStage => self.metadata_batch_stage(body).await,
            AgentAction::JobMetadataPageStage => self.metadata_page_stage(body).await,
            AgentAction::JobIndexPageQuery => self.index_page_query(body).await,
            AgentAction::JobObjectMissingQuery => self.object_missing_query(body).await,
            AgentAction::JobObjectUpload => self.object_upload(body).await,
            AgentAction::SessionClose => self.session_close(body).await,
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, AgentHttpError> {
    decode_bounded_unique_json(body, body.len().max(AGENT_MAX_REQUEST_BODY_BYTES))
        .map_err(|_| AgentHttpError::protocol_invalid())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, AgentHttpError> {
    serde_json::to_vec(value).map_err(|_| AgentHttpError::unavailable())
}

fn map_registry_error(error: CentralError) -> AgentHttpError {
    match error.code() {
        CentralErrorCode::StorageFailure | CentralErrorCode::Internal => {
            AgentHttpError::unavailable()
        }
        CentralErrorCode::ProtocolInvalid => AgentHttpError::protocol_invalid(),
        CentralErrorCode::GenerationMismatch => AgentHttpError::session_fenced(),
        CentralErrorCode::ConcurrentUpdate | CentralErrorCode::AgentSessionActive => {
            AgentHttpError::new(
                StatusCode::CONFLICT,
                "AGENT_ACTION_CONFLICT",
                "Agent action conflicts with authoritative state",
                true,
            )
        }
        CentralErrorCode::JobNotFound | CentralErrorCode::EnrollmentNotFound => {
            AgentHttpError::new(
                StatusCode::NOT_FOUND,
                "AGENT_RESOURCE_NOT_FOUND",
                "Agent-scoped resource was not found",
                false,
            )
        }
        CentralErrorCode::InvalidState
        | CentralErrorCode::AssignmentMismatch
        | CentralErrorCode::BatchTampered
        | CentralErrorCode::BatchUndeclared => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_ACTION_REJECTED",
            "Agent action is incompatible with authoritative state",
            false,
        ),
        _ => AgentHttpError::bootstrap_denied(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_session_errors_are_sanitized_and_fenced() {
        let mapped = map_registry_error(CentralError::new(
            CentralErrorCode::GenerationMismatch,
            "sensitive generation detail",
        ));
        assert_eq!(mapped.status, StatusCode::CONFLICT);
        assert_eq!(mapped.code, "AGENT_SESSION_FENCED");
        assert!(!mapped.detail.contains("generation"));
    }
}
