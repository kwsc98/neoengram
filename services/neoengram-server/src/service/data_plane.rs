use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentAuthenticatedRequest, AgentBootId, AgentId, AgentIndexPageQueryPayload,
    AgentIndexPageQueryResponse, AgentInstallationId, AgentObjectMissingQueryPayload,
    AgentObjectMissingQueryResponse, AgentObjectUploadPayload, AgentObjectUploadResponse,
    DecimalU64, DurabilityState, Extensions, IndexDeltaRecord, JobState, ObjectDurabilityReceipt,
    RequestId, SessionGeneration, SessionId, UnixMillis, PROTOCOL_VERSION_V1,
};
use neoengramd::{
    CentralError, CentralErrorCode, CentralResult, Clock, IndexKey, JobKey, JobRecord,
    SqliteAuthority,
};

use super::{CentralObjectStoreBackend, FilesystemObjectStoreError, StoredObject};
use crate::agent_transport::{AgentDataPlaneHandler, AgentHttpError};

/// Authoritative implementation behind the Agent listener's action-style data-plane methods.
pub struct AgentDataPlaneService {
    authority: Arc<SqliteAuthority>,
    objects: Arc<dyn CentralObjectStoreBackend>,
    clock: Arc<dyn Clock>,
}

#[async_trait]
impl AgentDataPlaneHandler for AgentDataPlaneService {
    async fn query_index_page(
        &self,
        request: AgentAuthenticatedRequest<AgentIndexPageQueryPayload>,
    ) -> Result<AgentIndexPageQueryResponse, AgentHttpError> {
        let context = AgentRequestContext::from_request(&request)?;
        self.query_index_page(request.request_id, &context, request.payload)
            .await
            .map_err(map_central_error)
    }

    async fn query_missing_objects(
        &self,
        request: AgentAuthenticatedRequest<AgentObjectMissingQueryPayload>,
    ) -> Result<AgentObjectMissingQueryResponse, AgentHttpError> {
        let context = AgentRequestContext::from_request(&request)?;
        self.query_missing_objects(request.request_id, &context, request.payload)
            .await
            .map_err(map_central_error)
    }

    async fn upload_object(
        &self,
        request: AgentAuthenticatedRequest<AgentObjectUploadPayload>,
    ) -> Result<AgentObjectUploadResponse, AgentHttpError> {
        let context = AgentRequestContext::from_request(&request)?;
        self.upload_object(request.request_id, &context, request.payload)
            .await
            .map_err(map_central_error)
    }
}

impl AgentDataPlaneService {
    #[must_use]
    pub fn new(
        authority: Arc<SqliteAuthority>,
        objects: Arc<dyn CentralObjectStoreBackend>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            authority,
            objects,
            clock,
        }
    }

    async fn query_index_page(
        &self,
        request_id: RequestId,
        context: &AgentRequestContext,
        payload: AgentIndexPageQueryPayload,
    ) -> CentralResult<AgentIndexPageQueryResponse> {
        payload.validate()?;
        let job = self
            .load_owned_job(&payload.tenant_id, &payload.job_id, context)
            .await?;
        require_active_execution_state(&job, "Index query")?;
        if job.spec.artifact_id != payload.artifact_id
            || job.spec.playground_id != payload.playground_id
            || !same_index_version(&job.spec.expected_index_version, &payload.index_version)
        {
            return Err(scope_mismatch(
                "Index query scope or version differs from the persisted assignment",
            ));
        }
        let index = self
            .authority
            .published_index(&IndexKey {
                tenant_id: payload.tenant_id,
                project_id: job.spec.project_id,
                artifact_id: payload.artifact_id,
                playground_id: payload.playground_id,
            })
            .await?;
        if !same_index_version(&index.version, &payload.index_version) {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "authoritative IndexVersion changed before the Agent downloaded it",
            ));
        }
        let page_size = usize::from(payload.max_records);
        let page_count_usize = index.records.len().div_ceil(page_size).max(1);
        let page_count = u32::try_from(page_count_usize).map_err(|_| {
            CentralError::new(
                CentralErrorCode::Internal,
                "Index page count exceeds the wire range",
            )
        })?;
        if payload.page_number >= page_count {
            return Err(CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                "Index page_number is outside the snapshot",
            )
            .with_retryable(false));
        }
        let start = usize::try_from(payload.page_number)
            .ok()
            .and_then(|page| page.checked_mul(page_size))
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::ProtocolInvalid,
                    "Index page offset overflow",
                )
            })?;
        let end = start.saturating_add(page_size).min(index.records.len());
        let records = index.records[start..end]
            .iter()
            .map(|record| IndexDeltaRecord::Upsert {
                path: record.path.clone(),
                manifest_id: record.manifest_id,
                total_size: DecimalU64::new(record.total_size),
                chunk_count: DecimalU64::new(record.chunk_count),
                extensions: Extensions::new(),
            })
            .collect();
        Ok(AgentIndexPageQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id,
            index_version: index.version,
            page_number: payload.page_number,
            page_count,
            records,
            extensions: Extensions::new(),
        })
    }

    async fn query_missing_objects(
        &self,
        request_id: RequestId,
        context: &AgentRequestContext,
        payload: AgentObjectMissingQueryPayload,
    ) -> CentralResult<AgentObjectMissingQueryResponse> {
        payload.validate()?;
        let job = self
            .load_owned_job(&payload.tenant_id, &payload.job_id, context)
            .await?;
        validate_object_scope(&job, &payload.project_id, &payload.artifact_id)?;
        let (missing, already_durable) = self
            .objects
            .missing(&payload.tenant_id, &payload.artifact_id, &payload.objects)
            .map_err(map_object_store_error)?;
        if !is_active_object_state(job.state) && !missing.is_empty() {
            return Err(invalid_object_state(job.state));
        }
        for object in &already_durable {
            let storage_version = self
                .objects
                .durable_version(&payload.tenant_id, &payload.artifact_id, object)
                .map_err(map_object_store_error)?
                .ok_or_else(|| {
                    CentralError::new(
                        CentralErrorCode::StorageFailure,
                        "object disappeared during durability negotiation",
                    )
                })?;
            self.record_durability(
                context.session_id.clone(),
                payload.tenant_id.clone(),
                payload.artifact_id.clone(),
                payload.job_id.clone(),
                StoredObject {
                    object: object.clone(),
                    storage_version,
                    replayed: true,
                },
            )
            .await?;
        }
        Ok(AgentObjectMissingQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id,
            missing,
            already_durable,
            extensions: Extensions::new(),
        })
    }

    async fn upload_object(
        &self,
        request_id: RequestId,
        context: &AgentRequestContext,
        payload: AgentObjectUploadPayload,
    ) -> CentralResult<AgentObjectUploadResponse> {
        let content = payload.decode_content()?;
        let job = self
            .load_owned_job(&payload.tenant_id, &payload.job_id, context)
            .await?;
        validate_object_scope(&job, &payload.project_id, &payload.artifact_id)?;
        let stored = if is_active_object_state(job.state) {
            self.objects
                .put(
                    &payload.tenant_id,
                    &payload.artifact_id,
                    &payload.object,
                    &content,
                )
                .map_err(map_object_store_error)?
        } else {
            let storage_version = self
                .objects
                .durable_version(&payload.tenant_id, &payload.artifact_id, &payload.object)
                .map_err(map_object_store_error)?
                .ok_or_else(|| invalid_object_state(job.state))?;
            StoredObject {
                object: payload.object.clone(),
                storage_version,
                replayed: true,
            }
        };
        let replayed = stored.replayed;
        let receipt = self
            .record_durability(
                context.session_id.clone(),
                payload.tenant_id,
                payload.artifact_id,
                payload.job_id,
                stored,
            )
            .await?;
        Ok(AgentObjectUploadResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id,
            receipt,
            replayed,
            extensions: Extensions::new(),
        })
    }

    async fn record_durability(
        &self,
        session_id: SessionId,
        tenant_id: neoengram_protocol::TenantId,
        artifact_id: neoengram_protocol::ArtifactId,
        job_id: neoengram_protocol::JobId,
        stored: StoredObject,
    ) -> CentralResult<ObjectDurabilityReceipt> {
        let receipt = ObjectDurabilityReceipt {
            tenant_id,
            artifact_id,
            session_id,
            job_id,
            state: DurabilityState::Durable {
                verified_digest: ContentDigest::from_bytes(*stored.object.object_id.as_bytes()),
                storage_version: stored.storage_version,
                extensions: Extensions::new(),
            },
            object: stored.object,
            checked_at_unix_ms: UnixMillis::new(self.clock.now().get()),
            extensions: Extensions::new(),
        };
        receipt.validate()?;
        self.authority
            .authority_store()
            .objects()
            .record_durability(&receipt)
            .await?;
        Ok(receipt)
    }

    async fn load_owned_job(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
        context: &AgentRequestContext,
    ) -> CentralResult<JobRecord> {
        let job = self
            .authority
            .authority_store()
            .jobs()
            .get(&JobKey::new(tenant_id.clone(), job_id.clone()))
            .await?
            .ok_or_else(|| {
                CentralError::new(CentralErrorCode::JobNotFound, "Agent Job does not exist")
                    .with_retryable(false)
            })?;
        let assignment = job.assignment.as_ref().ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::InvalidState,
                "Agent Job has no persisted assignment",
            )
        })?;
        if assignment.agent_id != context.agent_id {
            return Err(scope_mismatch(
                "authenticated Agent does not own the persisted assignment",
            ));
        }
        let registry = self
            .authority
            .authority_store()
            .agent_registry()
            .ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::Internal,
                    "authority has no Agent registry composition",
                )
            })?;
        let owner = registry
            .get_current_by_volume(tenant_id, &assignment.storage_volume_id)
            .await?
            .ok_or_else(|| scope_mismatch("assigned Volume has no current Agent owner"))?;
        let instance = owner
            .instance
            .as_ref()
            .ok_or_else(|| scope_mismatch("assigned Volume owner has no active Agent instance"))?;
        if instance.agent_id != context.agent_id
            || instance.installation_id != context.installation_id
            || instance.active_boot_id.as_ref() != Some(&context.boot_id)
            || instance.active_session_id.as_ref() != Some(&context.session_id)
            || instance.session_generation != Some(context.session_generation)
            || owner.mount.storage_volume_id != assignment.storage_volume_id
            || owner.mount.agent_mount_id != assignment.agent_mount_id
            || owner.mount.mount_generation != assignment.mount_generation
            || owner.owner.tenant_id != *tenant_id
            || owner.owner.storage_volume_id != assignment.storage_volume_id
            || owner.owner.active_agent_id.as_ref() != Some(&context.agent_id)
            || owner.owner.active_agent_mount_id.as_ref() != Some(&assignment.agent_mount_id)
            || owner.owner.owner_generation != assignment.owner_generation
        {
            return Err(scope_mismatch(
                "Agent session no longer owns the assignment mount generations",
            ));
        }
        Ok(job)
    }
}

#[derive(Debug, Clone)]
struct AgentRequestContext {
    agent_id: AgentId,
    installation_id: AgentInstallationId,
    boot_id: AgentBootId,
    session_id: SessionId,
    session_generation: SessionGeneration,
}

impl AgentRequestContext {
    fn from_request<T>(request: &AgentAuthenticatedRequest<T>) -> Result<Self, AgentHttpError> {
        Ok(Self {
            agent_id: request.agent_id.clone(),
            installation_id: request.installation_id.clone(),
            boot_id: request.boot_id.clone(),
            session_id: request
                .session_id
                .clone()
                .ok_or_else(AgentHttpError::protocol_invalid)?,
            session_generation: request
                .session_generation
                .ok_or_else(AgentHttpError::protocol_invalid)?,
        })
    }
}

fn require_active_execution_state(job: &JobRecord, action: &'static str) -> CentralResult<()> {
    if matches!(
        job.state,
        JobState::Assigned | JobState::Accepted | JobState::Running
    ) {
        Ok(())
    } else {
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            format!("{action} is not allowed in state {:?}", job.state),
        )
        .with_retryable(false))
    }
}

fn is_active_object_state(state: JobState) -> bool {
    // Accepted/Progress are durably queued by the Agent before synchronous execution, but the
    // transport cannot flush them until the Assignment handler yields back to the poll loop.
    matches!(
        state,
        JobState::Assigned | JobState::Accepted | JobState::Running
    )
}

fn invalid_object_state(state: JobState) -> CentralError {
    CentralError::new(
        CentralErrorCode::InvalidState,
        format!("new object durability is not allowed in state {state:?}"),
    )
    .with_retryable(false)
}

fn validate_object_scope(
    job: &JobRecord,
    project_id: &neoengram_protocol::ProjectId,
    artifact_id: &neoengram_protocol::ArtifactId,
) -> CentralResult<()> {
    if &job.spec.project_id != project_id || &job.spec.artifact_id != artifact_id {
        Err(scope_mismatch(
            "object action scope differs from the persisted assignment",
        ))
    } else {
        Ok(())
    }
}

fn same_index_version(
    left: &neoengram_protocol::WireIndexVersion,
    right: &neoengram_protocol::WireIndexVersion,
) -> bool {
    left.revision == right.revision && left.digest == right.digest
}

fn scope_mismatch(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::AssignmentMismatch, message).with_retryable(false)
}

fn map_object_store_error(error: FilesystemObjectStoreError) -> CentralError {
    let code = match error {
        FilesystemObjectStoreError::ObjectTooLarge
        | FilesystemObjectStoreError::LengthMismatch
        | FilesystemObjectStoreError::HashMismatch
        | FilesystemObjectStoreError::InvalidScope(_) => CentralErrorCode::ProtocolInvalid,
        FilesystemObjectStoreError::Storage(_) | FilesystemObjectStoreError::LockPoisoned => {
            CentralErrorCode::StorageFailure
        }
    };
    CentralError::new(code, error.to_string())
}

fn map_central_error(error: CentralError) -> AgentHttpError {
    match error.code() {
        CentralErrorCode::StorageFailure | CentralErrorCode::Internal => {
            AgentHttpError::unavailable()
        }
        CentralErrorCode::ProtocolInvalid | CentralErrorCode::MetadataInvalid => {
            AgentHttpError::protocol_invalid()
        }
        CentralErrorCode::GenerationMismatch => AgentHttpError::session_fenced(),
        CentralErrorCode::ConcurrentUpdate => AgentHttpError::new(
            StatusCode::CONFLICT,
            "AGENT_ACTION_CONFLICT",
            "Agent action conflicts with authoritative state",
            true,
        ),
        CentralErrorCode::JobNotFound => AgentHttpError::new(
            StatusCode::NOT_FOUND,
            "AGENT_RESOURCE_NOT_FOUND",
            "Agent-scoped resource was not found",
            false,
        ),
        CentralErrorCode::InvalidState | CentralErrorCode::AssignmentMismatch => {
            AgentHttpError::new(
                StatusCode::CONFLICT,
                "AGENT_ACTION_REJECTED",
                "Agent action is incompatible with authoritative state",
                false,
            )
        }
        _ => AgentHttpError::new(
            StatusCode::FORBIDDEN,
            "AGENT_ACTION_DENIED",
            "Agent action was denied",
            false,
        ),
    }
}
