use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use neoengram_core::{CommitId, FileRecord, Manifest, ManifestId};
use neoengram_protocol::{
    AgentAuthenticatedRequest, AgentBootId, AgentId, AgentIndexPageQueryPayload,
    AgentIndexPageQueryResponse, AgentInstallationId, AgentManifestPageQueryPayload,
    AgentManifestPageQueryResponse, AgentMountId, DecimalU64, Extensions, IndexDeltaRecord,
    JobState, MountGeneration, OwnerGeneration, RequestId, SessionGeneration, SessionId,
    StorageVolumeId, WireChunkRef, PROTOCOL_VERSION_V1,
};
use neoengramd::{
    CentralError, CentralErrorCode, CentralResult, IndexKey, JobKey, JobOperation, JobRecord,
    SqliteAuthority,
};

use crate::agent_transport::{AgentDataPlaneHandler, AgentHttpError};

/// Authoritative metadata implementation behind the Agent listener's Index query action.
pub struct AgentDataPlaneService {
    authority: Arc<SqliteAuthority>,
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

    async fn query_manifest_page(
        &self,
        request: AgentAuthenticatedRequest<AgentManifestPageQueryPayload>,
    ) -> Result<AgentManifestPageQueryResponse, AgentHttpError> {
        let context = AgentRequestContext::from_request(&request)?;
        self.query_manifest_page(request.request_id, &context, request.payload)
            .await
            .map_err(map_central_error)
    }
}

impl AgentDataPlaneService {
    #[must_use]
    pub fn new(authority: Arc<SqliteAuthority>) -> Self {
        Self { authority }
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
        let (version, records) = match job.operation {
            JobOperation::Add => self.load_add_index(&job, &payload).await?,
            JobOperation::WorkspaceMaterialize => {
                self.load_materialization_index(&job, &payload).await?
            }
            JobOperation::SnapshotMount => self.load_snapshot_index(&job, &payload).await?,
        };
        let (page_count, records) = page_index_records(
            &records,
            payload.page_number,
            usize::from(payload.max_records),
        )?;
        Ok(AgentIndexPageQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id,
            index_version: version,
            page_number: payload.page_number,
            page_count,
            records,
            extensions: Extensions::new(),
        })
    }

    async fn load_add_index(
        &self,
        job: &JobRecord,
        payload: &AgentIndexPageQueryPayload,
    ) -> CentralResult<(neoengram_protocol::WireIndexVersion, Vec<FileRecord>)> {
        let playground_id = payload
            .playground_id
            .as_ref()
            .ok_or_else(|| scope_mismatch("Add Index query requires playground_id"))?;
        if job.spec.artifact_id != payload.artifact_id
            || &job.spec.playground_id != playground_id
            || !same_index_version(&job.spec.expected_index_version, &payload.index_version)
        {
            return Err(scope_mismatch(
                "Index query scope or version differs from the persisted Add assignment",
            ));
        }
        let index = self
            .authority
            .published_index(&IndexKey {
                tenant_id: payload.tenant_id.clone(),
                project_id: job.spec.project_id.clone(),
                artifact_id: payload.artifact_id.clone(),
                playground_id: playground_id.clone(),
            })
            .await?;
        if !same_index_version(&index.version, &payload.index_version) {
            return Err(CentralError::new(
                CentralErrorCode::ConcurrentUpdate,
                "authoritative IndexVersion changed before the Agent downloaded it",
            ));
        }
        Ok((index.version, index.records))
    }

    async fn load_materialization_index(
        &self,
        job: &JobRecord,
        payload: &AgentIndexPageQueryPayload,
    ) -> CentralResult<(neoengram_protocol::WireIndexVersion, Vec<FileRecord>)> {
        let spec = workspace_spec(job)?;
        let expected_version = spec.base_index_version.as_ref().ok_or_else(|| {
            scope_mismatch("an empty Workspace baseline has no Index snapshot to query")
        })?;
        let playground_id = payload
            .playground_id
            .as_ref()
            .ok_or_else(|| scope_mismatch("materialization Index query requires playground_id"))?;
        if spec.artifact_id != payload.artifact_id
            || &spec.playground_id != playground_id
            || !same_index_version(expected_version, &payload.index_version)
        {
            return Err(scope_mismatch(
                "Index query scope or version differs from the persisted materialization assignment",
            ));
        }
        let commit = self.load_materialization_commit(job).await?;
        if !same_index_version(&commit.index_version, expected_version) {
            return Err(authority_integrity_error(
                "materialization Commit IndexVersion differs from the frozen assignment",
            ));
        }
        Ok((commit.index_version, commit.records))
    }

    async fn load_snapshot_index(
        &self,
        job: &JobRecord,
        payload: &AgentIndexPageQueryPayload,
    ) -> CentralResult<(neoengram_protocol::WireIndexVersion, Vec<FileRecord>)> {
        let spec = snapshot_spec(job)?;
        let snapshot_id = payload
            .snapshot_id
            .as_ref()
            .ok_or_else(|| scope_mismatch("Snapshot Index query requires snapshot_id"))?;
        if spec.artifact_id != payload.artifact_id
            || &spec.snapshot_id != snapshot_id
            || !same_index_version(&spec.index_version, &payload.index_version)
        {
            return Err(scope_mismatch(
                "Index query scope or version differs from the persisted SnapshotMount assignment",
            ));
        }
        let commit = self.load_snapshot_commit(job).await?;
        if !same_index_version(&commit.index_version, &spec.index_version) {
            return Err(authority_integrity_error(
                "Snapshot Commit IndexVersion differs from the frozen assignment",
            ));
        }
        Ok((commit.index_version, commit.records))
    }

    async fn query_manifest_page(
        &self,
        request_id: RequestId,
        context: &AgentRequestContext,
        payload: AgentManifestPageQueryPayload,
    ) -> CentralResult<AgentManifestPageQueryResponse> {
        payload.validate()?;
        let job = self
            .load_owned_job(&payload.tenant_id, &payload.job_id, context)
            .await?;
        require_active_execution_state(&job, "Manifest query")?;
        let commit = match job.operation {
            JobOperation::WorkspaceMaterialize => {
                let spec = workspace_spec(&job)?;
                if spec.artifact_id != payload.artifact_id {
                    return Err(scope_mismatch(
                        "Manifest query Artifact differs from the materialization assignment",
                    ));
                }
                self.load_materialization_commit(&job).await?
            }
            JobOperation::SnapshotMount => {
                let spec = snapshot_spec(&job)?;
                if spec.artifact_id != payload.artifact_id {
                    return Err(scope_mismatch(
                        "Manifest query Artifact differs from the SnapshotMount assignment",
                    ));
                }
                self.load_snapshot_commit(&job).await?
            }
            JobOperation::Add => {
                return Err(scope_mismatch(
                    "Manifest query requires a frozen immutable Commit assignment",
                ));
            }
        };
        require_manifest_reference(&commit.records, payload.manifest_id)?;
        let manifest = self
            .authority
            .manifest(
                &payload.tenant_id,
                &payload.artifact_id,
                payload.manifest_id,
            )
            .await?
            .ok_or_else(|| {
                authority_integrity_error("frozen Commit references a missing immutable Manifest")
            })?;
        for record in commit
            .records
            .iter()
            .filter(|record| record.manifest_id == payload.manifest_id)
        {
            validate_manifest_reference(record, &manifest, payload.manifest_id)?;
        }
        let (page_count, chunks) = page_manifest_chunks(
            &manifest,
            payload.page_number,
            usize::from(payload.max_chunks),
        )?;
        let response = AgentManifestPageQueryResponse {
            protocol_version: PROTOCOL_VERSION_V1,
            request_id,
            manifest_id: payload.manifest_id,
            total_size: DecimalU64::new(manifest.total_size),
            chunking: manifest.chunking.into(),
            page_number: payload.page_number,
            page_count,
            chunks,
            extensions: Extensions::new(),
        };
        response.validate()?;
        Ok(response)
    }

    async fn load_materialization_commit(
        &self,
        job: &JobRecord,
    ) -> CentralResult<neoengramd::CommitRecord> {
        let spec = workspace_spec(job)?;
        let commit_id = spec.base_commit_id.ok_or_else(|| {
            scope_mismatch("an empty Workspace baseline has no Commit metadata to query")
        })?;
        let precommits = self
            .authority
            .authority_store()
            .precommits()
            .ok_or_else(|| authority_integrity_error("Commit authority is unavailable"))?;
        precommits
            .get_commit(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                CommitId::from_digest(commit_id),
            )
            .await?
            .ok_or_else(|| {
                authority_integrity_error("materialization base Commit is missing from authority")
            })
    }

    async fn load_snapshot_commit(
        &self,
        job: &JobRecord,
    ) -> CentralResult<neoengramd::CommitRecord> {
        let spec = snapshot_spec(job)?;
        let precommits = self
            .authority
            .authority_store()
            .precommits()
            .ok_or_else(|| authority_integrity_error("Commit authority is unavailable"))?;
        precommits
            .get_commit(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                CommitId::from_digest(spec.commit_id),
            )
            .await?
            .ok_or_else(|| authority_integrity_error("Snapshot Commit is missing from authority"))
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
        let assignment = execution_placement(&job)?;
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
struct ExecutionPlacement {
    agent_id: AgentId,
    storage_volume_id: StorageVolumeId,
    agent_mount_id: AgentMountId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
}

fn execution_placement(job: &JobRecord) -> CentralResult<ExecutionPlacement> {
    match job.operation {
        JobOperation::Add => {
            if job.workspace_spec.is_some() || job.workspace_assignment.is_some() {
                return Err(authority_integrity_error(
                    "Add Job contains WorkspaceMaterialize authority fields",
                ));
            }
            let assignment = job.assignment.as_ref().ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "Add Job has no persisted assignment",
                )
            })?;
            assignment.validate()?;
            if assignment.operation() != job.spec.operation()
                || assignment.request_digest != job.spec.request_digest
            {
                return Err(authority_integrity_error(
                    "Add assignment differs from its immutable Job scope",
                ));
            }
            Ok(ExecutionPlacement {
                agent_id: assignment.agent_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                agent_mount_id: assignment.agent_mount_id.clone(),
                mount_generation: assignment.mount_generation,
                owner_generation: assignment.owner_generation,
            })
        }
        JobOperation::WorkspaceMaterialize => {
            if job.assignment.is_some() {
                return Err(authority_integrity_error(
                    "WorkspaceMaterialize Job contains an Add assignment",
                ));
            }
            let spec = workspace_spec(job)?;
            let assignment = job.workspace_assignment.as_ref().ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "WorkspaceMaterialize Job has no persisted assignment",
                )
            })?;
            assignment.validate()?;
            if assignment.operation() != spec.operation()
                || assignment.request_digest != spec.request_digest
            {
                return Err(authority_integrity_error(
                    "WorkspaceMaterialize assignment differs from its immutable Job scope",
                ));
            }
            Ok(ExecutionPlacement {
                agent_id: assignment.agent_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                agent_mount_id: assignment.agent_mount_id.clone(),
                mount_generation: assignment.mount_generation,
                owner_generation: assignment.owner_generation,
            })
        }
        JobOperation::SnapshotMount => {
            if job.assignment.is_some() || job.workspace_assignment.is_some() {
                return Err(authority_integrity_error(
                    "SnapshotMount Job contains another operation's assignment",
                ));
            }
            let spec = snapshot_spec(job)?;
            let assignment = job.snapshot_assignment.as_ref().ok_or_else(|| {
                CentralError::new(
                    CentralErrorCode::InvalidState,
                    "SnapshotMount Job has no persisted assignment",
                )
            })?;
            assignment.validate()?;
            if assignment.operation() != spec.operation()
                || assignment.request_digest != spec.request_digest
            {
                return Err(authority_integrity_error(
                    "SnapshotMount assignment differs from its immutable Job scope",
                ));
            }
            Ok(ExecutionPlacement {
                agent_id: assignment.agent_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                agent_mount_id: assignment.agent_mount_id.clone(),
                mount_generation: assignment.mount_generation,
                owner_generation: assignment.owner_generation,
            })
        }
    }
}

fn workspace_spec(job: &JobRecord) -> CentralResult<&neoengramd::WorkspaceMaterializeSpec> {
    job.workspace_spec.as_ref().ok_or_else(|| {
        authority_integrity_error("WorkspaceMaterialize Job lost its immutable operation scope")
    })
}

fn snapshot_spec(job: &JobRecord) -> CentralResult<&neoengramd::SnapshotMountSpec> {
    job.snapshot_spec.as_ref().ok_or_else(|| {
        authority_integrity_error("SnapshotMount Job lost its immutable operation scope")
    })
}

fn page_index_records(
    records: &[FileRecord],
    page_number: u32,
    page_size: usize,
) -> CentralResult<(u32, Vec<IndexDeltaRecord>)> {
    let (page_count, range) = page_range(records.len(), page_number, page_size, "Index")?;
    let records = records[range]
        .iter()
        .map(|record| IndexDeltaRecord::Upsert {
            path: record.path.clone(),
            manifest_id: record.manifest_id,
            total_size: DecimalU64::new(record.total_size),
            chunk_count: DecimalU64::new(record.chunk_count),
            extensions: Extensions::new(),
        })
        .collect();
    Ok((page_count, records))
}

fn page_manifest_chunks(
    manifest: &Manifest,
    page_number: u32,
    page_size: usize,
) -> CentralResult<(u32, Vec<WireChunkRef>)> {
    let (page_count, range) =
        page_range(manifest.chunks.len(), page_number, page_size, "Manifest")?;
    let chunks = manifest.chunks[range]
        .iter()
        .map(|chunk| WireChunkRef {
            object_id: chunk.object_id,
            offset: DecimalU64::new(chunk.offset),
            size: DecimalU64::new(chunk.size),
            extensions: Extensions::new(),
        })
        .collect();
    Ok((page_count, chunks))
}

fn page_range(
    record_count: usize,
    page_number: u32,
    page_size: usize,
    resource: &'static str,
) -> CentralResult<(u32, std::ops::Range<usize>)> {
    if page_size == 0 {
        return Err(CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            format!("{resource} page size must be greater than zero"),
        )
        .with_retryable(false));
    }
    let page_count_usize = record_count.div_ceil(page_size).max(1);
    let page_count = u32::try_from(page_count_usize).map_err(|_| {
        CentralError::new(
            CentralErrorCode::Internal,
            format!("{resource} page count exceeds the wire range"),
        )
    })?;
    if page_number >= page_count {
        return Err(CentralError::new(
            CentralErrorCode::ProtocolInvalid,
            format!("{resource} page_number is outside the immutable snapshot"),
        )
        .with_retryable(false));
    }
    let start = usize::try_from(page_number)
        .ok()
        .and_then(|page| page.checked_mul(page_size))
        .ok_or_else(|| {
            CentralError::new(
                CentralErrorCode::ProtocolInvalid,
                format!("{resource} page offset overflow"),
            )
        })?;
    let end = start.saturating_add(page_size).min(record_count);
    Ok((page_count, start..end))
}

fn validate_manifest_reference(
    record: &FileRecord,
    manifest: &Manifest,
    manifest_id: ManifestId,
) -> CentralResult<()> {
    let canonical_id = manifest.canonical_id().map_err(|error| {
        authority_integrity_error(format!("immutable Manifest is invalid: {error}"))
    })?;
    let chunk_count = manifest.chunk_count().map_err(|error| {
        authority_integrity_error(format!(
            "immutable Manifest chunk count is invalid: {error}"
        ))
    })?;
    if canonical_id != manifest_id
        || record.total_size != manifest.total_size
        || record.chunk_count != chunk_count
    {
        return Err(authority_integrity_error(
            "frozen Commit record differs from its immutable Manifest",
        ));
    }
    Ok(())
}

fn require_manifest_reference(
    records: &[FileRecord],
    manifest_id: ManifestId,
) -> CentralResult<&FileRecord> {
    records
        .iter()
        .find(|record| record.manifest_id == manifest_id)
        .ok_or_else(|| {
            scope_mismatch("Manifest is not referenced by the frozen materialization Commit")
        })
}

fn authority_integrity_error(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message)
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
    ) || (job.operation == JobOperation::SnapshotMount
        && matches!(job.state, JobState::Succeeded | JobState::RecoveryRequired))
    {
        Ok(())
    } else {
        Err(CentralError::new(
            CentralErrorCode::InvalidState,
            format!("{action} is not allowed in state {:?}", job.state),
        )
        .with_retryable(false))
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

#[cfg(test)]
mod tests {
    use neoengram_core::{ChunkRef, ChunkingStrategy, LogicalPath, ObjectId};

    use super::*;

    #[test]
    fn manifest_pages_expose_only_ordered_object_references() {
        let first = ChunkRef::new(ObjectId::for_bytes(b"ab"), 0, 2).unwrap();
        let second = ChunkRef::new(ObjectId::for_bytes(b"cd"), 2, 2).unwrap();
        let manifest = Manifest::new(
            4,
            ChunkingStrategy::FastCdc,
            vec![first.clone(), second.clone()],
        )
        .unwrap();

        let (page_count, first_page) = page_manifest_chunks(&manifest, 0, 1).unwrap();
        assert_eq!(page_count, 2);
        assert_eq!(first_page.len(), 1);
        assert_eq!(first_page[0].object_id, first.object_id);
        assert_eq!(first_page[0].offset.get(), 0);
        assert_eq!(first_page[0].size.get(), 2);

        let (_, second_page) = page_manifest_chunks(&manifest, 1, 1).unwrap();
        assert_eq!(second_page.len(), 1);
        assert_eq!(second_page[0].object_id, second.object_id);
        assert_eq!(second_page[0].offset.get(), 2);
        assert_eq!(second_page[0].size.get(), 2);

        let outside = page_manifest_chunks(&manifest, 2, 1).unwrap_err();
        assert_eq!(outside.code(), CentralErrorCode::ProtocolInvalid);
    }

    #[test]
    fn manifest_must_be_referenced_by_and_match_the_frozen_commit_index() {
        let manifest = Manifest::new(
            3,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(ObjectId::for_bytes(b"abc"), 0, 3).unwrap()],
        )
        .unwrap();
        let manifest_id = manifest.canonical_id().unwrap();
        let record =
            FileRecord::from_manifest(LogicalPath::parse("frozen/path.txt").unwrap(), &manifest)
                .unwrap();
        let records = vec![record.clone()];

        let referenced = require_manifest_reference(&records, manifest_id).unwrap();
        validate_manifest_reference(referenced, &manifest, manifest_id).unwrap();

        let unreferenced = require_manifest_reference(
            &records,
            ManifestId::from_digest(neoengram_core::ContentDigest::hash(b"other")),
        )
        .unwrap_err();
        assert_eq!(unreferenced.code(), CentralErrorCode::AssignmentMismatch);

        let mut mismatched = record;
        mismatched.total_size = 4;
        let corrupt = validate_manifest_reference(&mismatched, &manifest, manifest_id).unwrap_err();
        assert_eq!(corrupt.code(), CentralErrorCode::StorageFailure);
    }

    #[test]
    fn empty_snapshots_are_one_page_and_nonempty_index_pages_preserve_order() {
        let (empty_count, empty) = page_index_records(&[], 0, 32).unwrap();
        assert_eq!(empty_count, 1);
        assert!(empty.is_empty());

        let empty_manifest = Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).unwrap();
        let first =
            FileRecord::from_manifest(LogicalPath::parse("a.txt").unwrap(), &empty_manifest)
                .unwrap();
        let second =
            FileRecord::from_manifest(LogicalPath::parse("b.txt").unwrap(), &empty_manifest)
                .unwrap();
        let (page_count, page) = page_index_records(&[first, second], 1, 1).unwrap();
        assert_eq!(page_count, 2);
        assert!(matches!(
            &page[0],
            IndexDeltaRecord::Upsert { path, .. } if path.as_str() == "b.txt"
        ));
    }
}
