//! Placement-first control actions.
//!
//! The authority owns the logical request and its fencing identity.  Byte movement is delegated
//! to Agent/Gateway data-plane workers; this service never reads or stores object payloads.

use fusen_rs::{Error, ErrorCategory};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    ArtifactId, ProjectId, ReplicationId, ReplicationState, RequestId, StorageVolumeId, TenantId,
    WorkspaceId, WorkspaceLifecycle,
};

use crate::{
    dto::{
        CommitAvailabilityView, CreateCommitReplicationRequest, CreateCommitReplicationResponse,
        CreateWorkspaceRequest, CreateWorkspaceResponse, QueryCommitAvailabilityRequest,
        QueryCommitAvailabilityResponse, QueryCommitReplicationRequest,
        QueryCommitReplicationResponse, ReplicationView, WorkspaceView,
    },
    error::{application_error, invalid_request, map_central_error},
    identity::{AuthenticatedIdentity, Permission},
};

use super::CatalogService;

fn not_found(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::NotFound,
        "resource_not_found",
        "RESOURCE_NOT_FOUND",
        format!("{resource} not found"),
        false,
    )
}

fn parse_tenant(value: String) -> Result<TenantId, Error> {
    TenantId::new(value).map_err(|error| invalid_request(format!("tenant_id: {error}")))
}

fn parse_volume(value: String) -> Result<StorageVolumeId, Error> {
    StorageVolumeId::new(value)
        .map_err(|error| invalid_request(format!("target_storage_volume_id: {error}")))
}

fn parse_commit(value: String) -> Result<ContentDigest, Error> {
    value
        .parse()
        .map_err(|_| invalid_request("commit_id must be a 64-character digest"))
}

fn replication_id(request_id: &str) -> Result<ReplicationId, Error> {
    let digest = blake3::hash(format!("neoengram-replication\0{request_id}").as_bytes());
    ReplicationId::new(format!("replication-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("replication_id: {error}")))
}

fn workspace_id(request_id: &str) -> Result<WorkspaceId, Error> {
    let digest = blake3::hash(format!("neoengram-workspace\0{request_id}").as_bytes());
    WorkspaceId::new(format!("workspace-{}", &digest.to_hex()[..32]))
        .map_err(|error| invalid_request(format!("workspace_id: {error}")))
}

fn replication_state_name(state: ReplicationState) -> &'static str {
    match state {
        ReplicationState::Queued => "queued",
        ReplicationState::Planning => "planning",
        ReplicationState::Transferring => "transferring",
        ReplicationState::Verifying => "verifying",
        ReplicationState::Published => "published",
        ReplicationState::Failed => "failed",
        ReplicationState::Cancelled => "cancelled",
    }
}

fn workspace_lifecycle_name(state: WorkspaceLifecycle) -> &'static str {
    match state {
        WorkspaceLifecycle::Provisioning => "provisioning",
        WorkspaceLifecycle::Active => "active",
        WorkspaceLifecycle::Unavailable => "unavailable",
        WorkspaceLifecycle::Deleting => "deleting",
        WorkspaceLifecycle::Deleted => "deleted",
    }
}

fn idempotency_conflict(resource: &'static str) -> Error {
    application_error(
        ErrorCategory::Conflict,
        "request_id_reused",
        "REQUEST_ID_REUSED",
        format!("{resource} request_id is already bound to another payload"),
        false,
    )
}

fn replication_view(record: &crate::ReplicationRecord) -> ReplicationView {
    ReplicationView {
        replication_id: record.replication_id.to_string(),
        tenant_id: record.tenant_id.to_string(),
        commit_id: record.commit_id.to_string(),
        target_storage_volume_id: record.target_storage_volume_id.to_string(),
        state: replication_state_name(record.state).to_owned(),
        object_set_digest: record.object_set_digest.to_string(),
        completed_objects: record.completed_objects.to_string(),
        total_objects: record.total_objects.to_string(),
        issue: record
            .issue_code
            .as_ref()
            .map(|code| crate::dto::ResourceIssueSummary {
                code: code.clone(),
                message: record.issue_message.clone().unwrap_or_default(),
                retryable: false,
                occurred_at_unix_ms: Some(record.updated_at_unix_ms.to_string()),
            }),
    }
}

impl CatalogService {
    pub async fn create_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateCommitReplicationRequest,
    ) -> Result<CreateCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotCreate, &tenant_id)
            .await?;
        let commit_digest = parse_commit(request.commit_id)?;
        let target_volume = parse_volume(request.target_storage_volume_id)?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let replication_id = replication_id(&request.request_id)?;
        let Some(placement_repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };

        // Resolve the idempotency key before checking mutable target-volume state. A retry must
        // return the original authority row even when the target has since gone offline.
        if let Some(existing) = placement_repository
            .get_replication_by_request_id(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            if existing.commit_id != commit_digest
                || existing.target_storage_volume_id != target_volume
            {
                return Err(idempotency_conflict("replication"));
            }
            return Ok(CreateCommitReplicationResponse {
                replication: replication_view(&existing),
                replayed: true,
            });
        }

        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target_volume)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("storage volume"))?;
        // A replication target must be writable and fully ready. A degraded Volume may still
        // serve existing reads, but accepting it as a destination would publish a copy onto an
        // already unhealthy failure domain and make the resulting PlacementSet misleading.
        if !volume.lifecycle.is_active() || volume.state != crate::StorageVolumeState::Ready {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "target StorageVolume is not ready",
                true,
            ));
        }
        let object_set = placement_repository
            .get_commit_object_set(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("commit object set"))?;
        let availability = placement_repository
            .commit_availability(&tenant_id, &commit_digest)
            .await
            .map_err(map_central_error)?;
        if matches!(
            availability.data_health,
            neoengram_domain::protocol::DataHealth::Unavailable
        ) {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "source_unavailable",
                "SOURCE_UNAVAILABLE",
                "no verified PlacementSet can provide every Commit object",
                true,
            ));
        }
        let object_set_digest = object_set.object_set.object_set_digest;
        let total_objects = u64::try_from(object_set.object_set.objects.len())
            .map_err(|_| invalid_request("commit object count exceeds supported range"))?;
        let now = self.clock.now();
        let record = crate::ReplicationRecord {
            tenant_id: tenant_id.clone(),
            replication_id,
            commit_id: commit_digest,
            target_backend_id: target_volume.to_string(),
            target_storage_volume_id: target_volume,
            object_set_digest,
            state: ReplicationState::Queued,
            request_id,
            completed_objects: 0,
            total_objects,
            issue_code: None,
            issue_message: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let stored = placement_repository
            .insert_replication(record.clone())
            .await
            .map_err(map_central_error)?;
        let replayed = stored != record;
        let record = stored;
        Ok(CreateCommitReplicationResponse {
            replication: replication_view(&record),
            replayed,
        })
    }

    pub async fn query_commit_replication(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitReplicationRequest,
    ) -> Result<QueryCommitReplicationResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        let replication_id = ReplicationId::new(request.replication_id)
            .map_err(|error| invalid_request(format!("replication_id: {error}")))?;
        let Some(repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };
        let record = repository
            .get_replication(&tenant_id, &replication_id)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("replication"))?;
        Ok(QueryCommitReplicationResponse {
            replication: replication_view(&record),
        })
    }

    pub async fn query_commit_availability(
        &self,
        identity: &AuthenticatedIdentity,
        request: QueryCommitAvailabilityRequest,
    ) -> Result<QueryCommitAvailabilityResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::SnapshotRead, &tenant_id)
            .await?;
        let commit_id = parse_commit(request.commit_id)?;
        let availability = if let Some(repository) = &self.placement {
            repository
                .commit_availability(&tenant_id, &commit_id)
                .await
                .map_err(map_central_error)?
        } else {
            crate::CommitAvailabilityRecord {
                tenant_id: tenant_id.clone(),
                commit_id,
                data_health: neoengram_domain::protocol::DataHealth::Unavailable,
                verified_placements: 0,
                missing_objects: 0,
                verified_storage_volume_ids: Vec::new(),
            }
        };
        Ok(QueryCommitAvailabilityResponse {
            availability: CommitAvailabilityView {
                commit_id: availability.commit_id.to_string(),
                data_health: format!("{:?}", availability.data_health).to_lowercase(),
                verified_placements: availability.verified_placements.to_string(),
                missing_objects: availability.missing_objects.to_string(),
                verified_storage_volume_ids: availability
                    .verified_storage_volume_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
        })
    }

    pub async fn create_workspace(
        &self,
        identity: &AuthenticatedIdentity,
        request: CreateWorkspaceRequest,
    ) -> Result<CreateWorkspaceResponse, Error> {
        let tenant_id = parse_tenant(request.tenant_id)?;
        self.require_tenant(identity, Permission::PlaygroundCreate, &tenant_id)
            .await?;
        let project_id = ProjectId::new(request.project_id)
            .map_err(|error| invalid_request(format!("project_id: {error}")))?;
        let artifact_id = ArtifactId::new(request.artifact_id)
            .map_err(|error| invalid_request(format!("artifact_id: {error}")))?;
        let target = parse_volume(request.target_storage_volume_id)?;
        let request_id = RequestId::new(request.request_id.clone())
            .map_err(|error| invalid_request(format!("request_id: {error}")))?;
        let Some(placement_repository) = &self.placement else {
            return Err(application_error(
                ErrorCategory::Unavailable,
                "placement_authority_unavailable",
                "PLACEMENT_AUTHORITY_UNAVAILABLE",
                "placement authority is not configured",
                true,
            ));
        };
        if let Some(existing) = placement_repository
            .get_workspace_by_request_id(&tenant_id, &request_id)
            .await
            .map_err(map_central_error)?
        {
            let base_commit_id = request
                .base_commit_id
                .as_deref()
                .map(|value| parse_commit(value.to_owned()))
                .transpose()?;
            if existing.project_id != project_id
                || existing.artifact_id != artifact_id
                || existing.base_commit_id != base_commit_id
                || existing.target_storage_volume_id != target
            {
                return Err(idempotency_conflict("workspace"));
            }
            return Ok(CreateWorkspaceResponse {
                workspace: WorkspaceView {
                    workspace_id: existing.workspace_id.to_string(),
                    tenant_id: existing.tenant_id.to_string(),
                    project_id: existing.project_id.to_string(),
                    artifact_id: existing.artifact_id.to_string(),
                    base_commit_id: existing.base_commit_id.map(|value| value.to_string()),
                    target_storage_volume_id: existing.target_storage_volume_id.to_string(),
                    lifecycle: workspace_lifecycle_name(existing.lifecycle).to_owned(),
                },
                replayed: true,
            });
        }
        let volume = self
            .repository
            .get_storage_volume(&tenant_id, &target)
            .await
            .map_err(map_central_error)?
            .ok_or_else(|| not_found("storage volume"))?;
        if !volume.lifecycle.is_active()
            || !matches!(volume.state, crate::StorageVolumeState::Ready)
        {
            return Err(application_error(
                ErrorCategory::Conflict,
                "storage_volume_not_ready",
                "STORAGE_VOLUME_NOT_READY",
                "target StorageVolume is not ready",
                true,
            ));
        }
        let id = workspace_id(&request.request_id)?;
        let base_commit_id = request
            .base_commit_id
            .as_deref()
            .map(|value| parse_commit(value.to_owned()))
            .transpose()?;
        if let Some(commit_id) = base_commit_id {
            let availability = placement_repository
                .commit_availability(&tenant_id, &commit_id)
                .await
                .map_err(map_central_error)?;
            if matches!(
                availability.data_health,
                neoengram_domain::protocol::DataHealth::Unavailable
            ) {
                return Err(application_error(
                    ErrorCategory::Unavailable,
                    "data_unavailable",
                    "DATA_UNAVAILABLE",
                    "the requested base Commit has no readable verified PlacementSet",
                    true,
                ));
            }
        }
        let now = self.clock.now();
        let workspace = crate::WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            workspace_id: id,
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            base_commit_id,
            target_storage_volume_id: target.clone(),
            request_id,
            lifecycle: WorkspaceLifecycle::Provisioning,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        let stored = placement_repository
            .insert_workspace(workspace.clone())
            .await
            .map_err(map_central_error)?;
        let replayed = stored != workspace;
        let workspace = stored;
        Ok(CreateWorkspaceResponse {
            workspace: WorkspaceView {
                workspace_id: workspace.workspace_id.to_string(),
                tenant_id: workspace.tenant_id.to_string(),
                project_id: workspace.project_id.to_string(),
                artifact_id: workspace.artifact_id.to_string(),
                base_commit_id: workspace.base_commit_id.map(|value| value.to_string()),
                target_storage_volume_id: workspace.target_storage_volume_id.to_string(),
                lifecycle: workspace_lifecycle_name(workspace.lifecycle).to_owned(),
            },
            replayed,
        })
    }
}
